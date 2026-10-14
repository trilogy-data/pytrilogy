"""Decompose a datasource ``assert`` condition into the queries that check it.

An assertion is a boolean over aggregates of one build of the datasource,
optionally grouped (``count(id) by region``), and may compare them with the
same aggregates over the asset being replaced (``prior(count(id) by region)``).
The two sides cannot be read in one query -- they are the same columns at two
physical locations -- so the condition is split into:

- ``new_terms``: the aggregate subtrees outside any ``prior()``, read from
  the new build;
- ``prior_terms``: the argument of each ``prior()``, read from the previous
  asset;
- ``template``: the condition with every term replaced by a placeholder
  reference, which is evaluated once per group with the fetched values
  substituted back in.

Every grouped term must share one ``by`` grain; that grain is the join key
between the two sides, and the only columns the template may name directly.
"""

from __future__ import annotations

from dataclasses import dataclass, fields, is_dataclass
from dataclasses import replace as dc_replace
from typing import Any

from trilogy.core.enums import FunctionClass, FunctionType
from trilogy.core.models.author import (
    AggregateWrapper,
    Concept,
    ConceptRef,
    Function,
    FunctionCallWrapper,
)

NEW_PLACEHOLDER = "__assert_new_"
PRIOR_PLACEHOLDER = "__assert_prior_"


class AssertionSpecError(ValueError):
    """The condition cannot be evaluated as a build check."""


@dataclass
class AssertionTerm:
    placeholder: str
    expr: Any
    prior: bool


@dataclass
class AssertionPlan:
    group_keys: list[ConceptRef]
    new_terms: list[AssertionTerm]
    prior_terms: list[AssertionTerm]
    template: Any

    @property
    def terms(self) -> list[AssertionTerm]:
        return self.new_terms + self.prior_terms

    @property
    def uses_prior(self) -> bool:
        return bool(self.prior_terms)


def _is_leaf(node: Any) -> bool:
    return isinstance(node, (ConceptRef, Concept)) or not is_dataclass(node)


def map_expression(node: Any, fn: Any) -> Any:
    """Rebuild ``node`` bottom-up through ``fn``, which returns a replacement
    for a subtree or None to recurse into it. Leaves and non-AST values pass
    through untouched."""
    replaced = fn(node)
    if replaced is not None:
        return replaced
    if isinstance(node, list):
        return [map_expression(x, fn) for x in node]
    if _is_leaf(node):
        return node
    changes: dict[str, Any] = {}
    for f in fields(node):
        if not f.init:
            continue
        value = getattr(node, f.name)
        if isinstance(value, list):
            new_value: Any = [map_expression(x, fn) for x in value]
            if any(a is not b for a, b in zip(new_value, value)):
                changes[f.name] = new_value
        elif is_dataclass(value) and not isinstance(value, type):
            new_value = map_expression(value, fn)
            if new_value is not value:
                changes[f.name] = new_value
    return dc_replace(node, **changes) if changes else node


def walk_expression(node: Any) -> list[Any]:
    """Every dataclass subtree of ``node``, pre-order, leaves included."""
    found: list[Any] = []

    def visit(current: Any) -> None:
        if isinstance(current, list):
            for x in current:
                visit(x)
            return
        if not is_dataclass(current) or isinstance(current, type):
            return
        found.append(current)
        if _is_leaf(current):
            return
        for f in fields(current):
            visit(getattr(current, f.name))

    visit(node)
    return found


def _is_prior(node: Any) -> bool:
    return isinstance(node, Function) and node.operator == FunctionType.PRIOR


def _is_aggregate(node: Any) -> bool:
    if isinstance(node, AggregateWrapper):
        return True
    return (
        isinstance(node, Function)
        and node.operator in FunctionClass.AGGREGATE_FUNCTIONS.value
    )


def contains_prior(node: Any) -> bool:
    return any(_is_prior(x) for x in walk_expression(node))


def _group_keys(terms: list[AssertionTerm]) -> list[ConceptRef]:
    grains: dict[tuple[str, ...], list[ConceptRef]] = {}
    ungrouped: list[AssertionTerm] = []
    for term in terms:
        aggregates = [
            x for x in walk_expression(term.expr) if isinstance(x, AggregateWrapper)
        ]
        if not aggregates:
            continue
        for aggregate in aggregates:
            keys = []
            for item in aggregate.by:
                if not isinstance(item, ConceptRef):
                    raise AssertionSpecError(
                        f"`{aggregate}` groups by an expression; an assertion's"
                        " `by` keys must be columns of the datasource."
                    )
                keys.append(item)
            if not keys:
                ungrouped.append(term)
                continue
            grains.setdefault(tuple(sorted(k.address for k in keys)), keys)
    if len(grains) > 1:
        spelled = " vs ".join(", ".join(g) for g in grains)
        raise AssertionSpecError(
            f"grouped terms must share one `by` grain, got {spelled}."
        )
    if grains and ungrouped:
        raise AssertionSpecError(
            f"`{ungrouped[0].expr}` has no `by` grain but another term does;"
            " every aggregate in a grouped assertion needs the same `by`."
        )
    return next(iter(grains.values()), [])


def plan_assertion(condition: Any) -> AssertionPlan:
    """Split ``condition`` into its terms and template, validating the shape.

    Raises :class:`AssertionSpecError` for a nested ``prior()``, mixed or
    expression-valued ``by`` grains, a column referenced outside an aggregate
    (other than a ``by`` key), or a condition with nothing to measure.
    """
    terms: dict[str, AssertionTerm] = {}

    def term_for(expr: Any, prior: bool) -> ConceptRef:
        key = f"{'prior' if prior else 'new'}:{expr}"
        existing = terms.get(key)
        if existing is None:
            prefix = PRIOR_PLACEHOLDER if prior else NEW_PLACEHOLDER
            count = sum(1 for t in terms.values() if t.prior == prior)
            existing = AssertionTerm(
                placeholder=f"{prefix}{count}", expr=expr, prior=prior
            )
            terms[key] = existing
        return ConceptRef(address=existing.placeholder, datatype=expr.datatype)

    def lift_prior(node: Any) -> Any:
        if not _is_prior(node):
            return None
        inner = node.arguments[0]
        if contains_prior(inner):
            raise AssertionSpecError("prior() cannot be nested inside prior().")
        return term_for(inner, prior=True)

    def lift_aggregate(node: Any) -> Any:
        if _is_aggregate(node):
            return term_for(node, prior=False)
        return None

    def unwrap_call(node: Any) -> Any:
        # A `@function(...)` call carries its expanded body; the call itself
        # is only kept so the declaration renders as written.
        if isinstance(node, FunctionCallWrapper):
            return map_expression(node.content, unwrap_call)
        return None

    template = map_expression(condition, unwrap_call)
    template = map_expression(map_expression(template, lift_prior), lift_aggregate)
    planned = list(terms.values())
    new_terms = [t for t in planned if not t.prior]
    prior_terms = [t for t in planned if t.prior]
    if not planned:
        raise AssertionSpecError(
            "an assertion must aggregate the build (e.g. `count(id) >= 1`);"
            f" `{condition}` measures nothing."
        )
    group_keys = _group_keys(planned)
    allowed = {k.address for k in group_keys} | {t.placeholder for t in planned}
    stray = sorted(
        {
            x.address
            for x in walk_expression(template)
            if isinstance(x, ConceptRef) and x.address not in allowed
        }
    )
    if stray:
        raise AssertionSpecError(
            f"{', '.join(repr(s) for s in stray)} referenced outside an"
            " aggregate; an assertion compares aggregates of the build, so a"
            " column may only appear inside one or as its `by` key."
        )
    return AssertionPlan(
        group_keys=group_keys,
        new_terms=new_terms,
        prior_terms=prior_terms,
        template=template,
    )


def substitute(template: Any, values: dict[str, Any]) -> Any:
    """``template`` with each placeholder reference replaced by its value."""

    def swap(node: Any) -> Any:
        if isinstance(node, ConceptRef) and node.address in values:
            return values[node.address]
        return None

    return map_expression(template, swap)
