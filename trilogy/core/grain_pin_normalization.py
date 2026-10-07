"""Build-time pin of a NULL-absorbing select output to the select's grain.

``coalesce(x, d)`` and a CASE with an ELSE take a value where their reads are
NULL, so where they are evaluated decides what they say on a row a ``~``
binding pads. Evaluated at their reads' own keys they are absent there (NULL);
evaluated on the select's row the fallback fires. As a select output they are
evaluated on the select's row, the way a bare aggregate groups by the select's
grain: every select key their reads' rows do not cover becomes an input of a
``GRAIN_PIN``. The pin is part of the lineage, so a column persisted at the
reads' own grain is never read for it, while a select whose keys the reads
cover keeps the plain lineage and still reads that column.

Same contract as ``normalize_select_where_scope``: pure, and the pinned
concepts ride the returned copy's ``local_concepts`` under the output's own
address.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace as dc_replace
from typing import Any

from trilogy.core.domain_graph import DomainGraph
from trilogy.core.enums import Derivation, FunctionType, Purpose
from trilogy.core.having_normalization import _child_exprs
from trilogy.core.models.author import (
    AggregateWrapper,
    CaseElse,
    CaseSimpleWhen,
    CaseWhen,
    Comparison,
    Concept,
    ConceptRef,
    Conditional,
    Function,
    Parenthetical,
    SelectLineage,
    UndefinedConcept,
)
from trilogy.core.models.environment import Environment


def _lookup(
    address: str, local: Mapping[str, Concept], environment: Environment
) -> Concept | None:
    concept = local.get(address) or environment.concepts.get(address)
    if concept is None or isinstance(concept, UndefinedConcept):
        return None
    return concept


def _inline(expr: Any, local: Mapping[str, Concept], environment: Environment) -> Any:
    """`expr` with every BASIC concept it reads replaced by its lineage, so the
    whole expression is evaluated on one row."""
    if isinstance(expr, ConceptRef):
        concept = _lookup(expr.address, local, environment)
        if concept is None or concept.derivation != Derivation.BASIC:
            return expr
        if concept.lineage is None:
            return expr
        return _inline(concept.lineage, local, environment)
    if isinstance(expr, Function):
        return dc_replace(
            expr, arguments=[_inline(a, local, environment) for a in expr.arguments]
        )
    if isinstance(expr, Parenthetical):
        return dc_replace(expr, content=_inline(expr.content, local, environment))
    if isinstance(expr, (Comparison, Conditional)):
        return dc_replace(
            expr,
            left=_inline(expr.left, local, environment),
            right=_inline(expr.right, local, environment),
        )
    if isinstance(expr, CaseWhen):
        return dc_replace(
            expr,
            comparison=_inline(expr.comparison, local, environment),
            expr=_inline(expr.expr, local, environment),
        )
    if isinstance(expr, CaseSimpleWhen):
        return dc_replace(
            expr,
            value_expr=_inline(expr.value_expr, local, environment),
            expr=_inline(expr.expr, local, environment),
        )
    if isinstance(expr, CaseElse):
        return dc_replace(expr, expr=_inline(expr.expr, local, environment))
    return expr


def absorbs_null(expr: Any) -> bool:
    """A COALESCE or an ELSE in a row expression, outside any inline aggregate
    (whose argument is read on the aggregate's input rows)."""
    if isinstance(expr, CaseElse):
        return True
    if isinstance(expr, Function) and expr.operator == FunctionType.COALESCE:
        return True
    if isinstance(expr, AggregateWrapper):
        return False
    return any(absorbs_null(child) for child in _child_exprs(expr))


def _row_keys(concept: Concept) -> set[str]:
    if concept.purpose == Purpose.KEY:
        return {concept.address}
    if concept.derivation == Derivation.CONSTANT:
        return set()
    if concept.keys:
        return set(concept.keys)
    return set(concept.grain.components) or {concept.address}


def _own_keys(
    expr: Any,
    local: Mapping[str, Concept],
    environment: Environment,
    anchors: set[str],
) -> set[str]:
    """The keys of the rows `expr` reads. An inline aggregate is read on its
    `by` (a bare one on the select's grain), never on its argument's rows."""
    if isinstance(expr, ConceptRef):
        read = _lookup(expr.address, local, environment)
        return _row_keys(read) if read is not None else set()
    if isinstance(expr, AggregateWrapper):
        return {b.address for b in expr.by} if expr.by else set(anchors)
    out: set[str] = set()
    for child in _child_exprs(expr):
        out |= _own_keys(child, local, environment, anchors)
    return out


def _referenced(
    roots: list[str], local: Mapping[str, Concept], environment: Environment
) -> list[Concept]:
    """Every concept the statement reads by address, through row expressions
    and aggregates. A filter or window scopes its own reads, so it is not
    entered."""
    seen: dict[str, Concept] = {}
    stack = list(roots)
    while stack:
        address = stack.pop()
        if address in seen:
            continue
        concept = _lookup(address, local, environment)
        if concept is None:
            continue
        seen[address] = concept
        if concept.lineage is not None and concept.derivation in (
            Derivation.BASIC,
            Derivation.AGGREGATE,
        ):
            stack.extend(r.address for r in concept.lineage.concept_arguments)
    return list(seen.values())


def is_grain_pin(concept: Concept) -> bool:
    return (
        isinstance(concept.lineage, Function)
        and concept.lineage.operator == FunctionType.GRAIN_PIN
    )


def _is_absorbing_node(expr: Any) -> bool:
    if not isinstance(expr, Function):
        return False
    return expr.operator == FunctionType.COALESCE or (
        expr.operator == FunctionType.CASE
        and any(isinstance(a, CaseElse) for a in expr.arguments)
    )


class _Pinner:
    def __init__(
        self,
        local: Mapping[str, Concept],
        environment: Environment,
        graph: DomainGraph,
        output_keys: dict[str, frozenset[str]],
    ):
        self.local = local
        self.environment = environment
        self.graph = graph
        self.output_keys = output_keys

    def pin(self, expr: Any, owner: str | None) -> Function | None:
        """`expr` evaluated on the select's row, or None when its reads' rows
        already cover every select key but `owner`'s own."""
        anchors: set[str] = set().union(
            *(keys for address, keys in self.output_keys.items() if address != owner)
        ) - {owner}
        own = _own_keys(expr, self.local, self.environment, anchors)
        uncovered = sorted(k for k in anchors - own if not self.graph.covers(own, k))
        if not uncovered:
            return None
        return Function(
            operator=FunctionType.GRAIN_PIN,
            output_datatype=expr.output_datatype,
            output_purpose=expr.output_purpose,
            arguments=[
                expr,
                *(self.environment.concepts[k].reference for k in uncovered),
            ],
            arg_count=-1,
        )

    def inline_pins(self, expr: Any, owner: str | None, in_agg: bool) -> Any:
        """`expr` with each inline NULL-absorbing expression an aggregate reads
        (or, `in_agg`, any it holds) pinned; a concept read by address is
        pinned by address."""
        if in_agg and _is_absorbing_node(expr):
            inlined = _inline(expr, self.local, self.environment)
            return self.pin(inlined, owner) or expr
        if isinstance(expr, AggregateWrapper):
            return dc_replace(
                expr,
                function=dc_replace(
                    expr.function,
                    arguments=[
                        self.inline_pins(a, owner, True)
                        for a in expr.function.arguments
                    ],
                ),
            )
        if isinstance(expr, Function):
            return dc_replace(
                expr,
                arguments=[self.inline_pins(a, owner, in_agg) for a in expr.arguments],
            )
        if isinstance(expr, Parenthetical):
            return dc_replace(
                expr, content=self.inline_pins(expr.content, owner, in_agg)
            )
        if isinstance(expr, (Comparison, Conditional)):
            return dc_replace(
                expr,
                left=self.inline_pins(expr.left, owner, in_agg),
                right=self.inline_pins(expr.right, owner, in_agg),
            )
        return expr


def normalize_select_grain_pins(
    base: SelectLineage, environment: Environment, graph: DomainGraph
) -> SelectLineage:
    """One address carries one value per statement, as a bare aggregate does:
    a NULL-absorbing concept is pinned wherever the statement reads it, as an
    output, a grouping key, an aggregate's input or in the WHERE, and so is
    the same expression written inline in an aggregate or the WHERE."""
    # every output of a ROLLUP/CUBE select is a grouping key: evaluated on the
    # rows the pass groups, never on its subtotal rows
    if base.grouping is not None:
        return base
    local = base.local_concepts
    outputs = [
        c
        for ref in base.selection
        if (c := _lookup(ref.address, local, environment)) is not None
    ]
    # each output's own row identity: a window's partition key is determined
    # by the row it numbers, not a row of the select
    pinner = _Pinner(
        local,
        environment,
        graph,
        {c.address: graph.fd_minimal(_row_keys(c)) for c in outputs},
    )
    roots = [c.address for c in outputs]
    if base.where_clause is not None:
        roots += [r.address for r in base.where_clause.concept_arguments]
    pins: dict[str, Concept] = {}
    for concept in _referenced(roots, local, environment):
        if concept.lineage is None or concept.derivation not in (
            Derivation.BASIC,
            Derivation.AGGREGATE,
        ):
            continue
        inlined = _inline(concept.lineage, local, environment)
        if concept.derivation == Derivation.BASIC and absorbs_null(inlined):
            lineage = pinner.pin(inlined, concept.address)
        else:
            rewritten = pinner.inline_pins(concept.lineage, concept.address, False)
            lineage = None if rewritten == concept.lineage else rewritten
        if lineage is not None:
            pins[concept.address] = dc_replace(concept, lineage=lineage)
    where_clauses = [
        dc_replace(wc, conditional=pinner.inline_pins(wc.conditional, None, True))
        for wc in base.where_clauses
    ]
    if not pins and where_clauses == base.where_clauses:
        return base
    # pins build first, so every later read of the address resolves to them
    rest = {k: v for k, v in local.items() if k not in pins}
    return dc_replace(
        base, local_concepts={**pins, **rest}, where_clauses=where_clauses
    )
