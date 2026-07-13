"""Derived-value scope diagnostics.

Implements the factual scope report of docs/SPEC_query_derived_value_scopes.md:
for every aggregate and window computation a query uses, describe the effective
input population (filters applied before the value is computed), grouping or
partition/order, input grain for nested computations, and filters applied to
rows carrying the already-computed value.

Extraction runs on the post-normalization ``BuildSelectLineage`` (after HAVING
normalization and WHERE dual-scope splitting), so it reports the planner's
interpretation, not authored syntax. It is observational only: it must never
raise into query processing (callers wrap it) and executes no queries.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Literal, Union

from trilogy.constants import (
    DEFAULT_NAMESPACE,
    VIRTUAL_CONCEPT_PREFIX,
    MagicConstants,
)
from trilogy.core.constants import ALL_ROWS_CONCEPT
from trilogy.core.enums import (
    AggregateGroupingMode,
    BooleanOperator,
    Derivation,
    FunctionClass,
    FunctionType,
)
from trilogy.core.models.author import (
    AggregateWrapper,
    Between,
    Comparison,
    Concept,
    ConceptRef,
    Conditional,
    FilterItem,
    Function,
    MultiSelectLineage,
    NavigationWindowItem,
    NumberingWindowItem,
    Parenthetical,
    SelectLineage,
    WhereClause,
)
from trilogy.core.models.build import (
    BuildAggregateWrapper,
    BuildBetween,
    BuildCaseElse,
    BuildCaseWhen,
    BuildComparison,
    BuildConcept,
    BuildConceptArgs,
    BuildConditional,
    BuildFilterItem,
    BuildFunction,
    BuildMultiSelectLineage,
    BuildNavigationWindowItem,
    BuildNumberingWindowItem,
    BuildParenthetical,
    BuildRowsetItem,
    BuildRowsetLineage,
    BuildSelectLineage,
    BuildSubselectItem,
    BuildWhereClause,
)
from trilogy.core.models.core import ListWrapper, TupleWrapper
from trilogy.core.where_scope_normalization import WHERE_SCOPE_SALT

_WSCOPE_SUFFIX = f"_{WHERE_SCOPE_SALT}"

_INFIX_OPERATORS: dict[FunctionType, str] = {
    FunctionType.ADD: "+",
    FunctionType.SUBTRACT: "-",
    FunctionType.MULTIPLY: "*",
    FunctionType.DIVIDE: "/",
}


@dataclass
class ScopeOrder:
    expression: str
    direction: str

    def render(self) -> str:
        return f"{self.expression} {self.direction}"


@dataclass
class DerivedValueScope:
    """Factual scope of one planned aggregate/window computation."""

    name: str
    kind: Literal["aggregate", "window"]
    expression: str
    input_filters: list[str] = field(default_factory=list)
    input_grain: list[str] = field(default_factory=list)
    group_by: list[str] = field(default_factory=list)
    partition_by: list[str] = field(default_factory=list)
    order_by: list[ScopeOrder] = field(default_factory=list)
    output_filters: list[str] = field(default_factory=list)
    input_values: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        """JSON form, trimmed for agent consumption: empty fields are omitted
        (absent input_filters = the unrestricted population), and `kind` /
        `order_by` are not serialized — the expression is self-describing and
        ordering is deferred until grain + conditions prove out."""
        out: dict = {
            "name": self.name,
            "expression": self.expression,
        }
        if self.input_values:
            out["input_values"] = self.input_values
        if self.input_filters:
            out["input_filters"] = self.input_filters
        if self.input_grain:
            out["input_grain"] = self.input_grain
        if self.group_by:
            out["group_by"] = self.group_by
        if self.partition_by:
            out["partition_by"] = self.partition_by
        if self.output_filters:
            out["output_filters"] = self.output_filters
        return out


def _short(address: str) -> str:
    if address.startswith(f"{DEFAULT_NAMESPACE}."):
        return address[len(DEFAULT_NAMESPACE) + 1 :]
    return address


def render_scope_expr(expr: object, depth: int = 0) -> str:
    """Compact, namespace-stripped rendering of author/build expressions for
    diagnostics. Deliberately lossy (no grains, no datatypes); semantically
    identical plans should render equivalently. Anonymous virtual concepts
    (minted names) render through their lineage so internal names never leak.
    ``depth`` guards against pathological lineage recursion."""
    if depth > 24:
        return "..."
    sub = depth + 1
    if expr is None:
        return "null"
    if isinstance(expr, MagicConstants):
        return str(expr.value)
    if isinstance(expr, bool):
        return str(expr)
    if isinstance(expr, str):
        return f"'{expr}'"
    if isinstance(expr, (int, float)):
        return str(expr)
    if isinstance(expr, (date, datetime)):
        return expr.isoformat()
    if isinstance(expr, BuildConcept):
        if expr.name.startswith(VIRTUAL_CONCEPT_PREFIX) and expr.lineage is not None:
            return render_scope_expr(expr.lineage, sub)
        return _short(expr.address)
    if isinstance(expr, (Concept, ConceptRef)):
        return _short(expr.address)
    if isinstance(expr, (BuildWhereClause, WhereClause)):
        return render_scope_expr(expr.conditional, sub)
    if isinstance(expr, (BuildComparison, Comparison, BuildConditional, Conditional)):
        # comparison covers the subselect-comparison subclasses of both families
        return (
            f"{render_scope_expr(expr.left, sub)} {expr.operator.value}"
            f" {render_scope_expr(expr.right, sub)}"
        )
    if isinstance(expr, (BuildBetween, Between)):
        return (
            f"{render_scope_expr(expr.left, sub)} between"
            f" {render_scope_expr(expr.low, sub)} and"
            f" {render_scope_expr(expr.high, sub)}"
        )
    if isinstance(expr, (BuildParenthetical, Parenthetical)):
        return f"({render_scope_expr(expr.content, sub)})"
    if isinstance(expr, (BuildFilterItem, FilterItem)):
        return (
            f"{render_scope_expr(expr.content, sub)}"
            f" ? {render_scope_expr(expr.where.conditional, sub)}"
        )
    if isinstance(expr, (BuildAggregateWrapper, AggregateWrapper)):
        return render_scope_expr(expr.function, sub)
    if isinstance(expr, (BuildNavigationWindowItem, NavigationWindowItem)):
        inner = render_scope_expr(expr.content, sub)
        if expr.offset is not None:
            return f"{expr.type.value}({inner}, {expr.offset})"
        return f"{expr.type.value}({inner})"
    if isinstance(expr, (BuildNumberingWindowItem, NumberingWindowItem)):
        args = ", ".join(render_scope_expr(a, sub) for a in expr.arguments)
        return f"{expr.type.value}({args})"
    if isinstance(expr, BuildCaseWhen):
        return (
            f"when {render_scope_expr(expr.comparison, sub)}"
            f" then {render_scope_expr(expr.expr, sub)}"
        )
    if isinstance(expr, BuildCaseElse):
        return f"else {render_scope_expr(expr.expr, sub)}"
    if isinstance(expr, (BuildFunction, Function)):
        operator = expr.operator
        if operator in (FunctionType.CONSTANT, FunctionType.ALIAS) and expr.arguments:
            return render_scope_expr(expr.arguments[0], sub)
        rendered = [render_scope_expr(a, sub) for a in expr.arguments]
        infix = _INFIX_OPERATORS.get(operator)
        if infix and len(rendered) == 2:
            return f"{rendered[0]} {infix} {rendered[1]}"
        return f"{operator.value}({', '.join(rendered)})"
    if isinstance(expr, (ListWrapper, list)):
        return f"[{', '.join(render_scope_expr(a, sub) for a in expr)}]"
    if isinstance(expr, (TupleWrapper, tuple)):
        return f"({', '.join(render_scope_expr(a, sub) for a in expr)})"
    return str(expr)


def _conjuncts(expr: object) -> list:
    """Split a condition tree on top-level ANDs into reportable leaf conditions."""
    if (
        isinstance(expr, (BuildConditional, Conditional))
        and expr.operator == BooleanOperator.AND
    ):
        return _conjuncts(expr.left) + _conjuncts(expr.right)
    return [expr]


def _dedup(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


@dataclass(frozen=True)
class _Ctx:
    """Population context for the value being visited: filters restricting rows
    before it computes, and filters applied to rows carrying its result."""

    input_filters: tuple[str, ...]
    output_filters: tuple[str, ...]


class _Extractor:
    def __init__(self) -> None:
        self.records: dict[str, DerivedValueScope] = {}
        self.visited: set[str] = set()

    def extract(
        self, statement: BuildSelectLineage | BuildMultiSelectLineage
    ) -> list[DerivedValueScope]:
        where = statement.where_clause
        having = statement.having_clause
        where_strs = (
            [render_scope_expr(c) for c in _conjuncts(where.conditional)]
            if where
            else []
        )
        having_strs = (
            [render_scope_expr(c) for c in _conjuncts(having.conditional)]
            if having
            else []
        )
        # WHERE-scope pass first: cross-row values referenced by the WHERE gate
        # rows using the unrestricted population (dual-scope normalization has
        # already split them onto twin addresses where scopes diverge). Where a
        # single instance serves both scopes, the population description wins.
        if where:
            for conjunct in _conjuncts(where.conditional):
                ctx = _Ctx(
                    input_filters=(),
                    output_filters=(render_scope_expr(conjunct),),
                )
                if isinstance(conjunct, BuildConcept):
                    self.visit(conjunct, ctx)
                elif isinstance(conjunct, BuildConceptArgs):
                    for concept in conjunct.concept_arguments:
                        self.visit(concept, ctx)
        # Select scope: outputs, HAVING, ORDER BY all compute over WHERE-filtered
        # rows; HAVING then restricts the rows carrying their results.
        select_ctx = _Ctx(
            input_filters=tuple(where_strs), output_filters=tuple(having_strs)
        )
        for concept in statement.output_components:
            self.visit(concept, select_ctx)
        if having:
            for concept in having.concept_arguments:
                self.visit(concept, select_ctx)
        if statement.order_by:
            for item in statement.order_by.items:
                for concept in item.concept_arguments:
                    self.visit(concept, select_ctx)
        return self._finish()

    def visit(
        self, concept: BuildConcept, ctx: _Ctx, display: str | None = None
    ) -> None:
        if concept.address in self.visited:
            return
        self.visited.add(concept.address)
        lineage = concept.lineage
        if lineage is None:
            return
        if isinstance(lineage, BuildAggregateWrapper):
            self._record_aggregate(concept, lineage.function, lineage, ctx, display)
        elif isinstance(lineage, BuildFunction):
            if lineage.operator in FunctionClass.AGGREGATE_FUNCTIONS.value:
                self._record_aggregate(concept, lineage, None, ctx, display)
            else:
                for arg in lineage.concept_arguments:
                    self.visit(arg, ctx)
        elif isinstance(lineage, (BuildNumberingWindowItem, BuildNavigationWindowItem)):
            self._record_window(concept, lineage, ctx, display)
        elif isinstance(lineage, BuildFilterItem):
            # A row-scalar `x ? cond`: not itself reportable, but either side
            # may reference cross-row values. A filter directly feeding an
            # aggregate/window is handled at that computation's site instead.
            for arg in lineage.concept_arguments:
                self.visit(arg, ctx)
        elif isinstance(lineage, BuildRowsetItem):
            self._visit_rowset(concept, lineage, ctx)
        elif isinstance(lineage, BuildSubselectItem):
            for arg in lineage.concept_arguments:
                self.visit(arg, ctx)
        # BuildMultiSelectLineage handles (union arms) are not walked in v1.

    def _record_aggregate(
        self,
        concept: BuildConcept,
        function: BuildFunction,
        wrapper: BuildAggregateWrapper | None,
        ctx: _Ctx,
        display: str | None = None,
    ) -> None:
        input_filters = list(ctx.input_filters)
        input_values: list[str] = []
        input_grain: list[str] = []
        self._collect_inputs(
            function.concept_arguments, input_values, input_grain, input_filters
        )
        self.records[concept.address] = DerivedValueScope(
            name=display or _short(concept.address),
            kind="aggregate",
            expression=render_scope_expr(function),
            input_filters=_dedup(input_filters),
            input_grain=_dedup(input_grain),
            group_by=self._group_of(concept, wrapper),
            output_filters=_dedup(list(ctx.output_filters)),
            input_values=_dedup(input_values),
        )
        for arg in function.concept_arguments:
            self.visit(arg, ctx)
        if wrapper:
            for arg in wrapper.by:
                self.visit(arg, ctx)

    def _record_window(
        self,
        concept: BuildConcept,
        window: BuildNumberingWindowItem | BuildNavigationWindowItem,
        ctx: _Ctx,
        display: str | None = None,
    ) -> None:
        if isinstance(window, BuildNavigationWindowItem):
            content_concepts = [
                a for a in [window.content] if isinstance(a, BuildConcept)
            ]
        else:
            content_concepts = list(window.arguments)
        input_filters = list(ctx.input_filters)
        input_values: list[str] = []
        input_grain: list[str] = []
        self._collect_inputs(content_concepts, input_values, input_grain, input_filters)
        self.records[concept.address] = DerivedValueScope(
            name=display or _short(concept.address),
            kind="window",
            expression=render_scope_expr(window),
            input_filters=_dedup(input_filters),
            input_grain=_dedup(input_grain),
            partition_by=[_short(c.address) for c in window.over],
            order_by=[
                ScopeOrder(
                    expression=render_scope_expr(item.expr),
                    direction=item.order.value,
                )
                for item in window.order_by
            ],
            output_filters=_dedup(list(ctx.output_filters)),
            input_values=_dedup(input_values),
        )
        for arg in content_concepts + list(window.over):
            self.visit(arg, ctx)
        for item in window.order_by:
            for arg in item.concept_arguments:
                self.visit(arg, ctx)

    def _collect_inputs(
        self,
        args: list[BuildConcept],
        input_values: list[str],
        input_grain: list[str],
        input_filters: list[str],
    ) -> None:
        """Classify a computation's direct concept arguments: filtered-argument
        conditions and consumed rowset populations become input filters; consumed
        derived values contribute their names and grain."""
        for arg in args:
            lineage = arg.lineage
            if isinstance(lineage, BuildFilterItem):
                input_filters.extend(
                    render_scope_expr(c) for c in _conjuncts(lineage.where.conditional)
                )
                self._collect_inputs(
                    lineage.content_concept_arguments,
                    input_values,
                    input_grain,
                    input_filters,
                )
            elif arg.derivation == Derivation.AGGREGATE:
                input_values.append(_short(arg.address))
                wrapper = (
                    lineage if isinstance(lineage, BuildAggregateWrapper) else None
                )
                input_grain.extend(g for g in self._group_of(arg, wrapper) if g != "*")
            elif arg.derivation == Derivation.WINDOW:
                input_values.append(_short(arg.address))
                if arg.grain:
                    input_grain.extend(_short(c) for c in sorted(arg.grain.components))
            elif arg.derivation == Derivation.ROWSET and isinstance(
                lineage, BuildRowsetItem
            ):
                if lineage.content.derivation in (
                    Derivation.AGGREGATE,
                    Derivation.WINDOW,
                    Derivation.ROWSET,
                ):
                    input_values.append(_short(arg.address))
                input_filters.extend(self._rowset_filters(lineage.rowset))
                input_grain.extend(
                    _short(c) for c in self._rowset_grain(lineage.rowset)
                )

    def _visit_rowset(
        self, handle: BuildConcept, item: BuildRowsetItem, ctx: _Ctx
    ) -> None:
        """A rowset output computes against the rowset's own population: its
        select's WHERE (not the outer statement's) restricts its inputs, and its
        HAVING restricts rows carrying its results. The inner value is displayed
        under the outer handle's name (`rowset.alias`) rather than its internal
        mangled name."""
        select = item.rowset.select
        input_filters = tuple(self._rowset_filters(item.rowset))
        having = select.having_clause
        output_filters = (
            tuple(render_scope_expr(c) for c in _conjuncts(having.conditional))
            if having
            else ()
        )
        self.visit(
            item.content,
            _Ctx(input_filters, output_filters),
            display=_short(handle.address),
        )

    def _rowset_filters(self, rowset: BuildRowsetLineage) -> list[str]:
        where = rowset.select.where_clause
        if not where:
            return []
        return [render_scope_expr(c) for c in _conjuncts(where.conditional)]

    def _rowset_grain(self, rowset: BuildRowsetLineage) -> list[str]:
        select: SelectLineage | MultiSelectLineage = rowset.select
        return sorted(select.grain.components)

    def _group_of(
        self, concept: BuildConcept, wrapper: BuildAggregateWrapper | None
    ) -> list[str]:
        if wrapper is not None:
            if (
                wrapper.grouping == AggregateGroupingMode.GROUPING_SETS
                and wrapper.grouping_sets
            ):
                sets = ", ".join(
                    "(" + ", ".join(_short(c.address) for c in gs) + ")"
                    for gs in wrapper.grouping_sets
                )
                return [f"grouping sets ({sets})"]
            if wrapper.grouping in (
                AggregateGroupingMode.ROLLUP,
                AggregateGroupingMode.CUBE,
            ):
                hierarchy = ", ".join(_short(c.address) for c in wrapper.by)
                return [f"{wrapper.grouping.value}({hierarchy})"]
            by_keys = [
                _short(c.address) for c in wrapper.by if c.name != ALL_ROWS_CONCEPT
            ]
            return by_keys or ["*"]
        components = sorted(concept.grain.components) if concept.grain else []
        grain_keys = [
            _short(c) for c in components if not c.endswith(f".{ALL_ROWS_CONCEPT}")
        ]
        return grain_keys or ["*"]

    def _finish(self) -> list[DerivedValueScope]:
        """Disambiguate WHERE dual-scope twins: a salted (population-gate) entry
        matching exactly one unsalted entry of the same computation shape is
        paired as `(filter scope)` / `(output scope)`."""
        scopes = list(self.records.values())

        def _signature(scope: DerivedValueScope) -> tuple:
            return (
                scope.kind,
                scope.expression.replace(_WSCOPE_SUFFIX, ""),
                tuple(g.replace(_WSCOPE_SUFFIX, "") for g in scope.group_by),
                tuple(scope.partition_by),
            )

        for scope in scopes:
            if not scope.name.endswith(_WSCOPE_SUFFIX):
                continue
            matches = [
                other
                for other in scopes
                if other is not scope
                and not other.name.endswith(_WSCOPE_SUFFIX)
                and " (filter scope)" not in other.name
                and " (output scope)" not in other.name
                and _signature(other) == _signature(scope)
            ]
            if len(matches) == 1:
                base = matches[0].name
                matches[0].name = f"{base} (output scope)"
                scope.name = f"{base} (filter scope)"
            else:
                trimmed = scope.name[: -len(_WSCOPE_SUFFIX)]
                scope.name = f"{trimmed} (filter scope)"
        # Anonymous virtuals (inline WHERE/HAVING/ORDER BY computations) have
        # no stable authored name — display the expression instead of the
        # minted internal name.
        for scope in scopes:
            if scope.name.startswith(VIRTUAL_CONCEPT_PREFIX):
                suffix = (
                    " (filter scope)" if scope.name.endswith(" (filter scope)") else ""
                )
                scope.name = f"{scope.expression}{suffix}"
        return scopes


def extract_derived_value_scopes(
    statement: Union[BuildSelectLineage, BuildMultiSelectLineage],
) -> list[DerivedValueScope]:
    """Walk a built select's reachable concepts and report every unique
    aggregate/window computation's effective scope. Cycle-safe; deduplicates by
    planned concept address."""
    return _Extractor().extract(statement)


def render_derived_value_scopes(scopes: list[DerivedValueScope]) -> str:
    """Stable line-oriented text block (no ANSI styling). Empty string when
    there is nothing to report."""
    if not scopes:
        return ""
    lines: list[str] = ["Derived value scopes", ""]
    for scope in scopes:
        lines.append(scope.name)
        lines.append(f"  expression: {scope.expression}")
        if scope.input_values:
            lines.append(f"  input values: {', '.join(scope.input_values)}")
        if scope.input_filters:
            lines.append(f"  input filters: {'; '.join(scope.input_filters)}")
        if scope.input_grain:
            lines.append(f"  input grain: {', '.join(scope.input_grain)}")
        if scope.group_by:
            lines.append(f"  group by: {', '.join(scope.group_by)}")
        if scope.partition_by:
            lines.append(f"  partition by: {', '.join(scope.partition_by)}")
        if scope.output_filters:
            lines.append(f"  output filters: {'; '.join(scope.output_filters)}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"
