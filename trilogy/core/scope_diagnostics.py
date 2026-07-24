"""Derived-value scope diagnostics.

Implements the factual scope report of docs/SPEC_query_derived_value_scopes.md:
for every aggregate and window computation a query uses — including
computations reached through rowset membership, expression subqueries, and
named rowset references — describe the effective input population (filters
applied before the value is computed), grouping or partitioning, input grain
for nested computations, and filters applied to rows carrying the
already-computed value.

Each record carries a typed ``role``:

- ``selected_output`` — computed over the rows the statement WHERE admits;
- ``where_gate`` — computed to gate row admission in the WHERE (sees the
  population its own scope defines, NOT the peer WHERE conditions);
- ``upstream`` — computed inside a rowset/subquery this statement consumes.

Extraction runs on the post-normalization ``BuildSelectLineage`` (after HAVING
normalization and WHERE dual-scope splitting), so it reports the planner's
interpretation, not authored syntax. It is observational only: it must never
raise into query processing (callers wrap it) and executes no queries.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Literal

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
    JoinType,
)
from trilogy.core.models.author import (
    AggregateWrapper,
    Between,
    Comparison,
    Concept,
    ConceptArgs,
    ConceptRef,
    Conditional,
    FilterItem,
    Function,
    MultiSelectLineage,
    NavigationWindowItem,
    NumberingWindowItem,
    Parenthetical,
    RowsetItem,
    SelectLineage,
    SubqueryItem,
    SubselectItem,
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
from trilogy.core.models.environment import Environment
from trilogy.core.statements.author import SelectJoin
from trilogy.core.where_scope_normalization import WHERE_SCOPE_SALT

_WSCOPE_SUFFIX = f"_{WHERE_SCOPE_SALT}"

_UNRESTRICTED = "NONE — unrestricted population"

_INFIX_OPERATORS: dict[FunctionType, str] = {
    FunctionType.ADD: "+",
    FunctionType.SUBTRACT: "-",
    FunctionType.MULTIPLY: "*",
    FunctionType.DIVIDE: "/",
}

_CROSS_ROW_DERIVATIONS = frozenset(
    {Derivation.AGGREGATE, Derivation.WINDOW, Derivation.GROUP_TO}
)

Role = Literal["selected_output", "where_gate", "upstream"]


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
    role: Role = "selected_output"
    input_row_filters: list[str] = field(default_factory=list)
    # The planner's post-normalization spelling of `input_row_filters`, reported
    # ONLY when it materially differs — e.g. a scoped-join equivalence group
    # rewrites a WHERE endpoint to the group representative. Keeps the authored
    # predicate authoritative while still showing the effective form.
    normalized_input_row_filters: list[str] = field(default_factory=list)
    # Scoped-join domain declarations in effect for this statement (`union join
    # a = b`). Reported so a rewritten WHERE endpoint reads as a join equivalence,
    # not a vanished/renamed business filter.
    scoped_joins: list[str] = field(default_factory=list)
    # Conditions admitting rows using an already-computed cross-row value
    # (e.g. `total > 1.2 * benchmark` in WHERE). Kept apart from ordinary
    # input row filters so a recursive aggregate gate never reads as a
    # plain source-population restriction.
    admitted_by: list[str] = field(default_factory=list)
    input_grain: list[str] = field(default_factory=list)
    # Identity of the counted argument itself (count/count-distinct only) —
    # NOT the input relation's row grain, which discovery decides and lineage
    # extraction cannot know. Named to keep the two from being conflated.
    argument_grain: list[str] = field(default_factory=list)
    group_by: list[str] = field(default_factory=list)
    partition_by: list[str] = field(default_factory=list)
    order_by: list[ScopeOrder] = field(default_factory=list)
    output_row_filters: list[str] = field(default_factory=list)
    input_values: list[str] = field(default_factory=list)
    # Aggregate only: the author pinned no grain, so this value took the
    # enclosing select's grain. Drives the "inherited select grain" warning;
    # internal — never serialized into `to_dict`.
    grain_inherited: bool = False

    def to_dict(self) -> dict:
        """JSON form. `input_row_filters`/`output_row_filters` are schema-stable
        (serialized even when empty — an empty `input_row_filters` means the
        unrestricted population); genuinely inapplicable fields are omitted.
        `kind`/`order_by` are not serialized: the expression is
        self-describing and ordering is deferred until grain + conditions
        prove out."""
        out: dict = {
            "name": self.name,
            "expression": self.expression,
            "role": self.role,
        }
        if self.input_values:
            out["input_values"] = self.input_values
        out["input_row_filters"] = self.input_row_filters
        if self.normalized_input_row_filters:
            out["normalized_input_row_filters"] = self.normalized_input_row_filters
        if self.scoped_joins:
            out["scoped_joins"] = self.scoped_joins
        if self.admitted_by:
            out["admitted_by"] = self.admitted_by
        if self.input_grain:
            out["input_grain"] = self.input_grain
        if self.argument_grain:
            out["argument_grain"] = self.argument_grain
        if self.kind == "aggregate":
            out["group_by"] = self.group_by
        if self.kind == "window":
            out["partition_by"] = self.partition_by
        out["output_row_filters"] = self.output_row_filters
        return out


def _short(address: str) -> str:
    parts = address.split(".")
    if parts and parts[0] == DEFAULT_NAMESPACE:
        parts = parts[1:]
    # anonymous inline-subquery namespaces are planner bookkeeping, not names
    while len(parts) > 1 and parts[0].startswith("_subquery"):
        parts = parts[1:]
    return ".".join(parts)


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
    if isinstance(expr, (BuildSubselectItem, SubselectItem, SubqueryItem)):
        return f"(select {render_scope_expr(expr.content, sub)})"
    if isinstance(expr, (BuildRowsetItem, RowsetItem)):
        return render_scope_expr(expr.content, sub)
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


_JOIN_LABELS: dict[JoinType, str] = {
    JoinType.INNER: "inner",
    JoinType.LEFT_OUTER: "left",
    JoinType.RIGHT_OUTER: "right",
    JoinType.FULL: "full",
    JoinType.CROSS: "cross",
    JoinType.SUBSET: "subset",
    JoinType.UNION: "union",
}


def _render_scoped_join(join: SelectJoin) -> str:
    """Render a scoped join in its AUTHORED surface form. A subset join is stored
    with operands swapped onto the superset anchor; undo that so the displayed
    equality matches what the author wrote."""
    authored = join.authored or join.join_type
    if authored is JoinType.SUBSET:
        left, right = join.target_address, join.source_address
    else:
        left, right = join.source_address, join.target_address
    label = _JOIN_LABELS.get(authored, authored.value)
    return f"{label} join {_short(left)} = {_short(right)}"


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


def _conjunct_concepts(conjunct: object) -> list[BuildConcept]:
    if isinstance(conjunct, BuildConcept):
        return [conjunct]
    if isinstance(conjunct, BuildConceptArgs):
        return list(conjunct.concept_arguments)
    return []


def _references_cross_row(conjunct: object) -> bool:
    """True when the condition compares a value computed across the statement's
    own rows (aggregate/window/group-to, possibly expression-wrapped). Rowset
    handles are precomputed upstream populations and do not count."""
    stack = _conjunct_concepts(conjunct)
    seen: set[str] = set()
    while stack:
        concept = stack.pop()
        if concept.address in seen:
            continue
        seen.add(concept.address)
        if concept.derivation in _CROSS_ROW_DERIVATIONS:
            return True
        if concept.derivation == Derivation.ROWSET:
            continue
        if isinstance(concept.lineage, BuildConceptArgs):
            stack.extend(concept.lineage.concept_arguments)
    return False


@dataclass(frozen=True)
class _Ctx:
    """Population context for the value being visited: filters restricting rows
    before it computes, computed-value admission gates on those rows, filters
    applied to rows carrying its result, and its computation role."""

    input_row_filters: tuple[str, ...]
    output_row_filters: tuple[str, ...]
    role: Role = "selected_output"
    admitted_by: tuple[str, ...] = ()
    # Normalized (planner) spelling of the input row filters, set only when it
    # diverges from the authored `input_row_filters`; None means no divergence.
    normalized_input_row_filters: tuple[str, ...] | None = None
    # Scoped-join declarations governing this computation's population.
    scoped_joins: tuple[str, ...] = ()


class _Extractor:
    def __init__(
        self,
        environment: Environment | None = None,
        scoped_joins: list[SelectJoin] | None = None,
    ) -> None:
        self.environment = environment
        self.scoped_joins = scoped_joins or []
        self.records: dict[str, DerivedValueScope] = {}
        self.record_names: set[str] = set()
        self.visited: set[str] = set()
        self.expanded_rowsets: set[str] = set()
        self.author_visited: set[str] = set()

    def _resolve_author(self, address: str, select: SelectLineage) -> Concept | None:
        found = select.local_concepts.get(address)
        if found is None and self.environment is not None:
            found = self.environment.concepts.get(address)
        return found

    def extract(
        self, statement: BuildSelectLineage | BuildMultiSelectLineage
    ) -> list[DerivedValueScope]:
        where = statement.where_clause
        having = statement.having_clause
        where_conjuncts = _conjuncts(where.conditional) if where else []
        # Scoped-join key canonicalization rewrites WHERE endpoints to their
        # group representative in the build lineage; report the AUTHORED filter
        # so a restriction never reads as a different business predicate. The
        # build conjuncts still drive cross-row classification (they carry
        # derivation) and pair 1:1 with the authored ones — normalization
        # rewrites endpoints, it never splits or reorders the AND tree.
        authored_where = (
            statement.authored_where_clause
            if isinstance(statement, BuildSelectLineage)
            else None
        )
        authored_conjuncts = (
            _conjuncts(authored_where.conditional)
            if authored_where
            else where_conjuncts
        )
        if len(authored_conjuncts) != len(where_conjuncts):
            authored_conjuncts = where_conjuncts  # can't align — show normalized
        scoped_join_strs = tuple(_render_scoped_join(j) for j in self.scoped_joins)
        # A WHERE conjunct comparing a cross-row computed value admits rows by
        # that value; it is not a plain source restriction of its peers.
        row_filters: list[str] = []
        normalized_row_filters: list[str] = []
        admission_filters: list[str] = []
        for authored_c, build_c in zip(authored_conjuncts, where_conjuncts):
            if _references_cross_row(build_c):
                admission_filters.append(render_scope_expr(authored_c))
            else:
                row_filters.append(render_scope_expr(authored_c))
                normalized_row_filters.append(render_scope_expr(build_c))
        normalized_input = (
            tuple(normalized_row_filters)
            if normalized_row_filters != row_filters
            else None
        )
        having_strs = (
            [render_scope_expr(c) for c in _conjuncts(having.conditional)]
            if having
            else []
        )
        output_addresses = {c.address for c in statement.output_components}
        # WHERE-gate pass first: cross-row values referenced by the WHERE gate
        # rows using the population their own scope defines (dual-scope
        # normalization has already split them onto twin addresses where the
        # scopes diverge). A value that is ALSO a statement output shares one
        # planned computation — leave it to the select pass, where the
        # admission condition surfaces under `admitted_by`.
        for conjunct in where_conjuncts:
            ctx = _Ctx(
                input_row_filters=(),
                output_row_filters=(render_scope_expr(conjunct),),
                role="where_gate",
            )
            for concept in _conjunct_concepts(conjunct):
                if concept.address in output_addresses:
                    continue
                self.visit(concept, ctx)
        # Select scope: outputs, HAVING, ORDER BY all compute over WHERE-admitted
        # rows; HAVING then restricts the rows carrying their results.
        select_ctx = _Ctx(
            input_row_filters=tuple(row_filters),
            output_row_filters=tuple(having_strs),
            role="selected_output",
            admitted_by=tuple(admission_filters),
            normalized_input_row_filters=normalized_input,
            scoped_joins=scoped_join_strs,
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
            # walk INTO the subselect (content/where/order), not just the
            # outer correlation arguments, so its computations get reported
            for arg in lineage.inner_concept_arguments + lineage.outer_arguments:
                self.visit(arg, ctx)
        # BuildMultiSelectLineage handles (union arms) are not walked in v1.

    def _normalized_filters(self, ctx: _Ctx, addition_filters: list[str]) -> list[str]:
        """The record's input filters in planner (normalized) spelling — the
        divergent authored base plus the shared collected additions. Empty when
        the base did not diverge (the authored form already tells the story)."""
        if ctx.normalized_input_row_filters is None:
            return []
        return _dedup(list(ctx.normalized_input_row_filters) + addition_filters)

    def _add_record(self, key: str, scope: DerivedValueScope) -> None:
        # A rowset body expansion and a direct output reference can describe
        # the same computation; first record under a display name wins.
        if scope.name in self.record_names:
            return
        self.records[key] = scope
        self.record_names.add(scope.name)

    def _record_aggregate(
        self,
        concept: BuildConcept,
        function: BuildFunction,
        wrapper: BuildAggregateWrapper | None,
        ctx: _Ctx,
        display: str | None = None,
    ) -> None:
        addition_filters: list[str] = []
        input_values: list[str] = []
        input_grain: list[str] = []
        self._collect_inputs(
            function.concept_arguments, input_values, input_grain, addition_filters
        )
        input_row_filters = list(ctx.input_row_filters) + addition_filters
        # For count/count-distinct the counted identity is load-bearing: report
        # the argument's OWN grain so it can be compared with the row identity
        # the question asks for. This is not the input relation's row grain
        # (discovery decides that), hence the distinct field.
        argument_grain: list[str] = []
        if function.operator in (FunctionType.COUNT, FunctionType.COUNT_DISTINCT):
            for arg in function.concept_arguments:
                sources = (
                    arg.lineage.content_concept_arguments
                    if isinstance(arg.lineage, BuildFilterItem)
                    else [arg]
                )
                for src in sources:
                    if src.grain:
                        argument_grain.extend(
                            _short(c) for c in sorted(src.grain.components)
                        )
        self._add_record(
            concept.address,
            DerivedValueScope(
                name=display or _short(concept.address),
                kind="aggregate",
                expression=render_scope_expr(function),
                role=ctx.role,
                input_row_filters=_dedup(input_row_filters),
                normalized_input_row_filters=self._normalized_filters(
                    ctx, addition_filters
                ),
                scoped_joins=list(ctx.scoped_joins),
                admitted_by=_dedup(list(ctx.admitted_by)),
                input_grain=_dedup(input_grain),
                argument_grain=_dedup(argument_grain),
                group_by=self._group_of(concept, wrapper),
                output_row_filters=_dedup(list(ctx.output_row_filters)),
                input_values=_dedup(input_values),
                grain_inherited=wrapper.grain_inherited if wrapper else False,
            ),
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
        addition_filters: list[str] = []
        input_values: list[str] = []
        input_grain: list[str] = []
        self._collect_inputs(
            content_concepts, input_values, input_grain, addition_filters
        )
        input_row_filters = list(ctx.input_row_filters) + addition_filters
        self._add_record(
            concept.address,
            DerivedValueScope(
                name=display or _short(concept.address),
                kind="window",
                expression=render_scope_expr(window),
                role=ctx.role,
                input_row_filters=_dedup(input_row_filters),
                normalized_input_row_filters=self._normalized_filters(
                    ctx, addition_filters
                ),
                scoped_joins=list(ctx.scoped_joins),
                admitted_by=_dedup(list(ctx.admitted_by)),
                input_grain=_dedup(input_grain),
                partition_by=[_short(c.address) for c in window.over],
                order_by=[
                    ScopeOrder(
                        expression=render_scope_expr(item.expr),
                        direction=item.order.value,
                    )
                    for item in window.order_by
                ],
                output_row_filters=_dedup(list(ctx.output_row_filters)),
                input_values=_dedup(input_values),
            ),
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
        input_row_filters: list[str],
    ) -> None:
        """Classify a computation's direct concept arguments: filtered-argument
        conditions and consumed rowset populations become input row filters; consumed
        derived values contribute their names and grain."""
        for arg in args:
            lineage = arg.lineage
            if isinstance(lineage, BuildFilterItem):
                input_row_filters.extend(
                    render_scope_expr(c) for c in _conjuncts(lineage.where.conditional)
                )
                self._collect_inputs(
                    lineage.content_concept_arguments,
                    input_values,
                    input_grain,
                    input_row_filters,
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
                input_row_filters.extend(self._rowset_filters(lineage.rowset))
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
        mangled name. The rowset body is then expanded so gating computations
        (HAVING/WHERE eligibility aggregates) are reported even when the outer
        statement only consumes a plain key from the rowset."""
        select = item.rowset.select
        input_row_filters = tuple(self._rowset_filters(item.rowset))
        having = select.having_clause
        output_row_filters = (
            tuple(render_scope_expr(c) for c in _conjuncts(having.conditional))
            if having
            else ()
        )
        self.visit(
            item.content,
            _Ctx(input_row_filters, output_row_filters, role="upstream"),
            display=_short(handle.address),
        )
        self._expand_rowset(item.rowset.name, select)

    def _expand_rowset(
        self, name: str, select: SelectLineage | MultiSelectLineage
    ) -> None:
        """Report cross-row computations the rowset's own HAVING/WHERE depend
        on. These gate which rows the outer statement ever sees, so omitting
        them lets a consumer claim scopes validated logic the report never
        showed (q95 eligibility aggregates, q14 tuple qualification). Walks the
        AUTHOR select — the rowset's build lineage is not materialized on the
        outer statement — which is faithful for filters/grain and cheap."""
        if name in self.expanded_rowsets:
            return
        self.expanded_rowsets.add(name)
        if not isinstance(select, SelectLineage):
            return  # union/multiselect arm expansion is deferred
        where = select.where_clause
        having = select.having_clause
        input_row_filters = (
            tuple(render_scope_expr(c) for c in _conjuncts(where.conditional))
            if where
            else ()
        )
        having_strs = (
            tuple(render_scope_expr(c) for c in _conjuncts(having.conditional))
            if having
            else ()
        )
        addresses: list[str] = []
        if having:
            addresses.extend(ref.address for ref in having.concept_arguments)
        if where:
            addresses.extend(ref.address for ref in where.concept_arguments)
        for address in addresses:
            concept = self._resolve_author(address, select)
            if concept is not None:
                self._visit_author(
                    concept, name, select, input_row_filters, having_strs
                )

    def _visit_author(
        self,
        concept: Concept,
        rowset_name: str,
        select: SelectLineage,
        input_row_filters: tuple[str, ...],
        output_row_filters: tuple[str, ...],
    ) -> None:
        key = f"{rowset_name}::{concept.address}"
        if key in self.author_visited:
            return
        self.author_visited.add(key)
        # An anonymous `(select …)` subquery namespace holds an INLINED copy of
        # whatever it references — recording that copy here would stamp it with
        # this consumer's name, filters, and grain. Redirect to the true owner
        # (usually a named rowset output); if none resolves, report nothing
        # rather than something wrong.
        first_segment = concept.address.split(".", 1)[0]
        if first_segment.startswith("_subquery"):
            if self.environment is not None:
                stripped = concept.address.split(".", 1)[1]
                owner = self.environment.concepts.get(stripped)
                if owner is not None and owner.lineage is not None:
                    self._visit_author(
                        owner,
                        rowset_name,
                        select,
                        input_row_filters,
                        output_row_filters,
                    )
            return
        lineage = concept.lineage
        if lineage is None:
            return
        # Boundary crossings: a member of ANOTHER statement computes in that
        # statement's context, never this consumer's.
        if isinstance(lineage, RowsetItem):
            self._visit_author_rowset_member(lineage)
            return
        if isinstance(lineage, SubqueryItem):
            child = self._resolve_author(lineage.content.address, select)
            if child is not None:
                self._visit_author(
                    child, rowset_name, select, input_row_filters, output_row_filters
                )
            return
        # rowset bodies rewrite member names to `_{rowset}_{alias}`; strip the
        # bookkeeping prefix so records read as `rowset.alias`
        mangle = f"_{rowset_name}_"

        def clean(text: str) -> str:
            return text.replace(mangle, "")

        display = f"{rowset_name}.{clean(concept.name)}"
        input_strs = [clean(f) for f in input_row_filters]
        output_strs = [clean(f) for f in output_row_filters]
        if isinstance(lineage, AggregateWrapper):
            self._add_record(
                key,
                DerivedValueScope(
                    name=display,
                    kind="aggregate",
                    expression=clean(render_scope_expr(lineage.function)),
                    role="upstream",
                    input_row_filters=input_strs,
                    group_by=[
                        clean(g) for g in self._author_group(list(lineage.by), select)
                    ],
                    output_row_filters=output_strs,
                ),
            )
        elif isinstance(lineage, Function):
            if lineage.operator in FunctionClass.AGGREGATE_FUNCTIONS.value:
                self._add_record(
                    key,
                    DerivedValueScope(
                        name=display,
                        kind="aggregate",
                        expression=clean(render_scope_expr(lineage)),
                        role="upstream",
                        input_row_filters=input_strs,
                        group_by=[clean(g) for g in self._author_group([], select)],
                        output_row_filters=output_strs,
                    ),
                )
        elif isinstance(lineage, (NumberingWindowItem, NavigationWindowItem)):
            self._add_record(
                key,
                DerivedValueScope(
                    name=display,
                    kind="window",
                    expression=clean(render_scope_expr(lineage)),
                    role="upstream",
                    input_row_filters=input_strs,
                    partition_by=[clean(render_scope_expr(o)) for o in lineage.over],
                    output_row_filters=output_strs,
                ),
            )
        # follow references so expression-wrapped computations are reached
        if isinstance(lineage, ConceptArgs):
            for ref in lineage.concept_arguments:
                child = self._resolve_author(ref.address, select)
                if child is not None:
                    self._visit_author(
                        child,
                        rowset_name,
                        select,
                        input_row_filters,
                        output_row_filters,
                    )

    def _visit_author_rowset_member(self, item: RowsetItem) -> None:
        """Cross a rowset boundary during author traversal: the referenced
        member computes against ITS rowset's population (name, WHERE, HAVING,
        grain) — never the consuming statement's."""
        rowset = item.rowset
        select = rowset.select
        self._expand_rowset(rowset.name, select)
        if not isinstance(select, SelectLineage):
            return
        where = select.where_clause
        having = select.having_clause
        input_row_filters = (
            tuple(render_scope_expr(c) for c in _conjuncts(where.conditional))
            if where
            else ()
        )
        output_row_filters = (
            tuple(render_scope_expr(c) for c in _conjuncts(having.conditional))
            if having
            else ()
        )
        member = self._resolve_author(item.content.address, select)
        if member is not None:
            self._visit_author(
                member, rowset.name, select, input_row_filters, output_row_filters
            )

    def _author_group(self, by: list, select: SelectLineage) -> list[str]:
        keys = [
            render_scope_expr(b)
            for b in by
            if not (
                isinstance(b, (ConceptRef, Concept))
                and b.address.endswith(f".{ALL_ROWS_CONCEPT}")
            )
        ]
        if by:
            return keys or ["*"]
        components = sorted(select.grain.components)
        grain_keys = [
            _short(c) for c in components if not c.endswith(f".{ALL_ROWS_CONCEPT}")
        ]
        return grain_keys or ["*"]

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
        """Name cleanup and role annotation. WHERE dual-scope twins pair by
        computation shape so both entries carry the authored alias; roles are
        then made explicit in the display name — the role is never encoded
        only in the name (the typed field is authoritative)."""
        scopes = list(self.records.values())
        # Anonymous virtuals (inline WHERE/HAVING/ORDER BY computations) have
        # no stable authored name — display the expression instead of the
        # minted internal name.
        for scope in scopes:
            if scope.name.startswith(VIRTUAL_CONCEPT_PREFIX):
                scope.name = scope.expression

        def _signature(scope: DerivedValueScope) -> tuple:
            return (
                scope.kind,
                scope.expression.replace(_WSCOPE_SUFFIX, ""),
                tuple(g.replace(_WSCOPE_SUFFIX, "") for g in scope.group_by),
                tuple(scope.partition_by),
            )

        paired: set[int] = set()
        for gate in [s for s in scopes if s.role == "where_gate"]:
            matches = [
                other
                for other in scopes
                if other.role == "selected_output"
                and _signature(other) == _signature(gate)
            ]
            if len(matches) == 1:
                gate.name = matches[0].name
                paired.add(id(matches[0]))
        for scope in scopes:
            if scope.role == "where_gate":
                scope.name = f"{scope.name} — used by WHERE comparison"
            elif id(scope) in paired:
                scope.name = f"{scope.name} — selected output after row admission"
        return scopes


def extract_derived_value_scopes(
    statement: BuildSelectLineage | BuildMultiSelectLineage,
    environment: Environment | None = None,
    scoped_joins: list[SelectJoin] | None = None,
) -> list[DerivedValueScope]:
    """Walk a built select's reachable concepts — including rowset membership
    and subquery dependencies — and report every unique aggregate/window
    computation's effective scope. Cycle-safe; deduplicates by planned
    computation identity. ``environment`` enables resolving cross-rowset
    references (a rowset whose body consumes another rowset). ``scoped_joins``
    are the statement's authored query-scoped joins, reported so a WHERE
    endpoint the planner rewrote to a join representative reads as a join
    equivalence rather than a changed filter."""
    return _Extractor(environment, scoped_joins).extract(statement)


def render_derived_value_scopes(scopes: list[DerivedValueScope]) -> str:
    """Stable line-oriented text block (no ANSI styling). Empty string when
    there is nothing to report. An unrestricted input population renders
    loudly rather than by omission."""
    if not scopes:
        return ""
    lines: list[str] = ["Derived value scopes", ""]
    for scope in scopes:
        lines.append(scope.name)
        lines.append(f"  expression: {scope.expression}")
        if scope.input_values:
            lines.append(f"  input values: {', '.join(scope.input_values)}")
        if scope.input_row_filters:
            lines.append(f"  input row filters: {'; '.join(scope.input_row_filters)}")
        elif not scope.admitted_by:
            lines.append(f"  input row filters: {_UNRESTRICTED}")
        if scope.normalized_input_row_filters:
            lines.append(
                "  normalized input row filters: "
                f"{'; '.join(scope.normalized_input_row_filters)}"
            )
        if scope.scoped_joins:
            lines.append(f"  scoped joins: {'; '.join(scope.scoped_joins)}")
        if scope.admitted_by:
            lines.append(f"  admitted by: {'; '.join(scope.admitted_by)}")
        if scope.input_grain:
            lines.append(f"  input grain: {', '.join(scope.input_grain)}")
        if scope.argument_grain:
            lines.append(f"  argument grain: {', '.join(scope.argument_grain)}")
        if scope.group_by:
            lines.append(f"  group by: {', '.join(scope.group_by)}")
        if scope.partition_by:
            lines.append(f"  partition by: {', '.join(scope.partition_by)}")
        if scope.output_row_filters:
            lines.append(f"  output row filters: {'; '.join(scope.output_row_filters)}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def derived_value_warnings(scopes: list[DerivedValueScope]) -> list[dict]:
    """Actionable warnings distilled from the factual scope records: computation
    shapes that usually mean a filter or grain was misapplied. Observational
    only — each entry names the value, its computed form, and the fix.

    Two patterns are flagged:

    - a window value in the SELECT with a WHERE clause filtering its inputs —
      WHERE removes rows BEFORE the window computes, so filtering the window
      result needs a HAVING;
    - an aggregate in the WHERE that pinned no grain and so inherited the
      SELECT grain instead of computing a single global value."""
    out: list[dict] = []
    for scope in scopes:
        if (
            scope.kind == "window"
            and scope.role == "selected_output"
            and scope.input_row_filters
        ):
            out.append(
                {
                    "kind": "window_filter_needs_having",
                    "name": scope.name,
                    "expression": scope.expression,
                    "input_row_filters": scope.input_row_filters,
                    "message": (
                        f"WHERE ({'; '.join(scope.input_row_filters)}) removes rows "
                        f"BEFORE the window '{scope.expression}' is computed, so it "
                        "cannot filter on the window result. To keep only rows by "
                        "the post-window value (e.g. a rank cutoff), filter it in a "
                        "HAVING clause instead."
                    ),
                }
            )
        if (
            scope.kind == "aggregate"
            and scope.role == "where_gate"
            and scope.grain_inherited
            and scope.group_by != ["*"]
        ):
            grain = ", ".join(scope.group_by)
            out.append(
                {
                    "kind": "where_aggregate_inherited_grain",
                    "name": scope.name,
                    "expression": scope.expression,
                    "group_by": scope.group_by,
                    "message": (
                        f"Aggregate '{scope.expression}' in the WHERE clause pinned "
                        f"no grain, so it inherited the SELECT grain (group_by: "
                        f"{grain}) rather than computing a single global value. If "
                        "you meant a global comparison, pin it with `by *`."
                    ),
                }
            )
    return out
