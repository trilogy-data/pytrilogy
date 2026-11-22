from __future__ import annotations

from dataclasses import replace
from typing import Any

from trilogy.constants import DEFAULT_NAMESPACE, MAGIC_TVF_UNION_NAME
from trilogy.core.constants import ROLLUP_NAMESPACE_PREFIX, SUBQUERY_NAMESPACE_PREFIX
from trilogy.core.enums import BooleanOperator, ComparisonOperator, Modifier, Purpose
from trilogy.core.functions import LITERAL_CONSTANT_TYPES
from trilogy.core.models.author import (
    Comparison,
    Concept,
    ConceptRef,
    Conditional,
    OrderBy,
    Parenthetical,
    RowsetItem,
    SubqueryItem,
)
from trilogy.core.models.environment import Environment
from trilogy.core.statements.author import (
    RollupSelectStatement,
    RowsetDerivationStatement,
    SelectItem,
    SelectStatement,
)
from trilogy.parsing.v2.rowset_semantics import rowset_to_concepts_v2
from trilogy.parsing.v2.rules.concept_rules import metadata_from_meta
from trilogy.parsing.v2.rules.tvf_rules import (
    TVF_GROUPING_MODES,
    TVF_INVOCATION_OPERATORS,
    bind_set_op,
    lower_grouping_select,
)
from trilogy.parsing.v2.rules_context import (
    HydrateFunction,
    NodeHydrator,
    RuleContext,
    core_meta,
    fail,
)
from trilogy.parsing.v2.select_finalize import finalize_select_tree
from trilogy.parsing.v2.syntax import SyntaxNode, SyntaxNodeKind
from trilogy.utility import string_to_hash


def rowset_derivation_statement(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> RowsetDerivationStatement:
    children = list(node.children)
    # Hydrate the rowset name first so the scope is active before the
    # SELECT body is hydrated: SELECT aliases declared inside the body
    # must be namespaced to this rowset (see SemanticState.rowset_alias_scope).
    name = str(hydrate(children[0]))
    with context.semantic_state.rowset_alias_scope(name):
        rest = [hydrate(child) for child in children[1:]]
    select = rest[0]
    # rowset_to_concepts_v2 relies on as_lineage, which needs the inner
    # select(s) finalized before lineage conversion. finalize runs outside
    # the scope intentionally — it operates on already-resolved addresses.
    finalize_select_tree(select, context)
    if isinstance(select, SelectStatement):
        select = lower_grouping_select(select, context, name, core_meta(node.meta))
    output = RowsetDerivationStatement(
        name=name,
        select=select,
        namespace=context.environment.namespace or DEFAULT_NAMESPACE,
    )
    _register_rowset(output, context, core_meta(node.meta))
    return output


def scalar_subquery(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> SubqueryItem:
    """Desugar an inline ``(select ...)`` into an anonymous single-output rowset.

    Mirrors ``rowset_derivation_statement`` but mints the name itself and
    validates a single output. Returns a ``SubqueryItem`` referencing that one
    output; the enclosing expression treats it as a scalar (grain-less bodies
    cross-join). The synthetic rowset is never rendered — the ``select`` payload
    reproduces the inline form.
    """
    select_node = next(
        (
            c
            for c in node.child_nodes()
            if c.kind
            in (
                SyntaxNodeKind.SELECT_STATEMENT,
                SyntaxNodeKind.MULTI_SELECT_STATEMENT,
            )
        ),
        None,
    )
    if select_node is None:
        raise fail(node, "subquery requires a `(select ...)` body")
    if select_node.kind is SyntaxNodeKind.MULTI_SELECT_STATEMENT:
        raise fail(node, "a `(select ...)` subquery cannot be a multi-select (merge)")
    meta = node.meta
    name = (
        f"{SUBQUERY_NAMESPACE_PREFIX}"
        f"{getattr(meta, 'line', 0)}_{getattr(meta, 'column', 0)}"
    )
    with context.semantic_state.rowset_alias_scope(name):
        select = hydrate(select_node)
    finalize_select_tree(select, context)
    if not isinstance(select, SelectStatement):
        raise fail(node, "subquery requires a `(select ...)` body")
    # A single-column projection is required in scalar contexts; a multi-column
    # projection is only admissible as the RHS of a tuple `in`/`not in`, where
    # it lowers to a row-wise membership set (see subselect_comparison).
    if (
        len(select.output_components) != 1
        and not context.semantic_state.in_membership_subquery
    ):
        raise fail(
            node,
            "a `(select ...)` subquery used as a scalar value or membership set "
            "must select exactly one column; project only the key/value consumed "
            "by the outer expression",
        )
    scalar = not context.semantic_state.in_membership_subquery
    # a scalar is joined beside every row, so a body of several rows would
    # repeat them; SQL raises at run time, this says so at parse time
    if scalar and not _one_row(select, context.environment):
        raise fail(
            node,
            "a `(select ...)` used as a scalar value must return one row: "
            "aggregate it with no `by` (`(select max(v))`) or add `limit 1`",
        )
    body = lower_grouping_select(select, context, name, core_meta(node.meta))
    output = RowsetDerivationStatement(
        name=name,
        select=body,
        namespace=context.environment.namespace or DEFAULT_NAMESPACE,
        scalar=scalar,
    )
    concepts = _register_rowset(output, context, core_meta(node.meta))
    context.environment.add_rowset(name, body.as_lineage(context.environment))
    return SubqueryItem(
        content=concepts[0].reference,
        select=select,
        name=name,
        contents=[c.reference for c in concepts],
    )


def _register_rowset(
    output: RowsetDerivationStatement, context: RuleContext, meta: Any
) -> list[Concept]:
    result = rowset_to_concepts_v2(output, context)
    for new_concept in result.concepts:
        context.add_rowset_concept(new_concept, meta=meta, force=True)
    if result.alias_updates:
        context.semantic_state.stage_rowset_aliases(result.alias_updates)
    return result.concepts


def _is_call(node: SyntaxNode) -> bool:
    return node.kind in TVF_GROUPING_MODES or node.kind in TVF_INVOCATION_OPERATORS


def _call_node(node: SyntaxNode) -> SyntaxNode:
    return next(c for c in node.child_nodes() if _is_call(c))


def _call_rowset_name(call: SyntaxNode, context: RuleContext) -> str:
    """Named for the call's text and place in it: a re-parse reuses the
    relation's concepts, and two calls never share them."""
    place = (
        f"{context.source_text}:{getattr(call.meta, 'line', 0)}:"
        f"{getattr(call.meta, 'column', 0)}"
    )
    return f"{ROLLUP_NAMESPACE_PREFIX}{string_to_hash(place)}"


def _lower_call(
    arm: SelectStatement, name: str, context: RuleContext, meta: Any
) -> tuple[RowsetDerivationStatement, list[Concept]]:
    """A finalized grouping arm as an anonymous rowset over its rollup TVF,
    with that rowset's handles, one per arm column."""
    body = lower_grouping_select(arm, context, name, meta)
    assert isinstance(body, RollupSelectStatement)
    rowset = RowsetDerivationStatement(
        name=name,
        select=body,
        namespace=context.environment.namespace or DEFAULT_NAMESPACE,
    )
    handles = _register_rowset(rowset, context, meta)
    context.environment.add_rowset(name, body.as_lineage(context.environment))
    return rowset, handles


def _bind_grouping_call(
    call: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
    meta: Any,
) -> tuple[RowsetDerivationStatement, dict[str, Concept]]:
    """Lower a signed grouping call read in place to an anonymous rowset.
    Returns it with each signature name's handle. The handles live in the
    rowset's own namespace, so a signature name may repeat a model concept's
    name or another statement's."""
    name = _call_rowset_name(call, context)
    with context.semantic_state.rowset_alias_scope(name):
        arm: SelectStatement = hydrate(call)
    finalize_select_tree(arm, context)
    rowset, handles = _lower_call(arm, name, context, meta)
    body = rowset.select
    assert isinstance(body, RollupSelectStatement)
    shown = [h for item, h in zip(body.align.items, handles) if not item.hidden]
    return rowset, {out.name: h for out, h in zip(arm.signature, shown)}


def _label(select: SelectStatement, named: dict[str, Concept], namespace: str) -> None:
    """Show each handle the select reads under its signature name."""
    select.labels = {
        handle.address: ConceptRef(
            address=f"{namespace}.{name}", datatype=handle.datatype
        )
        for name, handle in named.items()
    }


def tvf_select_statement(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> SelectStatement:
    """Inline `from <tvf>(...) -> (...) select ...`: the trailing select, with
    the TVF's outputs bound under their signature names."""
    call = _call_node(node)
    select_node = next(
        c for c in node.child_nodes() if c.kind == SyntaxNodeKind.SELECT_STATEMENT
    )
    meta = core_meta(node.meta)
    if call.kind in TVF_INVOCATION_OPERATORS:
        union, _ = bind_set_op(call, context, hydrate, meta)
        select: SelectStatement = hydrate(select_node)
        select.source = RowsetDerivationStatement(
            name=MAGIC_TVF_UNION_NAME, select=union, namespace=union.namespace
        )
        return select
    if not call.child_nodes(SyntaxNodeKind.TVF_OUTPUT):
        raise fail(
            call,
            "an inline grouping call names its outputs: "
            "`from rollup((select ...), keys) -> (col1, col2) select ...`. "
            "To show every column as the select names it, write "
            "`select *rollup((select ...), keys);`.",
        )
    rowset, named = _bind_grouping_call(call, context, hydrate, meta)
    bindings = {name: handle.address for name, handle in named.items()}
    with context.semantic_state.name_bindings(bindings):
        select = hydrate(select_node)
    select.source = rowset
    _label(select, named, rowset.namespace)
    return select


def _spread_items(outputs: list[Concept], signature: list[Any]) -> list[SelectItem]:
    return [
        SelectItem(
            content=concept.reference,
            modifiers=[Modifier.HIDDEN] if out.hidden else [],
        )
        for concept, out in zip(outputs, signature)
    ]


def _clause(node: SyntaxNode, kind: SyntaxNodeKind, hydrate: HydrateFunction) -> Any:
    found = node.child_nodes(kind)
    return hydrate(found[0]) if found else None


def _spread_arm(
    node: SyntaxNode, call: SyntaxNode, hydrate: HydrateFunction
) -> SelectStatement:
    """The select an unsigned grouping call wraps, with the clauses written
    after the call: they read its columns by the names it gives them."""
    arm: SelectStatement = hydrate(call)
    having = _clause(node, SyntaxNodeKind.HAVING, hydrate)
    order_by = _clause(node, SyntaxNodeKind.ORDER_BY, hydrate)
    limit = _clause(node, SyntaxNodeKind.LIMIT, hydrate)
    if node.child_nodes(SyntaxNodeKind.WHERE_SERIES):
        raise fail(
            node,
            "after `select *rollup(...)`, filter the rolled-up rows with `having`; "
            "a `where` on the input rows goes inside the wrapped select.",
        )
    twice = [
        name
        for name, outer, inner in (
            ("having", having, arm.having_clause),
            ("order by", order_by, arm.order_by),
            ("limit", limit, arm.limit),
        )
        if outer is not None and inner is not None
    ]
    if twice:
        raise fail(
            node,
            f"`{twice[0]}` is written once: after the call, not also inside the "
            "select it wraps.",
        )
    arm.having_clause = having or arm.having_clause
    arm.order_by = order_by or arm.order_by
    arm.limit = limit.count if limit else arm.limit
    return arm


def _spread_grouping(
    node: SyntaxNode,
    call: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
    meta: Any,
) -> SelectStatement:
    """`select *rollup((select ...), keys) ...` with no signature: the select
    over the call's handles, each shown under its arm column's name. A HAVING
    is the arm's; an ORDER BY is finalized there too, so an expression it
    sorts on is an arm column, then read through its handle."""
    arm = _spread_arm(node, call, hydrate)
    finalize_select_tree(arm, context)
    if not arm.grain.components:
        # one standard group: the wrapped select is the statement (the
        # statement plan finalizing it again is a no-op)
        return arm
    rowset, handles = _lower_call(
        replace(arm, order_by=None, limit=None),
        _call_rowset_name(call, context),
        context,
        meta,
    )
    columns = arm.output_components
    order_by = arm.order_by
    if order_by is not None:
        replacements = [
            (column.address, handle.reference)
            for column, handle in zip(columns, handles)
        ]
        order_by = OrderBy(
            items=[
                item.with_reference_replacement(replacements) for item in order_by.items
            ]
        )
    return SelectStatement(
        selection=[
            SelectItem(
                content=handle.reference,
                modifiers=(
                    [Modifier.HIDDEN] if column.address in arm.hidden_components else []
                ),
            )
            for column, handle in zip(columns, handles)
        ],
        order_by=order_by,
        limit=arm.limit,
        meta=arm.meta,
        source=rowset,
        labels={handle.address: column for column, handle in zip(columns, handles)},
        spread=True,
    )


def tvf_statement(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> SelectStatement:
    """`select *<tvf call> [where] [having] [order by] [limit]`: every output
    of the call, under the names the call gives them."""
    spreads = node.child_nodes(SyntaxNodeKind.TVF_SPREAD)
    if len(spreads) > 1:
        raise fail(
            node,
            "one `*<call>` per select for now: name each relation "
            "(`with a as rollup(...);`) and join the names in a select.",
        )
    call = _call_node(spreads[0])
    meta = core_meta(node.meta)
    named: dict[str, Concept] = {}
    if call.kind in TVF_INVOCATION_OPERATORS:
        union, outputs = bind_set_op(call, context, hydrate, meta)
        source = RowsetDerivationStatement(
            name=MAGIC_TVF_UNION_NAME, select=union, namespace=union.namespace
        )
        signature = union.outputs
    elif call.child_nodes(SyntaxNodeKind.TVF_OUTPUT):
        source, named = _bind_grouping_call(call, context, hydrate, meta)
        outputs = list(named.values())
        signature = source.select.selects[0].signature  # type: ignore[union-attr]
    else:
        return _spread_grouping(node, call, context, hydrate, meta)
    bindings = {name: handle.address for name, handle in named.items()}
    with context.semantic_state.name_bindings(bindings):
        where = _clause(node, SyntaxNodeKind.WHERE_SERIES, hydrate)
        having = _clause(node, SyntaxNodeKind.HAVING, hydrate)
        order_by = _clause(node, SyntaxNodeKind.ORDER_BY, hydrate)
    limit = _clause(node, SyntaxNodeKind.LIMIT, hydrate)
    select = SelectStatement(
        selection=_spread_items(outputs, signature),
        where_clauses=where.stages if where else [],
        having_clause=having,
        order_by=order_by,
        limit=limit.count if limit else None,
        meta=metadata_from_meta(node.meta),
        source=source,
        spread=True,
    )
    _label(select, named, source.namespace)
    return select


def _one_row(select: SelectStatement, environment: Environment) -> bool:
    """No grain, `limit 1`, or every grain component pinned by an `=` in the
    WHERE, itself or through its keys: to a literal or constant
    (`where cat_avg.category = 'a'`), or to another scope's column, a
    correlation the planner refuses with its own message."""
    if select.limit == 1:
        return True
    pinned = (
        _equated(select.where_clause.conditional, environment)
        if select.where_clause
        else set()
    )
    return all(
        _pinned(address, pinned, environment) for address in select.grain.components
    )


def _pinned(address: str, pinned: set[str], environment: Environment) -> bool:
    if address in pinned:
        return True
    keys = environment.concepts[address].keys or set()
    return bool(keys) and keys <= pinned


def _equated(condition: object, environment: Environment) -> set[str]:
    if isinstance(condition, Parenthetical):
        return _equated(condition.content, environment)
    if isinstance(condition, Conditional) and condition.operator == BooleanOperator.AND:
        return _equated(condition.left, environment) | _equated(
            condition.right, environment
        )
    if (
        isinstance(condition, Comparison)
        and condition.operator == ComparisonOperator.EQ
    ):
        left, right = condition.left, condition.right
        return {
            side.address
            for side, other in ((left, right), (right, left))
            if isinstance(side, ConceptRef) and _fixed(side, other, environment)
        }
    return set()


def _fixed(side: ConceptRef, value: object, environment: Environment) -> bool:
    if isinstance(value, ConceptRef):
        other = environment.concepts[value.address]
        return other.purpose == Purpose.CONSTANT or _rowset_of(
            other.address, environment
        ) != _rowset_of(side.address, environment)
    return isinstance(value, LITERAL_CONSTANT_TYPES)


def _rowset_of(address: str, environment: Environment) -> str | None:
    lineage = environment.concepts[address].lineage
    return lineage.rowset.name if isinstance(lineage, RowsetItem) else None


ROWSET_NODE_HYDRATORS: dict[SyntaxNodeKind, NodeHydrator] = {
    SyntaxNodeKind.ROWSET_DERIVATION_STATEMENT: rowset_derivation_statement,
    SyntaxNodeKind.SCALAR_SUBQUERY: scalar_subquery,
    SyntaxNodeKind.TVF_SELECT_STATEMENT: tvf_select_statement,
    SyntaxNodeKind.TVF_STATEMENT: tvf_statement,
}
