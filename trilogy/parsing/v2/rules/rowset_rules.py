from __future__ import annotations

from dataclasses import replace
from typing import Any

from trilogy.constants import DEFAULT_NAMESPACE
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
from trilogy.parsing.v2.rules.tvf_rules import (
    TVF_GROUPING_MODES,
    lower_grouping_select,
    set_op_select_statement,
)
from trilogy.parsing.v2.rules_context import (
    HydrateFunction,
    NodeHydrator,
    RuleContext,
    core_meta,
    fail,
)
from trilogy.parsing.v2.select_finalize import (
    finalize_select_statement,
    finalize_select_tree,
)
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


def lower_statement_grouping(select: SelectStatement, context: RuleContext) -> None:
    """Give a finalized top-level `select ... by rollup|cube|grouping sets` its
    relational form: the select (less ORDER BY and LIMIT) is the arm of a
    rollup TVF read through an anonymous rowset, and `select.relational` is
    the select over that rowset's handles the planner runs. The handles render
    under the authored names, so the statement's surface is unchanged."""
    if select.relational is not None or select.grouping is None:
        return
    arm = replace(select, order_by=None, limit=None)
    # named for its text and place in it: a re-parse reuses the relation's
    # concepts, and two statements never share them
    place = f"{context.source_text}:{select.meta.line_number}:{select.meta.column}"
    name = f"{ROLLUP_NAMESPACE_PREFIX}{string_to_hash(place)}"
    body = lower_grouping_select(arm, context, name)
    if not isinstance(body, RollupSelectStatement):
        return
    rowset = RowsetDerivationStatement(
        name=name,
        select=body,
        namespace=context.environment.namespace or DEFAULT_NAMESPACE,
    )
    handles = _register_rowset(rowset, context, None)
    context.environment.add_rowset(name, body.as_lineage(context.environment))
    replacements = [
        (column.address, handle.reference)
        for column, handle in zip(arm.output_components, handles)
    ]
    outer = SelectStatement(
        selection=[
            SelectItem(
                content=handle.reference,
                modifiers=(
                    [Modifier.HIDDEN] if column.address in arm.hidden_components else []
                ),
            )
            for column, handle in zip(arm.output_components, handles)
        ],
        order_by=(
            OrderBy(
                items=[
                    item.with_reference_replacement(replacements)
                    for item in select.order_by.items
                ]
            )
            if select.order_by
            else None
        ),
        limit=select.limit,
        meta=select.meta,
    )
    finalize_select_statement(outer, context)
    select.relational = outer


def tvf_select_statement(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> SelectStatement:
    """Inline `from <tvf>(...) -> (...) select ...`: the trailing select, with
    the TVF's outputs bound under their signature names."""
    call = next(
        (c for c in node.child_nodes() if c.kind in TVF_GROUPING_MODES),
        None,
    )
    if call is None:
        return set_op_select_statement(node, context, hydrate)
    select_node = next(
        c for c in node.child_nodes() if c.kind == SyntaxNodeKind.SELECT_STATEMENT
    )
    meta = call.meta
    place = (
        f"{context.source_text}:{getattr(meta, 'line', 0)}:"
        f"{getattr(meta, 'column', 0)}"
    )
    name = f"{ROLLUP_NAMESPACE_PREFIX}{string_to_hash(place)}"
    with context.semantic_state.rowset_alias_scope(name):
        arm = hydrate(call)
    if not arm.signature:
        raise fail(
            call,
            "an inline grouping call names its outputs: "
            "`from rollup((select ...), keys) -> (col1, col2) select ...`. "
            "Or name the relation: `with r as rollup((select ...), keys);`.",
        )
    finalize_select_tree(arm, context)
    body = lower_grouping_select(arm, context, name, core_meta(node.meta))
    namespace = context.environment.namespace or DEFAULT_NAMESPACE
    rowset = RowsetDerivationStatement(name=name, select=body, namespace=namespace)
    handles = _register_rowset(rowset, context, core_meta(node.meta))
    context.environment.add_rowset(name, body.as_lineage(context.environment))
    # a signature name means its handle inside the trailing select only, so it
    # may repeat a model concept's name or another statement's
    assert isinstance(body, RollupSelectStatement)
    named = [h for item, h in zip(body.align.items, handles) if not item.hidden]
    bindings = {out.name: h.address for out, h in zip(arm.signature, named)}
    with context.semantic_state.name_bindings(bindings):
        select: SelectStatement = hydrate(select_node)
    select.source = rowset
    select.labels = {
        h.address: ConceptRef(address=f"{namespace}.{out.name}", datatype=h.datatype)
        for out, h in zip(arm.signature, named)
    }
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
}
