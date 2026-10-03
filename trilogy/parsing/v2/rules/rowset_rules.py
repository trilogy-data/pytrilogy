from __future__ import annotations

from trilogy.constants import DEFAULT_NAMESPACE
from trilogy.core.constants import SUBQUERY_NAMESPACE_PREFIX
from trilogy.core.enums import BooleanOperator, ComparisonOperator, Purpose
from trilogy.core.functions import LITERAL_CONSTANT_TYPES
from trilogy.core.models.author import (
    Comparison,
    ConceptRef,
    Conditional,
    Parenthetical,
    SubqueryItem,
)
from trilogy.core.models.environment import Environment
from trilogy.core.statements.author import RowsetDerivationStatement, SelectStatement
from trilogy.parsing.v2.rowset_semantics import rowset_to_concepts_v2
from trilogy.parsing.v2.rules_context import (
    HydrateFunction,
    NodeHydrator,
    RuleContext,
    core_meta,
    fail,
)
from trilogy.parsing.v2.select_finalize import finalize_select_tree
from trilogy.parsing.v2.syntax import SyntaxNode, SyntaxNodeKind


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
    output = RowsetDerivationStatement(
        name=name,
        select=select,
        namespace=context.environment.namespace or DEFAULT_NAMESPACE,
    )
    result = rowset_to_concepts_v2(output, context)
    for new_concept in result.concepts:
        context.add_rowset_concept(new_concept, meta=core_meta(node.meta), force=True)
    if result.alias_updates:
        context.semantic_state.stage_rowset_aliases(result.alias_updates)
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
    output = RowsetDerivationStatement(
        name=name,
        select=select,
        namespace=context.environment.namespace or DEFAULT_NAMESPACE,
        scalar=scalar,
    )
    result = rowset_to_concepts_v2(output, context)
    for new_concept in result.concepts:
        context.add_rowset_concept(new_concept, meta=core_meta(node.meta), force=True)
    if result.alias_updates:
        context.semantic_state.stage_rowset_aliases(result.alias_updates)
    context.environment.add_rowset(name, select.as_lineage(context.environment))
    return SubqueryItem(
        content=result.concepts[0].reference,
        select=select,
        name=name,
        contents=[c.reference for c in result.concepts],
    )


def _one_row(select: SelectStatement, environment: Environment) -> bool:
    """No grain, `limit 1`, or every grain component pinned to a literal or
    constant by an `=` in the WHERE, itself or through its keys
    (`where cat_avg.category = 'a'`)."""
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
            if isinstance(side, ConceptRef) and _fixed(other, environment)
        }
    return set()


def _fixed(value: object, environment: Environment) -> bool:
    if isinstance(value, ConceptRef):
        return environment.concepts[value.address].purpose == Purpose.CONSTANT
    return isinstance(value, LITERAL_CONSTANT_TYPES)


ROWSET_NODE_HYDRATORS: dict[SyntaxNodeKind, NodeHydrator] = {
    SyntaxNodeKind.ROWSET_DERIVATION_STATEMENT: rowset_derivation_statement,
    SyntaxNodeKind.SCALAR_SUBQUERY: scalar_subquery,
}
