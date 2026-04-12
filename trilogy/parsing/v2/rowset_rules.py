from __future__ import annotations

from trilogy.constants import DEFAULT_NAMESPACE
from trilogy.core.statements.author import RowsetDerivationStatement
from trilogy.parsing.v2.rowset_semantics import rowset_to_concepts_v2
from trilogy.parsing.v2.rules_context import (
    HydrateFunction,
    NodeHydrator,
    RuleContext,
    core_meta,
    hydrated_children,
)
from trilogy.parsing.v2.select_finalize import finalize_select_tree
from trilogy.parsing.v2.syntax import SyntaxNode, SyntaxNodeKind


def rowset_derivation_statement(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> RowsetDerivationStatement:
    args = hydrated_children(node, hydrate)
    name = str(args[0])
    select = args[1]
    # rowset_to_concepts_v2 relies on as_lineage, which needs the inner
    # select(s) finalized before lineage conversion.
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


ROWSET_NODE_HYDRATORS: dict[SyntaxNodeKind, NodeHydrator] = {
    SyntaxNodeKind.ROWSET_DERIVATION_STATEMENT: rowset_derivation_statement,
}
