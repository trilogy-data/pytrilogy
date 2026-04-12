from __future__ import annotations

from trilogy.constants import DEFAULT_NAMESPACE
from trilogy.core.statements.author import RowsetDerivationStatement
from trilogy.parsing.common import rowset_to_concepts
from trilogy.parsing.v2.rules_context import (
    HydrateFunction,
    NodeHydrator,
    RuleContext,
    core_meta,
    hydrated_children,
)
from trilogy.parsing.v2.statement_plans import finalize_select_tree
from trilogy.parsing.v2.syntax import SyntaxNode, SyntaxNodeKind


def rowset_derivation_statement(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> RowsetDerivationStatement:
    args = hydrated_children(node, hydrate)
    name = str(args[0])
    select = args[1]
    # rowset_to_concepts relies on as_lineage, which needs the inner
    # select(s) finalized before lineage conversion.
    finalize_select_tree(select, context.environment)
    output = RowsetDerivationStatement(
        name=name,
        select=select,
        namespace=context.environment.namespace or DEFAULT_NAMESPACE,
    )
    for new_concept in rowset_to_concepts(output, context.environment):
        context.add_concept(new_concept, meta=core_meta(node.meta), force=True)
    return output


ROWSET_NODE_HYDRATORS: dict[SyntaxNodeKind, NodeHydrator] = {
    SyntaxNodeKind.ROWSET_DERIVATION_STATEMENT: rowset_derivation_statement,
}
