from __future__ import annotations

from trilogy.core.statements.author import RawSQLStatement
from trilogy.parsing.v2.rules_context import (
    HydrateFunction,
    NodeHydrator,
    RuleContext,
    hydrated_children,
)
from trilogy.parsing.v2.syntax import SyntaxNode, SyntaxNodeKind


def rawsql_statement(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> RawSQLStatement:
    args = hydrated_children(node, hydrate)
    return RawSQLStatement(text=str(args[0]))


RAWSQL_NODE_HYDRATORS: dict[SyntaxNodeKind, NodeHydrator] = {
    SyntaxNodeKind.RAWSQL_STATEMENT: rawsql_statement,
}
