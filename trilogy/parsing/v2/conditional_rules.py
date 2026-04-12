from __future__ import annotations

from typing import Any

from trilogy.constants import NULL_VALUE
from trilogy.core.enums import ComparisonOperator, FunctionType
from trilogy.core.models.author import (
    Comparison,
    Conditional,
    HavingClause,
    Parenthetical,
    WhereClause,
)
from trilogy.core.models.core import DataType, arg_to_datatype
from trilogy.parsing.v2.rules_context import (
    HydrateFunction,
    NodeHydrator,
    RuleContext,
    hydrated_children,
)
from trilogy.parsing.v2.syntax import SyntaxNode, SyntaxNodeKind


def _expr_to_boolean(root: Any, context: RuleContext) -> Any:
    if not isinstance(root, (Comparison, Conditional)):
        if arg_to_datatype(root) == DataType.BOOL:
            root = Comparison(left=root, right=True, operator=ComparisonOperator.EQ)
        elif arg_to_datatype(root) == DataType.INTEGER:
            root = Comparison(
                left=context.function_factory.create_function(
                    [root], FunctionType.BOOL
                ),
                right=True,
                operator=ComparisonOperator.EQ,
            )
        else:
            root = Comparison(
                left=root, right=NULL_VALUE, operator=ComparisonOperator.IS_NOT
            )
    return root


def conditional(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Any:
    args = hydrated_children(node, hydrate)

    def munch(args: list[Any]) -> Any:
        if len(args) == 1:
            return args[0]
        return Conditional(left=args[0], operator=args[1], right=munch(args[2:]))

    return munch(args)


def condition_parenthetical(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Any:
    args = hydrated_children(node, hydrate)
    if len(args) == 2:
        return Comparison(
            left=Parenthetical(content=args[1]),
            right=False,
            operator=ComparisonOperator.EQ,
        )
    return Parenthetical(content=args[0])


def where_clause(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> WhereClause:
    args = hydrated_children(node, hydrate)
    root = _expr_to_boolean(args[0], context)
    return WhereClause(conditional=root)


def having_clause(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> HavingClause:
    args = hydrated_children(node, hydrate)
    root = args[0]
    if not isinstance(root, (Comparison, Conditional, Parenthetical)):
        from trilogy.constants import MagicConstants

        if arg_to_datatype(root) == DataType.BOOL:
            root = Comparison(left=root, right=True, operator=ComparisonOperator.EQ)
        else:
            root = Comparison(
                left=root,
                right=MagicConstants.NULL,
                operator=ComparisonOperator.IS_NOT,
            )
    return HavingClause(conditional=root)


CONDITIONAL_NODE_HYDRATORS: dict[SyntaxNodeKind, NodeHydrator] = {
    SyntaxNodeKind.CONDITIONAL: conditional,
    SyntaxNodeKind.CONDITION_PARENTHETICAL: condition_parenthetical,
    SyntaxNodeKind.WHERE: where_clause,
    SyntaxNodeKind.HAVING: having_clause,
}
