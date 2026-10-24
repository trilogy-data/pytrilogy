from __future__ import annotations

from dataclasses import replace as dc_replace
from typing import Any

from trilogy.constants import NULL_VALUE
from trilogy.core.enums import BooleanOperator, ComparisonOperator, FunctionType
from trilogy.core.models.author import (
    Between,
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


def negate_two_valued(operand: Any, context: RuleContext) -> Comparison | Conditional:
    """`not operand`, where an UNKNOWN operand negates to TRUE. Membership is
    total, so its negation is the complementary membership, which keeps the
    set an existence input rather than a value read inside `coalesce`; De
    Morgan carries the negation down to it (exact for this `not`,
    formal/Trilogy/Logic.lean `not_and`, `not_or`)."""
    inner = operand
    while isinstance(inner, Parenthetical):
        inner = inner.content
    if isinstance(inner, Conditional):
        return Conditional(
            left=negate_two_valued(inner.left, context),
            operator=(
                BooleanOperator.OR
                if inner.operator == BooleanOperator.AND
                else BooleanOperator.AND
            ),
            right=negate_two_valued(inner.right, context),
        )
    if isinstance(inner, Comparison) and inner.operator == ComparisonOperator.IN:
        return dc_replace(inner, operator=ComparisonOperator.NOT_IN)
    if isinstance(inner, Comparison) and inner.operator == ComparisonOperator.NOT_IN:
        return dc_replace(inner, operator=ComparisonOperator.IN)
    return Comparison(
        left=context.function_factory.create_function(
            [operand, False], FunctionType.COALESCE
        ),
        right=False,
        operator=ComparisonOperator.EQ,
    )


def _expr_to_boolean(root: Any, context: RuleContext) -> Any:
    if not isinstance(root, (Comparison, Conditional, Between)):
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


def _operand_to_boolean(operand: Any, context: RuleContext) -> Any:
    """A bare `active` beside `and`/`or` becomes `active = true`, as a lone
    `where active` does, so conjunct decomposition sees every operand as a
    condition instead of refusing to split the conjunction."""
    if isinstance(operand, Parenthetical):
        return operand
    return _expr_to_boolean(operand, context)


def conditional(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Any:
    args = hydrated_children(node, hydrate)

    def munch(args: list[Any]) -> Any:
        if len(args) == 1:
            return args[0]
        return Conditional(
            left=_operand_to_boolean(args[0], context),
            operator=args[1],
            right=_operand_to_boolean(munch(args[2:]), context),
        )

    return munch(args)


def condition_parenthetical(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Any:
    args = hydrated_children(node, hydrate)
    if len(args) == 2:
        # Two-valued like `fnot`: an UNKNOWN operand negates to TRUE, so
        # `not (a or b)` agrees with `not a and not b`.
        return negate_two_valued(Parenthetical(content=args[1]), context)
    return Parenthetical(content=args[0])


def where_clause(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> WhereClause:
    args = hydrated_children(node, hydrate)
    root = _expr_to_boolean(args[0], context)
    return WhereClause(conditional=root)


class StagedWhere:
    """Carrier for a `where ... then where ...` chain, one WhereClause per stage."""

    def __init__(self, stages: list[WhereClause]):
        self.stages = stages


def where_series(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> StagedWhere:
    stages = hydrated_children(node, hydrate)
    return StagedWhere(stages=[s for s in stages if isinstance(s, WhereClause)])


def having_clause(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> HavingClause:
    args = hydrated_children(node, hydrate)
    root = args[0]
    if not isinstance(root, (Comparison, Conditional, Parenthetical, Between)):
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
    SyntaxNodeKind.WHERE_SERIES: where_series,
    SyntaxNodeKind.HAVING: having_clause,
}
