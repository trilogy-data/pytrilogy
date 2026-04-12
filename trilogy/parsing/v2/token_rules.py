from __future__ import annotations

from trilogy.core.enums import (
    BooleanOperator,
    ComparisonOperator,
    DatePart,
    Modifier,
    Purpose,
    WindowType,
)
from trilogy.core.models.author import Comment
from trilogy.parsing.v2.rules_context import RuleContext, TokenHydrator
from trilogy.parsing.v2.syntax import SyntaxToken, SyntaxTokenKind


def PARSE_COMMENT(token: SyntaxToken, context: RuleContext) -> Comment:
    return Comment(text=token.value.rstrip())


def IDENTIFIER(token: SyntaxToken, context: RuleContext) -> str:
    return token.value


def QUOTED_IDENTIFIER(token: SyntaxToken, context: RuleContext) -> str:
    return token.value[1:-1]


def STRING_CHARS(token: SyntaxToken, context: RuleContext) -> str:
    return token.value


def SINGLE_STRING_CHARS(token: SyntaxToken, context: RuleContext) -> str:
    return token.value


def DOUBLE_STRING_CHARS(token: SyntaxToken, context: RuleContext) -> str:
    return token.value


def MULTILINE_STRING(token: SyntaxToken, context: RuleContext) -> str:
    return token.value[3:-3]


def PURPOSE(token: SyntaxToken, context: RuleContext) -> Purpose:
    return Purpose(token.value)


def AUTO(token: SyntaxToken, context: RuleContext) -> Purpose:
    return Purpose.AUTO


def CONST(token: SyntaxToken, context: RuleContext) -> Purpose:
    return Purpose.CONSTANT


def PROPERTY(token: SyntaxToken, context: RuleContext) -> Purpose:
    return Purpose.PROPERTY


def UNIQUE(token: SyntaxToken, context: RuleContext) -> str:
    return token.value


def COMPARISON_OPERATOR(
    token: SyntaxToken,
    context: RuleContext,
) -> ComparisonOperator:
    return ComparisonOperator(token.value.strip())


def PLUS_OR_MINUS(token: SyntaxToken, context: RuleContext) -> str:
    return token.value


def MULTIPLY_DIVIDE_PERCENT(token: SyntaxToken, context: RuleContext) -> str:
    return token.value


def CONCEPTS(token: SyntaxToken, context: RuleContext) -> str:
    return token.value


def DATASOURCES(token: SyntaxToken, context: RuleContext) -> str:
    return token.value


def LOGICAL_AND(token: SyntaxToken, context: RuleContext) -> BooleanOperator:
    return BooleanOperator.AND


def LOGICAL_OR(token: SyntaxToken, context: RuleContext) -> BooleanOperator:
    return BooleanOperator.OR


def CONDITION_NOT(token: SyntaxToken, context: RuleContext) -> str:
    return token.value


def IMPORT_DOT(token: SyntaxToken, context: RuleContext) -> str:
    return token.value


def PARSE_DATE_PART(token: SyntaxToken, context: RuleContext) -> DatePart:
    return DatePart(token.value)


def PARSE_HASH_TYPE(token: SyntaxToken, context: RuleContext) -> str:
    return token.value


def PARSE_WINDOW_TYPE_LEGACY(token: SyntaxToken, context: RuleContext) -> WindowType:
    return WindowType(token.value.strip())


def PARSE_WINDOW_TYPE_SQL(token: SyntaxToken, context: RuleContext) -> WindowType:
    name = token.value.strip().rstrip("(").strip()
    return WindowType(name)


def PARSE_ORDER_IDENTIFIER(token: SyntaxToken, context: RuleContext) -> str:
    return token.value.strip()


def PARSE_SHORTHAND_MODIFIER(token: SyntaxToken, context: RuleContext) -> Modifier:
    return Modifier(token.value)


def PARSE_WILDCARD_IDENTIFIER(token: SyntaxToken, context: RuleContext) -> str:
    return token.value


def PARSE_DATASOURCE_PARTIAL(token: SyntaxToken, context: RuleContext) -> Modifier:
    return Modifier(token.value)


TOKEN_HYDRATORS: dict[SyntaxTokenKind, TokenHydrator] = {
    SyntaxTokenKind.COMMENT: PARSE_COMMENT,
    SyntaxTokenKind.IDENTIFIER: IDENTIFIER,
    SyntaxTokenKind.QUOTED_IDENTIFIER: QUOTED_IDENTIFIER,
    SyntaxTokenKind.STRING_CHARS: STRING_CHARS,
    SyntaxTokenKind.SINGLE_STRING_CHARS: SINGLE_STRING_CHARS,
    SyntaxTokenKind.DOUBLE_STRING_CHARS: DOUBLE_STRING_CHARS,
    SyntaxTokenKind.MULTILINE_STRING: MULTILINE_STRING,
    SyntaxTokenKind.PURPOSE: PURPOSE,
    SyntaxTokenKind.AUTO: AUTO,
    SyntaxTokenKind.CONSTANT: CONST,
    SyntaxTokenKind.PROPERTY: PROPERTY,
    SyntaxTokenKind.UNIQUE: UNIQUE,
    SyntaxTokenKind.COMPARISON_OPERATOR: COMPARISON_OPERATOR,
    SyntaxTokenKind.PLUS_OR_MINUS: PLUS_OR_MINUS,
    SyntaxTokenKind.MULTIPLY_DIVIDE_PERCENT: MULTIPLY_DIVIDE_PERCENT,
    SyntaxTokenKind.CONCEPTS: CONCEPTS,
    SyntaxTokenKind.DATASOURCES: DATASOURCES,
    SyntaxTokenKind.LOGICAL_AND: LOGICAL_AND,
    SyntaxTokenKind.LOGICAL_OR: LOGICAL_OR,
    SyntaxTokenKind.CONDITION_NOT: CONDITION_NOT,
    SyntaxTokenKind.IMPORT_DOT: IMPORT_DOT,
    SyntaxTokenKind.DATE_PART: PARSE_DATE_PART,
    SyntaxTokenKind.HASH_TYPE: PARSE_HASH_TYPE,
    SyntaxTokenKind.WINDOW_TYPE_LEGACY: PARSE_WINDOW_TYPE_LEGACY,
    SyntaxTokenKind.WINDOW_TYPE_SQL: PARSE_WINDOW_TYPE_SQL,
    SyntaxTokenKind.ORDER_IDENTIFIER: PARSE_ORDER_IDENTIFIER,
    SyntaxTokenKind.SHORTHAND_MODIFIER: PARSE_SHORTHAND_MODIFIER,
    SyntaxTokenKind.WILDCARD_IDENTIFIER: PARSE_WILDCARD_IDENTIFIER,
    SyntaxTokenKind.DATASOURCE_PARTIAL: PARSE_DATASOURCE_PARTIAL,
}
