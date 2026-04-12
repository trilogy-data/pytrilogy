from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, TypeAlias, cast


class SyntaxNodeKind(str, Enum):
    DOCUMENT = "document"
    BLOCK = "block"
    CONCEPT = "concept"
    SHOW_STATEMENT = "show_statement"
    SELECT_STATEMENT = "select_statement"
    SHOW_CATEGORY = "show_category"
    PARAMETER_DEFAULT = "parameter_default"
    PARAMETER_DECLARATION = "parameter_declaration"
    CONCEPT_DECLARATION = "concept_declaration"
    CONCEPT_PROPERTY_DECLARATION = "concept_property_declaration"
    CONCEPT_DERIVATION = "concept_derivation"
    CONSTANT_DERIVATION = "constant_derivation"
    CONCEPT_LITERAL = "concept_literal"
    DATA_TYPE = "data_type"
    CONCEPT_NULLABLE_MODIFIER = "concept_nullable_modifier"
    METADATA = "metadata"
    INT_LITERAL = "int_literal"
    FLOAT_LITERAL = "float_literal"
    BOOL_LITERAL = "bool_literal"
    NULL_LITERAL = "null_literal"
    STRING_LITERAL = "string_literal"
    ARRAY_LITERAL = "array_literal"
    TUPLE_LITERAL = "tuple_literal"
    MAP_LITERAL = "map_literal"
    LITERAL = "literal"
    PRODUCT_OPERATOR = "product_operator"
    SUM_OPERATOR = "sum_operator"
    COMPARISON = "comparison"
    BETWEEN_COMPARISON = "between_comparison"
    PARENTHETICAL = "parenthetical"
    PROPERTY_IDENTIFIER = "property_identifier"
    PROPERTY_IDENTIFIER_WILDCARD = "property_identifier_wildcard"
    COMPARISON_ROOT = "comparison_root"
    SUM_CHAIN = "sum_chain"
    PRODUCT_CHAIN = "product_chain"
    ATOM = "atom"


class SyntaxTokenKind(str, Enum):
    COMMENT = "comment"
    IDENTIFIER = "identifier"
    QUOTED_IDENTIFIER = "quoted_identifier"
    STRING_CHARS = "string_chars"
    SINGLE_STRING_CHARS = "single_string_chars"
    DOUBLE_STRING_CHARS = "double_string_chars"
    MULTILINE_STRING = "multiline_string"
    PURPOSE = "purpose"
    AUTO = "auto"
    CONSTANT = "constant"
    PROPERTY = "property"
    UNIQUE = "unique"
    COMPARISON_OPERATOR = "comparison_operator"
    PLUS_OR_MINUS = "plus_or_minus"
    MULTIPLY_DIVIDE_PERCENT = "multiply_divide_percent"
    CONCEPTS = "concepts"
    DATASOURCES = "datasources"
    LINE_SEPARATOR = "line_separator"
    INT_LITERAL_PART = "int_literal_part"


LARK_NODE_KIND: dict[str, SyntaxNodeKind] = {
    "start": SyntaxNodeKind.DOCUMENT,
    "block": SyntaxNodeKind.BLOCK,
    "concept": SyntaxNodeKind.CONCEPT,
    "show_statement": SyntaxNodeKind.SHOW_STATEMENT,
    "select_statement": SyntaxNodeKind.SELECT_STATEMENT,
    "show_category": SyntaxNodeKind.SHOW_CATEGORY,
    "parameter_default": SyntaxNodeKind.PARAMETER_DEFAULT,
    "parameter_declaration": SyntaxNodeKind.PARAMETER_DECLARATION,
    "concept_declaration": SyntaxNodeKind.CONCEPT_DECLARATION,
    "concept_property_declaration": SyntaxNodeKind.CONCEPT_PROPERTY_DECLARATION,
    "concept_derivation": SyntaxNodeKind.CONCEPT_DERIVATION,
    "constant_derivation": SyntaxNodeKind.CONSTANT_DERIVATION,
    "concept_lit": SyntaxNodeKind.CONCEPT_LITERAL,
    "data_type": SyntaxNodeKind.DATA_TYPE,
    "concept_nullable_modifier": SyntaxNodeKind.CONCEPT_NULLABLE_MODIFIER,
    "metadata": SyntaxNodeKind.METADATA,
    "int_lit": SyntaxNodeKind.INT_LITERAL,
    "float_lit": SyntaxNodeKind.FLOAT_LITERAL,
    "bool_lit": SyntaxNodeKind.BOOL_LITERAL,
    "null_lit": SyntaxNodeKind.NULL_LITERAL,
    "string_lit": SyntaxNodeKind.STRING_LITERAL,
    "array_lit": SyntaxNodeKind.ARRAY_LITERAL,
    "tuple_lit": SyntaxNodeKind.TUPLE_LITERAL,
    "map_lit": SyntaxNodeKind.MAP_LITERAL,
    "literal": SyntaxNodeKind.LITERAL,
    "product_operator": SyntaxNodeKind.PRODUCT_OPERATOR,
    "sum_operator": SyntaxNodeKind.SUM_OPERATOR,
    "comparison": SyntaxNodeKind.COMPARISON,
    "between_comparison": SyntaxNodeKind.BETWEEN_COMPARISON,
    "parenthetical": SyntaxNodeKind.PARENTHETICAL,
    "prop_ident": SyntaxNodeKind.PROPERTY_IDENTIFIER,
    "prop_ident_wildcard": SyntaxNodeKind.PROPERTY_IDENTIFIER_WILDCARD,
    "comparison_root": SyntaxNodeKind.COMPARISON_ROOT,
    "sum_chain": SyntaxNodeKind.SUM_CHAIN,
    "product_chain": SyntaxNodeKind.PRODUCT_CHAIN,
    "atom": SyntaxNodeKind.ATOM,
}


LARK_TOKEN_KIND: dict[str, SyntaxTokenKind] = {
    "PARSE_COMMENT": SyntaxTokenKind.COMMENT,
    "IDENTIFIER": SyntaxTokenKind.IDENTIFIER,
    "QUOTED_IDENTIFIER": SyntaxTokenKind.QUOTED_IDENTIFIER,
    "STRING_CHARS": SyntaxTokenKind.STRING_CHARS,
    "SINGLE_STRING_CHARS": SyntaxTokenKind.SINGLE_STRING_CHARS,
    "DOUBLE_STRING_CHARS": SyntaxTokenKind.DOUBLE_STRING_CHARS,
    "MULTILINE_STRING": SyntaxTokenKind.MULTILINE_STRING,
    "PURPOSE": SyntaxTokenKind.PURPOSE,
    "AUTO": SyntaxTokenKind.AUTO,
    "CONST": SyntaxTokenKind.CONSTANT,
    "PROPERTY": SyntaxTokenKind.PROPERTY,
    "UNIQUE": SyntaxTokenKind.UNIQUE,
    "COMPARISON_OPERATOR": SyntaxTokenKind.COMPARISON_OPERATOR,
    "PLUS_OR_MINUS": SyntaxTokenKind.PLUS_OR_MINUS,
    "MULTIPLY_DIVIDE_PERCENT": SyntaxTokenKind.MULTIPLY_DIVIDE_PERCENT,
    "CONCEPTS": SyntaxTokenKind.CONCEPTS,
    "DATASOURCES": SyntaxTokenKind.DATASOURCES,
    "LINE_SEPARATOR": SyntaxTokenKind.LINE_SEPARATOR,
    "__ANON_17": SyntaxTokenKind.INT_LITERAL_PART,
}


@dataclass(frozen=True)
class SyntaxMeta:
    line: int | None
    column: int | None
    end_line: int | None
    end_column: int | None
    start_pos: int | None
    end_pos: int | None

    @classmethod
    def from_parser_meta(cls, meta: Any | None) -> "SyntaxMeta | None":
        if meta is None:
            return None
        return cls(
            line=getattr(meta, "line", None),
            column=getattr(meta, "column", None),
            end_line=getattr(meta, "end_line", None),
            end_column=getattr(meta, "end_column", None),
            start_pos=getattr(meta, "start_pos", None),
            end_pos=getattr(meta, "end_pos", None),
        )


@dataclass(frozen=True)
class SyntaxToken:
    name: str
    value: str
    meta: SyntaxMeta | None = None
    kind: SyntaxTokenKind | None = None

    @property
    def type(self) -> str:
        return self.name

    def __str__(self) -> str:
        return self.value

    def lower(self) -> str:
        return self.value.lower()

    def capitalize(self) -> str:
        return self.value.capitalize()

    def strip(self) -> str:
        return self.value.strip()

    def split(self, *args: Any, **kwargs: Any) -> list[str]:
        return self.value.split(*args, **kwargs)

    def rsplit(self, *args: Any, **kwargs: Any) -> list[str]:
        return self.value.rsplit(*args, **kwargs)

    def startswith(self, *args: Any, **kwargs: Any) -> bool:
        return self.value.startswith(*args, **kwargs)

    def endswith(self, *args: Any, **kwargs: Any) -> bool:
        return self.value.endswith(*args, **kwargs)


@dataclass(frozen=True)
class SyntaxNode:
    name: str
    children: tuple["SyntaxNode | SyntaxToken", ...]
    meta: SyntaxMeta | None = None
    kind: SyntaxNodeKind | None = None

    def child_nodes(self, kind: SyntaxNodeKind | None = None) -> list["SyntaxNode"]:
        nodes = [child for child in self.children if isinstance(child, SyntaxNode)]
        if kind is None:
            return nodes
        return [child for child in nodes if child.kind == kind]

    def child_tokens(self, kind: SyntaxTokenKind | None = None) -> list["SyntaxToken"]:
        tokens = [child for child in self.children if isinstance(child, SyntaxToken)]
        if kind is None:
            return tokens
        return [child for child in tokens if child.kind == kind]

    def only_child_node(self, kind: SyntaxNodeKind | None = None) -> "SyntaxNode":
        nodes = self.child_nodes(kind)
        if len(nodes) != 1:
            expected = kind.value if kind else "node"
            raise ValueError(
                f"Expected one child {expected} under {syntax_name(self)}, found {len(nodes)}"
            )
        return nodes[0]

    def first_child_node(self, kind: SyntaxNodeKind | None = None) -> "SyntaxNode":
        nodes = self.child_nodes(kind)
        if not nodes:
            expected = kind.value if kind else "node"
            raise ValueError(f"Expected child {expected} under {syntax_name(self)}")
        return nodes[0]


SyntaxElement: TypeAlias = SyntaxNode | SyntaxToken


def syntax_name(element: SyntaxElement) -> str:
    return element.kind.value if element.kind else element.name


@dataclass(frozen=True)
class SyntaxDocument:
    text: str
    tree: SyntaxNode

    @property
    def forms(self) -> list[SyntaxElement]:
        return list(self.tree.children)


def syntax_from_parser(element: Any) -> SyntaxElement:
    data = getattr(element, "data", None)
    token_type = getattr(element, "type", None)
    if data is not None:
        return SyntaxNode(
            name=data,
            children=tuple(syntax_from_parser(child) for child in element.children),
            meta=SyntaxMeta.from_parser_meta(getattr(element, "meta", None)),
            kind=LARK_NODE_KIND.get(data),
        )
    if token_type is not None:
        return SyntaxToken(
            name=token_type,
            value=element.value,
            meta=SyntaxMeta.from_parser_meta(element),
            kind=LARK_TOKEN_KIND.get(token_type),
        )
    msg = f"Unknown syntax element {element!r}"
    raise TypeError(msg)


def syntax_document_from_parser(text: str, tree: Any) -> SyntaxDocument:
    syntax = syntax_from_parser(tree)
    return SyntaxDocument(text=text, tree=cast(SyntaxNode, syntax))
