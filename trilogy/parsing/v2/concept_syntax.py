from __future__ import annotations

from dataclasses import dataclass

from trilogy.parsing.v2.model import HydrationDiagnostic, HydrationError
from trilogy.parsing.v2.syntax import (
    SyntaxElement,
    SyntaxNode,
    SyntaxNodeKind,
    SyntaxToken,
    SyntaxTokenKind,
    syntax_name,
)


def syntax_error(syntax: SyntaxElement, message: str) -> HydrationError:
    return HydrationError(HydrationDiagnostic.from_syntax(message, syntax))


def require_node(syntax: SyntaxElement, kind: SyntaxNodeKind) -> SyntaxNode:
    if not isinstance(syntax, SyntaxNode) or syntax.kind != kind:
        raise syntax_error(
            syntax,
            f"Expected syntax node '{kind.value}', got '{syntax_name(syntax)}'",
        )
    return syntax


def require_token(syntax: SyntaxElement, kind: SyntaxTokenKind) -> SyntaxToken:
    if not isinstance(syntax, SyntaxToken) or syntax.kind != kind:
        raise syntax_error(
            syntax,
            f"Expected syntax token '{kind.value}', got '{syntax_name(syntax)}'",
        )
    return syntax


def optional_node(
    children: list[SyntaxElement], kind: SyntaxNodeKind
) -> SyntaxNode | None:
    found = [
        child
        for child in children
        if isinstance(child, SyntaxNode) and child.kind == kind
    ]
    if len(found) > 1:
        raise syntax_error(found[1], f"Expected at most one '{kind.value}' node")
    return found[0] if found else None


def optional_token(
    children: list[SyntaxElement],
    kind: SyntaxTokenKind,
) -> SyntaxToken | None:
    found = [
        child
        for child in children
        if isinstance(child, SyntaxToken) and child.kind == kind
    ]
    if len(found) > 1:
        raise syntax_error(found[1], f"Expected at most one '{kind.value}' token")
    return found[0] if found else None


def only_child_node(parent: SyntaxNode, kind: SyntaxNodeKind) -> SyntaxNode:
    found = parent.child_nodes(kind)
    if len(found) != 1:
        raise syntax_error(
            parent,
            f"Expected one child '{kind.value}' node, found {len(found)}",
        )
    return found[0]


@dataclass(frozen=True)
class ParameterDeclarationSyntax:
    name: SyntaxToken
    datatype: SyntaxNode
    default: SyntaxNode | None
    metadata: SyntaxNode | None
    nullable: SyntaxNode | None

    @classmethod
    def from_node(cls, node: SyntaxNode) -> "ParameterDeclarationSyntax":
        require_node(node, SyntaxNodeKind.PARAMETER_DECLARATION)
        children = list(node.children)
        identifiers = [
            require_token(child, SyntaxTokenKind.IDENTIFIER)
            for child in children
            if isinstance(child, SyntaxToken)
            and child.kind == SyntaxTokenKind.IDENTIFIER
        ]
        if len(identifiers) != 1:
            raise syntax_error(node, "Parameter declaration requires one identifier")
        return cls(
            name=identifiers[0],
            datatype=only_child_node(node, SyntaxNodeKind.DATA_TYPE),
            default=optional_node(children, SyntaxNodeKind.PARAMETER_DEFAULT),
            metadata=optional_node(children, SyntaxNodeKind.METADATA),
            nullable=optional_node(children, SyntaxNodeKind.CONCEPT_NULLABLE_MODIFIER),
        )


@dataclass(frozen=True)
class ConceptDeclarationSyntax:
    purpose: SyntaxToken
    name: SyntaxToken
    datatype: SyntaxNode
    metadata: SyntaxNode | None
    nullable: SyntaxNode | None

    @classmethod
    def from_node(cls, node: SyntaxNode) -> "ConceptDeclarationSyntax":
        require_node(node, SyntaxNodeKind.CONCEPT_DECLARATION)
        children = list(node.children)
        return cls(
            purpose=require_token(children[0], SyntaxTokenKind.PURPOSE),
            name=require_token(children[1], SyntaxTokenKind.IDENTIFIER),
            datatype=only_child_node(node, SyntaxNodeKind.DATA_TYPE),
            metadata=optional_node(children, SyntaxNodeKind.METADATA),
            nullable=optional_node(children, SyntaxNodeKind.CONCEPT_NULLABLE_MODIFIER),
        )


@dataclass(frozen=True)
class ConceptPropertyDeclarationSyntax:
    unique: SyntaxToken | None
    property_token: SyntaxToken
    declaration: SyntaxElement
    datatype: SyntaxNode
    metadata: SyntaxNode | None
    nullable: SyntaxNode | None

    @classmethod
    def from_node(cls, node: SyntaxNode) -> "ConceptPropertyDeclarationSyntax":
        require_node(node, SyntaxNodeKind.CONCEPT_PROPERTY_DECLARATION)
        children = list(node.children)
        property_index = next(
            (
                index
                for index, child in enumerate(children)
                if isinstance(child, SyntaxToken)
                and child.kind == SyntaxTokenKind.PROPERTY
            ),
            None,
        )
        if property_index is None:
            raise syntax_error(node, "Property declaration requires a property token")
        declaration_index = property_index + 1
        if declaration_index >= len(children):
            raise syntax_error(
                node, "Property declaration requires a declaration target"
            )
        return cls(
            unique=optional_token(children, SyntaxTokenKind.UNIQUE),
            property_token=require_token(
                children[property_index], SyntaxTokenKind.PROPERTY
            ),
            declaration=children[declaration_index],
            datatype=only_child_node(node, SyntaxNodeKind.DATA_TYPE),
            metadata=optional_node(children, SyntaxNodeKind.METADATA),
            nullable=optional_node(children, SyntaxNodeKind.CONCEPT_NULLABLE_MODIFIER),
        )


@dataclass(frozen=True)
class ConceptDerivationSyntax:
    purpose: SyntaxToken
    name: SyntaxElement
    source: SyntaxElement
    metadata: SyntaxNode | None

    @classmethod
    def from_node(cls, node: SyntaxNode) -> "ConceptDerivationSyntax":
        require_node(node, SyntaxNodeKind.CONCEPT_DERIVATION)
        children = list(node.children)
        if len(children) < 3:
            raise syntax_error(
                node, "Concept derivation requires purpose, name, and source"
            )
        if not isinstance(children[0], SyntaxToken):
            raise syntax_error(children[0], "Concept derivation requires purpose token")
        return cls(
            purpose=children[0],
            name=children[1],
            source=children[2],
            metadata=optional_node(children, SyntaxNodeKind.METADATA),
        )


@dataclass(frozen=True)
class ConstantDerivationSyntax:
    name: SyntaxToken
    source: SyntaxElement
    metadata: SyntaxNode | None

    @classmethod
    def from_node(cls, node: SyntaxNode) -> "ConstantDerivationSyntax":
        require_node(node, SyntaxNodeKind.CONSTANT_DERIVATION)
        children = list(node.children)
        if len(children) < 3:
            raise syntax_error(
                node, "Constant derivation requires const, name, and source"
            )
        return cls(
            name=require_token(children[1], SyntaxTokenKind.IDENTIFIER),
            source=children[2],
            metadata=optional_node(children, SyntaxNodeKind.METADATA),
        )


@dataclass(frozen=True)
class PropertyIdentifierSyntax:
    grains: list[SyntaxToken]
    name: SyntaxToken

    @classmethod
    def from_node(cls, node: SyntaxNode) -> "PropertyIdentifierSyntax":
        require_node(node, SyntaxNodeKind.PROPERTY_IDENTIFIER)
        identifiers = node.child_tokens(SyntaxTokenKind.IDENTIFIER)
        if len(identifiers) < 2:
            raise syntax_error(node, "Property identifier requires grain and name")
        return cls(grains=identifiers[:-1], name=identifiers[-1])


@dataclass(frozen=True)
class PropertyWildcardSyntax:
    name: SyntaxToken

    @classmethod
    def from_node(cls, node: SyntaxNode) -> "PropertyWildcardSyntax":
        require_node(node, SyntaxNodeKind.PROPERTY_IDENTIFIER_WILDCARD)
        identifiers = node.child_tokens(SyntaxTokenKind.IDENTIFIER)
        if len(identifiers) != 1:
            raise syntax_error(node, "Wildcard property identifier requires one name")
        return cls(name=identifiers[0])
