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
        identifiers = node.child_tokens(SyntaxTokenKind.IDENTIFIER)
        if len(identifiers) != 1:
            raise syntax_error(node, "Parameter declaration requires one identifier")
        return cls(
            name=identifiers[0],
            datatype=node.only_child_node(SyntaxNodeKind.DATA_TYPE),
            default=node.optional_node(SyntaxNodeKind.PARAMETER_DEFAULT),
            metadata=node.optional_node(SyntaxNodeKind.METADATA),
            nullable=node.optional_node(SyntaxNodeKind.CONCEPT_NULLABLE_MODIFIER),
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
        children = node.children
        return cls(
            purpose=require_token(children[0], SyntaxTokenKind.PURPOSE),
            name=require_token(children[1], SyntaxTokenKind.IDENTIFIER),
            datatype=node.only_child_node(SyntaxNodeKind.DATA_TYPE),
            metadata=node.optional_node(SyntaxNodeKind.METADATA),
            nullable=node.optional_node(SyntaxNodeKind.CONCEPT_NULLABLE_MODIFIER),
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
        property_token = node.optional_token(SyntaxTokenKind.PROPERTY)
        if property_token is None:
            raise syntax_error(node, "Property declaration requires a property token")
        children = node.children
        declaration_index = children.index(property_token) + 1
        if declaration_index >= len(children):
            raise syntax_error(
                node, "Property declaration requires a declaration target"
            )
        return cls(
            unique=node.optional_token(SyntaxTokenKind.UNIQUE),
            property_token=property_token,
            declaration=children[declaration_index],
            datatype=node.only_child_node(SyntaxNodeKind.DATA_TYPE),
            metadata=node.optional_node(SyntaxNodeKind.METADATA),
            nullable=node.optional_node(SyntaxNodeKind.CONCEPT_NULLABLE_MODIFIER),
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
        children = node.children
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
            metadata=node.optional_node(SyntaxNodeKind.METADATA),
        )


@dataclass(frozen=True)
class ConstantDerivationSyntax:
    name: SyntaxToken
    source: SyntaxElement
    metadata: SyntaxNode | None

    @classmethod
    def from_node(cls, node: SyntaxNode) -> "ConstantDerivationSyntax":
        require_node(node, SyntaxNodeKind.CONSTANT_DERIVATION)
        children = node.children
        if len(children) < 3:
            raise syntax_error(
                node, "Constant derivation requires const, name, and source"
            )
        return cls(
            name=require_token(children[1], SyntaxTokenKind.IDENTIFIER),
            source=children[2],
            metadata=node.optional_node(SyntaxNodeKind.METADATA),
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
