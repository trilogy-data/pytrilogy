from __future__ import annotations

from dataclasses import dataclass

from trilogy.core.models.author import Concept
from trilogy.core.statements.author import (
    ConceptDeclarationStatement,
    ShowStatement,
)
from trilogy.parsing.v2.model import HydrationDiagnostic, HydrationError
from trilogy.parsing.v2.rules_context import (
    HydrateFunction,
    RuleContext,
    apply_source_location,
)
from trilogy.parsing.v2.syntax import SyntaxNode, SyntaxNodeKind


def only_child_node(
    parent: SyntaxNode, kind: SyntaxNodeKind | None = None
) -> SyntaxNode:
    found = parent.child_nodes(kind)
    if len(found) != 1:
        expected = kind.value if kind else "node"
        raise HydrationError(
            HydrationDiagnostic.from_syntax(
                f"Expected one child {expected} under statement node, found {len(found)}",
                parent,
            )
        )
    return found[0]


_SHOW_CONTENT_KINDS = {
    SyntaxNodeKind.SHOW_CATEGORY,
    SyntaxNodeKind.VALIDATE_STATEMENT,
    SyntaxNodeKind.SELECT_STATEMENT,
    SyntaxNodeKind.PERSIST_STATEMENT,
}


@dataclass(frozen=True)
class ShowStatementSyntax:
    content: SyntaxNode

    @classmethod
    def from_node(cls, node: SyntaxNode) -> "ShowStatementSyntax":
        nodes = node.child_nodes()
        for child in nodes:
            if child.kind in _SHOW_CONTENT_KINDS:
                return cls(content=child)
        raise HydrationError(
            HydrationDiagnostic.from_syntax(
                "show statement requires a show_category, validate, select, or persist child",
                node,
            )
        )


def hydrate_concept_statement(
    concept_node: SyntaxNode,
    context: RuleContext,
    hydrate_rule: HydrateFunction,
) -> ConceptDeclarationStatement:
    declaration = only_child_node(concept_node)
    output = hydrate_rule(declaration)
    if isinstance(output, list):
        concept_value = output[0]
    elif isinstance(output, Concept):
        concept_value = output
    else:
        concept_value = output.concept
    apply_source_location(concept_value, concept_node.meta)
    return ConceptDeclarationStatement(concept=concept_value)


def hydrate_show_statement(
    show_node: SyntaxNode,
    hydrate_rule: HydrateFunction,
) -> ShowStatement:
    syntax = ShowStatementSyntax.from_node(show_node)
    return ShowStatement(content=hydrate_rule(syntax.content))
