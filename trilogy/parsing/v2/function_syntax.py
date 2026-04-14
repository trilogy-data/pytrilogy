from __future__ import annotations

from dataclasses import dataclass

from trilogy.parsing.v2.concept_syntax import (
    optional_node,
    require_node,
    syntax_error,
)
from trilogy.parsing.v2.syntax import (
    SyntaxNode,
    SyntaxNodeKind,
    SyntaxToken,
    SyntaxTokenKind,
)

FUNCTION_DEFINITION_KINDS = {
    SyntaxNodeKind.RAW_FUNCTION,
    SyntaxNodeKind.TABLE_FUNCTION,
}


@dataclass(frozen=True)
class FunctionBindingSyntax:
    name: SyntaxToken

    @classmethod
    def from_node(cls, node: SyntaxNode) -> "FunctionBindingSyntax":
        require_node(node, SyntaxNodeKind.FUNCTION_BINDING_ITEM)
        for child in node.child_tokens(SyntaxTokenKind.IDENTIFIER):
            return cls(name=child)
        raise syntax_error(
            node, "Function binding item requires a parameter identifier"
        )


@dataclass(frozen=True)
class FunctionDefinitionSyntax:
    name: SyntaxToken
    bindings: list[FunctionBindingSyntax]
    node: SyntaxNode

    @classmethod
    def from_node(cls, node: SyntaxNode) -> "FunctionDefinitionSyntax":
        if (
            not isinstance(node, SyntaxNode)
            or node.kind not in FUNCTION_DEFINITION_KINDS
        ):
            raise syntax_error(
                node,
                "Expected raw_function or table_function",
            )
        name_token: SyntaxToken | None = None
        for child in node.child_tokens(SyntaxTokenKind.IDENTIFIER):
            name_token = child
            break
        if name_token is None:
            raise syntax_error(node, "Function definition requires a name identifier")

        binding_list = optional_node(node, SyntaxNodeKind.FUNCTION_BINDING_LIST)
        bindings: list[FunctionBindingSyntax] = []
        if binding_list is not None:
            for item in binding_list.child_nodes(SyntaxNodeKind.FUNCTION_BINDING_ITEM):
                bindings.append(FunctionBindingSyntax.from_node(item))
        return cls(name=name_token, bindings=bindings, node=node)

    @property
    def parameter_names(self) -> list[str]:
        return [binding.name.value for binding in self.bindings]
