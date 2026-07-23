from __future__ import annotations

from trilogy.core.enums import FunctionType
from trilogy.core.models.author import CustomType
from trilogy.core.models.core import CONCRETE_TYPES, ValidatedType
from trilogy.core.statements.author import TypeDeclaration
from trilogy.parsing.v2.rules_context import (
    HydrateFunction,
    NodeHydrator,
    RuleContext,
    fail,
    hydrated_children,
)
from trilogy.parsing.v2.syntax import SyntaxNode, SyntaxNodeKind


def type_drop_clause(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> list[FunctionType]:
    values = hydrated_children(node, hydrate)
    return [FunctionType(str(v)) for v in values]


def type_declaration(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> TypeDeclaration:
    name_tokens = node.child_tokens()
    if not name_tokens:
        raise fail(node, "type declaration missing name identifier")
    name = str(hydrate(name_tokens[0]))
    drop_clause = node.optional_node(SyntaxNodeKind.TYPE_DROP_CLAUSE)
    drop_on: list[FunctionType] = hydrate(drop_clause) if drop_clause else []
    datatypes: list[CONCRETE_TYPES] = [
        hydrate(child)
        for child in node.child_nodes()
        if child.kind != SyntaxNodeKind.TYPE_DROP_CLAUSE
    ]
    if len(datatypes) > 1 and any(isinstance(d, ValidatedType) for d in datatypes):
        raise fail(node, "validators are not supported on union type declarations")
    final: CONCRETE_TYPES | list[CONCRETE_TYPES] = (
        datatypes[0] if len(datatypes) == 1 else datatypes
    )
    new_type = CustomType(
        name=name,
        type=final,
        drop_on=drop_on,
        add_on=[],
    )
    return TypeDeclaration(type=new_type)


TYPE_NODE_HYDRATORS: dict[SyntaxNodeKind, NodeHydrator] = {
    SyntaxNodeKind.TYPE_DECLARATION: type_declaration,
    SyntaxNodeKind.TYPE_DROP_CLAUSE: type_drop_clause,
}
