from __future__ import annotations

from trilogy.core.enums import FunctionType
from trilogy.core.models.author import CustomType
from trilogy.core.models.core import DataType
from trilogy.core.statements.author import TypeDeclaration
from trilogy.parsing.v2.rules_context import (
    HydrateFunction,
    NodeHydrator,
    RuleContext,
    fail,
    hydrated_children,
)
from trilogy.parsing.v2.syntax import SyntaxNode, SyntaxNodeKind, SyntaxToken


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
    name: str | None = None
    datatypes: list[DataType] = []
    drop_on: list[FunctionType] = []
    for child in node.children:
        hydrated = hydrate(child)
        if name is None and isinstance(child, SyntaxToken):
            name = str(hydrated)
            continue
        if (
            isinstance(child, SyntaxNode)
            and child.kind == SyntaxNodeKind.TYPE_DROP_CLAUSE
        ):
            drop_on = hydrated
            continue
        datatypes.append(hydrated)
    if name is None:
        raise fail(node, "type declaration missing name identifier")
    final: DataType | list[DataType] = (
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
