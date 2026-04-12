from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from trilogy.core.models.author import (
    ArgBinding,
    Concept,
    OrderItem,
    SubselectItem,
    TraitDataType,
    WhereClause,
)
from trilogy.core.models.core import DataType
from trilogy.core.statements.author import FunctionDeclaration
from trilogy.parsing.v2.rules_context import (
    HydrateFunction,
    NodeHydrator,
    RuleContext,
    hydrated_children,
)
from trilogy.parsing.v2.syntax import SyntaxNode, SyntaxNodeKind


@dataclass
class FunctionBindingType:
    type: DataType | TraitDataType | None = None


def function_binding_list(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> list[ArgBinding]:
    return hydrated_children(node, hydrate)


def function_binding_type(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> FunctionBindingType:
    args = hydrated_children(node, hydrate)
    return FunctionBindingType(type=args[0])


def function_binding_default(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Any:
    args = hydrated_children(node, hydrate)
    return args[1] if len(args) > 1 else args[0]


def function_binding_item(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> ArgBinding:
    args = hydrated_children(node, hydrate)
    default = None
    dtype = None
    for arg in args[1:]:
        if isinstance(arg, FunctionBindingType):
            dtype = arg.type
        else:
            default = arg
    return ArgBinding(
        name=str(args[0]), datatype=dtype or DataType.UNKNOWN, default=default
    )


def raw_function(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> FunctionDeclaration:
    args = hydrated_children(node, hydrate)
    identity = str(args[0])
    function_arguments: list[ArgBinding] = args[1]
    output = args[2]
    return FunctionDeclaration(name=identity, args=function_arguments, expr=output)


def table_function(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> FunctionDeclaration:
    args = hydrated_children(node, hydrate)
    identity = str(args[0])
    idx = 1
    function_arguments: list[ArgBinding] = []
    if idx < len(args) and isinstance(args[idx], list):
        function_arguments = args[idx]
        idx += 1
    content = args[idx]
    if isinstance(content, Concept):
        content = content.reference
    idx += 1
    where = None
    order_by: list[OrderItem] = []
    limit = None
    for arg in args[idx:]:
        if isinstance(arg, WhereClause):
            where = arg
        elif isinstance(arg, list):
            order_by = arg
        elif isinstance(arg, int):
            limit = arg
    sub = SubselectItem(content=content, where=where, order_by=order_by, limit=limit)
    return FunctionDeclaration(name=identity, args=function_arguments, expr=sub)


FUNCTION_DEFINITION_NODE_HYDRATORS: dict[SyntaxNodeKind, NodeHydrator] = {
    SyntaxNodeKind.RAW_FUNCTION: raw_function,
    SyntaxNodeKind.TABLE_FUNCTION: table_function,
    SyntaxNodeKind.FUNCTION_BINDING_LIST: function_binding_list,
    SyntaxNodeKind.FUNCTION_BINDING_ITEM: function_binding_item,
    SyntaxNodeKind.FUNCTION_BINDING_TYPE: function_binding_type,
    SyntaxNodeKind.FUNCTION_BINDING_DEFAULT: function_binding_default,
}
