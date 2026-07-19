from __future__ import annotations

from typing import Any

from trilogy.parsing.v2.rules_context import (
    HydrateFunction,
    NodeHydrator,
    RuleContext,
    hydrated_children,
)
from trilogy.parsing.v2.syntax import SyntaxNode, SyntaxNodeKind


def expr_tuple(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Any:
    from trilogy.core.models.core import (
        DataType,
        TupleWrapper,
        arg_to_datatype,
        reduce_tuple_element_datatypes,
    )

    # A merged element type only makes sense for a value-list tuple; a composite
    # (row) membership tuple compares position-wise and may legitimately mix
    # types, so defer validation to the comparison rule (per-position for row
    # tuples, per-element-vs-left for scalar value lists).
    args = hydrated_children(node, hydrate)
    datatypes = [arg_to_datatype(x) for x in args]
    try:
        dtype, nullable = reduce_tuple_element_datatypes(datatypes)
    except ValueError:
        dtype = DataType.UNKNOWN
        nullable = any(d == DataType.NULL for d in datatypes)
    return TupleWrapper(val=tuple(args), type=dtype, nullable=nullable)


SUBSELECT_NODE_HYDRATORS: dict[SyntaxNodeKind, NodeHydrator] = {
    SyntaxNodeKind.EXPR_TUPLE: expr_tuple,
}
