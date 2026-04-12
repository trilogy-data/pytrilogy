from __future__ import annotations

from typing import Any

from trilogy.core.enums import ConceptSource, Modifier, Ordering
from trilogy.core.models.author import (
    AlignClause,
    AlignItem,
    Comparison,
    ConceptRef,
    DeriveClause,
    DeriveItem,
    Function,
    HavingClause,
    Metadata,
    MultiSelectLineage,
    OrderBy,
    OrderItem,
    WhereClause,
    WindowItem,
)
from trilogy.core.models.environment import Environment
from trilogy.core.statements.author import (
    ConceptTransform,
    FromClause,
    Limit,
    MultiSelectStatement,
    SelectItem,
    SelectStatement,
)
from trilogy.parsing.common import (
    align_item_to_concept,
    arbitrary_to_concept,
    derive_item_to_concept,
    unwrap_transformation,
)
from trilogy.parsing.v2.concept_rules import metadata_from_meta, parse_concept_reference
from trilogy.parsing.v2.rules_context import (
    HydrateFunction,
    NodeHydrator,
    RuleContext,
    core_meta,
    fail,
    hydrated_children,
)
from trilogy.parsing.v2.syntax import SyntaxNode, SyntaxNodeKind


def select_statement(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> SelectStatement:
    args = hydrated_children(node, hydrate)
    select_items: list[SelectItem] | None = None
    limit: int | None = None
    order_by: OrderBy | None = None
    from_clause: FromClause | None = None
    where: WhereClause | None = None
    having: HavingClause | None = None
    for arg in args:
        atype = type(arg)
        if atype is list:
            select_items = arg
        elif atype is Limit:
            limit = arg.count
        elif atype is OrderBy:
            order_by = arg
        elif atype is FromClause:
            from_clause = arg
        elif atype is WhereClause:
            if where is not None:
                raise fail(node, "Multiple where clauses are not supported")
            where = arg
        elif atype is HavingClause:
            having = arg
    if not select_items:
        raise fail(node, "Malformed select, missing select items")
    return SelectStatement(
        selection=select_items,
        order_by=order_by,
        where_clause=where,
        having_clause=having,
        limit=limit,
        eligible_datasources=from_clause.sources if from_clause else None,
        meta=metadata_from_meta(node.meta),
    )


def select_item(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> SelectItem | None:
    args = hydrated_children(node, hydrate)
    modifiers = [arg for arg in args if isinstance(arg, Modifier)]
    content_args = [arg for arg in args if not isinstance(arg, Modifier)]
    if not content_args:
        return None
    content = content_args[0]
    return SelectItem(content=content, modifiers=modifiers)


def select_transform(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> ConceptTransform:
    args = hydrated_children(node, hydrate)
    # args: [expr, IDENTIFIER] or [expr, IDENTIFIER, metadata]
    # Filter out assignment tokens
    content_args = [a for a in args if not isinstance(a, str) or a not in ("->", "as")]
    # The expr is first non-string, the name is last string before optional metadata
    expr_val = content_args[0]
    output_name: str = content_args[1]
    metadata: Metadata | None = content_args[2] if len(content_args) > 2 else None
    transformation = unwrap_transformation(expr_val, context.environment)
    _, namespace, output_name, _ = parse_concept_reference(
        output_name, context.environment
    )
    meta = metadata_from_meta(node.meta, concept_source=ConceptSource.SELECT)
    if metadata:
        meta = metadata
    concept = arbitrary_to_concept(
        transformation,
        environment=context.environment,
        namespace=namespace,
        name=output_name,
        metadata=meta,
    )
    context.add_concept(concept, meta=core_meta(node.meta))
    return ConceptTransform(function=transformation, output=concept)


def select_list(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> list[SelectItem]:
    args = hydrated_children(node, hydrate)
    return [arg for arg in args if arg is not None]


def select_hide_modifier(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Modifier:
    return Modifier.HIDDEN


def select_partial_modifier(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Modifier:
    return Modifier.PARTIAL


def from_clause(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> FromClause:
    args = hydrated_children(node, hydrate)
    return FromClause(sources=[str(arg) for arg in args])


def order_by(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> OrderBy:
    args = hydrated_children(node, hydrate)
    return OrderBy(items=args[0])


def _resolve_order_ref(name: str, env: Environment) -> ConceptRef:
    # Namespace resolution only: returns a lightweight ConceptRef whether or not the
    # target concept is fully defined yet — downstream phases resolve the rest.
    return ConceptRef(address=env.concepts[name].address)


def order_list(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> list[OrderItem]:
    args = hydrated_children(node, hydrate)
    return [
        OrderItem(
            expr=(
                _resolve_order_ref(str(x), context.environment)
                if isinstance(x, str)
                else x
            ),
            order=y,
        )
        for x, y in zip(args[::2], args[1::2])
    ]


def ordering(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Ordering:
    args = hydrated_children(node, hydrate)
    base = "asc"
    null_sort: str | None = None
    if args:
        first = str(args[0]).lower()
        if first in {"asc", "desc"}:
            base = first
            if len(args) > 1:
                null_sort = str(args[-1]).lower()
        else:
            null_sort = first
    if null_sort:
        return Ordering(" ".join([base, "nulls", null_sort]))
    return Ordering(base)


def limit_clause(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Limit:
    args = hydrated_children(node, hydrate)
    return Limit(count=int(str(args[0])))


def expr_tuple(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Any:
    from trilogy.core.models.core import DataType, TupleWrapper, arg_to_datatype

    args = hydrated_children(node, hydrate)
    datatypes = set(arg_to_datatype(x) for x in args)
    if len(datatypes) != 1:
        raise fail(node, "Tuple must have same type for all elements")
    dtype = datatypes.pop()
    if not isinstance(dtype, DataType):
        raise fail(node, f"Tuple element type {dtype} is not a base DataType")
    return TupleWrapper(val=tuple(args), type=dtype)


def subselect_comparison(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Any:
    from trilogy.core.models.author import (
        AggregateWrapper,
        Concept,
        FilterItem,
        Function,
        Parenthetical,
        SubselectComparison,
        WindowItem,
    )
    from trilogy.core.models.core import ListWrapper, TupleWrapper
    from trilogy.parsing.common import arbitrary_to_concept

    args = hydrated_children(node, hydrate)
    left = args[0]
    operator = args[1]
    right = args[2]
    while isinstance(right, Parenthetical) and isinstance(
        right.content,
        (
            Concept,
            Function,
            FilterItem,
            WindowItem,
            AggregateWrapper,
            ListWrapper,
            TupleWrapper,
        ),
    ):
        right = right.content
    if isinstance(right, (Function, FilterItem, WindowItem, AggregateWrapper)):
        right_concept = arbitrary_to_concept(right, environment=context.environment)
        context.add_concept(right_concept, meta=core_meta(node.meta))
        right = right_concept.reference
    return SubselectComparison(left=left, right=right, operator=operator)


def align_item(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> AlignItem:
    args = hydrated_children(node, hydrate)
    alias = str(args[0])
    concepts = [context.environment.concepts[str(a)].reference for a in args[1:]]
    return AlignItem(
        alias=alias,
        namespace=context.environment.namespace,
        concepts=concepts,
    )


def align_clause(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> AlignClause:
    return AlignClause(items=hydrated_children(node, hydrate))


def derive_item(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> DeriveItem:
    args = hydrated_children(node, hydrate)
    return DeriveItem(
        expr=args[0], name=str(args[1]), namespace=context.environment.namespace
    )


def derive_clause(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> DeriveClause:
    return DeriveClause(items=hydrated_children(node, hydrate))


def multi_select_statement(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> MultiSelectStatement:
    args = hydrated_children(node, hydrate)
    selects: list[SelectStatement] = []
    align_c: AlignClause | None = None
    limit_val: int | None = None
    order_by_val: OrderBy | None = None
    where: WhereClause | None = None
    having: HavingClause | None = None
    derive: DeriveClause | None = None
    for arg in args:
        atype = type(arg)
        if atype is SelectStatement:
            selects.append(arg)
        elif atype is Limit:
            limit_val = arg.count
        elif atype is OrderBy:
            order_by_val = arg
        elif atype is WhereClause:
            where = arg
        elif atype is HavingClause:
            having = arg
        elif atype is AlignClause:
            align_c = arg
        elif atype is DeriveClause:
            derive = arg

    if align_c is None:
        raise fail(node, "Multi-select statement requires an align clause")
    # Mirror v1's SelectStatement.from_inputs: finalize inner selects before
    # `as_lineage` so grain/local_concepts are populated.
    for sel in selects:
        sel.finalize(context.environment)
    derived_concepts = []
    new_selects = [x.as_lineage(context.environment) for x in selects]
    lineage = MultiSelectLineage(
        selects=new_selects,
        align=align_c,
        derive=derive,
        namespace=context.environment.namespace,
        where_clause=where,
        having_clause=having,
        limit=limit_val,
        hidden_components=set(y for x in new_selects for y in x.hidden_components),
    )
    for x in align_c.items:
        concept = align_item_to_concept(
            x,
            align_c,
            selects,
            where=where,
            having=having,
            limit=limit_val,
            environment=context.environment,
        )
        derived_concepts.append(concept)
        context.add_concept(concept, meta=core_meta(node.meta))
    if derive:
        for derived in derive.items:
            derivation = derived.expr
            name = derived.name
            if not isinstance(derivation, (Function, Comparison, WindowItem)):
                raise fail(
                    node,
                    f"Invalid derive expression {derivation}, must be a function or conditional",
                )
            concept = derive_item_to_concept(
                derivation, name, lineage, context.environment.namespace
            )
            derived_concepts.append(concept)
            context.add_concept(concept, meta=core_meta(node.meta))
    return MultiSelectStatement(
        selects=selects,
        align=align_c,
        namespace=context.environment.namespace,
        where_clause=where,
        order_by=order_by_val,
        limit=limit_val,
        meta=metadata_from_meta(node.meta),
        derived_concepts=derived_concepts,
        derive=derive,
    )


SELECT_NODE_HYDRATORS: dict[SyntaxNodeKind, NodeHydrator] = {
    SyntaxNodeKind.SELECT_STATEMENT: select_statement,
    SyntaxNodeKind.SELECT_ITEM: select_item,
    SyntaxNodeKind.SELECT_TRANSFORM: select_transform,
    SyntaxNodeKind.SELECT_LIST: select_list,
    SyntaxNodeKind.SELECT_HIDE_MODIFIER: select_hide_modifier,
    SyntaxNodeKind.SELECT_PARTIAL_MODIFIER: select_partial_modifier,
    SyntaxNodeKind.FROM_CLAUSE: from_clause,
    SyntaxNodeKind.ORDER_BY: order_by,
    SyntaxNodeKind.ORDER_LIST: order_list,
    SyntaxNodeKind.ORDERING: ordering,
    SyntaxNodeKind.LIMIT: limit_clause,
    SyntaxNodeKind.EXPR_TUPLE: expr_tuple,
    SyntaxNodeKind.SUBSELECT_COMPARISON: subselect_comparison,
    SyntaxNodeKind.ALIGN_ITEM: align_item,
    SyntaxNodeKind.ALIGN_CLAUSE: align_clause,
    SyntaxNodeKind.DERIVE_ITEM: derive_item,
    SyntaxNodeKind.DERIVE_CLAUSE: derive_clause,
    SyntaxNodeKind.MULTI_SELECT_STATEMENT: multi_select_statement,
}
