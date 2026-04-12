from __future__ import annotations

from trilogy.core.models.author import (
    AlignClause,
    AlignItem,
    Comparison,
    DeriveClause,
    DeriveItem,
    Function,
    HavingClause,
    MultiSelectLineage,
    OrderBy,
    WhereClause,
    WindowItem,
)
from trilogy.core.statements.author import MultiSelectStatement, SelectStatement
from trilogy.parsing.common import align_item_to_concept, derive_item_to_concept
from trilogy.parsing.v2.concept_rules import metadata_from_meta
from trilogy.parsing.v2.rules_context import (
    HydrateFunction,
    NodeHydrator,
    RuleContext,
    core_meta,
    fail,
    hydrated_children,
)
from trilogy.parsing.v2.syntax import SyntaxNode, SyntaxNodeKind


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
    # Phased hydration: align-derived concepts (e.g. `report_date`) are
    # registered in the environment before clauses that may reference them
    # (ORDER BY, HAVING, DERIVE) get hydrated.
    select_nodes: list[SyntaxNode] = []
    align_node: SyntaxNode | None = None
    derive_node: SyntaxNode | None = None
    where_node: SyntaxNode | None = None
    having_node: SyntaxNode | None = None
    order_by_node: SyntaxNode | None = None
    limit_node: SyntaxNode | None = None
    for child in node.children:
        if not isinstance(child, SyntaxNode):
            continue
        kind = child.kind
        if kind == SyntaxNodeKind.SELECT_STATEMENT:
            select_nodes.append(child)
        elif kind == SyntaxNodeKind.ALIGN_CLAUSE:
            align_node = child
        elif kind == SyntaxNodeKind.DERIVE_CLAUSE:
            derive_node = child
        elif kind == SyntaxNodeKind.WHERE:
            where_node = child
        elif kind == SyntaxNodeKind.HAVING:
            having_node = child
        elif kind == SyntaxNodeKind.ORDER_BY:
            order_by_node = child
        elif kind == SyntaxNodeKind.LIMIT:
            limit_node = child

    if align_node is None:
        raise fail(node, "Multi-select statement requires an align clause")

    selects: list[SelectStatement] = [hydrate(n) for n in select_nodes]
    align_c: AlignClause = hydrate(align_node)

    # Finalize inner selects so as_lineage / align_item_to_concept see
    # populated grain and local_concepts.
    for sel in selects:
        sel.finalize(context.environment)

    # WHERE/LIMIT do not reference align-derived outputs, so hydrate them
    # up front and fold them into the align concepts' lineage.
    where: WhereClause | None = hydrate(where_node) if where_node else None
    limit_val: int | None = hydrate(limit_node).count if limit_node else None

    derived_concepts: list = []
    for x in align_c.items:
        concept = align_item_to_concept(
            x,
            align_c,
            selects,
            where=where,
            having=None,
            limit=limit_val,
            environment=context.environment,
        )
        derived_concepts.append(concept)
        context.add_concept(concept, meta=core_meta(node.meta))

    # These clauses may reference align-derived outputs (e.g. `report_date`),
    # so hydrate them after the align concepts are registered.
    having: HavingClause | None = hydrate(having_node) if having_node else None
    derive: DeriveClause | None = hydrate(derive_node) if derive_node else None
    order_by_val: OrderBy | None = hydrate(order_by_node) if order_by_node else None

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


MULTISELECT_NODE_HYDRATORS: dict[SyntaxNodeKind, NodeHydrator] = {
    SyntaxNodeKind.ALIGN_ITEM: align_item,
    SyntaxNodeKind.ALIGN_CLAUSE: align_clause,
    SyntaxNodeKind.DERIVE_ITEM: derive_item,
    SyntaxNodeKind.DERIVE_CLAUSE: derive_clause,
    SyntaxNodeKind.MULTI_SELECT_STATEMENT: multi_select_statement,
}
