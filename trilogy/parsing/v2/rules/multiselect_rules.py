from __future__ import annotations

from trilogy.core.enums import Modifier
from trilogy.core.exceptions import InvalidSyntaxException
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
    WindowItem,
)
from trilogy.core.statements.author import MultiSelectStatement, SelectStatement
from trilogy.parsing.v2.concept_factory import (
    align_item_to_concept_v2,
    derive_item_to_concept_v2,
)
from trilogy.parsing.v2.rules.concept_rules import metadata_from_meta
from trilogy.parsing.v2.rules_context import (
    HydrateFunction,
    NodeHydrator,
    RuleContext,
    core_meta,
    fail,
    hydrated_children,
)
from trilogy.parsing.v2.select_finalize import finalize_select_statement
from trilogy.parsing.v2.syntax import SyntaxNode, SyntaxNodeKind


def align_item(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> AlignItem:
    args = hydrated_children(node, hydrate)
    hidden = False
    rest = list(args)
    if rest and rest[0] is Modifier.HIDDEN:
        hidden = True
        rest = rest[1:]
    alias = str(rest[0])
    concepts = [context.concepts.reference(str(a)) for a in rest[1:]]
    return AlignItem(
        alias=alias,
        namespace=context.environment.namespace,
        concepts=concepts,
        hidden=hidden,
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
    having_node: SyntaxNode | None = None
    order_by_node: SyntaxNode | None = None
    limit_node: SyntaxNode | None = None
    for child in node.child_nodes():
        kind = child.kind
        if kind == SyntaxNodeKind.SELECT_STATEMENT:
            select_nodes.append(child)
        elif kind == SyntaxNodeKind.ALIGN_CLAUSE:
            align_node = child
        elif kind == SyntaxNodeKind.DERIVE_CLAUSE:
            derive_node = child
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
        finalize_select_statement(sel, context)

    # Arms sharing an output address collapse to one graph node (concepts are
    # keyed by address), so codegen can't tell which arm's CTE a reference points
    # at — it emits INVALID_REFERENCE_BUG / drops virtual aliases. Reject with an
    # actionable message: use distinct names per arm and tie them with `align`.
    seen: dict[str, int] = {}
    for arm_index, sel in enumerate(selects):
        for out_ref in sel.output_components:
            prior = seen.get(out_ref.address)
            if prior is not None and prior != arm_index:
                short = out_ref.address.split(".", 1)[-1]
                raise InvalidSyntaxException(
                    f"Multi-select arms must use distinct output names; "
                    f"'{short}' appears in more than one arm. Give each arm its "
                    f"own name and tie them with `align` "
                    f"(e.g. `... as {short}1` / `... as {short}2`, "
                    f"`align grp: {short}1, {short}2`)."
                )
            seen[out_ref.address] = arm_index

    # An `align` group output is a NEW merged concept tying one column from each
    # arm. If its name reuses an arm output address, the two collapse to one
    # graph node (concepts are keyed by address) and codegen can't tell which
    # arm's CTE the merged dimension came from — it emits INVALID_REFERENCE_BUG.
    # Reject with the same actionable shape: give the align group its own name.
    for item in align_c.items:
        if item.aligned_concept in seen:
            short = item.alias
            members = ", ".join(c.address.split(".", 1)[-1] for c in item.concepts)
            raise InvalidSyntaxException(
                f"Multi-select align group '{short}' reuses an arm output name; "
                f"give the align group its own name, distinct from every arm "
                f"column (e.g. `align {short}_grp: {members}`)."
            )

    # A multi-select has no top-level WHERE: a pre-combination filter would have
    # to sit before the first arm (indistinguishable from that arm's own WHERE),
    # so per-arm WHERE is the only input filter. Post-combination filtering of
    # aligned/derived outputs is expressed with HAVING (below).
    limit_val: int | None = hydrate(limit_node).count if limit_node else None

    derived_concepts: list = []
    for x in align_c.items:
        concept = align_item_to_concept_v2(
            x,
            align_c,
            selects,
            where=None,
            having=None,
            limit=limit_val,
            context=context,
        )
        derived_concepts.append(concept)
        context.add_multiselect_concept(concept, meta=core_meta(node.meta))

    # DERIVE only builds the clause (no concept refs needed yet); register its
    # output concepts before hydrating HAVING / ORDER BY, which may reference
    # those derived outputs (e.g. `cnt_2000 <= cnt_1999`).
    derive: DeriveClause | None = hydrate(derive_node) if derive_node else None

    new_selects = [x.as_lineage(context.environment) for x in selects]
    multi_hidden: set[str] = set(y for x in new_selects for y in x.hidden_components)
    for item in align_c.items:
        if item.hidden:
            multi_hidden.add(item.aligned_concept)
    lineage = MultiSelectLineage(
        selects=new_selects,
        align=align_c,
        derive=derive,
        namespace=context.environment.namespace,
        having_clause=None,
        limit=limit_val,
        hidden_components=multi_hidden,
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
            concept = derive_item_to_concept_v2(
                derivation, name, lineage, context=context
            )
            derived_concepts.append(concept)
            context.add_multiselect_concept(concept, meta=core_meta(node.meta))

    # Hydrate after align- AND derive-derived concepts are registered so these
    # clauses can resolve them.
    having: HavingClause | None = hydrate(having_node) if having_node else None
    order_by_val: OrderBy | None = hydrate(order_by_node) if order_by_node else None
    return MultiSelectStatement(
        selects=selects,
        align=align_c,
        namespace=context.environment.namespace,
        having_clause=having,
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
