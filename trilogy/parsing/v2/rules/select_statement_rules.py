from __future__ import annotations

from trilogy.constants import DEFAULT_NAMESPACE
from trilogy.core.enums import ConceptSource, JoinType, Modifier
from trilogy.core.models.author import (
    Concept,
    HavingClause,
    Metadata,
    OrderBy,
    UndefinedConcept,
    UndefinedConceptFull,
    WhereClause,
)
from trilogy.core.statements.author import (
    ConceptTransform,
    FromClause,
    Limit,
    SelectItem,
    SelectJoin,
    SelectStatement,
)
from trilogy.parsing.v2.concept_factory import (
    arbitrary_to_concept_v2,
    unwrap_transformation_v2,
)
from trilogy.parsing.v2.rules.concept_rules import (
    metadata_from_meta,
    parse_concept_reference,
)
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
    from_clause_val: FromClause | None = None
    where: WhereClause | None = None
    having: HavingClause | None = None
    join_clauses: list[SelectJoin] = []
    for arg in args:
        atype = type(arg)
        if atype is list:
            select_items = arg
        elif atype is Limit:
            limit = arg.count
        elif atype is OrderBy:
            order_by = arg
        elif atype is FromClause:
            from_clause_val = arg
        elif atype is SelectJoin:
            join_clauses.append(arg)
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
        eligible_datasources=from_clause_val.sources if from_clause_val else None,
        join_clauses=join_clauses,
        meta=metadata_from_meta(node.meta),
    )


def join_clause(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> SelectJoin:
    args = hydrated_children(node, hydrate)
    join_type = next(a for a in args if isinstance(a, JoinType))
    if join_type not in (JoinType.INNER, JoinType.LEFT_OUTER):
        raise fail(
            node,
            f"`{join_type.value}` join is not yet supported in query-scoped joins;"
            " use INNER or LEFT",
        )
    idents = [a for a in args if isinstance(a, str)]
    model, left_key, right_key = idents[0], idents[1], idents[2]
    resolved: dict[str, Concept] = {}
    for key in (left_key, right_key):
        concept = context.concepts.require(key)
        if isinstance(concept, (UndefinedConcept, UndefinedConceptFull)):
            raise fail(node, f"Join key `{key}` does not exist")
        resolved[key] = concept
    model_keys = [k for k, c in resolved.items() if c.namespace == model]
    if len(model_keys) != 1:
        raise fail(
            node,
            f"Join `on` must reference exactly one key from joined model `{model}`"
            " and one anchor key from the surrounding query",
        )
    source = model_keys[0]
    target = right_key if source == left_key else left_key
    return SelectJoin(
        join_type=join_type,
        source_address=resolved[source].address,
        target_address=resolved[target].address,
        namespace=model,
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


def _existing_select_definition_target(
    context: RuleContext,
    address: str,
) -> Concept | None:
    # Side-effect-free lookup: avoid ConceptLookup.get(), which can auto-derive
    # or stage placeholders. We only want to know whether this address is
    # already bound to a durable or pending concept.
    pending = context.semantic_state.pending_lookup(address)
    if pending is not None:
        return pending
    return context.environment.concepts.data.get(address)


def select_transform(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> ConceptTransform:
    args = hydrated_children(node, hydrate)
    content_args = [a for a in args if not isinstance(a, str) or a not in ("->", "as")]
    expr_val = content_args[0]
    output_name: str = content_args[1]
    metadata: Metadata | None = content_args[2] if len(content_args) > 2 else None
    transformation = unwrap_transformation_v2(expr_val, context)
    _, namespace, output_name, _ = parse_concept_reference(
        output_name, context.environment
    )
    # A SELECT alias declared inside a rowset body is private to that
    # rowset. Give it a hidden, per-rowset name so two rowsets aliasing
    # different expressions to the same user-facing name don't collide in
    # the shared default namespace. Only mangle when the alias landed in
    # the default namespace (an explicit `expr -> other.foo` keeps its
    # namespace untouched). `_rowset_concept` strips this prefix to
    # recover the user-facing rowset-output name.
    rowset_name = context.semantic_state.current_rowset_name
    default_ns = context.environment.namespace or DEFAULT_NAMESPACE
    if rowset_name is not None and namespace == default_ns:
        output_name = context.semantic_state.mangle_rowset_alias(
            rowset_name, output_name
        )
    meta = metadata_from_meta(node.meta, concept_source=ConceptSource.SELECT)
    if metadata:
        meta = metadata
    concept = arbitrary_to_concept_v2(
        transformation,
        context=context,
        namespace=namespace,
        name=output_name,
        metadata=meta,
    )
    existing = _existing_select_definition_target(context, concept.address)
    if (
        existing is None
        or isinstance(existing, (UndefinedConcept, UndefinedConceptFull))
        or (
            existing.metadata
            and existing.metadata.concept_source == ConceptSource.SELECT
        )
    ):
        context.add_select_concept(concept, meta=core_meta(node.meta))
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


SELECT_STATEMENT_NODE_HYDRATORS: dict[SyntaxNodeKind, NodeHydrator] = {
    SyntaxNodeKind.SELECT_STATEMENT: select_statement,
    SyntaxNodeKind.JOIN_CLAUSE: join_clause,
    SyntaxNodeKind.SELECT_ITEM: select_item,
    SyntaxNodeKind.SELECT_TRANSFORM: select_transform,
    SyntaxNodeKind.SELECT_LIST: select_list,
    SyntaxNodeKind.SELECT_HIDE_MODIFIER: select_hide_modifier,
    SyntaxNodeKind.SELECT_PARTIAL_MODIFIER: select_partial_modifier,
    SyntaxNodeKind.FROM_CLAUSE: from_clause,
}
