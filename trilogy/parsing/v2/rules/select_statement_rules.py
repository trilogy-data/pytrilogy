from __future__ import annotations

from trilogy.constants import DEFAULT_NAMESPACE
from trilogy.core.enums import (
    BooleanOperator,
    ComparisonOperator,
    ConceptSource,
    JoinType,
    Modifier,
)
from trilogy.core.models.author import (
    AggregateGrouping,
    Concept,
    ConceptRef,
    Conditional,
    Expr,
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
from trilogy.parsing.v2.errors import create_syntax_error
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
    grouping: AggregateGrouping | None = None
    join_clauses: list[SelectJoin] = []
    for arg in args:
        atype = type(arg)
        if atype is AggregateGrouping:
            grouping = arg
            continue
        if atype is list:
            # join_clause hydrates to list[SelectJoin]; select_list to
            # list[SelectItem]. Disambiguate on element type (both non-empty).
            if arg and isinstance(arg[0], SelectJoin):
                join_clauses.extend(arg)
            else:
                select_items = arg
        elif atype is Limit:
            limit = arg.count
        elif atype is OrderBy:
            order_by = arg
        elif atype is FromClause:
            from_clause_val = arg
        elif atype is WhereClause:
            # Agents commonly split row filters across a pre-`select` `where` and
            # a post-join `where`; both mean "filter input rows", so AND them into
            # one clause rather than erroring (post-aggregation filtering is
            # `having`, never a second `where`).
            if where is not None:
                where = WhereClause(
                    conditional=Conditional(
                        left=where.conditional,
                        right=arg.conditional,
                        operator=BooleanOperator.AND,
                    )
                )
            else:
                where = arg
        elif atype is HavingClause:
            having = arg
    if not select_items:
        raise fail(node, "Malformed select, missing select items")
    _validate_join_groups(node, join_clauses)
    return SelectStatement(
        selection=select_items,
        order_by=order_by,
        where_clause=where,
        having_clause=having,
        limit=limit,
        eligible_datasources=from_clause_val.sources if from_clause_val else None,
        join_clauses=join_clauses,
        grouping=grouping,
        meta=metadata_from_meta(node.meta),
    )


def _validate_join_groups(node: SyntaxNode, joins: list[SelectJoin]) -> None:
    """A query-scoped join collapses all `=`-related keys into one equivalence
    group (union-find). A FULL edge spans rows absent from either side, so it
    cannot coherently coexist with an INNER/LEFT edge on the SAME group (the
    INNER says "key must match" while the FULL says "key may be one-sided") — a
    FULL group must be entirely FULL. INNER and LEFT may mix freely (a shared
    anchor with some required and some optional sources is well-defined), and
    distinct (disjoint-key) groups may use any types."""
    parent: dict[str, str] = {}

    def find(x: str) -> str:
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for j in joins:
        rs, rt = find(j.source_address), find(j.target_address)
        if rs != rt:
            parent[rs] = rt
    types_by_group: dict[str, set[JoinType]] = {}
    for j in joins:
        types_by_group.setdefault(find(j.source_address), set()).add(j.join_type)
    for types in types_by_group.values():
        if JoinType.FULL in types and len(types) > 1:
            names = ", ".join(sorted(t.value for t in types))
            raise fail(
                node,
                f"Conflicting join types ({names}) on keys joined into one group: a "
                "FULL join cannot be mixed with another type on the same key (it is "
                "ambiguous whether the key is required or one-sided). Make the whole "
                "group FULL (e.g. `FULL JOIN a = b = c`), or use a distinct key.",
            )


def join_group(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> list[Expr]:
    """One `=`-chained key group (`a = b = c`) within a join clause. Each key is
    an arbitrary expression — a bare concept reference, arithmetic, an aggregate,
    a window, ... — not just a plain identifier. The keys are separated by
    COMPARISON_OPERATOR tokens (parsed positionally: keys at even indices,
    operators at odd); only `=` equality is allowed."""
    parts = [a for a in hydrated_children(node, hydrate) if a is not None]
    for op in parts[1::2]:
        if op != ComparisonOperator.EQ:
            # A non-`=` operator in a join group is a key inequality (unsupported)
            # or, far more commonly, a filter mistakenly chained onto the join
            # (`... join a = b and amount > 0`). Surface the same guidance the
            # parse-level "clause after join" path gives: filters belong in WHERE.
            raise create_syntax_error(220, node.start_pos or 0, context.source_text)
    return parts[0::2]


def join_clause(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> list[SelectJoin]:
    args = hydrated_children(node, hydrate)
    join_type = next(a for a in args if isinstance(a, JoinType))
    if join_type not in (JoinType.INNER, JoinType.LEFT_OUTER, JoinType.FULL):
        raise fail(
            node,
            f"`{join_type.value}` join is not yet supported in query-scoped joins;"
            " use INNER, LEFT, or FULL",
        )
    # Each `join_group` is one `=`-chained equivalence group. Multiple groups
    # arise from the `a = b and c = d` sugar, which is exactly equivalent to two
    # separate `JOIN_TYPE join` clauses (distinct groups, same type).
    joins: list[SelectJoin] = []
    for keys in (a for a in args if isinstance(a, list)):
        joins.extend(_resolve_join_group(node, context, join_type, keys))
    return joins


def _resolve_join_group(
    node: SyntaxNode,
    context: RuleContext,
    join_type: JoinType,
    keys: list[Expr],
) -> list[SelectJoin]:
    # Positional direction: the first key is the anchor, each subsequent key is a
    # brought-in (source) concept. `a = b = c` chains into ONE equivalence group:
    # adjacent pairs (a,b),(b,c) all share this join type.
    resolved: list[tuple[str, str]] = [
        _resolve_join_key(node, context, key) for key in keys
    ]
    # A `=` chain binds ONE logical key equal across DISTINCT sources
    # (`a.k = b.k = base.k`); an import/rowset source may appear at most once. Two
    # bare-concept keys from the same source are a composite key mistakenly folded
    # into one chain (`web.item = cat.item = web.cust = cat.cust`): the adjacency
    # pairing below would then emit a cross-concept garbage pair
    # (`cat.item = web.cust`) that downstream silently drops, collapsing the join
    # toward a cross product. Reject it and point at the composite-key syntax. Only
    # meaningful for 3+ keys — a 2-key chain is a single explicit pair (incl. a
    # self-join). The address root identifies the source only for namespaced
    # imports/rowsets; bare top-level concepts all share `local`, so skip that root
    # (`ka = kb = kc` are distinct keys from distinct datasources).
    if len(keys) >= 3:
        seen_roots: dict[str, str] = {}
        for (addr, label), key in zip(resolved, keys):
            if not isinstance(key, ConceptRef):
                continue
            root = addr.split(".", 1)[0]
            if root == DEFAULT_NAMESPACE:
                continue
            if root in seen_roots:
                raise fail(
                    node,
                    f"Join chain repeats source `{root}` (keys "
                    f"`{seen_roots[root]}` and `{label}`). A `=` chain joins ONE "
                    "key across distinct sources; join a composite key with `and` "
                    "or separate clauses (e.g. `a.k1 = b.k1 and a.k2 = b.k2`).",
                )
            seen_roots[root] = label
    joins: list[SelectJoin] = []
    for (la, ll), (ra, rl) in zip(resolved, resolved[1:]):
        if la == ra:
            raise fail(
                node,
                f"Cannot join `{ll}` to itself (`{rl}` resolves to the same key "
                f"`{la}`), which degenerates to `1=1`. Join distinct keys (e.g. "
                "separate rowset outputs or distinct expressions).",
            )
        joins.append(
            SelectJoin(
                join_type=join_type,
                source_address=la,
                target_address=ra,
            )
        )
    return joins


def _resolve_join_key(
    node: SyntaxNode,
    context: RuleContext,
    key: Expr,
) -> tuple[str, str]:
    """Resolve a single join key to ``(address, label)``. A bare concept
    reference resolves to its existing concept; any other expression is
    materialized as an anonymous (virtual) concept — mirroring how select and
    order-by expressions become concepts — so the build phase and discovery
    source it like any other derived concept. ``label`` is used only for error
    messages."""
    if isinstance(key, ConceptRef):
        concept = context.concepts.require(key.address)
        if isinstance(concept, (UndefinedConcept, UndefinedConceptFull)):
            # Join keys are concept literals, so they are pre-declared as scoped
            # placeholders and resolve to a non-durable UndefinedConceptFull
            # rather than raising during hydration. Surface the same undefined
            # error the rest of the parser uses — via ConceptLookup so staged
            # named-statement outputs feed the suggestions.
            context.concepts._raise_undefined(key.address)
        return concept.address, key.address
    anon = arbitrary_to_concept_v2(key, context=context)
    context.add_virtual_concept(anon, meta=core_meta(node.meta))
    return anon.address, str(key)


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
    SyntaxNodeKind.JOIN_GROUP: join_group,
    SyntaxNodeKind.SELECT_ITEM: select_item,
    SyntaxNodeKind.SELECT_TRANSFORM: select_transform,
    SyntaxNodeKind.SELECT_LIST: select_list,
    SyntaxNodeKind.SELECT_HIDE_MODIFIER: select_hide_modifier,
    SyntaxNodeKind.SELECT_PARTIAL_MODIFIER: select_partial_modifier,
    SyntaxNodeKind.FROM_CLAUSE: from_clause,
}
