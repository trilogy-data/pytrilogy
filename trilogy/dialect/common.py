from collections.abc import Callable

from trilogy.core.constants import UNNEST_NAME
from trilogy.core.enums import JoinType, Modifier, UnnestMode
from trilogy.core.models.build import (
    BoolExpr,
    BuildAggregateWrapper,
    BuildConcept,
    BuildDatasource,
    BuildFunction,
    BuildParamaterizedConceptReference,
)
from trilogy.core.models.execute import (
    CTE,
    ConceptPair,
    CTEConceptPair,
    InstantiatedUnnestJoin,
    Join,
    UnionCTE,
    _datasource_column_for_concept,
)

# Renders one join key. The join type is passed because dialects may restrict
# what a given join's ON clause can contain (BigQuery, FULL joins).
NullWrapper = Callable[[str, str, list[Modifier], JoinType], str]


def render_unnest(
    unnest_mode: UnnestMode,
    quote_character: str,
    concept: BuildConcept | BuildParamaterizedConceptReference | BuildFunction,
    render_func: Callable[
        [BuildConcept | BuildParamaterizedConceptReference | BuildFunction, CTE], str
    ],
    cte: CTE,
):
    if not isinstance(concept, (BuildConcept, BuildParamaterizedConceptReference)):
        address = UNNEST_NAME
    else:
        address = concept.safe_address
    if unnest_mode == UnnestMode.CROSS_JOIN_UNNEST:
        return f"unnest({render_func(concept, cte)}) as {quote_character}{address}{quote_character}"
    elif unnest_mode == UnnestMode.PRESTO:
        return f"unnest({render_func(concept, cte)}) as t({quote_character}{UNNEST_NAME}{quote_character})"
    elif unnest_mode == UnnestMode.SNOWFLAKE:
        # if we don't actually have a join, we're directly unnesting a concept, and we can skip the flatten
        if not cte.render_from_clause:
            return f"{render_func(concept, cte)} as unnest_wrapper ( unnest1, unnest2, unnest3, unnest4, {quote_character}{cte.join_derived_concepts[0].safe_address}{quote_character})"
        # otherwise, flatten the concept for the join
        return f"flatten({render_func(concept, cte)}) as unnest_wrapper ( unnest1, unnest2, unnest3, unnest4, {quote_character}{cte.join_derived_concepts[0].safe_address}{quote_character})"
    return f"{render_func(concept, cte)} as {quote_character}{address}{quote_character}"


def render_join_concept(
    name: str,
    quote_character: str,
    node: CTE | UnionCTE,
    concept: BuildConcept,
    col,
    render_expr,
    use_map: dict[str, set[str]],
):
    # ``name`` is the consumer-resolved alias; ``col`` is the consumer-scoped
    # column (safe_address for a normal parent, raw column when folded in).
    # Non-str columns (computed/raw expressions) render via expr.
    if isinstance(col, str):
        use_map[name].add(concept.address)
        return f"{quote_character}{name}{quote_character}.{quote_character}{col}{quote_character}"
    # A folded datasource may return the side-appropriate derivation (a
    # cross-namespace merge key it computes from its own columns) rather than the
    # merged concept, whose own lineage points at the other namespace. Render the
    # returned expression in that case; otherwise render the concept.
    if isinstance(col, (BuildFunction, BuildAggregateWrapper)):
        return render_expr(col, node)
    return render_expr(concept, node)


def _render_unnest_join(
    join: InstantiatedUnnestJoin,
    unnest_mode: UnnestMode,
    quote_character: str,
    render_expr_func: Callable,
    cte: CTE,
) -> str | None:
    if unnest_mode == UnnestMode.DIRECT:
        return None
    if not cte:
        raise ValueError("must provide a cte to build an unnest joins")
    unnest_clause = render_unnest(
        unnest_mode, quote_character, join.object_to_unnest, render_expr_func, cte
    )
    if unnest_mode in (UnnestMode.CROSS_JOIN_UNNEST, UnnestMode.PRESTO):
        return f"CROSS JOIN {unnest_clause}"
    if unnest_mode == UnnestMode.SNOWFLAKE:
        return f"LEFT JOIN LATERAL {unnest_clause}"
    return f"FULL JOIN {unnest_clause}"


def _collect_modifiers(pair: ConceptPair, join: Join) -> list[Modifier]:
    return (
        pair.modifiers
        + (pair.left.modifiers or [])
        + (pair.right.modifiers or [])
        + (join.modifiers or [])
    )


def _renders_in_from(consumer: CTE, join: Join, node: CTE | UnionCTE) -> bool:
    """Whether ``node`` is the consumer's base or the right side of one of its
    joins. A join rendered outside the consumer's own join list has no FROM
    scope to check against and is accepted as is."""
    if not any(j is join for j in consumer.joins):
        return True
    alias = join.name_for(consumer, node)
    return alias == consumer.base_alias or any(
        isinstance(j, Join) and join.name_for(consumer, j.right_cte) == alias
        for j in consumer.joins
    )


def _render_left_concept(
    pair: CTEConceptPair,
    join: Join,
    consumer: CTE | UnionCTE,
    quote_character: str,
    render_expr_func: Callable,
    use_map: dict[str, set[str]],
) -> str:
    node = join.authoritative(consumer, pair.cte)
    if join.left_is_local:
        # LHS key is the rendering branch's own base column (no self-alias).
        # If the key also resolves through a hoisted dim, the generic concept
        # render would COALESCE the fact FK with the dim's own key into a
        # tautological ON clause (cross join). Pin the LHS to its own
        # left-base datasource column in that case.
        ds = pair.existing_datasource
        sources = (
            consumer.source_map.get(pair.left.address)
            if isinstance(consumer, CTE)
            else None
        )
        if isinstance(ds, BuildDatasource) and sources and len(sources) > 1:
            col = _datasource_column_for_concept(ds, pair.left)
            if isinstance(col, str):
                use_map[ds.safe_identifier].add(pair.left.address)
                return (
                    f"{quote_character}{ds.safe_identifier}{quote_character}"
                    f".{quote_character}{col}{quote_character}"
                )
        return render_expr_func(pair.left, consumer)
    if isinstance(consumer, CTE) and not _renders_in_from(consumer, join, node):
        raise ValueError(
            f"Join key {pair.left.address} of {consumer.name} references {node.name},"
            f" which is not in its FROM scope (base {consumer.base_alias})"
        )
    col = (
        consumer.column_for(node, pair.left)
        if isinstance(consumer, CTE)
        else pair.left.safe_address
    )
    return render_join_concept(
        join.name_for(consumer, node),
        quote_character,
        node,
        pair.left,
        col,
        render_expr_func,
        use_map=use_map,
    )


def _render_right_concept(
    pair: ConceptPair,
    join: Join,
    consumer: CTE | UnionCTE,
    quote_character: str,
    render_expr_func: Callable,
    use_map: dict[str, set[str]],
) -> str:
    node = join.authoritative(consumer, join.right_cte)
    col = (
        consumer.column_for(node, pair.right)
        if isinstance(consumer, CTE)
        else pair.right.safe_address
    )
    return render_join_concept(
        join.name_for(consumer, node),
        quote_character,
        node,
        pair.right,
        col,
        render_expr_func,
        use_map=use_map,
    )


def _build_joinkeys(
    join: Join,
    consumer: CTE | UnionCTE,
    quote_character: str,
    render_expr_func: Callable,
    use_map: dict[str, set[str]],
    null_wrapper: NullWrapper,
) -> list[str]:
    if not join.joinkey_pairs:
        return ["1=1"]
    # Group pairs by right concept address to detect coalesce scenarios.
    # When multiple pairs share the same right concept but come from
    # different left CTEs, use COALESCE on the left values.
    right_groups: dict[str, list] = {}
    for pair in join.joinkey_pairs:
        right_groups.setdefault(pair.right.address, []).append(pair)

    result: list[str] = []
    for pairs in right_groups.values():
        right_render = _render_right_concept(
            pairs[0], join, consumer, quote_character, render_expr_func, use_map
        )
        if join.jointype in (
            JoinType.LEFT_OUTER,
            JoinType.RIGHT_OUTER,
            JoinType.FULL,
        ):
            left_renders = [
                _render_left_concept(
                    p, join, consumer, quote_character, render_expr_func, use_map
                )
                for p in pairs
            ]
            unique_renders = list(dict.fromkeys(left_renders))
            if len(unique_renders) > 1:
                coalesced = f"coalesce({', '.join(unique_renders)})"
                result.append(
                    null_wrapper(
                        coalesced,
                        right_render,
                        [
                            modifier
                            for pair in pairs
                            for modifier in _collect_modifiers(pair, join)
                        ],
                        join.jointype,
                    )
                )
                continue
        # Sub-group by left address: same left concept from different CTEs
        # can be COALESCE'd; different left concepts are separate AND conditions.
        left_addr_groups: dict[str, list] = {}
        for pair in pairs:
            left_addr_groups.setdefault(pair.left.address, []).append(pair)

        for sub_pairs in left_addr_groups.values():
            left_renders = [
                _render_left_concept(
                    p, join, consumer, quote_character, render_expr_func, use_map
                )
                for p in sub_pairs
            ]
            unique_renders = list(dict.fromkeys(left_renders))
            if len(unique_renders) > 1:
                coalesced = f"coalesce({', '.join(unique_renders)})"
                result.append(f"{coalesced} = {right_render}")
            else:
                result.append(
                    null_wrapper(
                        unique_renders[0],
                        right_render,
                        _collect_modifiers(sub_pairs[0], join),
                        join.jointype,
                    )
                )
    return result or ["1=1"]


def render_join(
    join: Join | InstantiatedUnnestJoin,
    quote_character: str,
    render_expr_func: Callable[
        [
            BuildConcept
            | BuildParamaterizedConceptReference
            | BuildFunction
            | BoolExpr,
            CTE,
        ],
        str,
    ],
    cte: CTE,
    use_map: dict[str, set[str]],
    null_wrapper: NullWrapper,
    unnest_mode: UnnestMode = UnnestMode.CROSS_APPLY,
) -> str | None:
    if isinstance(join, InstantiatedUnnestJoin):
        return _render_unnest_join(
            join, unnest_mode, quote_character, render_expr_func, cte
        )
    joinkeys = " AND ".join(
        sorted(
            _build_joinkeys(
                join, cte, quote_character, render_expr_func, use_map, null_wrapper
            )
        )
    )
    right_ref = join.reference_for(cte, join.right_cte, quote_character)
    base = f"{join.jointype.value.upper()} JOIN {right_ref} on {joinkeys}"
    if join.condition:
        base = f"{base} and {render_expr_func(join.condition, cte)}"
    return base
