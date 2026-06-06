from typing import Callable

from trilogy.core.constants import UNNEST_NAME
from trilogy.core.enums import Modifier, UnnestMode
from trilogy.core.models.build import (
    BoolExpr,
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
    if unnest_mode == UnnestMode.CROSS_JOIN:
        return f"{render_func(concept, cte)} as {quote_character}{address}{quote_character}"
    elif unnest_mode == UnnestMode.CROSS_JOIN_UNNEST:
        return f"unnest({render_func(concept, cte)}) as {quote_character}{address}{quote_character}"
    elif unnest_mode == UnnestMode.PRESTO:
        return f"unnest({render_func(concept, cte)}) as t({quote_character}{UNNEST_NAME}{quote_character})"
    elif unnest_mode == UnnestMode.CROSS_JOIN_ALIAS:
        return f"{render_func(concept, cte)} as unnest_wrapper ({quote_character}{address}{quote_character})"
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
    if unnest_mode in (
        UnnestMode.CROSS_JOIN,
        UnnestMode.CROSS_JOIN_UNNEST,
        UnnestMode.CROSS_JOIN_ALIAS,
        UnnestMode.PRESTO,
    ):
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


def _render_left_concept(
    pair: CTEConceptPair,
    join: Join,
    consumer: CTE | UnionCTE,
    quote_character: str,
    render_expr_func: Callable,
    use_map: dict[str, set[str]],
) -> str:
    node = join.authoritative(consumer, pair.cte)
    # A join key whose CTE resolves to the consumer itself is a local column —
    # it comes from the consumer's own FROM, not a joined CTE. Emitting
    # ``self_name.col`` is invalid SQL (the CTE can't reference itself); this
    # arises when an optimization merges the inner source that supplied the key
    # up into the consumer. Pin it to the consumer's FROM-base source (not the
    # generic concept render, which could pick the join's own right side and
    # produce a tautological ON).
    if isinstance(consumer, CTE) and node.name == consumer.name:
        base = next(
            (p for p in consumer.dependency_nodes() if p.name == consumer.base_alias),
            None,
        )
        if base is not None:
            return render_join_concept(
                join.name_for(consumer, base),
                quote_character,
                base,
                pair.left,
                consumer.column_for(base, pair.left),
                render_expr_func,
                use_map=use_map,
            )
        return render_expr_func(pair.left, consumer)
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
    null_wrapper: Callable[[str, str, list[Modifier]], str],
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
    null_wrapper: Callable[[str, str, list[Modifier]], str],
    unnest_mode: UnnestMode = UnnestMode.CROSS_APPLY,
) -> str | None:
    if isinstance(join, InstantiatedUnnestJoin):
        return _render_unnest_join(
            join, unnest_mode, quote_character, render_expr_func, cte
        )
    join.quote = quote_character
    joinkeys = " AND ".join(
        sorted(
            _build_joinkeys(
                join, cte, quote_character, render_expr_func, use_map, null_wrapper
            )
        )
    )
    right_ref = join.reference_for(cte, join.right_cte)
    base = f"{join.jointype.value.upper()} JOIN {right_ref} on {joinkeys}"
    if join.condition:
        base = f"{base} and {render_expr_func(join.condition, cte)}"
    return base
