from typing import Callable

from trilogy.core.constants import UNNEST_NAME
from trilogy.core.enums import Modifier, UnnestMode
from trilogy.core.models.build import (
    BuildComparison,
    BuildConcept,
    BuildConditional,
    BuildFunction,
    BuildParamaterizedConceptReference,
    BuildParenthetical,
)
from trilogy.core.models.execute import (
    CTE,
    ConceptPair,
    CTEConceptPair,
    InstantiatedUnnestJoin,
    Join,
    UnionCTE,
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
    cte: CTE | UnionCTE,
    concept: BuildConcept,
    render_expr,
    inlined_ctes: set[str],
    use_map: dict[str, set[str]],
):
    if cte.name in inlined_ctes:
        base = render_expr(concept, cte)
        return base
    use_map[name].add(concept.address)
    return f"{quote_character}{name}{quote_character}.{quote_character}{concept.safe_address}{quote_character}"


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
    quote_character: str,
    render_expr_func: Callable,
    use_map: dict[str, set[str]],
) -> str:
    return render_join_concept(
        join.get_name(pair.cte),
        quote_character,
        pair.cte,
        pair.left,
        render_expr_func,
        join.inlined_ctes,
        use_map=use_map,
    )


def _render_right_concept(
    pair: ConceptPair,
    join: Join,
    quote_character: str,
    render_expr_func: Callable,
    use_map: dict[str, set[str]],
) -> str:
    return render_join_concept(
        join.right_name,
        quote_character,
        join.right_cte,
        pair.right,
        render_expr_func,
        join.inlined_ctes,
        use_map=use_map,
    )


def _build_joinkeys(
    join: Join,
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
            pairs[0], join, quote_character, render_expr_func, use_map
        )
        # Sub-group by left address: same left concept from different CTEs
        # can be COALESCE'd; different left concepts are separate AND conditions.
        left_addr_groups: dict[str, list] = {}
        for pair in pairs:
            left_addr_groups.setdefault(pair.left.address, []).append(pair)

        for sub_pairs in left_addr_groups.values():
            left_renders = [
                _render_left_concept(
                    p, join, quote_character, render_expr_func, use_map
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
            | BuildConditional
            | BuildComparison
            | BuildParenthetical,
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
                join, quote_character, render_expr_func, use_map, null_wrapper
            )
        )
    )
    base = f"{join.jointype.value.upper()} JOIN {join.right_ref} on {joinkeys}"
    if join.condition:
        base = f"{base} and {render_expr_func(join.condition, cte)}"
    return base
