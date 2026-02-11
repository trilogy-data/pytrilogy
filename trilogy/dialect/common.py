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
    # {% for key in join.joinkeys %}{{ key.inner }} = {{ key.outer}}{% endfor %}
    if isinstance(join, InstantiatedUnnestJoin):
        if unnest_mode == UnnestMode.DIRECT:
            return None
        if not cte:
            raise ValueError("must provide a cte to build an unnest joins")
        if unnest_mode in (
            UnnestMode.CROSS_JOIN,
            UnnestMode.CROSS_JOIN_UNNEST,
            UnnestMode.CROSS_JOIN_ALIAS,
            UnnestMode.PRESTO,
        ):
            return f"CROSS JOIN {render_unnest(unnest_mode, quote_character, join.object_to_unnest, render_expr_func, cte)}"
        if unnest_mode == UnnestMode.SNOWFLAKE:
            return f"LEFT JOIN LATERAL {render_unnest(unnest_mode, quote_character, join.object_to_unnest, render_expr_func, cte)}"
        return f"FULL JOIN {render_unnest(unnest_mode, quote_character, join.object_to_unnest, render_expr_func, cte)}"
    # left_name = join.left_name
    right_name = join.right_name
    join.quote = quote_character
    # if cte.quote_address.get(join.right_name, False):
    #     join.quote = quote_character
    right_base = join.right_ref
    base_joinkeys = []
    if join.joinkey_pairs:
        # Group pairs by right concept address to detect coalesce scenarios.
        # When multiple pairs share the same right concept but come from
        # different left CTEs, use COALESCE on the left values.
        right_groups: dict[str, list] = {}
        for pair in join.joinkey_pairs:
            key = pair.right.address
            if key not in right_groups:
                right_groups[key] = []
            right_groups[key].append(pair)

        for right_addr, pairs in right_groups.items():
            if len(pairs) > 1:
                right_render = render_join_concept(
                    right_name,
                    quote_character,
                    join.right_cte,
                    pairs[0].right,
                    render_expr_func,
                    join.inlined_ctes,
                    use_map=use_map,
                )
                # Sub-group by left address: same left concept from
                # different CTEs can be COALESCE'd when partial;
                # different left concepts are separate AND conditions.
                left_addr_groups: dict[str, list] = {}
                for pair in pairs:
                    left_addr_groups.setdefault(pair.left.address, []).append(
                        pair
                    )
                for left_addr, sub_pairs in left_addr_groups.items():
                    left_renders = [
                        render_join_concept(
                            join.get_name(p.cte),
                            quote_character,
                            p.cte,
                            p.left,
                            render_expr_func,
                            join.inlined_ctes,
                            use_map=use_map,
                        )
                        for p in sub_pairs
                    ]
                    unique_renders = list(dict.fromkeys(left_renders))
                    if len(unique_renders) > 1:
                        coalesced = f"coalesce({', '.join(unique_renders)})"
                        base_joinkeys.append(
                            f"{coalesced} = {right_render}"
                        )
                    else:
                        base_joinkeys.append(
                            null_wrapper(
                                unique_renders[0],
                                right_render,
                                sub_pairs[0].modifiers
                                + (sub_pairs[0].left.modifiers or [])
                                + (sub_pairs[0].right.modifiers or [])
                                + (join.modifiers or []),
                            )
                        )
            else:
                pair = pairs[0]
                base_joinkeys.append(
                    null_wrapper(
                        render_join_concept(
                            join.get_name(pair.cte),
                            quote_character,
                            pair.cte,
                            pair.left,
                            render_expr_func,
                            join.inlined_ctes,
                            use_map=use_map,
                        ),
                        render_join_concept(
                            right_name,
                            quote_character,
                            join.right_cte,
                            pair.right,
                            render_expr_func,
                            join.inlined_ctes,
                            use_map=use_map,
                        ),
                        pair.modifiers
                        + (pair.left.modifiers or [])
                        + (pair.right.modifiers or [])
                        + (join.modifiers or []),
                    )
                )
    if not base_joinkeys:
        base_joinkeys = ["1=1"]

    joinkeys = " AND ".join(sorted(base_joinkeys))
    base = f"{join.jointype.value.upper()} JOIN {right_base} on {joinkeys}"
    if join.condition:
        base = f"{base} and {render_expr_func(join.condition, cte)}"
    return base
