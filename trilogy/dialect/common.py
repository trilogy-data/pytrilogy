from typing import Callable

from trilogy.core.enums import Modifier, UnnestMode
from trilogy.core.models.build import BuildConcept, BuildFunction
from trilogy.core.models.datasource import RawColumnExpr
from trilogy.core.models.execute import (
    CTE,
    InstantiatedUnnestJoin,
    Join,
)


def null_wrapper(lval: str, rval: str, modifiers: list[Modifier]) -> str:
    if Modifier.NULLABLE in modifiers:
        return f"({lval} = {rval} or ({lval} is null and {rval} is null))"
    return f"{lval} = {rval}"


def render_unnest(
    unnest_mode: UnnestMode,
    quote_character: str,
    concept: BuildConcept,
    render_func: Callable[[BuildConcept, CTE, bool], str],
    cte: CTE,
):
    if unnest_mode == UnnestMode.CROSS_JOIN:
        return f"{render_func(concept, cte, False)} as {quote_character}{concept.safe_address}{quote_character}"
    elif unnest_mode == UnnestMode.CROSS_JOIN_ALIAS:
        return f"{render_func(concept, cte, False)} as unnest_wrapper ({quote_character}{concept.safe_address}{quote_character})"
    elif unnest_mode == UnnestMode.SNOWFLAKE:
        # if we don't actually have a join, we're directly unnesting a concept, and we can skip the flatten
        if not cte.render_from_clause:
            return f"{render_func(concept, cte, False)} as unnest_wrapper ( unnest1, unnest2, unnest3, unnest4, {quote_character}{cte.join_derived_concepts[0].safe_address}{quote_character})"
        # otherwise, flatten the concept for the join
        return f"flatten({render_func(concept, cte, False)}) as unnest_wrapper ( unnest1, unnest2, unnest3, unnest4, {quote_character}{cte.join_derived_concepts[0].safe_address}{quote_character})"
    return f"{render_func(concept, cte, False)} as {quote_character}{concept.safe_address}{quote_character}"


def render_join_concept(
    name: str,
    quote_character: str,
    cte: CTE,
    concept: BuildConcept,
    render_expr,
    inlined_ctes: set[str],
):
    if cte.name in inlined_ctes:
        ds = cte.source.datasources[0]
        raw_content = ds.get_alias(concept)
        if isinstance(raw_content, RawColumnExpr):
            rval = raw_content.text
            return rval
        elif isinstance(raw_content, BuildFunction):
            rval = render_expr(raw_content, cte=cte)
            return rval
        return f"{name}.{quote_character}{raw_content}{quote_character}"
    return f"{name}.{quote_character}{concept.safe_address}{quote_character}"


def render_join(
    join: Join | InstantiatedUnnestJoin,
    quote_character: str,
    render_func: Callable[[BuildConcept, CTE, bool], str],
    render_expr_func: Callable[[BuildConcept, CTE], str],
    cte: CTE,
    unnest_mode: UnnestMode = UnnestMode.CROSS_APPLY,
) -> str | None:
    # {% for key in join.joinkeys %}{{ key.inner }} = {{ key.outer}}{% endfor %}
    if isinstance(join, InstantiatedUnnestJoin):
        if unnest_mode == UnnestMode.DIRECT:
            return None
        if not cte:
            raise ValueError("must provide a cte to build an unnest joins")
        if unnest_mode == UnnestMode.CROSS_JOIN:
            return f"CROSS JOIN {render_unnest(unnest_mode, quote_character, join.concept_to_unnest, render_func, cte)}"
        if unnest_mode == UnnestMode.CROSS_JOIN_ALIAS:
            return f"CROSS JOIN {render_unnest(unnest_mode, quote_character, join.concept_to_unnest, render_func, cte)}"
        if unnest_mode == UnnestMode.SNOWFLAKE:
            return f"LEFT JOIN LATERAL {render_unnest(unnest_mode, quote_character, join.concept_to_unnest, render_func, cte)}"
        return f"FULL JOIN {render_unnest(unnest_mode, quote_character, join.concept_to_unnest, render_func, cte)}"
    # left_name = join.left_name
    right_name = join.right_name
    if cte.quote_address.get(join.right_name, False):
        join.quote = quote_character
    right_base = join.right_ref
    base_joinkeys = []
    if join.joinkey_pairs:
        base_joinkeys.extend(
            [
                null_wrapper(
                    render_join_concept(
                        join.get_name(pair.cte),
                        quote_character,
                        pair.cte,
                        pair.left,
                        render_expr_func,
                        join.inlined_ctes,
                    ),
                    render_join_concept(
                        right_name,
                        quote_character,
                        join.right_cte,
                        pair.right,
                        render_expr_func,
                        join.inlined_ctes,
                    ),
                    modifiers=pair.modifiers
                    + (pair.left.modifiers or [])
                    + (pair.right.modifiers or []),
                )
                for pair in join.joinkey_pairs
            ]
        )
    if not base_joinkeys:
        base_joinkeys = ["1=1"]

    joinkeys = " AND ".join(sorted(base_joinkeys))
    return f"{join.jointype.value.upper()} JOIN {right_base} on {joinkeys}"
