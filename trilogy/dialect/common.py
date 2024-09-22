from trilogy.core.models import Join, InstantiatedUnnestJoin, CTE, Concept, Datasource
from trilogy.core.enums import UnnestMode, Modifier
from typing import Optional, Callable


def null_wrapper(lval: str, rval: str, modifiers: list[Modifier]) -> str:
    if Modifier.NULLABLE in modifiers:
        return f"({lval} = {rval} or ({lval} is null and {rval} is null))"
    return f"{lval} = {rval}"


def render_unnest(
    unnest_mode: UnnestMode,
    quote_character: str,
    concept: Concept,
    render_func: Callable[[Concept, CTE, bool], str],
    cte: CTE,
):
    if unnest_mode == UnnestMode.CROSS_JOIN:
        return f"{render_func(concept, cte, False)} as {quote_character}{concept.safe_address}{quote_character}"
    return f"{render_func(concept, cte, False)} as unnest_wrapper ({quote_character}{concept.safe_address}{quote_character})"


def render_join(
    join: Join | InstantiatedUnnestJoin,
    quote_character: str,
    render_func: Optional[Callable[[Concept, CTE, bool], str]] = None,
    cte: Optional[CTE] = None,
    unnest_mode: UnnestMode = UnnestMode.CROSS_APPLY,
) -> str | None:
    # {% for key in join.joinkeys %}{{ key.inner }} = {{ key.outer}}{% endfor %}
    if isinstance(join, InstantiatedUnnestJoin):
        if unnest_mode == UnnestMode.DIRECT:
            return None
        if not render_func:
            raise ValueError("must provide a render function to build an unnest joins")
        if not cte:
            raise ValueError("must provide a cte to build an unnest joins")
        if unnest_mode == UnnestMode.CROSS_JOIN:
            return f"CROSS JOIN {render_unnest(unnest_mode, quote_character, join.concept_to_unnest, render_func, cte)}"
        if unnest_mode == UnnestMode.CROSS_JOIN_ALIAS:
            return f"CROSS JOIN {render_unnest(unnest_mode, quote_character, join.concept_to_unnest, render_func, cte)}"
        return f"FULL JOIN {render_unnest(unnest_mode, quote_character, join.concept_to_unnest, render_func, cte)}"
    left_name = join.left_name
    right_name = join.right_name
    right_base = join.right_ref
    base_joinkeys = [
        null_wrapper(
            f"{left_name}.{quote_character}{join.left_cte.get_alias(key.concept) if isinstance(join.left_cte, Datasource) else key.concept.safe_address}{quote_character}",
            f"{right_name}.{quote_character}{join.right_cte.get_alias(key.concept) if isinstance(join.right_cte, Datasource) else key.concept.safe_address}{quote_character}",
            modifiers=key.concept.modifiers or [],
        )
        for key in join.joinkeys
    ]
    if join.joinkey_pairs:
        base_joinkeys.extend(
            [
                null_wrapper(
                    f"{left_name}.{quote_character}{join.left_cte.get_alias(pair.left) if isinstance(join.left_cte, Datasource) else pair.left.safe_address}{quote_character}",
                    f"{right_name}.{quote_character}{join.right_cte.get_alias(pair.right) if isinstance(join.right_cte, Datasource) else pair.right.safe_address}{quote_character}",
                    modifiers=pair.modifiers
                    + (pair.left.modifiers or [])
                    + (pair.right.modifiers or []),
                )
                for pair in join.joinkey_pairs
            ]
        )
    if not base_joinkeys:
        base_joinkeys = ["1=1"]
    joinkeys = " AND ".join(base_joinkeys)
    return f"{join.jointype.value.upper()} JOIN {right_base} on {joinkeys}"
