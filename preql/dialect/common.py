from preql.core.models import Join, InstantiatedUnnestJoin, CTE, Concept
from preql.core.enums import UnnestMode
from typing import Optional, Callable


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
            raise ValueError("must provide a render func to build an unnest joins")
        if not cte:
            raise ValueError("must provide a cte to build an unnest joins")
        if unnest_mode == UnnestMode.CROSS_JOIN:
            return f"CROSS JOIN {render_func(join.concept, cte, False)} as {quote_character}{join.concept.safe_address}{quote_character}"

        return f"FULL JOIN {render_func(join.concept, cte, False)} as unnest_wrapper({quote_character}{join.concept.safe_address}{quote_character})"

    base_joinkeys = [
        f"{join.left_cte.name}.{quote_character}{key.concept.safe_address}{quote_character} ="
        f" {join.right_cte.name}.{quote_character}{key.concept.safe_address}{quote_character}"
        for key in join.joinkeys
    ]
    if not base_joinkeys:
        base_joinkeys = ["1=1"]
    joinkeys = " AND ".join(base_joinkeys)
    return f"{join.jointype.value.upper()} JOIN {join.right_cte.name} on {joinkeys}"
