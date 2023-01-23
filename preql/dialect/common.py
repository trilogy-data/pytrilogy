from typing import List

from preql.core.models import CTE
from preql.core.models import Join, OrderItem


def render_join(join: Join, quote_character: str = '"') -> str:
    # {% for key in join.joinkeys %}{{ key.inner }} = {{ key.outer}}{% endfor %}
    joinkeys = " AND ".join(
        [
            f"{join.left_cte.name}.{quote_character}{key.concept.safe_address}{quote_character} = {join.right_cte.name}.{quote_character}{key.concept.safe_address}{quote_character}"
            for key in join.joinkeys
        ]
    )
    return f"{join.jointype.value.upper()} JOIN {join.right_cte.name} on {joinkeys}"
