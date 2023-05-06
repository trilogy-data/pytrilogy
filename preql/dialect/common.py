from preql.core.models import Join


def render_join(join: Join, quote_character: str = '"') -> str:
    # {% for key in join.joinkeys %}{{ key.inner }} = {{ key.outer}}{% endfor %}
    base_joinkeys = [
        f"{join.left_cte.name}.{quote_character}{key.concept.safe_address}{quote_character} ="
        f" {join.right_cte.name}.{quote_character}{key.concept.safe_address}{quote_character}"
        for key in join.joinkeys
    ]
    if not base_joinkeys:
        base_joinkeys = ["1=1"]
    joinkeys = " AND ".join(base_joinkeys)
    return f"{join.jointype.value.upper()} JOIN {join.right_cte.name} on {joinkeys}"
