from preql.core.models import Join, InstantiatedUnnestJoin


def render_join(join: Join | InstantiatedUnnestJoin, render_function) -> str:
    # {% for key in join.joinkeys %}{{ key.inner }} = {{ key.outer}}{% endfor %}
    if isinstance(join, InstantiatedUnnestJoin):
        return f'{render_function(join.concept, join.cte)} AS {join.alias}'

    base_joinkeys = [
        f"{join.left_cte.name}.{render_function(key.concept, join.left_cte)} ="
        f" {join.right_cte.name}.{render_function(key.concept, join.right_cte)}"
        for key in join.joinkeys
    ]
    if not base_joinkeys:
        base_joinkeys = ["1=1"]
    joinkeys = " AND ".join(base_joinkeys)
    return f"{join.jointype.value.upper()} JOIN {join.right_cte.name} on {joinkeys}"
