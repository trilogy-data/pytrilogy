from trilogy.core.models.execute import CTE, Join, UnionCTE


def replace_parent(old: CTE, new: CTE, target: CTE | UnionCTE) -> None:
    """Replace old parent with new parent in target CTE's source map."""
    target.parent_ctes = [
        x for x in target.parent_ctes if x.safe_identifier != old.safe_identifier
    ] + [new]
    for k, v in target.source_map.items():
        if isinstance(v, list):
            new_sources = []
            for x in v:
                if x == old.safe_identifier:
                    new_sources.append(new.safe_identifier)
                else:
                    new_sources.append(x)
            target.source_map[k] = new_sources
    if not isinstance(target, CTE):
        return
    if target.base_alias_override == old.safe_identifier:
        target.base_alias_override = new.safe_identifier
    if target.base_name_override == old.safe_identifier:
        target.base_name_override = new.safe_identifier

    for join in target.joins:
        if not isinstance(join, Join):
            continue
        if join.left_cte and join.left_cte.safe_identifier == old.safe_identifier:
            join.left_cte = new
        if join.joinkey_pairs:
            for pair in join.joinkey_pairs:
                if pair.cte and pair.cte.safe_identifier == old.safe_identifier:
                    pair.cte = new
        if join.right_cte.safe_identifier == old.safe_identifier:
            join.right_cte = new


def is_sole_consumer(
    cte: CTE,
    parent: CTE,
    inverse_map: dict[str, list[CTE | UnionCTE]],
) -> bool:
    """Return True if cte is the only consumer of parent in the inverse map."""
    children = {c.name for c in inverse_map.get(parent.name, [])}
    return len(children) == 1 and cte.name in children


def repoint_consumers(
    old: CTE,
    new: CTE,
    inverse_map: dict[str, list[CTE | UnionCTE]],
) -> None:
    """Redirect all consumers of old to new and update the inverse map."""
    consumers = inverse_map.get(old.name, [])
    for child in consumers:
        replace_parent(old, new, child)
    if consumers:
        inverse_map[new.name] = inverse_map.get(new.name, []) + consumers
