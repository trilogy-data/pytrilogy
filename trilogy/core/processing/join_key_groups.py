from trilogy.core.models.build_environment import BuildEnvironment


def axis_member_keys(address: str, environment: BuildEnvironment) -> frozenset[str]:
    """The keys of every member of the join key group `address` is the
    canonical of (`union join`, `subset join` or `merge` of two dimensions'
    attributes): each arm reaches the shared value through its own key."""
    members = environment.scoped_join_key_groups.get(address)
    if not members:
        return frozenset()
    out: set[str] = set()
    for member in members:
        concept = environment.alias_origin_lookup.get(
            member
        ) or environment.concepts.get(member)
        if concept is not None:
            out |= set(concept.keys or ())
    return frozenset(out)


def is_join_key_group(address: str, environment: BuildEnvironment) -> bool:
    """`address` canonicalizes attributes of two dimensions an authored
    relation merges: its value is the group's, not a property of one arm's
    key, so it is its own grain wherever that key is not read."""
    return len(axis_member_keys(address, environment)) > 1
