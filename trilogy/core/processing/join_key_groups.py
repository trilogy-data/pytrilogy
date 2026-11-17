from collections.abc import Mapping

from trilogy.core.domain_graph import DomainRelation, EdgeProvenance
from trilogy.core.models.build_environment import BuildEnvironment


def axis_member_keys(address: str, environment: BuildEnvironment) -> frozenset[str]:
    """The keys of every member of the join key group `address` is the
    canonical of (`union join`, `subset join` or `merge` of two dimensions'
    attributes): each arm reaches the shared value through its own key."""
    members = environment.scoped_join_key_groups.get(address)
    if not members:
        return frozenset()
    # Imported lazily: the v4 helper package imports join resolution.
    from trilogy.core.processing.v4_helper.functional_dependency import row_keys

    out: set[str] = set()
    for member in members:
        concept = environment.alias_origin_lookup.get(
            member
        ) or environment.concepts.get(member)
        if concept is not None:
            out |= row_keys(concept, environment)
    return frozenset(out)


def is_join_key_group(address: str, environment: BuildEnvironment) -> bool:
    """`address` canonicalizes attributes of two dimensions an authored
    relation merges: its value is the group's, not a property of one arm's
    key, so it is its own grain wherever that key is not read."""
    return len(axis_member_keys(address, environment)) > 1


def authored_edges(environment: BuildEnvironment) -> dict[frozenset[str], str | None]:
    """Each declared `subset`/`union` relation's two members, with the member
    holding the other's every value (a `subset` anchor; none for `union`)."""
    out: dict[frozenset[str], str | None] = {}
    for edge in environment.domain_graph.edges:
        if edge.provenance is not EdgeProvenance.DECLARED:
            continue
        if edge.relation is DomainRelation.SUBSET:
            out[frozenset({edge.source, edge.target})] = edge.target
        elif edge.relation is DomainRelation.INCOMPARABLE:
            out[frozenset({edge.source, edge.target})] = None
    return out


_Edges = Mapping[frozenset[str], str | None]


def _linked(edges: _Edges, a: frozenset[str], b: frozenset[str]) -> bool:
    return any(frozenset({x, y}) in edges for x in a for y in b)


def _anchors(edges: _Edges, via: frozenset[str], other: frozenset[str]) -> bool:
    """Some member of `via` is the anchor of a relation to one of `other`."""
    return any(edges.get(frozenset({x, y}), "") == x for x in via for y in other)


def without_transits(
    keys: Mapping[frozenset[str], frozenset[str]],
    datasources: Mapping[str, str],
    environment: BuildEnvironment,
) -> dict[frozenset[str], frozenset[str]]:
    """`keys` (side pair -> what it pairs on) less each authored key group two
    sides share only through a third that lacks values: `ss ⋈ sr` and
    `ss ⋈ cs` on customer say nothing of `cs ⋈ sr`, whose outer join pads
    before `ss` arrives. Through an anchor of both (`ss ⊑ customer`,
    `cs ⊑ customer`) the pairing is implied: the anchor holds every value.
    `datasources` names the datasource each side scans."""
    edges = authored_edges(environment)
    if not edges:
        return dict(keys)
    graph = environment.domain_graph
    members = frozenset().union(*edges)
    bound = {
        side: frozenset(m for m in members if ds in graph.binding_sources(m))
        for side, ds in datasources.items()
    }

    out = dict(keys)
    for pair in keys:
        if len(pair) != 2 or not pair <= bound.keys():
            continue
        left, right = sorted(pair)
        for group in environment.scoped_join_relations():
            mine, theirs = bound[left] & group, bound[right] & group
            if not mine or not theirs or _linked(edges, mine, theirs):
                continue
            vias = [
                via
                for other in bound.keys() - pair
                if _linked(edges, mine, via := bound[other] & group)
                and _linked(edges, theirs, via)
            ]
            if vias and not any(
                _anchors(edges, via, mine) and _anchors(edges, via, theirs)
                for via in vias
            ):
                out[pair] = out[pair] - group
    return out
