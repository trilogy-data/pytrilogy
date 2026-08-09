from collections.abc import Callable, Iterable, Sequence
from itertools import combinations
from typing import (
    NamedTuple,
    cast,
)

from trilogy.constants import logger
from trilogy.core import graph as nx
from trilogy.core.domain_graph import DomainRelation, EdgeProvenance
from trilogy.core.enums import Derivation, Purpose
from trilogy.core.graph_models import ReferenceGraph, concept_to_node
from trilogy.core.models.build import (
    BuildAggregateWrapper,
    BuildConcept,
    BuildDatasource,
    BuildFilterItem,
    BuildFunction,
    BuildGrain,
    BuildUnionDatasource,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.utility import unique

AGGREGATE_TYPES = (BuildAggregateWrapper,)
FUNCTION_TYPES = (BuildFunction,)
PROPERTY_PURPOSES = (Purpose.PROPERTY, Purpose.UNIQUE_PROPERTY)


def _union_key_siblings(
    concept: BuildConcept, environment: BuildEnvironment
) -> list[BuildConcept]:
    """Sibling `union(...)` concepts that stack a key for every arm of this
    union — e.g. `all_k <- union(k1, k2)` beside `all_amt <- union(amt, pad)`.
    Such a sibling is the stacked row identity of this union's output."""
    if not isinstance(concept.lineage, BuildFunction):
        return []
    arms = concept.lineage.concept_arguments
    out: list[BuildConcept] = []
    for other in environment.concepts.values():
        if other.address == concept.address or other.derivation != Derivation.UNION:
            continue
        if not isinstance(other.lineage, BuildFunction):
            continue
        other_args = {x.address for x in other.lineage.concept_arguments}
        if all(
            a.address in other_args or (a.keys and set(a.keys) & other_args)
            for a in arms
        ):
            out.append(other)
    return unique(out, "address")


def _walk_aggregate_grain_inputs(
    concept: BuildConcept,
    environment: BuildEnvironment,
    seen: set[str] | None = None,
) -> list[BuildConcept]:
    """Collect row-identity concepts an aggregate needs from its arg's
    upstream — without crossing a row-identity boundary.

    Each concept defines its own row identity if it is:
      - a rowset (row identity = its declared grain)
      - a property with keys (row identity = its keys)

    Walks through grain-preserving wrappers to find the row identity, then
    stops:
      - FilterItem: walk only ``content`` (the value being filtered defines
        row identity; ``where`` predicates do not)
      - Function (BASIC): walk all concept args (a row-level expression
        inherits row identity from its inputs)
      - AGGREGATE / ROWSET: do not descend (the inner aggregate has already
        collapsed its upstream rows to its own ``by`` grain; a rowset
        defines a fresh row identity we've already captured)"""
    seen = seen if seen is not None else set()
    if concept.address in seen:
        return []
    seen.add(concept.address)

    if concept.derivation == Derivation.AGGREGATE:
        return []
    if concept.derivation == Derivation.ROWSET:
        return [
            environment.concepts[c]
            for c in concept.grain.components
            if c in environment.concepts
        ]
    if concept.derivation == Derivation.UNION:
        # A union output's per-arm keys can't be stacked into one column, so
        # its usable row identity is a sibling union over those keys (which a
        # UnionNode CAN output). Without one, fall through to the per-arm key
        # demand — unsatisfiable, but loud, never a silent dedup.
        siblings = _union_key_siblings(concept, environment)
        if siblings:
            return siblings
    if concept.purpose == Purpose.PROPERTY and concept.keys:
        return [
            environment.concepts[c] for c in concept.keys if c in environment.concepts
        ]
    if concept.lineage is None:
        return []
    if isinstance(concept.lineage, BuildFilterItem):
        # A filter's row identity is its content's; the where clause is a
        # predicate, not part of the result's row identity.
        content = concept.lineage.content
        if isinstance(content, BuildConcept):
            return _walk_aggregate_grain_inputs(content, environment, seen)
        return []
    collected: list[BuildConcept] = []
    for arg in concept.lineage.concept_arguments:
        if isinstance(arg, BuildConcept):
            collected.extend(_walk_aggregate_grain_inputs(arg, environment, seen))
    return collected


LOGGER_PREFIX = "[COMMON]"


def prune_and_merge(
    G: ReferenceGraph,
    keep_node_lambda: Callable[[str], bool],
) -> ReferenceGraph:
    """Prune nodes of one type and create direct connections between remaining nodes."""
    nodes_to_keep = [n for n in G.nodes if keep_node_lambda(n)]
    new_graph = G.subgraph(nodes_to_keep).copy()
    nodes_to_remove = [n for n in G.nodes() if n not in nodes_to_keep]

    for node_pair in combinations(nodes_to_keep, 2):
        n1, n2 = node_pair
        try:
            path = nx.shortest_path(G, n1, n2)
            if len(path) > 2 or any(node in nodes_to_remove for node in path[1:-1]):
                new_graph.add_edge(n1, n2)
        except nx.NetworkXNoPath:
            continue

    return new_graph


LOGGER_PREFIX = "[COMMON]"


# -----------------------------
# Small, testable helpers
# -----------------------------


def is_ds_node(n: str) -> bool:
    return n.startswith("ds~")


def build_ds_column_index(
    datasource_lookup: dict[str, BuildDatasource | BuildUnionDatasource],
) -> dict[str, dict[str, BuildConcept]]:
    """
    ds -> { concept_address -> BuildConcept }
    """
    base = {
        ds: {col.concept.address: col.concept for col in node.columns}
        for ds, node in datasource_lookup.items()
    }
    return base


def iter_unique_ds_pairs(
    g: nx.Graph | nx.DiGraph,
) -> Iterable[tuple[str, str]]:
    """
    Yield each unordered datasource pair once.
    """
    seen = set()
    for ds in g.nodes:
        for nbr in g.neighbors(ds):
            pair = cast(tuple[str, str], tuple(sorted((ds, nbr))))
            if pair in seen:
                continue
            seen.add(pair)
            yield pair


def get_concept_node_cached(cache: dict[str, str], concept: BuildConcept):
    """
    Memoized concept -> graph node resolution.
    """
    addr = concept.address
    if addr not in cache:
        cache[addr] = concept_to_node(concept.with_default_grain())
    return cache[addr]


def existing_join_addresses(
    final: ReferenceGraph,
    concepts: Iterable[BuildConcept],
    get_node,
) -> set[str]:
    """
    Return addresses already present in the final graph.
    """
    existing = set()
    for c in concepts:
        if get_node(c) in final.nodes:
            existing.add(c.address)
    return existing


def injectable_concepts(
    common: dict[str, BuildConcept],
    reduced: set[str],
    existing: set[str],
    synonyms: dict[str, str],
    add_joins: bool,
) -> Iterable[BuildConcept]:
    """
    Yield concepts eligible for reinjection.
    """
    for addr, concept in common.items():
        if addr in synonyms:
            continue
        if addr not in reduced:
            continue
        # Aggregate metrics (e.g. `count`) can show up on multiple persisted
        # datasources at different grains — but they are computed values, not
        # join keys. Joining datasources on a shared aggregate is meaningless
        # and would emit a virtual `_virt_agg_*` node into the graph.
        if concept.is_aggregate:
            continue
        if addr in existing and not add_joins:
            continue
        if any(p in existing for p in concept.pseudonyms):
            continue
        yield concept


class AuthoredJoinPair(NamedTuple):
    """A declared relation's endpoints in AUTHOR address space (each retaining
    its own `keys` for FK-carrier checks) plus the single BUILD concept the
    merged key resolves to — ROOT members substitute onto one canonical, so
    injection targets the canonical exactly like a physically shared key."""

    left: BuildConcept
    right: BuildConcept
    canonical: BuildConcept


def _member_origin(environment: BuildEnvironment, address: str) -> BuildConcept | None:
    """The author-space concept for a relation endpoint: a substituted member's
    original survives in alias_origin_lookup; an unsubstituted endpoint is its
    own build concept."""
    origin = environment.alias_origin_lookup.get(address)
    if origin is not None:
        return origin
    concept = environment.concepts.get(address)
    if concept is not None and concept.address == address:
        return concept
    return None


def _canonical_key_addresses(
    environment: BuildEnvironment, member: BuildConcept
) -> set[str]:
    out: set[str] = set()
    for key_addr in member.keys or ():
        key_concept = environment.concepts.get(key_addr)
        out.add(key_concept.address if key_concept is not None else key_addr)
    return out


def authored_join_pair_candidates(
    environment: BuildEnvironment,
) -> list[AuthoredJoinPair]:
    """Declared SUBSET/INCOMPARABLE relation endpoint pairs on ROOT members,
    scope-blind (global merge and query join are the same declaration at
    different scopes). EQUAL relations are excluded (either side authoritative;
    enforcement rides the canonical machinery); derived (BASIC/ROWSET) members
    stay with the rowset key-exposure and merge-node exposure machinery.

    A pair of properties keyed by the SAME canonical entity key is excluded:
    the relation is a functional consequence of that key's own merge, which
    discovery already enforces as a shared canonical — pairing the properties
    independently is redundant (conformed-dimension property merges, stocks).
    A pair whose keys stay distinct (q25's per-side customer surrogates) is the
    load-bearing bridge and stays."""
    out: list[AuthoredJoinPair] = []
    for edge in environment.domain_graph.edges:
        if edge.provenance is not EdgeProvenance.DECLARED:
            continue
        if edge.relation not in (DomainRelation.SUBSET, DomainRelation.INCOMPARABLE):
            continue
        left = _member_origin(environment, edge.source)
        right = _member_origin(environment, edge.target)
        if left is None or right is None:
            continue
        if (
            left.derivation is not Derivation.ROOT
            or right.derivation is not Derivation.ROOT
        ):
            continue
        canonical = environment.concepts.get(edge.source)
        mate = environment.concepts.get(edge.target)
        if canonical is None or mate is None or canonical.address != mate.address:
            continue
        left_keys = _canonical_key_addresses(environment, left)
        if left_keys and left_keys == _canonical_key_addresses(environment, right):
            continue
        out.append(AuthoredJoinPair(left=left, right=right, canonical=canonical))
    return out


def _member_carriers(
    member: BuildConcept, bound_by_ds: dict[str, set[str]]
) -> set[str]:
    """Datasources that can anchor `member`'s side of a relation: they bind the
    member itself, or bind ALL of its keys (an FK carrier able to join to the
    member's dimension at its grain). Exact author addresses only — canonical
    or pseudonym matching would anchor both sides from one source."""
    keys = set(member.keys or ())
    return {
        identifier
        for identifier, bound in bound_by_ds.items()
        if member.address in bound or (keys and keys <= bound)
    }


def _member_needs_fk_hop(
    member: BuildConcept, bound_by_ds: dict[str, set[str]]
) -> bool:
    """A request datasource binds the member's keys but not the member itself:
    that source can only reach the member through its dimension hop, so the
    relation still needs discovery help even if some other request datasource
    (e.g. the dimension scan pulled in by projecting the merged key) binds the
    member directly."""
    keys = set(member.keys or ())
    if not keys:
        return False
    return any(
        keys <= bound and member.address not in bound for bound in bound_by_ds.values()
    )


def _relevant_authored_join_pairs(
    all_concepts: Sequence[BuildConcept],
    environment: BuildEnvironment,
) -> tuple[list[AuthoredJoinPair], dict[str, set[str]]]:
    """Authored relation pairs the request traverses AND that need discovery
    help, plus the author-space binding index of the request's datasources.

    Traversed: each side is anchored by a requested-concept datasource the
    other side lacks. One-sided requests and single-scan (denormalized)
    coverage skip, so a declared relation stays lazy unless the query actually
    relates the two sides.

    Needs help: some member is NOT directly bound on a request datasource, or
    is bound only away from an FK carrier that must hop to reach it. When
    every member is a physical column wherever its keys appear (the
    fact→date-spine merge, the both-facts-bind-the-sk q25 form) the merged
    concept is already a natural shared join key and injection only perturbs
    the plan (q2 date-spine regression).

    Carrier matching runs in AUTHOR address space via the domain graph's
    binding edges (datasource columns rebind to the canonical at build time,
    which would erase the sides)."""
    candidates = authored_join_pair_candidates(environment)
    if not candidates:
        return [], {}
    requested: set[str] = set()
    for c in all_concepts:
        requested.add(c.address)
        requested.update(c.pseudonyms)
    bound_by_ds: dict[str, set[str]] = {}
    for binding in environment.domain_graph.binding_edges:
        bound_by_ds.setdefault(binding.datasource, set()).add(binding.concept)
    bound_by_ds = {
        identifier: bound
        for identifier, bound in bound_by_ds.items()
        if bound & requested
    }
    if not bound_by_ds:
        return [], {}
    all_bound: set[str] = set()
    for bound in bound_by_ds.values():
        all_bound.update(bound)
    out: list[AuthoredJoinPair] = []
    for pair in candidates:
        if (
            pair.left.address in all_bound
            and pair.right.address in all_bound
            and not _member_needs_fk_hop(pair.left, bound_by_ds)
            and not _member_needs_fk_hop(pair.right, bound_by_ds)
        ):
            continue
        left_carriers = _member_carriers(pair.left, bound_by_ds)
        right_carriers = _member_carriers(pair.right, bound_by_ds)
        if left_carriers - right_carriers and right_carriers - left_carriers:
            out.append(pair)
    return out, bound_by_ds


def relevant_authored_join_pairs(
    all_concepts: Sequence[BuildConcept],
    environment: BuildEnvironment,
) -> list[AuthoredJoinPair]:
    return _relevant_authored_join_pairs(all_concepts, environment)[0]


def inject_authored_join_key_terminals(
    all_concepts: list[BuildConcept],
    environment: BuildEnvironment,
) -> list[BuildConcept]:
    """Force the merged key of each traversed authored join relation into the
    resolution as a mandatory terminal, mirroring shared-key treatment: each
    side's subgraph must materialize the key, so the merge join pairs the
    authored equality instead of silently dropping it (TPC-DS q17/q25).

    Each member's keys are injected too, pinning BOTH sides' FK hops as
    mandatory — otherwise the side-paths to the one merged key read as
    alternative resolutions and raise ambiguity."""
    pairs, _ = _relevant_authored_join_pairs(all_concepts, environment)
    if not pairs:
        return all_concepts
    wanted: list[BuildConcept] = []
    for pair in pairs:
        wanted.append(pair.canonical)
        for member in (pair.left, pair.right):
            for key_addr in sorted(member.keys or ()):
                key_concept = environment.concepts.get(key_addr)
                if (
                    key_concept is not None
                    and key_concept.derivation is Derivation.ROOT
                ):
                    wanted.append(key_concept)
    existing = {c.address for c in all_concepts}
    additions = unique([c for c in wanted if c.address not in existing], "address")
    if not additions:
        return all_concepts
    logger.info(
        f"{LOGGER_PREFIX} injecting authored join key terminals "
        f"{[c.address for c in additions]}"
    )
    return unique(all_concepts + additions, "address")


# -----------------------------
# Main function
# -----------------------------


def reinject_common_join_keys_v2(
    base_graph: ReferenceGraph,
    final: ReferenceGraph,
    synonyms: dict[str, str],
    add_joins: bool = False,
) -> bool:
    """
    Reinjection of inferred join keys between datasource nodes.
    """
    datasource_lookup = {**base_graph.datasources, **final.datasources}

    ds_graph = prune_and_merge(final, is_ds_node)
    if not ds_graph.nodes:
        return False

    # Precompute once
    ds_columns = build_ds_column_index(datasource_lookup)
    concept_node_cache: dict[str, str] = {}

    injected = False

    for ds1, ds2 in iter_unique_ds_pairs(ds_graph):
        if ds1 not in ds_columns or ds2 not in ds_columns:
            continue

        cols1 = ds_columns[ds1]
        cols2 = ds_columns[ds2]

        common_addrs = cols1.keys() & cols2.keys()
        if not common_addrs:
            continue

        common_concepts = {addr: cols1[addr] for addr in common_addrs}

        reduced = set(BuildGrain.from_concepts(common_concepts.values()).components)

        get_node = lambda c: get_concept_node_cached(concept_node_cache, c)

        existing = existing_join_addresses(
            final,
            common_concepts.values(),
            get_node,
        )

        for concept in injectable_concepts(
            common_concepts,
            reduced,
            existing,
            synonyms,
            add_joins,
        ):
            cnode = get_node(concept)
            final.add_edge(ds1, cnode)
            final.add_edge(ds2, cnode)

            logger.debug(
                f"{LOGGER_PREFIX} reinjecting common join key {cnode} "
                f"between {ds1} and {ds2}, existing {existing}"
            )

            existing.add(concept.address)
            injected = True

    return injected
