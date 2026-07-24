from collections.abc import Callable, Iterable, Sequence
from itertools import combinations
from typing import (
    NamedTuple,
    cast,
)

from trilogy.constants import logger
from trilogy.core import graph as nx
from trilogy.core.domain_graph import DomainRelation, EdgeProvenance
from trilogy.core.enums import Derivation, JoinType, Purpose
from trilogy.core.graph_models import ReferenceGraph, concept_to_node
from trilogy.core.models.build import (
    BoolExpr,
    BuildAggregateWrapper,
    BuildBetween,
    BuildComparison,
    BuildConcept,
    BuildConditional,
    BuildDatasource,
    BuildFilterItem,
    BuildFunction,
    BuildGrain,
    BuildUnionDatasource,
    BuildWhereClause,
    LooseBuildConceptList,
    concept_is_relevant,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.condition_utility import (
    combine_condition_atoms,
    condition_implies,
    condition_required_addresses,
    decompose_condition,
)
from trilogy.core.processing.nodes import (
    History,
    NodeJoin,
)
from trilogy.core.processing.nodes.base_node import StrategyNode
from trilogy.core.processing.nodes.merge_node import MergeNode
from trilogy.utility import unique

AGGREGATE_TYPES = (BuildAggregateWrapper,)
FUNCTION_TYPES = (BuildFunction,)
PROPERTY_PURPOSES = (Purpose.PROPERTY, Purpose.UNIQUE_PROPERTY)


def optional_satisfied(
    concept: BuildConcept, output_addresses: set[str], partial_addresses: set[str]
) -> bool:
    """An optional is already served by a node if the node outputs it directly
    (or a pseudonym of it) and that output is not partial. A partial output can't
    satisfy it — joining for the full column is what keeps unmatched rows."""
    if concept.address in partial_addresses:
        return False
    return concept.address in output_addresses or any(
        addr in concept.pseudonyms for addr in output_addresses
    )


def unsatisfied_optionals(
    local_optional: list[BuildConcept], node: StrategyNode
) -> list[BuildConcept]:
    """The optionals a node does not already serve (pseudonym- and partial-aware)."""
    output_addresses = {x.address for x in node.output_concepts}
    partial_addresses = {x.address for x in node.partial_concepts}
    return [
        x
        for x in local_optional
        if not optional_satisfied(x, output_addresses, partial_addresses)
    ]


def _node_has_preexisting_conditions(
    node: StrategyNode,
    condition: BoolExpr,
) -> bool:
    return node.preexisting_conditions == condition or (
        node.preexisting_conditions is not None
        and condition_implies(node.preexisting_conditions, condition)
    )


def _preexisting_conditions_from_parents(
    parents: list[StrategyNode],
    conditions: BuildWhereClause | None,
) -> BoolExpr | None:
    if conditions is None or not parents:
        return None
    if all(
        _node_has_preexisting_conditions(parent, conditions.conditional)
        for parent in parents
    ):
        return conditions.conditional
    return None


def _condition_available_from_parents(
    parents: list[StrategyNode],
    condition: BoolExpr,
) -> bool:
    available = {
        concept.canonical_address
        for parent in parents
        for concept in parent.usable_outputs
    }
    return condition_required_addresses(condition).issubset(available)


def _local_property_conditions(
    conditions: BuildWhereClause | None,
    required: list[BuildConcept],
    key_addresses: set[str],
) -> tuple[BuildWhereClause | None, list[BuildConcept]]:
    if conditions is None:
        return None, []
    available = {c.canonical_address for c in required}
    condition_concepts: list[BuildConcept] = []
    atoms: list[BoolExpr] = []
    for atom in decompose_condition(conditions.conditional):
        atom_concepts = [
            c for c in atom.row_arguments if c.derivation != Derivation.CONSTANT
        ]
        extra_concepts = [
            c
            for c in atom_concepts
            if c.canonical_address not in available
            and c.keys
            and c.keys.issubset(key_addresses)
        ]
        lineage_concepts = [
            c
            for c in atom_concepts
            if c.canonical_address not in available
            and any(
                required_concept.derivation
                not in (Derivation.ROOT, Derivation.CONSTANT)
                and concept_is_relevant(required_concept, [c])
                for required_concept in required
            )
        ]
        atom_addresses = {c.canonical_address for c in atom_concepts}
        extra_addresses = {c.canonical_address for c in extra_concepts}
        lineage_addresses = {c.canonical_address for c in lineage_concepts}
        if atom_addresses.issubset(available | extra_addresses | lineage_addresses):
            atoms.append(atom)
            condition_concepts.extend(extra_concepts + lineage_concepts)
            available.update(extra_addresses | lineage_addresses)
    condition = combine_condition_atoms(atoms)
    if condition is None:
        return None, []
    return BuildWhereClause(conditional=condition), unique(
        condition_concepts, "address"
    )


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


def resolve_function_parent_concepts(
    concept: BuildConcept, environment: BuildEnvironment
) -> list[BuildConcept]:
    if not isinstance(
        concept.lineage,
        (
            *FUNCTION_TYPES,
            *AGGREGATE_TYPES,
            BuildComparison,
            BuildConditional,
            BuildBetween,
        ),
    ):
        raise TypeError(
            f"Concept {concept} lineage is not function or aggregate, is {type(concept.lineage)}"
        )
    if concept.derivation == Derivation.AGGREGATE:
        base: list[BuildConcept] = []
        if not concept.grain.abstract:
            base = concept.lineage.concept_arguments + [
                environment.concepts[c] for c in concept.grain.components
            ]
            # if the base concept being aggregated is a property with a key
            # keep the key as a parent
        else:
            base = concept.lineage.concept_arguments
        if isinstance(concept.lineage, AGGREGATE_TYPES):
            # for aggregate wrapper, don't include the by
            extra_property_grain = concept.lineage.function.concept_arguments
        else:
            extra_property_grain = concept.lineage.concept_arguments
        for x in extra_property_grain:
            if isinstance(x, BuildConcept):
                # Walk wrappers (filter, basic) to surface row-identity
                # concepts the aggregate needs from each arg: rowset grain
                # components and property keys. Stops at aggregate
                # boundaries — an inner aggregate has already collapsed its
                # upstream rows to its own ``by`` grain.
                base += _walk_aggregate_grain_inputs(x, environment)
        return unique(base, "address")
    # TODO: handle basic lineage chains?
    return unique(concept.lineage.concept_arguments, "address")


def resolve_condition_parent_concepts(
    condition: BuildWhereClause,
) -> tuple[list[BuildConcept], list[tuple[BuildConcept, ...]]]:
    base_rows: list[BuildConcept] = []
    base_rows += condition.row_arguments
    base_existence = list(condition.existence_arguments)
    return unique(base_rows, "address"), base_existence


def resolve_filter_parent_concepts(
    concept: BuildConcept,
    environment: BuildEnvironment,
) -> tuple[list[BuildConcept], list[tuple[BuildConcept, ...]]]:
    if not isinstance(concept.lineage, (BuildFilterItem,)):
        raise TypeError(
            f"Concept {concept} lineage is not filter item, is {type(concept.lineage)}"
        )
    direct_parent = concept.lineage.content
    base_existence = []
    base_rows = [direct_parent] if isinstance(direct_parent, BuildConcept) else []
    condition_rows, condition_existence = resolve_condition_parent_concepts(
        concept.lineage.where
    )
    base_rows += condition_rows
    base_existence += condition_existence
    # this is required so that
    if (
        isinstance(direct_parent, BuildConcept)
        and direct_parent.purpose in (Purpose.PROPERTY, Purpose.METRIC)
        and direct_parent.keys
    ):
        base_rows += [environment.concepts[c] for c in direct_parent.keys]

    if concept.lineage.where.existence_arguments:
        return (
            unique(base_rows, "address"),
            base_existence,
        )
    return unique(base_rows, "address"), []


def _base_lookup_keys(base_node: StrategyNode) -> list[BuildConcept]:
    return unique(
        [c for c in base_node.usable_outputs if c.purpose == Purpose.KEY],
        "address",
    )


def _lookup_closure(
    roots: list[BuildConcept],
    environment: BuildEnvironment,
) -> dict[str, tuple[set[str], set[str]]]:
    closure: dict[str, tuple[set[str], set[str]]] = {
        root.address: ({root.address}, {root.address}) for root in roots
    }
    changed = True
    while changed:
        changed = False
        for concept in sorted(environment.concepts.values(), key=lambda x: x.address):
            if concept.address in closure:
                continue
            if concept.purpose not in (Purpose.KEY, *PROPERTY_PURPOSES):
                continue
            if not concept.keys or not concept.keys.issubset(closure):
                continue
            roots_for_concept: set[str] = set()
            path_for_concept: set[str] = {concept.address}
            for key in concept.keys:
                key_roots, key_path = closure[key]
                roots_for_concept.update(key_roots)
                path_for_concept.update(key_path)
            closure[concept.address] = (roots_for_concept, path_for_concept)
            changed = True
    return closure


def _condition_key_addresses(required: list[BuildConcept]) -> set[str]:
    key_addresses = {c.address for c in required}
    for concept in required:
        key_addresses.update(concept.keys or set())
    return key_addresses


def _property_lookup_groups(
    extra_properties: list[BuildConcept],
    closure: dict[str, tuple[set[str], set[str]]],
) -> dict[tuple[str, ...], tuple[set[str], set[str]]]:
    groups: dict[tuple[str, ...], tuple[set[str], set[str]]] = {}
    for prop in extra_properties:
        if not prop.keys:
            raise SyntaxError(f"Property {prop.address} missing keys in lookup")
        if prop.address not in closure:
            return {}
        roots, path = closure[prop.address]
        key = tuple(sorted(roots))
        group_props, group_path = groups.get(key, (set(), set()))
        group_props.add(prop.address)
        group_path.update(path)
        groups[key] = (group_props, group_path)
    return groups


def gen_property_enrichment_node(
    base_node: StrategyNode,
    extra_properties: list[BuildConcept],
    history: History,
    environment: BuildEnvironment,
    g,
    depth: int,
    source_concepts,
    log_lambda: Callable,
    conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    roots = _base_lookup_keys(base_node)
    if not roots or not extra_properties:
        return None
    closure = _lookup_closure(roots, environment)
    lookup_groups = _property_lookup_groups(extra_properties, closure)
    if not lookup_groups:
        return None

    final_nodes: list[StrategyNode] = []
    input_concepts = list(base_node.output_concepts)
    any_atoms_dropped = False
    for root_key, (properties, path) in lookup_groups.items():
        required = [environment.concepts[address] for address in sorted(path)]
        log_lambda(
            f"Generating property enrichment node for {root_key} with {sorted(properties)}"
        )
        local_conditions, atoms_dropped = _split_conditions_for_enrichment(
            conditions, required
        )
        if atoms_dropped:
            any_atoms_dropped = True
            log_lambda(
                f"Property enrichment for {root_key} dropped out-of-scope filter "
                f"atoms; base node carries them. Will force INNER on the merge."
            )
        enrich_node: StrategyNode | None = source_concepts(
            mandatory_list=required,
            environment=environment,
            g=g,
            depth=depth + 1,
            history=history,
            conditions=local_conditions,
        )
        if not enrich_node:
            return None
        final_nodes.append(enrich_node)
        input_concepts.extend(required)

    return MergeNode(
        input_concepts=unique(input_concepts, "address"),
        output_concepts=unique(base_node.output_concepts + extra_properties, "address"),
        environment=environment,
        parents=[
            base_node,
        ]
        + final_nodes,
        preexisting_conditions=conditions.conditional if conditions else None,
        force_join_type=JoinType.INNER if any_atoms_dropped else None,
    )


def concepts_to_grain_concepts(
    concepts: list[BuildConcept],
    environment: BuildEnvironment,
) -> list[BuildConcept]:
    """Resolve concepts to their grain-level keys for use as join keys."""
    return [
        environment.concepts[c]
        for c in BuildGrain.from_concepts(
            concepts=concepts, environment=environment
        ).components
    ]


def _split_conditions_for_enrichment(
    conditions: BuildWhereClause | None,
    enrichment_concepts: list[BuildConcept],
) -> tuple[BuildWhereClause | None, bool]:
    """Split ``conditions`` into atoms in-scope for the enrichment vs not.

    "In scope" = every row-arg of the atom is the enrichment's join key, an
    extra-required output, or one of their key parents. Atoms that reach
    outside that scope (e.g. ``order.date IN range`` when the enrichment is
    a customer-properties lookup) are assumed to be applied by the base node
    and dropped from the enrichment's filter. The caller must force the
    resulting merge to INNER when anything is dropped — otherwise outer-join
    NULL-padding would bypass the now-missing predicate.
    """
    if conditions is None:
        return None, False
    in_scope: set[str] = set()
    for c in enrichment_concepts:
        in_scope.add(c.address)
        # Only follow `.keys` for property-purpose concepts (where keys means
        # "what this is a property of"). For KEY concepts, `.keys` is the row
        # grain in the *source* namespace — e.g. ``store_sales.customer.id``
        # is a foreign key whose ``.keys`` = ``{ticket_number, item.id}`` from
        # store_sales, *not* customer. Walking those would drag store_sales
        # identifiers into the enrichment's scope and keep atoms that filter
        # the wrong side.
        if c.purpose in PROPERTY_PURPOSES:
            in_scope.update(c.keys or set())
    kept: list[BoolExpr] = []
    dropped = False
    for atom in decompose_condition(conditions.conditional):
        atom_addresses = {
            arg.address
            for arg in atom.row_arguments
            if arg.derivation != Derivation.CONSTANT
        }
        if atom_addresses.issubset(in_scope):
            kept.append(atom)
        else:
            dropped = True
    if not kept:
        return None, dropped
    combined = combine_condition_atoms(kept)
    if combined is None:
        return None, dropped
    return BuildWhereClause(conditional=combined), dropped


def gen_enrichment_node(
    base_node: StrategyNode,
    join_keys: list[BuildConcept],
    local_optional: list[BuildConcept],
    environment: BuildEnvironment,
    g,
    depth: int,
    source_concepts,
    log_lambda,
    history: History,
    conditions: BuildWhereClause | None = None,
    partial_concepts: list[BuildConcept] | None = None,
):
    local_opts = LooseBuildConceptList(concepts=local_optional)

    extra_required = [
        x
        for x in local_opts
        if x not in base_node.output_lcl or x in base_node.partial_lcl
    ]

    # Property enrichment resolves each extra via its key closure, so every
    # extra must be a property *with* keys. A keyless property (e.g. a rowset
    # column) has no closure to join on; fall through to the general path.
    if extra_required and all(
        x.purpose in PROPERTY_PURPOSES and x.keys for x in extra_required
    ):
        property_node = gen_property_enrichment_node(
            base_node,
            extra_required,
            environment=environment,
            g=g,
            depth=depth,
            source_concepts=source_concepts,
            history=history,
            conditions=conditions,
            log_lambda=log_lambda,
        )
        if property_node:
            log_lambda(
                f"{type(base_node).__name__!s} returning property enrichment node"
            )
            return property_node
    log_lambda(
        f"{type(base_node).__name__!s} searching for join keys {LooseBuildConceptList(concepts=join_keys)} and extra required {local_opts}"
    )
    enrich_node: StrategyNode = source_concepts(  # this fetches the parent + join keys
        # to then connect to the rest of the query
        mandatory_list=join_keys + extra_required,
        environment=environment,
        g=g,
        depth=depth + 1,
        history=history,
        conditions=conditions,
    )
    if not enrich_node:
        log_lambda(
            f"{type(base_node).__name__!s} enrichment node unresolvable, returning just group node"
        )
        return base_node
    log_lambda(
        f"{type(base_node).__name__!s} returning merge node with group node + enrichment node"
    )
    non_hidden = [
        x
        for x in base_node.output_concepts
        if x.address not in base_node.hidden_concepts
    ]
    return MergeNode(
        input_concepts=unique(join_keys + extra_required + non_hidden, "address"),
        output_concepts=unique(join_keys + extra_required + non_hidden, "address"),
        environment=environment,
        parents=[enrich_node, base_node],
        force_group=False,
        preexisting_conditions=conditions.conditional if conditions else None,
        depth=depth,
        partial_concepts=partial_concepts,
    )


def resolve_join_order(joins: list[NodeJoin]) -> list[NodeJoin]:
    if not joins:
        return []
    available_aliases: set[StrategyNode] = set()
    final_joins_pre = [*joins]
    final_joins = []
    left = set()
    right = set()
    for join in joins:
        left.add(join.left_node)
        right.add(join.right_node)

    potential_basis = left.difference(right)
    base_candidates = [x for x in final_joins_pre if x.left_node in potential_basis]
    if not base_candidates:
        raise SyntaxError(
            f"Unresolvable join dependencies, left requires {left} and right requires {right}"
        )
    base = base_candidates[0]
    final_joins.append(base)
    available_aliases.add(base.left_node)
    available_aliases.add(base.right_node)
    while final_joins_pre:
        new_final_joins_pre: list[NodeJoin] = []
        for join in final_joins_pre:
            if join.left_node in available_aliases:
                # we don't need to join twice
                # so whatever join we found first, works
                if join.right_node in available_aliases:
                    continue
                final_joins.append(join)
                available_aliases.add(join.left_node)
                available_aliases.add(join.right_node)
            else:
                new_final_joins_pre.append(join)
        final_joins_pre = new_final_joins_pre
    return final_joins


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
