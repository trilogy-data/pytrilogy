from itertools import combinations
from typing import Callable, Dict, Iterable, List, Set, Tuple, cast

from trilogy.constants import logger
from trilogy.core import graph as nx
from trilogy.core.enums import Derivation, Purpose
from trilogy.core.graph_models import ReferenceGraph, concept_to_node
from trilogy.core.models.build import (
    BoolExpr,
    BuildAggregateWrapper,
    BuildComparison,
    BuildConcept,
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
from trilogy.core.processing.where_path import BuildWherePath
from trilogy.utility import unique

AGGREGATE_TYPES = (BuildAggregateWrapper,)
FUNCTION_TYPES = (BuildFunction,)
PROPERTY_PURPOSES = (Purpose.PROPERTY, Purpose.UNIQUE_PROPERTY)


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


def _concept_dependency_addresses(
    concept: BuildConcept,
    seen: set[str] | None = None,
) -> set[str]:
    seen = seen if seen is not None else set()
    if concept.address in seen:
        return seen
    seen.add(concept.address)
    for source in concept.sources:
        _concept_dependency_addresses(source, seen)
    for argument in concept.concept_arguments:
        _concept_dependency_addresses(argument, seen)
    return seen


def _condition_argument_addresses(condition: BoolExpr) -> set[str]:
    addresses: set[str] = set()
    for arg in condition.row_arguments:
        addresses.update(_concept_dependency_addresses(arg))
    return addresses


def child_source_conditions(
    concept: BuildConcept,
    conditions: BuildWhereClause | None,
    where_path: BuildWherePath | None,
) -> tuple[BuildWhereClause | None, BuildWherePath | None]:
    if where_path is None:
        return conditions, None
    current = where_path.current_condition
    if current is None:
        return conditions, where_path
    if concept.address in _condition_argument_addresses(current.conditional):
        return where_path.applied_condition, None
    return conditions, BuildWherePath(
        applied=where_path.applied,
        pending=(current,),
    )


def conditions_for_addresses(
    conditions: BuildWhereClause | None,
    addresses: set[str],
) -> BuildWhereClause | None:
    if conditions is None:
        return None
    condition = combine_condition_atoms(
        [
            atom
            for atom in decompose_condition(conditions.conditional)
            if all(c.address in addresses for c in atom.concept_arguments)
        ]
    )
    if condition is None:
        return None
    return BuildWhereClause(conditional=condition)


def where_path_for_addresses(
    where_path: BuildWherePath | None,
    addresses: set[str],
) -> BuildWherePath | None:
    if where_path is None:
        return None
    applied = tuple(
        clause
        for clause in (
            conditions_for_addresses(clause, addresses) for clause in where_path.applied
        )
        if clause is not None
    )
    pending = tuple(
        clause
        for clause in (
            conditions_for_addresses(clause, addresses) for clause in where_path.pending
        )
        if clause is not None
    )
    if not applied and not pending:
        return None
    return BuildWherePath(applied=applied, pending=pending)


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


def _walk_aggregate_grain_inputs(
    concept: BuildConcept,
    environment: BuildEnvironment,
    seen: Set[str] | None = None,
) -> List[BuildConcept]:
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
    collected: List[BuildConcept] = []
    for arg in concept.lineage.concept_arguments:
        if isinstance(arg, BuildConcept):
            collected.extend(_walk_aggregate_grain_inputs(arg, environment, seen))
    return collected


def resolve_function_parent_concepts(
    concept: BuildConcept, environment: BuildEnvironment
) -> List[BuildConcept]:
    if not isinstance(
        concept.lineage, (*FUNCTION_TYPES, *AGGREGATE_TYPES, BuildComparison)
    ):
        raise ValueError(
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
) -> Tuple[List[BuildConcept], List[Tuple[BuildConcept, ...]]]:
    base_existence = []
    base_rows: list[BuildConcept] = []
    base_rows += condition.row_arguments
    for ctuple in condition.existence_arguments:
        base_existence.append(ctuple)
    return unique(base_rows, "address"), base_existence


def resolve_filter_parent_concepts(
    concept: BuildConcept,
    environment: BuildEnvironment,
) -> Tuple[List[BuildConcept], List[Tuple[BuildConcept, ...]]]:
    if not isinstance(concept.lineage, (BuildFilterItem,)):
        raise ValueError(
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
    where_path: BuildWherePath | None = None,
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
    for root_key, (properties, path) in lookup_groups.items():
        required = [environment.concepts[address] for address in sorted(path)]
        log_lambda(
            f"Generating property enrichment node for {root_key} with {sorted(properties)}"
        )
        local_conditions, _ = _local_property_conditions(
            conditions,
            required,
            _condition_key_addresses(required),
        )
        local_where_path = where_path_for_addresses(
            where_path,
            {concept.address for concept in required},
        )
        enrich_node: StrategyNode | None = source_concepts(
            mandatory_list=required,
            environment=environment,
            g=g,
            depth=depth + 1,
            history=history,
            conditions=local_conditions,
            where_path=local_where_path,
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
        preexisting_conditions=_preexisting_conditions_from_parents(
            [base_node] + final_nodes,
            conditions,
        ),
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


def gen_enrichment_node(
    base_node: StrategyNode,
    join_keys: List[BuildConcept],
    local_optional: list[BuildConcept],
    environment: BuildEnvironment,
    g,
    depth: int,
    source_concepts,
    log_lambda,
    history: History,
    conditions: BuildWhereClause | None = None,
    where_path: BuildWherePath | None = None,
):
    local_opts = LooseBuildConceptList(concepts=local_optional)

    extra_required = [
        x
        for x in local_opts
        if x not in base_node.output_lcl or x in base_node.partial_lcl
    ]

    if extra_required and all(x.purpose in PROPERTY_PURPOSES for x in extra_required):
        property_node = gen_property_enrichment_node(
            base_node,
            extra_required,
            environment=environment,
            g=g,
            depth=depth,
            source_concepts=source_concepts,
            history=history,
            conditions=conditions,
            where_path=where_path,
            log_lambda=log_lambda,
        )
        if property_node:
            log_lambda(
                f"{str(type(base_node).__name__)} returning property enrichment node"
            )
            return property_node
    log_lambda(
        f"{str(type(base_node).__name__)} searching for join keys {LooseBuildConceptList(concepts=join_keys)} and extra required {local_opts}"
    )
    enrich_node: StrategyNode = source_concepts(  # this fetches the parent + join keys
        # to then connect to the rest of the query
        mandatory_list=join_keys + extra_required,
        environment=environment,
        g=g,
        depth=depth + 1,
        history=history,
        conditions=conditions,
        where_path=where_path,
    )
    if not enrich_node:
        log_lambda(
            f"{str(type(base_node).__name__)} enrichment node unresolvable, returning just group node"
        )
        return base_node
    log_lambda(
        f"{str(type(base_node).__name__)} returning merge node with group node + enrichment node"
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
        preexisting_conditions=_preexisting_conditions_from_parents(
            [enrich_node, base_node],
            conditions,
        ),
        depth=depth,
    )


def resolve_join_order(joins: List[NodeJoin]) -> List[NodeJoin]:
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
        new_final_joins_pre: List[NodeJoin] = []
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
    datasource_lookup: Dict[str, BuildDatasource | BuildUnionDatasource],
) -> Dict[str, Dict[str, BuildConcept]]:
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
) -> Iterable[Tuple[str, str]]:
    """
    Yield each unordered datasource pair once.
    """
    seen = set()
    for ds in g.nodes:
        for nbr in g.neighbors(ds):
            pair = cast(Tuple[str, str], tuple(sorted((ds, nbr))))
            if pair in seen:
                continue
            seen.add(pair)
            yield pair


def get_concept_node_cached(cache: Dict[str, str], concept: BuildConcept):
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
) -> Set[str]:
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
    concept_node_cache: Dict[str, str] = {}

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

        get_node = lambda c: get_concept_node_cached(concept_node_cache, c)  # noqa E731

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
