from typing import TYPE_CHECKING

from trilogy.constants import DEFAULT_NAMESPACE, VIRTUAL_CONCEPT_PREFIX, logger
from trilogy.core.enums import (
    Derivation,
    Granularity,
    Purpose,
)
from trilogy.core.exceptions import DisconnectedConceptsException
from trilogy.core.models.build import (
    BuildAggregateWrapper,
    BuildConcept,
    BuildConditional,
    BuildDatasource,
    BuildFilterItem,
    BuildGrain,
    BuildParenthetical,
    BuildRowsetItem,
    BuildSubselectComparison,
    BuildWhereClause,
    get_concept_arguments,
    get_concept_row_arguments,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.execute import QueryDatasource, UnnestJoin
from trilogy.core.processing.constants import ROOT_DERIVATIONS
from trilogy.core.processing.grain_utility import (
    _grain_coverage_addresses,
    concept_source_address,
)
from trilogy.core.processing.rowset_islanding import (
    island_rowsets_for_connectivity,
    link_rowset_outputs_for_connectivity,
)
from trilogy.core.processing.utility import GroupRequiredResponse
from trilogy.utility import unique

if TYPE_CHECKING:
    from trilogy.core.graph_models import ReferenceGraph


def depth_to_prefix(depth: int) -> str:
    return "\t" * depth


NO_PUSHDOWN_DERIVATIONS: list[Derivation] = ROOT_DERIVATIONS + [
    Derivation.BASIC,
    Derivation.ROWSET,
    Derivation.UNNEST,
]


LOGGER_PREFIX = "[DISCOVERY LOOP]"


def calculate_effective_parent_grain(
    node: QueryDatasource | BuildDatasource,
) -> BuildGrain:
    # calculate the effective grain of the parent node
    # this is the union of all parent grains
    if isinstance(node, QueryDatasource):
        if node.group_required:
            return node.grain
        grain = BuildGrain()
        qds = node
        if not qds.joins:
            base = qds.base_datasource
            if base is not None:
                return base.grain
            if not qds.datasources:
                # sourceless literal aggregate (count(1)/by *) = single global-scalar row
                return BuildGrain()
            return qds.datasources[0].grain
        seen = set()
        for join in qds.joins:
            if isinstance(join, UnnestJoin):
                grain += BuildGrain(components={x.address for x in join.concepts})
                continue
            pairs = join.concept_pairs or []
            for key in pairs:
                left = key.existing_datasource
                logger.debug(f"adding left grain {left.grain} for join key {key.left}")
                grain += left.grain
                seen.add(left.name)
            keys = [key.right for key in pairs]
            join_grain = BuildGrain.from_concepts(keys)
            if join_grain == join.right_datasource.grain:
                logger.debug(f"irrelevant right join {join}, does not change grain")
            else:
                logger.debug(
                    f"join changes grain, adding {join.right_datasource.grain} to {grain}"
                )
                grain += join.right_datasource.grain
            seen.add(join.right_datasource.name)
        for x in qds.datasources:
            # if we haven't seen it, it's still contributing to grain
            # unless used ONLY in a subselect
            # so the existence check is a [bad] proxy for that
            if x.name not in seen and not (
                qds.condition
                and qds.condition.existence_arguments
                and any(
                    c.address in block
                    for c in x.output_concepts
                    for block in qds.condition.existence_arguments
                )
            ):
                logger.debug(f"adding unjoined grain {x.grain} for datasource {x.name}")
                grain += x.grain
        return grain
    else:
        return node.grain or BuildGrain()


def check_if_group_required(
    downstream_concepts: list[BuildConcept],
    parents: list[QueryDatasource | BuildDatasource],
    environment: BuildEnvironment,
    depth: int = 0,
) -> GroupRequiredResponse:
    padding = "\t" * depth
    target_grain = BuildGrain.from_concepts(
        downstream_concepts,
        environment=environment,
    )

    comp_grain = BuildGrain()
    for source in parents:
        comp_grain += calculate_effective_parent_grain(source)

    # dynamically select if we need to group
    # we must avoid grouping if we are already at grain
    if comp_grain.abstract and not target_grain.abstract:
        logger.info(
            f"{padding}{LOGGER_PREFIX} Group requirement check: upstream grain is abstract, cannot determine grouping requirement, assuming group required"
        )
        return GroupRequiredResponse(target_grain, comp_grain, True)
    if comp_grain.issubset(target_grain):

        logger.info(
            f"{padding}{LOGGER_PREFIX} Group requirement check:  {comp_grain}, target: {target_grain}, grain is subset of target, no group node required"
        )
        return GroupRequiredResponse(target_grain, comp_grain, False)
    # Expand target via concept-coverage so a MULTISELECT align identity
    # covers its source keys (e.g. local.customer_id covers customer.id and
    # store_sales.customer.id), and a comp_grain that arrives carrying the
    # source keys does not look like extra grain. When *every* target
    # component is itself an aggregate (e.g. TPC-H Q13 distribution shape:
    # ``select count(x) by Y -> per_y, count(Y) -> dist``), exclude aggregate
    # by-keys from coverage: there is no non-aggregate concept anchoring the
    # output to the by-grain, so an upstream at the by-grain is strictly
    # finer and a regroup is required to roll up to the aggregate's grain.
    target_has_only_aggregates = bool(target_grain.components) and all(
        environment.concepts[address].is_aggregate
        for address in target_grain.components
    )
    target_coverage = _grain_coverage_addresses(
        target_grain,
        environment,
        include_aggregate_by_keys=not target_has_only_aggregates,
    )
    if comp_grain.components.issubset(target_coverage):
        logger.info(
            f"{padding}{LOGGER_PREFIX} Group requirement check:  {comp_grain} covered by target coverage {target_coverage}, no group node required"
        )
        return GroupRequiredResponse(target_grain, comp_grain, False)
    # find out what extra is in the comp grain vs target grain
    difference = [
        environment.concepts[c] for c in (comp_grain - target_grain).components
    ]
    logger.info(
        f"{padding}{LOGGER_PREFIX} Group requirement check: upstream grain: {comp_grain}, desired grain: {target_grain} from, difference {[x.address for x in difference]}"
    )
    for x in difference:
        logger.info(
            f"{padding}{LOGGER_PREFIX} Difference concept {x.address} purpose {x.purpose} keys {x.keys}"
        )

    # if the difference is all unique properties whose keys are in the source grain
    # we can also suppress the group
    if difference and all(
        x.keys
        and all(
            environment.concepts[z].address in comp_grain.components for z in x.keys
        )
        for x in difference
    ):
        logger.info(
            f"{padding}{LOGGER_PREFIX} Group requirement check: skipped due to unique property validation"
        )
        return GroupRequiredResponse(target_grain, comp_grain, False)
    if difference and all(x.purpose == Purpose.KEY for x in difference):
        logger.info(
            f"{padding}{LOGGER_PREFIX} checking if downstream is unique properties of key"
        )
        replaced_grain_raw: list[set[str]] = [
            (x.keys or set() if x.purpose == Purpose.UNIQUE_PROPERTY else {x.address})
            for x in downstream_concepts
            if x.address in target_grain.components
        ]
        replaced_grain = [item for sublist in replaced_grain_raw for item in sublist]
        # if the replaced grain is a subset of the comp grain, we can skip the group
        unique_grain_comp = BuildGrain.from_concepts(
            replaced_grain, environment=environment
        )
        if comp_grain.issubset(unique_grain_comp):
            logger.info(
                f"{padding}{LOGGER_PREFIX} Group requirement check: skipped due to unique property validation"
            )
            return GroupRequiredResponse(target_grain, comp_grain, False)
    logger.info(
        f"{padding}{LOGGER_PREFIX} Checking for grain equivalence for filters and rowsets"
    )
    ngrain = []
    for con in target_grain.components:
        full = environment.concepts[con]
        ngrain.append(concept_source_address(full))
    target_grain2 = BuildGrain.from_concepts(
        ngrain,
        environment=environment,
    )
    if comp_grain.issubset(target_grain2):
        logger.info(
            f"{padding}{LOGGER_PREFIX} Group requirement check: {comp_grain}, {target_grain2}, pre rowset grain is subset of target, no group node required"
        )
        return GroupRequiredResponse(target_grain2, comp_grain, False)

    logger.info(f"{padding}{LOGGER_PREFIX} Group requirement check: group required")
    return GroupRequiredResponse(
        target=target_grain, upstream=comp_grain, required=True
    )


def get_upstream_concepts(base: BuildConcept, nested: bool = False) -> set[str]:
    return _upstream_concepts(base, nested, {})


def _upstream_concepts(
    base: BuildConcept, nested: bool, cache: dict[int, set[str]]
) -> set[str]:
    # Lineage DAGs are diamond-shaped: the same concept is reached via many
    # paths, so an unmemoized recursion is exponential. BuildConcepts are
    # immutable during resolution, so memoize the (nested=True) result by
    # identity for the lifetime of the top-level call.
    if nested:
        memoized = cache.get(id(base))
        if memoized is not None:
            return memoized
    upstream: set[str] = set()
    if nested:
        upstream.add(base.address)
    if base.lineage:
        for x in base.lineage.concept_arguments:
            # if it's derived from any value in a rowset, ALL rowset items are
            # upstream. use the rowset's already-namespaced derived_concepts
            # rather than splicing `rowset.name` onto the underlying SELECT's
            # addresses, which would produce nonsense like
            # `deduped.local.group_key` and silently miss real upstreams.
            if x.derivation == Derivation.ROWSET:
                assert isinstance(x.lineage, BuildRowsetItem), type(x.lineage)
                upstream.update(x.lineage.rowset.derived_concepts)
            upstream |= _upstream_concepts(x, True, cache)
    if nested:
        cache[id(base)] = upstream
    return upstream


def _crossjoinable(concept: BuildConcept) -> bool:
    """Single-row / constant / literal-derived concepts cross-join into any
    component, so they never cause a disconnection (mirrors
    calculate_graph_relevance; the literal-derived extension covers multi-row
    generated values like ``unnest([1,2])`` and derivations over them)."""
    return (
        concept.granularity == Granularity.SINGLE_ROW
        or concept.derivation == Derivation.CONSTANT
        or _literal_derived(concept)
    )


def _literal_derived(concept: BuildConcept, _seen: set[str] | None = None) -> bool:
    """Generated purely from literals, transitively: ``unnest([1,2])``, ``sum(1)``,
    a window over either. No datasource anywhere in the derivation means the value
    can be produced beside any component without a join (these plan as cross
    joins), so it must not count toward disconnection."""
    if concept.derivation == Derivation.CONSTANT:
        return True
    if concept.derivation == Derivation.ROOT or concept.lineage is None:
        return False
    seen = _seen if _seen is not None else set()
    if concept.address in seen:
        return True
    seen.add(concept.address)
    return all(
        _literal_derived(arg, seen)
        for arg in concept.concept_arguments
        if isinstance(arg, BuildConcept)
    )


def _anchor_nodes(concept: BuildConcept) -> list[str]:
    """Reference-graph nodes that tie a concept into the model graph: its own
    node, its default-grain node, and its direct source args' default-grain
    nodes (derived concepts may not carry their own node, but their sources do).
    """
    from trilogy.core.graph_models import concept_to_node

    nodes = [
        concept_to_node(concept),
        concept_to_node(concept.with_default_grain()),
    ]
    for arg in concept.concept_arguments:
        if isinstance(arg, BuildConcept):
            nodes.append(concept_to_node(arg.with_default_grain()))
    return nodes


def _aggregate_grain_only_parents(
    environment: BuildEnvironment,
) -> dict[str, set[str]]:
    """Map each aggregate concept address -> the addresses of its grain-only
    ``by`` keys: grain components that are NOT functional inputs of the measure.

    An aggregate can be regrouped to any grain, so an edge from it to its ``by``
    key is not a join relationship and must not bridge otherwise-unconnected
    components. ``add_concept`` adds a graph edge for every ``concept_argument``,
    and an aggregate's ``concept_arguments`` include its ``by`` keys — so without
    this, ``sum(web.measure) by store.county`` would connect the (separate) web
    and store subgraphs through ``store.county``. Mirrors the
    "aggregate up to an arbitrary grain can be joined in later" rule in
    ``calculate_graph_relevance``.
    """
    out: dict[str, set[str]] = {}
    for c in environment.concepts.values():
        if not isinstance(c, BuildConcept):
            continue
        if c.derivation != Derivation.AGGREGATE or not c.grain.components:
            continue
        if isinstance(c.lineage, BuildAggregateWrapper):
            measure = c.lineage.function.concept_arguments
        elif c.lineage is not None:
            measure = c.lineage.concept_arguments
        else:
            measure = []
        measure_addrs = {a.address for a in measure if isinstance(a, BuildConcept)}
        if not measure_addrs:
            # Literal measure (sum(1), count(1)): the grain keys ARE the row
            # demand — the aggregate cannot be computed anywhere but over its
            # grain's row set, so those edges are real join relationships.
            continue
        grain_only = set(c.grain.components) - measure_addrs
        if grain_only:
            out[c.address] = grain_only
    return out


def _component_map(
    environment: BuildEnvironment,
    g: "ReferenceGraph | None" = None,
    island_rowsets: bool = True,
    excluded_addresses: frozenset[str] = frozenset(),
) -> "tuple[dict[str, int], ReferenceGraph]":
    """Build the connectivity map node -> weakly-connected-component id, dropping
    aggregate grain-only edges (and optionally islanding rowsets) first. Shared by
    ``disconnected_components`` and the connected-equivalent suggestion path so both
    judge reachability identically. Returns the map and the graph it was built on.

    ``excluded_addresses`` are treated as absent for reachability only. A nested
    select must not reach connectivity through the outputs of the construct it is
    the body of: `yr -> rs.yr -> rowset~rs -> rs.oname -> oname` otherwise makes
    two unrelated models look joined, and the gate can never fire on the very
    query defining `rs`. Dropped from the undirected COPY, so no shared concept,
    pseudonym or graph is touched -- narrowing the environment instead is not an
    option, since surviving concepts still name these addresses as pseudonyms."""
    from trilogy.core import graph as gx
    from trilogy.core.env_processor import generate_graph

    g = g if g is not None else generate_graph(environment)

    # Compute connectivity on an undirected copy so we can drop aggregate
    # grain-only edges without mutating the shared resolution graph.
    cg = g.to_undirected()
    grain_only = _aggregate_grain_only_parents(environment)
    if grain_only:
        for node, concept in g.concepts.items():
            keys = grain_only.get(concept.address)
            if not keys or node not in cg:
                continue
            for neighbor in list(gx.neighbors(cg, node)):
                neighbor_concept = g.concepts.get(neighbor)
                if neighbor_concept is not None and neighbor_concept.address in keys:
                    cg.remove_edge(node, neighbor)

    if island_rowsets:
        island_rowsets_for_connectivity(g, cg, grain_only)
    else:
        # Even without islanding, one rowset's co-produced outputs are a single
        # sub-query — weld them so a join declared INSIDE the rowset body
        # (invisible at this level) doesn't split its own handles.
        link_rowset_outputs_for_connectivity(g, cg)

    if excluded_addresses:
        for node, concept in g.concepts.items():
            if concept.address in excluded_addresses and node in cg:
                cg.remove_node(node)

    comp_of: dict[str, int] = {}
    for i, component in enumerate(gx.connected_components(cg)):
        for node in component:
            comp_of[node] = i
    return comp_of, g


def disconnected_components(
    environment: BuildEnvironment,
    concepts: list[BuildConcept],
    g: "ReferenceGraph | None" = None,
    island_rowsets: bool = True,
    excluded_addresses: frozenset[str] = frozenset(),
) -> list[list[BuildConcept]]:
    """Partition concepts by true join reachability: two concepts share a group
    iff their reference-graph nodes are in the same weakly-connected component
    (i.e. some join / FK / merge path relates them). >1 group means a genuinely
    unconnected set — a real missing join/merge, not merely a grain conflict.

    Pass the resolution's graph as ``g`` to reuse it; otherwise one is built from
    ``environment``. Crossjoinable (single-row/constant) concepts are skipped.
    Aggregate grain-only ``by`` edges are dropped first (see
    ``_aggregate_grain_only_parents``) so a regroupable aggregate never bridges
    two otherwise-disconnected models through its grouping key.

    ``island_rowsets`` controls rowset islanding (see
    ``island_rowsets_for_connectivity``): when
    set, a base concept reachable only by navigating into a rowset's derivation is
    not treated as a real join path. This is correct as a *post-failure* message
    refiner (called once discovery has already failed independently), but as a
    *pre-check gate* it false-positives on legitimate rowset join-backs (a base
    key that IS a rowset output, or a concept DERIVED from one) — so the
    pre-gate disables it and lets discovery decide. Defaults to on.

    See ``_component_map`` for ``excluded_addresses``.
    """
    comp_of, _ = _component_map(environment, g, island_rowsets, excluded_addresses)

    # concept -> the component id it resolves into; a concept whose nodes are
    # absent from the graph gets a synthetic per-address component so it surfaces
    # rather than silently vanishing.
    buckets: dict[object, list[BuildConcept]] = {}
    for concept in concepts:
        if _crossjoinable(concept):
            continue
        cid: object | None = None
        for node in _anchor_nodes(concept):
            if node in comp_of:
                cid = comp_of[node]
                break
        if cid is None:
            cid = f"orphan::{concept.address}"
        buckets.setdefault(cid, []).append(concept)

    groups = [sorted(grp, key=lambda c: c.address) for grp in buckets.values()]
    return sorted(groups, key=lambda grp: min(c.address for c in grp))


def _output_is_rootless(outputs: list[BuildConcept]) -> bool:
    """Every output has no datasource dependency: a constant, a single-row scalar,
    or a value generated purely from literals (lineage but no concept arguments,
    e.g. ``unnest([1,2,3,4])``). Such an output cannot correlate with any
    datasource, so a disconnected WHERE on a real model can only be an EXISTS gate."""
    if not outputs:
        return False
    return all(
        c.granularity == Granularity.SINGLE_ROW
        or c.derivation == Derivation.CONSTANT
        or (
            c.lineage is not None
            and c.derivation != Derivation.ROOT
            and not any(isinstance(a, BuildConcept) for a in c.concept_arguments)
        )
        for c in outputs
    )


def _is_global_aggregate_gate(
    group: list[BuildConcept], output_addresses: set[str]
) -> bool:
    """True when a disconnected subgraph is a pure WHERE aggregate gate rather than
    a missing join: every member is an aggregate row-arg (not an output) at a grain
    absent from the outputs. Such a condition is a global filter gate — the planner
    bridges it via the gate's grain and cross-joins/dedups the (constant) outputs
    (e.g. `where sum(x) by name < ... select <const>`). A disconnected
    raw-column arg (`where bv > 0`) implies a row-level correlation that genuinely
    needs a join, so it is NOT a gate and must still raise."""
    return all(
        c.address not in output_addresses and c.derivation == Derivation.AGGREGATE
        for c in group
    )


def _collect_subselect_comparisons(node: object) -> list[BuildSubselectComparison]:
    if isinstance(node, BuildSubselectComparison):
        return [node]
    if isinstance(node, BuildConditional):
        return _collect_subselect_comparisons(
            node.left
        ) + _collect_subselect_comparisons(node.right)
    if isinstance(node, BuildParenthetical):
        return _collect_subselect_comparisons(node.content)
    return []


def membership_span_note(
    conditions: "BuildWhereClause | None",
    subgraphs: list[list[BuildConcept]],
    environment: BuildEnvironment,
    g: "ReferenceGraph | None" = None,
    island_rowsets: bool = True,
) -> str | None:
    """When the disconnected-subgraph error is about to be raised and the WHERE
    holds membership/existence predicates whose left side sits in one reported
    subgraph while the right side derives from another, name them. A membership
    semi-join only filters its left side — it never relates the two sides for
    outputs or grouping — and its right-side concepts plan as a separate island,
    so without this note they are absent from the reported components and the
    error reads as if the authored predicate was silently dropped."""
    if conditions is None or len(subgraphs) < 2:
        return None
    comparisons = _collect_subselect_comparisons(conditions.conditional)
    if not comparisons:
        return None
    comp_of, _ = _component_map(environment, g, island_rowsets)

    # mirror disconnected_components: a subgraph member's component is its FIRST
    # mapped anchor (an aggregate's later anchors reach into its `by` keys'
    # components and would over-claim)
    subgraph_of_component: dict[int, int] = {}
    for idx, group in enumerate(subgraphs):
        for concept in group:
            for node in _anchor_nodes(concept):
                if node in comp_of:
                    subgraph_of_component.setdefault(comp_of[node], idx)
                    break

    def subgraph_ids(concepts: list[BuildConcept]) -> set[int]:
        # operands consider ALL anchors: an islanded rowset concept's own node
        # sits in its island, but its source args tie back to a real model
        ids = set()
        for concept in concepts:
            for node in _anchor_nodes(concept):
                cid = comp_of.get(node)
                if cid is not None and cid in subgraph_of_component:
                    ids.add(subgraph_of_component[cid])
        return ids

    notes = []
    for comparison in comparisons:
        left = get_concept_row_arguments(comparison.left)
        right = get_concept_arguments(comparison.right)
        spans = subgraph_ids(left) | subgraph_ids(right)
        if len(spans) < 2:
            continue
        render_left = ", ".join(_strip_default_namespace(c.address) for c in left)
        render_right = ", ".join(_strip_default_namespace(c.address) for c in right)
        notes.append(f"`({render_left}) {comparison.operator.value} ({render_right})`")
    if not notes:
        return None
    rendered = "; ".join(notes)
    return (
        f"Note: the membership predicate(s) {rendered} span these subgraphs, but "
        "membership only filters rows on its left side — it does not join the two "
        "sides, so it cannot relate them for outputs or grouping. To combine "
        "values from both sides, author a query-scoped join or a merge on shared "
        "keys."
    )


def raise_if_disconnected_for(
    outputs: list[BuildConcept],
    conditions: "BuildWhereClause | None",
    environment: BuildEnvironment,
    g: "ReferenceGraph | None" = None,
    island_rowsets: bool = True,
    line_number: int | None = None,
    excluded_addresses: frozenset[str] = frozenset(),
) -> None:
    """Connectivity gate for a select's required concepts (its outputs plus any
    WHERE row args): raise the typed subgraph error when they span unconnected
    reference-graph components. Crossjoinable (single-row/constant) concepts are
    skipped by ``disconnected_components``, so e.g. two ungrouped scalar aggregates
    still resolve via cross-join. A disconnected subgraph consisting solely of
    aggregate WHERE row-args is a global filter gate (not a missing join) and is
    dropped before counting — see ``_is_global_aggregate_gate``. Shared verbatim by
    the top-level select and nested rowset inner selects — rowset discovery is
    recursive query discovery, so the connectivity diagnostic must be identical. See
    ``disconnected_components`` for ``island_rowsets`` (the v4 pre-gate passes
    ``False``)."""
    concepts = list(outputs)
    output_addresses = {c.address for c in concepts}
    if conditions:
        concepts += [
            c for c in conditions.row_arguments if c.address not in output_addresses
        ]
    subgraphs = disconnected_components(
        environment,
        concepts,
        g,
        island_rowsets=island_rowsets,
        excluded_addresses=excluded_addresses,
    )
    outputs_rootless = _output_is_rootless(outputs)
    subgraphs = [
        grp
        for grp in subgraphs
        if not _is_global_aggregate_gate(grp, output_addresses)
        and not (
            outputs_rootless and all(c.address not in output_addresses for c in grp)
        )
    ]
    if len(subgraphs) > 1:
        message = format_disconnected_subgraphs_error(
            subgraphs, environment, g, island_rowsets, line_number, excluded_addresses
        )
        note = membership_span_note(
            conditions, subgraphs, environment, g, island_rowsets
        )
        if note:
            message = f"{message}\n{note}"
        raise DisconnectedConceptsException(
            message,
            subgraphs=[[c.address for c in group] for group in subgraphs],
        )


def connected_equivalent_suggestions(
    environment: BuildEnvironment | None,
    subgraphs: list[list[BuildConcept]],
    g: "ReferenceGraph | None" = None,
    island_rowsets: bool = True,
    excluded_addresses: frozenset[str] = frozenset(),
) -> list[tuple[str, str]]:
    """Detect the separate-import mistake: a model imported a second time as a
    disconnected copy, so concepts like ``date.year`` split off from a measure that
    already reaches them via a chainable import (``all_sales.date.year``).

    Steer toward the largest subgraph (the connected target). For each concept in a
    smaller, stranded subgraph, look for an environment concept whose address is the
    stranded path under one extra namespace prefix AND that lands in the target's
    connected component; shortest such prefix wins. Returns
    ``(stranded_address, connected_address)`` pairs, or ``[]`` when no twin exists
    (the caller then falls back to the generic join/merge hint). Reachability is
    judged with ``_component_map`` — same ``island_rowsets``/``excluded_addresses``
    as the split — so it matches ``disconnected_components``."""
    if environment is None:
        return []
    comp_of, _ = _component_map(environment, g, island_rowsets, excluded_addresses)

    def component_of(concept: BuildConcept) -> int | None:
        for node in _anchor_nodes(concept):
            if node in comp_of:
                return comp_of[node]
        return None

    target = max(subgraphs, key=len)
    target_comps = {component_of(c) for c in target} - {None}
    if not target_comps:
        return []

    suggestions: list[tuple[str, str]] = []
    for group in subgraphs:
        if group is target:
            continue
        for concept in group:
            stranded = concept.address
            if VIRTUAL_CONCEPT_PREFIX in stranded:
                continue
            suffix = "." + stranded.removeprefix(f"{DEFAULT_NAMESPACE}.")
            best: str | None = None
            for candidate in environment.concepts.values():
                addr = candidate.address
                if addr == stranded or addr in excluded_addresses:
                    continue
                if VIRTUAL_CONCEPT_PREFIX in addr or not addr.endswith(suffix):
                    continue
                if component_of(candidate) not in target_comps:
                    continue
                if best is None or len(addr) < len(best):
                    best = addr
            if best is not None:
                suggestions.append((stranded, best))
    return suggestions


def _strip_default_namespace(addr: str) -> str:
    # default namespace is implicit; keep other namespaces qualified
    default_prefix = f"{DEFAULT_NAMESPACE}."
    return addr.removeprefix(default_prefix)


def format_disconnected_subgraphs_error(
    subgraphs: list[list[BuildConcept]],
    environment: BuildEnvironment | None = None,
    g: "ReferenceGraph | None" = None,
    island_rowsets: bool = True,
    line_number: int | None = None,
    excluded_addresses: frozenset[str] = frozenset(),
) -> str:
    def render(group: list[BuildConcept]) -> str:
        addrs = sorted(c.address for c in group)
        # drop internal _virt_* scaffolding, but keep raw if that empties a group
        cleaned = [a for a in addrs if VIRTUAL_CONCEPT_PREFIX not in a]
        return (
            "{"
            + ", ".join(_strip_default_namespace(a) for a in (cleaned or addrs))
            + "}"
        )

    rendered = "; ".join(render(group) for group in subgraphs)
    location = f" (statement at line {line_number})" if line_number else ""
    head = (
        "Discovery error: cannot merge all concepts into one connected query"
        f"{location}. The requested concepts split into {len(subgraphs)} "
        f"disconnected subgraphs: {rendered}."
    )

    suggestions = (
        connected_equivalent_suggestions(
            environment, subgraphs, g, island_rowsets, excluded_addresses
        )
        if environment is not None
        else []
    )
    if suggestions:
        lines = "\n".join(
            f"  - `{disc}` is disconnected — did you mean `{conn}`? "
            "(connected to the other concepts)"
            for disc, conn in suggestions
        )
        example = suggestions[0][1]
        return (
            f"{head}\n{lines}\nThese look like separately-imported copies of models "
            "already reachable through a connected import; chain through that path "
            f"(e.g. `{example}`) instead of importing a second, disconnected copy."
        )
    return f"{head} Are you missing a join or merge statement to relate them?"


def _filter_hidden_concepts(
    output_concepts: list[BuildConcept],
) -> list[BuildConcept]:
    """The concepts a FILTER output hides inside its lineage: the value it filters
    and its ``? <cond>`` row-arguments. The top-level disconnect check only sees
    outputs + WHERE row-args, so a filter whose condition can't be related to the
    value it filters never splits — it dead-ends on the opaque virtual address.
    Surfacing these lets the standard reachability check do its job."""
    extra: list[BuildConcept] = []
    for c in output_concepts:
        if not isinstance(c.lineage, BuildFilterItem):
            continue
        extra.extend(c.lineage.content_concept_arguments)
        extra.extend(c.lineage.where.row_arguments)
    return extra


def filter_disconnect_context(output_concepts: list[BuildConcept]) -> str:
    """Specific context appended to the general disconnected-subgraphs message when
    the split runs through a filter on a rowset output. The filtered value (a rowset
    output) and the condition concept genuinely can't be related without relating the
    two — so name the concrete ways to do that: pull the condition into the rowset,
    compare via an existence/membership set, or join the rowset back to the source.
    Empty string when no such filter is involved."""
    for c in output_concepts:
        if not isinstance(c.lineage, BuildFilterItem):
            continue
        content = c.lineage.content
        if not isinstance(content, BuildConcept) or not isinstance(
            content.lineage, BuildRowsetItem
        ):
            continue
        rs = content.lineage.rowset
        missing = unique(
            [
                a
                for a in c.lineage.where.row_arguments
                if a.address not in set(rs.derived_concepts)
            ],
            "address",
        )
        if missing:
            names = ", ".join(f"`{a.address}`" for a in missing)
            return (
                f" Here {names} is referenced only inside a filter on rowset output "
                f"`{content.address}` (rowset `{rs.name}`), and isn't related to it "
                f"without a join. Relate them by adding {names} to rowset `{rs.name}`"
                "'s select, by an existence comparison against a base-concept set "
                f"(e.g. `{content.address} in (<base concept> ? <condition>)`), or by "
                "joining the rowset back to the source."
            )
    return ""


def raise_if_filter_disconnected(
    output_concepts: list[BuildConcept],
    environment: BuildEnvironment,
    g: "ReferenceGraph | None" = None,
    extra_required: list[BuildConcept] | None = None,
    island_rowsets: bool = True,
) -> None:
    """Re-run the reachability check with FILTER outputs' hidden condition concepts
    surfaced (see ``_filter_hidden_concepts``). When that splits the set, raise the
    standard named-subgraph error — same 'add a join or merge' diagnostic as any
    disconnected grouping — plus a rowset-filter-specific hint. No-op otherwise, so
    the caller falls through to the generic connectivity error.

    ``island_rowsets`` as in ``disconnected_components``, and it is NOT simply
    "off for pre-check gates" — it turns on whether the caller reads rowset
    outputs across the boundary. A WHERE-scope gate resolves its concepts
    against the base model, where a key that happens to be a rowset output is a
    legitimate join-back, so it must pass False. A HAVING-scope gate filters the
    statement's outputs, where the rowset is genuinely opaque and islanding is
    the whole diagnostic, so it keeps the default."""
    # Drop the FILTER outputs themselves: their connectivity is fully captured by
    # the surfaced content + condition concepts, and the virtual filter concept
    # has no condition edges in the graph (see add_concept), so keeping it would
    # surface a bare `_virt_*` orphan subgraph when its content is islanded.
    real_outputs = [
        c for c in output_concepts if not isinstance(c.lineage, BuildFilterItem)
    ]
    required = unique(
        real_outputs
        + list(extra_required or [])
        + _filter_hidden_concepts(output_concepts),
        "address",
    )
    groups = disconnected_components(environment, required, g, island_rowsets)
    if len(groups) > 1:
        raise DisconnectedConceptsException(
            format_disconnected_subgraphs_error(groups, environment, g)
            + filter_disconnect_context(output_concepts),
            subgraphs=[[c.address for c in group] for group in groups],
        )
