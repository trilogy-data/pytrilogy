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
    # the union of all parent grains, minus joins that do not change grain
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
            # an unjoined source still contributes grain unless it is used only
            # in a subselect; the existence check is a proxy for that
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
    # covers its source keys and a comp_grain carrying them does not look like
    # extra grain. When EVERY target component is itself an aggregate (an
    # aggregate-of-aggregate distribution: ``count(x) by y`` then ``count(y)``),
    # exclude aggregate by-keys from coverage: nothing anchors the output to
    # the by-grain, so an upstream at the by-grain is strictly finer and a
    # regroup is required.
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

    # a difference of only properties whose keys sit in the source grain does
    # not need a group
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
    # Lineage DAGs are diamond-shaped, so unmemoized recursion is exponential.
    # BuildConcepts are immutable during resolution, so the (nested=True) result
    # is memoized by identity for the lifetime of the top-level call.
    if nested:
        memoized = cache.get(id(base))
        if memoized is not None:
            return memoized
    upstream: set[str] = set()
    if nested:
        upstream.add(base.address)
    if base.lineage:
        for x in base.lineage.concept_arguments:
            # A value derived from a rowset has ALL rowset items upstream. The
            # rowset's derived_concepts are already namespaced; splicing the
            # rowset name onto the body's addresses would miss real upstreams.
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
    and an aggregate's ``concept_arguments`` include its ``by`` keys, so
    ``sum(a.measure) by b.key`` would otherwise connect the separate ``a`` and
    ``b`` subgraphs. Mirrors the "aggregate up to an arbitrary grain can be
    joined in later" rule in ``calculate_graph_relevance``.
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
            # demand, so those edges are real join relationships.
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
    select must not reach connectivity through the outputs of the construct it
    is the body of (`yr -> rs.yr -> rowset~rs -> rs.oname -> oname` makes two
    unrelated models look joined). Dropped from the undirected COPY only:
    surviving concepts still name these addresses as pseudonyms, so narrowing
    the environment is not an option."""
    from trilogy.core import graph as gx
    from trilogy.core.env_processor import generate_graph

    g = g if g is not None else generate_graph(environment)

    # undirected copy, so edges can be dropped without mutating the shared graph
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
        # sub-query; weld them so a join declared INSIDE the rowset body
        # (invisible at this level) does not split its own outputs.
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
    iff their reference-graph nodes are in the same weakly-connected component.
    More than one group means a genuinely unconnected set, a missing join or
    merge rather than a grain conflict.

    Pass the resolution's graph as ``g`` to reuse it; otherwise one is built
    from ``environment``. Crossjoinable (single-row/constant) concepts are
    skipped. Aggregate grain-only ``by`` edges are dropped first (see
    ``_aggregate_grain_only_parents``).

    ``island_rowsets`` (see ``island_rowsets_for_connectivity``): when set, a
    base concept reachable only by navigating into a rowset's derivation is not
    a real join path. Correct as a post-failure message refiner, but as a
    pre-check gate it false-positives on legitimate rowset join-backs (a base
    key that IS a rowset output, or a concept DERIVED from one), so the
    pre-gate disables it and lets discovery decide.

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
    """True when a disconnected subgraph is a pure WHERE aggregate gate rather
    than a missing join: every member is an aggregate row-arg (not an output).
    The planner bridges such a gate via its grain and cross-joins the constant
    outputs. A disconnected raw-column arg implies a row-level correlation
    that genuinely needs a join, so it is NOT a gate and must still raise."""
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
    """Name membership/existence predicates in the WHERE whose left side sits in
    one reported subgraph while the right side derives from another. A
    membership semi-join only filters its left side and never relates the two
    sides for outputs or grouping; its right-side concepts plan as a separate
    island and would otherwise be absent from the reported components."""
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
        "membership only filters rows on its left side, it does not join the two "
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
    """Connectivity gate for a select's required concepts (outputs plus WHERE
    row args): raise the typed subgraph error when they span unconnected
    reference-graph components. Crossjoinable concepts are skipped by
    ``disconnected_components``. A disconnected subgraph consisting solely of
    aggregate WHERE row-args is a global filter gate, not a missing join, and
    is dropped before counting (``_is_global_aggregate_gate``). Shared by the
    top-level select and nested rowset inner selects so the diagnostic is
    identical. See ``disconnected_components`` for ``island_rowsets``."""
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
    judged with ``_component_map`` under the same ``island_rowsets`` and
    ``excluded_addresses`` as the split, so it matches
    ``disconnected_components``."""
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
            f"  - `{disc}` is disconnected, did you mean `{conn}`? "
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
    """The concepts a FILTER output hides inside its lineage: the value it
    filters and its ``? <cond>`` row-arguments. The top-level disconnect check
    only sees outputs + WHERE row-args, so a filter whose condition can't be
    related to the value it filters never splits; it dead-ends on the opaque
    virtual address. Surfacing these lets the reachability check see it."""
    extra: list[BuildConcept] = []
    for c in output_concepts:
        if not isinstance(c.lineage, BuildFilterItem):
            continue
        extra.extend(c.lineage.content_concept_arguments)
        extra.extend(c.lineage.where.row_arguments)
    return extra


def filter_disconnect_context(output_concepts: list[BuildConcept]) -> str:
    """Context appended to the disconnected-subgraphs message when the split
    runs through a filter on a rowset output: names the concrete ways to relate
    the filtered value and the condition concept (pull the condition into the
    rowset, compare via a membership set, or join the rowset back to the
    source). Empty string when no such filter is involved."""
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


def _reachable_components(
    concept: BuildConcept,
    environment: BuildEnvironment,
    comp_of: dict[str, int],
) -> set[int]:
    """Component ids the concept's own row sources land in, walking its whole
    lineage. A single-row aggregate has no component of its own; its sources do."""
    found: set[int] = set()
    for address in {concept.address} | get_upstream_concepts(concept):
        resolved = environment.concepts.get(address)
        if resolved is None or _crossjoinable(resolved):
            continue
        for node in _anchor_nodes(resolved):
            if node in comp_of:
                found.add(comp_of[node])
    return found


def raise_if_where_population_split(
    outputs: list[BuildConcept],
    conditions: "BuildWhereClause | None",
    environment: BuildEnvironment,
    g: "ReferenceGraph | None" = None,
    line_number: int | None = None,
) -> None:
    """Post-failure refiner: a top-level WHERE defines ONE row population, so every
    output must read from a source the filter can reach.

    ``raise_if_disconnected_for`` skips single-row outputs as freely crossjoinable.
    That is right with no WHERE (two scalar aggregates over unrelated models are a
    well-defined cross join) and wrong with one, because an output whose source no
    join relates to the filter would ride through silently unfiltered. Anchor those
    skipped outputs by their upstream row sources instead and name the split. Only
    called once discovery has already failed, so it can only sharpen the message."""
    if conditions is None:
        return
    filter_args = [c for c in conditions.row_arguments if not _crossjoinable(c)]
    if not filter_args:
        return
    comp_of, _ = _component_map(environment, g)
    filter_components: set[int] = set()
    for arg in filter_args:
        filter_components |= _reachable_components(arg, environment, comp_of)
    if not filter_components:
        return
    stranded = [
        c
        for c in outputs
        if (components := _reachable_components(c, environment, comp_of))
        and not components & filter_components
    ]
    if not stranded:
        return
    filter_addrs = sorted(_strip_default_namespace(c.address) for c in filter_args)
    stranded_addrs = sorted(_strip_default_namespace(c.address) for c in stranded)
    location = f" (statement at line {line_number})" if line_number else ""
    raise DisconnectedConceptsException(
        f"WHERE input(s) {filter_addrs} cannot restrict output(s) "
        f"{stranded_addrs}{location}: no join or merge relates the filter's source "
        "to the source of those outputs, so the WHERE has no single row population "
        "to define -- the outputs would cross-join in unfiltered. Add a join/merge "
        "relating them, or scope the filter to the source it belongs to with an "
        "inline filtered aggregate (e.g. `sum(x ? <condition>)`).",
        subgraphs=[
            [c.address for c in stranded],
            [c.address for c in filter_args],
        ],
    )


def raise_if_filter_disconnected(
    output_concepts: list[BuildConcept],
    environment: BuildEnvironment,
    g: "ReferenceGraph | None" = None,
    extra_required: list[BuildConcept] | None = None,
    island_rowsets: bool = True,
) -> None:
    """Re-run the reachability check with FILTER outputs' hidden condition
    concepts surfaced (see ``_filter_hidden_concepts``). When that splits the
    set, raise the standard named-subgraph error plus a rowset-filter-specific
    hint. No-op otherwise, so the caller falls through to the generic error.

    ``island_rowsets`` as in ``disconnected_components``; it turns on whether
    the caller reads rowset outputs across the boundary. A WHERE-scope gate
    resolves its concepts against the base model, where a key that is also a
    rowset output is a legitimate join-back, so it must pass False. A
    HAVING-scope gate filters the statement's outputs, where the rowset is
    genuinely opaque, so it keeps the default."""
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
