"""Stage 1: walk every mandatory concept (and condition input) back to its
roots and produce a DAG of concept-level lineage + d1→d0 constraint edges.

For each concept added, an upstream-fetcher (dispatched on
`concept.derivation`) decides what additional concepts the input CTE for
this node must contain. The default fetcher returns
`lineage.concept_arguments` — the parents the expression directly
consumes. Specialized fetchers (AGGREGATE, FILTER, WINDOW, SUBSELECT) add
row-identity concepts that aren't visible from the lineage walk alone:
property keys, grain components, partition keys. Everything the fetcher
returns gets a `kind="lineage"` edge — an aggregate's grain keys aren't
optional metadata, they're what keeps row identity intact through the SUM.

This mirrors the per-derivation `resolve_*_parent_concepts` helpers used
by the v3 generators (`gen_group_node`, `gen_filter_node`,
`gen_window_node`, `gen_subselect_node`).
"""

from typing import Callable, List

import networkx as nx

from trilogy.core.enums import Derivation, Purpose
from trilogy.core.models.author import MultiSelectLineage, SelectLineage
from trilogy.core.models.build import (
    BuildAggregateWrapper,
    BuildConcept,
    BuildFilterItem,
    BuildRowsetItem,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.condition_utility import decompose_condition
from trilogy.core.processing.node_generators.common import (
    _walk_aggregate_grain_inputs,
)

from .constants import LABEL_DEPTH, ROW_SHAPE_BARRIER_DERIVATIONS

UpstreamFetcher = Callable[[BuildConcept, BuildEnvironment], List[BuildConcept]]


def classify_depth(concept: BuildConcept, condition_addresses: set[str]) -> str:
    """Tag a concept by its placement role in the plan."""
    if concept.address in condition_addresses:
        return "d1"
    if concept.derivation in ROW_SHAPE_BARRIER_DERIVATIONS:
        return "d0"
    return "d*"


def _lineage_args(
    concept: BuildConcept, environment: BuildEnvironment
) -> List[BuildConcept]:
    """The default — concepts the lineage's expression directly consumes."""
    if concept.lineage is None:
        return []
    return [
        environment.concepts.get(p.address, p)
        for p in concept.lineage.concept_arguments
    ]


def _upstream_default(
    concept: BuildConcept, environment: BuildEnvironment
) -> List[BuildConcept]:
    return _lineage_args(concept, environment)


def _upstream_aggregate(
    concept: BuildConcept, environment: BuildEnvironment
) -> List[BuildConcept]:
    """AGGREGATE: lineage args plus row-identity concepts of each function
    arg (property keys, rowset grain). Stops at inner aggregate boundaries
    via `_walk_aggregate_grain_inputs`."""
    base = list(_lineage_args(concept, environment))
    if isinstance(concept.lineage, BuildAggregateWrapper):
        for arg in concept.lineage.function.arguments:
            if isinstance(arg, BuildConcept):
                base.extend(_walk_aggregate_grain_inputs(arg, environment))
    return base


def _upstream_filter(
    concept: BuildConcept, environment: BuildEnvironment
) -> List[BuildConcept]:
    """FILTER: lineage args plus property keys of the filtered concept
    (matches `resolve_filter_parent_concepts`). A filter over a property
    needs the property's keys to keep the row stream identifiable."""
    base = list(_lineage_args(concept, environment))
    if isinstance(concept.lineage, BuildFilterItem):
        direct_parent = concept.lineage.content
        if (
            isinstance(direct_parent, BuildConcept)
            and direct_parent.purpose in (Purpose.PROPERTY, Purpose.METRIC)
            and direct_parent.keys
        ):
            base += [
                environment.concepts[k]
                for k in direct_parent.keys
                if k in environment.concepts
            ]
    return base


def _grain_and_keys(
    concept: BuildConcept, environment: BuildEnvironment
) -> List[BuildConcept]:
    extras: list[BuildConcept] = []
    if concept.grain:
        for g in concept.grain.components:
            if g in environment.concepts:
                extras.append(environment.concepts[g])
    if concept.keys:
        for k in concept.keys:
            if k in environment.concepts:
                extras.append(environment.concepts[k])
    return extras


def _upstream_window(
    concept: BuildConcept, environment: BuildEnvironment
) -> List[BuildConcept]:
    """WINDOW: lineage args plus grain components and keys (matches
    `resolve_window_parent_concepts`)."""
    return list(_lineage_args(concept, environment)) + _grain_and_keys(
        concept, environment
    )


def _upstream_subselect(
    concept: BuildConcept, environment: BuildEnvironment
) -> List[BuildConcept]:
    """SUBSELECT: lineage args plus grain components (matches
    `resolve_subselect_parent_concepts`)."""
    base = list(_lineage_args(concept, environment))
    if concept.grain:
        for g in concept.grain.components:
            if g in environment.concepts:
                base.append(environment.concepts[g])
    return base


_UPSTREAM: dict[Derivation, UpstreamFetcher] = {
    Derivation.AGGREGATE: _upstream_aggregate,
    Derivation.FILTER: _upstream_filter,
    Derivation.WINDOW: _upstream_window,
    Derivation.SUBSELECT: _upstream_subselect,
}


def node_id(label: str, address: str) -> str:
    """Compose a concept-graph node key from (label, address).

    For the default outer-query label (``""``), the key is just the bare
    address so existing code that reads addresses as keys keeps working.
    For a labeled sub-graph (a rowset's inner walk, label = rowset name),
    the key is prefixed: ``"[q5_results]local.channel_label"``. The
    bracketed prefix is what keeps the inner and outer copies of the
    same concept distinct when both appear in the graph."""
    return f"[{label}]{address}" if label else address


def _add_concept(
    concept: BuildConcept,
    environment: BuildEnvironment,
    graph: nx.DiGraph,
    condition_addresses: set[str],
    label: str = "",
) -> None:
    nid = node_id(label, concept.address)
    if nid in graph:
        return
    graph.add_node(
        nid,
        address=concept.address,
        label=label,
        derivation=concept.derivation.value,
        purpose=concept.purpose.value,
        granularity=concept.granularity.value,
        depth_label=classify_depth(concept, condition_addresses),
        grain_components=(
            frozenset(concept.grain.components) if concept.grain else frozenset()
        ),
    )

    # Rowset boundary: a ROWSET concept is the outer's "handle" on a
    # sub-query. From the outer graph's perspective it's a leaf — the
    # actual lineage lives inside the rowset's inner select, which we
    # walk separately under `label=rowset.name`. Stopping here is what
    # prevents the outer BASIC group (e.g. q05's `local.sales`) from
    # absorbing the rowset's internal BASIC computations (q05's
    # `q5_results.sales_metric`) and forming a group-level cycle.
    if concept.derivation == Derivation.ROWSET:
        return

    # Per-derivation upstream fetcher (see `_UPSTREAM`): everything the
    # fetcher returns is a real lineage dependency — the concept's input
    # CTE has to contain it for this node to render correctly. An
    # aggregate's grain keys aren't optional metadata; they're what keeps
    # row identity intact through the SUM. Same story for window
    # partition keys and filter property keys. So every fetcher result
    # gets a lineage edge, not just `concept_arguments`.
    fetcher = _UPSTREAM.get(concept.derivation, _upstream_default)
    for upstream in fetcher(concept, environment):
        _add_concept(upstream, environment, graph, condition_addresses, label=label)
        graph.add_edge(node_id(label, upstream.address), nid, kind="lineage")


def _rowset_inner_outputs(
    rowset_concept: BuildConcept,
    environment: BuildEnvironment,
) -> tuple[list[BuildConcept], list[BuildWhereClause]]:
    """For a ROWSET concept (lineage = BuildRowsetItem), return the
    inner select's output components (resolved against `environment`) and
    its inner WHERE clauses. The outputs become the mandatory list for
    the rowset's labeled sub-graph; the wheres become its conditions —
    same shape as the outer planner's inputs."""
    lineage = rowset_concept.lineage
    if not isinstance(lineage, BuildRowsetItem):
        return [], []
    select = lineage.rowset.select
    if not isinstance(select, (SelectLineage, MultiSelectLineage)):
        return [], []
    # `select` is the AUTHOR-level lineage; its `output_components` /
    # `selection` carry author-level ConceptRefs by address. Resolve each
    # against the build environment to recover the BuildConcept used by
    # _add_concept.
    raw = select.output_components
    outputs = [
        environment.concepts[c.address]
        for c in raw
        if c.address in environment.concepts
    ]
    # `where_clause` on a SelectLineage / MultiSelectLineage is the
    # author-level WhereClause; the build env materializes a build
    # version somewhere, but for the inner walk we only need its
    # row_arguments to classify d1. Skip the build dance for now and
    # return any pre-built where the rowset attached.
    conditions: list[BuildWhereClause] = []
    where = getattr(select, "where_clause", None)
    if where is not None and isinstance(where, BuildWhereClause):
        conditions.append(where)
    return outputs, conditions


def build_concept_graph(
    mandatory_list: List[BuildConcept],
    environment: BuildEnvironment,
    conditions: list[BuildWhereClause],
) -> nx.DiGraph:
    """Build the concept-level DAG. Constraint edges (d1→d0) record the
    invariant that filter inputs must be available above any row-shape barrier
    that consumes their filtered output.

    Rowset handling: a ROWSET concept in the outer mandatory list is
    walked as a leaf (no lineage edges) by `_add_concept`, and after the
    outer walk completes we discover every ROWSET node and build its
    inner sub-graph under `label=rowset.name`. The labeled sub-graph's
    nodes use keys like ``"[q5_results]local.channel_label"`` so they
    can't collide with an outer-namespace copy of the same address. This
    is what keeps the outer query's BASIC groups independent of any
    rowset's internal BASICs (which would otherwise get bucketed
    together by `partition_basics_by_subset_grain` and form a group-
    level cycle through the rowset)."""
    graph: nx.DiGraph = nx.DiGraph()
    condition_addresses = {
        c.address for clause in conditions for c in clause.concept_arguments
    }
    for concept in mandatory_list:
        _add_concept(concept, environment, graph, condition_addresses)
    for clause in conditions:
        for concept in clause.concept_arguments:
            resolved = environment.concepts.get(concept.address, concept)
            _add_concept(resolved, environment, graph, condition_addresses)

    # Walk each ROWSET leaf in the outer graph. Multiple outer concepts
    # may all point at the same rowset (q05: channel, id, sales_metric,
    # returns_metric, profit_metric all live in q5_results) — dedup by
    # rowset name so we only walk the inner select once.
    seen_rowsets: set[str] = set()
    pending_rowsets: list[BuildConcept] = []
    for nid, data in list(graph.nodes(data=True)):
        if data.get("derivation") != Derivation.ROWSET.value:
            continue
        if data.get("label", ""):
            # Skip rowsets discovered inside an already-labeled sub-graph;
            # nested rowsets are handled by the outer recursion below.
            continue
        address = data.get("address", nid)
        concept = environment.concepts.get(address)
        if concept is None or not isinstance(concept.lineage, BuildRowsetItem):
            continue
        rowset_name = concept.lineage.rowset.name
        if rowset_name in seen_rowsets:
            continue
        seen_rowsets.add(rowset_name)
        pending_rowsets.append(concept)

    for rowset_concept in pending_rowsets:
        rowset_name = rowset_concept.lineage.rowset.name
        inner_outputs, inner_conditions = _rowset_inner_outputs(
            rowset_concept, environment
        )
        inner_condition_addresses = {
            c.address
            for clause in inner_conditions
            for c in clause.concept_arguments
        }
        for inner_concept in inner_outputs:
            _add_concept(
                inner_concept,
                environment,
                graph,
                inner_condition_addresses,
                label=rowset_name,
            )
        for clause in inner_conditions:
            for c in clause.concept_arguments:
                resolved = environment.concepts.get(c.address, c)
                _add_concept(
                    resolved,
                    environment,
                    graph,
                    inner_condition_addresses,
                    label=rowset_name,
                )

    # Add a constraint edge from every d1 to every d0 — except when a
    # lineage edge already connects them. NetworkX merges duplicate edges
    # (last write wins), so overwriting would silently demote the lineage
    # relationship to "constraint only" and break downstream group-graph
    # construction (the agg group would lose its by-clause inputs because
    # those happen to be d1 conditions).
    #
    # Constraint edges are within-label only. Cross-label d1/d0 pairs are
    # unrelated planning decisions: a rowset's inner d0 barrier doesn't
    # need to sit above the outer query's d1 condition (they live in
    # different sub-graphs).
    nodes_by_label_and_depth: dict[tuple[str, str], list[str]] = {}
    for n, d in graph.nodes(data=True):
        key = (d.get("label", ""), d.get(LABEL_DEPTH, "d*"))
        nodes_by_label_and_depth.setdefault(key, []).append(n)
    labels_present = {lbl for lbl, _ in nodes_by_label_and_depth}
    for label_value in labels_present:
        d1_nodes = nodes_by_label_and_depth.get((label_value, "d1"), [])
        d0_nodes = nodes_by_label_and_depth.get((label_value, "d0"), [])
        for src in d1_nodes:
            for dst in d0_nodes:
                if graph.has_edge(src, dst):
                    graph.edges[src, dst]["is_constraint"] = True
                else:
                    graph.add_edge(src, dst, kind="constraint", is_constraint=True)

    # If a d1 calc still has no outgoing edge (no d0 barrier sinks it),
    # connect it to the mandatory output concepts so the condition has a
    # consumer group to land on. Without this, a query whose SELECT is
    # purely scalar (no aggregate output) but whose WHERE references an
    # aggregate-by-grain — e.g. q04's `store_first_year > 0` over
    # customer-grain rows — leaves the aggregate node as a sink, and
    # `_inject_conditions` can't find any group whose reachable input
    # contains the aggregate.
    #
    # Constraints, to avoid demoting lineage or closing a cycle:
    #   - Skip d1 roots (condition inputs that already live in the scan);
    #     mandatory basics typically descend from them.
    #   - Skip d1 calcs that only appear as existence args (e.g. a rowset
    #     on the right of `x IN rowset`) — they don't need a row-stream
    #     consumer, only a side-channel subselect. A filter group used
    #     this way shares a group with the root scan that consumes it,
    #     so adding a back-edge here closes the cycle (q37 regression).
    #   - Don't add to a dst that is already a lineage ancestor of src.
    # Same d1-calc backfill, but scoped to the outer label (the only
    # one with an external `mandatory_list`). Inner sub-graphs have their
    # own select outputs as inner mandatory, but we don't backfill them
    # here — the inner walk's conditions are local to it.
    row_arg_addresses: set[str] = set()
    for clause in conditions:
        for atom in decompose_condition(clause.conditional):
            for c in atom.row_arguments:
                row_arg_addresses.add(c.address)
    mandatory_addresses = {c.address for c in mandatory_list}
    outer_d1_nodes = nodes_by_label_and_depth.get(("", "d1"), [])
    ancestors_of: dict[str, set[str]] = {}
    for src in outer_d1_nodes:
        if graph.nodes[src].get("derivation") == Derivation.ROOT.value:
            continue
        src_address = graph.nodes[src].get("address", src)
        if src_address not in row_arg_addresses:
            continue
        if any(True for _ in graph.successors(src)):
            continue
        if src not in ancestors_of:
            ancestors_of[src] = nx.ancestors(graph, src)
        src_ancestors = ancestors_of[src]
        for dst in mandatory_addresses:
            if dst == src_address or dst not in graph.nodes:
                continue
            if dst in src_ancestors or graph.has_edge(src, dst):
                continue
            graph.add_edge(src, dst, kind="constraint", is_constraint=True)
    return graph
