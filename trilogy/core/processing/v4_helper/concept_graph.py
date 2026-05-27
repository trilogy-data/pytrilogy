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


# Phase suffix appended to a label when a concept is reached via the WHERE
# recursion. The same concept can appear once in the SELECT (blank) sub-graph
# and once in the WHERE (condition) sub-graph; the suffix is what keeps them
# distinct so we don't try to promote/demote a single shared node.
PHASE_CONDITION_SUFFIX = "@condition"


def _scope_and_phase(label: str) -> tuple[str, str]:
    """Split a label into its (scope, phase) parts. scope is "" for the outer
    query and the rowset name for rowset internals; phase is "blank" or
    "condition"."""
    if label.endswith(PHASE_CONDITION_SUFFIX):
        return label[: -len(PHASE_CONDITION_SUFFIX)], "condition"
    return label, "blank"


def _condition_label(scope_label: str) -> str:
    """Build the condition-phase label from a blank-phase label."""
    return f"{scope_label}{PHASE_CONDITION_SUFFIX}"


def _effective_label(concept: BuildConcept, label: str) -> str:
    """ROOT concepts represent input columns; their scan is shared between
    the SELECT and WHERE phases, so they always live in the blank-phase
    (scope-only) label. Everything else uses the recursion's label as-is."""
    if concept.derivation == Derivation.ROOT:
        return _scope_and_phase(label)[0]
    return label


def classify_depth(concept: BuildConcept, label: str) -> str:
    """Tag a concept by its placement role.

    `d1` is no longer "address appears in a WHERE clause" — it's "this node
    was reached via the condition-phase recursion." The phase is encoded in
    the label, so the SELECT and WHERE walks build disjoint sub-graphs and
    a concept that participates in both gets two distinct nodes."""
    _, phase = _scope_and_phase(label)
    if phase == "condition":
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
    label: str = "",
) -> None:
    """Walk lineage from a concept toward its roots, under a fixed label.

    The label encodes (scope, phase) — scope is "" for the outer query / the
    rowset name for rowset internals; phase is "blank" by default, or
    "condition" via the ``@condition`` suffix. The same concept reached from
    the SELECT walk and from the WHERE walk thus lands in two separate nodes
    (the WHERE one is d1, the SELECT one keeps its derivation-driven label).
    No second-pass promotion is needed — the depth falls out of the label."""
    eff_label = _effective_label(concept, label)
    nid = node_id(eff_label, concept.address)
    if nid in graph:
        return
    graph.add_node(
        nid,
        address=concept.address,
        label=eff_label,
        derivation=concept.derivation.value,
        purpose=concept.purpose.value,
        granularity=concept.granularity.value,
        depth_label=classify_depth(concept, eff_label),
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
        _add_concept(upstream, environment, graph, label=label)
        upstream_label = _effective_label(upstream, label)
        graph.add_edge(
            node_id(upstream_label, upstream.address), nid, kind="lineage"
        )


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
    # Outer SELECT: blank-phase label "".
    for concept in mandatory_list:
        _add_concept(concept, environment, graph)
    # Outer WHERE: condition-phase label "@condition". The same concept that
    # also appears in the SELECT gets a separate node here, so we never
    # have to retro-promote depth labels.
    for clause in conditions:
        for concept in clause.concept_arguments:
            resolved = environment.concepts.get(concept.address, concept)
            _add_concept(resolved, environment, graph, label=_condition_label(""))

    # Walk each ROWSET leaf in the outer graph. Multiple outer concepts
    # may all point at the same rowset (q05: channel, id, sales_metric,
    # returns_metric, profit_metric all live in q5_results) — dedup by
    # rowset name so we only walk the inner select once.
    seen_rowsets: set[str] = set()
    pending_rowsets: list[BuildConcept] = []
    for nid, data in list(graph.nodes(data=True)):
        if data.get("derivation") != Derivation.ROWSET.value:
            continue
        # Skip rowsets discovered inside an already-labeled sub-graph;
        # nested rowsets are handled by the outer recursion below. Only
        # the outer blank-phase nodes have label="" so this test still works.
        if data.get("label", ""):
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
        for inner_concept in inner_outputs:
            _add_concept(inner_concept, environment, graph, label=rowset_name)
        for clause in inner_conditions:
            for c in clause.concept_arguments:
                resolved = environment.concepts.get(c.address, c)
                _add_concept(
                    resolved,
                    environment,
                    graph,
                    label=_condition_label(rowset_name),
                )

    # Classify how each atom uses its concept arguments. A row-argument
    # gets joined into the consumer's row stream; an existence-argument
    # is consumed via a side-channel subselect (IN/EXISTS) and must never
    # be modeled as JOIN partner. We collect both sets so the constraint
    # and existence edge passes below can flow strictly along the right
    # channel for each address.
    row_arg_addresses: set[str] = set()
    existence_arg_pairs: list[tuple[str, str]] = []  # (existence_addr, row_addr)
    for clause in conditions:
        for atom in decompose_condition(clause.conditional):
            atom_row_addrs = [c.address for c in atom.row_arguments]
            for c in atom.row_arguments:
                row_arg_addresses.add(c.address)
            for arg_group in getattr(atom, "existence_arguments", ()) or ():
                for ec in arg_group:
                    for row_addr in atom_row_addrs:
                        existence_arg_pairs.append((ec.address, row_addr))

    # Group nodes by scope-and-phase. Condition-phase nodes are d1 by
    # construction; the only d0 candidates live in the matching blank-phase
    # scope. Constraint edges flow strictly from condition-phase ROW-ARG
    # nodes to blank-phase row-shape barriers in the same scope — these
    # are the only d1s that will JOIN into the d0's row stream. A condition
    # concept that only ever appears as an existence-arg gets an explicit
    # `existence` edge instead (below), so the dataflow distinction is
    # carried in the graph rather than recovered later via heuristics.
    nodes_by_scope_phase: dict[tuple[str, str], list[str]] = {}
    for n, d in graph.nodes(data=True):
        scope, phase = _scope_and_phase(d.get("label", ""))
        nodes_by_scope_phase.setdefault((scope, phase), []).append(n)
    scopes_present = {scope for scope, _ in nodes_by_scope_phase}
    for scope in scopes_present:
        condition_nodes = nodes_by_scope_phase.get((scope, "condition"), [])
        d0_blank_nodes = [
            n
            for n in nodes_by_scope_phase.get((scope, "blank"), [])
            if graph.nodes[n].get(LABEL_DEPTH) == "d0"
        ]
        for src in condition_nodes:
            src_address = graph.nodes[src].get("address", src)
            if src_address not in row_arg_addresses:
                continue
            for dst in d0_blank_nodes:
                if graph.has_edge(src, dst):
                    graph.edges[src, dst]["is_constraint"] = True
                else:
                    graph.add_edge(src, dst, kind="constraint", is_constraint=True)

    # Existence edges: for each `... IN <subselect>` atom, the existence
    # source must be built and topologically ordered before the host
    # consumer, but its rows never JOIN into the host's row stream — the
    # renderer reads them via a subselect against the source CTE. Mark
    # this with a distinct edge kind so downstream passes (group-edge
    # propagation, JOIN-key projection, strategy parent selection) can
    # treat existence siblings as side-channel, not JOIN partners.
    for existence_addr, row_addr in existence_arg_pairs:
        existence_nid = node_id(_condition_label(""), existence_addr)
        # The row arg may be in either the blank or @condition phase
        # depending on whether it's also a SELECT output. Connect to
        # whichever exists; the atom's host bucket consumes from there.
        for candidate_label in ("", _condition_label("")):
            row_nid = node_id(candidate_label, row_addr)
            if existence_nid in graph and row_nid in graph and existence_nid != row_nid:
                if not graph.has_edge(existence_nid, row_nid):
                    graph.add_edge(existence_nid, row_nid, kind="existence")

    # Backfill: if a condition-phase node has no successor (no d0 barrier
    # consumed it), wire a constraint from it to the matching blank-phase
    # mandatory outputs so the condition has somewhere to land. q04's
    # `store_first_year > 0` over customer-grain rows is the motivating
    # case — purely scalar SELECT, no d0 to absorb the WHERE.
    #
    # Limits, mirroring the original logic:
    #   - skip ROOT-derivation condition nodes (those represent in-scan
    #     attributes that the mandatory walk already produces);
    #   - require the node's address to actually appear as a row argument
    #     (existence args don't need row-stream consumers);
    #   - skip nodes that already have any outgoing edge.
    mandatory_blank_ids = {
        node_id("", c.address) for c in mandatory_list
    }
    outer_condition_nodes = nodes_by_scope_phase.get(("", "condition"), [])
    for src in outer_condition_nodes:
        if graph.nodes[src].get("derivation") == Derivation.ROOT.value:
            continue
        src_address = graph.nodes[src].get("address", src)
        if src_address not in row_arg_addresses:
            continue
        if any(True for _ in graph.successors(src)):
            continue
        for dst in mandatory_blank_ids:
            if dst not in graph.nodes or graph.has_edge(src, dst):
                continue
            graph.add_edge(src, dst, kind="constraint", is_constraint=True)
    return graph
