from collections.abc import Sequence
from dataclasses import dataclass, field

from trilogy.core.enums import Derivation, Granularity, Purpose
from trilogy.core.graph_models import (
    ReferenceGraph,
    ScopeDatasources,
    concept_to_node,
    datasource_to_node,
    union_to_node,
)
from trilogy.core.models.build import (
    BuildConcept,
    BuildDatasource,
    BuildFilterItem,
    BuildRowsetItem,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.aggregate_rollup import (
    _is_additive_aggregate,
    get_additive_rollup_concepts,
)
from trilogy.core.processing.basic_graph import (
    build_basic_concept_graph,
    get_derivable_concepts,
)
from trilogy.core.processing.node_generators.select_helpers.datasource_injection import (
    union_sources,
)


@dataclass(slots=True)
class _GraphSink:
    """Accumulates the concept-recursion's node/edge inserts so the graph core
    sees two batched calls instead of one crossing per insert. Records nodes in
    first-touch order (an edge endpoint counts as a touch, matching the core's
    on-demand endpoint creation) so the flushed node order is identical to the
    per-call order."""

    nodes: list[str] = field(default_factory=list)
    node_set: set[str] = field(default_factory=set)
    edges: list[tuple[str, str]] = field(default_factory=list)
    edge_set: set[tuple[str, str]] = field(default_factory=set)

    def add_node(self, node: str) -> None:
        if node not in self.node_set:
            self.node_set.add(node)
            self.nodes.append(node)

    def add_edge(self, left: str, right: str) -> None:
        self.add_node(left)
        self.add_node(right)
        key = (left, right)
        if key not in self.edge_set:
            self.edge_set.add(key)
            self.edges.append(key)

    def flush(self, g: ReferenceGraph) -> None:
        g.add_nodes_from(self.nodes)
        g.add_edges_from(self.edges)


def add_concept(
    concept: BuildConcept,
    g: ReferenceGraph,
    concept_mapping: dict[str, BuildConcept],
    default_concept_graph: dict[str, BuildConcept],
    seen: set[str],
    node_stash: dict[str, str],
    sink: _GraphSink,
):
    # if we have sources, recursively add them
    node_name = concept_to_node(concept, node_stash)
    if node_name in seen:
        return
    seen.add(node_name)
    g.concepts[node_name] = concept
    sink.add_node(node_name)
    root_name = node_name.split("@", 1)[0]
    # A FILTER concept's `? <cond>` args are not join inputs — the filtered value
    # is sourced from its content alone, and the condition may legitimately
    # reference an outer/correlated concept in a different model. Adding condition
    # edges would falsely bridge otherwise-disconnected components through the
    # filter node (masking a genuine missing-join from disconnect detection), so
    # restrict edges to the content side.
    if isinstance(concept.lineage, BuildFilterItem):
        sources: Sequence[BuildConcept] = concept.lineage.content_concept_arguments
        # A filter over grainless content (`sum(1 ? cond)`) has no content-side
        # row identity: its mask varies per row of the condition's row args, so
        # those args ARE its source rows. Without them the filter node floats
        # disconnected from the model whose rows it counts.
        if not any(
            s.derivation != Derivation.CONSTANT
            and s.granularity != Granularity.SINGLE_ROW
            for s in sources
        ):
            sources = list(concept.lineage.where.row_arguments)
    else:
        sources = concept.concept_arguments
    if sources:
        for source in sources:
            if not isinstance(source, BuildConcept):
                raise TypeError(
                    f"Invalid non-build concept {source} passed into graph generation from {concept}"
                )
            generic = get_default_grain_concept(source, default_concept_graph)
            generic_node = concept_to_node(generic, stash=node_stash)
            add_concept(
                generic,
                g,
                concept_mapping,
                default_concept_graph,
                seen,
                node_stash,
                sink,
            )

            sink.add_edge(generic_node, node_name)
    for ps_address in concept.pseudonyms:
        if ps_address not in concept_mapping:
            raise SyntaxError(f"Concept {concept} has invalid pseudonym {ps_address}")
        pseudonym = concept_mapping[ps_address]
        pseudonym = get_default_grain_concept(pseudonym, default_concept_graph)
        pseudonym_node = concept_to_node(pseudonym, stash=node_stash)
        if (pseudonym_node, node_name) in sink.edge_set and (
            node_name,
            pseudonym_node,
        ) in sink.edge_set:
            continue
        if pseudonym_node.split("@", 1)[0] == root_name:
            continue
        sink.add_edge(pseudonym_node, node_name)
        sink.add_edge(node_name, pseudonym_node)
        g.pseudonyms.add((pseudonym_node, node_name))
        g.pseudonyms.add((node_name, pseudonym_node))
        add_concept(
            pseudonym, g, concept_mapping, default_concept_graph, seen, node_stash, sink
        )


def get_default_grain_concept(
    concept: BuildConcept, default_concept_graph: dict[str, BuildConcept]
) -> BuildConcept:
    """Get the default grain concept from the graph."""
    if concept.address in default_concept_graph:
        return default_concept_graph[concept.address]
    default = concept.with_default_grain()
    default_concept_graph[concept.address] = default
    return default


def additive_rollup_edges(
    concepts: list[BuildConcept],
    datasources: Sequence[BuildDatasource],
    node_stash: dict[str, str],
) -> list[tuple[str, str]]:
    """Edges from a summary table to each aggregate it can SUM-roll up to.

    An aggregate's canonical name bakes in its by-grain, so `sum(x)<[a,b]>` and
    the coarser `sum(x)<[a]>` are different canonicals: a rollup is a derivation,
    not an identity. A *named* metric still reaches its summary table because
    query and column share an ``address``; an inline alias (`sum(x) as total`)
    shares an address with nothing, so without these edges the table is never
    even a candidate source and the query silently scans the raw fact.

    The edge is candidacy only. `create_datasource_node` re-runs
    `get_additive_rollup_concepts` with the query's real conditions and drops the
    aggregate from the scan's outputs when the rollup does not hold, so an edge
    added here can never by itself produce a wrong plan.
    """
    binding = [
        ds
        for ds in datasources
        if any(_is_additive_aggregate(c) for c in ds.output_concepts)
    ]
    if not binding:
        return []
    concepts_by_address = {c.address: c for c in concepts}
    targets = [c for c in concepts if c.is_aggregate and _is_additive_aggregate(c)]
    edges: list[tuple[str, str]] = []
    for datasource in binding:
        ds_node = datasource_to_node(datasource)
        bound = {c.address for c in datasource.output_concepts}
        for concept in targets:
            # Already bound by address — the existing column edge covers it.
            if concept.address in bound:
                continue
            if not get_additive_rollup_concepts(
                datasource=datasource,
                requested_concepts=[concept],
                concepts_by_address=concepts_by_address,
                datasources=datasources,
                target_grain=concept.grain,
            ):
                continue
            cnode = concept_to_node(concept, node_stash)
            edges.append((ds_node, cnode))
            edges.append((cnode, ds_node))
    return edges


def generate_adhoc_graph(
    concepts: list[BuildConcept],
    scope: ScopeDatasources,
    default_concept_graph: dict[str, BuildConcept],
) -> ReferenceGraph:
    g = ReferenceGraph()
    g.scope = scope
    datasources = scope.datasources
    concept_mapping = {x.address: x for x in concepts}
    node_stash: dict[str, str] = {}
    seen: set[str] = set()
    for concept in concepts:
        if not isinstance(concept, BuildConcept):
            raise TypeError(f"Invalid non-build concept {concept}")

    # add all parsed concepts; node/edge inserts accumulate in the sink and
    # land in two batched core calls
    sink = _GraphSink()
    for concept in concepts:
        add_concept(
            concept, g, concept_mapping, default_concept_graph, seen, node_stash, sink
        )
    sink.flush(g)

    # A rowset has no datasource node, so its outputs are otherwise uncoordinated
    # in the graph — unlike a datasource's columns, which the datasource node
    # co-locates. Add a co-locating anchor node per rowset connecting its outputs,
    # so a derivation over one output (e.g. `rowset.x * 2`) can reach the rowset's
    # other outputs (e.g. a scoped-join key), making the resolution path identical
    # to the datasource case. Exclude aggregate (metric) outputs: a rowset that
    # aggregates from several unrelated models (an invalid combine) must still
    # surface as disconnected, so its measures are not bridged through the anchor
    # (mirrors the aggregate grain-only edge drop in disconnected_components).
    rowset_members: dict[str, list[str]] = {}
    for concept in concepts:
        if (
            concept.derivation == Derivation.ROWSET
            and isinstance(concept.lineage, BuildRowsetItem)
            and concept.purpose != Purpose.METRIC
        ):
            rowset_members.setdefault(concept.lineage.rowset.name, []).append(
                concept_to_node(concept, node_stash)
            )
    for rowset_name, members in rowset_members.items():
        if len(members) < 2:
            continue
        anchor = f"rowset~{rowset_name}"
        g.add_node(anchor)
        anchor_edges: list[tuple[str, str]] = []
        for member in members:
            anchor_edges.append((anchor, member))
            anchor_edges.append((member, anchor))
        g.add_edges_from(anchor_edges)

    basic_graph = build_basic_concept_graph(concepts)

    for dataset in datasources:
        node = datasource_to_node(dataset)
        g.datasources[node] = dataset
        g.add_datasource_node(node, dataset)
        eligible = dataset.concepts
        already_present = {x.canonical_address for x in eligible}
        complete_contains = {
            c.concept.canonical_address for c in dataset.columns if c.is_complete
        }
        for derived in get_derivable_concepts(
            basic_graph, complete_contains, already_present
        ):
            eligible.append(derived)

        # Collect this datasource's edges and inject them in one Rust call;
        # the per-edge Python<->Rust crossing dominated otherwise. The core
        # adds endpoint nodes as needed, so explicit add_node is unnecessary.
        edges: list[tuple[str, str]] = []
        for concept in eligible:
            cnode = concept_to_node(concept, node_stash)
            g.concepts[cnode] = concept

            edges.append((node, cnode))
            edges.append((cnode, node))
            # if there is a key on a table at a different grain
            # add an FK edge to the canonical source, if it exists
            # for example, order ID on order product table
            default = get_default_grain_concept(concept, default_concept_graph)

            if concept != default:

                dcnode = concept_to_node(default, node_stash)
                g.concepts[dcnode] = default
                edges.append((cnode, dcnode))
                edges.append((dcnode, cnode))
        g.add_edges_from(edges)
    g.add_edges_from(additive_rollup_edges(concepts, datasources, node_stash))
    return g


def generate_graph(
    environment: BuildEnvironment,
    scope: ScopeDatasources | None = None,
) -> ReferenceGraph:
    """The environment's reference graph over ``scope``: a statement's
    bindings as decided by ``generate_scope_graph``, or, left out, the
    environment's as authored. The covering unions over the scope's partition
    families are source nodes of the graph like any scan (`union_sources`)."""
    default_concept_graph: dict[str, BuildConcept] = {}
    g = generate_adhoc_graph(
        list(environment.concepts.values())
        + list(environment.alias_origin_lookup.values()),
        scope or ScopeDatasources(environment.datasources.values()),
        default_concept_graph=default_concept_graph,
    )
    edges: list[tuple[str, str]] = []
    for union, emits in union_sources(g.scope, environment):
        node = union_to_node(union)
        g.datasources[node] = union
        g.add_datasource_node(node, union)
        for concept in emits:
            cnode = concept_to_node(concept)
            g.concepts.setdefault(cnode, concept)
            edges.append((node, cnode))
            edges.append((cnode, node))
    g.add_edges_from(edges)
    return g
