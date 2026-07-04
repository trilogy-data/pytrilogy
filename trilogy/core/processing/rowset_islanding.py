"""Rowset islanding — one invariant, two graph shapes.

A rowset is a materialized result: from outside it you can reach only its
declared outputs, through an explicit scoped join/merge — you cannot navigate
INTO its derivation to recover the base concepts it was computed from. The raw
reference graph does not honor that: a rowset output links to the internal
concepts behind its select, and through a shared base those internals reach the
OTHER rowset's internals — a phantom cross-rowset bridge that competes with the
real join key. Both reshapes below enforce the same two rules:

1. SEVER navigation across the rowset boundary, so internals stop acting as
   join paths.
2. LINK a rowset's co-produced outputs to each other via a per-rowset hub node
   (a rowset measure reaches its own grain key only through nodes the
   surrounding search prunes, so without the hub the outputs would isolate
   from each other). Each rowset gets its OWN hub, so distinct rowsets relate
   only through explicit scoped-join/pseudonym edges, never through a hub.

The two entry points apply those rules to different graphs and therefore sever
at different granularities:

* ``island_rowsets_for_weak_merge`` mutates the weak-merge search graph, whose
  concept nodes key on CANONICAL address with one instance per grain. It severs
  exactly the output<->content edges (the alias link a rowset output keeps to
  the concept it renamed), leaving downstream consumers attached — the search
  needs them reachable directly.
* ``island_rowsets_for_connectivity`` mutates the undirected connectivity copy
  used by ``disconnected_components``. It severs EVERY boundary-crossing edge,
  then re-welds the legitimate external links: downstream consumers of a
  declared output (minus aggregate grain-only ``by`` consumers, which would
  bridge unrelated models) and cross-rowset scoped-join pseudonym edges.
"""

from typing import TYPE_CHECKING

from trilogy.core.enums import Derivation
from trilogy.core.models.build import BuildConcept, BuildRowsetItem

if TYPE_CHECKING:
    from trilogy.core.graph_models import ReferenceGraph

ROWSET_HUB_PREFIX = "rowset_hub~"
ROWSET_ISLAND_HUB_PREFIX = "rowset_island~"


def extract_address(node: str) -> str:
    return node.split("~")[1].split("@")[0]


def _add_hub(graph, hub: str, members: list[str], bidirectional: bool) -> None:
    for member in members:
        graph.add_edge(hub, member)
        if bidirectional:
            graph.add_edge(member, hub)


def island_rowsets_for_weak_merge(
    g: "ReferenceGraph", requested_concepts: list[BuildConcept]
) -> None:
    """Apply the islanding invariant to the weak-merge search graph.

    Severs output<->content alias edges across ALL grain instances (matching by
    CANONICAL address — graph nodes key on ``canonical_address``, not the
    friendly ``address``; the default-grain-only lineage prune misses the other
    instances). Downstream consumers of an output (e.g. the derived join key
    `a.grp + 1`) are NOT severed — something downstream of a rowset
    legitimately links back to it. Hubs are named without a ``c~``/``ds~``
    prefix so concept/datasource extraction skips them — pure connectivity
    glue."""
    nodes_by_canon: dict[str, list[str]] = {}
    for node in g.nodes:
        if str(node).startswith("c~"):
            nodes_by_canon.setdefault(extract_address(node), []).append(node)

    canon_by_rowset: dict[str, set[str]] = {}
    to_remove: list[tuple[str, str]] = []
    for concept in g.concepts.values():
        if not isinstance(concept.lineage, BuildRowsetItem):
            continue
        canon_by_rowset.setdefault(concept.lineage.rowset.name, set()).add(
            concept.canonical_address
        )
        content_canon = concept.lineage.content.canonical_address
        for out_node in nodes_by_canon.get(concept.canonical_address, []):
            for content_node in nodes_by_canon.get(content_canon, []):
                to_remove.append((content_node, out_node))
                to_remove.append((out_node, content_node))
    g.remove_edges_from([e for e in to_remove if e in g.edges])

    for name, canon_addrs in canon_by_rowset.items():
        members = [
            node for addr in canon_addrs for node in nodes_by_canon.get(addr, [])
        ]
        if len(members) < 2:
            continue
        _add_hub(g, f"{ROWSET_HUB_PREFIX}{name}", members, bidirectional=True)


def island_rowsets_for_connectivity(
    g: "ReferenceGraph", cg, grain_only: dict[str, set[str]] | None = None
) -> None:
    """Apply the islanding invariant to the undirected connectivity copy ``cg``.

    Severs every edge crossing a rowset boundary, then re-welds (a) each
    rowset's own outputs through its hub, (b) external downstream consumers of
    a declared output, and (c) outputs related across rowsets by a scoped-join
    pseudonym.

    Without this, a property keyed on a base concept (e.g. ``store_id.name``)
    looks falsely reachable from a rowset whose key was *renamed* off that base
    concept (``select store_id as sk_a``): the global graph connects
    ``store_id`` to the rowset through that internal derivation, so a genuine
    scoped-join disconnection (the join group is ``{sk_a, sk_b}``, not
    ``store_id``) is masked and surfaces as the generic unresolvable error
    instead of a named subgraph split."""
    members_by_rowset: dict[str, list[str]] = {}
    nodes_by_address: dict[str, list[str]] = {}
    rowset_nodes: set[str] = set()
    for node, concept in g.concepts.items():
        if concept.derivation != Derivation.ROWSET:
            continue
        rowset_nodes.add(node)
        nodes_by_address.setdefault(concept.address, []).append(node)
        if isinstance(concept.lineage, BuildRowsetItem):
            members_by_rowset.setdefault(concept.lineage.rowset.name, []).append(node)

    if not rowset_nodes:
        return

    island = rowset_nodes | {
        n for n in cg.nodes if isinstance(n, str) and n.startswith("rowset~")
    }
    cg.remove_edges_from(
        [(u, v) for u, v in cg.edges if (u in island) != (v in island)]
    )

    for name, members in members_by_rowset.items():
        hub = f"{ROWSET_ISLAND_HUB_PREFIX}{name}"
        cg.add_node(hub)
        _add_hub(cg, hub, [m for m in members if m in cg], bidirectional=False)
        # A concept DERIVED from a rowset's declared output (a filtered aggregate
        # `sum(cur.sales ? ...)`, `cur.x * 2`, etc.) legitimately consumes that
        # output, but islanding severed the edge as a boundary crossing — wrongly
        # orphaning the consumer (q02 `_virt_filter` over a rowset measure). A
        # downstream consumer is a `g`-successor of an output; reconnect each
        # external one to the hub. Only UPSTREAM navigation (predecessors — the
        # base concepts the rowset was computed from) stays severed, which is the
        # whole point of islanding. Skip a consumer that merely groups `by` the
        # output (a grain-only parent): re-welding that edge would bridge two
        # unrelated models through an aggregate's grouping key — exactly the bridge
        # `_aggregate_grain_only_parents` drops upstream.
        for member in members:
            member_concept = g.concepts.get(member)
            member_addr = member_concept.address if member_concept else None
            for consumer in g.successors(member):
                if consumer in island or consumer not in cg:
                    continue
                consumer_concept = g.concepts.get(consumer)
                if (
                    grain_only
                    and consumer_concept is not None
                    and member_addr in grain_only.get(consumer_concept.address, set())
                ):
                    continue
                cg.add_edge(hub, consumer)

    for node in rowset_nodes:
        concept = g.concepts[node]
        for pseudonym in concept.pseudonyms:
            if pseudonym == concept.address:
                continue
            for other in nodes_by_address.get(pseudonym, []):
                if node in cg and other in cg:
                    cg.add_edge(node, other)
