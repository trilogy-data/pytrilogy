"""Rowset islanding on the undirected connectivity graph.

A rowset is a materialized result: from outside it you can reach only its
declared outputs, through an explicit scoped join/merge; you cannot navigate
INTO its derivation to recover the base concepts it was computed from. The raw
reference graph does not honor that: a rowset output links to the internal
concepts behind its select, and through a shared base those internals reach the
OTHER rowset's internals, a phantom cross-rowset bridge that competes with the
real join key. Two rules enforce the boundary:

1. SEVER navigation across the rowset boundary, so internals stop acting as
   join paths.
2. LINK a rowset's co-produced outputs to each other via a per-rowset hub node
   (a rowset measure reaches its own grain key only through nodes the
   surrounding search prunes, so without the hub the outputs would isolate
   from each other). Each rowset gets its OWN hub, so distinct rowsets relate
   only through explicit scoped-join/pseudonym edges, never through a hub.

``island_rowsets_for_connectivity`` mutates the undirected connectivity copy
used by ``disconnected_components``. It severs EVERY boundary-crossing edge,
then re-welds the legitimate external links: downstream consumers of a
declared output (minus aggregate grain-only ``by`` consumers, which would
bridge unrelated models) and cross-rowset scoped-join pseudonym edges.
``link_rowset_outputs_for_connectivity`` applies rule 2 alone.
"""

from typing import TYPE_CHECKING

from trilogy.core.enums import Derivation
from trilogy.core.models.build import BuildRowsetItem

if TYPE_CHECKING:
    from trilogy.core.graph_models import ReferenceGraph

ROWSET_ISLAND_HUB_PREFIX = "rowset_island~"


def _add_hub(graph, hub: str, members: list[str]) -> None:
    for member in members:
        graph.add_edge(hub, member)


def link_rowset_outputs_for_connectivity(g: "ReferenceGraph", cg) -> None:
    """Rule 2 alone, with no severing: weld each rowset's co-produced outputs
    through its per-rowset hub on the undirected connectivity copy ``cg``.

    Raw edges are not enough: a rowset whose outputs wrap unrelated base models,
    related only by a scoped join declared inside the rowset body, has no
    cross-model edge at the outer level, so its own outputs would split into
    two components even though one sub-query produces them together."""
    members_by_rowset: dict[str, list[str]] = {}
    for node, concept in g.concepts.items():
        if concept.derivation != Derivation.ROWSET:
            continue
        if isinstance(concept.lineage, BuildRowsetItem):
            members_by_rowset.setdefault(concept.lineage.rowset.name, []).append(node)
    for name, members in members_by_rowset.items():
        present = [m for m in members if m in cg]
        if len(present) < 2:
            continue
        hub = f"{ROWSET_ISLAND_HUB_PREFIX}{name}"
        cg.add_node(hub)
        _add_hub(cg, hub, present)


def island_rowsets_for_connectivity(
    g: "ReferenceGraph", cg, grain_only: dict[str, set[str]] | None = None
) -> None:
    """Apply the islanding invariant to the undirected connectivity copy ``cg``.

    Severs every edge crossing a rowset boundary, then re-welds (a) each
    rowset's own outputs through its hub, (b) external downstream consumers of
    a declared output, and (c) outputs related across rowsets by a scoped-join
    pseudonym.

    Without this, a property keyed on a base concept looks falsely reachable
    from a rowset whose key was renamed off that base concept: the global graph
    connects the base concept to the rowset through the internal derivation,
    masking a genuine scoped-join disconnection."""
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
        _add_hub(cg, hub, [m for m in members if m in cg])
        # Re-weld external downstream consumers (`g`-successors) of each output;
        # only upstream navigation into the rowset's base concepts stays severed.
        # A consumer that merely groups `by` the output (grain-only parent) is
        # skipped: that edge would bridge unrelated models through an aggregate's
        # grouping key, the same bridge `_aggregate_grain_only_parents` drops.
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
