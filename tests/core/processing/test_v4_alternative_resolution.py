"""Unit coverage for the pseudonym-hub (`alt_group`) resolution machinery in
`concept_graph`. The end-to-end models in `test_v4_pseudonym_multi_origin`
exercise the happy path; these hit the graph-surgery branches that a terminal
sink-only hub never reaches — a hub with a downstream consumer (successor
redirect), losing-arm pruning that must stop at shared sinks, and the lineage
walk's skip conditions — by driving the helpers on hand-built graphs.
"""

from types import SimpleNamespace

from trilogy.core import graph as nx
from trilogy.core.enums import Derivation, Granularity, Purpose
from trilogy.core.processing.v4_helper.concept_graph import (
    _backbone_datasource_nodes,
    _lineage_ancestors,
    _prune_orphan_branch,
    resolve_alternatives,
)
from trilogy.core.processing.v4_helper.constants import DepthLabel, EdgeKind
from trilogy.core.processing.v4_helper.edges import EdgeMap, add_edge
from trilogy.core.processing.v4_helper.models import ConceptAttrs


def _attrs(address: str, derivation: Derivation = Derivation.ROOT) -> ConceptAttrs:
    return ConceptAttrs(
        address=address,
        label="",
        derivation=derivation,
        purpose=Purpose.KEY,
        granularity=Granularity.MULTI_ROW,
        depth_label=DepthLabel.ROOT,
    )


def _env() -> SimpleNamespace:
    # Resolution only reads `alias_origin_lookup`; empty is fine — no arm is
    # `concept_satisfiable`, so selection falls through to the cost/address tie-break.
    return SimpleNamespace(alias_origin_lookup={})


def test_resolve_redirects_hub_consumer_and_prunes_loser():
    """Hub with a downstream consumer: the winner inherits the consumer edge,
    the hub is dropped, and the losing arm (plus its now-orphaned datasource)
    is pruned — while a node the losing arm shares with a sink survives."""
    graph: nx.DiGraph = nx.DiGraph()
    edges: EdgeMap = {}
    attrs = {
        "ds_a": _attrs("a"),
        "ds_b": _attrs("b"),
        "arm_a": _attrs("uA.x"),
        "arm_b": _attrs("uB.x"),
        "hub": _attrs("local.x"),
        "consumer": _attrs("local.consumer"),
        "shared": _attrs("local.shared"),
    }
    add_edge(graph, edges, "ds_a", "arm_a", EdgeKind.LINEAGE)
    add_edge(graph, edges, "ds_b", "arm_b", EdgeKind.LINEAGE)
    add_edge(graph, edges, "shared", "arm_b", EdgeKind.LINEAGE)
    add_edge(graph, edges, "arm_a", "hub", EdgeKind.LINEAGE, alt_group="local.x")
    add_edge(graph, edges, "arm_b", "hub", EdgeKind.LINEAGE, alt_group="local.x")
    add_edge(graph, edges, "hub", "consumer", EdgeKind.LINEAGE)

    ds = frozenset({"a", "b"})
    sinks = {"consumer", "shared"}
    resolve_alternatives(graph, edges, attrs, _env(), ds, sinks)

    assert "hub" not in graph
    assert "arm_a" in graph  # lowest-address arm wins on the tie-break
    assert graph.has_edge("arm_a", "consumer")  # successor redirected onto winner
    assert all(a.alt_group is None for a in edges.values())
    # Losing arm and its orphaned datasource pruned; the shared sink protected.
    assert "arm_b" not in graph
    assert "ds_b" not in graph
    assert "shared" in graph


def test_resolve_handles_hub_removed_before_its_turn():
    """A nested hub: `h2` is itself an alternative arm of the earlier-processed
    `h1`. Losing `h1` prunes `h2` away, so by the time the loop reaches `h2` it
    is already gone — the loop must skip it, not crash."""
    graph: nx.DiGraph = nx.DiGraph()
    edges: EdgeMap = {}
    attrs = {
        "armW": _attrs("uW.a"),
        "armP": _attrs("uP.k"),
        "armQ": _attrs("uQ.k"),
        "h1": _attrs("local.a"),
        "h2": _attrs("local.k"),
    }
    # h2 is an arm of h1; armW is h1's other (winning) arm.
    add_edge(graph, edges, "armW", "h1", EdgeKind.LINEAGE, alt_group="local.a")
    add_edge(graph, edges, "h2", "h1", EdgeKind.LINEAGE, alt_group="local.a")
    # h2's own arms.
    add_edge(graph, edges, "armP", "h2", EdgeKind.LINEAGE, alt_group="local.k")
    add_edge(graph, edges, "armQ", "h2", EdgeKind.LINEAGE, alt_group="local.k")

    resolve_alternatives(graph, edges, attrs, _env(), frozenset(), {"h1"})

    assert "h2" not in graph  # pruned as h1's losing arm
    assert "h1" not in graph  # contracted away onto armW
    assert "armW" in graph
    assert all(a.alt_group is None for a in edges.values())


def test_resolve_noop_without_hubs():
    graph: nx.DiGraph = nx.DiGraph()
    edges: EdgeMap = {}
    add_edge(graph, edges, "ds_a", "leaf", EdgeKind.LINEAGE)
    attrs = {"ds_a": _attrs("a"), "leaf": _attrs("uA.x")}
    resolve_alternatives(graph, edges, attrs, _env(), frozenset({"a"}), {"leaf"})
    assert graph.has_edge("ds_a", "leaf")


def test_lineage_ancestors_skips_nonlineage_and_dedupes_diamond():
    """A non-LINEAGE predecessor is excluded, and a diamond (node reachable two
    ways) is visited once."""
    graph: nx.DiGraph = nx.DiGraph()
    edges: EdgeMap = {}
    add_edge(graph, edges, "root", "left", EdgeKind.LINEAGE)
    add_edge(graph, edges, "root", "right", EdgeKind.LINEAGE)
    add_edge(graph, edges, "left", "sink", EdgeKind.LINEAGE)
    add_edge(graph, edges, "right", "sink", EdgeKind.LINEAGE)
    add_edge(graph, edges, "existence_src", "sink", EdgeKind.EXISTENCE)
    ancestors = _lineage_ancestors(graph, edges, "sink")
    assert ancestors == {"left", "right", "root"}
    assert "existence_src" not in ancestors


def test_lineage_ancestors_excludes_alt_when_not_following():
    graph: nx.DiGraph = nx.DiGraph()
    edges: EdgeMap = {}
    add_edge(graph, edges, "backbone", "hub", EdgeKind.LINEAGE)
    add_edge(graph, edges, "alt", "hub", EdgeKind.LINEAGE, alt_group="g")
    assert _lineage_ancestors(graph, edges, "hub", follow_alt=False) == {"backbone"}
    assert _lineage_ancestors(graph, edges, "hub", follow_alt=True) == {
        "backbone",
        "alt",
    }


def test_backbone_skips_absent_sink_and_collects_datasource_roots():
    graph: nx.DiGraph = nx.DiGraph()
    edges: EdgeMap = {}
    add_edge(graph, edges, "ds", "mid", EdgeKind.LINEAGE)
    add_edge(graph, edges, "mid", "sink", EdgeKind.LINEAGE)
    attrs = {"ds": _attrs("a"), "mid": _attrs("uA.x"), "sink": _attrs("local.x")}
    roots = _backbone_datasource_nodes(
        graph, edges, attrs, frozenset({"a"}), {"sink", "ghost"}
    )
    assert roots == {"ds"}


def test_prune_orphan_branch_noop_for_absent_start():
    graph: nx.DiGraph = nx.DiGraph()
    edges: EdgeMap = {}
    add_edge(graph, edges, "a", "b", EdgeKind.LINEAGE)
    _prune_orphan_branch(graph, edges, "missing", set())
    assert graph.has_edge("a", "b")
