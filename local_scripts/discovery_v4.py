"""Prototype harness for the v4 discovery engine.

Run the v4 search against either an inline mini-query (the default demo) or
any TPC-DS preql in tests/modeling/tpc_ds_duckdb so we can stress the planner
across the query shapes that suite already exercises.

    # inline demo:
    python local_scripts/discovery_v4.py

    # a tpc_ds query (any of these forms work):
    python local_scripts/discovery_v4.py --query 1
    python local_scripts/discovery_v4.py --query 01
    python local_scripts/discovery_v4.py --query query01.preql

Outputs go to local_scripts/<stem>.png and <stem>_groups.png. Pass --no-sql
to skip the SQL compile step.
"""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import networkx as nx  # noqa: E402

from trilogy import Environment
from trilogy.core.enums import ComparisonOperator
from trilogy.core.env_processor import generate_graph
from trilogy.core.models.build import (
    BuildComparison,
    BuildMultiSelectLineage,
    BuildSelectLineage,
    BuildWhereClause,
    Factory,
    get_canonical_pseudonyms,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.concept_strategies_v4 import (
    FINAL_NODE_ID,
    BuildInfo,
    V4History,
    search_concepts,
)
from trilogy.core.processing.condition_utility import strip_tautological_not_null
from trilogy.core.statements.author import MultiSelectStatement, SelectStatement

REPO_ROOT = Path(__file__).resolve().parent.parent
TPCDS_DIR = REPO_ROOT / "tests" / "modeling" / "tpc_ds_duckdb"
OUT_DIR = Path(__file__).parent


_RE_GRAIN = re.compile(r"@Grain<[^>]*>")

DEPTH_COLORS = {
    "d0": "#ffe082",  # row-shape barriers (aggregate/window/unnest/...)
    "d1": "#f48fb1",  # condition inputs
    "d*": "#cfd8dc",  # movable (anything else)
    "root": "#90caf9",  # unified root source group (may span d1 + d*)
    "final": "#81c784",
}


# Typed views over the raw node-attribute dicts written by
# trilogy.core.processing.concept_strategies_v4. Mirror the shape produced
# there; defaults cover the FINAL group node, which only carries a subset.


@dataclass
class ConceptNodeData:
    depth_label: str = "d*"
    derivation: str = ""
    purpose: str = ""
    granularity: str = ""
    grain_components: frozenset[str] = frozenset()


@dataclass
class GroupNodeData:
    depth_label: str = "d*"
    derivation: str = ""
    grain_components: frozenset[str] = frozenset()
    members: tuple[str, ...] = ()
    primary_members: tuple[str, ...] = ()
    secondary_members: tuple[str, ...] = ()
    member_depths: dict[str, str] = field(default_factory=dict)
    conditions: list[str] = field(default_factory=list)


def _concept_data(concept_attrs: dict, node: str) -> ConceptNodeData:
    raw = concept_attrs.get(node)
    if raw is None:
        return ConceptNodeData()
    return ConceptNodeData(
        depth_label=raw.depth_label,
        derivation=raw.derivation,
        purpose=raw.purpose,
        granularity=raw.granularity,
        grain_components=raw.grain_components,
    )


def _group_data(attrs: dict, node: str) -> GroupNodeData:
    """Adapt a `GroupAttrs` (from the v4 helper) onto the visualizer-local
    `GroupNodeData` view. The visualizer's struct is kept narrower so the
    renderer stays decoupled from the helper's full attribute set."""
    a = attrs.get(node)
    if a is None:
        return GroupNodeData()
    return GroupNodeData(
        depth_label=a.depth_label,
        derivation=a.derivation,
        grain_components=a.grain_components,
        members=a.members,
        primary_members=a.primary_members,
        secondary_members=a.secondary_members,
        member_depths=dict(a.member_depths),
        conditions=list(a.conditions),
    )


def _center_truncate(s: str, max_len: int = 36) -> str:
    """Shorten `s` to <= max_len chars by replacing its middle with `…`. Keeps
    the head and tail so prefixes/suffixes (table qualifiers, hash signatures)
    remain readable."""
    if len(s) <= max_len:
        return s
    keep = max_len - 1
    head = (keep + 1) // 2
    tail = keep - head
    return f"{s[:head]}…{s[-tail:]}" if tail else f"{s[:head]}…"


def _layered_layout(
    graph: nx.DiGraph,
    layering_edges: list[tuple[str, str]],
    *,
    pin_last: str | None = None,
    layer_gap: float = 2.4,
    x_gap: float = 3.4,
    sweeps: int = 40,
    cluster_of: dict[str, str] | None = None,
) -> dict[str, tuple[float, float]]:
    """Sugiyama-style layered layout. Layer = topological generation over
    `layering_edges` (sources at top, sinks at bottom). Per-layer x-ordering
    runs a barycenter sweep so a node sits near the mean position of its
    neighbors in the adjacent layer, keeping vertical chains vertical.
    `pin_last` is forced to the bottom layer (e.g. FINAL sink).

    `cluster_of` maps each node to a cluster id; clusters with non-empty
    ids are stacked ABOVE the empty-id cluster (visually higher = built
    earlier). This makes rowset-internal pipelines (label='rowset_name')
    appear above the outer pipeline they feed into, even when there's no
    explicit cross-cluster edge connecting them."""
    sub = graph.edge_subgraph(layering_edges).copy()
    for n in graph.nodes:
        sub.add_node(n)
    try:
        generations = [list(g) for g in nx.topological_generations(sub)]
    except nx.NetworkXUnfeasible:
        generations = [list(graph.nodes)]
    # Cluster-aware layering: re-shuffle generations so each cluster occupies
    # its own contiguous layer range, with non-empty clusters (rowset
    # internals) stacked above the empty cluster (outer pipeline). Skip if
    # only one cluster is present.
    if cluster_of is not None and len({cluster_of.get(n, "") for n in graph.nodes}) > 1:
        per_cluster: dict[str, list[list[str]]] = {}
        for layer in generations:
            for node in layer:
                if pin_last is not None and node == pin_last:
                    continue
                cluster = cluster_of.get(node, "")
                bucket = per_cluster.setdefault(cluster, [])
                bucket.append([node])  # placeholder; refined next
        # Rebuild per-cluster generations by re-topo-sorting each cluster's
        # subgraph independently (so within-cluster ordering uses real edges).
        per_cluster_layers: dict[str, list[list[str]]] = {}
        for cluster, _ in per_cluster.items():
            cnodes = [
                n
                for n in graph.nodes
                if cluster_of.get(n, "") == cluster and n != pin_last
            ]
            csub = sub.subgraph(cnodes).copy()
            for n in cnodes:
                csub.add_node(n)
            try:
                cgens = [list(g) for g in nx.topological_generations(csub)]
            except nx.NetworkXUnfeasible:
                cgens = [cnodes]
            per_cluster_layers[cluster] = cgens
        # Stack order: non-empty clusters first (above), then empty cluster.
        ordered_clusters = sorted(per_cluster_layers.keys(), key=lambda k: (k == "", k))
        generations = []
        for cluster in ordered_clusters:
            generations.extend(per_cluster_layers[cluster])
        if pin_last is not None and pin_last in graph.nodes:
            generations.append([pin_last])
    elif pin_last is not None and pin_last in graph.nodes:
        for layer in generations:
            if pin_last in layer:
                layer.remove(pin_last)
        generations = [layer for layer in generations if layer]
        generations.append([pin_last])

    # Initial ordering: alphabetical. Stable starting point so sweeps are
    # deterministic — barycenter on its own ties many positions on the first
    # round when all parents/children have the same fresh-pass x.
    layers = [sorted(layer) for layer in generations]
    node_layer = {n: i for i, layer in enumerate(layers) for n in layer}

    def _slot_map() -> dict[str, float]:
        return {n: i for layer in layers for i, n in enumerate(layer)}

    # Successors / predecessors restricted to adjacent layers in the layering
    # subgraph — cross-layer skip edges would otherwise pull a node toward a
    # non-neighbor and undo the vertical alignment.
    def _neighbors(node: str, direction: str) -> list[str]:
        my_layer = node_layer[node]
        if direction == "up":
            target = my_layer - 1
            cand = sub.predecessors(node)
        else:
            target = my_layer + 1
            cand = sub.successors(node)
        return [n for n in cand if node_layer.get(n) == target]

    # Barycenter is known to oscillate (often period-2) when the down and up
    # sweeps disagree. Track the best seen ordering by total edge length and
    # return that — final-iteration may be worse than something we passed.
    # Cost is computed on layer-centered x-positions (slot - len/2) so cross-
    # layer comparisons are meaningful; raw slot indices would penalize
    # vertically-aligned chains whenever adjacent layers have different counts.
    def _edge_cost(layers: list[list[str]]) -> float:
        x_of: dict[str, float] = {}
        for layer in layers:
            n = len(layer)
            for slot, node in enumerate(layer):
                x_of[node] = slot + 0.5 - n / 2
        cost = 0.0
        for u, v in sub.edges():
            if u in x_of and v in x_of:
                cost += abs(x_of[u] - x_of[v])
        return cost

    best_layers = [list(lyr) for lyr in layers]
    best_cost = _edge_cost(best_layers)
    last_signature: tuple[tuple[str, ...], ...] = tuple(tuple(lyr) for lyr in layers)
    stable = 0
    for _ in range(sweeps):
        for li in range(1, len(layers)):
            # Refresh slot map between layers so each layer's reorder feeds
            # into the next layer's barycenter (otherwise the down sweep is
            # one parallel pass that ignores its own progress).
            slots = _slot_map()

            def key(n: str, slots=slots) -> tuple[float, str]:
                ups = _neighbors(n, "up")
                if ups:
                    return (sum(slots[u] for u in ups) / len(ups), n)
                return (slots[n], n)

            layers[li] = sorted(layers[li], key=key)
        for li in range(len(layers) - 2, -1, -1):
            slots = _slot_map()

            def key(n: str, slots=slots) -> tuple[float, str]:
                downs = _neighbors(n, "down")
                if downs:
                    return (sum(slots[d] for d in downs) / len(downs), n)
                return (slots[n], n)

            layers[li] = sorted(layers[li], key=key)
        cost = _edge_cost(layers)
        if cost < best_cost:
            best_cost = cost
            best_layers = [list(lyr) for lyr in layers]
        signature = tuple(tuple(lyr) for lyr in layers)
        if signature == last_signature:
            stable += 1
            if stable >= 2:
                break
        else:
            stable = 0
        last_signature = signature
    layers = best_layers

    pos: dict[str, tuple[float, float]] = {}
    for layer_idx, layer in enumerate(layers):
        n = len(layer)
        for slot, node in enumerate(layer):
            x = (slot + 0.5 - n / 2) * x_gap
            pos[node] = (x, -layer_idx * layer_gap)
    # any unplaced nodes (shouldn't happen, but be defensive)
    missing = [n for n in graph.nodes if n not in pos]
    if missing:
        y = -len(layers) * layer_gap
        for i, node in enumerate(sorted(missing)):
            pos[node] = ((i + 0.5 - len(missing) / 2) * x_gap, y)
    _, ymin = min(pos.values(), key=lambda xy: xy[1])
    pos = {n: (x, y - ymin) for n, (x, y) in pos.items()}
    return pos


def render_digraph(graph: nx.DiGraph, concept_attrs: dict, output_path: Path) -> None:
    """Concept-dependency digraph: top-down by lineage depth, rectangular
    labels so long concept addresses stay readable."""
    lineage_edges = [
        (u, v) for u, v, d in graph.edges(data=True) if d.get("kind") == "lineage"
    ]
    constraint_edges = [
        (u, v) for u, v, d in graph.edges(data=True) if d.get("kind") == "constraint"
    ]

    # Constraint edges (d1 → d0 "must apply above") are real build-order
    # dependencies, not just visual hints — the d0 derivation can't be
    # planned until its d1 inputs are placed. Layering on lineage alone
    # leaves d0 targets floating at the same row as their d1 sources, which
    # contradicts the constraint arrows the chart actually draws.
    pos = _layered_layout(
        graph,
        lineage_edges + constraint_edges,
        layer_gap=2.2,
        x_gap=5.4,
    )
    concept_data = {n: _concept_data(concept_attrs, n) for n in graph.nodes}
    labels = {
        n: f"{_center_truncate(n, 28)}\n({d.depth_label})"
        for n, d in concept_data.items()
    }
    node_colors = {
        n: DEPTH_COLORS.get(concept_data[n].depth_label, "#eeeeee") for n in graph.nodes
    }

    # Vertical room scales with layer count and label rows; horizontal room
    # with the widest layer. Single-row labels here, so heights stay modest.
    ys = sorted({y for _, y in pos.values()}, reverse=True)
    layer_count = len(ys)
    max_layer_width = max(sum(1 for n in pos if pos[n][1] == y) for y in ys)
    fig_w = max(12.0, max_layer_width * 3.4)
    fig_h = max(8.0, layer_count * 2.0)
    fig, ax = plt.subplots(figsize=(fig_w, fig_h))

    for n in graph.nodes:
        x, y = pos[n]
        ax.text(
            x,
            y,
            labels[n],
            ha="center",
            va="center",
            fontsize=8.5,
            zorder=3,
            bbox=dict(
                boxstyle="round,pad=0.4",
                facecolor=node_colors[n],
                edgecolor="#444444",
                linewidth=0.8,
            ),
        )

    nx.draw_networkx_edges(
        graph,
        pos,
        ax=ax,
        edgelist=lineage_edges,
        edge_color="#555555",
        alpha=0.55,
        arrows=True,
        arrowsize=12,
        node_size=2800,
        width=0.8,
    )
    nx.draw_networkx_edges(
        graph,
        pos,
        ax=ax,
        edgelist=constraint_edges,
        edge_color="#c2185b",
        style="dashed",
        arrows=True,
        arrowsize=14,
        connectionstyle="arc3,rad=0.2",
        node_size=2800,
        width=1.0,
    )
    legend_handles = [
        plt.Line2D(
            [0],
            [0],
            marker="s",
            color="w",
            markerfacecolor=DEPTH_COLORS["d0"],
            markersize=12,
            label="d0 (row-shape barrier)",
        ),
        plt.Line2D(
            [0],
            [0],
            marker="s",
            color="w",
            markerfacecolor=DEPTH_COLORS["d1"],
            markersize=12,
            label="d1 (condition input)",
        ),
        plt.Line2D(
            [0],
            [0],
            marker="s",
            color="w",
            markerfacecolor=DEPTH_COLORS["d*"],
            markersize=12,
            label="d* (movable)",
        ),
        plt.Line2D(
            [0],
            [0],
            color="#c2185b",
            linestyle="dashed",
            label="d1 → d0 (must apply above)",
        ),
    ]
    ax.legend(handles=legend_handles, loc="lower right", fontsize=9, framealpha=0.95)
    ax.set_axis_off()
    ax.margins(x=0.05, y=0.05)
    fig.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def _short(addr: str) -> str:
    return addr.split(".")[-1]


def _wrap_members(
    members: tuple[str, ...], per_line: int = 2, max_len: int = 20
) -> str:
    """Pack short member names into lines so the label box doesn't grow wider
    than its neighbors. `per_line` items per row, each center-truncated."""
    if not members:
        return ""
    items = [_center_truncate(_short(m), max_len) for m in members]
    rows = [", ".join(items[i : i + per_line]) for i in range(0, len(items), per_line)]
    return "\n".join(rows)


def _gid_title(node: str) -> str:
    """Strip the `grp:<derivation>:<depth>:` prefix from a group id; the
    label already shows derivation + depth elsewhere, so the title only needs
    the disambiguating tail (grain + sig)."""
    if node == FINAL_NODE_ID:
        return "FINAL"
    if node.startswith("grp:"):
        parts = node.split(":", 3)
        tail = parts[3] if len(parts) > 3 else ""
        return _center_truncate(tail, 26) if tail else node
    return _center_truncate(node, 26)


def _condition_summary(cond: str) -> str:
    """The BuildSubselectComparison repr is verbose. Pull out left + operator
    + right shape so the chart's WHERE line stays scannable."""
    # crude but cheap: strip the BuildSubselectComparison(...) wrapper
    inner = cond
    for prefix in (
        "BuildSubselectComparison(",
        "BuildComparison(",
        "BuildConditional(",
    ):
        if inner.startswith(prefix) and inner.endswith(")"):
            inner = inner[len(prefix) : -1]
            break
    # collapse `left=<x>@Grain<...>` to just `<x>`
    inner = _RE_GRAIN.sub("", inner)
    return _center_truncate(inner, 50)


def _group_label(node: str, data: GroupNodeData) -> str:
    if node == FINAL_NODE_ID:
        label = "FINAL"
        if data.members:
            label += "\n" + _wrap_members(data.members, per_line=3)
        if data.conditions:
            label += "\nWHERE " + _condition_summary(" AND ".join(data.conditions))
        return label

    title = f"{data.derivation} · {data.depth_label}"
    subtitle = _gid_title(node)
    parts = [title]
    if subtitle and subtitle != title:
        parts.append(subtitle)
    if data.primary_members:
        parts.append(_wrap_members(data.primary_members, per_line=2))
    if data.secondary_members:
        parts.append("+ " + _wrap_members(data.secondary_members, per_line=2))
    if data.conditions:
        parts.append("WHERE " + _condition_summary(" AND ".join(data.conditions)))
    return "\n".join(parts)


def render_group_digraph(
    graph: nx.DiGraph,
    attrs: dict,
    output_path: Path,
) -> None:
    """Group graph: top-down by lineage generation, FINAL pinned to the
    bottom. Conditions show inline so the place where each WHERE atom lands
    is obvious at a glance."""

    def _edge_color(data: dict) -> str:
        phase = data.get("phase")
        if phase == "pre_condition":
            return "#c62828"
        if phase == "post_condition":
            return "#2e7d32"
        return "#555555"

    lineage_edges = [
        (u, v) for u, v, d in graph.edges(data=True) if d.get("kind") == "lineage"
    ]
    merge_edges = [
        (u, v) for u, v, d in graph.edges(data=True) if d.get("kind") == "merge"
    ]
    existence_edges = [
        (u, v) for u, v, d in graph.edges(data=True) if d.get("kind") == "existence"
    ]
    constraint_edges = [
        (u, v) for u, v, d in graph.edges(data=True) if d.get("kind") == "constraint"
    ]
    # All three edge kinds express build-order: a d1 condition filter must
    # be built before the root that subselects from it (existence), a sibling
    # constraint must be ready before its dependent, lineage is the obvious
    # parent. Include them all in the layering so the chart reads strictly
    # top-down — visualizing "what must exist before what" matches how the
    # SQL renderer walks the CTE graph.
    layering = lineage_edges + constraint_edges + existence_edges

    pos = _layered_layout(
        graph,
        layering,
        pin_last=FINAL_NODE_ID if FINAL_NODE_ID in graph.nodes else None,
        layer_gap=4.5,
        x_gap=7.0,
    )

    group_data = {n: _group_data(attrs, n) for n in graph.nodes}
    labels = {n: _group_label(n, d) for n, d in group_data.items()}
    node_colors = {
        n: DEPTH_COLORS.get(group_data[n].depth_label, "#eeeeee") for n in graph.nodes
    }

    ys = sorted({y for _, y in pos.values()}, reverse=True)
    layer_count = len(ys)
    max_layer_width = max(sum(1 for n in pos if pos[n][1] == y) for y in ys)
    # Multi-line labels need more vertical air than the concept chart.
    fig_w = max(14.0, max_layer_width * 4.0)
    fig_h = max(9.0, layer_count * 2.6)
    fig, ax = plt.subplots(figsize=(fig_w, fig_h))

    for n in graph.nodes:
        x, y = pos[n]
        is_cond = bool(group_data[n].conditions) and n != FINAL_NODE_ID
        ax.text(
            x,
            y,
            labels[n],
            ha="center",
            va="center",
            fontsize=8.5,
            zorder=3,
            bbox=dict(
                boxstyle="round,pad=0.45",
                facecolor=node_colors[n],
                edgecolor="#c2185b" if is_cond else "#444444",
                linewidth=2.2 if is_cond else 0.9,
            ),
        )

    if lineage_edges:
        nx.draw_networkx_edges(
            graph,
            pos,
            ax=ax,
            edgelist=lineage_edges,
            edge_color=[_edge_color(graph.edges[u, v]) for u, v in lineage_edges],
            arrows=True,
            arrowsize=16,
            node_size=3200,
            width=1.2,
        )
    if merge_edges:
        nx.draw_networkx_edges(
            graph,
            pos,
            ax=ax,
            edgelist=merge_edges,
            edge_color=[_edge_color(graph.edges[u, v]) for u, v in merge_edges],
            style="dotted",
            arrows=True,
            arrowsize=14,
            connectionstyle="arc3,rad=0.12",
            node_size=3200,
            width=0.9,
        )
    if existence_edges:
        nx.draw_networkx_edges(
            graph,
            pos,
            ax=ax,
            edgelist=existence_edges,
            edge_color="#6a1b9a",
            style="dashed",
            arrows=True,
            arrowsize=14,
            connectionstyle="arc3,rad=0.25",
            node_size=3200,
            width=1.0,
        )
    if constraint_edges:
        nx.draw_networkx_edges(
            graph,
            pos,
            ax=ax,
            edgelist=constraint_edges,
            edge_color="#ef6c00",
            style="dashdot",
            arrows=True,
            arrowsize=14,
            connectionstyle="arc3,rad=0.18",
            node_size=3200,
            width=1.0,
        )

    legend_handles = [
        plt.Line2D(
            [0],
            [0],
            marker="s",
            color="w",
            markerfacecolor=DEPTH_COLORS["root"],
            markersize=12,
            label="root (source scan)",
        ),
        plt.Line2D(
            [0],
            [0],
            marker="s",
            color="w",
            markerfacecolor=DEPTH_COLORS["d0"],
            markersize=12,
            label="d0 (row-shape barrier)",
        ),
        plt.Line2D(
            [0],
            [0],
            marker="s",
            color="w",
            markerfacecolor=DEPTH_COLORS["d1"],
            markersize=12,
            label="d1 (condition feeder)",
        ),
        plt.Line2D(
            [0],
            [0],
            marker="s",
            color="w",
            markerfacecolor=DEPTH_COLORS["d*"],
            markersize=12,
            label="d* (movable)",
        ),
        plt.Line2D(
            [0],
            [0],
            marker="s",
            color="w",
            markerfacecolor=DEPTH_COLORS["final"],
            markersize=12,
            label="FINAL (sink)",
        ),
        plt.Line2D([0], [0], color="#555555", label="lineage edge"),
        plt.Line2D([0], [0], color="#c62828", label="pre-condition phase"),
        plt.Line2D([0], [0], color="#2e7d32", label="post-condition phase"),
        plt.Line2D(
            [0],
            [0],
            color="#6a1b9a",
            linestyle="dashed",
            label="existence (side-channel)",
        ),
        plt.Line2D([0], [0], color="#ef6c00", linestyle="dashdot", label="constraint"),
        plt.Line2D(
            [0], [0], color="#555555", linestyle="dotted", label="merge → FINAL"
        ),
        plt.Line2D(
            [0],
            [0],
            marker="s",
            color="w",
            markerfacecolor="#ffffff",
            markeredgecolor="#c2185b",
            markeredgewidth=2.5,
            markersize=12,
            label="condition injected here",
        ),
    ]
    ax.legend(handles=legend_handles, loc="lower right", fontsize=8.5, framealpha=0.95)
    ax.set_axis_off()
    ax.margins(x=0.05, y=0.05)
    fig.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------------------
# Plan generation
# ---------------------------------------------------------------------------


def _materialize_for_query(
    environment: Environment,
    statement: SelectStatement | MultiSelectStatement,
    history: V4History,
) -> tuple[
    BuildSelectLineage | BuildMultiSelectLineage,
    BuildEnvironment,
    Optional[BuildWhereClause],
]:
    """Mirror trilogy.core.query_processor.get_query_node up to (but not
    including) the source_query_concepts call, returning the inputs v4 needs."""
    lineage = statement.as_lineage(environment)
    caches = history.build_caches
    if caches.pseudonym_map is None:
        caches.pseudonym_map = get_canonical_pseudonyms(environment)
    factory = Factory(
        environment=environment,
        build_cache=caches.build_cache,
        canonical_build_cache=caches.canonical_build_cache,
        grain_build_cache=caches.grain_build_cache,
        pseudonym_map=caches.pseudonym_map,
    )
    build_statement: BuildSelectLineage | BuildMultiSelectLineage = factory.build(
        lineage
    )
    build_env = environment.materialize_for_select(
        build_statement.local_concepts,
        build_cache=caches.build_cache,
        pseudonym_map=factory.pseudonym_map,
        grain_build_cache=factory.grain_build_cache,
        canonical_build_cache=caches.canonical_build_cache,
        datasource_build_cache=caches.datasource_build_cache,
    )

    protected: set[str] = set()
    for component in build_statement.output_components:
        protected.add(component.address)
        protected.add(component.canonical_address)
    order_by = build_statement.order_by
    if order_by is not None:
        for item in order_by.items:
            for arg in item.concept_arguments:
                protected.add(arg.address)
                protected.add(arg.canonical_address)
    conditions = strip_tautological_not_null(
        build_statement.where_clause, build_env, protected
    )
    return build_statement, build_env, conditions


def _find_select(queries: list) -> SelectStatement | MultiSelectStatement:
    selects = [
        q for q in queries if isinstance(q, (SelectStatement, MultiSelectStatement))
    ]
    if not selects:
        raise ValueError(
            "No SelectStatement / MultiSelectStatement found in parsed queries — "
            "persist statements aren't wired into this harness yet."
        )
    return selects[-1]


def run_inline_demo() -> tuple[BuildInfo, BuildEnvironment, str, None]:
    base = Environment()
    env, _ = base.parse("""
key order_id int;

properties order_id (
            value float,
            );

key customer_id int;

properties customer_id (
            name string
               )
               ;

datasource orders(
               order_id,
               customer_id,
               value)

grain (order_id)
query '''select 1 as order_id, 1 as customer_id, 2.0 as value''';

datasource customers(
               customer_id,
               name)
grain (customer_id)
query '''select 1 as customer_id, "Alice" as name''';

select
    sum(value) by name as total_revenue,
    avg(value) by name as avg_revenue,
    upper(name)-> upper_name,
;
        """)
    benv = env.materialize_for_select()
    conditions = BuildWhereClause(
        conditional=BuildComparison(
            left=benv.concepts["local.customer_id"],
            right=1,
            operator=ComparisonOperator.EQ,
        )
    )
    info = search_concepts(
        mandatory_list=[
            benv.concepts["local.total_revenue"],
            benv.concepts["local.avg_revenue"],
            benv.concepts["local.upper_name"],
        ],
        history=V4History(base_environment=env),
        environment=benv,
        depth=0,
        g=generate_graph(benv),
        conditions=[conditions],
    )
    return info, benv, "discovery_v4", None


def run_tpcds_query(
    name: str,
) -> tuple[
    BuildInfo,
    BuildEnvironment,
    str,
    Optional[BuildSelectLineage | BuildMultiSelectLineage],
]:
    """Resolve `name` to a preql file under TPCDS_DIR, build the v4 plan.
    Returns the BuildSelectLineage too so compile_sql can apply HAVING,
    ORDER BY, hidden_concepts, and LIMIT from the original statement."""
    if name.isdigit():
        filename = f"query{int(name):02d}.preql"
    elif name.endswith(".preql"):
        filename = name
    else:
        filename = f"{name}.preql"
    path = TPCDS_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"No such tpc_ds preql: {path}")
    text = path.read_text()
    env = Environment(working_path=TPCDS_DIR)
    _, queries = env.parse(text)
    select = _find_select(queries)

    history = V4History(base_environment=env)
    build_stmt, build_env, conditions = _materialize_for_query(env, select, history)

    info = search_concepts(
        mandatory_list=list(build_stmt.output_components),
        history=history,
        environment=build_env,
        depth=0,
        g=generate_graph(build_env),
        conditions=[conditions] if conditions else [],
    )
    return info, build_env, path.stem, build_stmt


def compile_sql(
    info: BuildInfo,
    build_env: BuildEnvironment,
    build_stmt: Optional[BuildSelectLineage | BuildMultiSelectLineage] = None,
) -> str | None:
    """Compile the v4 strategy node to SQL. When `build_stmt` is supplied,
    apply HAVING / ORDER BY / hidden_concepts / LIMIT from the original
    statement so the SQL matches the user's authored query (mirrors what
    v3's process_query does at the end)."""
    if info.strategy_node is None:
        return None
    from trilogy.core.enums import BooleanOperator
    from trilogy.core.models.build import BuildConditional
    from trilogy.core.optimization import optimize_ctes
    from trilogy.core.processing.nodes import SelectNode
    from trilogy.core.query_processor import datasource_to_cte, flatten_ctes
    from trilogy.core.statements.execute import ProcessedQuery
    from trilogy.dialect.duckdb import DuckDBDialect

    node = info.strategy_node.copy()

    if build_stmt is not None and getattr(build_stmt, "having_clause", None):
        having = build_stmt.having_clause.conditional
        combined = (
            BuildConditional(
                left=node.conditions,
                right=having,
                operator=BooleanOperator.AND,
            )
            if node.conditions
            else having
        )
        node = SelectNode(
            output_concepts=list(build_stmt.output_components),
            input_concepts=list(node.usable_outputs),
            parents=[node],
            environment=node.environment,
            partial_concepts=list(node.partial_concepts),
            conditions=combined,
        )

    if build_stmt is not None:
        # Merge user-declared hidden components with whatever the strategy
        # node already carries from the per-group backward pass — grain
        # keys promoted to hidden by `_compute_concept_sets` would
        # otherwise be wiped here and leak into the user-visible SELECT.
        existing_hidden = set(node.hidden_concepts or set())
        node.hidden_concepts = existing_hidden | set(build_stmt.hidden_components)
        node.ordering = build_stmt.order_by
    else:
        node.hidden_concepts = set(node.hidden_concepts or set())
        node.ordering = None
    node.rebuild_cache()

    qds = node.resolve()
    root_cte = datasource_to_cte(qds, build_env.cte_name_map)
    raw_ctes = list(reversed(flatten_ctes(root_cte)))
    seen: dict = {}
    for cte in raw_ctes:
        if cte.name not in seen:
            seen[cte.name] = cte
        else:
            seen[cte.name] = seen[cte.name] + cte
    for cte in raw_ctes:
        cte.parent_ctes = [seen[x.name] for x in cte.parent_ctes]
    deduped = list(seen.values())

    if build_stmt is not None:
        root_cte.limit = build_stmt.limit
        root_cte.hidden_concepts = set(build_stmt.hidden_components)

    # Match v3's process_query tail: run the optimizer pass plan (direct
    # return, predicate pushdown, join hoist, inlining, etc.) before
    # rendering. Without this the v4 SQL stays verbose — each scan in its
    # own CTE, no merging — which is correct but a lot bigger.
    if build_stmt is not None:
        deduped = optimize_ctes(deduped, root_cte, build_stmt)

    outputs = [c for c in node.output_concepts if c.address not in node.hidden_concepts]
    pq = ProcessedQuery(output_columns=outputs, ctes=deduped, base=root_cte)
    return DuckDBDialect().compile_statement(pq)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--query",
        "-q",
        help="TPC-DS query identifier: '1', '01', 'query01', or a *.preql filename. "
        "Omit to run the inline demo.",
    )
    parser.add_argument(
        "--no-sql",
        action="store_true",
        help="Skip SQL compilation (graph rendering only).",
    )
    args = parser.parse_args()

    if args.query:
        info, build_env, stem, build_stmt = run_tpcds_query(args.query)
    else:
        info, build_env, stem, build_stmt = run_inline_demo()

    print("concept_graph:", info.concept_graph)
    print("group_graph:", info.group_graph)
    print("strategy_node:", info.strategy_node)
    if info.strategy_node is not None:
        outputs = [c.address for c in info.strategy_node.output_concepts]
        print(" strategy outputs:", outputs)

    concept_out = OUT_DIR / f"{stem}.png"
    group_out = OUT_DIR / f"{stem}_groups.png"
    render_digraph(info.concept_graph, info.concept_attrs, concept_out)
    render_group_digraph(info.group_graph, info.group_attrs, group_out)
    print(f"wrote {concept_out}")
    print(f"wrote {group_out}")

    if not args.no_sql:
        sql = compile_sql(info, build_env, build_stmt)
        if sql is not None:
            print("\n--- Generated SQL (from v4 strategy_node) ---")
            print(sql)


if __name__ == "__main__":
    main()
