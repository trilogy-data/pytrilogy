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
    History,
    search_concepts,
)
from trilogy.core.processing.condition_utility import strip_tautological_not_null
from trilogy.core.statements.author import SelectStatement

REPO_ROOT = Path(__file__).resolve().parent.parent
TPCDS_DIR = REPO_ROOT / "tests" / "modeling" / "tpc_ds_duckdb"
OUT_DIR = Path(__file__).parent


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


def _concept_data(graph: nx.DiGraph, node: str) -> ConceptNodeData:
    raw = graph.nodes[node]
    return ConceptNodeData(
        depth_label=raw.get("depth_label", "d*"),
        derivation=raw.get("derivation", ""),
        purpose=raw.get("purpose", ""),
        granularity=raw.get("granularity", ""),
        grain_components=raw.get("grain_components", frozenset()),
    )


def _group_data(graph: nx.DiGraph, node: str) -> GroupNodeData:
    raw = graph.nodes[node]
    return GroupNodeData(
        depth_label=raw.get("depth_label", "d*"),
        derivation=raw.get("derivation", ""),
        grain_components=raw.get("grain_components", frozenset()),
        members=tuple(raw.get("members", ())),
        primary_members=tuple(raw.get("primary_members", ())),
        secondary_members=tuple(raw.get("secondary_members", ())),
        member_depths=dict(raw.get("member_depths", {})),
        conditions=list(raw.get("conditions") or []),
    )


def render_digraph(graph: nx.DiGraph, output_path: Path) -> None:
    """Render a concept-dependency digraph to a PNG, sorted top-down by lineage depth."""
    lineage_edges = [
        (u, v) for u, v, d in graph.edges(data=True) if d.get("kind") == "lineage"
    ]
    constraint_edges = [
        (u, v) for u, v, d in graph.edges(data=True) if d.get("kind") == "constraint"
    ]
    lineage_only = graph.edge_subgraph(lineage_edges).copy()
    for n in graph.nodes:
        lineage_only.add_node(n)

    for layer, nodes in enumerate(nx.topological_generations(lineage_only)):
        for node in nodes:
            graph.nodes[node]["layer"] = layer

    pos = nx.multipartite_layout(graph, subset_key="layer", align="horizontal")
    pos = {node: (x, -y) for node, (x, y) in pos.items()}

    concept_data = {n: _concept_data(graph, n) for n in graph.nodes}
    labels = {n: f"{n}\n({d.depth_label})" for n, d in concept_data.items()}
    node_colors = [
        DEPTH_COLORS.get(concept_data[n].depth_label, "#eeeeee") for n in graph.nodes
    ]

    plt.figure(figsize=(max(8, len(graph) * 1.2), max(4, len(graph) * 0.6)))
    nx.draw_networkx_nodes(graph, pos, node_color=node_colors, node_size=2600)
    nx.draw_networkx_labels(graph, pos, labels=labels, font_size=8)
    nx.draw_networkx_edges(
        graph,
        pos,
        edgelist=lineage_edges,
        edge_color="#555555",
        arrows=True,
        arrowsize=14,
        node_size=2600,
    )
    nx.draw_networkx_edges(
        graph,
        pos,
        edgelist=constraint_edges,
        edge_color="#c2185b",
        style="dashed",
        arrows=True,
        arrowsize=14,
        connectionstyle="arc3,rad=0.15",
        node_size=2600,
    )
    legend_handles = [
        plt.Line2D(
            [0], [0], marker="o", color="w",
            markerfacecolor=DEPTH_COLORS["d0"], markersize=10,
            label="d0 (row-shape barrier)",
        ),
        plt.Line2D(
            [0], [0], marker="o", color="w",
            markerfacecolor=DEPTH_COLORS["d1"], markersize=10,
            label="d1 (condition input)",
        ),
        plt.Line2D(
            [0], [0], marker="o", color="w",
            markerfacecolor=DEPTH_COLORS["d*"], markersize=10,
            label="d* (movable)",
        ),
        plt.Line2D(
            [0], [0], color="#c2185b", linestyle="dashed",
            label="d1 → d0 (must apply above)",
        ),
    ]
    plt.legend(handles=legend_handles, loc="lower right")
    plt.margins(x=0.15, y=0.15)
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()


def _short(addr: str) -> str:
    return addr.split(".")[-1]


def _group_label(node: str, data: GroupNodeData) -> str:
    if node == FINAL_NODE_ID:
        label = "FINAL\n[" + ", ".join(_short(m) for m in data.members) + "]"
        if data.conditions:
            label += "\nwhere " + " AND ".join(data.conditions)
        return label

    def _member(m: str) -> str:
        d = data.member_depths.get(m, data.depth_label)
        return f"{_short(m)} ({d})" if d != data.depth_label else _short(m)

    label = (
        f"{data.depth_label} · {data.derivation}\n"
        f"[{', '.join(_member(m) for m in data.primary_members)}]"
    )
    if data.secondary_members:
        label += f"\n+[{', '.join(_member(m) for m in data.secondary_members)}]"
    if data.conditions:
        label += "\nwhere " + " AND ".join(data.conditions)
    return label


def render_group_digraph(graph: nx.DiGraph, output_path: Path) -> None:
    """Render the post-processed group graph (compatible-concept clusters + final sink)."""
    def _edge_color(data: dict) -> str:
        phase = data.get("phase")
        if phase == "pre_condition":
            return "#c62828"  # red — upstream of condition
        if phase == "post_condition":
            return "#2e7d32"  # green — downstream of condition
        return "#555555"  # neutral

    lineage_edges = [
        (u, v) for u, v, d in graph.edges(data=True) if d.get("kind") == "lineage"
    ]
    merge_edges = [
        (u, v) for u, v, d in graph.edges(data=True) if d.get("kind") == "merge"
    ]
    lineage_only = graph.edge_subgraph(lineage_edges).copy()
    for n in graph.nodes:
        lineage_only.add_node(n)

    # Lay groups out in lineage-topological generations when possible. If
    # the lineage graph has a cycle (a v4 planner bug we still want to be
    # able to visualize), fall back to placing every node on layer 0 — the
    # render won't be pretty but it'll be drawn, and the cycle will be
    # visible as the loop in the arrows.
    try:
        for layer, nodes in enumerate(nx.topological_generations(lineage_only)):
            for node in nodes:
                graph.nodes[node]["layer"] = layer
    except nx.NetworkXUnfeasible:
        for n in graph.nodes:
            graph.nodes[n]["layer"] = 0
    # force the final node to be the last layer
    max_layer = max((graph.nodes[n].get("layer", 0) for n in graph.nodes), default=0)
    if FINAL_NODE_ID in graph.nodes:
        graph.nodes[FINAL_NODE_ID]["layer"] = max_layer + 1

    pos = nx.multipartite_layout(graph, subset_key="layer", align="horizontal")
    pos = {node: (x, -y) for node, (x, y) in pos.items()}

    group_data = {n: _group_data(graph, n) for n in graph.nodes}
    labels = {n: _group_label(n, d) for n, d in group_data.items()}
    node_colors = [
        DEPTH_COLORS.get(group_data[n].depth_label, "#eeeeee") for n in graph.nodes
    ]
    node_edge_colors = [
        "#c2185b" if group_data[n].conditions and n != FINAL_NODE_ID else "#888888"
        for n in graph.nodes
    ]
    node_linewidths = [
        2.5 if group_data[n].conditions and n != FINAL_NODE_ID else 1.0
        for n in graph.nodes
    ]

    plt.figure(figsize=(max(10, len(graph) * 1.6), max(5, len(graph) * 0.8)))
    nx.draw_networkx_nodes(
        graph, pos,
        node_color=node_colors,
        edgecolors=node_edge_colors,
        linewidths=node_linewidths,
        node_size=3600,
    )
    nx.draw_networkx_labels(graph, pos, labels=labels, font_size=8)
    lineage_colors = [_edge_color(graph.edges[u, v]) for u, v in lineage_edges]
    nx.draw_networkx_edges(
        graph, pos,
        edgelist=lineage_edges,
        edge_color=lineage_colors,
        arrows=True, arrowsize=14, node_size=3600,
    )
    merge_colors = [_edge_color(graph.edges[u, v]) for u, v in merge_edges]
    nx.draw_networkx_edges(
        graph, pos,
        edgelist=merge_edges,
        edge_color=merge_colors,
        style="dotted",
        arrows=True, arrowsize=14,
        connectionstyle="arc3,rad=0.1",
        node_size=3600,
    )

    legend_handles = [
        plt.Line2D([0], [0], marker="o", color="w",
                   markerfacecolor=DEPTH_COLORS["d0"], markersize=10,
                   label="d0 group (barrier)"),
        plt.Line2D([0], [0], marker="o", color="w",
                   markerfacecolor=DEPTH_COLORS["d1"], markersize=10,
                   label="d1 group (condition)"),
        plt.Line2D([0], [0], marker="o", color="w",
                   markerfacecolor=DEPTH_COLORS["d*"], markersize=10,
                   label="d* group (movable)"),
        plt.Line2D([0], [0], marker="o", color="w",
                   markerfacecolor=DEPTH_COLORS["root"], markersize=10,
                   label="root group (sourceable)"),
        plt.Line2D([0], [0], marker="o", color="w",
                   markerfacecolor=DEPTH_COLORS["final"], markersize=10,
                   label="final (merge sink)"),
        plt.Line2D([0], [0], color="#c62828",
                   label="upstream of condition"),
        plt.Line2D([0], [0], color="#2e7d32",
                   label="downstream of condition"),
        plt.Line2D([0], [0], marker="o", color="w",
                   markerfacecolor="#ffffff", markeredgecolor="#c2185b",
                   markeredgewidth=2.5, markersize=10,
                   label="condition injected here"),
    ]
    plt.legend(handles=legend_handles, loc="lower right")
    plt.margins(x=0.15, y=0.15)
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()


# ---------------------------------------------------------------------------
# Plan generation
# ---------------------------------------------------------------------------


def _materialize_for_query(
    environment: Environment,
    statement: SelectStatement,
    history: History,
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
    build_statement: BuildSelectLineage | BuildMultiSelectLineage = factory.build(lineage)
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


def _find_select(queries: list) -> SelectStatement:
    selects = [q for q in queries if isinstance(q, SelectStatement)]
    if not selects:
        raise ValueError(
            "No SelectStatement found in parsed queries — multiselect / persist "
            "statements aren't wired into this harness yet."
        )
    return selects[-1]


def run_inline_demo() -> tuple[BuildInfo, BuildEnvironment, str, None]:
    base = Environment()
    env, _ = base.parse(
        """
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
        """
    )
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
        history=History(base_environment=env),
        environment=benv,
        depth=0,
        g=generate_graph(benv),
        conditions=[conditions],
    )
    return info, benv, "discovery_v4", None


def run_tpcds_query(
    name: str,
) -> tuple[BuildInfo, BuildEnvironment, str, Optional[BuildSelectLineage | BuildMultiSelectLineage]]:
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

    history = History(base_environment=env)
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

    outputs = [
        c for c in node.output_concepts if c.address not in node.hidden_concepts
    ]
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
    render_digraph(info.concept_graph, concept_out)
    render_group_digraph(info.group_graph, group_out)
    print(f"wrote {concept_out}")
    print(f"wrote {group_out}")

    if not args.no_sql:
        sql = compile_sql(info, build_env, build_stmt)
        if sql is not None:
            print("\n--- Generated SQL (from v4 strategy_node) ---")
            print(sql)


if __name__ == "__main__":
    main()
