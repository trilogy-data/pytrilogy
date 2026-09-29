"""A row-preserving input every reader computes inline is folded out of the
group graph before it is built (docs/handoff_open_optimization_items.md,
item 2)."""

from tests.core.processing.test_v4_dim_peel_not_built import _MODEL
from trilogy import Dialects, Environment
from trilogy.core import graph as nx
from trilogy.core.enums import Derivation
from trilogy.core.processing import plan_trace
from trilogy.core.processing.v4_helper.constants import (
    FINAL_NODE_ID,
    DepthLabel,
    EdgeKind,
)
from trilogy.core.processing.v4_helper.edges import EdgeMap, add_edge
from trilogy.core.processing.v4_helper.models import (
    FinalAssemblyContract,
    FinalContributorContract,
    GroupAttrs,
    GroupInputContract,
)
from trilogy.core.processing.v4_helper.strategy_builder import _fold_into_readers


def _executor():
    env, _ = Environment().parse(_MODEL)
    return Dialects.DUCK_DB.default_executor(environment=env)


def _trace(executor, query: str) -> plan_trace.PlanTrace:
    plan_trace.start(query)
    try:
        executor.generate_sql(query)
    finally:
        trace = plan_trace.stop()
    assert trace is not None
    return trace


def _built(trace: plan_trace.PlanTrace, derivation: str) -> list[str]:
    return [
        s.data.group
        for s in trace.steps
        if s.phase == "node" and s.data.derivation == derivation
    ]


def _final_graph_groups(trace: plan_trace.PlanTrace) -> set[str]:
    graphs = [s for s in trace.steps if s.phase == "group_graph"]
    return set(graphs[-1].data.graph.nodes)


def test_inlined_basic_is_not_built():
    executor = _executor()
    query = "select order_id, sum(item_margin) as margin order by order_id asc;"
    trace = _trace(executor, query)
    assert not _built(trace, "basic")
    assert not [g for g in _final_graph_groups(trace) if g.startswith("grp:basic")]
    rows = [tuple(r) for r in executor.execute_text(query)[-1].fetchall()]
    assert rows == [(10, 9.0), (11, 2.0)]


def test_basic_the_final_reads_is_built():
    executor = _executor()
    query = """select line_id, item_margin, sum(item_margin) by order_id as margin
order by line_id asc;"""
    trace = _trace(executor, query)
    assert _built(trace, "basic")
    rows = [tuple(r) for r in executor.execute_text(query)[-1].fetchall()]
    assert rows == [(1, 4.0, 9.0), (2, 5.0, 9.0), (3, 2.0, 2.0)]


def test_fold_puts_the_parents_in_the_folded_groups_place():
    graph = nx.DiGraph()
    edges: EdgeMap = {}
    for parent, child, kind in [
        ("root", "agg", EdgeKind.LINEAGE),
        ("dim", "basic", EdgeKind.LINEAGE),
        ("root", "basic", EdgeKind.LINEAGE),
        ("basic", "agg", EdgeKind.LINEAGE),
        ("other", "agg", EdgeKind.CONSTRAINT),
        ("basic", FINAL_NODE_ID, EdgeKind.MERGE),
        ("agg", FINAL_NODE_ID, EdgeKind.MERGE),
    ]:
        add_edge(graph, edges, parent, child, kind)
    attrs = {
        gid: GroupAttrs(depth_label=DepthLabel.D0, derivation=derivation)
        for gid, derivation in [
            ("root", Derivation.ROOT),
            ("dim", Derivation.ROOT),
            ("other", Derivation.ROOT),
            ("basic", Derivation.BASIC),
            ("agg", Derivation.AGGREGATE),
            (FINAL_NODE_ID, None),
        ]
    }
    attrs["basic"].primary_members = ("local.item_margin",)
    attrs["agg"].input_contracts = tuple(
        GroupInputContract(parent_group_id=parent, consumer_group_id="agg")
        for parent in ("root", "basic", "other")
    )
    attrs[FINAL_NODE_ID].final_contract = FinalAssemblyContract(
        contributor_contracts=tuple(
            FinalContributorContract(group_id=gid) for gid in ("agg", "basic")
        )
    )

    _fold_into_readers(graph, edges, attrs, "basic")

    assert list(graph.predecessors("agg")) == ["root", "dim", "other"]
    assert edges[("dim", "agg")].kind == EdgeKind.LINEAGE
    assert edges[("other", "agg")].kind == EdgeKind.CONSTRAINT
    assert "basic" not in graph and "basic" not in attrs
    assert not [edge for edge in edges if "basic" in edge]
    assert attrs["agg"].inlined_members == ("local.item_margin",)
    assert [c.parent_group_id for c in attrs["agg"].input_contracts] == [
        "root",
        "other",
    ]
    contract = attrs[FINAL_NODE_ID].final_contract
    assert contract is not None
    assert [c.group_id for c in contract.contributor_contracts] == ["agg"]
