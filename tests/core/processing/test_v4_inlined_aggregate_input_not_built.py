"""A row-preserving input every reader computes inline is folded out of the
group graph before it is built (docs/handoff_open_optimization_items.md,
item 2)."""

from pytest import raises

from tests.core.processing.test_v4_dim_peel_not_built import _MODEL
from trilogy import Dialects, Environment
from trilogy.core import graph as nx
from trilogy.core.enums import Derivation
from trilogy.core.env_processor import generate_graph
from trilogy.core.processing import plan_trace
from trilogy.core.processing.nodes import History
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
from trilogy.core.processing.v4_helper.strategy_builder import (
    _fold_into_readers,
    _parent_nodes_for,
)


def _executor():
    env, _ = Environment().parse(_MODEL)
    return Dialects.DUCK_DB.default_executor(environment=env)


def _trace(executor, query: str) -> plan_trace.PlanTrace:
    with plan_trace.recording(query) as trace:
        executor.generate_sql(query)
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


def test_input_behind_an_inlined_filter_is_not_built():
    executor = _executor()
    query = """select order_id, sum(item_margin ? item_margin > 3) as big_margin
order by order_id asc;"""
    trace = _trace(executor, query)
    assert not _built(trace, "basic") and not _built(trace, "filter")
    rows = [tuple(r) for r in executor.execute_text(query)[-1].fetchall()]
    assert rows == [(10, 9.0), (11, None)]


def test_condition_phase_twins_are_not_built():
    executor = _executor()
    query = """auto ca_avg <- avg(sale_price ? user_id = 1) by order_id;
where order_id in (10, 11) and ca_avg > 1
select line_id, rank(order_id) over (order by ca_avg asc) as rnk
order by line_id asc;"""
    trace = _trace(executor, query)
    assert not _built(trace, "filter")
    rows = [tuple(r) for r in executor.execute_text(query)[-1].fetchall()]
    assert rows == [(1, 1), (2, 1)]


def test_reader_holding_folded_members_is_never_built():
    env, _ = Environment().parse(_MODEL)
    build_env = env.materialize_for_select()
    graph = nx.DiGraph()
    graph.add_node("filter")
    attrs = {
        "filter": GroupAttrs(
            depth_label=DepthLabel.D0,
            derivation=Derivation.FILTER,
            inlined_members=("local.item_margin",),
        )
    }
    with raises(ValueError, match="planned to be computed inline"):
        _parent_nodes_for(
            graph,
            {},
            attrs,
            {},
            "filter",
            build_env,
            generate_graph(build_env),
            History(base_environment=env),
            needed=set(),
            root_requests={},
            mandatory_list=[],
        )


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
    attrs["basic"].inlined_members = ("local.cost_basis",)
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
    assert attrs["agg"].inlined_members == ("local.cost_basis", "local.item_margin")
    assert [c.parent_group_id for c in attrs["agg"].input_contracts] == [
        "root",
        "other",
    ]
    contract = attrs[FINAL_NODE_ID].final_contract
    assert contract is not None
    assert [c.group_id for c in contract.contributor_contracts] == ["agg"]
