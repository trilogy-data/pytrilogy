"""A row-preserving input every reader computes inline is folded out of the
group graph before it is built (docs/handoff_open_optimization_items.md,
item 2)."""

from tests.core.processing.test_v4_dim_peel_not_built import _MODEL
from trilogy import Dialects, Environment
from trilogy.core.processing import plan_trace


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
