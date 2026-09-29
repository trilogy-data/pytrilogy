"""The root partition (`root_partition.partition_root_demand`): which root
columns are sourced together, and for which reader."""

from pathlib import Path

from trilogy import Dialects, Environment
from trilogy.core.processing import plan_trace
from trilogy.core.processing.v4_helper.models import RootReason

_MODEL = """
key user_id int;
property user_id.state string;
key product_id int;
property product_id.category string;
property product_id.brand string;
property product_id.cost float;
key line_id int;
property line_id.sale_price float;

datasource users (uid: user_id, st: state)
grain (user_id)
query '''
select 1 uid, 'ca' st union all select 2 uid, 'ny' st union all select 3 uid, 'wa' st
''';

datasource products (pid: product_id, cat: category, b: brand, c: cost)
grain (product_id)
query '''
select 1 pid, 'toys' cat, 'acme' b, 1.0 c union all
select 2 pid, 'toys' cat, 'zed' b, 2.0 c union all
select 3 pid, 'books' cat, 'acme' b, 3.0 c
''';

datasource order_items (lid: line_id, uid: ~user_id, pid: product_id, price: sale_price)
grain (line_id)
query '''
select 1 lid, 1 uid, 1 pid, 5.0 price union all
select 2 lid, 1 uid, 2 pid, 7.0 price union all
select 3 lid, 2 uid, 3 pid, 3.0 price
''';
"""


def _trace(query: str) -> tuple[plan_trace.PlanTrace, list[tuple]]:
    env, _ = Environment().parse(_MODEL)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    plan_trace.start(query)
    try:
        executor.generate_sql(query)
    finally:
        trace = plan_trace.stop()
    assert trace is not None
    rows = [tuple(r) for r in executor.execute_text(query)[-1].fetchall()]
    return trace, rows


def _root_reasons(trace: plan_trace.PlanTrace) -> dict[str, str | None]:
    """ROOT group id -> reason, off the last group graph of the plan."""
    graph = [s for s in trace.steps if s.phase == "group_graph"][-1].data.graph
    return {
        gid: node.get("reason")
        for gid, node in graph.nodes.items()
        if node.get("derivation") == "root"
    }


def _built(trace: plan_trace.PlanTrace) -> list[str]:
    return [s.data.group for s in trace.steps if s.phase == "node"]


def test_every_root_group_carries_its_reason():
    trace, _ = _trace(
        "where sum(sale_price) by user_id > 4"
        " select user_id, state, sum(sale_price) as revenue;"
    )
    reasons = _root_reasons(trace)
    assert reasons
    assert None not in reasons.values()
    assert RootReason.REGION.value in reasons.values()
    assert RootReason.ROW_STREAM.value in reasons.values()


def test_basic_input_root_is_named_apart_from_a_peel():
    path = Path(__file__).parents[2] / "modeling" / "thelook_duckdb" / "adhoc04.preql"
    env, statements = Environment(working_path=path.parent).parse(path.read_text())
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    plan_trace.start("adhoc04")
    try:
        executor.generate_sql(statements[-1])
    finally:
        trace = plan_trace.stop()
    assert trace is not None
    reasons = _root_reasons(trace)
    inputs = [g for g, r in reasons.items() if r == RootReason.BASIC_INPUT.value]
    assert inputs == ["grp:root:root:∅:basic_input:local.id"]


def test_cluster_stays_on_the_row_stream_that_reads_its_table():
    """`category` filters before the aggregate, so the row stream reads
    `products` whatever is peeled: `brand` rides it."""
    trace, rows = _trace(
        "where category = 'toys'"
        " select product_id, brand, sum(sale_price) as revenue"
        " order by product_id asc;"
    )
    assert not [g for g in _built(trace) if ":dim:" in g]
    assert rows == [(1, "acme", 5.0), (2, "zed", 7.0)]


def test_cluster_is_peeled_when_the_row_stream_does_not_read_its_table():
    trace, rows = _trace(
        "select product_id, brand, sum(sale_price) as revenue order by product_id asc;"
    )
    assert [g for g in _built(trace) if ":dim:local.product_id" in g]
    assert rows == [(1, "acme", 5.0), (2, "zed", 7.0), (3, "acme", 3.0)]
