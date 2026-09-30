"""The root partition (`root_partition.partition_root_demand`): which root
columns are sourced together, and for which reader."""

from pathlib import Path

import pytest

from trilogy import Dialects, Environment
from trilogy.core.processing import plan_trace
from trilogy.core.processing.v4_helper.models import RootReason
from trilogy.core.processing.v4_helper.region_domains import DomainKind

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


# two families off one fact, and a second fact the items fan out over
_FORKED = """
key user_id int;
property user_id.region string;
key product_id int;
property product_id.brand string;
key order_id int;
property order_id.amount int;
key item_id int;
property item_id.qty int;
key warehouse_id int;
property warehouse_id.wname string;

datasource users (user_id: user_id, region: region)
grain (user_id)
query '''select 1 as user_id, 'west' as region union all select 2, 'east'
union all select 3, 'south' ''';

datasource products (product_id: product_id, brand: brand)
grain (product_id)
query '''select 10 as product_id, 'A' as brand union all select 20, 'B'
union all select 30, 'C' ''';

datasource orders (order_id: order_id, user_id: ~user_id, amount: amount)
grain (order_id)
query '''select 100 as order_id, 1 as user_id, 50 as amount union all select 101, 2, 60''';

datasource items (
    item_id: item_id, order_id: order_id, product_id: ~product_id,
    user_id: ~user_id, qty: qty,
)
grain (item_id)
query '''select 1000 as item_id, 100 as order_id, 10 as product_id, 1 as user_id, 5 as qty
union all select 1001, 100, 20, 1, 7
union all select 1002, 101, 10, 2, 11''';

datasource shipments (item_id: item_id, warehouse_id: warehouse_id)
grain (item_id, warehouse_id)
query '''select 1000 as item_id, 1 as warehouse_id union all select 1000, 2
union all select 1001, 1 union all select 1002, 2''';

datasource warehouses (warehouse_id: warehouse_id, wname: wname)
grain (warehouse_id)
query '''select 1 as warehouse_id, 'w1' as wname union all select 2, 'w2' ''';
"""

_UNION = """
key cid int;
property cid.cname string;
key oid int;
property oid.ocust int;
property oid.amt float;
datasource customers (cid:cid, cname:cname) grain (cid)
  query '''select * from (values (1,'a'),(2,'b')) as t(cid, cname)''';
datasource orders (oid:oid, ocust:~ocust, amt:amt) grain (oid)
  query '''select * from (values (10,2,5.0),(11,4,7.0)) as t(oid, ocust, amt)''';
"""

_SCALAR = """
key customer_id int;
property customer_id.bal float;
key order_id int;

datasource customers (customer_id: customer_id, bal: bal)
grain (customer_id)
query '''select 1 as customer_id, 100.0 as bal union all select 2, 300.0''';

datasource orders (order_id: order_id, customer_id: ~customer_id)
grain (order_id)
query '''select 100 as order_id, 1 as customer_id''';

auto avg_bal <- avg(bal) by *;
"""


def _trace(query: str, model: str = _MODEL) -> tuple[plan_trace.PlanTrace, list[tuple]]:
    env, _ = Environment().parse(model)
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


def _domains(trace: plan_trace.PlanTrace) -> dict[tuple[str, ...], str]:
    """Region spans -> where its rows come from, over every plan of the trace."""
    return {
        tuple(d.spans): d.kind
        for s in trace.steps
        if s.title == "region domains added"
        for d in s.data.domains
    }


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


def test_entity_cluster_is_peeled_onto_its_key():
    trace, rows = _trace(
        "select product_id, brand, sum(sale_price) as revenue order by product_id asc;"
    )
    assert [g for g in _built(trace) if ":dim:local.product_id" in g]
    assert rows == [(1, "acme", 5.0), (2, "zed", 7.0), (3, "acme", 3.0)]


@pytest.mark.parametrize(
    "model,query,expected",
    [
        (
            _MODEL,
            "select user_id, state, sum(sale_price) as revenue;",
            {("local.user_id",): DomainKind.OWN},
        ),
        (
            _MODEL,
            "with r as select user_id, state, line_id; select r.user_id, r.line_id;",
            {("local.user_id",): DomainKind.OWN, ("r.user_id",): DomainKind.BOUNDARY},
        ),
        (
            _UNION,
            "select cid, cname, sum(amt) as total union join ocust = cid;",
            {("local.ocust",): DomainKind.RELATION},
        ),
        (
            _SCALAR,
            "where bal > avg_bal and order_id is null select count(customer_id) as n;",
            {("local.customer_id",): DomainKind.PADDED},
        ),
    ],
)
def test_a_demanded_region_says_where_its_rows_come_from(
    model: str, query: str, expected: dict[tuple[str, ...], DomainKind]
):
    trace, _ = _trace(query, model)
    assert _domains(trace) == {k: v.value for k, v in expected.items()}


def test_families_peeled_off_two_keys_source_as_one_cluster():
    trace, rows = _trace(
        "select item_id, warehouse_id, brand, region, sum(qty) as q,"
        " sum(amount) by order_id as order_amount"
        " order by item_id asc, warehouse_id asc, brand asc;",
        _FORKED,
    )
    assert [g for g in _built(trace) if g.endswith(":dim:local.item_id|local.order_id")]
    assert rows == [
        (1000, 1, "A", "west", 5, 50),
        (1000, 2, "A", "west", 5, 50),
        (1001, 1, "B", "west", 7, 50),
        (1002, 2, "A", "east", 11, 60),
        (None, None, "C", None, None, None),
        (None, None, None, "south", None, None),
    ]


def test_cluster_a_domain_carries_whole_rides_the_row_stream():
    trace, rows = _trace(
        "select item_id, order_id, brand, region, sum(qty) as q"
        " order by item_id asc, brand asc;",
        _FORKED,
    )
    assert not [g for g in _built(trace) if ":dim:" in g]
    assert rows == [
        (1000, 100, "A", "west", 5),
        (1001, 100, "B", "west", 7),
        (1002, 101, "A", "east", 11),
        (None, None, "C", None, None),
        (None, None, None, "south", None),
    ]
