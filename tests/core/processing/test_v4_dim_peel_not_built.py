"""A dimension peel (`_split_root_dimension_clusters`) is not built when it
cannot be read: keyed by the bucket's own row key, it rescans the fact; keyed
by an extension region's span, the region domain takes its members and the
domain is what FINAL reads (docs/handoff_dim_peel_beside_region_domains.md)."""

from trilogy import Dialects, Environment
from trilogy.core.processing import plan_trace

_MODEL = """
key line_id int;
key order_id int;
key user_id int;
key product_id int;
property line_id.sale_price float;
property user_id.state string;
property product_id.cost float;
property line_id.item_margin <- sale_price - cost;
auto revenue <- sum(sale_price);
auto margin <- sum(item_margin);

datasource order_items (
    lid: line_id,
    oid: order_id,
    uid: ~user_id,
    pid: ~product_id,
    price: sale_price,
)
grain (line_id)
query '''
select 1 lid, 10 oid, 1 uid, 1 pid, 5.0 price union all
select 2 lid, 10 oid, 1 uid, 2 pid, 7.0 price union all
select 3 lid, 11 oid, 2 uid, 1 pid, 3.0 price
''';

datasource users (uid: user_id, st: state)
grain (user_id)
query '''
select 1 uid, 'ca' st union all select 2 uid, 'ny' st union all select 3 uid, 'wa' st
''';

datasource products (pid: product_id, c: cost)
grain (product_id)
query '''
select 1 pid, 1.0 c union all select 2 pid, 2.0 c union all select 3 pid, 3.0 c
''';
"""


def _built_peels(executor, query: str) -> list[str]:
    """The dimension peels built; the regraft's solid source is
    `grp:root:root:∅:basic_input:<key>` and is not one."""
    with plan_trace.recording(query) as trace:
        executor.generate_sql(query)
    return [
        s.data.group
        for s in trace.steps
        if s.phase == "node" and ":dim:" in s.data.group
    ]


def _executor():
    env, _ = Environment().parse(_MODEL)
    return Dialects.DUCK_DB.default_executor(environment=env)


def test_no_peel_keyed_by_a_span_the_domain_takes():
    executor = _executor()
    query = "select user_id, state, sum(sale_price) as revenue order by user_id asc;"
    assert not _built_peels(executor, query)
    rows = [tuple(r) for r in executor.execute_text(query)[-1].fetchall()]
    assert rows == [(1, "ca", 12.0), (2, "ny", 3.0), (3, "wa", None)]


def test_no_peel_keyed_by_the_facts_own_key():
    executor = _executor()
    query = """
    select order_id, line_id, user_id, product_id, revenue, margin
    order by line_id asc nulls last, user_id asc nulls last, product_id asc nulls last;
    """
    assert not _built_peels(executor, query)
    rows = [tuple(r) for r in executor.execute_text(query)[-1].fetchall()]
    assert rows == [
        (10, 1, 1, 1, 5.0, 4.0),
        (10, 2, 1, 2, 7.0, 5.0),
        (11, 3, 2, 1, 3.0, 2.0),
        (None, None, 3, None, None, None),
        (None, None, None, 3, None, None),
    ]
