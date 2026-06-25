"""Minimal repros for row-preserving aggregate inputs (audit: q62/q73).

A filtered aggregate `sum(filter amt where <derived predicate>)` grouped by a
joined dimension produces extra CTEs under v4 vs v3. v3 renders the FILTER's
`CASE WHEN ... THEN amt ELSE NULL END` lineage *inline* inside `sum(...)` in the
single grouped SELECT. v4's Stage-2 group graph gives each `_virt_filter_*`
concept its own bucket, so it materializes the CASE columns in a standalone CTE
at the fact's own row grain, then **joins that CTE back** on the fact PK and
re-aggregates -- two extra SELECTs for no rows difference.

The same row-preserving rule applies to BASIC scalar wrappers, e.g.
`sum(amt - discount)`: the scalar expression should render inside the aggregate
instead of becoming an input-grain projection CTE.

Rows are identical between planners; this is purely plan size / CTE shape. The
asserts below lock the desired v3-parity shape so v4 keeps row-preserving
aggregate inputs inline."""

from trilogy import Dialects, Environment
from trilogy.constants import CONFIG

_MODEL = """
key order_id int;
key item_id int;
property <order_id,item_id>.ship int;
property <order_id,item_id>.ord int;
property <order_id,item_id>.amt float;
property <order_id,item_id>.days <- ship - ord;

key region_id int;
property region_id.region_name string;
property <order_id,item_id>.region_id int;

datasource facts (
    oid: order_id,
    iid: item_id,
    ship: ship,
    ord: ord,
    amt: amt,
    rid: region_id,
)
grain (order_id, item_id)
query '''
select 1 oid, 1 iid, 40 ship, 10 ord, 5.0 amt, 1 rid union all
select 1 oid, 2 iid, 90 ship, 10 ord, 7.0 amt, 1 rid union all
select 2 oid, 1 iid, 30 ship, 10 ord, 9.0 amt, 2 rid
''';

datasource regions (
    rid: region_id,
    rname: region_name,
)
grain (region_id)
query '''
select 1 rid, 'east' rname union all select 2 rid, 'west' rname
''';
"""

_QUERY = """
select
    region_name,
    coalesce(sum(filter amt where days <= 30),0) -> early,
    coalesce(sum(filter amt where days > 30),0) -> late,
order by region_name asc;
"""

_EXPECTED_ROWS = [("east", 5.0, 7.0), ("west", 9.0, 0.0)]

_BASIC_MODEL = """
key order_id int;
key item_id int;
property <order_id,item_id>.amt float;
property <order_id,item_id>.discount float;
property <order_id,item_id>.net_amount <- amt - discount;

key region_id int;
property region_id.region_name string;
property <order_id,item_id>.region_id int;

datasource facts (
    oid: order_id,
    iid: item_id,
    amt: amt,
    discount: discount,
    rid: region_id,
)
grain (order_id, item_id)
query '''
select 1 oid, 1 iid, 5.0 amt, 1.0 discount, 1 rid union all
select 1 oid, 2 iid, 7.0 amt, 2.0 discount, 1 rid union all
select 2 oid, 1 iid, 9.0 amt, 3.0 discount, 2 rid
''';

datasource regions (
    rid: region_id,
    rname: region_name,
)
grain (region_id)
query '''
select 1 rid, 'east' rname union all select 2 rid, 'west' rname
''';
"""

_BASIC_QUERY = """
select
    region_name,
    sum(net_amount) -> total_net,
order by region_name asc;
"""

_BASIC_EXPECTED_ROWS = [("east", 9.0), ("west", 6.0)]

_HAVING_BASIC_KEY_MODEL = """
key order_id int;
key item_id int;
key date_id int;
property item_id.item_desc string;
property item_id.desc_truncated <- substring(item_desc, 1, 3);

datasource facts (
    oid: order_id,
    iid: item_id,
    did: date_id,
)
grain (order_id)
query '''
select 1 oid, 10 iid, 100 did union all
select 2 oid, 10 iid, 100 did union all
select 3 oid, 20 iid, 100 did
''';

datasource items (
    iid: item_id,
    item_desc: item_desc,
)
grain (item_id)
query '''
select 10 iid, 'abcdef' item_desc union all select 20 iid, 'uvwxyz' item_desc
''';

auto combo_count <- count(order_id) by desc_truncated, item_id, date_id;

rowset frequent_items <- select
    item_id as frequent_item_id,
    --combo_count,
having
    combo_count > 1
;
"""

_HAVING_BASIC_KEY_QUERY = """
select
    frequent_items.frequent_item_id,
order by frequent_items.frequent_item_id asc;
"""


def _gen_sql(v4: bool) -> str:
    prior = CONFIG.use_v4_discovery
    CONFIG.use_v4_discovery = v4
    try:
        env = Environment()
        env, _ = env.parse(_MODEL)
        engine = Dialects.DUCK_DB.default_executor(environment=env)
        return engine.generate_sql(_QUERY)[-1]
    finally:
        CONFIG.use_v4_discovery = prior


def _gen_basic_sql(v4: bool) -> str:
    prior = CONFIG.use_v4_discovery
    CONFIG.use_v4_discovery = v4
    try:
        env = Environment()
        env, _ = env.parse(_BASIC_MODEL)
        engine = Dialects.DUCK_DB.default_executor(environment=env)
        return engine.generate_sql(_BASIC_QUERY)[-1]
    finally:
        CONFIG.use_v4_discovery = prior


def _run(v4: bool):
    prior = CONFIG.use_v4_discovery
    CONFIG.use_v4_discovery = v4
    try:
        env = Environment()
        env, _ = env.parse(_MODEL)
        engine = Dialects.DUCK_DB.default_executor(environment=env)
        rows = engine.execute_text(_QUERY)[-1].fetchall()
        return [(r[0], float(r[1]), float(r[2])) for r in rows]
    finally:
        CONFIG.use_v4_discovery = prior


def _run_basic(v4: bool):
    prior = CONFIG.use_v4_discovery
    CONFIG.use_v4_discovery = v4
    try:
        env = Environment()
        env, _ = env.parse(_BASIC_MODEL)
        engine = Dialects.DUCK_DB.default_executor(environment=env)
        rows = engine.execute_text(_BASIC_QUERY)[-1].fetchall()
        return [(r[0], float(r[1])) for r in rows]
    finally:
        CONFIG.use_v4_discovery = prior


def _gen_having_basic_key_sql() -> str:
    prior = CONFIG.use_v4_discovery
    CONFIG.use_v4_discovery = True
    try:
        env = Environment()
        env, _ = env.parse(_HAVING_BASIC_KEY_MODEL)
        engine = Dialects.DUCK_DB.default_executor(environment=env)
        return engine.generate_sql(_HAVING_BASIC_KEY_QUERY)[-1]
    finally:
        CONFIG.use_v4_discovery = prior


def test_virt_filter_rows_match_baseline():
    """Both planners are correct -- this is a size issue, not a rows issue."""
    assert _run(v4=False) == _EXPECTED_ROWS
    assert _run(v4=True) == _EXPECTED_ROWS


def test_virt_filter_no_extra_cte_under_v4():
    """v4 should not split the FILTER CASE into its own CTE that is joined back.

    This guards against emitting a standalone `_virt_filter_*` projection CTE at
    the fact PK grain and re-joining it before aggregating (q62/q73 verbosity
    pattern).
    """
    v3_sql = _gen_sql(v4=False)
    v4_sql = _gen_sql(v4=True)

    # The virtual-filter CASE alias should never surface as a *selected column*
    # in any CTE -- v3 keeps it inline inside sum(...), never as `... as "_virt_filter"`.
    assert ' as "_virt_filter' not in v3_sql
    assert ' as "_virt_filter' not in v4_sql, (
        "v4 materialized the FILTER CASE in its own CTE then joined it back; "
        "it should be inlined into the consuming sum(...)"
    )

    # Same SELECT/CTE count as v3 (no extra projection + self-join layer).
    assert v4_sql.lower().count("select") == v3_sql.lower().count("select")


def test_basic_aggregate_input_rows_match_baseline():
    assert _run_basic(v4=False) == _BASIC_EXPECTED_ROWS
    assert _run_basic(v4=True) == _BASIC_EXPECTED_ROWS


def test_basic_aggregate_input_no_extra_cte_under_v4():
    v3_sql = _gen_basic_sql(v4=False)
    v4_sql = _gen_basic_sql(v4=True)

    assert ' as "net_amount"' not in v3_sql
    assert ' as "net_amount"' not in v4_sql
    assert "sum(" in v4_sql
    assert v4_sql.lower().count("select") == v3_sql.lower().count("select")


def test_having_aggregate_with_basic_group_key_stays_materialized():
    sql = _gen_having_basic_key_sql()

    assert "HAVING" in sql
    assert "count(" in sql
