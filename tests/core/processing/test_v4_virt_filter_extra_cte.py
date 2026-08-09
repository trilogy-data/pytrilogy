"""Minimal repros for row-preserving aggregate inputs (audit: q62/q73).

A filtered aggregate `sum(filter amt where <derived predicate>)` grouped by a
joined dimension can produce extra CTEs. The FILTER's
`CASE WHEN ... THEN amt ELSE NULL END` lineage belongs *inline* inside `sum(...)`
in the single grouped SELECT. Left to itself the Stage-2 group graph gives each
`_virt_filter_*` concept its own bucket, so it materializes the CASE columns in a
standalone CTE at the fact's own row grain, then **joins that CTE back** on the
fact PK and re-aggregates -- two extra SELECTs for no rows difference.

The same row-preserving rule applies to BASIC scalar wrappers, e.g.
`sum(amt - discount)`: the scalar expression should render inside the aggregate
instead of becoming an input-grain projection CTE.

This is purely plan size / CTE shape. The asserts below lock the desired shape
so row-preserving aggregate inputs stay inline."""

from trilogy import Dialects, Environment

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

_COMBINED_MODEL = """
key line_id int;
property line_id.amt float;
property line_id.discount float;
property line_id.ship int;
property line_id.ord int;
property line_id.days <- ship - ord;
property line_id.net_amount <- amt - discount;

key region_id int;
property region_id.region_name string;
property line_id.region_id int;

datasource facts (
    lid: line_id,
    amt: amt,
    discount: discount,
    ship: ship,
    ord: ord,
    rid: region_id,
)
grain (line_id)
query '''
select 1 lid, 5.0 amt, 1.0 discount, 40 ship, 10 ord, 1 rid union all
select 2 lid, 7.0 amt, 2.0 discount, 20 ship, 10 ord, 1 rid union all
select 3 lid, 9.0 amt, 3.0 discount, 60 ship, 10 ord, 2 rid
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

_COMBINED_QUERY = """
select
    region_name,
    count(line_id) -> line_count,
    sum(net_amount) -> total_net,
    coalesce(sum(filter amt where days > 30),0) -> late_amt,
order by region_name asc;
"""

_COMBINED_EXPECTED_ROWS = [("east", 2, 9.0, 0.0), ("west", 1, 6.0, 9.0)]

_ROW_FILTER_MODEL = """
key line_id int;
property line_id.ship int;
property line_id.ord int;
property line_id.days <- ship - ord;
property line_id.region_name string;

datasource facts (
    lid: line_id,
    ship: ship,
    ord: ord,
    rname: region_name,
)
grain (line_id)
query '''
select 1 lid, 40 ship, 10 ord, 'east' rname union all
select 2 lid, 20 ship, 10 ord, 'east' rname union all
select 3 lid, 60 ship, 10 ord, 'west' rname
''';
"""

_ROW_FILTER_QUERY = """
where days > 30
select
    region_name,
    count(line_id) -> line_count,
order by region_name asc;
"""

_ROW_FILTER_EXPECTED_ROWS = [("west", 1)]

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


def _gen_sql() -> str:
    env = Environment()
    env, _ = env.parse(_MODEL)
    engine = Dialects.DUCK_DB.default_executor(environment=env)
    return engine.generate_sql(_QUERY)[-1]


def _gen_basic_sql() -> str:
    env = Environment()
    env, _ = env.parse(_BASIC_MODEL)
    engine = Dialects.DUCK_DB.default_executor(environment=env)
    return engine.generate_sql(_BASIC_QUERY)[-1]


def _gen_combined_sql() -> str:
    env = Environment()
    env, _ = env.parse(_COMBINED_MODEL)
    engine = Dialects.DUCK_DB.default_executor(environment=env)
    return engine.generate_sql(_COMBINED_QUERY)[-1]


def _run():
    env = Environment()
    env, _ = env.parse(_MODEL)
    engine = Dialects.DUCK_DB.default_executor(environment=env)
    rows = engine.execute_text(_QUERY)[-1].fetchall()
    return [(r[0], float(r[1]), float(r[2])) for r in rows]


def _run_basic():
    env = Environment()
    env, _ = env.parse(_BASIC_MODEL)
    engine = Dialects.DUCK_DB.default_executor(environment=env)
    rows = engine.execute_text(_BASIC_QUERY)[-1].fetchall()
    return [(r[0], float(r[1])) for r in rows]


def _run_combined():
    env = Environment()
    env, _ = env.parse(_COMBINED_MODEL)
    engine = Dialects.DUCK_DB.default_executor(environment=env)
    rows = engine.execute_text(_COMBINED_QUERY)[-1].fetchall()
    return [(r[0], r[1], float(r[2]), float(r[3])) for r in rows]


def _gen_row_filter_sql() -> str:
    env = Environment()
    env, _ = env.parse(_ROW_FILTER_MODEL)
    engine = Dialects.DUCK_DB.default_executor(environment=env)
    return engine.generate_sql(_ROW_FILTER_QUERY)[-1]


def _run_row_filter() -> list[tuple[str, int]]:
    env = Environment()
    env, _ = env.parse(_ROW_FILTER_MODEL)
    engine = Dialects.DUCK_DB.default_executor(environment=env)
    return engine.execute_text(_ROW_FILTER_QUERY)[-1].fetchall()


def _gen_having_basic_key_sql() -> str:
    env = Environment()
    env, _ = env.parse(_HAVING_BASIC_KEY_MODEL)
    engine = Dialects.DUCK_DB.default_executor(environment=env)
    return engine.generate_sql(_HAVING_BASIC_KEY_QUERY)[-1]


def test_virt_filter_rows_match_baseline():
    assert _run() == _EXPECTED_ROWS


def test_virt_filter_no_extra_cte():
    """The FILTER CASE must not split into its own CTE that is joined back.

    This guards against emitting a standalone `_virt_filter_*` projection CTE at
    the fact PK grain and re-joining it before aggregating (q62/q73 verbosity
    pattern).
    """
    sql = _gen_sql()

    # The virtual-filter CASE alias should never surface as a *selected column*
    # in any CTE -- it belongs inline inside sum(...).
    assert ' as "_virt_filter' not in sql, (
        "the FILTER CASE was materialized in its own CTE then joined back; "
        "it should be inlined into the consuming sum(...)"
    )
    # No extra projection + self-join layer.
    assert sql.lower().count("select") == 8, sql


def test_basic_aggregate_input_rows_match_baseline():
    assert _run_basic() == _BASIC_EXPECTED_ROWS


def test_basic_aggregate_input_no_extra_cte():
    sql = _gen_basic_sql()

    assert ' as "net_amount"' not in sql
    assert "sum(" in sql
    assert sql.lower().count("select") == 8, sql


def test_root_basic_and_filter_inputs_share_compatible_aggregate():
    assert _run_combined() == _COMBINED_EXPECTED_ROWS

    sql = _gen_combined_sql()

    assert ' as "net_amount"' not in sql
    assert ' as "days"' not in sql
    assert ' as "_virt_filter' not in sql
    assert "count(" in sql
    assert '"quizzical"."ship" - "quizzical"."ord"' in sql
    assert sql.lower().count("select") == 8, sql


def test_basic_row_filter_stays_on_shared_preaggregate_scan():
    assert _run_row_filter() == _ROW_FILTER_EXPECTED_ROWS

    sql = _gen_row_filter_sql()

    assert ' as "days"' not in sql
    assert "JOIN" not in sql
    assert sql.lower().count("select") == 4, sql


def test_having_aggregate_with_basic_group_key_stays_materialized():
    sql = _gen_having_basic_key_sql()

    assert "HAVING" in sql
    assert "count(" in sql
