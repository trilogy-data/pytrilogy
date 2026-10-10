"""Count-of-a-key buckets fold into a finer-grain sibling aggregate stream as
COUNT(DISTINCT ...) instead of planning a dedup CTE re-joined at the output
grain (s45; TPC-DS q83/q16/q95 shape). Guards: the fold requires the counted
key to ride the finer stream LITERALLY, and never fires for a key with a home
datasource (its count population is the home table, not the fact's image)."""

from trilogy import Dialects

FACT_SETUP = """
key order_id int;
key channel string;
property <order_id, channel>.quantity int;

datasource order_lines (
    order_id,
    channel,
    quantity
)
grain (order_id, channel)
query '''
SELECT 1 as order_id, 'WEB' as channel, 10 as quantity
UNION ALL
SELECT 1, 'STORE', 20
UNION ALL
SELECT 2, 'WEB', 30
UNION ALL
SELECT 3, 'STORE', 40
''';
"""

HOME_TABLE_SETUP = """
key user_id int;
key post_id int;

datasource posts (
    user_id,
    id: post_id
)
grain (post_id)
query '''
SELECT 1 as id, 100 as user_id
UNION ALL
SELECT 2, 100
''';

datasource users (
    id: user_id
)
grain (user_id)
query '''
SELECT 100 as id
UNION ALL
SELECT 200
''';
"""


def test_count_of_key_folds_to_count_distinct():
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(FACT_SETUP)
    sql = executor.generate_sql("""select
    sum(quantity ? channel = 'WEB') as web_qty,
    count(order_id ? channel = 'WEB') as web_orders,
;""")[-1]
    assert "count(distinct " in sql.lower(), sql
    assert "1=1" not in sql, sql
    rows = executor.execute_text("""select
    sum(quantity ? channel = 'WEB') as web_qty,
    count(order_id ? channel = 'WEB') as web_orders,
;""")[-1].fetchall()
    assert rows[0].web_qty == 40, rows
    assert rows[0].web_orders == 2, rows


def test_count_of_home_table_key_counts_full_population():
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(HOME_TABLE_SETUP)
    query = """select
    count(post_id) as post_count,
    count(user_id) as user_count,
;"""
    rows = executor.execute_text(query)[-1].fetchall()
    assert rows[0].post_count == 2, rows
    assert rows[0].user_count == 2, rows


PARTIAL_FACT_SETUP = """
key order_id int;
key line int;
key ship_id int;
property <order_id, line>.amount float;
property <order_id, ship_id>.cost float;

datasource lines (order_id, line, amount)
grain (order_id, line)
query '''
SELECT 1 as order_id, 1 as line, 5.0 as amount
UNION ALL
SELECT 1, 2, 6.0
UNION ALL
SELECT 2, 1, 7.0
UNION ALL
SELECT 3, 1, 1.0
''';

datasource ships (~order_id, ship_id, cost)
grain (order_id, ship_id)
query '''
SELECT 1 as order_id, 1 as ship_id, 1.0 as cost
UNION ALL
SELECT 1, 2, 2.0
''';
"""


def test_count_of_key_never_shares_a_stream_binding_it_partially():
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(PARTIAL_FACT_SETUP)
    rows = executor.execute_text(
        "select count(order_id) as orders, sum(cost) as total_cost;"
    )[-1].fetchall()
    assert rows[0].orders == 3, rows
    assert rows[0].total_cost == 3, rows
    sql = executor.generate_sql(
        "select count(order_id) as orders, sum(amount) as total_amount;"
    )[-1]
    assert "count(distinct " in sql.lower(), sql


def test_ratio_folded_beside_count_distinct_renders_it_distinct(monkeypatch):
    from dataclasses import replace

    from trilogy.core import optimization

    original = optimization.build_optimization_rule_plan

    def plan(*args, **kwargs):
        phases = original(*args, **kwargs)
        (collapse,) = [p for p in phases if p.name == "collapse_single_parent"]
        return phases + [
            replace(collapse, name="collapse_again", depends_on=(), refires_after=())
        ]

    monkeypatch.setattr(optimization, "build_optimization_rule_plan", plan)
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(FACT_SETUP)
    query = """select
    sum(quantity) as qty,
    count(order_id) as orders,
    qty / orders as per_order,
;"""
    sql = executor.generate_sql(query)[-1]
    assert "WITH" not in sql, sql
    assert sql.lower().count("count(distinct") == 2, sql
    rows = executor.execute_text(query)[-1].fetchall()
    assert rows[0].orders == 3, rows
    assert rows[0].per_order == 100 / 3, rows
