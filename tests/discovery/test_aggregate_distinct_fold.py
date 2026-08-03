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
