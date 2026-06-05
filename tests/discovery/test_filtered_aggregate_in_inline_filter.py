import re
from pathlib import Path

from trilogy import Dialects, Environment


def setup():
    env = Environment(working_path=Path(__file__).parent)
    return Dialects.DUCK_DB.default_executor(environment=env)


def _group_by_blocks(sql: str) -> list[str]:
    return re.findall(
        r"GROUP BY(.*?)(?:HAVING|ORDER BY|\)|$)", sql, re.DOTALL | re.IGNORECASE
    )


# regression: an inline filtered aggregate (`agg(x ? cond) by grain`) used as the
# predicate of an outer `?` row-filter used to render the aggregate / its
# `_virt_filter_*` argument inside a GROUP BY, producing invalid SQL
# (DuckDB "GROUP BY clause cannot contain aggregates").
def test_inline_filtered_aggregate_not_in_group_by():
    exec = setup()
    sql = exec.generate_sql("""
import aggregate_testing;
auto z <- product_name ? (count(customer_id ? order_value > 40) by product_name) > 1;
select z order by z;
""")[-1]
    for block in _group_by_blocks(sql):
        assert "count(" not in block.lower(), f"aggregate in GROUP BY: {sql}"
        assert "_virt_filter" not in block, f"_virt_filter in GROUP BY: {sql}"


def test_inline_filtered_aggregate_matches_two_step():
    exec = setup()
    inline = exec.execute_query("""
import aggregate_testing;
auto z <- product_name ? (count(customer_id ? order_value > 40) by product_name) > 1;
select z order by z;
""").fetchall()
    two_step = setup().execute_query("""
import aggregate_testing;
auto pc <- count(customer_id ? order_value > 40) by product_name;
auto z <- product_name ? pc > 1;
select z order by z;
""").fetchall()
    assert inline == two_step
    assert sorted(r[0] for r in inline if r[0] is not None) == ["Keyboard", "Mouse"]


# regression: an inline-filtered aggregate referenced in the SELECT, plus a
# top-level WHERE on a different (finer-grain) field, used to raise
# `SyntaxError: Have {...flag...} and need {outer where}` — the StrategyNode
# wrapping the filter's row parent dropped the global condition the parent had
# already applied, so condition validation couldn't prove it. The two filters
# must stack at the row level before aggregation.
def test_inline_filtered_agg_plus_outer_where_stacks():
    exec = setup()
    sql = exec.generate_sql("""
import aggregate_testing;
auto pc <- count(order_id ? order_value > 40) by product_id;
select product_id, pc where customer_id = 101 order by product_id;
""")[-1]
    assert "order_value" in sql and "customer_id" in sql
    rows = setup().execute_query("""
import aggregate_testing;
auto pc <- count(order_id ? order_value > 40) by product_id;
select product_id, pc where customer_id = 101 order by product_id;
""").fetchall()
    assert [tuple(r) for r in rows] == [(201, 0), (202, 0), (203, 1)]
