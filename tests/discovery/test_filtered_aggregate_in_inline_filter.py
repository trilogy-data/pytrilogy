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
