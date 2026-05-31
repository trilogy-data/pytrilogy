import re

from trilogy import Dialects
from trilogy.core.query_processor import process_query
from trilogy.core.statements.author import SelectStatement
from trilogy.dialect.bigquery import BigqueryDialect
from trilogy.hooks.query_debugger import DebuggingHook
from trilogy.parser import parse


def test_select():
    declarations = """
key user_id int metadata(description="the description");
property user_id.display_name string metadata(description="The display name ");
property user_id.about_me string metadata(description="User provided description");

key tag_id int;

key post_id int;
metric post_count <-count(post_id);
metric user_post_count <- count(post_id) by tag_id;
metric avg_user_post_count <-avg(user_post_count) by user_id;


datasource posts (
    user_id: user_id,
    id: post_id,
    tag_id: tag_id
    )
    grain (post_id)
    address `bigquery-public-data.stackoverflow.post_history`
;


datasource users (
    id: user_id,
    display_name: display_name,
    about_me: about_me,
    )
    grain (user_id)
    address `bigquery-public-data.stackoverflow.users`
;


select
    avg_user_post_count
;


    """
    env, parsed = parse(declarations)
    select: SelectStatement = parsed[-1]

    query = process_query(statement=select, environment=env, hooks=[DebuggingHook()])

    generator = BigqueryDialect()
    sql = generator.compile_statement(query)

    assert re.search(r"(count\([A-z0-9\_]+\.`id`\) as `user_post_count`)", sql)
    assert re.search(
        r"avg\([A-z0-9\_]+\.`user_post_count`\) as `avg_user_post_count`", sql
    )


def test_aliased_aggregate_referenced_in_having_and_order_by() -> None:
    """Regression for q57: when an aggregate-auto is given a SELECT alias
    (e.g. `avg_monthly_overall as avg_sales`), HAVING/ORDER BY references
    to the un-aliased source used to silently collapse to the aggregate's
    inner concept in downstream CTEs — producing
    `monthly_total - monthly_total` instead of `monthly_total - avg_sales`.

    Two fixes back this up:
      (A) `_rewrite_aliased_source_refs` rewrites source refs in HAVING/ORDER
          BY to point at the SELECT alias before downstream rendering sees them.
      (B) `_cte_at_aggregate_grain` guards the FUNCTION_GRAIN_MATCH_MAP
          fallback so any remaining miss raises INVALID_REFERENCE_BUG
          rather than silently emitting the aggregate's first argument.
    """
    exec = Dialects.DUCK_DB.default_executor()
    src = """
key category string;
key brand string;
key year int;
key month int;
key month_seq int;
property <category, brand, year, month, month_seq>.sales float;
datasource sales (
    category: category, brand: brand,
    year: year, month: month, month_seq: month_seq,
    sales: sales,
) grain (category, brand, year, month, month_seq) address sales;

auto monthly_total <- sum(sales) by category, brand, year, month, month_seq;
auto avg_monthly_overall <- avg(monthly_total) by category, brand;

select category, brand, year, month,
    avg_monthly_overall as avg_sales,
    monthly_total,
having avg_monthly_overall > 0
order by (monthly_total - avg_monthly_overall) asc;
"""
    sql = exec.generate_sql(src)[0]
    assert "INVALID_REFERENCE_BUG" not in sql
    order_by_section = sql.split("ORDER BY")[1]
    # The arithmetic must keep both operands distinct — no collapse.
    assert 'monthly_total" - "' in order_by_section.replace("`", '"')
    assert "avg_sales" in order_by_section
    # Outer WHERE (HAVING after pushdown) must filter on the alias.
    where_section = sql.split("WHERE")[-1].split("ORDER BY")[0]
    assert "avg_sales" in where_section
    assert '"monthly_total" > 0' not in where_section.replace("`", '"')
