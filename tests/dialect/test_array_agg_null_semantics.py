"""`array_agg` collects present values on every dialect.

A NULL value on a real row is not an
element, and a group of only NULLs is NULL like an empty group. Backends
disagree natively (DuckDB and Postgres keep the NULL, ClickHouse drops it,
BigQuery raises), so each dialect lowers to its own NULL-ignoring form.
ClickHouse has no Nullable(Array), so its all-NULL group is `[]`, not NULL.
"""

import pytest

from tests.helpers.rows import executor_for, fetch_rows
from trilogy.core.enums import FunctionType
from trilogy.dialect.base import BaseDialect
from trilogy.dialect.bigquery import BigqueryDialect
from trilogy.dialect.clickhouse import ClickhouseDialect
from trilogy.dialect.duckdb import DuckDBDialect
from trilogy.dialect.postgres import PostgresDialect
from trilogy.dialect.presto import PrestoDialect, TrinoDialect
from trilogy.dialect.snowflake import SnowflakeDialect

MODEL = """
key customer_id int;
property customer_id.name string;
key order_id int;
property order_id.amount int?;

root datasource customers (customer_id: customer_id, name: name)
grain (customer_id)
query '''
select 1 as customer_id, 'ann' as name union all
select 2, 'bob' union all
select 3, 'cat'
''';

root datasource orders (order_id: order_id, customer_id: ~customer_id, amount: amount)
grain (order_id)
query '''
select 100 as order_id, 1 as customer_id, 10 as amount union all
select 101, 1, null union all
select 102, 2, null
''';
"""

RENDERINGS = [
    (BaseDialect, "array_agg(x) FILTER (WHERE x IS NOT NULL)"),
    (DuckDBDialect, "array_agg(x) FILTER (WHERE x IS NOT NULL)"),
    (PostgresDialect, "array_agg(x) FILTER (WHERE x IS NOT NULL)"),
    (PrestoDialect, "array_agg(x) FILTER (WHERE x IS NOT NULL)"),
    (TrinoDialect, "array_agg(x) FILTER (WHERE x IS NOT NULL)"),
    (BigqueryDialect, "ARRAY_AGG(x IGNORE NULLS)"),
    (SnowflakeDialect, "NULLIF(array_agg(x), ARRAY_CONSTRUCT())"),
    (ClickhouseDialect, "groupArray(x)"),
]


@pytest.mark.parametrize(
    "dialect,expected", RENDERINGS, ids=lambda d: getattr(d, "__name__", d)
)
def test_every_dialect_drops_null_elements(dialect, expected):
    assert dialect.FUNCTION_MAP[FunctionType.ARRAY_AGG](["x"], []) == expected


def test_grain_match_singleton_is_null_for_a_null_value():
    formula = BaseDialect.FUNCTION_GRAIN_MATCH_MAP[FunctionType.ARRAY_AGG](["x"], [])
    assert formula == "CASE WHEN x IS NOT NULL THEN [x] ELSE NULL END"


def test_null_values_are_not_elements_and_all_null_is_null():
    rows = sorted(
        (c, sorted(a) if a is not None else None)
        for c, a in fetch_rows(
            executor_for(MODEL), "select customer_id, array_agg(amount) as amts;"
        )
    )
    # ann: one value and one NULL; bob: only a NULL; cat: no orders
    assert rows == [(1, [10]), (2, None), (3, None)]
