from tests.helpers.executor import mock_factory
from trilogy import Dialects
from trilogy.dialect import PrestoConfig

_GRAND_TOTAL_QUERY = """
key date_string string;
key date_converted <- date_string::date;
key id int;
auto latest_date <- max(date_converted) by *;
datasource ds_info(
    date_string,
    id
)
grain (date_string, id)
query '''
select '2021-01-01' as date_string, 1 as id
union all select '2021-02-01' as date_string, 2 as id
''';
where date_converted = latest_date
select date_converted, count(id) as id_count;
"""


def test_grand_total_aggregate_cross_joins_not_all_rows() -> None:
    """A `max(x) by *` grand-total aggregate is single-row: it must cross-join
    ON 1=1 and never materialize the abstract `__preql_internal.all_rows` marker
    as a real join column. The planner regressed this twice by sourcing that
    concept (once demanding it as an aggregate `by` input in `_upstream_aggregate`),
    which forced `1 as __preql_internal_all_rows` on both sides and an equality
    join."""
    sql = Dialects.DUCK_DB.default_executor().generate_sql(_GRAND_TOTAL_QUERY)[-1]
    assert "__preql_internal_all_rows" not in sql, sql
    assert "on 1=1" in sql, sql


def test_bound_conversion_existence() -> None:
    executor = Dialects.DUCK_DB.default_executor()

    results = executor.execute_text("""

key date_string string;
auto date_converted <- date_string::date;
key id int;

auto latest_date <- max(date_converted) by *;
                          
datasource ds_info(
    date_string,
    id
)
grain (date_string, id)
query '''
select
    '2021-01-01' as date_string,
    1 as id
    union all
    select
    '2021-02-01' as date_string,
    2 as id
    union all
    select
    '2021-02-01' as date_string,
    3 as id
    union all
    select
    '2021-02-01' as date_string,
    4 as id
''';
                          
where date_converted=latest_date and 1=1
select
    date_converted,
    count(id) as id_count
;
""")[-1]

    rows = results.fetchall()[0]
    assert rows.date_converted is not None
    assert rows.date_converted.isoformat() == "2021-02-01"
    assert rows.id_count == 3


def test_bound_conversion_existence_presto() -> None:
    executor = Dialects.PRESTO.default_executor(
        conf=PrestoConfig(
            host="localhost",
            port=8080,
            username="user",
            password="password",
            catalog="default",
        ),
        _engine_factory=mock_factory,
    )

    results = executor.generate_sql("""

key date_string string;
key date_converted <- date_string::date;
key id int;

auto latest_date <- max(date_converted) by *;
                          
datasource ds_info(
    date_string,
    id
)
grain (date_string, id)
query '''
select
    '2021-01-01' as date_string,
    1 as id
    union all
    select
    '2021-02-01' as date_string,
    2 as id
    union all
    select
    '2021-02-01' as date_string,
    3 as id
    union all
    select
    '2021-02-01' as date_string,
    4 as id
''';
                          
where date_converted=latest_date and 1=1
select
    date_converted,
    count(id) as id_count
;
""")[-1]

    # The essence guarded: the latest_date bound joins in and filters BEFORE
    # the count aggregation, over a (date_converted, id) dedup. CTE names are
    # planner-specific (no re-applied final WHERE; rows execution-verified on
    # duckdb 2026-07-28), so match structure, not names.
    import re

    assert re.search(
        r'WHERE\s+"\w+"\."date_converted" = "\w+"\."latest_date" and True\s+'
        r"GROUP BY\s+1,\s+2\)",
        results,
    ), results
