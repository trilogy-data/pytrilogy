"""A summary table is not a legal source for an aggregate the query filters
below the summary's own grain.

`flight_summary` is keyed (origin_code, destination_code, flight_date) and the
request groups to a property of origin_code, so the date drops out. A SUM-roll
to that grain is only correct if the date filter is applied to the summary
BEFORE the roll. It is not: the predicate is routed to whichever source binds
the filter column, which here is the raw fact, so the roll sums every row and
the plan then inner-joins it to a filtered, non-distinct fact scan and re-sums
-- dropping the filter and fanning the aggregate out at the same time
(west 6, east 2 against a five-row table).

`network_build.rollup_concepts_by_node` therefore withholds the rollup binding
whenever a filter finer than the target grain is in play, including one this
request has deferred to a caller (`SourceRequest.deferred_conditions`) -- which
is how it arrives here, since gen_root re-plans the row scan unconditioned and
applies the WHERE above.
"""

from trilogy import Dialects

MODEL = """
key id int;
key origin_code string;
key destination_code string;
property id.flight_date date;
property origin_code.origin_region string;

auto flight_count <- count(id);

datasource flight (
    id: id,
    origin_code: origin_code,
    destination_code: destination_code,
    flight_date: flight_date,
)
grain (id)
query '''
select 1 as id, 'A' as origin_code, 'X' as destination_code, '2024-01-01'::date as flight_date
union all select 2, 'A', 'Y', '2024-01-01'::date
union all select 3, 'B', 'X', '2024-01-01'::date
union all select 4, 'A', 'X', '2024-01-02'::date
union all select 5, 'B', 'Y', '2024-01-02'::date
''';

datasource origin_dim (
    origin_code: origin_code,
    origin_region: origin_region,
)
grain (origin_code)
query '''select 'A' as origin_code, 'west' as origin_region
union all select 'B', 'east' ''';

datasource flight_summary (
    origin_code: origin_code,
    destination_code: destination_code,
    flight_date: flight_date,
    flight_count: flight_count,
)
grain (origin_code, destination_code, flight_date)
query '''
select 'A' as origin_code, 'X' as destination_code, '2024-01-01'::date as flight_date, 1 as flight_count
union all select 'A', 'Y', '2024-01-01'::date, 1
union all select 'B', 'X', '2024-01-01'::date, 1
union all select 'A', 'X', '2024-01-02'::date, 1
union all select 'B', 'Y', '2024-01-02'::date, 1
''';
"""


def _executor():
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(MODEL)
    return executor


def _rows(executor, query: str) -> list[tuple]:
    sql = executor.generate_sql(query)[-1]
    assert "INVALID_REFERENCE_BUG" not in sql, sql
    return sorted(tuple(r) for r in executor.execute_raw_sql(sql).fetchall())


def test_filter_finer_than_grain_counts_only_matching_rows():
    executor = _executor()
    query = "select origin_region, flight_count where flight_date = '2024-01-01'::date;"
    assert _rows(executor, query) == [("east", 1), ("west", 2)]


def test_filter_on_a_dropped_key_counts_only_matching_rows():
    executor = _executor()
    query = "select origin_region, flight_count where destination_code = 'X';"
    assert _rows(executor, query) == [("east", 1), ("west", 2)]


def test_unfiltered_request_still_rolls_the_summary():
    executor = _executor()
    query = "select origin_region, flight_count;"
    assert _rows(executor, query) == [("east", 2), ("west", 3)]
    assert '"flight_summary"' in executor.generate_sql(query)[-1]


def test_filter_at_the_target_grain_still_rolls_the_summary():
    executor = _executor()
    query = "select origin_region, flight_count where origin_region = 'west';"
    assert _rows(executor, query) == [("west", 3)]
    assert '"flight_summary"' in executor.generate_sql(query)[-1]
