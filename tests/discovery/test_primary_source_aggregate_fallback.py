"""Tests that aggregate concepts can be computed from primary sources when a
materialized aggregate datasource cannot satisfy the query (or doesn't exist).

The bug: discovery's path-finding stage references a `_virt_agg_*@Grain<Abstract>`
node that was never injected into the search graph (aggregate-derivation nodes
are intentionally stripped at node_merge_node.py:149-166). When the discovery
loop promotes a materialized aggregate to ROOT in get_loop_iteration_targets,
priority selection picks a non-aggregate ROOT first; gen_merge_node's path
search then references the missing virt_agg node and aborts.
"""

import pytest

from trilogy import Dialects

PRIMARY_ONLY_SETUP = """
key id int;
key origin_code string;
key destination_code string;
key flight_date date;

auto count <- count(id);

datasource flights (
    id:id,
    origin:~origin_code,
    destination:~destination_code,
    flight_date:flight_date,
)
grain (id)
query '''
select 1 as id, 'JFK' as origin, 'LAX' as destination, DATE '2024-01-15' as flight_date
union all
select 2 as id, 'JFK' as origin, 'ORD' as destination, DATE '2024-01-15' as flight_date
union all
select 3 as id, 'LAX' as origin, 'JFK' as destination, DATE '2024-01-16' as flight_date
union all
select 4 as id, 'LAX' as origin, 'ORD' as destination, DATE '2024-01-16' as flight_date
union all
select 5 as id, 'ORD' as origin, 'JFK' as destination, DATE '2024-01-17' as flight_date
''';

datasource origin_codes (
    origin_code:origin_code,
)
grain (origin_code)
query '''
select 'JFK' as origin_code
union all
select 'LAX' as origin_code
union all
select 'ORD' as origin_code
union all
select 'SFO' as origin_code
''';

datasource destination_codes (
    destination_code:destination_code,
)
grain (destination_code)
query '''
select 'JFK' as destination_code
union all
select 'LAX' as destination_code
union all
select 'ORD' as destination_code
union all
select 'SFO' as destination_code
''';
"""


PARTIAL_AGGREGATE_SUFFIX = """
datasource flight_count_by_origin_destination_date (
    origin_code:~origin_code,
    destination_code:~destination_code,
    flight_date:flight_date,
    flight_count:count,
)
grain (origin_code, destination_code, flight_date)
query '''
select 'JFK' as origin_code, 'LAX' as destination_code, DATE '2024-01-15' as flight_date, 1 as flight_count
union all
select 'JFK' as origin_code, 'ORD' as destination_code, DATE '2024-01-15' as flight_date, 1 as flight_count
union all
select 'LAX' as origin_code, 'JFK' as destination_code, DATE '2024-01-16' as flight_date, 1 as flight_count
union all
select 'LAX' as origin_code, 'ORD' as destination_code, DATE '2024-01-16' as flight_date, 1 as flight_count
union all
select 'ORD' as origin_code, 'JFK' as destination_code, DATE '2024-01-17' as flight_date, 1 as flight_count
''';
"""


SIMPLE_SETUP = """
key id int;
key code string;

auto count <- count(id);

datasource events (
    id:id,
    event_code:~code,
)
grain (id)
query '''
select 1 as id, 'A' as event_code
union all
select 2 as id, 'A' as event_code
union all
select 3 as id, 'B' as event_code
''';

datasource codes (
    code:code,
)
grain (code)
query '''
select 'A' as code
union all
select 'B' as code
union all
select 'C' as code
''';
"""


def test_primary_source_aggregate_simple_count_with_dim():
    exec = Dialects.DUCK_DB.default_executor()
    exec.parse_text(SIMPLE_SETUP)
    generated = exec.generate_sql("SELECT code, count;")[-1]
    assert "events" in generated, generated
    assert "group by" in generated.lower(), generated


def test_primary_source_aggregate_no_precomputed():
    exec = Dialects.DUCK_DB.default_executor()
    exec.parse_text(PRIMARY_ONLY_SETUP)
    generated = exec.generate_sql(
        "SELECT origin_code, destination_code, flight_date, count;"
    )[-1]
    assert "flights" in generated, generated
    assert "group by" in generated.lower(), generated


def test_partial_precomputed_uses_aggregate():
    """When a pre-computed aggregate exists at the query grain (even with
    partial join keys), discovery should use the aggregate rather than
    rescanning primary rows — that is the whole point of materializing it."""
    exec = Dialects.DUCK_DB.default_executor()
    exec.parse_text(PRIMARY_ONLY_SETUP + PARTIAL_AGGREGATE_SUFFIX)
    generated = exec.generate_sql(
        "SELECT origin_code, destination_code, flight_date, count;"
    )[-1]
    assert "flight_count_by_origin_destination_date" in generated, generated


def test_partial_precomputed_uses_aggregate_with_filter_in_select():
    """When the WHERE concept is also in the SELECT (so the query grain
    matches the materialization's grain), discovery must still pick the
    precomputed aggregate rather than rescanning the primary table."""
    exec = Dialects.DUCK_DB.default_executor()
    exec.parse_text(PRIMARY_ONLY_SETUP + PARTIAL_AGGREGATE_SUFFIX)
    generated = exec.generate_sql(
        "WHERE flight_date >= '2024-01-16'::date "
        "SELECT flight_date, origin_code, destination_code, count;"
    )[-1]
    assert "flight_count_by_origin_destination_date" in generated, generated
    assert "flights" not in generated, generated


@pytest.mark.xfail(
    reason=(
        "Aggregate rollup combined with a WHERE on a grain component (the "
        "filter is on flight_date which is a grain key of the materialization, "
        "but flight_date isn't in the SELECT) currently falls back to the "
        "primary table rather than rolling up the aggregate. The rollup "
        "feature handles unfiltered rollup; WHERE+rollup is the open case."
    ),
    strict=True,
)
def test_partial_precomputed_uses_aggregate_with_grain_filter():
    """A WHERE clause on a grain component of the aggregate (here flight_date)
    should still let discovery pick the precomputed aggregate, applying the
    filter at the source and rolling up to the requested grain — instead of
    forcing a rescan of the primary table to recompute count(id)."""
    exec = Dialects.DUCK_DB.default_executor()
    exec.parse_text(PRIMARY_ONLY_SETUP + PARTIAL_AGGREGATE_SUFFIX)
    generated = exec.generate_sql(
        "WHERE flight_date >= '2024-01-16'::date "
        "SELECT origin_code, destination_code, count;"
    )[-1]
    assert "flight_count_by_origin_destination_date" in generated, generated
    assert "flights" not in generated, generated
