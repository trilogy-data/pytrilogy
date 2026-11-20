"""Aggregating over a union of `root partial` sources, grouped by a key derived
from a row property, must answer as a single complete source does (#1058).
The derived key is widened onto the union's arms after they resolved; a stale
arm resolution dropped it ("Missing source map entry for local.run_date")."""

import pytest

from trilogy import Dialects, Environment

MODEL = """
key id string;
property id.org_id string;
property id.shard enum<string>['a','b','c'];
property id.created_at timestamp;
property id.started_at timestamp;
property id.finished_at timestamp;
auto duration_seconds <- date_diff(started_at, finished_at, second);
auto run_date <- date(created_at);
auto run_hour <- hour(created_at);
auto org_upper <- upper(org_id);
"""

COLS = "run_id:id, org_id:org_id, shard:shard, created_at:created_at, started_at:started_at, finished_at:finished_at"

SETUP = """
create table runs_all as select * from (values
  ('r1', 'o1', 'a', timestamp '2026-11-18 05:00', timestamp '2026-11-18 05:01', timestamp '2026-11-18 05:03'),
  ('r2', 'o1', 'b', timestamp '2026-11-18 06:00', timestamp '2026-11-18 06:00', timestamp '2026-11-18 06:01'),
  ('r3', 'o2', 'b', timestamp '2026-11-19 07:00', timestamp '2026-11-19 07:00', timestamp '2026-11-19 07:10'),
  ('r4', 'o2', 'c', timestamp '2026-11-19 07:30', timestamp '2026-11-19 07:30', timestamp '2026-11-19 07:32')
) as t(run_id, org_id, shard, created_at, started_at, finished_at);
create table runs_a as select * from runs_all where shard = 'a';
create table runs_b as select * from runs_all where shard = 'b';
create table runs_c as select * from runs_all where shard = 'c';
create table runs_empty as select * from runs_all where false;
"""


def _partials(*shards: str) -> str:
    return "\n".join(
        f"root partial datasource runs_{s} ({COLS}) grain (id) "
        f"complete where shard = '{s}' address runs_{s};"
        for s in shards
    )


SINGLE = f"root datasource runs_all ({COLS}) grain (id) address runs_all;"
TWO_ARMS_ONE_EMPTY = (
    f"root partial datasource runs_a ({COLS}) grain (id) "
    "complete where shard = 'a' address runs_a;\n"
    f"root partial datasource runs_b ({COLS}) grain (id) "
    "complete where shard in ('b', 'c') address runs_empty;"
)

QUERIES = {
    "issue_avg_derived_by_derived_key": "select run_date, org_id, avg(duration_seconds) -> v;",
    "sum_derived_by_derived_key": "select run_date, sum(duration_seconds) -> v;",
    "max_derived_by_two_derived_keys": "select run_date, run_hour, max(duration_seconds) -> v;",
    "derived_keys_both_sides": "select run_date, org_upper, avg(duration_seconds) -> v;",
    "count_by_derived_key": "select run_date, count(id) -> v;",
    "filtered_avg_by_derived_key": "where org_id = 'o1' select run_date, avg(duration_seconds) -> v;",
    "multiple_derived_aggregates": "select run_date, avg(duration_seconds) -> v, min(duration_seconds) -> w;",
}


def _rows(sources: str, query: str) -> list[tuple]:
    ex = Dialects.DUCK_DB.default_executor(environment=Environment())
    ex.execute_raw_sql(SETUP)
    ex.parse_text(MODEL + sources)
    return sorted(ex.execute_text(query)[-1].fetchall(), key=repr)


@pytest.mark.parametrize("query", QUERIES.values(), ids=QUERIES.keys())
def test_partial_union_matches_single_source(query: str):
    expected = _rows(SINGLE, query)
    assert expected
    assert _rows(_partials("a", "b", "c"), query) == expected


@pytest.mark.parametrize("query", QUERIES.values(), ids=QUERIES.keys())
def test_two_partial_arms_one_empty(query: str):
    expected = _rows(
        f"root datasource runs_a ({COLS}) grain (id) address runs_a;", query
    )
    assert _rows(TWO_ARMS_ONE_EMPTY, query) == expected


def test_issue_1058_repro():
    rows = _rows(
        TWO_ARMS_ONE_EMPTY,
        "select run_date, org_id, avg(duration_seconds) -> avg_duration;",
    )
    assert [(str(d), o, v) for d, o, v in rows] == [("2026-11-18", "o1", 120.0)]
