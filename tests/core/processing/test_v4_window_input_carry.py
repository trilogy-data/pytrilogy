"""Regression lock: a derivation reading both a window's output and the value
the window read is planned, not dropped.

`gap <- event_time - lag(event_time) over (...)` needs the window's output AND
its input. `_satisfy_parent_projection_contract` credited the WINDOW sibling
with `event_time` because it measured what a sibling supplies as that sibling's
*grandparents'* outputs -- and a window READS `event_time` while emitting only
`prior_event_time`. So the dimension parent's projection stripped `event_time`
as already-supplied, `satisfiable_outputs` then found `gap` unsourceable, and
everything derived from it went with it: the `avg(gap) by route, direction`
aggregate came out as a GroupNode whose outputs were its grouping keys alone.

Nothing raised. The columns simply left the projection, so a select answered
with fewer columns than it asked for, and a persist -- which writes
positionally -- shifted every later column into the wrong field. Distilled from
a transit model whose ten-column target rendered seven.

Same seam and same class as the 2026-08-16 sibling-aggregate drop, which was
the other half of the same conflation: there a parent's *own* computed values
were invisible to `parent_needed`. See
`tests/persistence/test_persist_projection_matrix.py::window_over_derived_input`
for the persist-arity half of this one.

`event_time` must be DERIVED (a coalesce over two raw columns) to reproduce: a
raw column survives because the scan below still offers it, while a value
computed under the window has nowhere above it to be re-derived from."""

from trilogy import Dialects, Environment

_MODEL = """
key route string;
key stop string;
key direction int;
key vehicle string;
key event_id int;
property event_id.arrival_time int?;
property event_id.departure_time int?;

auto event_time <- coalesce(arrival_time, departure_time);

datasource events (
    eid: event_id,
    route: ?route,
    stop: ?stop,
    direction: ?direction,
    vehicle: ?vehicle,
    arr: ?arrival_time,
    dep: ?departure_time,
)
grain (event_id)
query '''
select 1 eid, 'A' route, 's1' stop, 0 direction, 'v1' vehicle, 0 arr, null dep union all
select 2 eid, 'A' route, 's1' stop, 0 direction, 'v2' vehicle, 10 arr, null dep union all
select 3 eid, 'A' route, 's1' stop, 0 direction, 'v3' vehicle, null arr, 30 dep union all
select 4 eid, 'B' route, 's2' stop, 1 direction, 'v4' vehicle, 0 arr, null dep union all
select 5 eid, 'B' route, 's2' stop, 1 direction, 'v5' vehicle, 5 arr, null dep
''';

auto prior_event_time <- lag(event_time, 1) over (partition by route, stop, direction order by event_time asc);
auto gap <- event_time - prior_event_time;
auto route_avg_gap <- avg(gap) by route, direction;
auto gap_class <- case
    when gap is null then 'first'
    when gap >= route_avg_gap then 'wide'
    else 'normal'
end;
"""


def _run(query: str) -> tuple[str, list[tuple]]:
    env = Environment()
    env, _ = env.parse(_MODEL)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    sql = executor.generate_sql(query)[-1]
    rows = executor.execute_text(query)[-1].fetchall()
    return sql, [tuple(r) for r in rows]


def test_window_input_reaches_dependent_aggregate():
    """Selecting a key the window does not partition by forces a group -- the
    shape that lost the whole `gap` branch and answered with one column."""
    sql, rows = _run("""
select
    vehicle,
    gap_class,
order by vehicle asc;
""")
    assert "gap_class" in sql, sql
    assert rows == [
        ("v1", "first"),
        ("v2", "normal"),
        ("v3", "wide"),
        ("v4", "first"),
        ("v5", "wide"),
    ], rows


def test_window_input_and_output_project_together():
    _, rows = _run("""
select
    event_id,
    gap,
    route_avg_gap,
order by event_id asc;
""")
    assert rows == [
        (1, None, 15.0),
        (2, 10, 15.0),
        (3, 20, 15.0),
        (4, None, 5.0),
        (5, 5, 5.0),
    ], rows
