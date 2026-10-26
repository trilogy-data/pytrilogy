"""A ratio of filtered aggregates by a derived key, selected beside a `<*>`
property, must plan without a keyless join (issue #769)."""

import re
from datetime import date, datetime

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment

MODEL = """
key pageview_id string;
key site_key string;
key session_id string;
property pageview_id.bot_reason int;
property pageview_id.event_time datetime;
property <*>.updated_through datetime;

auto event_date <- date(event_time);

datasource pv (
    pageview_id: pageview_id,
    site_key: site_key,
    session_id: ?session_id,
    bot_reason: bot_reason,
    event_time: event_time,
    updated_through: updated_through,
)
grain (pageview_id)
query '''
select 'a:1' as pageview_id, 'a' as site_key, 's1' as session_id, 0 as bot_reason,
  timestamp '2026-10-20 09:00:00' as event_time, timestamp '2026-10-21 10:00:00' as updated_through
union all select 'a:2', 'a', 's1', 0, timestamp '2026-10-20 09:05:00', timestamp '2026-10-21 10:00:00'
union all select 'a:3', 'a', 's2', 1, timestamp '2026-10-20 09:10:00', timestamp '2026-10-21 10:00:00'
''';

auto daily_pageviews <- count(pageview_id ? bot_reason = 0) by site_key, event_date;
auto daily_sessions <- {agg}(session_id ? bot_reason = 0) by site_key, event_date;
auto pages_per_session <- daily_pageviews / daily_sessions;
auto weekday_ratio <- pages_per_session + day_of_week(event_date);
auto pageview_events <- count(event_time ? bot_reason = 0) by pageview_id;
"""

UPDATED = datetime(2026, 10, 21, 10, 0, 0)


def _executor(agg: str = "count_distinct"):
    executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    executor.parse_text(MODEL.replace("{agg}", agg))
    return executor


def _rows(query: str, agg: str = "count_distinct") -> list[tuple]:
    return [tuple(r) for r in _executor(agg).execute_text(query)[-1].fetchall()]


@pytest.mark.parametrize("agg", ["count_distinct", "count"])
def test_filtered_ratio_by_derived_key_beside_abstract_property(agg: str):
    query = "select site_key, event_date, pages_per_session, updated_through;"
    assert _rows(query, agg) == [("a", date(2026, 10, 20), 2.0, UPDATED)]


def test_ratio_beside_its_own_by_key_is_not_pageview_grained():
    query = "select site_key, event_date, weekday_ratio, updated_through;"
    assert _rows(query) == [("a", date(2026, 10, 20), 5.0, UPDATED)]


def test_abstract_property_cross_joins_instead_of_rekeying():
    executor = _executor()
    query = "select pageview_id, event_date, pages_per_session, updated_through;"
    sql = executor.generate_sql(query)[-1]
    *_, scalar_cte = re.findall(r'"(\w+)"\."updated_through" as "updated_through"', sql)
    assert f'JOIN "{scalar_cte}" on 1=1' in sql
    assert sorted(_rows(query)) == [
        (f"a:{i}", date(2026, 10, 20), 2.0, UPDATED) for i in (1, 2, 3)
    ]


def test_aggregates_by_entity_and_derived_key_bridge_on_both_keys():
    query = "select pageview_id, pageview_events, daily_pageviews, updated_through;"
    assert sorted(_rows(query)) == [
        ("a:1", 1, 2, UPDATED),
        ("a:2", 1, 2, UPDATED),
        ("a:3", 0, 2, UPDATED),
    ]
