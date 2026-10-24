"""A ratio of filtered aggregates by a derived key, selected beside a `<*>`
property, must plan without a keyless join (issue #769)."""

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
"""


@pytest.mark.parametrize("agg", ["count_distinct", "count"])
def test_filtered_ratio_by_derived_key_beside_abstract_property(agg: str):
    executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    executor.parse_text(MODEL.replace("{agg}", agg))
    rows = executor.execute_text(
        "select site_key, event_date, pages_per_session, updated_through;"
    )[-1].fetchall()
    assert [tuple(r) for r in rows] == [
        ("a", date(2026, 10, 20), 2.0, datetime(2026, 10, 21, 10, 0, 0))
    ]
