"""Regression coverage for sharing grouped join streams across consumers."""

from decimal import Decimal

from trilogy import Dialects, Environment
from trilogy.constants import CONFIG

_MODEL = """
key sale_id int;
key date_id int;
property sale_id.date_id int;
property sale_id.amount float;
property date_id.year int;
property date_id.month int;

datasource sales (
    sid: sale_id,
    did: date_id,
    amt: amount,
)
grain (sale_id)
query '''
select 1 sid, 10 did, 5.0 amt union all
select 2 sid, 11 did, 7.0 amt union all
select 3 sid, 12 did, 9.0 amt
''';

datasource dates (
    did: date_id,
    yr: year,
    mo: month,
)
grain (date_id)
query '''
select 10 did, 2024 yr, 1 mo union all
select 11 did, 2024 yr, 2 mo union all
select 12 did, 2025 yr, 1 mo
''';

auto monthly_sales <- sum(amount) by year, month;
auto yearly_avg <- avg(monthly_sales) by year;
"""

_QUERY = """
select
    year,
    month,
    monthly_sales,
    yearly_avg,
order by year asc, month asc;
"""

_EXPECTED_ROWS = [
    (2024, 1, Decimal("5.0"), 6.0),
    (2024, 2, Decimal("7.0"), 6.0),
    (2025, 1, Decimal("9.0"), 9.0),
]


def _engine():
    env = Environment()
    env, _ = env.parse(_MODEL)
    return Dialects.DUCK_DB.default_executor(environment=env)


def test_aggregate_consumer_reuses_grouped_join_stream_rows():
    prior = CONFIG.use_v4_discovery
    try:
        CONFIG.use_v4_discovery = False
        assert _engine().execute_text(_QUERY)[-1].fetchall() == _EXPECTED_ROWS
        CONFIG.use_v4_discovery = True
        assert _engine().execute_text(_QUERY)[-1].fetchall() == _EXPECTED_ROWS
    finally:
        CONFIG.use_v4_discovery = prior


def test_aggregate_consumer_does_not_rescan_base_join_stream():
    prior = CONFIG.use_v4_discovery
    CONFIG.use_v4_discovery = True
    try:
        sql = _engine().generate_sql(_QUERY)[-1]
    finally:
        CONFIG.use_v4_discovery = prior

    assert sql.count(') as "sales"') == 1
    assert sql.count(') as "dates"') == 1
    assert 'avg("wakeful"."monthly_sales")' in sql
