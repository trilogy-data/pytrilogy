from trilogy import Dialects
from trilogy.core.models.environment import Environment
from trilogy.execution.state.state_store import refresh_stale_assets
from trilogy.execution.state.watermarks import schema_mismatch

MODEL = """
key id int;
property id.ts timestamp;

root datasource raw_source (id, ts)
grain (id)
query '''
select 1 as id, '2024-01-15 10:30:00+00'::timestamptz as ts
union all select 2 as id, '{latest}'::timestamptz as ts
''';

datasource processed (id, ts)
grain (id)
address tz_processed
incremental by ts;
"""


def _executor(latest: str = "2024-01-15 22:30:00+00"):
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_raw_sql("SET TimeZone='America/New_York'")
    executor.parse_text(MODEL.format(latest=latest))
    return executor


def test_aware_target_settles_after_one_refresh():
    executor = _executor()
    assert refresh_stale_assets(executor).refreshed_count == 1
    assert refresh_stale_assets(executor).refreshed_count == 0
    column_type = executor.execute_raw_sql(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_name = 'tz_processed' AND column_name = 'ts'"
    ).fetchone()[0]
    assert column_type == "TIMESTAMP WITH TIME ZONE"


def test_aware_incremental_appends_only_newer_instants():
    executor = _executor()
    refresh_stale_assets(executor)
    executor.environment = Environment()
    executor.parse_text(MODEL.format(latest="2024-01-16 01:00:00+00"))
    assert refresh_stale_assets(executor).refreshed_count == 1
    rows = executor.execute_raw_sql(
        "SELECT id, ts FROM tz_processed ORDER BY ts"
    ).fetchall()
    assert [r[0] for r in rows] == [1, 2, 2]
    assert refresh_stale_assets(executor).refreshed_count == 0


def test_naive_table_under_aware_declaration_is_drift():
    executor = _executor()
    executor.execute_raw_sql(
        "CREATE TABLE tz_processed AS SELECT 1 AS id, TIMESTAMP '2024-01-15' AS ts"
    )
    drift = schema_mismatch(executor.environment.datasources["processed"], executor)
    assert drift == "type of ts (datetime vs timestamp)"
