import os
from pathlib import Path

import pytest

from trilogy import Dialects
from trilogy.core.models.core import DataType
from trilogy.core.models.environment import Environment
from trilogy.execution.state.state_store import refresh_stale_assets

V1_PATH = Path(__file__).parent / "schema_refresh_v1.preql"
V2_PATH = Path(__file__).parent / "schema_refresh_v2.preql"
V3_PATH = Path(__file__).parent / "schema_refresh_v3_type_change.preql"
V4_PATH = Path(__file__).parent / "schema_refresh_v4_incremental.preql"


def _make_executor():
    return Dialects.DUCK_DB.default_executor()


def _get_columns(executor, table: str) -> set[str]:
    rows = executor.execute_raw_sql(
        f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}'"
    ).fetchall()
    return {row[0] for row in rows}


def test_schema_change_triggers_full_refresh():
    """When a table schema changes, refresh detects the mismatch and recreates the table."""
    executor = _make_executor()

    with open(V1_PATH) as f:
        executor.parse_text(f.read())

    result_v1 = refresh_stale_assets(executor)
    assert result_v1.refreshed_count > 0

    cols_v1 = _get_columns(executor, "schema_refresh_test")
    assert cols_v1 == {"id", "value", "version"}

    # Reset environment and re-parse with updated schema (new 'label' column)
    executor.environment = Environment()
    with open(V2_PATH) as f:
        executor.parse_text(f.read())

    # Incremental key is unchanged (version=1 in both root and table), so only
    # schema mismatch detection should mark this as stale.
    result_v2 = refresh_stale_assets(executor)
    assert (
        result_v2.refreshed_count > 0
    ), "Expected refresh due to schema mismatch but no assets were refreshed"

    cols_v2 = _get_columns(executor, "schema_refresh_test")
    assert (
        "label" in cols_v2
    ), f"Expected 'label' column after schema refresh, got {cols_v2}"
    assert cols_v2 == {"id", "value", "label", "version"}


def test_type_change_triggers_full_refresh():
    """When a column's type changes, refresh detects the mismatch and recreates the table."""
    executor = _make_executor()

    # v1: 'value' is a string column
    with open(V1_PATH) as f:
        executor.parse_text(f.read())

    result_v1 = refresh_stale_assets(executor)
    assert result_v1.refreshed_count > 0

    db_type_v1 = executor.execute_raw_sql(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_name = 'schema_refresh_test' AND column_name = 'value'"
    ).fetchone()[0]
    assert db_type_v1.upper() in ("VARCHAR", "TEXT", "STRING")

    # v3: same columns but 'value' is now int
    executor.environment = Environment()
    with open(V3_PATH) as f:
        executor.parse_text(f.read())

    result_v3 = refresh_stale_assets(executor)
    assert (
        result_v3.refreshed_count > 0
    ), "Expected refresh due to type mismatch but no assets were refreshed"

    db_type_v3 = executor.execute_raw_sql(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_name = 'schema_refresh_test' AND column_name = 'value'"
    ).fetchone()[0]
    assert db_type_v3.upper() in ("INTEGER", "INT", "INT4")


def test_incremental_refresh_appends_only_new_rows():
    """Incremental staleness should INSERT only rows with version > current max, not replace."""
    executor = _make_executor()

    # v4 uses a grain-specific version property (property id.version int) so the planner
    # can push WHERE version > N into the source query.
    with open(V4_PATH) as f:
        executor.parse_text(f.read())

    # First run: table absent, full OVERWRITE creates it with version=1 rows (ids 1,2)
    # and version=2 rows (ids 3,4) - but wait, the incremental key is version, so
    # the first run gets ALL rows (no existing watermark to filter against).
    result_v1 = refresh_stale_assets(executor)
    assert result_v1.refreshed_count > 0

    rows_after_v1 = executor.execute_raw_sql(
        "SELECT id, version FROM schema_refresh_incremental ORDER BY id"
    ).fetchall()
    assert len(rows_after_v1) == 4

    # Simulate source gaining new data at version=3 by re-parsing with an extended root.
    executor.environment = Environment()
    executor.parse_text(
        """
key id int;
property id.value string;
property id.version int;

root datasource raw_source (id, value, version)
grain (id)
query '''
select 1 as id, 'hello' as value, 1 as version
union all select 2 as id, 'world' as value, 1 as version
union all select 3 as id, 'new' as value, 2 as version
union all select 4 as id, 'also_new' as value, 2 as version
union all select 5 as id, 'newest' as value, 3 as version
''';

datasource processed (id, value, version)
grain (id)
address schema_refresh_incremental
incremental by version;
"""
    )

    result_v2 = refresh_stale_assets(executor)
    assert (
        result_v2.refreshed_count > 0
    ), "Expected incremental refresh for new version data"

    rows_after_v2 = executor.execute_raw_sql(
        "SELECT id, version FROM schema_refresh_incremental ORDER BY id"
    ).fetchall()
    # The table should have 5 rows: the original 4 plus the new version=3 row
    assert (
        len(rows_after_v2) == 5
    ), f"Expected 5 rows (4 old + 1 new), got {len(rows_after_v2)}"
    ids = {r[0] for r in rows_after_v2}
    assert ids == {1, 2, 3, 4, 5}


# Snowflake preql uses the same v1/v3 files but targets a Snowflake executor (via fakesnow).
# Snowflake stores INTEGER as NUMBER and VARCHAR as TEXT in information_schema.
SNOWFLAKE_V1 = """
property <*>.version int;
key id int;
property id.value string;

root datasource raw_source (id, value, version)
grain (id)
query '''
select 1 as id, 'hello' as value, 1 as version
union all
select 2 as id, 'world' as value, 1 as version
''';

datasource processed (id, value, version)
grain (id)
address schema_refresh_sf
incremental by version;
"""

SNOWFLAKE_V3 = """
property <*>.version int;
key id int;
property id.value int;

root datasource raw_source (id, value, version)
grain (id)
query '''
select 1 as id, 42 as value, 1 as version
union all
select 2 as id, 99 as value, 1 as version
''';

datasource processed (id, value, version)
grain (id)
address schema_refresh_sf
incremental by version;
"""


def test_snowflake_type_change_triggers_refresh(snowflake_engine):
    """Snowflake (fakesnow): type change on a column is detected as schema mismatch."""
    executor = snowflake_engine

    executor.environment = Environment()
    executor.parse_text(SNOWFLAKE_V1)
    result_v1 = refresh_stale_assets(executor)
    assert result_v1.refreshed_count > 0

    db_type_v1 = executor.execute_raw_sql(
        "SELECT data_type FROM information_schema.columns "
        "WHERE UPPER(table_name) = 'SCHEMA_REFRESH_SF' AND UPPER(column_name) = 'VALUE'"
    ).fetchone()[0]
    assert db_type_v1.upper() in ("TEXT", "VARCHAR")

    executor.environment = Environment()
    executor.parse_text(SNOWFLAKE_V3)
    result_v3 = refresh_stale_assets(executor)
    assert (
        result_v3.refreshed_count > 0
    ), "Expected refresh due to Snowflake type mismatch but no assets were refreshed"

    db_type_v3 = executor.execute_raw_sql(
        "SELECT data_type FROM information_schema.columns "
        "WHERE UPPER(table_name) = 'SCHEMA_REFRESH_SF' AND UPPER(column_name) = 'VALUE'"
    ).fetchone()[0]
    assert db_type_v3.upper() in ("NUMBER", "INTEGER", "INT")


# --- normalize_db_type unit tests ---


def test_duckdb_normalize_db_type():
    dialect = Dialects.DUCK_DB.default_executor().generator
    assert dialect.normalize_db_type("INTEGER") == DataType.INTEGER
    assert dialect.normalize_db_type("VARCHAR") == DataType.STRING
    assert dialect.normalize_db_type("GEOMETRY") == DataType.GEOGRAPHY
    assert dialect.normalize_db_type("GEOMETRY('OGC:CRS84')") == DataType.GEOGRAPHY
    assert dialect.normalize_db_type("BOOLEAN") == DataType.BOOL
    assert dialect.normalize_db_type("DATE") == DataType.DATE
    assert dialect.normalize_db_type("TIMESTAMP") == DataType.DATETIME
    assert dialect.normalize_db_type("TIMESTAMP WITH TIME ZONE") == DataType.TIMESTAMP
    assert dialect.normalize_db_type("DOUBLE") == DataType.FLOAT
    assert dialect.normalize_db_type("DECIMAL(18,3)") == DataType.NUMERIC


def test_snowflake_normalize_db_type():
    from trilogy.dialect.snowflake import SnowflakeDialect

    dialect = SnowflakeDialect()
    assert dialect.normalize_db_type("NUMBER") == DataType.INTEGER
    assert dialect.normalize_db_type("TEXT") == DataType.STRING
    assert dialect.normalize_db_type("FLOAT") == DataType.FLOAT
    assert dialect.normalize_db_type("BOOLEAN") == DataType.BOOL
    assert dialect.normalize_db_type("DATE") == DataType.DATE
    assert dialect.normalize_db_type("TIMESTAMP_NTZ") == DataType.DATETIME
    assert dialect.normalize_db_type("TIMESTAMP_LTZ") == DataType.TIMESTAMP


def test_bigquery_normalize_db_type():
    from trilogy.dialect.bigquery import BigqueryDialect

    dialect = BigqueryDialect()
    assert dialect.normalize_db_type("INT64") == DataType.INTEGER
    assert dialect.normalize_db_type("INTEGER") == DataType.INTEGER
    assert dialect.normalize_db_type("STRING") == DataType.STRING
    assert dialect.normalize_db_type("FLOAT64") == DataType.FLOAT
    assert dialect.normalize_db_type("FLOAT") == DataType.FLOAT
    assert dialect.normalize_db_type("BOOL") == DataType.BOOL
    assert dialect.normalize_db_type("BOOLEAN") == DataType.BOOL
    assert dialect.normalize_db_type("DATE") == DataType.DATE
    assert dialect.normalize_db_type("DATETIME") == DataType.DATETIME
    assert dialect.normalize_db_type("TIMESTAMP") == DataType.TIMESTAMP
    assert dialect.normalize_db_type("NUMERIC") == DataType.NUMERIC


@pytest.mark.skip(
    reason="Requires BigQuery write credentials and BQ_TEST_DATASET env var"
)
def test_bigquery_type_change_triggers_refresh():
    """BQ e2e: type change on a column is detected as schema mismatch.

    Set BQ_TEST_DATASET to a writable dataset (e.g. 'my_project.my_dataset') to run.
    """
    dataset = os.environ["BQ_TEST_DATASET"]
    executor = Dialects.BIGQUERY.default_executor()

    bq_v1 = f"""
property <*>.version int;
key id int;
property id.value string;

root datasource raw_source (id, value, version)
grain (id)
query '''
select 1 as id, 'hello' as value, 1 as version
union all
select 2 as id, 'world' as value, 1 as version
''';

datasource processed (id, value, version)
grain (id)
address {dataset}.schema_refresh_bq
incremental by version;
"""

    bq_v3 = f"""
property <*>.version int;
key id int;
property id.value int;

root datasource raw_source (id, value, version)
grain (id)
query '''
select 1 as id, 42 as value, 1 as version
union all
select 2 as id, 99 as value, 1 as version
''';

datasource processed (id, value, version)
grain (id)
address {dataset}.schema_refresh_bq
incremental by version;
"""

    executor.parse_text(bq_v1)
    result_v1 = refresh_stale_assets(executor)
    assert result_v1.refreshed_count > 0

    executor.environment = Environment()
    executor.parse_text(bq_v3)
    result_v3 = refresh_stale_assets(executor)
    assert result_v3.refreshed_count > 0, "Expected BQ refresh due to type mismatch"
