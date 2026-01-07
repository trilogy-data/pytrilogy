from logging import INFO

from tests.conftest import load_secret
from trilogy import Dialects, Executor
from trilogy.core.enums import DatasourceState
from trilogy.hooks import DebuggingHook


def test_partition_persistence(executor: Executor):
    years = executor.execute_query(
        "import root; select ride_year order by ride_year asc;"
    ).fetchall()
    base_row = 0
    DebuggingHook(INFO)
    for row in years:
        executor.environment.set_parameters(load_year=row.ride_year)
        results = executor.execute_file("build_daily.preql")

        # count_result = executor.execute_raw_sql(
        #     """select count(*) as cnt from tbl_daily_fact where ride_year = :ride_year;""",
        #     variables={"ride_year": row.ride_year},
        # ).fetchone()
        count_result = executor.execute_raw_sql(
            """select count(*) as cnt from tbl_daily_fact;"""
        ).fetchone()
        assert count_result
        assert count_result.cnt > base_row
        base_row = count_result.cnt
        print(f"Processed year {row.ride_year}, total rows: {count_result.cnt}")

    q1 = executor.generate_sql(
        "select ride_year, ride_month, total_rides order by ride_year asc, ride_month asc;"
    )[0]
    results = executor.execute_raw_sql(q1).fetchall()
    assert "daily_fact" not in q1, q1
    executor.execute_text(" publish datasources daily_fact;")
    assert (
        executor.environment.datasources["daily_fact"].status
        == DatasourceState.PUBLISHED
    )
    q2 = executor.generate_sql(
        "select ride_year, ride_month, total_rides order by ride_year asc, ride_month asc;"
    )[0]
    comp_results = executor.execute_raw_sql(q2).fetchall()
    assert results == comp_results
    assert "daily_fact" in q2, q2


def test_simple_partition_persistence(executor: Executor):
    DebuggingHook()
    executor.environment.set_parameters(load_year=2000)
    executor.execute_file("implicit_build_full.preql")

    results = executor.execute_raw_sql(
        ""
        "select ride_year, count(*) as count from tbl_daily_fact group by ride_year order by ride_year;"
    )
    years = 0
    for row in results.fetchall():

        assert row.count > 0
        years += 1
    assert years == 5


def test_simple_incremental_partition_persistence(executor: Executor):
    DebuggingHook()
    executor.environment.set_parameters(load_year=2021)
    executor.execute_file("implicit_build_partial.preql")

    results = executor.execute_raw_sql(
        ""
        "select ride_year, count(*) as count from tbl_daily_fact group by ride_year order by ride_year;"
    )
    years = 0
    for row in results.fetchall():

        assert row.count > 0
        years += 1
    assert years == 1


def test_duckdb_local_persistence():
    from pathlib import Path

    import pyarrow.parquet as pq

    target = Path("./gcs_export.parquet")
    if target.exists():
        target.unlink()

    exec = Dialects.DUCK_DB.default_executor()

    text = """
auto base <- unnest([1,2,3,4,5]);

datasource gcs_export (
base
)
file `./gcs_export.parquet`
state unpublished;

overwrite gcs_export;
"""
    exec.execute_text(text)

    assert target.exists(), "parquet file was not created"
    table = pq.read_table(target)
    assert table.num_rows == 5
    assert table.column("base").to_pylist() == [1, 2, 3, 4, 5]


def test_duckdb_gcs_persistence():
    from trilogy.dialect.config import DuckDBConfig

    load_secret("GOOGLE_HMAC_KEY")
    load_secret("GOOGLE_HMAC_SECRET")
    config = DuckDBConfig(enable_gcs=True)
    exec = Dialects.DUCK_DB.default_executor(conf=config)

    text = """
auto base <- unnest([1,2,3,4,5]);

datasource gcs_export (
base
)
file `gcs://trilogy_public_models/tests/gcs_export.parquet`
state unpublished;

overwrite gcs_export;
"""
    exec.execute_text(text)


def test_duckdb_gcs_persistence_read_write():
    from trilogy.dialect.config import DuckDBConfig

    load_secret("GOOGLE_HMAC_KEY")
    load_secret("GOOGLE_HMAC_SECRET")
    config = DuckDBConfig(enable_gcs=True)
    exec = Dialects.DUCK_DB.default_executor(conf=config)

    text = """
auto base <- unnest([1,2,3,4,5]);

datasource gcs_export (
base
)
file `https://storage.googleapis.com/trilogy_public_models/tests/gcs_export_rw.parquet`:`gcs://trilogy_public_models/tests/gcs_export_rw.parquet`
state unpublished;

overwrite gcs_export;

select base order by base asc;
"""
    results = exec.execute_text(text)
    assert results[-1].fetchall() == [(1,), (2,), (3,), (4,), (5,)]


def test_duckdb_gcs_refresh():
    """Test refresh command with GCS-backed datasources (mimics sf_trees refresh)."""
    from pathlib import Path

    from trilogy.core.models.environment import Environment
    from trilogy.dialect.config import DuckDBConfig
    from trilogy.execution.state.state_store import refresh_stale_assets

    load_secret("GOOGLE_HMAC_KEY")
    load_secret("GOOGLE_HMAC_SECRET")

    # Path to the sf_trees model
    sf_trees_path = Path(
        r"C:\Users\ethan\coding_projects\trilogy-public-models\trilogy_public_models\duckdb\sf_trees"
    )
    tree_info_file = sf_trees_path / "raw" / "tree_info.preql"

    if not tree_info_file.exists():
        import pytest

        pytest.skip("sf_trees model not available")

    # Create executor with GCS enabled and python datasources
    config = DuckDBConfig(enable_gcs=True, enable_python_datasources=True)
    environment = Environment(working_path=str(sf_trees_path / "raw"))
    exec = Dialects.DUCK_DB.default_executor(environment=environment, conf=config)

    # Execute startup SQL if needed
    setup_sql = sf_trees_path / "setup" / "setup_dev.sql"
    if setup_sql.exists():
        exec.execute_file(setup_sql)

    # Parse the tree_info file
    with open(tree_info_file, "r") as f:
        exec.parse_text(f.read())

    # Debug: print datasources
    print(f"Datasources: {list(exec.environment.datasources.keys())}")
    for ds_name, ds in exec.environment.datasources.items():
        print(f"  {ds_name}: address={ds.address}, is_root={ds.is_root}")

    # Run refresh
    def on_watermarks(wm):
        print("Watermarks:")
        for ds_id, watermark in sorted(wm.items()):
            for key_name, update_key in watermark.keys.items():
                print(
                    f"  {ds_id}.{key_name}: {update_key.value} ({update_key.type.value})"
                )

    def on_stale_found(count, root, all_):
        print(f"Found {count} stale assets (root: {root}, all: {all_})")

    def on_refresh(asset_id, reason):
        print(f"Refreshing {asset_id}: {reason}")

    result = refresh_stale_assets(
        exec,
        on_stale_found=on_stale_found,
        on_refresh=on_refresh,
        on_watermarks=on_watermarks,
    )
    print(f"Refreshed {result.refreshed_count} assets")
