from trilogy import Dialects, Environment, Executor
from trilogy.dialect.config import DuckDBConfig
import pytest
from trilogy.hooks.query_debugger import DebuggingHook
from pathlib import Path
from logging import INFO
from tests.modeling.tpc_ds_duckdb.analyze_test_results import analyze

working_path = Path(__file__).parent


@pytest.fixture(scope="session")
def engine():
    env = Environment(working_path=working_path)
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=env,
        hooks=[DebuggingHook(level=INFO, process_other=False, process_ctes=False)],
        conf=DuckDBConfig(),
    )
    memory = working_path / "memory" / "schema.sql"
    if Path(memory).exists():
        # TODO: Detect if loaded
        engine.execute_raw_sql(f"IMPORT DATABASE '{working_path / "memory"}';")
    results = engine.execute_raw_sql("SHOW TABLES;").fetchall()
    tables = [r[0] for r in results]
    if "store_sales" not in tables:
        engine.execute_raw_sql(
            f"""
        INSTALL tpcds;
        LOAD tpcds;
        SELECT * FROM dsdgen(sf=1);
        EXPORT DATABASE '{working_path / "memory"}' (FORMAT PARQUET);"""
        )
    yield engine


@pytest.fixture(autouse=True, scope="session")
def my_fixture():
    # setup_stuff
    yield
    # teardown_stuff
    analyze()
