import os
from logging import INFO
from pathlib import Path

import pytest

from trilogy import Dialects, Executor
from trilogy.constants import CONFIG, ParserVersion
from trilogy.core.models.environment import Environment
from trilogy.dialect.config import DuckDBConfig
from trilogy.hooks.query_debugger import DebuggingHook

working_path = Path(__file__).parent


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--parser-v2",
        action="store_true",
        default=False,
        help="Use v2 parser instead of v1",
    )


def pytest_configure(config: pytest.Config) -> None:
    if config.getoption("--parser-v2", default=False):
        CONFIG.parser_version = ParserVersion.V2


@pytest.fixture(scope="session")
def engine():
    env = Environment(working_path=working_path)
    debugger = DebuggingHook(level=INFO, process_other=False, process_ctes=False)
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=env,
        hooks=[debugger],
        conf=DuckDBConfig(),
    )
    memory = working_path / "memory" / "schema.sql"
    import_path = working_path / "memory"
    if Path(memory).exists():
        # TODO: Detect if loaded
        engine.execute_raw_sql(f"IMPORT DATABASE '{import_path}';")
    results = engine.execute_raw_sql("SHOW TABLES;").fetchall()
    tables = [r[0] for r in results]
    if "store_sales" not in tables:
        engine.execute_raw_sql(
            f"""
        INSTALL tpcds;
        LOAD tpcds;
        SELECT * FROM dsdgen(sf=1);
        EXPORT DATABASE '{import_path}' (FORMAT PARQUET);"""
        )
    yield engine
    # debugger.write()


@pytest.fixture(autouse=True, scope="session")
def my_fixture():
    # setup_stuff
    yield
    # teardown_stuff - skip on CI (no display/tkinter available)
    if not os.environ.get("CI"):
        from tests.modeling.tpc_ds_duckdb.analyze_test_results import analyze

        analyze()
