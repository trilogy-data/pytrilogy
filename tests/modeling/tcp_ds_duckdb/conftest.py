from trilogy import Dialects, Environment, Executor
from trilogy.dialect.config import DuckDBConfig
import pytest
from trilogy.hooks.query_debugger import DebuggingHook
from pathlib import Path
from logging import INFO

working_path = Path(__file__).parent


@pytest.fixture(scope="session")
def engine():
    env = Environment(working_path=working_path)
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=env,
        hooks=[DebuggingHook(level=INFO, process_other=False, process_ctes=False)],
        conf=DuckDBConfig(),
    )

    # TODO: Detect if loaded
    engine.execute_raw_sql(
        """
    INSTALL tpcds;
    LOAD tpcds;
    SELECT * FROM dsdgen(sf=1);"""
    )

    yield engine
