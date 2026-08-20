import os
from logging import INFO
from pathlib import Path

import pytest

from tests.modeling.thelook_duckdb.db_build import seed
from trilogy import Dialects, Executor
from trilogy.core.models.environment import Environment
from trilogy.dialect.config import DuckDBConfig
from trilogy.hooks.query_debugger import DebuggingHook

working_path = Path(__file__).parent


@pytest.fixture(scope="session")
def engine():
    env = Environment(working_path=working_path)
    debugger = DebuggingHook(level=INFO, process_other=False, process_ctes=False)
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=env,
        hooks=[debugger],
        conf=DuckDBConfig(),
    )
    seed(engine)
    engine.execute_raw_sql("SET enable_progress_bar=false;")
    engine.connection.commit()
    yield engine


@pytest.fixture(autouse=True, scope="session")
def _emit_reports():
    yield
    # teardown - skip on CI (no display/tkinter available)
    if not os.environ.get("CI"):
        from tests.modeling.thelook_duckdb.analyze_test_results import analyze

        analyze()
