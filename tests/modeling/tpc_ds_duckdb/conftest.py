import os
from logging import INFO
from pathlib import Path

import pytest

from trilogy import Dialects, Executor
from trilogy.core.models.environment import Environment
from trilogy.dialect.config import DuckDBConfig
from trilogy.hooks.query_debugger import DebuggingHook

working_path = Path(__file__).parent


def _ensure_dataset(import_path: Path, sf: float) -> None:
    """Generate sf=N parquet dataset via raw duckdb (avoids capturing trilogy's
    uv_run macro into the exported schema.sql)."""
    import duckdb

    # schema.sql / load.sql are committed for the smaller scale factors but
    # *.parquet is gitignored, so on a fresh checkout the schema files exist
    # while the data files don't. Gate on a representative parquet to detect
    # a partially-populated directory and regenerate.
    if (import_path / "call_center.parquet").exists():
        return
    import_path.mkdir(parents=True, exist_ok=True)
    for stale in ("schema.sql", "load.sql"):
        (import_path / stale).unlink(missing_ok=True)
    con = duckdb.connect(":memory:")
    con.execute(f"""
    INSTALL tpcds;
    LOAD tpcds;
    SELECT * FROM dsdgen(sf={sf});
    EXPORT DATABASE '{import_path}' (FORMAT PARQUET);""")
    con.close()


def _make_engine(sf: float, subdir: str) -> Executor:
    import_path = working_path / subdir
    _ensure_dataset(import_path, sf)
    env = Environment(working_path=working_path)
    debugger = DebuggingHook(level=INFO, process_other=False, process_ctes=False)
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=env,
        hooks=[debugger],
        conf=DuckDBConfig(),
    )
    engine.execute_raw_sql(f"IMPORT DATABASE '{import_path}';")
    engine.execute_raw_sql("SET enable_progress_bar=false;")
    if not os.environ.get("CI"):
        # Cap memory so a pathological plan errors instead of taking the machine down.
        engine.execute_raw_sql("SET memory_limit='3GB';")
    return engine


@pytest.fixture(scope="session")
def engine():
    yield _make_engine(sf=1, subdir="memory")


@pytest.fixture(scope="session")
def engine_sf01():
    """sf=0.1 dataset for tests where the sf=1 reference PRAGMA hangs/OOMs."""
    yield _make_engine(sf=0.1, subdir="memory_sf01")


@pytest.fixture(scope="session")
def engine_sf001():
    """sf=0.01 dataset for tests where the reference PRAGMA is slow even at sf=0.1
    (e.g. query 72's non-equi inventory x catalog_sales join)."""
    yield _make_engine(sf=0.01, subdir="memory_sf001")


@pytest.fixture(autouse=True, scope="session")
def my_fixture():
    # setup_stuff
    yield
    # teardown_stuff - skip on CI (no display/tkinter available)
    if not os.environ.get("CI"):
        from tests.modeling.tpc_ds_duckdb.analyze_test_results import analyze

        analyze()
