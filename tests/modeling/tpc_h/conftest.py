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
    """Generate sf=N parquet dataset via raw duckdb. tpch's dbgen creates the
    standard 8 tables (customer, lineitem, nation, orders, part, partsupp,
    region, supplier); EXPORT DATABASE produces a schema.sql / load.sql pair
    that IMPORT DATABASE replays."""
    import duckdb

    if (import_path / "customer.parquet").exists():
        return
    import_path.mkdir(parents=True, exist_ok=True)
    for stale in ("schema.sql", "load.sql"):
        (import_path / stale).unlink(missing_ok=True)
    con = duckdb.connect(":memory:")
    con.execute(f"""
    INSTALL tpch;
    LOAD tpch;
    CALL dbgen(sf={sf});
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
    # tpch PRAGMA requires the extension to be loaded in this connection too.
    engine.execute_raw_sql("INSTALL tpch; LOAD tpch;")
    engine.execute_raw_sql("SET enable_progress_bar=false;")
    return engine


@pytest.fixture(scope="session")
def engine():
    yield _make_engine(sf=0.1, subdir="memory")


@pytest.fixture(scope="session")
def engine_sf001():
    """sf=0.01 dataset for tests where the sf=0.1 reference is slow."""
    yield _make_engine(sf=0.01, subdir="memory_sf001")


@pytest.fixture(autouse=True, scope="session")
def _emit_reports():
    yield
    # teardown - skip on CI (no display/tkinter available)
    if not os.environ.get("CI"):
        from tests.modeling.tpc_h.analyze_test_results import analyze

        analyze()
