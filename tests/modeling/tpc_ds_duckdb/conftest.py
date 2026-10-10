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


def _make_engine(sf: float, subdir: str, scratch: Path) -> Executor:
    """A DuckDB engine over the sf=N dataset, backed by a file rather than
    loaded into memory.

    `IMPORT DATABASE` into an in-memory database makes every table resident:
    sf=1 is ~2.2GB of RSS for the life of the package, and the reference
    PRAGMAs push it towards 3GB. The same import into a file costs a few
    seconds more once, after which table pages live in the buffer pool and are
    evicted under `memory_limit` instead of pinned -- the package's peak drops
    by ~2GB with identical query timings. The file is named
    `memory` so the catalog keeps the name the models address tables by
    (`address memory.store_sales`), and it is built fresh per session into
    pytest's temp dir so a test that writes a table cannot leak it into the
    next run.

    Spilling stays off: with the data file-backed the limit only governs
    intermediates, and a pathological plan should still error rather than
    thrash the disk."""
    import duckdb

    import_path = working_path / subdir
    _ensure_dataset(import_path, sf)
    db_dir = scratch / subdir
    db_dir.mkdir(parents=True, exist_ok=True)
    db_path = db_dir / "memory.duckdb"
    con = duckdb.connect(str(db_path))
    # Bound the import too: unlimited, it stages the whole dataset in memory
    # before the first checkpoint.
    con.execute("SET memory_limit='1GB';")
    con.execute(f"IMPORT DATABASE '{import_path}';")
    con.close()
    env = Environment(working_path=working_path)
    debugger = DebuggingHook(level=INFO, process_other=False, process_ctes=False)
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=env,
        hooks=[debugger],
        conf=DuckDBConfig(path=str(db_path)),
    )
    engine.execute_raw_sql("SET enable_progress_bar=false;")
    engine.execute_raw_sql("SET memory_limit='2GB';")
    engine.execute_raw_sql("SET temp_directory='';")
    engine.connection.commit()
    return engine


@pytest.fixture(scope="package")
def engine(tmp_path_factory: pytest.TempPathFactory):
    engine = _make_engine(
        sf=1, subdir="memory", scratch=tmp_path_factory.mktemp("tpcds")
    )
    yield engine
    engine.close()


@pytest.fixture(scope="package")
def engine_sf01(tmp_path_factory: pytest.TempPathFactory):
    """sf=0.1 dataset for tests where the sf=1 reference PRAGMA hangs/OOMs."""
    engine = _make_engine(
        sf=0.1, subdir="memory_sf01", scratch=tmp_path_factory.mktemp("tpcds")
    )
    yield engine
    engine.close()


@pytest.fixture(scope="package")
def engine_sf001(tmp_path_factory: pytest.TempPathFactory):
    """sf=0.01 dataset for tests where the reference PRAGMA is slow even at sf=0.1
    (e.g. query 72's non-equi inventory x catalog_sales join)."""
    engine = _make_engine(
        sf=0.01, subdir="memory_sf001", scratch=tmp_path_factory.mktemp("tpcds")
    )
    yield engine
    engine.close()


@pytest.fixture(autouse=True, scope="session")
def my_fixture():
    # setup_stuff
    yield
    # teardown_stuff - skip on CI (no display/tkinter available)
    if not os.environ.get("CI"):
        from tests.modeling.tpc_ds_duckdb.analyze_test_results import analyze

        analyze()
