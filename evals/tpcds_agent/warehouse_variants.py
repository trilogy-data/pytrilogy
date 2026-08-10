"""Messy-warehouse variants for the TPC-DS agent evaluation."""

from __future__ import annotations

import time
from pathlib import Path

import duckdb

from common import agent_runner, schema_md
from common.spec import BenchmarkSpec

ASSET_DIR = Path(__file__).resolve().parent / "warehouse"
AGGREGATE_MODEL_FILES = ("store_sales.preql", "catalog_sales.preql", "web_sales.preql")


def _run_sql(db_path: Path, filename: str) -> tuple[float, int]:
    start = time.perf_counter()
    with duckdb.connect(str(db_path)) as connection:
        connection.execute((ASSET_DIR / filename).read_text(encoding="utf-8"))
        table_count = connection.execute(
            "select count(*) from information_schema.tables "
            "where table_schema = 'main' and table_type = 'BASE TABLE'"
        ).fetchone()[0]
        connection.execute("CHECKPOINT")
    return time.perf_counter() - start, table_count


def _append_aggregate_models(workspace: Path) -> None:
    for filename in AGGREGATE_MODEL_FILES:
        snippet = (ASSET_DIR / "aggregate_models" / filename).read_text(
            encoding="utf-8"
        )
        path = workspace / "raw" / filename
        path.write_text(
            f"{path.read_text(encoding='utf-8').rstrip()}\n\n{snippet.strip()}\n",
            encoding="utf-8",
        )


def _sql_schema_setup(
    workspace: Path, spec: BenchmarkSpec, db_path: Path, variant_sql: str
) -> dict:
    duration, table_count = _run_sql(db_path, variant_sql)
    start = time.perf_counter()
    dest = schema_md.write_schema_md(
        db_path, spec.name, workspace / "schema.md", spec.schema_md_file
    )
    duration += time.perf_counter() - start
    return {
        "exit_code": 0,
        "duration": duration,
        "stdout": f"wrote {dest.name}; augmented database has {table_count} tables.\n",
        "stderr": "",
    }


def _enriched_setup(
    workspace: Path,
    spec: BenchmarkSpec,
    db_path: Path,
    enriched_dir: Path | None,
    variant_sql: str,
    include_aggregate_models: bool,
) -> dict:
    duration, table_count = _run_sql(db_path, variant_sql)
    source = enriched_dir or spec.default_enriched_dir
    if source is None:
        raise ValueError("enriched warehouse variants need an enriched model directory")
    result = agent_runner.install_enriched_model(
        workspace, Path(source), spec.enriched_skip_prefixes
    )
    if include_aggregate_models:
        _append_aggregate_models(workspace)
    return {
        **result,
        "duration": duration + result["duration"],
        "stdout": (
            f"augmented database has {table_count} tables.\n{result['stdout']}"
        ),
    }


def setup_sql_schema_aggregates(
    workspace: Path, spec: BenchmarkSpec, *, db_path: Path, enriched_dir: Path | None
) -> dict:
    return _sql_schema_setup(workspace, spec, Path(db_path), "aggregates.sql")


def setup_enriched_aggregates(
    workspace: Path, spec: BenchmarkSpec, *, db_path: Path, enriched_dir: Path | None
) -> dict:
    return _enriched_setup(
        workspace,
        spec,
        Path(db_path),
        enriched_dir,
        "aggregates.sql",
        include_aggregate_models=True,
    )


def setup_sql_schema_noise(
    workspace: Path, spec: BenchmarkSpec, *, db_path: Path, enriched_dir: Path | None
) -> dict:
    return _sql_schema_setup(workspace, spec, Path(db_path), "noise.sql")


def setup_enriched_noise(
    workspace: Path, spec: BenchmarkSpec, *, db_path: Path, enriched_dir: Path | None
) -> dict:
    return _enriched_setup(
        workspace,
        spec,
        Path(db_path),
        enriched_dir,
        "noise.sql",
        include_aggregate_models=False,
    )
