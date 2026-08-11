"""Messy-warehouse variants for the TPC-DS agent evaluation."""

from __future__ import annotations

import time
from pathlib import Path

import duckdb
from common import agent_runner, schema_md
from common.spec import BenchmarkSpec

ASSET_DIR = Path(__file__).resolve().parent / "warehouse"
DIMENSION_TABLES = (
    "call_center",
    "catalog_page",
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "household_demographics",
    "income_band",
    "item",
    "promotion",
    "reason",
    "ship_mode",
    "store",
    "time_dim",
    "warehouse",
    "web_page",
    "web_site",
)
FACT_TABLES = (
    "catalog_returns",
    "catalog_sales",
    "inventory",
    "store_returns",
    "store_sales",
    "web_returns",
    "web_sales",
)
PHYSICAL_TABLE_RENAMES = {
    **{name: f"dim_{name}" for name in DIMENSION_TABLES},
    **{name: f"fact_{name}" for name in FACT_TABLES},
}
AGGREGATE_MODEL_FILES = (
    "store_sales.preql",
    "catalog_sales.preql",
    "web_sales.preql",
)


def _rename_benchmark_tables(db_path: Path) -> float:
    start = time.perf_counter()
    with duckdb.connect(str(db_path)) as connection:
        existing = {
            row[0]
            for row in connection.execute(
                "select table_name from information_schema.tables "
                "where table_schema = 'main' and table_type = 'BASE TABLE'"
            ).fetchall()
        }
        for source, target in PHYSICAL_TABLE_RENAMES.items():
            if target in existing:
                continue
            if source not in existing:
                raise RuntimeError(f"benchmark table {source!r} is missing")
            connection.execute(f'ALTER TABLE "{source}" RENAME TO "{target}"')
        connection.execute("CHECKPOINT")
    return time.perf_counter() - start


def _run_sql(db_path: Path, filename: str) -> tuple[float, int]:
    start = time.perf_counter()
    with duckdb.connect(str(db_path)) as connection:
        connection.execute((ASSET_DIR / filename).read_text(encoding="utf-8"))
        table_count_row = connection.execute(
            "select count(*) from information_schema.tables "
            "where table_schema = 'main' and table_type = 'BASE TABLE'"
        ).fetchone()
        connection.execute("CHECKPOINT")
    if table_count_row is None:
        raise RuntimeError("DuckDB did not return a table count")
    return time.perf_counter() - start, table_count_row[0]


def _prepare_database(db_path: Path, *, include_noise: bool) -> tuple[float, int]:
    duration = _rename_benchmark_tables(db_path)
    aggregate_duration, table_count = _run_sql(db_path, "aggregates.sql")
    duration += aggregate_duration
    if include_noise:
        noise_duration, table_count = _run_sql(db_path, "noise.sql")
        duration += noise_duration
    return duration, table_count


def _retarget_model_addresses(workspace: Path) -> int:
    rewrites = 0
    for path in sorted((workspace / "raw").glob("*.preql")):
        text = path.read_text(encoding="utf-8")
        for source, target in PHYSICAL_TABLE_RENAMES.items():
            old = f"address {source};"
            count = text.count(old)
            if count:
                text = text.replace(old, f"address {target};")
                rewrites += count
        path.write_text(text, encoding="utf-8")
    return rewrites


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
    workspace: Path, spec: BenchmarkSpec, db_path: Path, *, include_noise: bool
) -> dict:
    duration, table_count = _prepare_database(db_path, include_noise=include_noise)
    start = time.perf_counter()
    dest = schema_md.write_schema_md(
        db_path, workspace / "schema.md", spec.schema_md_file
    )
    duration += time.perf_counter() - start
    return {
        "exit_code": 0,
        "duration": duration,
        "stdout": f"wrote {dest.name}; augmented database has {table_count} tables.\n",
        "stderr": "",
        "separate_reference_database": True,
    }


def _enriched_setup(
    workspace: Path,
    spec: BenchmarkSpec,
    db_path: Path,
    enriched_dir: Path | None,
    *,
    include_noise: bool,
) -> dict:
    duration, table_count = _prepare_database(db_path, include_noise=include_noise)
    source = enriched_dir or spec.default_enriched_dir
    if source is None:
        raise ValueError("enriched warehouse variants need an enriched model directory")
    result = agent_runner.install_enriched_model(
        workspace, Path(source), spec.enriched_skip_prefixes
    )
    address_rewrites = _retarget_model_addresses(workspace)
    _append_aggregate_models(workspace)
    return {
        **result,
        "duration": duration + result["duration"],
        "stdout": (
            f"augmented database has {table_count} tables; retargeted "
            f"{address_rewrites} model addresses.\n{result['stdout']}"
        ),
        "separate_reference_database": True,
    }


def setup_sql_schema_aggregates(
    workspace: Path, spec: BenchmarkSpec, *, db_path: Path, enriched_dir: Path | None
) -> dict:
    return _sql_schema_setup(workspace, spec, Path(db_path), include_noise=False)


def setup_enriched_aggregates(
    workspace: Path, spec: BenchmarkSpec, *, db_path: Path, enriched_dir: Path | None
) -> dict:
    return _enriched_setup(
        workspace,
        spec,
        Path(db_path),
        enriched_dir,
        include_noise=False,
    )


def setup_sql_schema_noise(
    workspace: Path, spec: BenchmarkSpec, *, db_path: Path, enriched_dir: Path | None
) -> dict:
    return _sql_schema_setup(workspace, spec, Path(db_path), include_noise=True)


def setup_enriched_noise(
    workspace: Path, spec: BenchmarkSpec, *, db_path: Path, enriched_dir: Path | None
) -> dict:
    return _enriched_setup(
        workspace,
        spec,
        Path(db_path),
        enriched_dir,
        include_noise=True,
    )
