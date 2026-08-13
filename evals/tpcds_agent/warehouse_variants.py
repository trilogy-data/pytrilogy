"""Messy-warehouse variants for the TPC-DS agent evaluation."""

from __future__ import annotations

import re
import time
from collections.abc import Callable
from functools import partial
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
NOISE_TABLES = (
    "dim_hr_department",
    "dim_hr_employee",
    "fact_payroll_entry",
    "fact_support_ticket",
    "fact_support_ticket_event",
    "dim_marketing_campaign",
    "fact_marketing_touch",
    "dim_supplier_contract",
    "dim_fleet_vehicle",
    "fact_fleet_maintenance",
    "dim_project_milestone",
    "fact_application_audit_event",
)
# Noise-dose replicas read as regional subsidiary systems: each suffix stamps
# a full copy of the 12 base noise tables. 24 regions x 2 generations = up to
# x49 the base set (588 noise tables), past the predicted raw-token
# SQL/enriched crossover (~440 clean tables at 20260811-145002 overheads).
_NOISE_REGIONS = (
    "emea",
    "apac",
    "latam",
    "noram",
    "uk",
    "de",
    "fr",
    "jp",
    "au",
    "ca",
    "mx",
    "br",
    "in",
    "cn",
    "sg",
    "za",
    "ae",
    "se",
    "pl",
    "it",
    "es",
    "nl",
    "ch",
    "kr",
)
NOISE_REPLICA_SUFFIXES = _NOISE_REGIONS + tuple(
    f"{region}_legacy" for region in _NOISE_REGIONS
)
# Multipliers over the 12-table base noise set exposed as eval categories.
# Full-schema sizes: x4 ~43k chars (realistic messy warehouse), x12 ~71k,
# x24 ~115k (predicted cache-adjusted crossover), x48 ~202k (past the
# predicted raw-token crossover) — the sweep brackets both.
NOISE_DOSES = (4, 12, 24, 48)


# --- confusable noise (wave 3) -----------------------------------------------
# Plausible in-domain traps derived from the REAL tables, so table choice can
# no longer be settled by reading schema.md: near-duplicate row samples are
# quietly wrong for every aggregate, daily rollups masquerade at line-grain
# names, stale dim copies miss rows. The real tables stay present and complete
# (reference results hold). Traps carry the COMMENTS of the table they were
# derived from: a deprecated copy was curated when it was made, so bare-column
# traps would leak "this one is fake" through the documentation gap (the
# 20260812-153111 run had that leak — its 0-trap-picks result conflates the
# name prior with the doc asymmetry). Grain traps take the LINE fact's
# comments to match their masquerading column names — the docs lie exactly as
# hard as the names do. Levels are cumulative: x1 = 7 tables, x2 = 20, x3 = 32.
CONFUSABLE_FACT_SAMPLES: dict[int, tuple[tuple[str, int], ...]] = {
    1: (("v2", 88),),
    2: (("bak", 93),),
    3: (("staging", 80),),
}
CONFUSABLE_DIM_SNAPSHOTS = (
    "dim_customer",
    "dim_customer_address",
    "dim_item",
    "dim_store",
    "dim_promotion",
)
# Channel prefix for de-aggregating fact_agg_<name>_daily measure columns
# (sum_quantity -> ss_quantity) so the grain trap reads as a line-grain fact.
_DAILY_TRAP_PREFIXES = {
    "store_sales": "ss",
    "store_returns": "sr",
    "catalog_sales": "cs",
    "catalog_returns": "cr",
    "web_sales": "ws",
    "web_returns": "wr",
}


def _copy_comments(
    connection: duckdb.DuckDBPyConnection, source: str, target: str
) -> None:
    """Stamp ``target`` with ``source``'s table comment and every column
    comment whose column name also exists on ``target``. CTAS drops comments,
    which would leave traps visibly undocumented next to curated real tables."""

    def quote(text: str) -> str:
        return "'" + text.replace("'", "''") + "'"

    table_comment = connection.execute(
        "select comment from duckdb_tables() where table_name = ?", [source]
    ).fetchone()
    if table_comment and table_comment[0]:
        connection.execute(f'COMMENT ON TABLE "{target}" IS {quote(table_comment[0])}')
    target_cols = {
        row[0]
        for row in connection.execute(
            "select column_name from duckdb_columns() where table_name = ?",
            [target],
        ).fetchall()
    }
    for column, comment in connection.execute(
        "select column_name, comment from duckdb_columns() "
        "where table_name = ? and comment is not null",
        [source],
    ).fetchall():
        if column in target_cols:
            connection.execute(
                f'COMMENT ON COLUMN "{target}"."{column}" IS {quote(comment)}'
            )


def _confusable_ddl(connection: duckdb.DuckDBPyConnection, level: int) -> int:
    """Create trap tables for the cumulative dose ``level``; returns count."""
    created = 0
    facts = [f"fact_{name}" for name in FACT_TABLES]
    for lvl, samples in CONFUSABLE_FACT_SAMPLES.items():
        if lvl > level:
            continue
        for suffix, percent in samples:
            for seed_offset, fact in enumerate(facts):
                connection.execute(
                    f'CREATE OR REPLACE TABLE "{fact}_{suffix}" AS '
                    f'SELECT * FROM "{fact}" '
                    f"USING SAMPLE {percent} PERCENT (bernoulli, {lvl * 100 + seed_offset})"
                )
                _copy_comments(connection, fact, f"{fact}_{suffix}")
                created += 1
    if level >= 2:
        for name, prefix in _DAILY_TRAP_PREFIXES.items():
            cols = [
                row[0]
                for row in connection.execute(
                    "select column_name from duckdb_columns() "
                    "where table_name = ? order by column_index",
                    [f"fact_agg_{name}_daily"],
                ).fetchall()
            ]
            selects = []
            for col in cols:
                if col.startswith("count_"):
                    continue
                if col.startswith("sum_"):
                    selects.append(f'"{col}" AS "{prefix}_{col[4:]}"')
                else:
                    selects.append(f'"{col}"')
            connection.execute(
                f'CREATE OR REPLACE TABLE "fact_{name}_daily" AS '
                f'SELECT {", ".join(selects)} FROM "fact_agg_{name}_daily"'
            )
            # The line fact, not the aggregate: renamed measure columns share
            # the line fact's names, so its docs complete the masquerade —
            # aggregate-sourced docs would announce the daily grain and defuse
            # the trap.
            _copy_comments(connection, f"fact_{name}", f"fact_{name}_daily")
            created += 1
    if level >= 3:
        for seed_offset, dim in enumerate(CONFUSABLE_DIM_SNAPSHOTS):
            connection.execute(
                f'CREATE OR REPLACE TABLE "{dim}_snapshot" AS '
                f'SELECT * FROM "{dim}" '
                f"USING SAMPLE 90 PERCENT (bernoulli, {400 + seed_offset})"
            )
            _copy_comments(connection, dim, f"{dim}_snapshot")
            created += 1
    return created


def _apply_confusable(db_path: Path, level: int) -> tuple[float, int]:
    start = time.perf_counter()
    with duckdb.connect(str(db_path)) as connection:
        created = _confusable_ddl(connection, level)
        connection.execute("CHECKPOINT")
    return time.perf_counter() - start, created


_SUMMARY_MODEL_ALIASES = {
    "store_sales.preql": ("store_sales", "ss"),
    "catalog_sales.preql": ("catalog_sales", "cs"),
    "web_sales.preql": ("web_sales", "ws"),
}
_AUTO_LINE = re.compile(r"^auto _warehouse_(\w+) <- (\w+)\(([^)]+)\);")
_COLUMN_LINE = re.compile(r"^(\s+)(\??)(\w+): (\??)([\w.]+),?\s*$")


def daily_summary_model(snippet_file: str) -> str:
    """A public, own-file semantic model for the ``fact_*_daily`` trap tables,
    derived from the private aggregate-binding snippet: concepts renamed
    ``_warehouse_*`` -> ``daily_*``, base concepts qualified through a channel
    import, count columns dropped (the trap tables omit them), and addresses
    retargeted from ``fact_agg_X_daily`` to ``fact_X_daily``. This is the
    'plausible curated layer' arm: the modeler documents the genuinely useful
    summaries; backup/staging junk stays unmodeled."""
    import_name, alias = _SUMMARY_MODEL_ALIASES[snippet_file]
    snippet = (ASSET_DIR / "aggregate_models" / snippet_file).read_text(
        encoding="utf-8"
    )
    out = [
        "# Daily pre-aggregated summary tables maintained by the warehouse team.",
        "# DAILY GRAIN: one row per key combination per day — no ticket/order",
        f"# line detail. Use the base {import_name} model for line-item analysis;",
        "# these summaries answer daily/period totals more cheaply.",
        f"import {import_name} as {alias};",
        "",
    ]
    block_prefix = ""
    for raw_line in snippet.splitlines():
        line = raw_line.rstrip()
        auto = _AUTO_LINE.match(line)
        if auto:
            name, func, arg = auto.groups()
            if name.startswith("count_"):
                continue
            out.append(f"auto daily_{name} <- {func}({alias}.{arg.strip()});")
            continue
        if line.startswith("# Private materializations"):
            continue
        if line.startswith("datasource _warehouse_"):
            block = line.removeprefix("datasource _warehouse_").split(" ")[0]
            block_prefix = _DAILY_TRAP_PREFIXES[block.removesuffix("_daily")]
            out.append(line.replace("datasource _warehouse_", "datasource daily_"))
            continue
        if line.startswith("address fact_agg_"):
            out.append(line.replace("address fact_agg_", "address fact_"))
            continue
        column = _COLUMN_LINE.match(line)
        if column:
            indent, _, key, opt, value = column.groups()
            if key.startswith("count_") or value == "line_item_count":
                continue
            if value.startswith("_warehouse_"):
                target = value.replace("_warehouse_", "daily_", 1)
                opt = ""
            else:
                target = f"{alias}.{value}"
            if key.startswith("sum_"):
                key = f"{block_prefix}_{key[4:]}"
            out.append(f"{indent}{key}: {opt}{target},")
            continue
        stripped = line.strip().rstrip(",")
        if (
            stripped
            and re.fullmatch(r"[\w.]+", stripped)
            and not stripped.startswith("daily_")
            and line.startswith("    ")
        ):
            # grain(...) entries — base concepts, qualify through the import
            suffix = "," if line.rstrip().endswith(",") else ""
            out.append(f"    {alias}.{stripped}{suffix}")
            continue
        out.append(line)
    return "\n".join(out) + "\n"


# --- column-rename (pretraining-recall discriminator) -----------------------
# TPC-DS column names (ss_sold_date_sk, d_moy, ...) are a pretraining surface
# even after table renames. The colrename cell strips the canonical
# abbreviation prefixes into honest warehouse-style names so verbatim recall
# breaks while semantics stay intact; the A/B against sql_schema_aggregates
# measures the identifier-mapping subsidy. Reference SQL is untouched — it
# runs against the separate unrenamed reference database.

# Longest-match-first: cc_/cp_/ca_/cd_/cs_/cr_ before c_, sm_/sr_/ss_ before
# s_, inv_/ib_ before i_, web_/wp_/wr_/ws_ before w_.
COLUMN_PREFIXES = (
    "ss_",
    "sr_",
    "cs_",
    "cr_",
    "ws_",
    "wr_",
    "inv_",
    "cc_",
    "cp_",
    "ca_",
    "cd_",
    "c_",
    "d_",
    "hd_",
    "ib_",
    "i_",
    "p_",
    "r_",
    "sm_",
    "s_",
    "t_",
    "web_",
    "wp_",
    "w_",
)
# Stems that would come out as bare SQL keywords or cryptic abbreviations.
COLUMN_STEM_OVERRIDES = {
    "date": "calendar_date",
    "year": "calendar_year",
    "time": "seconds_of_day",
    "hour": "hour_of_day",
    "minute": "minute_of_hour",
    "second": "second_of_minute",
    "month": "month_name",
    "day": "day_name",
    "quarter": "quarter_name",
    "moy": "month_of_year",
    "qoy": "quarter_of_year",
    "dom": "day_of_month",
    "dow": "day_of_week",
}
# Keyword-stem columns get entity-qualified names (what a real warehouse does).
COLUMN_NAME_OVERRIDES = {
    "cc_name": "call_center_name",
    "cc_class": "call_center_class",
    "cc_county": "call_center_county",
    "cc_state": "call_center_state",
    "cc_country": "call_center_country",
    "cp_type": "catalog_page_type",
    "ca_county": "address_county",
    "ca_state": "address_state",
    "ca_country": "address_country",
    "i_class": "item_class",
    "sm_type": "ship_mode_type",
    "s_county": "store_county",
    "s_state": "store_state",
    "s_country": "store_country",
    "w_county": "warehouse_county",
    "w_state": "warehouse_state",
    "w_country": "warehouse_country",
    "wp_type": "web_page_type",
    "web_name": "site_name",
    "web_class": "site_class",
    "web_county": "site_county",
    "web_state": "site_state",
    "web_country": "site_country",
}


def renamed_column(column: str) -> str | None:
    """New name for a canonical TPC-DS column, or None if it has no known
    prefix (noise tables, agg-derived sum_*/count_* columns)."""
    if column in COLUMN_NAME_OVERRIDES:
        return COLUMN_NAME_OVERRIDES[column]
    for prefix in COLUMN_PREFIXES:
        if column.startswith(prefix):
            stem = column[len(prefix) :]
            stem = COLUMN_STEM_OVERRIDES.get(stem, stem)
            if stem.endswith("_sk"):
                stem = stem[:-3] + "_key"
            return stem
    return None


def _rename_benchmark_columns(db_path: Path) -> tuple[float, int]:
    """Apply the mechanical column-rename map to every base table (the agg
    tables' prefixed FK columns share names with the facts, so one global map
    keeps joins consistent), then rewrite COMMENT text that mentions old
    names. Returns (duration, columns renamed)."""
    start = time.perf_counter()
    renames = 0
    with duckdb.connect(str(db_path)) as connection:
        columns = connection.execute(
            "select table_name, column_name, comment from duckdb_columns() "
            "where schema_name = 'main'"
        ).fetchall()
        mapping = {
            (table, column): renamed_column(column)
            for table, column, _ in columns
            if renamed_column(column) is not None
        }
        for (table, column), target in mapping.items():
            connection.execute(
                f'ALTER TABLE "{table}" RENAME COLUMN "{column}" TO "{target}"'
            )
            renames += 1
        # Comments (comments.sql text) reference old names — keep docs honest.
        old_to_new = sorted(
            {(old, new) for (_, old), new in mapping.items()},
            key=lambda pair: -len(pair[0]),
        )

        def fix_text(text: str) -> str:
            for old, new in old_to_new:
                text = re.sub(rf"\b{old}\b", new, text)
            return text

        for table, comment in connection.execute(
            "select table_name, comment from duckdb_tables() "
            "where comment is not null"
        ).fetchall():
            fixed = fix_text(comment)
            if fixed != comment:
                connection.execute(
                    f"COMMENT ON TABLE \"{table}\" IS '{fixed.replace(chr(39), chr(39) * 2)}'"
                )
        for table, column, comment in columns:
            if not comment:
                continue
            target = mapping.get((table, column), column)
            fixed = fix_text(comment)
            connection.execute(
                f'COMMENT ON COLUMN "{table}"."{target}" IS '
                f"'{fixed.replace(chr(39), chr(39) * 2)}'"
            )
        connection.execute("CHECKPOINT")
    return time.perf_counter() - start, renames


def setup_sql_schema_colrename(
    workspace: Path, spec: BenchmarkSpec, *, db_path: Path, enriched_dir: Path | None
) -> dict:
    """sql_schema_aggregates with warehouse-style column names — the
    pretraining-recall discriminator cell."""
    duration, table_count = _prepare_database(Path(db_path), include_noise=False)
    rename_duration, renames = _rename_benchmark_columns(Path(db_path))
    duration += rename_duration
    start = time.perf_counter()
    dest = schema_md.write_schema_md(
        Path(db_path), workspace / "schema.md", spec.schema_md_file
    )
    duration += time.perf_counter() - start
    return {
        "exit_code": 0,
        "duration": duration,
        "stdout": (
            f"wrote {dest.name}; {table_count} tables, " f"{renames} columns renamed.\n"
        ),
        "stderr": "",
        "separate_reference_database": True,
    }


def noise_replica_sql(suffix: str) -> str:
    """The base noise DDL re-stamped for one subsidiary: every table name gets
    ``_<suffix>`` and every COMMENT a region tag. Longest-first replacement so
    ``fact_support_ticket`` never clips ``fact_support_ticket_event``."""
    text = (ASSET_DIR / "noise.sql").read_text(encoding="utf-8")
    for name in sorted(NOISE_TABLES, key=len, reverse=True):
        text = re.sub(rf"\b{name}\b", f"{name}_{suffix}", text)
    return text.replace("IS '", f"IS '[{suffix.upper()}] ")


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


def _run_sql_text(db_path: Path, sql: str) -> tuple[float, int]:
    start = time.perf_counter()
    with duckdb.connect(str(db_path)) as connection:
        connection.execute(sql)
        table_count_row = connection.execute(
            "select count(*) from information_schema.tables "
            "where table_schema = 'main' and table_type = 'BASE TABLE'"
        ).fetchone()
        connection.execute("CHECKPOINT")
    if table_count_row is None:
        raise RuntimeError("DuckDB did not return a table count")
    return time.perf_counter() - start, table_count_row[0]


def _run_sql(db_path: Path, filename: str) -> tuple[float, int]:
    return _run_sql_text(db_path, (ASSET_DIR / filename).read_text(encoding="utf-8"))


def _prepare_database(
    db_path: Path, *, include_noise: bool, noise_multiplier: int = 1
) -> tuple[float, int]:
    duration = _rename_benchmark_tables(db_path)
    aggregate_duration, table_count = _run_sql(db_path, "aggregates.sql")
    duration += aggregate_duration
    # Column/table descriptions matching the curated model's enrichment, so the
    # sql_schema legs see equivalent documentation via schema.md (noise-table
    # comments live in noise.sql, since those tables only exist with noise on).
    comment_duration, table_count = _run_sql(db_path, "comments.sql")
    duration += comment_duration
    if include_noise:
        noise_duration, table_count = _run_sql(db_path, "noise.sql")
        duration += noise_duration
        replicas = NOISE_REPLICA_SUFFIXES[: max(noise_multiplier - 1, 0)]
        if replicas:
            replica_sql = "\n".join(noise_replica_sql(suffix) for suffix in replicas)
            replica_duration, table_count = _run_sql_text(db_path, replica_sql)
            duration += replica_duration
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
    workspace: Path,
    spec: BenchmarkSpec,
    db_path: Path,
    *,
    include_noise: bool,
    noise_multiplier: int = 1,
) -> dict:
    duration, table_count = _prepare_database(
        db_path, include_noise=include_noise, noise_multiplier=noise_multiplier
    )
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
    workspace: Path,
    spec: BenchmarkSpec,
    *,
    db_path: Path,
    enriched_dir: Path | None,
    noise_multiplier: int = 1,
) -> dict:
    return _sql_schema_setup(
        workspace,
        spec,
        Path(db_path),
        include_noise=True,
        noise_multiplier=noise_multiplier,
    )


def sql_noise_dose_setup(multiplier: int) -> Callable[..., dict]:
    """Setup callable for a ``sql_schema_noise_x<multiplier>`` category."""
    return partial(setup_sql_schema_noise, noise_multiplier=multiplier)


def setup_sql_bare_noise(
    workspace: Path,
    spec: BenchmarkSpec,
    *,
    db_path: Path,
    enriched_dir: Path | None,
    noise_multiplier: int = 1,
) -> dict:
    """Discovery-regime cell: same noisy database, NO schema.md. The agent
    must find the real tables via run_query, so token cost scales with its
    own probing behavior instead of a whole-schema injection — the empirical
    test of whether incremental discovery compounds (each probe result is
    rebilled on every later turn) or stays sub-linear (selective reading)."""
    duration, table_count = _prepare_database(
        Path(db_path), include_noise=True, noise_multiplier=noise_multiplier
    )
    return {
        "exit_code": 0,
        "duration": duration,
        "stdout": (
            f"sql_bare_noise: augmented database has {table_count} tables; "
            "no schema.md written.\n"
        ),
        "stderr": "",
        "separate_reference_database": True,
    }


def bare_noise_dose_setup(multiplier: int) -> Callable[..., dict]:
    """Setup callable for a ``sql_bare_noise_x<multiplier>`` category."""
    return partial(setup_sql_bare_noise, noise_multiplier=multiplier)


def noise_dose_output_limit(multiplier: int) -> int:
    """Agent tool-output cap sized so read_file('schema.md') never truncates:
    ~29k chars of base schema + ~3.6k per noise replica, plus slack."""
    return max(32_768, 40_000 + multiplier * 3_600)


def setup_sql_schema_confusable(
    workspace: Path,
    spec: BenchmarkSpec,
    *,
    db_path: Path,
    enriched_dir: Path | None,
    confusable_level: int = 1,
) -> dict:
    duration, table_count = _prepare_database(Path(db_path), include_noise=False)
    conf_duration, created = _apply_confusable(Path(db_path), confusable_level)
    duration += conf_duration
    start = time.perf_counter()
    dest = schema_md.write_schema_md(
        Path(db_path), workspace / "schema.md", spec.schema_md_file
    )
    duration += time.perf_counter() - start
    return {
        "exit_code": 0,
        "duration": duration,
        "stdout": (
            f"wrote {dest.name}; {table_count + created} tables "
            f"({created} confusable traps at level {confusable_level}).\n"
        ),
        "stderr": "",
        "separate_reference_database": True,
    }


def sql_confusable_dose_setup(level: int) -> Callable[..., dict]:
    return partial(setup_sql_schema_confusable, confusable_level=level)


def setup_sql_bare_confusable(
    workspace: Path,
    spec: BenchmarkSpec,
    *,
    db_path: Path,
    enriched_dir: Path | None,
    confusable_level: int = 1,
) -> dict:
    """Discovery-regime arm of the confusable treatment: same trap tables, NO
    schema.md. The bare agent's keyword LIKE filters surface the traps right
    next to the real tables (unlike clean noise, whose vocabulary is
    disjoint), and its usual tools — SHOW TABLES / information_schema /
    DESCRIBE — expose neither the comments nor the grain, so the traps must
    be resolved by the name prior or by probing. Closes the matrix cell wave
    3 skipped."""
    duration, table_count = _prepare_database(Path(db_path), include_noise=False)
    conf_duration, created = _apply_confusable(Path(db_path), confusable_level)
    return {
        "exit_code": 0,
        "duration": duration + conf_duration,
        "stdout": (
            f"sql_bare_confusable: {table_count + created} tables "
            f"({created} traps at level {confusable_level}); no schema.md written.\n"
        ),
        "stderr": "",
        "separate_reference_database": True,
    }


def bare_confusable_dose_setup(level: int) -> Callable[..., dict]:
    return partial(setup_sql_bare_confusable, confusable_level=level)


def setup_enriched_confusable(
    workspace: Path,
    spec: BenchmarkSpec,
    *,
    db_path: Path,
    enriched_dir: Path | None,
    confusable_level: int = 3,
) -> dict:
    """Enriched arm of the confusable treatment: same trap tables physically,
    and the curated layer models the genuinely useful ones (daily summaries)
    as their own clearly-described files — backup/staging junk stays
    unmodeled, as a real curator would leave it."""
    result = _enriched_setup(
        workspace, spec, Path(db_path), enriched_dir, include_noise=False
    )
    conf_duration, created = _apply_confusable(Path(db_path), confusable_level)
    summaries = 0
    if confusable_level >= 2:
        for snippet_file, (import_name, _) in _SUMMARY_MODEL_ALIASES.items():
            (workspace / "raw" / f"{import_name}_daily_summary.preql").write_text(
                daily_summary_model(snippet_file), encoding="utf-8"
            )
            summaries += 1
    return {
        **result,
        "duration": result["duration"] + conf_duration,
        "stdout": (
            f"{result['stdout']}confusable level {confusable_level}: "
            f"{created} trap tables, {summaries} daily-summary models.\n"
        ),
    }


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
