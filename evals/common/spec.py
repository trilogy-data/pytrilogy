"""Benchmark-specific knobs that parameterise the shared agent-eval pipeline.

Everything that differs between TPC-DS, TPC-H, and any future benchmark lives
here. Adding a new benchmark = a new ``BenchmarkSpec`` instance and a
``run_eval.py`` shim that calls ``common.main.run(spec)``.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class BenchmarkSpec:
    name: str
    """Human-readable label used in headers and prints ('TPC-DS', 'TPC-H')."""

    short_name: str
    """Stable slug used for cache filenames ('tpcds', 'tpch')."""

    duckdb_extension: str
    """Name of the DuckDB extension to INSTALL + LOAD ('tpcds', 'tpch')."""

    generator_sql: str
    """SQL to populate the freshly-created database. ``{sf}`` is interpolated.
    Examples: ``SELECT * FROM dsdgen(sf={sf})`` (tpcds is a lazy table fn),
    ``CALL dbgen(sf={sf})`` (tpch is a procedural call)."""

    db_filename: str
    """Workspace database filename agents see ('tpcds.duckdb', 'tpch.duckdb')."""

    eval_dir: Path
    """The benchmark's own directory — where prompts/results/charts live."""

    prompts_file: Path
    """Path to ``query_prompts.json`` for this benchmark."""

    enriched_skip_prefixes: tuple[str, ...] = ("query", "adhoc")
    """File-name prefixes to skip when seeding raw/ from an enriched model dir.
    TPC-H adds ``cache`` because its test dir has cache_warm helpers."""

    default_enriched_dir: Path | None = None
    """Default enriched-model dir for ``--both-modes`` to use on the enriched
    leg. Lets ``run_eval.py --both-modes`` work without an explicit
    ``--enriched-model-dir`` argument."""

    references_dir: Path | None = None
    """Directory of ``query<NN>.sql`` reference SQL files. When present, the
    scorer prefers these over the built-in ``PRAGMA <extension>(<NN>)`` for
    queries where a file exists — used to override spec-default filter values
    that yield empty results at our scale factor (q32, q41, q44 on TPC-DS
    SF=0.1, where the canonical manufact_id / store_id / profile filters
    don't match any rows). Falls back to PRAGMA when no file is found."""

    schema_md_file: Path | None = None
    """Optional curated schema-markdown doc for the ``sql_schema`` no-Trilogy
    baseline. When set and the file exists, it is copied into the workspace as
    ``schema.md`` instead of auto-generating one from DuckDB introspection."""

    default_scale_factor: float = 0.01
    default_num_queries: int = 22

    @property
    def cache_dir(self) -> Path:
        return self.eval_dir / ".cache"

    @property
    def results_dir(self) -> Path:
        return self.eval_dir / "results"

    @property
    def charts_dir(self) -> Path:
        return self.eval_dir / "charts"

    @property
    def cache_filename(self) -> str:
        return f"{self.short_name}_sf{{sf}}.duckdb"
