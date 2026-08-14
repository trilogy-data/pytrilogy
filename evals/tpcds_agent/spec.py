"""TPC-DS benchmark spec. The bootstrap (sys.path / import shim) lives in the
per-mode scripts; this module just defines SPEC."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from common.categories import Category
from common.spec import BenchmarkSpec

if TYPE_CHECKING:
    from tpcds_agent import warehouse_variants
elif __package__:
    from . import warehouse_variants
else:
    import warehouse_variants

EVAL_DIR = Path(__file__).resolve().parent

MESSY_WAREHOUSE_CATEGORIES = (
    Category(
        "sql_schema_aggregates",
        "db+schema+aggregates",
        "sql",
        ".sql",
        warehouse_variants.setup_sql_schema_aggregates,
    ),
    Category(
        "enriched_aggregates",
        "enriched+aggregates",
        "trilogy",
        ".preql",
        warehouse_variants.setup_enriched_aggregates,
    ),
    Category(
        "sql_schema_noise",
        "db+schema+aggregates+noise",
        "sql",
        ".sql",
        warehouse_variants.setup_sql_schema_noise,
    ),
    Category(
        "enriched_noise",
        "enriched+aggregates+noise",
        "trilogy",
        ".preql",
        warehouse_variants.setup_enriched_noise,
    ),
    # Pretraining-recall discriminator: sql_schema_aggregates with the
    # canonical TPC-DS column names replaced by warehouse-style names.
    # A/B against sql_schema_aggregates isolates the identifier-mapping
    # subsidy the model gets from having memorized the benchmark schema.
    Category(
        "sql_schema_colrename",
        "db+schema+aggregates, renamed cols",
        "sql",
        ".sql",
        warehouse_variants.setup_sql_schema_colrename,
    ),
    # Parameter-shift discriminator: canonical schema, but every prompt's
    # parameter values (manufacturer 128, month 11, TN, ...) are replaced and
    # scoring runs against matching shifted references in refs_shifted/.
    # Combined with colrename, the only remaining recall surface is question
    # structure itself.
    Category(
        "sql_schema_paramshift",
        "db+schema+aggregates, shifted params",
        "sql",
        ".sql",
        warehouse_variants.setup_sql_schema_aggregates,
        prompt_field="prompt_shifted",
        references_dir=EVAL_DIR / "refs_shifted",
    ),
    Category(
        "sql_schema_colrename_paramshift",
        "db+schema+aggs, renamed+shifted",
        "sql",
        ".sql",
        warehouse_variants.setup_sql_schema_colrename,
        prompt_field="prompt_shifted",
        references_dir=EVAL_DIR / "refs_shifted",
    ),
    # Noise-dose sweep cells (SQL path only — the enriched path is insensitive
    # to un-modeled physical tables by construction, so the enriched cells
    # above are the flat reference line). xN = N times the 12-table base set.
    # sql_schema_* = full-injection regime (whole schema.md handed over, cap
    # raised to fit); sql_bare_* = discovery regime (no schema.md, default
    # cap — the agent probes for itself, so cost tracks its behavior).
    *(
        Category(
            f"sql_schema_noise_x{multiplier}",
            f"db+schema+aggregates+noise×{multiplier}",
            "sql",
            ".sql",
            warehouse_variants.sql_noise_dose_setup(multiplier),
            tool_output_limit=warehouse_variants.noise_dose_output_limit(multiplier),
        )
        for multiplier in warehouse_variants.NOISE_DOSES
    ),
    *(
        Category(
            f"sql_bare_noise_x{multiplier}",
            f"db-only+aggregates+noise×{multiplier}",
            "sql",
            ".sql",
            warehouse_variants.bare_noise_dose_setup(multiplier),
        )
        for multiplier in warehouse_variants.NOISE_DOSES
    ),
    # Wave 3 — confusable in-domain traps (near-duplicate samples, grain
    # traps, stale dim copies). Reading schema.md can no longer settle table
    # choice, so this treatment can produce audit turns or wrong-table picks.
    # The enriched arm models the daily summaries as own curated files;
    # backup/staging junk stays unmodeled (plausible curation).
    *(
        Category(
            f"sql_schema_confusable_x{level}",
            f"db+schema+aggregates+confusable×{level}",
            "sql",
            ".sql",
            warehouse_variants.sql_confusable_dose_setup(level),
            tool_output_limit=131_072,
        )
        for level in (1, 2, 3)
    ),
    # Discovery-regime confusable cells: same traps, no schema.md. Traps now
    # carry copied source comments (no documentation bleed), and the bare
    # agent's LIKE filters surface them beside the real tables — the cell
    # where the canonical-name prior is tested nearly alone.
    *(
        Category(
            f"sql_bare_confusable_x{level}",
            f"db-only+aggregates+confusable×{level}",
            "sql",
            ".sql",
            warehouse_variants.bare_confusable_dose_setup(level),
        )
        for level in (1, 2, 3)
    ),
    Category(
        "enriched_confusable",
        "enriched+aggregates+confusable",
        "trilogy",
        ".preql",
        warehouse_variants.setup_enriched_confusable,
    ),
)

SPEC = BenchmarkSpec(
    name="TPC-DS",
    short_name="tpcds",
    duckdb_extension="tpcds",
    # dsdgen is a lazy table function — common.db.build_database materializes
    # SELECT-shaped generators via fetchall and runs CALL-shaped ones directly.
    generator_sql="SELECT * FROM dsdgen(sf={sf})",
    db_filename="warehouse.duckdb",
    eval_dir=EVAL_DIR,
    prompts_file=EVAL_DIR / "query_prompts.json",
    enriched_skip_prefixes=("query", "adhoc"),
    # tests/modeling/tpc_ds_duckdb is the hand-curated semantic model the
    # enriched leg of `--both-modes` seeds from instead of `trilogy ingest --all`.
    # The same directory also holds query<NN>.sql reference SQL files; the
    # scorer prefers these over PRAGMA tpcds() for queries where the spec
    # filter values yield empty results at our scale factor.
    default_enriched_dir=EVAL_DIR.parents[1] / "tests" / "modeling" / "tpc_ds_duckdb",
    references_dir=EVAL_DIR.parents[1] / "tests" / "modeling" / "tpc_ds_duckdb",
    # sf=1 by default: smaller factors leave many TPC-DS queries with empty
    # result sets, which agents spin on (re-exploring instead of accepting a
    # valid 0-row answer). Override with --scale-factor for quick local runs.
    default_scale_factor=1.0,
    # Full TPC-DS set (99 queries) by default so a plain rebaseline covers
    # everything; pass --num-queries / --query-ids to scope a quick local run.
    default_num_queries=99,
    additional_categories=MESSY_WAREHOUSE_CATEGORIES,
    funnel_order=(
        "sql_bare",
        "sql_schema",
        "sql_schema_aggregates",
        "sql_schema_colrename",
        "sql_schema_paramshift",
        "sql_schema_colrename_paramshift",
        "sql_schema_noise",
        *(
            f"sql_schema_noise_x{multiplier}"
            for multiplier in warehouse_variants.NOISE_DOSES
        ),
        *(
            f"sql_bare_noise_x{multiplier}"
            for multiplier in warehouse_variants.NOISE_DOSES
        ),
        *(f"sql_schema_confusable_x{level}" for level in (1, 2, 3)),
        *(f"sql_bare_confusable_x{level}" for level in (1, 2, 3)),
        "ingest",
        "enriched",
        "enriched_aggregates",
        "enriched_noise",
        "enriched_confusable",
    ),
)
