"""Registry of suite tests that do not yet pass under the v4 discovery planner.

When the suite runs with v4 enabled (`TRILOGY_V4_DISCOVERY=1`), `conftest`'s
collection hook turns each listed test into an `xfail` (non-strict). Listed tests
that still fail show as xfailed; ones that now pass show as xpassed; either way the
v4 gate stays green, and a real regression (a test NOT listed here) still fails
loudly. Non-strict so a listed test that now passes shows xpassed (keeping the gate
green) instead of flipping it red; prune such entries once confirmed. To promote an
entry, re-check it in ISOLATION
(`pytest <nodeid>` with the env var) and, if it asserts SQL *shape*, condition the
expected SQL on `CONFIG.use_v4_discovery` so it passes under both planners.

This is the migration tracking list for v4 gaps that aren't yet at parity:
- structure regressions -- v4 returns correct rows but a worse plan (e.g. ignores
  a pre-aggregated summary table, so the assertion on the chosen source fails);
- crashes -- v4 raises while building/rendering.

Pure result-parity repros that distill to a standalone program live instead in
`local_scripts/v4_evals` (`cases/` once at parity, `failing_cases/` while known-bad).

Keys are pytest nodeids (path::test, with the leading `tests/`). Matched against
the part of the nodeid before any `[param]` suffix, so one key covers every
parametrization of a test.
"""

from __future__ import annotations

# Reason strings are deliberately coarse: they name the v4 capability gap, not a
# per-test diff. Group edits when a whole class of tests shares one root cause.
_INLINE = "v4 inlining/merge produces a different CTE shape than v3"
_MODELING = (
    "v4 modeling-sweep regression (row-count / CTE-shape / assertion diff vs v3) "
    "-- pending per-test classification into result vs structure"
)
_TPCDS_SIZE = (
    "v4 TPC-DS verbosity: rows match the official reference but generated SQL "
    "exceeds the v3-tuned length ceiling (more CTEs / less compact)"
)
# q23/q94: correct rows but over the v3 length ceiling because v4 now force-
# normalizes all-ROOT aggregate inputs to the input grain (the all-ROOT guard in
# `_aggregate_inputs_are_row_preserving`). That guard is a CORRECTNESS floor --
# without it `count(order_number)` over un-deduped line rows over-counts (q16:
# 818 vs 233). q23/q94 were compact only by skipping that dedup, which was
# correct-by-data-luck. A grain-aware skip (normalize only when the parent rows
# are genuinely finer than the input grain) would restore their compactness, but
# there is no reliable parent-row-grain signal at that point yet -- deferred to a
# v4-focused pass. Do NOT relax the q23/q94 ceilings (v3 must keep holding them).
_TPCDS_SIZE_NORMALIZE = (
    "v4 TPC-DS verbosity: correct rows, but over the v3 ceiling because the "
    "all-ROOT input-grain normalization (a q16 correctness floor) adds CTEs; "
    "needs a grain-aware skip -- see project_v4_verbosity_regressions_0626"
)
# Genuine v4 crashes (NOT size/shape). The existence-recursion crash (q10/q2.1/rowset)
# was FIXED 2026-06-25: `_existence_parents_for` deep-copies a cyclic existence-parent
# subtree, and `gen_root` resolves multi-arg existence sources at build time. Those
# three reverted to _TPCDS_SIZE / _INLINE. _CRASH_INVALID_REF (filter-over-constant)
# remains -- see local_scripts/v4_existence_recursion_handoff.md part B.
_CRASH_INVALID_REF = (
    "v4 ValueError: filter-over-constant renders an unresolvable concept reference "
    "into the SELECT (dialect/base.py:2370)"
)
V4_KNOWN_FAILING: dict[str, str] = {
    # --- optimization: CTE-shape snapshot diffs ---
    "tests/optimization/test_inlining.py::test_non_nullable_null_guard_does_not_block_datasource_inlining": _INLINE,
    "tests/optimization/test_inlining.py::test_select_literal_is_rendered_with_aggregate_projection": _INLINE,
    "tests/optimization/test_union_branch_projection_collision.py::test_nested_greatest_refresh_keeps_watermark_projection": _INLINE,
    # --- complex: shape diffs (assert on SQL, not crashes) ---
    "tests/complex/test_bound_conversion_existence.py::test_bound_conversion_existence_presto": _INLINE,
    "tests/complex/test_complex_source_fetching.py::test_aggregate_of_aggregate": _INLINE,
    "tests/complex/test_rowset.py::test_rowset_alias_name_collision": _INLINE,
    # --- persistence / etl: persisted-source reuse + shape diffs ---
    "tests/persistence/test_basic_persistence.py::test_persist_with_where": _INLINE,
    # --- engine: rendering / source-selection / crashes ---
    "tests/engine/test_duckdb_filter.py::test_aggregate_filter_uses_having": _INLINE,
    "tests/engine/test_duckdb_filter.py::test_filter_scalar_aggregate_not_restricted_by_staging": _INLINE,
    "tests/engine/test_duckdb_filter.py::test_in_subselect_with_inlined_datasource": _INLINE,
    # --- modeling (non-TPC) sweep ---
    "tests/modeling/ncaa/test_ncaa.py::test_adhoc07": _MODELING,
    "tests/modeling/stocks/test_stocks.py::test_provider_name": _MODELING,
    "tests/modeling/usa_names/test_names.py::test_aggregate_filter_anonymous": _MODELING,
    # --- tpc-h: adhoc07 shape ---
    "tests/modeling/tpc_h/instantiated/tpc_h/test_instantiated_tpc_h.py::test_adhoc07": _MODELING,
    # --- tpc-ds: SQL-length-ceiling regressions (correct rows, more verbose) ---
    # NOTE: test_two (q02) and test_seventy_six (q76) now PASS in isolation (verified
    # 8x / 3x, 2026-06-25); kept listed until a full v4 sweep confirms, then prune.
    "tests/modeling/tpc_ds_duckdb/test_queries.py::test_two": _TPCDS_SIZE,
    "tests/modeling/tpc_ds_duckdb/test_queries.py::test_ten": _TPCDS_SIZE,
    "tests/modeling/tpc_ds_duckdb/test_queries.py::test_two_one": _TPCDS_SIZE,
    "tests/modeling/tpc_ds_duckdb/test_queries.py::test_two_two": _TPCDS_SIZE,
    "tests/modeling/tpc_ds_duckdb/test_queries.py::test_thirty_alt": _TPCDS_SIZE,
    "tests/modeling/tpc_ds_duckdb/test_queries.py::test_forty_seven": _TPCDS_SIZE,
    "tests/modeling/tpc_ds_duckdb/test_queries.py::test_fifty_seven": _TPCDS_SIZE,
    "tests/modeling/tpc_ds_duckdb/test_queries.py::test_seventy_three": _TPCDS_SIZE,
    "tests/modeling/tpc_ds_duckdb/test_queries.py::test_seventy_six": _TPCDS_SIZE,
    "tests/modeling/tpc_ds_duckdb/test_queries.py::test_eighty_one": _TPCDS_SIZE,
    # q23/q94: over ceiling only because of the q16 all-ROOT normalization
    # correctness floor (2026-06-26); rows correct. Re-optimize in a v4 pass.
    "tests/modeling/tpc_ds_duckdb/test_queries.py::test_twenty_three": _TPCDS_SIZE_NORMALIZE,
    "tests/modeling/tpc_ds_duckdb/test_queries.py::test_ninety_four": _TPCDS_SIZE_NORMALIZE,
    # --- tpc-ds non-benchmark: result / feature regressions ---
    "tests/modeling/tpc_ds_duckdb/test_non_benchmark_queries.py::test_rowset_arithmetic_argument_keeps_precedence": _INLINE,
    "tests/modeling/tpc_ds_duckdb/test_non_benchmark_queries.py::test_two_merge_aggregate_compacts_inline_window_query": _MODELING,
}
