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
#
# 2026-06-28 MEASUREMENT AUDIT: the _INLINE/_MODELING buckets were assumed to be
# "cosmetic, rows match, only SQL shape differs". Measuring v3-vs-v4 on each entry
# (local_scripts: generate under CONFIG.use_v4_discovery False/True, compare len +
# JOIN/CTE counts, EXECUTE rows on synthetic data) showed that is FALSE for most:
#   - ONE returns WRONG ROWS (rowset_alias, see _V4_WRONG_ROWS) -- masked because
#     the test only asserts SQL shape and never executes.
#   - ~7 are real VERBOSITY regressions (v4 materially longer), see _V4_VERBOSITY.
#   - ~5 are STRUCTURAL (join-type / source-selection / shape-guard) diffs that can
#     diverge on edge rows, see _V4_STRUCTURE.
#   - Only ncaa::adhoc07 (+3%, same join) is genuinely cosmetic.
# Reasons below were re-bucketed accordingly. Do NOT condition these to green
# without fixing the underlying v4 gap -- that would mask the regression.
_INLINE = "v4 inlining/merge produces a different CTE shape than v3 (cosmetic; rows + length verified equal-or-better)"
_MODELING = (
    "v4 modeling-sweep regression (row-count / CTE-shape / assertion diff vs v3) "
    "-- pending per-test classification into result vs structure"
)
_V4_VERBOSITY = (
    "v4 rows match v3 but generated SQL is materially LONGER (un-inlined "
    "passthrough/source -> extra CTE or forced cross join). Real regression, NOT "
    "cosmetic -- measured longer 2026-06-28. Fix the v4 plan, don't relax the test."
)
_V4_STRUCTURE = (
    "v4 rows match v3 on consistent data, but the plan differs structurally (join "
    "type INNER->OUTER, datasource/source selection, or a v3-specific shape guard) "
    "and can diverge on orphan/unmatched rows. Verify rows before relying on parity."
)
_V4_WRONG_ROWS = (
    "v4 returns WRONG ROWS -- masked because the test only asserts SQL shape and "
    "never executes. rowset alias-collision: v4 drops the shared join key and emits "
    "`FULL JOIN ... on 1=1` -> cartesian product (verified 3 rows -> 27 on synthetic "
    "data, 2026-06-28). This is a correctness bug, not verbosity."
)
_TPCDS_SIZE = (
    "v4 TPC-DS verbosity: rows match the official reference but generated SQL "
    "exceeds the v3-tuned length ceiling (more CTEs / less compact)"
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
    # rowset_alias wrong-rows bug FIXED + pruned 2026-06-28: resolve_rowset now
    # exposes an unfiltered rowset's grain key, and _final_merge_grain/
    # _group_final_grain_contribution resolve the rowset-namespaced key to the shared
    # base so the FINAL merge INNER-joins siblings on it instead of `FULL JOIN on 1=1`
    # (cartesian). Rows now match (executing guard:
    # local_scripts/v4_evals/cases/rowset_alias_collision.preql); the shape test is
    # dual-conditioned on CONFIG.use_v4_discovery. Passes under both planners.
    # --- VERBOSITY: rows match, v4 materially longer (measured 2026-06-28) ---
    # select_literal pruned 2026-06-30 (294->117, == v3): a constant-only FINAL
    # contributor got its own CTE + `FULL JOIN on 1=1`; `_fold_constant_parents`
    # (strategy_builder) now folds a constant into a non-constant sibling's
    # projection (constants render inline anywhere), mirroring v3. XPASS in
    # isolation + full sweep.
    # bound_conversion presto: 1022->1249 (+22%).
    "tests/complex/test_bound_conversion_existence.py::test_bound_conversion_existence_presto": _V4_VERBOSITY,
    # aggregate_filter HAVING pruned 2026-06-30 (522->442, no more CASE WHEN):
    # predicate pushdown relocates `count(...) > 1` into the group parent's
    # HAVING and strips the copy from the filter consumer, so `cte.condition` no
    # longer named it and the filter-item reverted to a redundant `CASE WHEN
    # count > 1 THEN order_id ELSE NULL`. Fix: the renderer
    # (`_filter_guaranteed_by_sole_parent`, base.py) also renders the content
    # bare when the CTE's SOLE (non-join) parent's condition implies the filter's
    # where -- the pushdown's own safety invariant guarantees every surviving row
    # satisfies it. XPASS in isolation + full v4 sweep; v3 + v4 sweeps clean.
    # in_subselect: IN-subquery source not inlined; references concept alias cs_item_id.
    "tests/engine/test_duckdb_filter.py::test_in_subselect_with_inlined_datasource": _V4_VERBOSITY,
    # usa_names anonymous aggregate-filter: same joins, +70% extra CTE/structure.
    "tests/modeling/usa_names/test_names.py::test_aggregate_filter_anonymous": _V4_VERBOSITY,
    # --- STRUCTURE: rows match on consistent data; plan/join/source differs (2026-06-28) ---
    # nested_greatest: v4 emits no group-by CTE projecting multi_wm (v3-shape guard).
    "tests/optimization/test_union_branch_projection_collision.py::test_nested_greatest_refresh_keeps_watermark_projection": _V4_STRUCTURE,
    # persist_with_where: persisted upper_name source not reused -> recomputes CASE.
    "tests/persistence/test_basic_persistence.py::test_persist_with_where": _V4_STRUCTURE,
    # filter_scalar staging: v4 sources staged_sales_tbl where v3 does not (ROWS UNVERIFIED).
    "tests/engine/test_duckdb_filter.py::test_filter_scalar_aggregate_not_restricted_by_staging": _V4_STRUCTURE,
    # provider_name: drops a join, FULL JOIN on real key vs v3 LEFT OUTER+INNER; rows match
    # on consistent data, diverge on orphan/dividend-less rows (full vs inner).
    "tests/modeling/stocks/test_stocks.py::test_provider_name": _V4_STRUCTURE,
    # tpc_h adhoc07: INNER -> RIGHT/FULL outer join types; rows VERIFIED MATCH (sf=0.01).
    "tests/modeling/tpc_h/instantiated/tpc_h/test_instantiated_tpc_h.py::test_adhoc07": _V4_STRUCTURE,
    # ncaa::test_adhoc07 pruned 2026-06-30: not a size diff -- v4 renders the
    # user-named `eligible` concept (CASE WHEN count(game_id) > 10 THEN 1 ELSE 0)
    # as a materialized column referenced by name in the window ORDER BY, where v3
    # inlines the same CASE. Provably equivalent rows (the column IS that CASE);
    # the ncaa source is a BigQuery public dataset, not locally executable, so
    # equivalence is verified from the SQL. The test regex is now dual-conditioned
    # on CONFIG.use_v4_discovery to accept either rendering. Passes under both.
    # test_aggregate_of_aggregate pruned 2026-06-30: passes under v4 in isolation
    # AND full sweep (was a stale prune-candidate).
    # --- tpc-ds: SQL-length-ceiling regressions (correct rows, more verbose) ---
    # Pruned 2026-06-26 (pass in isolation + tracked-group + full sweep): test_two (q02),
    # test_forty_seven (q47), test_fifty_seven (q57), test_seventy_six (q76).
    # q10 pruned 2026-06-27: existence-source isolation (semijoin-RHS buyer-set
    # filters sourced as their own discovery; see group_graph/group_rules/filter)
    # lets the customer-dimension projection source standalone instead of through
    # the fact; 8308->6412, under the 7000 ceiling. XPASS in isolation + 2 full
    # sweeps.
    # q2.1 pruned 2026-06-29 (8747->7276, under the 7500 ceiling): the named
    # `*_sales` intermediate made the round() BASIC infer date.id grain (the
    # window's `order by date.week_seq` flattened up as a grain parent and
    # descended to its key), so the same-grain window merge that fixed q2.2 was
    # skipped. Fix is three-part: (1) `_get_relevant_parent_concepts`
    # (parsing/common) + `_row_grain_concept_refs` (author) exclude a navigation
    # window's order-by from a wrapping expression's grain inference, so the
    # round BASIC lands at date.week_seq; (2) `_feeds_extra_signature_group`
    # (group_rules) blocks the subset-nest merge that then put `*_sales` (window
    # input) and `*_increase` (window consumer) in one bucket -> group cycle; (3)
    # `_merge_basic_into_window_parent` (group_graph) accepts a partial-spine
    # window when it already sources every input the round needs, folding the
    # round inline (v3's window+round shape). XPASS in isolation + full sweep.
    # q2.2 pruned 2026-06-28 (8856->7276, under the 7500 ceiling): _merge_basic_into_
    # window_parent (group_graph) folds the same-grain round() BASIC into its WINDOW
    # producer so the leads render inline (v3's window+round shape) instead of the
    # window materializing 14 agg + 7 lead passthrough columns for a separate round
    # node. XPASS in isolation + full sweep.
    # q30.alt pruned 2026-06-30 (second web_returns GA-spine scan eliminated,
    # 6193->6112, and web_returns==1 / GROUP BY==2): the post-aggregate GA filter
    # (`billing_customer.address.state = 'GA'`, FD by billing_customer.id) was
    # kept on the fact bucket because it isn't a SELECTED output, spawning a
    # second fact scan just to apply it. Fix: `_split_root_dimension_clusters`
    # (group_graph) also peels a filter-only POST-aggregate (HAVING) arg into the
    # single-entity FD dim bucket (pre-aggregate args still stay on the fact), and
    # `_assemble_final_node` (strategy_builder) sources such a peeled filter arg
    # in the fresh root projection so the condition survives
    # `_root_atoms_satisfiable_from` and plan_source joins the dim table (v3's
    # `wakeful` = `customer join customer_address WHERE state = 'GA'`). XPASS in
    # isolation + full sweep; no net-new failures.
    # q73 pruned 2026-06-27: the single-entity FD dimension-cluster split
    # (`_split_root_dimension_clusters`) sources the customer dims standalone
    # instead of re-rooting them on the fact; 5220->2737, under ceiling. Passes
    # in isolation + full sweep.
    # q81 pruned 2026-06-27: dimension split + condition-aware feeder drop
    # (`_feeder_conditions_implied`) + a post-pushdown CollapseSingleParent rerun
    # with a PASSTHROUGH merge mode folding the bare dim-projection CTE; 9163->6567,
    # under ceiling. v3 + v4 full sweeps clean.
    # q94 pruned 2026-06-27: the per-consumer ROOT re-slice fix (share a built
    # conditioned ROOT instead of re-deriving the join) took it 5271->3508, well
    # under ceiling. Passes in isolation + full sweep.
    # q23 pruned 2026-06-27: the all-ROOT input-grain normalization is now skipped
    # when the parents already emit one row per input-grain key
    # (`_parents_already_at_input_grain`), so the q16 correctness floor no longer
    # adds CTEs here; 8515->8107, under the 8500 ceiling. XPASS in isolation + 2
    # full sweeps. The q16 floor itself is unchanged (still normalizes finer
    # fact-line scans).
    # --- tpc-ds non-benchmark: VERBOSITY (measured 2026-06-28) ---
    # rowset_arithmetic pruned 2026-06-29: was 6290->8747 (+39%), the same
    # window/round passthrough family as q2.1. The q2.1 grain + window-merge fix
    # (navigation-window order-by excluded from a wrapping expression's grain,
    # subset-nest cycle guard, partial-spine window absorb) clears it too. XPASS
    # in isolation + full sweep.
    # two_merge: merge_aggregate=True branch passes (5==5); merge_aggregate=False is 9->11 CTEs.
    "tests/modeling/tpc_ds_duckdb/test_non_benchmark_queries.py::test_two_merge_aggregate_compacts_inline_window_query": _V4_VERBOSITY,
}
