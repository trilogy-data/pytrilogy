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
_V4_DISJOINT_NULLSAFE = (
    "v4 disjoint scoped-join groups (one INNER + one FULL on separate derived "
    "keys): the derived-key INVALID_REFERENCE render is FIXED 2026-07-04 "
    "(_datasource_renders_derived, source_planning.py), but a distinct bug remains "
    "-- the disjoint FULL null-injects the INNER group's key into its one-sided "
    "rows, yet that key is not marked nullable through the assembly, so the final "
    "multi-measure merge joins on it with plain `=` instead of `IS NOT DISTINCT "
    "FROM` and the NULL-keyed row fans into spurious all-NULL copies. Nullability-"
    "propagation gap in the disjoint-group merge (get_modifiers, join_resolution), "
    "NOT the derived-key render family."
)
_V4_ROWSET_XDS_CONTAM = (
    "v4 rowset cross-datasource: PASSES in isolation and at file level, FAILS only "
    "in the full multi-file sweep -- pre-existing cross-file contamination (shared "
    "module-level executors), present on baseline too (verified 2026-07-04, NOT a "
    "regression of the derived-key render fix). Prematurely pruned earlier; "
    "re-tracked so the full-suite gate stays honest. Find + isolate the polluting "
    "file, then prune."
)
_V4_MASKED_LEAK = (
    "v4 failure EXPOSED 2026-07-02 by fixing a CONFIG.use_v4_discovery leak "
    "(test_v4_node_generators._generate_v4_sql restored it to a hardcoded False in "
    "a finally, so the rest of each sweep silently ran under v3). These fail under "
    "v4 but passed in-suite under the leaked v3 planner -- real, previously-masked "
    "v4 gaps now tracked pending per-family triage/fix. Dominant families: "
    "rowset-cross-datasource outer read, scoped-join outer key. Fix the plan, then "
    "prune; do NOT re-mask."
)
# Genuine v4 crashes (NOT size/shape). The existence-recursion crash (q10/q2.1/rowset)
# was FIXED 2026-06-25: `_existence_parents_for` deep-copies a cyclic existence-parent
# subtree, and `gen_root` resolves multi-arg existence sources at build time. Those
# three reverted to _TPCDS_SIZE / _INLINE. _CRASH_INVALID_REF (filter-over-constant)
# remains (filter-over-constant renders an unresolvable reference).
_CRASH_INVALID_REF = (
    "v4 ValueError: filter-over-constant renders an unresolvable concept reference "
    "into the SELECT (dialect/base.py:2370)"
)
_V4_UNSWEPT_GAP = (
    "v4 gap surfaced by the FIRST full-suite v4 sweep outside the gate dirs "
    "(2026-07-26 s38). Pre-existing: fails identically under the pre-refactor "
    "enumerate+repair engine (A/B'd via old-engine plugin) and passes under v3. "
    "Triage per family before the v4 default flip."
)

V4_KNOWN_FAILING: dict[str, str] = {
    # REGISTRY EMPTY again as of 2026-07-30 s52 — no known v4 correctness gaps
    # vs the legacy recursive engine. Keep the mechanism (and the fix records
    # below) for any future regression.
    # test_not_null_on_aggregate_grain_key_is_enforced pruned 2026-07-30 s52:
    # `where key is not null` beside an aggregate grouped BY that key rendered
    # nowhere. Placement and `build_node` were both correct — the conditioned
    # nodes were built and then discarded. `gen_root` sources from datasources,
    # not from its `parents`, so when a ROOT group's condition names a DERIVED
    # row arg (`total > 0`), `_resolve_root_condition_sources` re-plans that arg
    # through a fresh `search_concepts(conditions=[])`. The ancestor groups'
    # atoms are not in those rows, and passing no conditions dropped them: the
    # aggregate was rebuilt unfiltered and the NULL-key group survived the LEFT
    # join to the dimension. `preexisting_conditions` is now threaded through
    # `build_node` into `gen_root` and down into that sub-search. tpc-ds q11 was
    # the same bug (passing only by luck of the data) and its `channel in (...)`
    # / `year in (...)` atoms were no-ops for the same reason — all three now
    # render, pruning q11's dead catalog_sales union arm.
    # test_ambiguous_error un-parked 2026-07-26: model_ambiguity.py raises the
    # typed error from static path analysis BEFORE the source search runs.
    # --- 2026-07-26 s38: first full-suite sweep outside the gate dirs ---
    # test_partition_persistence pruned 2026-07-29 s49: after `publish
    # datasources daily_fact`, v4 joined the summary table BACK to the raw
    # ride scan and crashed rendering the derived join key `ride_month`
    # against the raw DatasourceCTE. Root cause: `_concepts_with_grain_keys`
    # (source_planning) expanded the authored host grain (`ride_id`) of the
    # requested aggregate's own axis members into the bridge search, making
    # the raw table a mandatory terminal. A requested AGGREGATE pins the
    # population at its own grain — its axis members join by themselves — so
    # their host keys are no longer expanded; the network answers with the
    # single summary scan and `_direct_source` renders it (v3 parity).
    # test_adhoc03 pruned 2026-07-29 s49: `auto recursive_parent <-
    # recurse_edge(id, parent); merge recursive_parent into root_parent.id`
    # planned a silent `INNER JOIN on 1=1` cartesian (no recursion at all).
    # Three mechanisms, one principle — a GLOBAL merge whose collapsed member
    # keeps a ROW-SHAPE computed origin is the same
    # axis-equality-between-different-lineages as a statement-scoped computed
    # join key (the s43 RELATION rail). `computed_origin_relation_members`
    # (concept_graph) is the shared predicate; each consumer is NARROWLY
    # gated (the gates were added only after the wide version regressed live
    # tests): (1) the RELATION re-injection fires only when the relation is
    # DEMANDED (a member is an output or condition arg) — a query that merely
    # filters on a side's property (`where parent.label = 'A' select
    # count(id)`) is served by the condition-feeder fallback, and
    # relation-partitioning its graph over-counted 6 vs 4 (engine
    # test_recursive_enrichment); (2) `_scoped_axis_mates` includes the
    # relation unconditionally so `_compute_concept_sets` exposes the axis and
    # the FINAL cover keeps the recursion group; (3) the statement-relation
    # set INSIDE `plan_condition_placements` (not
    # `_statement_relation_addresses` itself) includes it only when no
    # grouping contributor exists, so a side's WHERE defers past the
    # completion merge in row-level statements (`root_parent.type = 'story'`
    # renders post-join) but never floats above an aggregate (same
    # over-count). Plus `_candidate_groups` (condition_placement) bars
    # RECURSIVE groups from hosting row atoms — filtering the edge set changes
    # reachability (a 2024 post's parent chain crosses years) — so
    # `create_time.year = 2024` lands at FINAL. Scalar BASIC origins stay on
    # the derived merge-key rail (the join_matrix derived/union/merge nullable
    # cell lost its FULL null-extension when routed through the relation
    # axis). v4 now renders the full v3 `WITH RECURSIVE` shape.
    # test_cross_rowset_membership_sources_existence pruned 2026-07-27 s46
    # (WRONG ROWS, s43-diagnosed): placement fused the statement WHERE
    # (`ftr_sales.sales_amt is not null`) with the projection filter's `dw = 0`
    # onto ONE narrowed ftr stream, so ws=2 lost its ftr join partner and the
    # licensed join upgrades (FULL->RIGHT->INNER) dropped the anchor row. Root
    # cause was upstream of the fusion: `_group_in_active_relation`
    # (condition_placement) misclassified the ftr ROWSET boundary as
    # self-contained because the scoped-merge CANONICAL (`cur_sales.ws`,
    # relabeled from the collapsed handle) rides the boundary's grain — both
    # relation endpoints counted as own-side keys, so `mates = relation - keys`
    # came up empty (the same swallow-the-relation failure the function already
    # guards against for pseudonyms). Fix: a ROWSET boundary's own-side keys
    # are its OWN rowset's handles only (by ConceptAttrs.rowset_name /
    # BuildRowsetItem lineage, discriminator names the rowset); the foreign
    # canonical is the axis handle. The atom now defers to FINAL as a
    # post-join predicate: the ftr stream keeps raw rows (sun_ftr renders as a
    # projection, raw sales_amt carried up) and the not-null filters above the
    # completion merge. Both params pass; golden SQL 109/109 identical;
    # join_matrix + rowset xpass/xfail sets byte-identical to HEAD (worktree
    # A/B).
    # test_window_expression_join pruned 2026-07-27 s43 (both backends): the
    # scoped-merge canonicalization collapsed the JOIN AXIS — a WINDOW-derived
    # member (`union join rank orders.oid order by orders.amt desc =
    # customers.rnk`) was substituted onto the canonical, erasing its computed
    # lineage, so no group computed the rank and the plan degraded to a silent
    # `LEFT JOIN ON 1=1` cartesian (wrong rows; the aggregate-key sibling test
    # passed only by data luck). Fix models the axis as graph structure: (1)
    # concept_graph re-injects a collapsed computed member's origin (from
    # alias_origin_lookup) + its canonical as first-class nodes linked by a new
    # RELATION edge kind (rowset-handle origins excluded — self-weld
    # recursion); (2) partition_roots partitions root buckets by connectivity
    # EXCLUDING relation edges (sides recombine at the merge, not in one
    # scan); (3) group IO + _final_merge_grain expose a cross-contributor
    # hosted relation as the FINAL merge axis (gated on RELATION-edge
    # members); (4) _add_relation_axis_contributors adds the axis-hosting
    # group the mandatory cover would drop. v4 now renders the v3-equivalent
    # window CTE joined on rank=rnk. join_matrix xpass set == HEAD; golden SQL
    # 109/109 identical; TPC-DS battery 158/158.
    # test_validated_tvf_output pruned 2026-07-26 s39: `_resolve_union_select`
    # now wraps a BARE-scan arm (`(select id, score)`, no aliases) in a
    # rename-only SELECT (v3 `gen_union_select_node`'s shape) when the arm QDS
    # can't absorb the union-output re-exposure; compute arms keep the in-place
    # absorb (no shape drift). Passes both parser backends, isolation + file.
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
    # bound_conversion presto pruned 2026-07-28 s48b: the 2026-06-28 +22%
    # measurement is stale — on HEAD v4 renders SHORTER than v3 (1175 vs 1355
    # duckdb; v3 redundantly re-applies the latest_date WHERE + cross join at
    # the final select). Rows execution-verified equal. The test's exact
    # v3-CTE-name snippet assert replaced with a name-agnostic structural
    # regex (latest_date bound filters before the count, over the
    # (date_converted, id) dedup) that passes under both planners.
    # aggregate_filter HAVING pruned 2026-06-30 (522->442, no more CASE WHEN):
    # predicate pushdown relocates `count(...) > 1` into the group parent's
    # HAVING and strips the copy from the filter consumer, so `cte.condition` no
    # longer named it and the filter-item reverted to a redundant `CASE WHEN
    # count > 1 THEN order_id ELSE NULL`. Fix: the renderer
    # (`_filter_guaranteed_by_sole_parent`, base.py) also renders the content
    # bare when the CTE's SOLE (non-join) parent's condition implies the filter's
    # where -- the pushdown's own safety invariant guarantees every surviving row
    # satisfies it. XPASS in isolation + full v4 sweep; v3 + v4 sweeps clean.
    # in_subselect pruned 2026-07-01 (was a correctness bug, not verbosity):
    # InlineDatasource runs before predicate_pushdown and folds the membership's
    # subselect source into the child. Pushdown then tried to promote that
    # membership up to the child's parent, but the inlined source has no
    # dependency CTE to re-hang -- so the parent's IN referenced a dangling
    # `cs_catalog_sales` (the un-inlined `cs_item_id` alias) AND the child copy
    # survived (membership applied twice = invalid + duplicated). Fix: `_check_
    # parent` (predicate_pushdown) vetoes the push when an existence source is
    # inlined into the child (source_map entry with no promotable dependency). v4
    # now applies it once, inlined. Shared optimizer; v3 + v4 sweeps clean.
    # usa_names anonymous aggregate-filter pruned 2026-07-28 s48b (+70% fixed,
    # two shared-optimizer gaps): (1) PredicatePushdownRemove could not strip
    # the promoted-HAVING's redundant WHERE copy when a join CTE sat between
    # the copy and the HAVING group — `_parent_covers_condition` now takes a
    # transitive hop through a parent when every source of every referenced
    # column is a covering, non-NULL-padded ancestor. (2) The stripped
    # condition wrapper then still carried its unused aggregate virts as
    # passthrough columns, and `is_passthrough_projection` counted them as
    # local aggregate compute — a column with a non-empty source_map entry
    # (pulled from upstream) is now passthrough-safe whatever its derivation.
    # v4: 2151 -> 1284 chars (v3 1139; residual = one un-inlined base-scan
    # CTE). Test regex un-anchored from first-CTE position; the `WHERE not in
    # sql` no-duplicate assert passes under both planners.
    # --- STRUCTURE: rows match on consistent data; plan/join/source differs (2026-06-28) ---
    # nested_greatest pruned 2026-07-28 s48b: v4 re-derives multi_wm from the
    # complete root watermark sources (greatest(wm_a, wm_b) inline) instead of
    # reading the PARTIAL's stored column, so the group-parent/union-arm CTE
    # collision the test guards cannot arise (and reading a `partial` column
    # for a global watermark is the more questionable choice anyway). Rows
    # execution-verified equal; end-to-end binder guard
    # (test_nested_greatest_refresh_sql_executes) passes under v4. Structural
    # assert dual-conditioned: under v4 it pins that NO CTE projects multi_wm.
    # persist_with_where pruned 2026-07-01: v4 recomputed the CASE from
    # category_source instead of reading the persisted `upper_name`. The persist
    # `... where category_id = 1` stores only the derived column (no category_id),
    # so `_datasource_materializes` -> `_conditions_supported` rejected it (can't
    # re-express `category_id = 1`), even though its `non_partial_for` already
    # bakes in that exact population. Fix: `_datasource_materializes` skips the
    # re-expression requirement when the query `where` and the ds `non_partial_for`
    # are mutually implied (population is exactly the desired rows). v4 now reads
    # `upper_name` (no CASE, no category_id), matching v3. XPASS in isolation.
    # filter_scalar staging pruned 2026-07-01: ROWS VERIFIED CORRECT under v4 across
    # all 4 permutations. The filter-scalar avg(price) ranges over the full items
    # table in v4 (sale_count == 1, not 2 -- a restricted avg would give 2), so the
    # bug the test guards is absent. v4 additionally sources the OUTER scan from the
    # pre-joined staging table (its non_partial_for matches the outer sale_year=2023
    # filter) -- equivalent + correct, and v4 correctly avoids staging in
    # permutation 3 (no year filter -> staging incomplete). The P2 `staged not in
    # sql` shape assertion (over-broad: it conflated "avg restricted" with "staging
    # used at all") is now conditioned on the v3 planner; the sale_count row check
    # is the real guard and holds under both.
    # provider_name + instantiated adhoc07 (both _V4_STRUCTURE) pruned 2026-07-26
    # s38: xpassed in isolation under the obligation-search engine.
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
    # two_merge pruned 2026-07-01: merge_aggregate=True branch always passed
    # (5==5); the merge_aggregate=False branch was 11 vs v3's 9. v4's semijoin
    # membership (`date.week_seq in relevent_week_seq`) is modeled as a join
    # node; predicate pushdown later relocates it to the fact scan's WHERE
    # subselect, leaving a bare passthrough projection (v3 never materializes
    # that node). CollapseSingleParent would fold it, but the whole rule is
    # gated on merge_aggregate -- off in this test. Fix: a passthrough-only
    # CollapseSingleParent cleanup phase (`collapse_single_parent.
    # passthrough_after_pushdown`) that runs even when merge_aggregate is off
    # (passthrough removal is orthogonal to aggregate merging). v4 now 9 == v3;
    # default path (merge_aggregate=True) untouched. XPASS in isolation.
    # --- MASKED-LEAK batch (tracked 2026-07-02): real v4 failures that a
    # CONFIG.use_v4_discovery leak hid by silently running them under v3 in the
    # full sweep. Now tracked; triage/fix per family, then prune. ---
    # 2026-07-26 s38 ISOLATION RE-VERIFY (obligation-search cutover): 15 entries
    # xpassed in isolation and were pruned — stocks::test_provider_name,
    # stocks::test_import, instantiated tpc_h::test_adhoc07, tpch::test_four,
    # tpch::test_seventeen, outer_where_pushes_into_global_agg,
    # enum_union_arm_spanning ×3, ambiguous_error_with_forced_join ×2,
    # membership_in_having_auto_concept, rowset cross_datasource_join_resolves,
    # q64_join_form_plans, test_where_scalar. Three STALE keys pruned (tests no
    # longer exist): test_disjoint_inner_and_full_groups,
    # test_all_left_unaffected, test_inner_to_dim_plus_two_left_rowsets_compiles.
    # test_aggregate_by_grain_with_derived_of_key pruned 2026-07-28 s48:
    # `pair <- sum(1) by (prefix, item_sk)` — a LITERAL-measure aggregate.
    # `_aggregate_grain_only_parents` (discovery_utility) treated ALL its
    # by-keys as grain-only (measure has no concept inputs, so
    # `grain - measure_addrs` = the whole grain) and severed every edge tying
    # it to the model. For a literal measure the grain keys ARE the row demand
    # — the aggregate can only be computed over its grain's row set — so the
    # connectivity map now keeps those edges. Same fix clears the bare
    # `select store, sum(1)` shape (broken on HEAD, new test
    # test_literal_measure_aggregate_beside_key) and the empty-rollup entry
    # below.
    # test_high_value_customer_filter pruned 2026-07-28 s47 (was DIAGNOSED s46:
    # WHERE `customer_revenue > 100` silently DROPPED — v4 read customer_summary
    # unfiltered). Fix: `_materialized_root_addresses` now also marks condition
    # row-args (EXACT branch only; the rollup branch stays mandatory-only since
    # filtering pre-rollup rows by a rolled-up value is wrong), so the WHERE arg
    # becomes a ROOT member of the dim cluster, the atom passes
    # `_root_atoms_satisfiable_from`, and plan_source — seeing the population-
    # exact `partial ... complete where` table — picks high_value_customers
    # directly (no residual filter), matching v3. The anonymous-spelling
    # sibling (`WHERE sum(order_value) > 100`, test skipped) remains open: it
    # now fails LOUD (INVALID_REFERENCE_BUG — the `_wscope` virt's address is
    # bound by no datasource, only its canonical) where HEAD silently dropped
    # both the WHERE and the revenue output.
    # test_canonical_collision_single_source_both_columns pruned 2026-07-28
    # s48b: NOT a correctness gap — v4 plans each multiselect arm
    # independently (one facts scan per arm, each folded into its own spine
    # merge reading only its own physical column), where v3 shared one rename
    # scan via identical-CTE dedup. Rows execution-verified identical (12/12
    # incl. NULL-key row) on synthetic data; v4 SQL is shorter (1402 vs 1711).
    # Assertions dual-conditioned on CONFIG.use_v4_discovery. Cross-arm scan
    # CSE remains a shape nicety, deferred to the size/shape round.
    # test_global_avg_filter_does_not_fan_out_group pruned 2026-07-27 s43 (q22
    # shape, WRONG ROWS): the conjunction-recompute delivery vetoed any atom
    # with truthy `existence_arguments` — but a LITERAL membership
    # (`cntrycode in ('13','17','31')`) yields `[()]` (one EMPTY group), so
    # the IN atom never reached the d0 aggregate recompute host and the
    # aggregate counted rows the conjunction excludes ('99' leaked). The veto
    # now keys on `any(existence_arguments)` — an actual existence CONCEPT (a
    # feeder to wire) stays home, a literal membership travels like any row
    # atom (condition_placement._conjunction_recompute_placements). v3/v4 rows
    # identical; golden SQL unchanged by this fix alone.
    # test_subselect_non_correlated pruned 2026-07-28 s48, two halves:
    # (1) the v4 disconnect pre-gate counted `const <- unnest([1,2])` as a
    # disconnected subgraph — `_crossjoinable` only skipped SINGLE_ROW /
    # CONSTANT, missing multi-row LITERAL-DERIVED values; it now skips any
    # concept whose transitive derivation touches no datasource
    # (`_literal_derived`), matching v3's cross-join planning. (2) Past the
    # gate, the non-correlated `subselect(val ...)` computed its one global
    # value once per parent row and advertised a collapsed QDS grain
    # (from_concepts([top3]) = Grain<top3> while the stream still carried the
    # 5-row id grain), so no group was inserted and the FINAL cross join
    # fanned 2 rows to 10 — the test's single-row assert masked it. v4
    # gen_subselect now wraps a GroupNode over the outputs when every output
    # is a non-correlated subselect (v3's exact per-row-compute + GROUP BY
    # collapse shape). Rows now identical to v3.
    # test_composite_rollup_aggregate_keeps_group_by pruned 2026-07-14 s3
    # (isolation + in-suite verified): fixed by the non-standard-grouping
    # parent-fold gate + rollup FINAL passthrough in strategy_builder.
    # test_derived_membership_existence pruned 2026-07-27 s42 (all 5 params):
    # the s39-diagnosed 3-part fix, landed together under one principle — a
    # lineage-existence arg (the SubselectComparison RHS of `auto flag <- a in
    # b`) is never a row demand. (1) `lineage_existence_only` moved to
    # projection.py; `row_lineage_arguments` excludes existence args for ANY
    # BuildConceptArgs lineage (not just FILTER), so `satisfiable_outputs`
    # stops pruning the flag (the silent projected-form drop). (2)
    # concept_graph tags `existence_only` graph-structurally (non-sink address
    # consumed only through EXISTENCE edges), so partition_roots gives the RHS
    # its solo existence bucket instead of the shared ROOT (the
    # root<->condition-basic cycle). (3) root.py `_outputs_with_grain_keys`
    # subtracts the existence set from grain/keys expansion (the flag's
    # authored keys include the RHS; the condition-feeder search demanded it
    # as a row output -> disconnected-model FULL JOIN on 1=1 cartesian).
    # Golden SQL 109/109 identical; join_matrix xpass set identical to HEAD.
    # test_predicate_not_pushed_past_window_order_key pruned 2026-07-28 s48:
    # parent-selection MUTUAL ANNIHILATION. The window group (ratio =
    # sales_sum / lead(...) over (partition by store, nwk order by flag))
    # exposes only `r` — its `needed` walk expanded one lineage level
    # (r -> ratio) and never reached the true compute inputs nwk/sales_sum,
    # because ratio and the lead virt are primary members of the SAME group
    # (intermediates), not outputs. With those inputs missing, the aggregate
    # parent and the nwk BASIC parent had EQUAL `provides` sets — the
    # aggregate dropped as the BASIC's graph-ancestor, the BASIC dropped via
    # the grouping-sibling redundant-rescan rule against the aggregate — so
    # `_parent_nodes_for` returned NO parents and the build fell to
    # UnresolvableQueryException. Fix (strategy_builder `needed` loop): the
    # lineage walk now recurses through args that are themselves primary
    # members of the group (their inputs must come from parents); non-primary
    # args stay unwalked (the passthrough rule). The BASIC then provides a
    # strict superset and survives dedup. Goldens 109/109 identical.
    # test_rowset_membership_feeder_scoped_joined_to_own_output_no_recursion
    # pruned 2026-07-27 s46: the bridge-via-a-third-rowset shape now raises the
    # expected typed UnresolvableQueryException instead of planning a silent
    # `FULL JOIN ... on 1=1` cartesian. `_raise_if_rowset_islanded`
    # (strategy_builder) already split the FINAL contributors into components,
    # but the connectivity re-check passed (the authored `subset join weeks.ws
    # = cur.src_ws` + `subset join nxt.nxt_ws = weeks.nxt` DO relate the
    # models) and fell through to the cartesian. It now additionally detects a
    # BRIDGE: a rowset whose members the relation expansion routed into more
    # than one component while no contributor materializes it — the authored
    # axis is realizable only through that rowset's (ws, nxt) pairs, which
    # synthesis does not yet build. Joining through the bridge (the real
    # TPC-DS q02 idiom) remains future work; the error names the bridge and
    # suggests selecting one of its columns.
    # test_tvf_union_order_by_grouped_away_column pruned 2026-07-26 s39: the
    # multiselect/union intercept in `_search_concepts` fired on ANY mandatory
    # concept with (Union)MultiSelectLineage, so an ORDER-BY-carried union
    # column (`local._combined_sort_k`) beside outer aggregates hijacked the
    # whole statement onto the UnionNode (which then stamped label/total_a/
    # total_b as its own outputs and could not render the sort column). The
    # intercept now fires only when the lineage produces EVERY mandatory
    # concept; a demanded raw union output is tagged to its wrapping rowset
    # boundary (concept_graph) and exposed by resolve_rowset.
    # test_case_key pruned 2026-07-28 s48b (WRONG VALUES, masked by the shape
    # assert): `array_to_string(array_agg(launch_filter))` — launch_filter a
    # DERIVED KEY (`key launch_filter <- CASE ... launch_type_code ...`) —
    # aggregated RAW launch rows (one entry per launch) where v3 dedups the
    # key's domain first (one entry per distinct _launch_code per vehicle).
    # Two-part fix: (1) `_aggregate_input_grain` (concept_graph) — a derived
    # KEY argument contributes its defining keys as input grain, not its
    # authored (host-row) grain; (2) `_aggregate_inputs_are_row_preserving`
    # (strategy_builder) — a KEY-purpose content arrives at the scan's line
    # grain exactly like ROOT content (the s43 FILTER-content rule, one
    # purpose-class over), so the input-grain normalization floor fires and
    # the GroupNode dedups (by-keys, arg keys, arg value) below the
    # aggregate. Rows verified equal to v3 on synthetic data; gcat file 36/36
    # green; golden SQL 109/109 identical (no TPC-DS query aggregates a
    # derived key).
    # test_filter_node_group_injection pruned 2026-07-27 s43 (WRONG ROWS, 1288
    # vs 1229): `count(launch_tag ? fuel='Kero' and stage_no in ('0','1'))`
    # off the stage-grain fuel_dashboard_agg summary counted RAW rows — a
    # FILTER argument dodged the all-ROOT input-grain normalization floor in
    # `_aggregate_inputs_are_row_preserving`, though its CONTENT is a raw ROOT
    # column still arriving at the scan's line grain (the q16 over-count, one
    # wrapper deeper). The floor now classifies an arg by its content's
    # derivation (`_row_content_derivation`); and the normalization wrapper
    # widens a parent to materialize an aggregate argument it never output
    # (the filter virtual) so the condition is evaluated BELOW the dedup —
    # re-deriving it above both filtered post-dedup rows and stranded its row
    # inputs (test_parenthetical: missing source for `_launch_code`). Also
    # restored q83's silently-DROPPED three-part `having *_item_present > 0`
    # (the golden had baked the drop in; re-snapshotted after battery
    # verification). gcat file green under v4 both fixes together.
    # test_exact_match_merge_preserves_subgraph_filters pruned 2026-07-28
    # s48b: NOT a correctness gap — v4 filters the aggregate branch
    # (`boston_tree_info.species = 'Oak'`) and INNER-joins the enrichment
    # branch on species; the join equality applies the filter transitively
    # (algebraically identical result — the filtered side carries only Oak
    # rows and species = 'Oak' excludes NULLs on both sides). Assertions
    # dual-conditioned on CONFIG.use_v4_discovery with the boston-branch
    # filter + INNER join as the correctness carriers. Pushing the filter
    # into the enrichment scan too (v3's shape, smaller intermediate) is a
    # transitive-predicate-propagation nicety, deferred to the size/shape round.
    # test_window_clone pruned 2026-07-28 s48: `filtered <- lag nums order by
    # nums` over `nums <- unnest([1,2])` tripped the disconnect pre-gate —
    # a window over a literal-derived value is itself literal-derived, but
    # `_crossjoinable` tested only the concept's own derivation. The new
    # transitive `_literal_derived` walk skips it; the search then plans the
    # v3-identical cross join (rows verified equal).
    # test_forty_six pruned 2026-07-28 s48b: the FINAL merge rendered a
    # redundant (and semantically hazardous — identical output 7-tuples from
    # DIFFERENT customers would wrongly collapse; v3/reference keep both)
    # `GROUP BY 1..7`. `_final_merge_grain`'s ROWSET branch pinned the merge
    # grain to the authored subset-join keys ONLY (the q59 fan-out guard), so
    # the merge read COARSER than its rowset contributor (bought's
    # bought_city/ticket_number grain members dropped) and MergeNode.resolve
    # force-grouped on the failed pregrain check. Fix (group_graph): a
    # remaining rowset grain member that is itself a requested output and
    # hosted by no other FINAL contributor re-enters the merge grain — it
    # cannot add an unauthored join equality (q59's wk pseudonyms stay
    # excluded: not mandatory outputs). Golden drift exactly {44, 46, 68} —
    # same family, all three now render the join un-grouped with a final
    # passthrough projection; all three execution-verified against the
    # TPC-DS reference.
    # test_where_clause_inputs pruned 2026-07-27 s41: the order-dependent gap
    # (ambient wildcard `~date.*` merges from test_merge_comparison broke the
    # later multi-fact query) was the labelable-satisfier stall — first hops
    # only, so a chain intermediate already in the cover stranded the state.
    # `_label_chain_state` (network_obligations) now offers the walk FRONTIER as
    # satisfiers; minimal pair + full battery pass.
    # Scoped/merge join on a DERIVED key (`da <- o.amt+1` joined to `db <-
    # c.cost+1`) leaked INVALID_REFERENCE: the two keys collapse to one canonical
    # merge with a variant per side, and v4's bridge assigned BOTH variants to
    # BOTH datasource scans -- each can only render the variant sourced from its
    # own base column, so the other rendered an unbound column. FIXED + pruned
    # 2026-07-04: `_datasource_renders_derived` (source_planning.py) gates a BASIC
    # merge-key's assignment to a scan on renderability (direct physical/merge-
    # alias binding OR every ROOT lineage leaf bound), so each scan gets only its
    # own side and the merge joins on the equivalence. Covers INNER/LEFT/FULL,
    # 2/3/4-way, and chained equality. Isolation-verified; full v4 sweep +7 xpass,
    # 0 regressions vs baseline.
    # test_empty_top_level_rollup_inherits_build_grain pruned 2026-07-28 s48:
    # `sx <- sum(1)` was stranded by `_aggregate_grain_only_parents` dropping
    # ALL by-key edges of a literal-measure aggregate (see
    # test_aggregate_by_grain_with_derived_of_key above — same fix), and the
    # unnest consts a/b needed the `_literal_derived` crossjoinable extension.
    # Rows identical to v3 (modulo unordered select order).
    # test_circular_aliasing_inverse pruned 2026-07-29 s49 (the alias-merge
    # inverse recomposition: `merge composite_id_alt into composite_id` with
    # `alt <- concat(first, second)` and only first/second datasource-bound —
    # v4 raised UnresolvableQueryException). The graph walk substitutes the
    # unbound key's pseudonym origin (alt), but the origin's args are
    # bound-but-derived (`first <- split(composite_id)`) and their authored
    # lineage walked straight back into the unbound key — a dead-end cycle.
    # Fix: `_materialized_root_addresses` (concept_strategies_v4) also marks
    # the DIRECT args of an unbound bare key's pseudonym origin as
    # materialized-root candidates (each still gated by
    # `_datasource_materializes` + exact-grain), so the walk stops at the
    # physical columns. v4 now plans the single-scan concat recomposition;
    # rows match v3 (`[('123-abc',)]`).
    # test_query_aggregation pruned 2026-07-27 s43: NOT a correctness gap — the
    # unit assert counted v3's input_concepts (revenue + unused order_id grain
    # key); v4 projects only the aggregate argument. Rendered SQL + rows are
    # IDENTICAL under both planners (scratchpad A/B). Assertion dual-conditioned
    # on CONFIG.use_v4_discovery; file passes 9/9 under both legs.
    # 2026-07-13: pruned join_propagation / outer_read_key / left_k_aw /
    # readback_inner_k (renamed intersection_k, now passing) + the LEFT matrix
    # cells — the subordinate coalesced-key readback family is FIXED (nested
    # builds get fresh caches when the body adds scoped joins; coalesced handle
    # content re-exposed on the inner producer; scoped-collapsed keys relate
    # property roots in the ROOT split).
    # ROWSET_XDS_RESIDUAL family (full_k_aw / full_k_bv / left_k_bv / matrix)
    # FIXED + pruned 2026-07-27 s40, three pieces: (1) `_widen_merge_join_keys`
    # (strategy_builder) marks a licensed rowset handle carried onto a
    # non-rowset scan PARTIAL when the scan's own relation member under-covers
    # the handle's domain (declared SUBSET side, or any arm of a coalescing
    # relation) — `_carried_handle_is_partial`; the subset ANCHOR still carries
    # it complete (q35's shape). (2) Leaf-select QDS resolution unions
    # node-level partial stamps (select_node_v2, mirroring the nullable
    # handling) so the marking survives into `partial_concepts` and join
    # inference/dedup see it. (3) `_genuine_partial_stamp`
    # (value_set_join_upgrade) also excludes ROWSET-derivation stamps (the
    # planner's own relation-driven markings, exactly the set (1) mints) —
    # the new handle stamp otherwise read as an authored `~` coverage fact
    # and narrowed the rowset BODY's preserving FULL join to INNER. (ROOT
    # anchor-side stamps stay genuine: an authored `~` binding respelled onto
    # a declared anchor is real coverage evidence — excluding it cost the q02
    # date-spine INNER narrowing, caught by the golden-SQL snapshot.)
    # Plus the matrix's pre-existing full-count crash: an AGGREGATE
    # CollapseSingleParent fold whose aggregate ARGUMENT is a rowset
    # re-exposure the parent does not output now refuses (the argument renders
    # by floating lineage re-derivation, which settled on the unbound one-arm
    # authored key). Full file 27/27 green; join_matrix xpass set identical to
    # HEAD.
    # test_case_where pruned 2026-07-26 s39: `_prune_existence_exclusive_roots`
    # dropped an address from the shared ROOT that was itself a mandatory
    # OUTPUT / outer-WHERE row-arg (category_id: its blank-phase node feeds
    # only the membership filter, so the one-hop existence test called it
    # set-exclusive), leaving `category_id = <const>` with no main-lineage
    # host -> the condition-placement disconnect raise. The prune now protects
    # outputs + outer-WHERE row-args, and the existence-reach test walks the
    # condition subgraph transitively (order_id feeding the filter through the
    # intermediate CASE basic is pruned, restoring the clean category-only
    # main scan).
}
