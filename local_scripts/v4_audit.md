# v4 compatibility audit (last refreshed 2026-06-27, post full-sweep)

This file is the current handoff for v4 discovery work. The authoritative skip list is
`tests/v4_known_failing.py`; reclassify with `python local_scripts/v4_classify.py`
when changing planner behavior. The classifier buckets SIZE separately from SHAPE, runs
each test worst-of-N (`V4_CLASSIFY_REPEATS`, default 3) to hedge transient cross-run
variance in the parallel harness, and **exits non-zero on a label ESCALATION** — an
observed bucket more severe than the `v4_known_failing.py` reason allows (e.g. a CRASH
under `_TPCDS_SIZE`). The classifier runs each test in ISOLATION, which is *pessimistic*:
many entries that fail alone pass in the full suite. So it only re-checks listed tests
and can't confirm a prune by itself — **a full v4 sweep is the parity gate**, and a safe
prune needs the entry passing in isolation AND in-suite:

```bash
TRILOGY_V4_DISCOVERY=1 pytest -m "not adventureworks_execution" -q
```

## Distance to swapping v4 on by default

**Short answer (REVISED 2026-06-28): farther than previously stated. A measurement
audit of the "cosmetic shape-assert" bucket found it was mostly mislabeled — it
contains ONE verified wrong-rows bug, ~7 real verbosity regressions, and ~5
structural/join-semantics diffs. Only ~2 of the 15 were genuinely cosmetic. The
prior "no wrong-rows" claim was an artifact of shape-only tests that never execute.**

### 2026-06-28 measurement audit of the _INLINE/_MODELING bucket

Each entry was generated under both planners (`CONFIG.use_v4_discovery` False/True),
compared on length + JOIN/CTE counts, and where join-type differed, EXECUTED on
synthetic data to diff rows. Findings:

- **WRONG ROWS (1): `test_rowset_alias_name_collision` — FIXED 2026-06-28.** v4 had
  dropped the shared `id` join key and emitted `FULL JOIN ... on 1=1` -> cartesian
  (verified 3 rows -> 27), masked because the test only asserts SQL shape. Fix:
  `resolve_rowset` exposes a plain unfiltered NON-aggregate rowset's grain key, and
  `_final_merge_grain`/`_group_final_grain_contribution` (group_graph) resolve the
  rowset-namespaced key (`buyers_a.id`) through `BuildRowsetItem.content` to the
  shared base (`local.id`) so the FINAL merge INNER-joins on it. Gated to unfiltered
  (a filtered rowset's key is a subset -> would break `rowset_outer_addition`'s
  LEFT-add) and non-aggregate (an aggregate's grain key is a renamed grouping
  column, not a renderable passthrough -> would break the `query-structure` syntax
  example). Rows now match; executing guard at `local_scripts/v4_evals/cases/
  rowset_alias_collision.preql`; shape test dual-conditioned; pruned from
  `v4_known_failing.py`. Full v4 sweep clean (lone residual is an environmental
  snowflake/permission flake on q21, passes in isolation). **Lesson stands: a green
  shape-only sweep is necessary but NOT sufficient — verify rows by executing.**
- **VERBOSITY (7): rows match, v4 materially longer.** `test_select_literal...`
  (117->294, constant not inlined -> cross join), `test_aggregate_filter_uses_having`
  (272->522, +CASE WHEN), `test_bound_conversion_existence_presto` (1022->1249),
  `test_in_subselect_with_inlined_datasource` (source not inlined), `usa_names::
  test_aggregate_filter_anonymous` (+70%), `rowset_arithmetic` (6290->8747, +39%),
  `two_merge` (merge=False 9->11 CTEs). Several are the SAME passthrough-not-inlined
  family as q2.1/q2.2 — fixing that lever likely clears multiple at once.
- **STRUCTURE (5): rows match on consistent data, plan differs.** `tpc_h::adhoc07`
  (INNER->RIGHT/FULL outer, rows VERIFIED MATCH at sf=0.01), `stocks::provider_name`
  (drops join, FULL vs LEFT OUTER+INNER; matches on consistent data, diverges on
  orphan rows), `test_persist_with_where` (persisted source not reused, recomputes
  CASE), `filter_scalar...staging` (sources staged table where v3 doesn't; ROWS
  UNVERIFIED), `nested_greatest` (no group-by CTE for multi_wm; v3-shape guard).
- **Genuinely cosmetic (2): `ncaa::adhoc07`** (+3%, same join) and
  **`test_aggregate_of_aggregate`** (already passes under v4 in isolation).

Reasons in `tests/v4_known_failing.py` were re-bucketed (`_V4_WRONG_ROWS`,
`_V4_VERBOSITY`, `_V4_STRUCTURE`) to stop these reading as cosmetic. NEXT: fix the
wrong-rows bug; verify rows for `filter_scalar...staging`; attack the
passthrough-not-inlined family (overlaps q2.1).

### (Prior, now-superseded framing)
**What's left is plan verbosity on ~8 TPC-DS queries plus a batch of cosmetic
shape-assert tests. Correctness is there.** — FALSE per the audit above; kept for
context.

v4 discovery is still off by default (`CONFIG.use_v4_discovery = False`). As of 2026-06-27
the full sweep is **0 failed** (re-run twice to rule out parallel-harness flake) and the
classifier reports **0 CRASH / 0 escalations**. All the crashes that were open on
2026-06-25 are fixed (existence-source `RecursionError` family, q2.1 union
`BinderException`, `test_filter_constant` invalid-ref). What remains before flipping the
default:

1. **Plan verbosity — NO `_TPCDS_SIZE` tests remain.** q30.alt FIXED + pruned
   2026-06-30 (the last one): a filter-only post-aggregate GA arg is now peeled
   into the single-entity FD dim bucket + sourced in the fresh root projection, so
   `web_returns` scans once (was a second GA-spine scan) — see
   `v4_dimension_projection_rejoin_handoff.md`. **q2.1/q2.2 both
   FIXED + pruned** (the last genuine length regressions): see
   `v4_verbosity_handoff.md` — q2.2 via the window/round merge, q2.1 via the
   navigation-window grain-inference fix that made its named-intermediate round BASIC
   land at `date.week_seq` so the same merge fires.
   - **q10 FIXED + pruned 2026-06-27 (8308 → 6412, under the 7000 ceiling).** Root cause
     was co-bucketed semijoin-RHS buyer-set filters (`pcid in store_buyers` /
     `webcat_buyers`): their defining fact columns (`channel`, `date.year`) sat in the
     shared ROOT and dragged the customer-dimension projection (demographics) onto the
     fact. Fix (this round): isolate each existence-source filter as its own discovery —
     `_prune_existence_exclusive_roots` (group_graph) drops the existence-only roots from
     the shared ROOT, `partition_filters_by_signature` (group_rules) gives each
     semijoin-RHS filter a solo bucket, and `gen_filter` (filter.py) drops the
     pass-through for an `existence_source` filter so its single predicate pushes into a
     real WHERE. The dimension now sources standalone (`customer ⋈ demographics ⋈
     address`), matching v3.
   - **q23 FIXED + pruned 2026-06-27 (8515 → 8107, under the 8500 ceiling).** The q16
     all-ROOT input-grain normalization (a correctness floor) is now skipped when the
     parents already emit one row per input-grain key
     (`_parents_already_at_input_grain` in strategy_builder — the "true parent-row-grain
     signal" the floor deferred to). The q16 floor itself is unchanged: a finer fact-line
     scan still gets regrouped before aggregation.
   - **q73/q81 FIXED + pruned 2026-06-27** (q73 5220→2741, q81 9163→6410): single-entity
     FD dimension-cluster split + condition-aware feeder drop + a PASSTHROUGH-mode
     collapse rerun. See `project_v4_dimension_rejoin_root_cause`.
   - **q94 FIXED + pruned 2026-06-27 (5271 → 3508, under the 5000 ceiling).** Root cause
     was the per-consumer ROOT re-slice in `strategy_builder.parent_for_consumer`:
     a `count(distinct order_number)` aggregate re-derived the entire conditioned
     `web_sales ⋈ ship_address ⋈ ship_date ⋈ web_site` join (the dim joins are pinned by
     the WHERE, so nothing could be pruned) just to read `order_number`. Fix: build the
     narrow slice speculatively but adopt it ONLY when `_leaf_datasource_ids(sliced) <
     _leaf_datasource_ids(node)` (it strictly drops a join); else share the already-built
     ROOT and let column projection narrow it. **Also dropped q10 10208 → 8308** as a free
     side effect (same shared-ROOT shape). Full sweep 0 failed; rows byte-identical.
   - **q2.1 catastrophic blowup FIXED 2026-06-26 (60696 → 10231, −83%).** Was a 9.6× self-
     referential membership-filter blowup (a distinct bug class, not ordinary verbosity);
     now ordinary ~1.4× verbosity (13 CTEs, down from 75). Fix: `_CleanFeederCache` in
     `strategy_builder.py` re-sources a self-referential `IN`-set feeder STANDALONE instead
     of deep-copying the conditioned subtree (which had fired 15015× → 60696 chars). Rows
     unchanged; tpc_ds failure set net-zero; membership family green. Detail in
     `v4_verbosity_handoff.md` "*** q2.1 ***". Still over the 7500 ceiling (8747 real-
     fixture) — the residual is shared passthrough-projection bloat (q2.2 at 8856). Use
     `local_scripts/v4_real_size.py` (real fixture) over `v4_size_compare` (proxy over-reports).
2. **Cosmetic shape-assert tests — 10 `_INLINE` + 5 `_MODELING`.** Rows match, the SQL
   string differs. To flip the default each must be conditioned on
   `CONFIG.use_v4_discovery` or accepted. Mechanical, not risky.

## Correctness: at parity (full sweep 2026-06-27)

Latest full v4 sweep (`TRILOGY_V4_DISCOVERY=1`, all tests minus adventureworks):

**4289 passed, 20 skipped, 6 xfailed, 18 xpassed, 82 errors — 0 failed (8m).**

- **0 failed in the SWEEP, but that is not row-parity.** The sweep being green means
  no *listed-as-passing* test regressed — it does NOT mean v4 rows are correct, because
  several tracked entries assert SQL shape only and never execute. The 2026-06-28 audit
  found `test_rowset_alias_name_collision` returns a cartesian product (wrong rows)
  despite a green sweep. Treat "0 failed" as a regression gate, not a correctness proof.
  The default-planner (v3) sweep is also green (exit 0).
- The **82 errors** are all `tests/engine/test_clickhouse_server.py` — clickhouse.cloud
  connection errors, environmental (no local server). Ignore.
- **18 xpassed / 6 xfailed** are tracked `v4_known_failing.py` entries. NB the full suite
  is *more* favorable than isolation. Prune only entries that pass in BOTH (see below);
  the rest pass only with full-suite ordering and would flip red if run alone.

## 2026-06-30 — self-referential membership CRASH fixed (was an UNTRACKED v4 gap)

`tests/optimization/test_pushdown_optimization.py::test_dual_existence_filter_no_cycle`
(and the single-membership variant) **crashed under v4 in isolation** (`Missing
source map entry for local.a_buyers`) but was NOT in `v4_known_failing.py` — it
only passed via full-suite ordering, so the "0 failed" sweep masked it. A membership
`x in y` whose set `y` is derived from the same scan as the output was injected on
`y`'s own producer group (self-referential → the IN-RHS rendered against a dangling
CTE), because the shared scan is a d1-root excluded from condition candidates.
Three-part fix:

1. **`condition_placement.plan_condition_placements`** — when EVERY candidate for a
   membership atom is itself a membership-set producer, route the atom to FINAL
   (narrow: ordinary TPC-DS `x in <set>` over a separate output aggregate has a real
   consumer candidate and is untouched — verified net-zero on a full sweep).
2. **`_assemble_final_node` / `_apply_final_conditions`** (strategy_builder) — wire
   existence feeders for a FINAL-deferred membership (`_attach_existence_sources`
   runs BEFORE assembly and never saw the FINAL node), via `feeder_cache` threaded in.
3. Re-dedup the conditioned single-contributor result to the output grain (the
   contributor carries an extra grain key only so the IN-set subselect can read it,
   so filtered rows still duplicate at the output grain).

Lock: `test_self_referential_membership_filter` + `test_dual_existence_filter_no_cycle`
(both execute rows, pass under v3 + v4). Full v4 sweep clean (4301 passed, 0 real
failures; lone fail was a live-Gemini flake that passes on retry). **Caveat: the
rowset-cross-datasource isolation failures (`test_rowset_cross_datasource_outer_read`,
`test_scoped_join_rowset_outer_blend`) are ALSO untracked and fail in isolation but
pass in-suite — same masking pattern; verify before flipping the default.**

## 2026-06-30 — q30.alt dimension-rejoin FIXED + pruned (LAST size-ceiling miss)

The only remaining `_TPCDS_SIZE` entry is gone. q30.alt emitted a SECOND
`web_returns` fact scan (`cooperative`) just to apply the post-aggregate GA filter
`billing_customer.address.state = 'GA'` (FD by `billing_customer.id`). v3 folds
that filter into `wakeful` (the `customer ⋈ customer_address` dim scan that also
sources the output dims). Two coupled changes:

1. `_split_root_dimension_clusters` + `_post_aggregate_filter_args` (group_graph):
   also peel a filter-only POST-aggregate (HAVING) arg into the single-entity FD
   dim bucket. Faithful because a HAVING dim arg filters the OUTPUT after
   aggregation (does not perturb the full-population `avg`); the
   `pre_aggregate_filter_args` gate keeps a pre-aggregate WHERE arg on the fact.
2. `_assemble_final_node` is_root block (strategy_builder): source a peeled
   filter-only bucket member in the fresh root projection so
   `_root_atoms_satisfiable_from` keeps its atom and plan_source joins the dim
   table. Restricted to `_members_of(gid)` (a broader version regressed
   q46/q59/q64/global-agg-filter).

Result 6193 → 6112, `web_returns`==1, GROUP BY==2 (matches v3 `wakeful` shape).
Full v4 sweep 4304 passed / 0 failed; q30 + q30.alt pass under v3 + v4; mypy/ruff/
black clean. See `v4_dimension_projection_rejoin_handoff.md`. **No `_TPCDS_SIZE`
entries remain** — the size-ceiling gating work is done.

## 2026-06-30 — `select_literal` verbosity FIXED + pruned

`_fold_constant_parents` (strategy_builder): a constant-only FINAL contributor got
its own CTE + `FULL JOIN on 1=1` (294 vs v3's 117); now a constant folds into a
non-constant sibling's projection (constants render inline anywhere), matching v3.
Pruned `test_select_literal_is_rendered_with_aggregate_projection`.

## Current tracked state (re-bucketed 2026-06-28 — NOT all cosmetic)

`tests/v4_known_failing.py` tracks **7 entries**. Pruned 2026-06-26: q02, q47, q57, q76,
`test_non_nullable_null_guard`. Pruned 2026-06-27: q73, q81, q94, q10, q23. q2.2 pruned
2026-06-28. `rowset_alias` (wrong-rows), q2.1, and `rowset_arithmetic` pruned 2026-06-29.
`select_literal` (constant fold), q30.alt (dimension-rejoin GA-spine), both `_INLINE`
entries (ncaa::adhoc07 dual-conditioned; aggregate_of_aggregate stale), and
`aggregate_filter_uses_having` (filter-item CASE-WHEN-after-HAVING-pushdown) pruned
2026-06-30. `in_subselect_with_inlined_datasource` (existence-source pushdown/inline
interaction — was a correctness bug, not verbosity) and `two_merge_aggregate`
(passthrough-after-pushdown residue, merge_aggregate=False branch) pruned 2026-07-01.
The remaining 7 — NO size-ceiling and NO cosmetic entries left, all VERBOSITY or STRUCTURE:

| bucket | count | meaning |
| --- | --- | --- |
| `_V4_VERBOSITY` | 2 | rows match, v4 materially longer (measured). Real regressions. |
| `_V4_STRUCTURE` | 3 | rows match on consistent data; join-type/source/shape differs. |
| `_INLINE` | 0 | both pruned 2026-06-30 (ncaa dual-conditioned; aggregate_of_aggregate stale). |
| `_CRASH_INVALID_REF` | 0 | (unused — reserved string; no entries reference it). |
| `_TPCDS_SIZE` | 0 | q30.alt pruned 2026-06-30 — **no size-ceiling entries remain**. |

The `rowset_alias` wrong-rows correctness bug is FIXED (cartesian product → INNER
join on the shared grain key; rows verified 3=3). The last genuine **length**
regression (q2.1) and the last size-ceiling miss (q30.alt) are FIXED. Open themes
are now exactly two: (1) the remaining `_V4_VERBOSITY` family (2 left:
`bound_conversion_existence_presto` — partially improved 2026-07-01 (grand-total
all_rows join now `on 1=1`; residual is the derive-in-scan `wakeful` layer) — and
`aggregate_filter_anonymous` — a harder conditional-aggregate HAVING shape), and
(2) `_V4_STRUCTURE` join/source diffs to verify for row impact.

### 2026-07-01 — filter_scalar staging: ROWS VERIFIED, test conditioned + pruned (_V4_STRUCTURE)

`test_filter_scalar_aggregate_not_restricted_by_staging`: executed all 4
permutations under v4 — rows CORRECT everywhere. The filter-scalar `avg(price) by
category` ranges over the full items table in v4 (sale_count == 1, not 2 — a
2023-restricted avg would give 2), so the bug the test guards is absent. v4 also
sources the OUTER scan from the pre-joined staging table (its `non_partial_for`
matches the outer `sale_year=2023`), which is equivalent + correct; and v4
correctly AVOIDS staging in permutation 3 (no year filter → staging incomplete).
The over-broad P2 `staged not in sql` shape assertion (conflated "avg restricted"
with "staging used at all") is now gated on the v3 planner; the `sale_count` row
check is the real guard and holds under both. Not a v4 bug — a valid alternative
plan. Test-only change (no source), pruned.

### 2026-07-01 — persist-with-where partial-source reuse FIXED + pruned (_V4_STRUCTURE)

`test_persist_with_where`: v4 recomputed a `CASE` from the base table instead of
reading the persisted `upper_name` datasource. The persist
(`... from select test_upper_case_2 where category_id = 1`) stores only the
derived column (no `category_id`), so `_datasource_materializes` ->
`_conditions_supported` rejected it (can't re-express `category_id = 1`), even
though its `non_partial_for = category_id = 1` already bakes in that exact
population. Fix: `_datasource_materializes` (concept_strategies_v4, v4-only) skips
`_conditions_supported` when the query `where` and the ds `non_partial_for` are
mutually implied (population == desired rows). v4 now reads `upper_name` (no CASE,
no `category_id`), matching v3. Full v4 sweep clean; v4-only change.

### 2026-07-01 — two_merge (merge_aggregate=False) passthrough residue FIXED + pruned

`test_two_merge_aggregate_compacts_inline_window_query` merge_aggregate=False was
v4 11 vs v3 9 CTEs (the merge_aggregate=True branch always passed, 5==5). v4
models the semijoin membership (`date.week_seq in relevent_week_seq`) as a JOIN
node; predicate pushdown relocates it to the fact scan's WHERE subselect, leaving
a bare single-parent passthrough SELECT (v3 applies the filter in-scan, never
materializing the node). `CollapseSingleParent` PASSTHROUGH mode would fold it,
but the whole rule is gated on `merge_aggregate` (off in this test), and the
planner's `_elide_passthrough_tree` runs pre-pushdown when the node still has a
real join. Fix: `passthrough_only` param on `CollapseSingleParent` + a new
`collapse_single_parent.passthrough_after_pushdown` plan phase (the `elif not
merge_aggregate` branch of `.after_pushdown`). Near-zero blast radius — only this
one test sets merge_aggregate=False; the default path is unchanged. v4 now 9==v3.

### 2026-07-01 — grand-total (`by *`) aggregate now cross-joins ON 1=1 (partial: bound_conversion)

v4 was materializing the abstract `__preql_internal.all_rows` grand-total marker
as a REAL column: the input scan emitted `1 as __preql_internal_all_rows`, the
aggregate CTE emitted it too, and the consumer joined `INNER JOIN ... on
all_rows = all_rows` instead of v3's `on 1=1`. Root cause: `_upstream_aggregate`
(concept_graph) demands the aggregate's `by` concepts via
`BuildAggregateWrapper.concept_arguments = function.concept_arguments + self.by`,
which includes `all_rows` — v3 explicitly strips `ALL_ROWS_CONCEPT` in
`gen_group_node`. Fix: `_upstream_aggregate` drops `ALL_ROWS_CONCEPT` from the
demanded concepts (v4-only; provably equivalent since all_rows=1 on both sides ⟺
1=1). `bound_conversion_existence_presto` 1249→1094; constant column gone, join
now `on 1=1`. Broad win for ANY grand-total aggregate join. Full v4 sweep 4309/0.
Regression-locked (this is the SECOND time this sourcing regressed):
`test_grand_total_aggregate_cross_joins_not_all_rows` (test_bound_conversion_
existence.py) asserts no `__preql_internal_all_rows` column + `on 1=1`,
parametrized over BOTH planners; fails on the pre-fix v4 tree, passes after.

**NOT pruned:** the entry asserts a SQL fragment referencing
`"quizzical"."date_converted"`, but v4 still differs from v3 by an extra `wakeful`
grouping CTE + a `highfalutin` projection layer — v4 keeps the base key
(`date_string`) and RE-DERIVES `date_converted` rather than computing the cast in
the scan projection like v3. That residual is the separate passthrough/derive-in-
scan family (the hard residual), untouched here.

### 2026-07-01 — in_subselect existence-source pushdown/inline bug FIXED

`test_in_subselect_with_inlined_datasource` was mis-bucketed as verbosity — it was
a CORRECTNESS bug (invalid SQL). For `where it.price > 5 and it.item_id in
cs.item_id`, v4 applied the membership TWICE: on the items ROOT (referencing a
DANGLING `cs_catalog_sales` CTE = the un-inlined `cs_item_id` alias) AND on the
FINAL node (correctly inlined). Root cause is a SHARED optimizer-ordering bug:
`inline_datasource` runs BEFORE `predicate_pushdown` and folds the membership's
subselect source into the child; pushdown then tries to promote that membership
onto the child's parent, but the inlined source has no dependency CTE to re-hang —
so it writes a `source_map` entry with no backing dependency (dangling) and the
child copy isn't cleanly removed. The raw planner and pushdown-alone are both
correct; only the combination breaks. Fix: `_check_parent` (predicate_pushdown)
vetoes the push when an existence source is inlined into the child (a `source_map`
entry with no promotable dependency). v4 now applies the membership once, inlined,
valid. Shared optimizer; v3 sweep 4317/0 and v4 sweep 4308/0 both clean.

### 2026-06-30 — aggregate_filter HAVING CASE-WHEN verbosity FIXED

`filter X where count(...) > N` rendered a redundant `CASE WHEN count > N THEN X
ELSE NULL` in v4 (522 vs v3's 272). Root cause is shared, not v4-specific:
`_push_having_into_group_parent` (predicate_pushdown) relocates the aggregate
predicate into the group parent's HAVING and STRIPS the copy from the filter
consumer, so `cte.condition` no longer names it and the renderer's existing
bare-render check (base.py:1046, keyed on `cte.condition`) missed it — reverting
the filter-item to CASE WHEN. Fix: `_filter_guaranteed_by_sole_parent` (base.py)
also renders content bare when the CTE's SOLE (non-join, can't NULL-pad) parent's
condition implies the filter's where — exactly the pushdown's own safety
invariant. v4 522→442 (bare `order_id`, HAVING kept). Shared renderer change; v3
sweep 4317/0 and v4 sweep 4307/0 both clean.

## Size / verbosity analysis (2026-06-25)

### Core diagnostic tool: `local_scripts/discovery_v4.py`

Use `discovery_v4.py` as the primary graph/strategy diagnostic harness before making
size fixes. It already renders the v4 concept graph, merged group graph, and final
reordered group graph; the `--diagnostics` mode also writes searchable sidecars for
the planner contracts and materialized strategy tree.

```bash
.venv/Scripts/python.exe local_scripts/discovery_v4.py --query 81 --diagnostics --diagnostics-dir local_scripts/v4_diagnostics --no-sql
.venv/Scripts/python.exe local_scripts/discovery_v4.py --query query30-alt.preql --diagnostics --diagnostics-dir local_scripts/v4_diagnostics --no-sql
```

Outputs:

- `local_scripts/<stem>.png` - concept graph.
- `local_scripts/<stem>_groups.png` - merged group graph.
- `local_scripts/<stem>_groups_reordered.png` - final group graph after reordering.
- `local_scripts/v4_diagnostics/<stem>_diagnostics.json` - machine-readable concepts,
  groups, edges, contracts, and strategy nodes.
- `local_scripts/v4_diagnostics/<stem>_groups.md` - group attrs, IO, edge kinds,
  input contracts, and FINAL contract.
- `local_scripts/v4_diagnostics/<stem>_groups_merged.md` - same for the merged group
  graph.
- `local_scripts/v4_diagnostics/<stem>_strategy.md` - materialized `StrategyNode`
  tree with datasource choices and repeated parent reuse marked.

Read these before eyeballing SQL. Fast loop:

1. Inspect `__final__` in `<stem>_groups.md` to see merge grain and contributor
   contracts.
2. Search `<stem>_strategy.md` for repeated datasources or unexpected fact tables.
3. Use `<stem>_diagnostics.json` for scripted summaries when comparing two queries
   or checking whether a fix changed contracts vs only physical assembly.

Goal: v4 should produce **equal-or-less** verbose SQL than v3. The `_TPCDS_SIZE`
entries are queries where v4's rows are correct but the SQL is longer (more CTEs /
less compact) than v3's, tripping the per-test `assert len(query) < ceiling`.

Measure the v3-vs-v4 *relative* gap with `python -m local_scripts.v4_size_compare`
(generation-only). **Caveat:** that harness builds a minimal `Environment` with no
DB attached, so it does NOT apply datasource inlining — its absolute lengths run
higher than the real tests, which use the `engine` fixture (DB imported, small
sources inlined; `test_two` even asserts the raw `"memory"."store_sales"` is gone).
So a query can read "over ceiling" in the proxy yet PASS its actual test (q23, q94).
Trust the `test` column for pass/fail; use the proxy lengths only for v3-vs-v4 deltas.

### Measurements 

Trust real-fixture numbers from
`local_scripts/v4_real_size.py` instead. **Current real-fixture v3/v4 (2026-06-27),
`OVER` = fails its test:**

| q | ceiling | v3 | v4 | status |
| --- | ---: | ---: | ---: | :--- |
| 10 | 7000 | 6420 | 6412 | PASS (pruned) |
| 2.1 | 7500 | 6290 | 7276 | PASS (pruned 2026-06-29) |
| 2.2 | 7500 | 6290 | 7276 | PASS (pruned) |
| 30.alt | 12000 | 7152 | 6112 | PASS (pruned 2026-06-30) |
| 73 | 3000 | 2701 | 2741 | PASS (pruned) |
| 81 | 8000 | 7465 | 6410 | PASS (pruned) |
| 23 | 8500 | 8037 | 8107 | PASS (pruned) |
| 94 | 5000 | 3452 | 3153 | PASS (pruned) |

**Genuinely failing now (0):** q30.alt FIXED 2026-06-30 (6193 → 6112,
`web_returns`==1, GROUP BY==2): the second `web_returns` GA-spine scan for the
filter-only post-aggregate `address.state` filter is eliminated — the HAVING arg
is peeled into the single-entity FD dim bucket and sourced in the fresh root
projection, matching v3's `wakeful`. See
`v4_dimension_projection_rejoin_handoff.md`. q2.1 FIXED 2026-06-29 (8747 →
7276): the named `*_sales` intermediate made the round() BASIC infer date.id
grain (the window's `order by date.week_seq` flattened up as a grain parent and
descended to its key), skipping the same-grain window merge that fixed q2.2.
Three-part fix — (1) navigation-window order-by excluded from a wrapping
expression's grain inference (`_get_relevant_parent_concepts` in parsing/common +
`_row_grain_concept_refs` in author), (2) subset-nest cycle guard
(`_feeds_extra_signature_group` in group_rules) so the `*_sales` window-feeder
and `*_increase` window-consumer don't merge into one cycling bucket, (3)
partial-spine window absorb in `_merge_basic_into_window_parent`. Pruned q2.1 +
rowset_arithmetic (same family). Both full sweeps clean.


### Remaining size shapes — next targets (updated 2026-06-30)

**ALL size-ceiling tests are fixed and pruned** (q47, q57, q73, q81, q10, q23,
q94, q2.1, q2.2, and now q30.alt). The passthrough/window family (q2.1/q2.2) and
the dimension-rejoin family (q73/q81/q10/q94/q30.alt) are both closed. No
`_TPCDS_SIZE` entries remain. Next gating work for the default flip is the
`_V4_VERBOSITY` (4) + `_V4_STRUCTURE` (5) non-benchmark entries and conditioning
the cosmetic shape-asserts on `CONFIG.use_v4_discovery`.

## Phase boundary contract

The intended v4 path is:

1. Source concept demand: `concept_graph.py` walks mandatory output concepts and
   condition inputs back to root concepts, preserving row-vs-existence dataflow
   with typed edges. This phase should not pick concrete datasources or build
   `StrategyNode`s.
2. Group concepts: `group_graph.py` applies per-derivation grouping rules,
   injects conditions at groups, computes group IO, and appends the FINAL sink.
   This phase should decide which concepts can be sourced together, but should
   not render or materialize query nodes.
3. Materialize groups: `strategy_builder.py` walks the group DAG and dispatches
   each group to `v4_node_generators`. ROOT groups call `source_planning.py` for
   datasource selection, bridge planning, partial completion, and pinned rollup
   sourcing. This phase satisfies declared contracts; it should not infer
   projection grain, join keys, or final contributor contracts from physical
   sibling shape.
4. Zip final query: `_assemble_final_node` merges the minimum built contributors
   that cover the mandatory outputs, applies final-only filters, hides join keys
   that were carried only for assembly, and dedups to the requested output grain.
   Optimization happens after this planner returns a normal strategy node.

## Separation audit notes

The boundary is now enforced in code rather than only described here:

- `FinalAssemblyContract` and `FinalContributorContract` are Stage 2 outputs.
  Stage 3 requires them and no longer refreshes or synthesizes missing final
  contracts.
- `GroupInputContract` is the only source for parent projection grain and
  per-group bridge join keys. `_satisfy_parent_projection_contract` physically
  satisfies that grain, but no longer falls back to group grain or shaped sibling
  outputs.
- `_widen_merge_join_keys` only widens for declared join key addresses. It no
  longer scans sibling outputs for "key-ish" concepts.
- FINAL rowset joins declare the row-stream lineage key (for example
  `local.order_id`) in Stage 2, rather than relying on Stage 3 to infer it from
  nullable rowset aliases.
- `_fold_passthrough_parents` is pinned as a physical redundancy optimization:
  it can absorb row-preserving projections, but cannot dissolve row-shape
  barriers.
- Sole-contributor FINAL hiding is pinned as output hygiene: it hides
  non-mandatory carried grain keys only at the FINAL layer.
- `_group_to_grain_if_required` is pinned to `FinalAssemblyContract`; it may
  skip or perform physical deduping, but does not choose the logical output
  grain.

Still-watch areas:

- `source_planning.py` is the right home for concrete datasource selection. Its
  bridge and partial-completion helpers are sourcing concerns, not grouping
  concerns.
- `_regraft_group_sources` in `group_graph.py` is topology-only: it may add a
  better existing parent edge or a synthetic dimension ROOT bucket, but it must
  not call `plan_source`, inspect datasources, or build `StrategyNode`s.
- Existence edges must stay side-channel-only. They order subselect sources but
  should not be treated as row-stream JOIN parents.
- Rowset, recursive, aggregate, window, and group-to concepts remain row-shape
  barriers. Fixes should preserve them as materialized boundaries rather than
  decomposing their roots into another phase.

## Next cleanup loop

1. Size/shape (the gating work): **DONE** — all `_TPCDS_SIZE` entries fixed and
   pruned (q2.1/q2.2 passthrough-folding + inliner, q30.alt dimension-rejoin).
   Remaining verbosity/structure work is the non-benchmark `_V4_VERBOSITY` (4) +
   `_V4_STRUCTURE` (5) entries.
2. Condition the `_INLINE`/`_MODELING` shape-assert tests on `CONFIG.use_v4_discovery`
   so they pass under both planners (prerequisite for flipping the default).
3. Re-run `local_scripts/v4_classify.py` after any planner change, and a full v4
   sweep before claiming parity (the classifier only re-checks listed tests, so it is
   blind to regressions in newly added tests).
4. Keep new Stage 3 heuristics behind contract-driven tests: if materialization needs
   a key or projection grain, Stage 2 should declare it first.
