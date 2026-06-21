# v4 compatibility audit (last refreshed 2026-06-21, post-window-rollup-fix)

This file is the current handoff for v4 discovery work. Long fixed-case notes
from the 2026-06-11 through 2026-06-20 sweeps were removed because they were
stale and obscured the remaining work. The authoritative skip list is
`tests/v4_known_failing.py`; reclassify with `python local_scripts/v4_classify.py`
when changing planner behavior.

## 2026-06-21 follow-up: three correctness fixes (all v4-only)

The prior "no remaining wrong-rows" claim was wrong on two counts and the
inherited (then-uncommitted) `top_x` bridge fix had regressed two tests. All
three are now fixed in `v4_helper/` (v3 untouched):

1. **Constant-sibling drops the WHERE** (`test_select_literal_is_rendered_in_projection`
   was mis-bucketed INLINE/SHAPE but was wrong-rows). `where x = 1 select x, 'abc'`
   returned both rows: in the multi-contributor FINAL merge,
   `_fresh_final_root_projection` re-sourced the root scan with `conditions=None`,
   dropping the root group's own WHERE, then the constant CTE `1=1`-joined the
   unfiltered scan. Fix: thread `_wrap_atoms(_atoms_at(attrs, gid))` into the
   fresh re-source (`strategy_builder.py`). The remaining `:label`-in-separate-CTE
   diff is a genuine SHAPE diff (v3 inlines the constant); the entry stays INLINE.
2. **Window-over-rollup fan-out** (`test_window_over_rollup_preserves_grouping_rows`,
   untracked, v3-passing, hash-seed-flaky so the full suite "got lucky"). A
   `rank() over (partition by g1)` over a `sum() by rollup g1, g2` carried only
   the partition key `g1`, so the FINAL merge joined the window CTE back to the
   dims on `g1` alone and fanned out the ROLLUP subtotal rows (13 rows vs 6).
   Root cause: a WINDOW is a grouping derivation, so `_apply_grouping_parent_grain_overrides`
   skipped it, leaving its grain at the partition key. New
   `_widen_window_grain_to_grouping_parent` (`group_graph.py`) widens a WINDOW's
   grain to its (finer) grouping parent's grain — a window runs pointwise and
   preserves every parent row — so it carries the full join key and folds into
   the rollup CTE (no join-back).
3. **`top_x` bridge over-declaration** regressed `gcat::test_parenthetical` and
   `tpc_h::test_twenty_two` ("Invalid reference"/"no cover"), while a too-narrow
   first cut regressed `complex::test_bound_conversion_existence` (fan-out, 4 vs 3
   rows). `_shared_row_parent_join_keys` originally declared a preserve_key for
   ANY consumer with ≥2 row parents sharing a KEY output, which over-fired on
   plain projections of a common scan. Final discriminator: a shared key is a
   bridge only when it is BOTH a **lineage ancestor of the consumer's own
   grouping grain** (the column the group key is derived from — `top_orders<-order`,
   `date_converted<-date_string`) AND **not held by any parent as that parent's
   own grain** (q22's `customer.id` is the `cntrycode` projection's grain; folds).
   A key unrelated to the grouping grain (parenthetical's `vehicle.name`/`launch_tag`)
   is co-carried by foldable projections, not a bridge. `_refresh_input_contracts`
   now threads `concept_edges` for the lineage walk. The unit test was updated to
   wire the `order -> top_orders` lineage the discriminator now requires.

## Current tracked state

`tests/v4_known_failing.py` currently tracks 43 non-strict xfail entries:

| bucket | n | meaning |
| --- | ---: | --- |
| INLINE | 11 | SQL/CTE shape differs from v3, rows expected to match |
| RESULT | 1 | known wrong-row regression (test_ninety_seven_two; now xpasses) |
| MODELING | 13 | modeling sweep diffs still needing per-test classification |
| TPCDS_SIZE | 18 | TPC-DS rows match, SQL exceeds v3-tuned size ceilings |

The four "high-priority unclassified" entries from the prior audit
(`test_composite_rollup_aggregate_keeps_group_by`, both geography
`test_exact_match_resolution` / `test_exact_match_with_parenthetical_extra_filter`,
and `test_sixty_eight`) are now fixed and removed from the list. `test_top_x_by_metric`
is fixed and promoted to `v4_evals/cases/` (see below). Full v4 suite (no
adventureworks, clickhouse ignored), after the three 2026-06-21 fixes below:
**4022 passed, 4 xfailed, 40 xpassed, 0 failed**.

Two of those fixes were genuine wrong-rows bugs the prior "no remaining
wrong-rows" line had missed: `test_select_literal_is_rendered_in_projection`
(mis-bucketed INLINE; the classifier's "ROWS" tag was right) and the untracked,
hash-seed-flaky `test_window_over_rollup_preserves_grouping_rows`. The third was
fallout from the (then-uncommitted) `top_x` bridge fix. See the section above.

### Latest classifier run (2026-06-21, isolation, `--runxfail`)

`python local_scripts/v4_classify.py` over the then-44 entries: **38 SHAPE /
4 XPASS / 2 ROWS** (`top_x_by_metric` then classified SHAPE, but it was genuinely
wrong-rows — see below — now fixed and removed). The ROWS/SHAPE split is a crude
heuristic: `DebuggingHook` prints SQL to stderr, so the two "ROWS" hits,
`test_select_literal_is_rendered_in_projection` and
`test_aggregate_filter_uses_having`, are actually SQL-shape asserts and stay in
INLINE.

XPASS-in-isolation, promote candidates (re-confirm in a full-suite run before
removing — the list is non-strict precisely because of cross-test state leakage):

- `tests/modeling/test_complex.py::test_in_select`
- `tests/modeling/tpc_ds_duckdb/test_queries.py::test_sixty_nine`
- `tests/modeling/tpc_ds_duckdb/test_queries.py::test_ninety_seven_one`
- `tests/modeling/tpc_ds_duckdb/test_queries.py::test_ninety_seven_two`

### Result-regression (`_RESULT`) entries

- `tests/modeling/tpc_ds_duckdb/test_queries.py::test_ninety_seven_two` — **now
  passes** (xpasses in isolation and in the full suite); rows correct. Last
  `_RESULT` entry; safe to remove on the next cleanup pass.
- `tests/stdlib/test_report.py::test_top_x_by_metric` — **FIXED** (2026-06-21).
  Promoted to `local_scripts/v4_evals/cases/top_x_by_metric.preql`; removed from
  the known-failing list. Root cause + fix below.

#### `test_top_x_by_metric`: parent merge dropped the bridge join key (FIXED)

`@top_x_by_metric(order, sum(amount), 1, -1)` expands to
`CASE WHEN rank(order) over (order by sum(amount) desc) < 2 THEN order ELSE -1 END`
(a BASIC concept, grain `order`, over a WINDOW over a per-order AGGREGATE). The
outer `sum(amount) by top_orders` aggregate merges two row parents — the fact scan
(`amount`, carries `order`) and the window-derived `top_orders` dimension (carries
`order`). They must join on the shared `order` key; v4 instead cross-joined
(`FULL JOIN ON 1=1`), so the top bucket summed the global 10.0 instead of order 3's
own 6.0.

Two-part fix, decision in **group planning** (the input contract):

1. `group_graph.py` — `_shared_row_parent_join_keys` + `_refresh_input_contracts`:
   when a joining consumer (AGGREGATE / WINDOW / GROUP_TO / BASIC) has ≥2 row-stream
   parents that share a KEY-purpose output, that key is declared in the
   `GroupInputContract.preserve_keys` as the bridge join key. (Parallels the
   7de267f6 ROWSET/`_final_merge_grain` work, for a non-grouping row parent.)
2. `strategy_builder.py` — the materializer honors the contract by pulling the
   *extra* bridge keys (those not already in the group's grain/outputs) into
   `needed` before sourcing parents, so the root slice keeps `order`. The
   "extra-only" guard is essential: adding a key already in the grain re-forces a
   `by rollup` grouping key into the SELECT outside its GROUP BY (regressed
   `aligned-multi-select` before the guard was added).

v4 now emits `... INNER JOIN quizzical ON cheerful.order = quizzical.order`
(matching v3's shape). Both changes are v4-only (`v4_helper/`); v3 untouched.
Locked by `tests/core/processing/test_v4_group_behaviors.py::test_input_contract_declares_shared_row_parent_join_key`
and the promoted parity case.

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
   sourcing. This phase may project/dedup parents to preserve grain, but should
   not re-partition the conceptual groups.
4. Zip final query: `_assemble_final_node` merges the minimum built contributors
   that cover the mandatory outputs, applies final-only filters, hides join keys
   that were carried only for assembly, and dedups to the requested output grain.
   Optimization happens after this planner returns a normal strategy node.

## Separation audit notes

No obvious phase-breaking runtime issue was found in the current code read. The
notable sharp edges are:

- `source_planning.py` is the right home for concrete datasource selection. Its
  bridge and partial-completion helpers are sourcing concerns, not grouping
  concerns.
- `_regraft_group_sources` in `group_graph.py` is topology-only: it may add a
  better existing parent edge or a synthetic dimension ROOT bucket, but it must
  not call `plan_source`, inspect datasources, or build `StrategyNode`s.
- `_project_dimension_parents_to_group_grain`, `_wrap_for_grain`, and
  `_group_to_grain_if_required` in `strategy_builder.py` are materialization
  safeguards. They protect cardinality while assembling nodes; they should not
  become alternative grouping rules.
- Existence edges must stay side-channel-only. They order subselect sources but
  should not be treated as row-stream JOIN parents.
- Rowset, recursive, aggregate, window, and group-to concepts remain row-shape
  barriers. Fixes should preserve them as materialized boundaries rather than
  decomposing their roots into another phase.

## Next cleanup loop

With `top_x_by_metric` fixed, there are **no remaining genuine wrong-rows
regressions**; the rest of the list is SHAPE/SIZE plus pollution-flaky xpasses.

1. Remove the 4 XPASS candidates above (all confirmed xpassing in the latest
   full-suite run): `test_in_select`, `test_sixty_nine`, `test_ninety_seven_one`,
   `test_ninety_seven_two`. That empties the `_RESULT` bucket entirely.
2. Split the 13 `_MODELING` entries into `_INLINE` or `_TPCDS_SIZE` (all are
   SHAPE per the classifier — no rows diffs left).
3. Re-run `local_scripts/v4_classify.py` after any planner change.
