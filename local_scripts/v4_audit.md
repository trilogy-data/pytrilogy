# v4 compatibility audit (last refreshed 2026-06-21, post-geography-fix)

This file is the current handoff for v4 discovery work. Long fixed-case notes
from the 2026-06-11 through 2026-06-20 sweeps were removed because they were
stale and obscured the remaining work. The authoritative skip list is
`tests/v4_known_failing.py`; reclassify with `python local_scripts/v4_classify.py`
when changing planner behavior.

## Current tracked state

`tests/v4_known_failing.py` currently tracks 44 non-strict xfail entries:

| bucket | n | meaning |
| --- | ---: | --- |
| INLINE | 11 | SQL/CTE shape differs from v3, rows expected to match |
| RESULT | 2 | known wrong-row regressions |
| MODELING | 13 | modeling sweep diffs still needing per-test classification |
| TPCDS_SIZE | 18 | TPC-DS rows match, SQL exceeds v3-tuned size ceilings |

The four "high-priority unclassified" entries from the prior audit
(`test_composite_rollup_aggregate_keeps_group_by`, both geography
`test_exact_match_resolution` / `test_exact_match_with_parenthetical_extra_filter`,
and `test_sixty_eight`) are now fixed and removed from the list.

### Latest classifier run (2026-06-21, isolation, `--runxfail`)

`python local_scripts/v4_classify.py` over the 44 entries: **38 SHAPE / 4 XPASS /
2 ROWS**. (The ROWS/SHAPE split is a crude heuristic — `DebuggingHook` prints SQL
to stderr, so the two "ROWS" hits, `test_select_literal_is_rendered_in_projection`
and `test_aggregate_filter_uses_having`, are actually SQL-shape asserts and stay
in INLINE.)

XPASS-in-isolation, promote candidates (re-confirm in a full-suite run before
removing — the list is non-strict precisely because of cross-test state leakage):

- `tests/modeling/test_complex.py::test_in_select`
- `tests/modeling/tpc_ds_duckdb/test_queries.py::test_sixty_nine`
- `tests/modeling/tpc_ds_duckdb/test_queries.py::test_ninety_seven_one`
- `tests/modeling/tpc_ds_duckdb/test_queries.py::test_ninety_seven_two`

### Known result-regression (`_RESULT`) entries

- `tests/modeling/tpc_ds_duckdb/test_queries.py::test_ninety_seven_two` —
  **now passes in isolation** (rows correct). Promote/remove pending a full-suite
  confirmation; no longer a genuine wrong-rows case.
- `tests/stdlib/test_report.py::test_top_x_by_metric` — **still wrong**
  (`Decimal('10.0') == 6.0`). Distilled in
  `local_scripts/v4_evals/failing_cases/top_x_by_metric.preql`. Root cause below.

#### `test_top_x_by_metric`: FINAL cross-join drops the merge join key

`@top_x_by_metric(order, sum(amount), 1, -1)` expands to
`CASE WHEN rank(order) over (order by sum(amount) desc) < 2 THEN order ELSE -1 END`
(a BASIC concept, grain `order`, over a WINDOW over a per-order AGGREGATE). The
outer select is `top_orders, sum(amount) by top_orders`.

- **v3** keeps the CASE *in the FINAL select* and joins the window CTE (`wakeful`,
  carries `order`) to the raw scan (`quizzical`, carries `order`) `ON order`, then
  groups — correct (top bucket = order 3's own 6.0).
- **v4** materializes the BASIC CASE into its own CTE (`thoughtful`) that projects
  **only `top_orders`**, dropping the `order` lineage/grain key. The FINAL
  `sum(amount) by top_orders` then has no shared key and degrades to
  `quizzical FULL JOIN thoughtful ON 1=1` — every amount row pairs with every
  bucket, so the top bucket sums the global 10.0.

This is the same class as the 7de267f6 "Declare merge join keys before
materialization" fix, but for a non-grouping contributor: `_final_merge_grain`
(group_graph.py) only contributes grain for `GROUPING_DERIVATIONS` and `ROWSET`,
so the BASIC `top_orders` (grain `order`, derived from a row-shape barrier) never
declares `order` as a merge join key, and its CTE isn't asked to carry it. Fix
direction: extend the merge-grain / join-key declaration so a contributor
exposing a row-barrier-derived concept declares that concept's grain key, OR fold
the BASIC CASE into the FINAL group as v3 does. Touching the shared merge logic is
delicate (see memory on prior regressions) — verify against q97/q11/forced-join
family before landing.

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

1. Confirm the 4 XPASS candidates above in a full-suite run, then remove them
   (start with `test_ninety_seven_two`, which clears the last genuine wrong-rows
   case besides `top_x_by_metric`).
2. Fix `test_top_x_by_metric` — the sole remaining genuine wrong-rows regression
   (FINAL `ON 1=1` cross-join; see diagnosis above).
3. Split the 13 `_MODELING` entries into `_RESULT`, `_INLINE`, or `_TPCDS_SIZE`.
4. Re-run `local_scripts/v4_classify.py` after any planner change.
