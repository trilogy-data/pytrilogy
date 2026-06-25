# v4 compatibility audit (last refreshed 2026-06-25, post full-sweep)

This file is the current handoff for v4 discovery work. The authoritative skip list is
`tests/v4_known_failing.py`; reclassify with `python local_scripts/v4_classify.py`
when changing planner behavior. The classifier only re-checks tests already on the
skip list, so it is blind to regressions in tests added since the list was last
curated — **a fresh full v4 sweep is the only way to confirm parity** after any new
PR. Run it, don't trust the classifier alone:

```bash
TRILOGY_V4_DISCOVERY=1 pytest -m "not adventureworks_execution" -q
```

## Correctness: at parity (full sweep 2026-06-25)

Latest full v4 sweep (`TRILOGY_V4_DISCOVERY=1`, all tests minus adventureworks):

**4153 passed, 20 skipped, 5 xfailed, 27 xpassed, 82 errors — 0 failed (exit 0).**

- **0 failed** — no untracked wrong-rows / crash / invalid-render regressions, including
  across #586/#587/#588 ("V4 Parity" PRs) and the two size fixes below (row-preserving
  inline + join-stream sharing).
- The **82 errors** are all `tests/engine/test_clickhouse_server.py` — clickhouse.cloud
  connection errors, environmental (no local server). Ignore.
- **xfailed / xpassed** are the tracked `v4_known_failing.py` entries. The list is
  non-strict so an entry that now passes shows as xpassed and keeps the gate green
  rather than flipping it red; confirmed-passing entries are pruned (see the cleanup
  loop). To prune one, verify it passes in ISOLATION (`pytest <nodeid>` with the env
  var), not just in the full suite.

v4 discovery remains off by default (`CONFIG.use_v4_discovery = False`); this is
migration-gating work, not a live-path regression surface.

### Recently closed (were the last open correctness items)

- **disconnected-component cross-join semantics** — all 13 tests pass under v4 and v3;
  the `_DISCONNECTED` bucket is removed from the skip list. Handoff:
  `local_scripts/v4_disconnected_handoff.md` (COMPLETE 13/13).
- **`membership in having` over UNION ALL** — fixed (existence subselect now sourced for
  HAVING memberships and projected membership flags). Was the prior "gate red" item.
- **condition-root bridge co-source** (gcat `test_join_discovery` + `by all_rows`
  grand-total cross-join) — fixed.

## Current tracked state — all SHAPE/SIZE (no correctness)

`tests/v4_known_failing.py` tracks the following, none of which are wrong-rows:

| bucket | meaning |
| --- | --- |
| `_INLINE` | SQL/CTE shape differs from v3; rows match. Cosmetic. |
| `_MODELING` | modeling-sweep shape/CTE diffs; rows match (classifier: all SHAPE). |
| `_TPCDS_SIZE` | rows match the official reference, generated SQL exceeds v3-tuned length ceilings. Verbosity. |

27 entries (2026-06-25): 10 `_INLINE`, 7 `_MODELING`, 10 `_TPCDS_SIZE`. The single
open *theme* is **plan verbosity / CTE shape**, not correctness. That is the focus
of the size-analysis work below.

## Size / verbosity analysis (2026-06-25)

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

### Measurements (proxy lengths from `v4_size_compare`; `test` = real verdict)

`v4 base` = the 2026-06-24 baseline (before any size fix). `v4` = current, after the
two fixes below (row-preserving inline + join-stream sharing). `test` PASS rows were
pruned from `v4_known_failing.py`.

| q | ceiling | v3 | v4 base | v4 | test |
| --- | ---: | ---: | ---: | ---: | :---: |
| 02 | 7500 | 4625 | 7725 | 7685 | fail* |
| 2.2 | 7500 | 6770 | 10267 | 10267 | fail |
| 12 | 3200 | 2026 | 4132 | **2026** | **PASS** |
| 20 | 3200 | 1869 | 4214 | **2314** | **PASS** |
| 23 | 8500 | 7792 | 10610 | 8423 | **PASS** |
| 30.alt | 12000 | 7147 | 11670 | 9006 | fail |
| 47 | 6800 | 5486 | 11568 | **7868** | fail |
| 50 | 7000 | 4725 | 8776 | **6889** | **PASS** |
| 57 | 6500 | 4894 | 10267 | **6900** | fail |
| 62 | 2500 | 2160 | 3073 | 2160 | **PASS** |
| 69 | 5000 | 4059 | 4485 | 4485 | **PASS** |
| 73 | 3000 | 2823 | 5665 | 5496 | fail |
| 76 | 10000 | 7477 | 10957 | 10957 | fail |
| 81 | 8000 | 7460 | 10192 | 9163 | fail |
| 94 | 5000 | 3544 | 5265 | 4810 | **PASS** |
| 97.1 | 4250 | 2989 | 2357 | 2357 | **PASS** |

\* q02 fails by **rendering an invalid empty identifier** (`""`), not on size — a
pre-existing bug (reproduces with both size fixes reverted), masked by the xfail.
(q2.1 and q10 `RecursionError` in the generation-only harness — separate
planner-recursion concern, not size.)

**Still failing `_TPCDS_SIZE` (10):** 02, 2.1, 2.2, 10, 30.alt, 47, 57, 73, 76, 81.
The join-stream spike cut these substantially (q47 11568→7868, q57 10267→6900) but
they remain over ceiling. q47/q57 are now close; q2.2/q76 are the most verbose
multi-fact / many-sibling shapes left.

### The row-preserving aggregate-input inline fix (2026-06-25)

Lives in `aggregate.py` + `strategy_builder.py`. A row-preserving aggregate input
(`Derivation.ROOT`/`BASIC`/`FILTER`, lineage not crossing a row-shape barrier) is
now rendered **inline** in the consuming aggregate's `GroupNode` —
`sum(CASE WHEN p THEN x ELSE NULL END)` in one grouped SELECT — instead of
materializing it as a separate CTE joined back on the fact PK. `gen_aggregate`
expands the aggregate's `input_concepts` to the render inputs; `_parent_nodes_for`
replaces a row-preserving BASIC/FILTER parent with its row-stream predecessors when
the group is renderable from them; the vacuous input-grain normalization GroupNode
is skipped. Guarded so q08-style disjoint filters (existence args) stay separate.
Cleared q62 (3073→2160, == v3) plus q23/q94/etc. Locked by
`tests/core/processing/test_v4_virt_filter_extra_cte.py`.

### The join-stream sharing fix (2026-06-25)

Lives in `strategy_builder.py` (`_add_aggregate_needed_concepts`). Multi-consumer /
multi-grain queries (q47: one `sum_sales` aggregate feeding an `avg` regroup, two
windows, and a scalar diff) re-derived the fact×dimension **join-stream** once per
consumer — v4 rebuilt the dim joins from a bare fact-key re-scan and FULL/RIGHT-joined
the streams back on all dim keys, instead of reusing the already-joined-and-aggregated
CTE. The fix adds an aggregate's `by` grouping keys (and row-preserving inputs) to the
`needed` set, so parent-dedup keeps the existing joined stream as the shared parent
rather than re-sourcing. q47 went 8 CTEs / 9 JOINs / 2 fact-scans → **4 / 5 / 1** (near
v3's 3). New stable passes: q12 (== v3), q20, q50. Full sweep 4153 passed / 0 failed.

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

1. DONE (2026-06-25): pruned 11 entries that pass in isolation (verified stable over
   3–4 runs each) — `test_select_literal_is_rendered_in_projection`,
   `test_exact_match_merge_preserves_subgraph_filters`, `test_adhoc08`, `test_in_select`,
   `test_group_by_with_existing`, and tpc-ds `test_twenty_three`, `test_sixty_two`,
   `test_sixty_nine`, `test_ninety_four`, `test_ninety_seven_one`, `test_ninety_seven_two`.
   The `_RESULT` bucket is gone. Then the join-stream fix added 3 more (`test_twelve`,
   `test_twenty`, `test_fifty`), leaving 27 entries that still fail in isolation.
2. Split the `_MODELING` entries into `_INLINE` or `_TPCDS_SIZE` (all are SHAPE per
   the classifier — no rows diffs left).
3. Re-run `local_scripts/v4_classify.py` after any planner change, and a full v4
   sweep before claiming parity.
4. Keep new Stage 3 heuristics behind contract-driven tests: if materialization
   needs a key or projection grain, Stage 2 should declare it first.
