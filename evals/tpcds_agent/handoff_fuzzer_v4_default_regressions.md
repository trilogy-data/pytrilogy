# Handoff: 20 fuzzer regressions — ALL trace to the V4-default flip (a6161b981, #602)

**Status: FIXED 2026-08-11 (follow-up session). All 20 cases green; the full
fuzzer corpus is 218/218 (including the two partial-binder bugs from the
companion handoff, fixed the same session). Committed unit repros:
`tests/engine/test_duckdb_fuzzer_regressions.py`. Fixes, by symptom class:**

1. Rowset-boundary Disconnected (6): the pre-discovery connectivity gate
   (`island_rowsets=False` mode) never welded one rowset's co-produced
   handles, so a join declared INSIDE the boundary body (invisible at the
   outer level) split them. `link_rowset_outputs_for_connectivity`
   (rowset_islanding.py) now hub-welds same-rowset handles in the
   non-islanding mode; the suggestion path also honors `excluded_addresses`.
2. grouping_placement (9): (a) a grouping-sets contributor suppresses the
   FINAL dedup, so `_wrap_for_grain` now dedups a row-grain leaf contributor
   itself (`dedup_orthogonal`, strategy_builder.py); (b) a grouping()
   identity flag now inherits its pass siblings' axis widening
   (`_grouping_pass_sibling_axis_members`, concept_graph.py) so it co-buckets
   with the pass instead of pairing via the literal-0 grain-match stamp.
3. union partition (4): `partition_roots` welds a PROPERTY root to roots
   sharing a datasource binding (new `ConceptAttrs.datasource_bindings`),
   guarded to non-aggregate consumers, so two pure renames of fact columns
   co-source one scan and the arm WHERE covers both.
4. coalescing_presence composite (1): `_aggregate_input_grain` widens by
   sibling key groups of the same composite relation
   (`_composite_relation_sibling_axes`), so presence sums range over the full
   (customer, item) axis.

Original bisect record below, kept for provenance.

---

**Status: BISECTED to one culprit commit, NOT fixed. All 20 old-family
failures in the 2026-08-11 full fuzzer run are V3→V4 parity gaps. V3 was
deleted two commits later (#632), so every fix is forward, in the v4
planner.**

## The bisect verdict

Probed by running the failing families in per-commit worktrees (method notes
below). Every probe used the era's own code, lark backend:

| Commit | Date | union / rowset_boundary / coalescing | grouping_placement |
| --- | --- | --- | --- |
| `4dcac11ea` (#594) | 07-05 | GREEN (36/36) | family absent |
| `fe61fa56a` (#595) | 07-10 | GREEN (36/36) | absent |
| `d44c49c91` (#600) | 07-13 | GREEN — exonerates the July v4-parity/where-scoping refactors #596–#600 | absent |
| `bfb1b27f5` (#607) | 07-23 | GREEN (deprecated `full join` variants error by design) | absent |
| `418901be6` (#629) | 08-04 | GREEN — direct parent of the culprit | absent |
| **`a6161b981` (#602 "V4 As Engine Default")** | 08-08 | **RED — all 11 appear here** | absent |
| `4825c9090` (#630, adds the family) | 08-09 | red | **born red (9/24), post-flip** |
| HEAD | 08-11 | red (same 11) | red (same 9) |

So: **11 regressions** (below) flipped green→red at exactly `a6161b981`,
whose parent is green. **9 grouping_placement failures** were born red one
day after the flip — authored under v4-default, never green. Nothing else in
five weeks of history moves these cases. The failures were invisible until
now because the full fuzzer corpus had not run since 2026-07-08 — the v4
parity burndown used the qNN registry and tests/join_matrix, and the fuzzer
corpus was never swept under v4 before V3 was removed.

## The 20 failures, by symptom class (repros: `local_scripts/fuzzer/repros/<case_id>/`)

1. **Rowset-boundary readback → DisconnectedConceptsException at compile**
   (6: `{edge,dense}__rowset_boundary__{subset,union}_subordinate_readback`,
   `{edge,dense}__rowset_boundary__subordinate_window_readback`).
   A `with boundary as select ... subset|union join subordinate.k = anchor.k;`
   then reading `boundary.subordinate.k` + `boundary.anchor_total` back out
   splits into two disconnected subgraphs — v4 discovery loses the join
   declared INSIDE the boundary rowset when the subordinate key is read back
   under its authored address. Hard compile break of a previously-planning
   query; loudest and probably deepest.
2. **Union-partition mismatches** (4: `union__nullable_partition` — edge
   returns 4 rows vs 3, `union__three_arm_partition` — right row count,
   wrong aggregate values, both seeds). UNION ALL arm stacks partitioned by
   complementary predicates, re-aggregated by gid.
3. **Coalescing-presence composite** (1:
   `dense__coalescing_presence__union_plain_composite`). Presence CASE-sums
   over a composite-key union join; single-row result, wrong counts. Edge
   seed passes — dense's key overlap pattern is what exposes it.
4. **grouping_placement, 9 instances** (extra-leaf-dim rollup/cube/
   grouping-sets mismatches off by one row — 8/7, 6/5, subtotal row vs leaf
   join-back — plus `rollup_label_over_union_joined_rowsets` and
   `rollup_one_of_two_union_join_keys` emitting SQL where a leaf column
   (`sale_side_g`/`sale_side_e`) leaks bare into the grouped CTE →
   BinderException). The family docstring states the intended contract:
   group at the grouping key list, LEFT JOIN leaf dims back on it.

Also in the same run, from the other handoff (separate root cause, not
flip-related): the 2 partial-binder filtered-count bugs —
`handoff_fuzzer_partial_binder_filtered_count_bugs.md`.

## How to re-run / verify

- Full sweep: `.venv/Scripts/python.exe -m local_scripts.fuzzer` (report under
  `local_scripts/fuzzer/runs/`). Targeted: `--family rowset_boundary
  --family union --family coalescing_presence --family grouping_placement`.
- A green target = 218/218 with only the two known partial-binder cases
  (`filtered_grain_row_population`, `filtered_named_derived_key_count`)
  allowed red until that bug is fixed too.

## Probe-methodology traps (cost this session two false attributions)

- **Shared venv pins the MAIN tree**: site-packages `pytrilogy.pth` adds the
  main repo root to sys.path in every venv process. Running a driver script
  that lives OUTSIDE a worktree makes sys.path[0] the script's dir and the
  .pth then resolves `trilogy`/`local_scripts` from the MAIN tree — every
  "old commit" probe silently runs current code (identical results at every
  commit is the tell). Run `python -m local_scripts.fuzzer` from the
  worktree cwd, or copy the driver INTO the worktree root.
- **The rebuilt pest wheel poisons old-commit probes**: the venv's rust wheel
  carries the CURRENT grammar (`where_series` from `then where`), which old
  hydrators can't consume (`No v2 hydrator for 'where_series'` on every
  where-bearing query). For cross-era probes force the lark backend
  (`CONFIG.parser_backend = ParserBackend.LARK` before importing the runner)
  — pure Python, loads its grammar from the worktree, backend does not
  affect planning.

## Suggested attack order

1. Rowset-boundary Disconnected (compile break, one mechanism, 6 cases).
2. grouping_placement BinderExceptions (generated-SQL invalid — framework
   class), then the off-by-one-row leaf join-backs.
3. Union-partition and coalescing-presence mismatches (wrong results, likely
   share arm/grain machinery).
4. Wire the fuzzer into the routine full-validation loop (its last full run
   before today predated the engine flip by a month) so the next default
   flip can't silently strand a suite.
