# v4 compatibility audit (last refreshed 2026-06-24, post full-sweep)

This file is the current handoff for v4 discovery work. The authoritative skip list is
`tests/v4_known_failing.py`; reclassify with `python local_scripts/v4_classify.py`
when changing planner behavior. The classifier only re-checks tests already on the
skip list, so it is blind to regressions in tests added since the list was last
curated — **a fresh full v4 sweep is the only way to confirm parity** after any new
PR. Run it, don't trust the classifier alone:

```bash
TRILOGY_V4_DISCOVERY=1 pytest -m "not adventureworks_execution" -q
```

## Correctness: at parity (full sweep 2026-06-24)

Latest full v4 sweep (`TRILOGY_V4_DISCOVERY=1`, all tests minus adventureworks):

**4134 passed, 20 skipped, 5 xfailed, 38 xpassed, 82 errors — 0 failed (exit 0).**

- **0 failed** — no untracked wrong-rows / crash / invalid-render regressions, including
  across #586/#587/#588 ("V4 Parity" PRs) added after the prior audit.
- The **82 errors** are all `tests/engine/test_clickhouse_server.py` — clickhouse.cloud
  connection errors, environmental (no local server). Ignore.
- **5 xfailed / 38 xpassed** are the tracked `v4_known_failing.py` entries. The xpasses
  are cross-test-pollution-flaky (the list is non-strict for exactly that reason).

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
| `_RESULT` | `test_ninety_seven_two` only — historical wrong-rows entry that **now xpasses** (rows correct); stale, safe to remove. |

The single open *theme* is therefore **plan verbosity / CTE shape**, not correctness.
That is the focus of the size-analysis work below.

## Size / verbosity analysis (in progress, 2026-06-24)

Goal: v4 should produce **equal-or-less** verbose SQL than v3. The `_TPCDS_SIZE`
entries are queries where v4's rows are correct but the SQL is longer (more CTEs /
less compact) than v3's, tripping length-ceiling asserts.

Measure with `python -m local_scripts.v4_size_compare` (generation-only, no exec;
writes each v4 plan to `tests/modeling/tpc_ds_duckdb/zsize_v4_<q>.sql`).

### Measurements (2026-06-24, stripped length / SELECT count)

| q | ceiling | v3 | v4 | Δ | v3 sel | v4 sel |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 02 | 7500 | 4625 | 7725 | +3100 | 8 | 10 |
| 2.2 | 7500 | 6770 | 10267 | +3497 | 8 | 17 |
| 12 | 3200 | 2026 | 4132 | +2106 | 3 | 6 |
| 20 | 3200 | 1869 | 4214 | +2345 | 3 | 7 |
| 23 | 8500 | 7792 | 10610 | +2818 | 22 | 30 |
| 30.alt | 12000 | 7147 | 11670 | +4523 | 5 | 10 |
| 47 | 6800 | 5486 | 11568 | +6082 | 4 | 9 |
| 50 | 7000 | 4725 | 8776 | +4051 | 4 | 6 |
| 57 | 6500 | 4894 | 10267 | +5373 | 4 | 9 |
| 62 | 2500 | 2160 | 3073 | +913 | 1 | 2 |
| 69 | 5000 | 4059 | 4485 | +426 | 8 | 11 |
| 73 | 3000 | 2823 | 5665 | +2842 | 2 | 5 |
| 76 | 10000 | 7477 | 10957 | +3480 | 16 | 28 |
| 81 | 8000 | 7460 | 10192 | +2732 | 5 | 9 |
| 94 | 5000 | 3544 | 5265 | +1721 | 13 | 22 |
| 97.1 | 4250 | 2989 | **2357** | **-632** | 5 | **4** |

v4 is ~1.5–2.1× longer with roughly **double the CTE count**. q97.1 is *smaller*
under v4 — existence proof the engine can be more compact. (q2.1 and q10 hit a
`RecursionError` in the generation-only harness but pass in-suite where the
dataset is imported — a separate planner-recursion concern, not size.)

### Root cause: one pattern, not 18

v4 emits extra intermediate CTEs around aggregates that v3 collapses into a single
grouped SELECT. Two concrete forms of the same class:

- **q62** — v3 inlines virtual-filter counters
  `sum(CASE WHEN … THEN 1 ELSE NULL END)` in one grouped SELECT. v4 builds an
  intermediate CTE that **GROUPs at the fact's own row grain**
  (`…, WS_ITEM_SK, WS_ORDER_NUMBER` — a no-op group; the fact is already unique
  there) just to materialize the CASE columns, then re-aggregates. Redundant
  double-group.
- **q73** — the `_virt_filter_id` CASE projection gets its **own standalone CTE**
  instead of being inlined into the `count(...)` that consumes it.

**This is NOT a missing post-hoc optimization** — v3 never builds these nodes.
The `_virt_filter_*` concepts (`Derivation.FILTER`, the `CASE WHEN cond THEN x
ELSE NULL END` of a `count(x ? cond)` filtered aggregate) are virtual concepts
minted at parse, identical for both planners (`VIRTUAL_CONCEPT_PREFIX`). v3's
`gen_group_node` renders the FILTER lineage **inline inside `sum(...)`** in the
one group node — no node boundary is ever created. v4's Stage-2 group graph runs
`group_rules.partition_filters_by_signature`, which gives **each FILTER concept
its own group bucket** → its own CTE (and the q62 "inner GROUP at fact row grain"
is that input-grain bucket rendered as a no-op group). The boundary is *injected
by the partitioner*; folding it back in the optimizer would paper over a Stage-2
decision and fight the phase-boundary contract.

v4 already inlines a FILTER into a *wrapping BASIC* concept (q08
`final_zips = substring(_virt_filter_zips,1,2)`; see `strategy_builder.py:145`),
but NOT when the FILTER is consumed **directly by an AGGREGATE** (q62 `sum`, q73
`count`) — the common TPC-DS case.

**Fix lives in the group graph (Stage 2), not the optimizer:** co-locate a
single-consumer scalar virtual FILTER into its consuming AGGREGATE's bucket, the
same way it already co-locates one into a wrapping BASIC. Guard with the existing
scan-compatibility check (q08's two `_virt_filter`s over disjoint upstreams must
stay separate — see the `partition_filters_by_signature` docstring — or they form
a back-edge through the shared consumer). Eliminates the extra CTE *and* the
redundant input-grain pre-group in one move.

Related: `_virtual_filter_scoped_columns` (`project_q21_virt_filter_propagation`)
and the BASIC-into-GROUP fold (`project_v4_basic_into_group_fold`).

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

1. Remove `test_ninety_seven_two` from `v4_known_failing.py` (now xpasses; empties
   the `_RESULT` bucket). Re-confirm in a full-suite run first.
2. Split the `_MODELING` entries into `_INLINE` or `_TPCDS_SIZE` (all are SHAPE per
   the classifier — no rows diffs left).
3. Re-run `local_scripts/v4_classify.py` after any planner change, and a full v4
   sweep before claiming parity.
4. Keep new Stage 3 heuristics behind contract-driven tests: if materialization
   needs a key or projection grain, Stage 2 should declare it first.
