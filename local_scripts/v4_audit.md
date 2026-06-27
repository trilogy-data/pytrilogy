# v4 compatibility audit (last refreshed 2026-06-26, post full-sweep)

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

**Short answer: no wrong-rows, no crashes. What's left is plan verbosity on ~8 TPC-DS
queries plus a batch of cosmetic shape-assert tests. Correctness is there.**

v4 discovery is still off by default (`CONFIG.use_v4_discovery = False`). As of 2026-06-26
the full sweep is **0 failed** and the classifier reports **0 CRASH / 0 escalations**. All
the crashes that were open on 2026-06-25 are fixed (existence-source `RecursionError`
family, q2.1 union `BinderException`, `test_filter_constant` invalid-ref). What remains
before flipping the default:

1. **Plan verbosity — 8 `_TPCDS_SIZE` tests** (q10, q2.1, q2.2, q30.alt, q73, q81 +
   q23/q94). Rows match; the SQL trips `assert len(query) < ceiling`. The only
   substantive engineering left. q23/q94 are a *deliberate* trade — the q16 all-ROOT
   input-grain normalization (a correctness floor) adds CTEs; they need a grain-aware
   skip. Handoffs: `v4_verbosity_handoff.md`, `v4_dimension_projection_rejoin_handoff.md`
   (q81), and `project_v4_verbosity_regressions_0626` (q23/q94).
2. **Cosmetic shape-assert tests — 10 `_INLINE` + 5 `_MODELING`.** Rows match, the SQL
   string differs. To flip the default each must be conditioned on
   `CONFIG.use_v4_discovery` or accepted. Mechanical, not risky.

## Correctness: at parity (full sweep 2026-06-26)

Latest full v4 sweep (`TRILOGY_V4_DISCOVERY=1`, all tests minus adventureworks):

**4281 passed, 20 skipped, 7 xfailed, 25 xpassed, 82 errors — 0 failed (exit 0, 8m35s).**

- **0 failed** — no wrong-rows / crash / invalid-render regressions.
- The **82 errors** are all `tests/engine/test_clickhouse_server.py` — clickhouse.cloud
  connection errors, environmental (no local server). Ignore.
- **25 xpassed / 7 xfailed** are tracked `v4_known_failing.py` entries. NB the full suite
  is *more* favorable than isolation — only ~5 of these xpass in isolation too. Prune
  only entries that pass in BOTH (see below); the rest pass only with full-suite ordering
  and would flip red if run alone.

## Current tracked state — SHAPE/SIZE only (no correctness)

`tests/v4_known_failing.py` tracks **23 entries** (after pruning 5 confirmed-passing on
2026-06-26: q02, q47, q57, q76, `test_non_nullable_null_guard`). No crash labels remain.

| bucket | count | meaning |
| --- | --- | --- |
| `_INLINE` | 10 | SQL/CTE shape differs from v3; rows match. Cosmetic. |
| `_MODELING` | 5 | modeling-sweep shape/CTE diffs; rows match. Cosmetic. |
| `_TPCDS_SIZE` | 6 | rows match the reference, SQL over the v3 length ceiling. Verbosity. |
| `_TPCDS_SIZE_NORMALIZE` | 2 | q23/q94 — over ceiling because the q16 correctness floor (all-ROOT input-grain normalization) adds CTEs. Needs a grain-aware skip. |

The one open theme is **plan verbosity** (the size work below — `v4_verbosity_handoff.md`).

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

### Measurements (proxy lengths from `v4_size_compare`; `test` = real verdict)

`v4 base` = the 2026-06-24 baseline (before any size fix). `v4` = current, after the
two fixes below (row-preserving inline + join-stream sharing). `test` PASS rows were
pruned from `v4_known_failing.py`.

| q | ceiling | v3 | v4 base | v4 | test |
| --- | ---: | ---: | ---: | ---: | :---: |
| 02 | 7500 | 4625 | 7725 | 7685 | **PASS** |
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
| 76 | 10000 | 7477 | 10957 | 10957 | **PASS** |
| 81 | 8000 | 7460 | 10192 | 9163 | fail |
| 94 | 5000 | 3544 | 5265 | 4810 | **PASS** |
| 97.1 | 4250 | 2989 | 2357 | 2357 | **PASS** |

Note (proxy table is stale on a few real verdicts; re-measured 2026-06-25 against the
real `engine` fixture): **q02 and q76 now PASS** their tests (the proxy over-reports
because it skips inlining). **q10 and q2.1 are CRASHES, not size** — they
`RecursionError` in the *real* test too (the existence-source cycle), not only in the
generation proxy as previously claimed. See `v4_existence_recursion_handoff.md`.

**Genuinely failing on size now (6):** 2.2, 30.alt, 47, 57, 73, 81. The join-stream
spike cut these substantially (q47 11568→7868, q57 10267→6900) but they remain over
ceiling. q47/q57 are close; q2.2 is the most verbose multi-fact / many-sibling shape
left. (q10/q2.1 leave this list when their crash is fixed, then re-measure for size.)

### Size fixes already landed (current behavior, 2026-06-25)

Two fixes drove the `v4 base`→`v4` column above and cleared q12/q20/q50/q62/q23/q94:

- **Row-preserving aggregate-input inline** (`aggregate.py` + `strategy_builder.py`):
  a row-preserving aggregate input (`Derivation.ROOT`/`BASIC`/`FILTER`, lineage not
  crossing a row-shape barrier) renders **inline** in the consuming `GroupNode`
  (`sum(CASE WHEN p THEN x ELSE NULL END)`) instead of as a separate CTE joined back on
  the fact PK. Guarded so q08-style disjoint existence-arg filters stay separate. Locked
  by `tests/core/processing/test_v4_virt_filter_extra_cte.py`.
- **Join-stream sharing** (`strategy_builder._add_aggregate_needed_concepts`):
  multi-consumer/multi-grain queries re-derived the fact×dimension join-stream once per
  consumer. Adding an aggregate's `by` keys (and row-preserving inputs) to the `needed`
  set lets parent-dedup reuse the already-joined-and-aggregated CTE. q47: 8 CTEs/9
  JOINs/2 fact-scans → 4/5/1.

### Remaining size shapes — next targets (2026-06-25)

Full write-up moved to **`local_scripts/v4_verbosity_handoff.md`** (6 queries: 2.2,
30.alt, 47, 57, 73, 81). Three patterns, in fix order:

1. **Passthrough-projection bloat — biggest, lowest-risk lever.** Pure single-source
   projection CTEs that should fold into their consumer; `_fold_passthrough_parents`
   isn't firing. Inflates q73, q47, q57, q2.2. q47/q57 are just over ceiling — folding
   alone may tip them under.
2. **Dimension-projection re-join — q81 & q30.alt** (identical fingerprint). Wide
   customer/address dims re-sourced through the fact instead of from
   `customer ⋈ customer_address`. Refined root cause + a regression trap (q65) in
   `local_scripts/v4_dimension_projection_rejoin_handoff.md`. Higher risk.
3. **Aggregate over-split — q73.** Overlaps heavily with (1); try the fold first.

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

1. Size verbosity (the gating work): land the passthrough-folding and q81/q30.alt
   dimension-re-join fixes above; q47/q57 may tip under ceiling from folding alone.
2. Fix the q02 invalid-identifier render bug (`v4_q02_invalid_alias_handoff.md`).
3. Condition the `_INLINE`/`_MODELING` shape-assert tests on `CONFIG.use_v4_discovery`
   so they pass under both planners (prerequisite for flipping the default).
4. Re-run `local_scripts/v4_classify.py` after any planner change, and a full v4
   sweep before claiming parity (the classifier only re-checks listed tests, so it is
   blind to regressions in newly added tests).
5. Keep new Stage 3 heuristics behind contract-driven tests: if materialization needs
   a key or projection grain, Stage 2 should declare it first.
