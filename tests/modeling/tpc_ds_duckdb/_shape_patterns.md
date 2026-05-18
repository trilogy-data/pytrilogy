# TPC-DS shape-optimization targets

**Scope: main queries only.** Alt `.1`/`.2` variants (q2.1/2.2/97.1/97.2)
exercise deliberately non-performant paths — ignore them when ranking.

Timings (`test_queries.py:run_query` → `zquery_timing_<fp>.log`):
`time.perf_counter` (monotonic, not Windows-quantized `datetime.now`)
around exec **including fetch** on both sides — DuckDB materializes
lazily, so fetch must be inside the window symmetrically. Any stage under
`REPEAT_TIME_CUTOFF` (0.15s) is re-run `REPEAT_COUNT` times interleaved
trilogy/reference and the **minimum** is kept (noise only adds time).
Above the cutoff: single cold sample (trilogy runs first, so it slightly
warms shared fact tables for the reference). Absolute deltas are still
noisy — **trust the ratio ranking + the structural SQL diffs, not any
single ms or the raw win count.** Win = trilogy `exec_time < comp_time`.
Tables below regenerated 2026-05-18 under the corrected harness.

## State

**40 / 99 main wins.** Trilogy 11.94s vs reference 55.11s (ignore totals:
~40s of the ref is the q64/q72 PRAGMA plan-blowups). Alt bucket 2/5.

The prior **47/99** was a measurement artifact: trilogy `exec_time`
excluded result materialization (fetch) while the reference included it,
over-counting ~7 wins. **40/99 is the honest baseline** — not a trilogy
regression. The RC-G strip is structurally validated (q65/q68 now at
parity, 1.0x) but did not move the honest count the way the inflated
metric implied.

## Root causes

| RC | Defect | Queries | Status |
|---|---|---|---|
| **RC-A** | Shared-grain multi-metric query split into per-source pipelines that each re-scan the fact tables, re-merged with `FULL JOIN … IS NOT DISTINCT FROM` on the group keys | q17, q25, q29; q50 (dim/metric variant) | open — fork half (the INDF half is fixed by RC-G strip) |
| **RC-C** | Aggregate-membership `IN/NOT IN` → materialized CTE + `IN (SELECT … IS NOT NULL)` probed 2×; `count>1` as CASE→NULL over full grain instead of `HAVING` | q94 (q16 same shape but ref 13ms — sub-floor) | open. Also a correctness smell: `count(wh)>1` where ref needs `count(distinct wh)` |
| **RC-D** | Channel-marked `UNION ALL` + `CASE WHEN channel=` pivot instead of per-channel pre-aggregate-then-join; returns anti-join lifted *above* per-channel aggregation | q78, q05 | open |
| **RC-E** | Disjoint constant buckets → CASE-WHEN over one loose scan; `count(DISTINCT)` runs over the loose superset, not a tight per-bucket WHERE | q28 | open |
| **RC-F** | `UNION ALL` lowered to a FULL-JOIN-on-all-output-columns tower; scalar branch cross-joined N× | q77 (4 FULL JOINs, 3 `ON 1=1`), q76 (also doubles every fact scan) | open |
| **RC-G** | Null-safe `IS NOT DISTINCT FROM` key join (FK provably non-null) — defeats hash joins; often paired with an unfiltered dim cross-section, post-join-only selectivity filter, or dead trailing GROUP BY | q50 residue; the cost half of every RC-A re-merge | **strip LANDED + validated** — q65 1.02x, q68 1.01x (parity, no longer losses). Only q50 residue remains, RC-A-dominant |
| RC-B | Leading unfiltered key-extraction CTE + re-scan for enrichment | q44 (residue) | resolved for q73/q81/q30 (single filtered scan now); q44 keeps an RC-B-adjacent 3rd store_sales scan |

## Targets (current, main, ref ≥ 15ms, by ratio)

| Query | trilogy | ref | Δ | ratio | RC |
|---|---:|---:|---:|---:|---|
| 28 | 0.200s | 0.037s | +0.163 | **5.47x** | RC-E — worst trustworthy ratio (ref above floor) |
| 94 | 0.061s | 0.019s | +0.043 | **3.28x** | RC-C |
| 17 | 0.123s | 0.046s | +0.077 | **2.67x** | RC-A |
| 25 | 0.087s | 0.036s | +0.051 | **2.42x** | RC-A |
| 66 | 0.362s | 0.153s | +0.209 | **2.37x** | unclassified — large absolute, needs triage |
| 83 | 0.084s | 0.036s | +0.048 | **2.33x** | UnionDimPushdown residue |
| 77 | 0.097s | 0.042s | +0.055 | **2.30x** | RC-F |
| 05 | 0.114s | 0.051s | +0.063 | **2.22x** | RC-D/A |
| 29 | 0.158s | 0.074s | +0.084 | **2.14x** | RC-A |
| 76 | 0.098s | 0.047s | +0.050 | **2.07x** | RC-F+B — ⚠ stale: `test_seventy_six` errors (coalesce type) |
| 78 | 0.456s | 0.226s | +0.231 | **2.02x** | RC-D — largest absolute |
| 69 | 0.386s | 0.199s | +0.187 | **1.94x** | unclassified — large absolute, was wrongly excluded |
| 50 | 0.302s | 0.174s | +0.128 | 1.74x | RC-A/G |
| 44 | 0.042s | 0.025s | +0.017 | 1.65x | RC-B-adjacent |
| 09 | 0.059s | 0.036s | +0.023 | 1.65x | unclassified |

Largest absolute losses: q78 (+0.231), q66 (+0.209), q69 (+0.187), q28
(+0.163), q50 (+0.128), q29 (+0.084).

**Exclude from win-rate targeting** (large absolute, ratio ≈ 1, not
structurally flippable): q67 (1.06x, trilogy 0.90s), q22 (1.02x, 0.76s).
**Measurement floor** (ref < 15ms, already algorithmically fine — ratio is
floor noise): q16 (ref 13ms, 7.46x — *was* the doc's #1 target; the old
~7.8x was a `datetime.now`-quantization artifact), q90 (ref 6ms). **Now at
parity** (RC-G strip + honest timing): q65 (1.02x), q68 (1.01x), q73
(1.11x).

## Priority

40/99 honest baseline. q28 + q94 are the two highest-confidence structural
flips with ref above the measurement floor.

1. **q28 (RC-E)** — disjoint buckets → 6 WHERE-filtered subqueries (the
   reference shape). Highest trustworthy ratio (5.47x), large absolute,
   isolated, high-confidence generator fix.
2. **q94 (RC-C)** — `count(...) by … > N` → real `HAVING` semi-join +
   single filtered scan; `IN (SELECT … IS NOT NULL)` → semi/anti-join,
   materialized once not probed 2×. Closes the `count` vs `count(distinct)`
   correctness gap. Same lowering also helps q16 (sub-floor, won't show in
   ratio but still structurally wrong).
3. **RC-A fork** — don't fork co-grain metrics into per-source pipelines
   re-merged by `FULL JOIN … IS NOT DISTINCT FROM` (q17/q25/q29; q50
   dim/metric variant). The null-safe join itself is fixed (RC-G validated);
   q50's residue is the post-join selectivity filter + dead trailing GROUP
   BY. Locus:
   `trilogy/core/processing/node_generators/select_merge_node.py`.
4. **Triage q66/q69/q83** — large absolute/ratio losses with no RC mapping
   yet; q76's RC-F row is stale until the coalesce-type generation error in
   `test_seventy_six` is fixed.

Secondary, larger rewrites: **q78/q05 (RC-D)** per-channel
pre-aggregate-then-join, keeping the returns anti-join per-channel +
date-filtered; **q76/q77 (RC-F)** keep `UNION ALL` as `UNION ALL`, no
FULL-JOIN tower, no N× scalar cross-join; **q44** drop the redundant 3rd
store_sales scan + the `1=1` scalar cross-join.

All fixes are generator-side; the `.preql` intent files already express the
reference-equivalent single-scan / UNION / HAVING / equi-join semantics.

## Landed

**RC-G null-safe-join strip (2026-05-17).** When an *applied* condition
proves a concept non-null, drop it from `nullable_concepts` so the join
scorer emits `=`/INNER instead of OUTER `IS NOT DISTINCT FROM`. New
`condition_proves_non_null()` in `condition_utility.py`; strip in
`create_datasource_node` (gated on `routed_conditions`) and
`GroupNode.resolve` (gated on scalar `preexisting_conditions` — the one that
flips q65, whose `store.id IS NOT NULL` is pushed into an upstream scan).
`MergeNode` left alone (merge-stage COALESCE caveat in `non_null_proofs`).
Effect: q65 join `RIGHT OUTER … IS NOT DISTINCT FROM` → `INNER … =` (+
`store` dim `LEFT OUTER`→`INNER`); q65/q68 now at parity (1.0x),
q17/q05/q78 improved. The reported 44→47/99 win bump was partly the
fetch-asymmetry artifact (see harness fix below); the structural join win
is real and confirmed by q65/q68 parity. 106/106 row-equality pass.

**Profiling harness fix (2026-05-18).** `run_query` now times with
`time.perf_counter` (was `datetime.now` — wall-clock, ~15ms-quantized on
Windows), wraps exec **including `fetchall`** symmetrically on both sides
(was: trilogy excluded fetch, reference included it), resolves reference
SQL outside the timing window, and takes the **min of `REPEAT_COUNT`
interleaved runs** for any stage under `REPEAT_TIME_CUTOFF` (0.15s).
Effect: honest count 47→**40/99** (fetch-asymmetry was over-counting ~7
trilogy wins); q16 unmasked as measurement-floor noise (ref 13ms; old
~7.8x was timer quantization), so it drops from the #1 priority slot; q94
becomes the trustworthy RC-C target. ruff/mypy/black clean.
