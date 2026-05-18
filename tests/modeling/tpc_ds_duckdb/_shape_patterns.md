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
Numbers in the tables below predate the symmetric-fetch fix (trilogy
`exec_time` was undercounting result materialization); they regenerate on
the next full run.

## State

**47 / 99 main wins.** Trilogy 12.06s vs reference ~57s (ignore totals:
~41s of the ref is the q64/q72 PRAGMA plan-blowups). **Target 50/99 → need
+3.** Alt bucket 2/5.

## Root causes

| RC | Defect | Queries | Status |
|---|---|---|---|
| **RC-A** | Shared-grain multi-metric query split into per-source pipelines that each re-scan the fact tables, re-merged with `FULL JOIN … IS NOT DISTINCT FROM` on the group keys | q17, q25, q29; q50 (dim/metric variant) | open — fork half (the INDF half is fixed by RC-G strip) |
| **RC-C** | Aggregate-membership `IN/NOT IN` → materialized CTE + `IN (SELECT … IS NOT NULL)` probed 2×; `count>1` as CASE→NULL over full grain instead of `HAVING` | q16, q94 | open. Also a correctness smell: `count(wh)>1` where ref needs `count(distinct wh)` |
| **RC-D** | Channel-marked `UNION ALL` + `CASE WHEN channel=` pivot instead of per-channel pre-aggregate-then-join; returns anti-join lifted *above* per-channel aggregation | q78, q05 | open |
| **RC-E** | Disjoint constant buckets → CASE-WHEN over one loose scan; `count(DISTINCT)` runs over the loose superset, not a tight per-bucket WHERE | q28 | open |
| **RC-F** | `UNION ALL` lowered to a FULL-JOIN-on-all-output-columns tower; scalar branch cross-joined N× | q77 (4 FULL JOINs, 3 `ON 1=1`), q76 (also doubles every fact scan) | open |
| **RC-G** | Null-safe `IS NOT DISTINCT FROM` key join (FK provably non-null) — defeats hash joins; often paired with an unfiltered dim cross-section, post-join-only selectivity filter, or dead trailing GROUP BY | q65, q68, q50; the cost half of every RC-A re-merge | **null-safe→equi-join strip LANDED** (q65 join now `INNER … =`; +3 wins). Non-join halves still open for q65/q68/q50 |
| RC-B | Leading unfiltered key-extraction CTE + re-scan for enrichment | q44 (residue) | resolved for q73/q81/q30 (single filtered scan now); q44 keeps an RC-B-adjacent 3rd store_sales scan |

## Targets (current, main, ref ≥ 15ms, by ratio)

| Query | trilogy | ref | Δ | ratio | RC |
|---|---:|---:|---:|---:|---|
| 16 | 0.112s | 0.014s | +0.097 | **~7.8x** | RC-C — worst ratio; ref at the 15ms floor (noisy) but structurally the top target |
| 28 | 0.218s | 0.051s | +0.167 | **4.29x** | RC-E |
| 29 | 0.199s | 0.077s | +0.122 | **2.58x** | RC-A |
| 25 | 0.093s | 0.041s | +0.052 | **2.25x** | RC-A |
| 17 | 0.130s | 0.058s | +0.072 | **2.24x** | RC-A |
| 77 | 0.108s | 0.049s | +0.059 | **2.20x** | RC-F |
| 94 | 0.054s | 0.024s | +0.029 | **2.19x** | RC-C |
| 68 | 0.159s | 0.073s | +0.086 | **2.18x** | RC-G (non-join half) |
| 76 | 0.098s | 0.047s | +0.050 | **2.07x** | RC-F+B |
| 05 | 0.150s | 0.075s | +0.075 | **2.00x** | RC-D/A |
| 50 | 0.373s | 0.207s | +0.166 | 1.80x | RC-A/G |
| 65 | 0.190s | 0.117s | +0.073 | 1.62x | RC-G (non-join half) |
| 44 | 0.060s | 0.038s | +0.022 | 1.58x | RC-B-adjacent |
| 78 | 0.362s | 0.235s | +0.127 | 1.54x | RC-D |

Largest absolute losses: q67 (+0.203) and q50/q78/q29 — but q67 is
near-parity (1.26x, trilogy 1.0s).

**Exclude from win-rate targeting** (large absolute, ratio ≈ 1, not
structurally flippable): q67, q69, q22. **Measurement floor** (ref < ~10ms,
already algorithmically fine): q90. **Variance, not structural** (RC-B
resolved; ratio jumps run-to-run): q73.

## Priority

Only **+3** needed (RC-G strip already banked +3). (1)+(2) clears 50/99.

1. **q16 + q94 (RC-C)** — `count(...) by … > N` → real `HAVING` semi-join +
   single filtered scan; `IN (SELECT … IS NOT NULL)` → semi/anti-join,
   materialized once not probed 2×. Worst ratio in the suite, isolated, two
   wins from one lowering change, and closes the `count` vs `count(distinct)`
   correctness gap.
2. **q28 (RC-E)** — disjoint buckets → 6 WHERE-filtered subqueries (the
   reference shape). Large absolute, isolated, high-confidence generator fix.
3. **RC-A fork + remaining RC-G halves** — (a) don't fork co-grain metrics
   into per-source pipelines re-merged by `FULL JOIN … IS NOT DISTINCT FROM`
   (q17/q25/q29; q50 dim/metric variant); (b) for q65/q68/q50, the null-safe
   join is already fixed — remaining is the post-join selectivity filter,
   dead trailing GROUP BY, and unfiltered dim cross-section. Locus:
   `trilogy/core/processing/node_generators/select_merge_node.py`.

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
Effect: 44→47/99 wins, 13.07→12.06s; q65 join `RIGHT OUTER … IS NOT
DISTINCT FROM` → `INNER … =` (+ `store` dim `LEFT OUTER`→`INNER`);
q17/q68/q05/q78 improved. 106/106 row-equality pass; ruff/mypy/black clean.
