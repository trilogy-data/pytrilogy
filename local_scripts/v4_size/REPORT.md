# v4 network discovery — generated-SQL size report (2026-07-30)

Reproduce: `.venv/Scripts/python.exe local_scripts/v4_size_report.py both`
(writes `v4_size_tpcds.tsv` / `v4_size_tpch.tsv` plus both planners' SQL under
`tpcds_sql/` and `tpch_sql/`). Symptom counts:
`.venv/Scripts/python.exe local_scripts/v4_sql_symptoms.py`. Planner census:
`.venv/Scripts/python.exe local_scripts/v4_search_census.py both`.

Metric is `query_size(sql, "sql")` — the same comment-stripped character count
the TPC-DS ceilings use. Generation only, DB imported so inlining and the CTE
optimizer run exactly as in the batteries. v3 and v4 are generated in the same
process from a fresh `Environment` each time, v3 first.

## 1. Headline

| suite | queries | v3 chars | v4 chars | ratio | smaller | identical | **LARGER** |
|---|---:|---:|---:|---:|---:|---:|---:|
| TPC-DS | 109 | 524,631 | 450,223 | **0.858** | 43 | 36 | **30** |
| TPC-H | 22 | 30,789 | 31,575 | **1.026** | 8 | 8 | **6** |
| combined | 131 | 555,420 | 481,798 | **0.867** | 51 | 44 | **36** |

**The "smaller on every query" gate does not pass.** v4 is decisively smaller
in aggregate on TPC-DS (−14.2%, driven by q28 alone contributing −68,767), but
36 of 131 queries render larger, and on TPC-H v4 is a net **2.6% larger** than
v3. TPC-H has never been part of the shape-audit loop — every prior size
measurement in this workstream was TPC-DS-only — and it is where the residual
defects are most concentrated relative to corpus size.

Both planners are far from hand-written SQL: v4 is **2.27×** the reference SQL
on TPC-DS and **2.30×** on TPC-H. That gap is the real optimization headroom;
v3-vs-v4 only measures the migration.

## 2. Where v4 is larger

TPC-DS, ranked by absolute regression:

| q | delta | ratio | CTEs | dominant symptom |
|---|---:|---:|:--:|---|
| 66 | +2,460 | 1.12 | 2→4 | recorded s44 residual: aggregate CTE re-joins `date_dim` because the union arms don't project D_MOY/D_YEAR they already join; `square_feet` from a fresh `warehouse` scan (repeat_scan +1) |
| 77 | +1,529 | 1.17 | 17→20 | 9 pure-rename passthrough CTEs (`bewildered` etc. are `SELECT a,b,c FROM <cte>` and nothing else) |
| 74 | +1,360 | 1.33 | 4→5 | extra layer between the two year-scoped aggregate branches |
| 29 | +1,170 | 1.37 | 1→3 | split_agg: aggregate never fuses with its scan |
| 59 | +1,160 | 1.19 | 5→8 | passthrough ×1 + split_agg ×1 |
| 72 | +1,047 | 1.32 | 0→3 | the recorded S4: one `date_diff` exiled to its own CTE (`young`) |
| 80 | +953 | 1.14 | 4→4 | wider per-channel projections |
| 75 | +876 | 1.13 | 6→7 | passthrough ×1 |
| 50 | +751 | 1.17 | 3→3 | wider projection through the layer |
| 68 / 46 | +644 / +595 | 1.18 | 3→4 / 2→3 | the s48b-recorded q44/q46/q68 final passthrough CTE that does not fold |
| 02-one / 02-two | +546 each | 1.09 | 4→5 | extra layer |
| 47 / 57 | +523 / +476 | 1.10 / 1.09 | 3→4 | passthrough ×1 each |

TPC-H, all six:

| q | delta | ratio | CTEs | what changed |
|---|---:|---:|:--:|---|
| 20 | +894 | **1.49** | 2→5 | three separate defects, see §3 |
| 02 | +714 | 1.26 | 2→5 | unfiltered scan CTE + a literal no-op passthrough (`questionable`) + split aggregate |
| 05 | +367 | 1.25 | 1→1 | row-level dedup GROUP BY carries 8 extra join columns; `price*(1-discount)` computed a layer up instead of in the scan |
| 03 | +240 | 1.18 | 2→3 | split_agg + the same deferred scalar |
| 04 | +127 | 1.19 | 1→2 | extra layer |
| 10 | +6 | 1.00 | 2→2 | noise |

## 3. TPC-H q20 — the worst case, read line by line

v4 renders 5 CTEs where v3 renders 2. Four independent defects, each of a class
already on the audit list, all visible in `tpch_sql/q20_v4.sql`:

1. **Predicate not pushed to its scan.** v3 emits
   `WHERE UPPER(n_name) = 'CANADA'` inside the supplier×nation CTE. v4 projects
   `UPPER(n_name)` out and applies the filter in the FINAL select. The join runs
   on all 25 nations' suppliers instead of one. This is a runtime defect as much
   as a size one.
2. **Duplicate `supplier` scan.** Because that CTE was narrowed to
   `(s_suppkey, nation_name)`, `s_name`/`s_address` are unavailable at the end,
   so the FINAL select scans `supplier` a second time with a `LEFT OUTER JOIN`
   and then needs a `GROUP BY 1,2` to dedup what the re-join fanned.
3. **Pure dedup sibling bucket.** `cheerful` is `SELECT part_available_quantity,
   part_id, part_supplier_id FROM wakeful GROUP BY 1,2,3` — no aggregate. It is
   re-joined to its own sibling `cooperative` on
   `is not distinct from` over the same parent's keys. This is symptom S2, alive
   on TPC-H.
4. **Aggregate never fuses with its scan.** `cooperative` is a bare
   `GROUP BY` over `wakeful`. v3 renders the `sum(CASE …)` inside the scan.
   Defects 3 and 4 are coupled: splitting the row stream into two buckets gives
   `wakeful` two consumers, which is exactly what blocks
   `collapse_single_parent` from folding either one back in.

> **UPDATE (same session): the q02 placement bug below is FIXED**
> (`_nested_scope_swallows_atom`, `condition_placement.py`; goldens 109/109
> identical, all gates green, both branches pinned by unit tests). q02 grew
> 3,412 → 3,811 chars as a result — it now applies the predicate it was
> dropping — so the TPC-H totals in §1 become 31,974 / **1.038**, 7 larger.
> The regression query written to guard it at sf=0.1
> (`query02-region.preql`) uncovered a SECOND, distinct wrong-rows defect and
> is now the registry's one entry: placement is correct, but the ROOT group
> renders as two scans merged on the part key alone, dropping the
> `supplier.id` correlation. Diagnose at the merge join keys.
> **UPDATE 2: the second defect is ALSO fixed.** `query02-region` exposed that
> an aggregate in the WHERE makes the ROOT search decline (the aggregate is a
> terminal no datasource binds), and `gen_root`'s documented fallback then
> sources the un-produced WHERE args in a separate search seeded only with
> their own grain — so `region.name` was materialized at (region, part) grain
> and rejoined on part alone. `_resolve_root_condition_sources` now also seeds
> that search with the node's own row identity (its grain, or its KEY outputs
> when it has none yet), with a fallback when the identity is unbindable.
> Registry back to **0**; TPC-H 29/29; goldens still 109/109; battery 107.
> Final TPC-H totals: 32,110 / **1.043** (q02 3,412 → 3,947).

> **UPDATE 3 (next session): q20 defects 3 and 4 are FIXED.** They were one
> bug and it was not in the CTE optimizer — `_split_root_dimension_clusters`
> peeled a dimension off the fact row bucket only when a SINGLE entity key
> determined it, so `part.available_quantity` (FD by `{part.id,
> part.supplier.id}`, by neither alone) stayed on the row stream and had to be
> deduped back to grain in the sibling bucket. `_composite_determining_grain`
> now peels members determined by a whole downstream d0 grouping grain.
> q20 renders 3 CTEs (was 5), 2,701 → 2,074 chars; the `p_name like 'forest%'`
> predicate also lands in the aggregate scan. Defects 1 and 2 are untouched.
> TPC-DS 109/109 byte-identical (A/B in one process, current tree). TPC-H
> totals on the 23-file basis: 35,456 → **34,829** v4 against 33,023 v3,
> ratio 1.074 → **1.055**. Locked by
> `tests/core/processing/test_v4_composite_dim_peel.py`.

> **UPDATE 4: every symptom count in this report is superseded.**
> `v4_sql_symptoms.py`'s `split_ctes` closed the last CTE at the last `)` in
> the file, so a trailing `LIMIT (100)` swallowed the whole final SELECT into
> it — undercounting, and mis-ranking the per-query list. Now depth-scanned
> (`_close_paren`), verified to produce identical CTE name lists on all 264
> stored SQL files. Corrected totals — TPC-DS v3→v4: `dedup_group` 91→**61**,
> `passthrough` 28→**40**, `split_agg` 43→**51**, `repeat_scan` 113→**66**;
> TPC-H: 5→**7**, 1→**1**, 3→**5**, 15→**13**. The queries that are worse
> under v4 are now q28 +3, q77 +2, q72 +2, then q66/q49/q38/q29/q18/q02-one/
> q02-two +1 (TPC-DS) and q02 +2, q03 +1, q20 +1 (TPC-H). **q81, q30, q30-alt,
> q59, q51, q24, q05, q75 and q23 were artifacts of the bug** — §2's per-query
> table still lists some of them and should not be trusted for symptoms
> (its char deltas are still good).
> Handoffs: `local_scripts/handoff_v4_shape_debt.md`,
> `local_scripts/handoff_v4_search_cost.md`.

## 3b. A correctness defect the size audit surfaced — TPC-H q02

Reading q02's diff for size turned up something bigger. The authored atom
`and supplier.nation.region.name = 'EUROPE'` **has no effect on v4's generated
SQL**: generating with and without that line produces byte-identical SQL under
v4, and different SQL under v3. It survives only inside the unrelated
`min(supply_cost ? … = 'EUROPE')` FILTER expression.

Placement tracing names the host: the atom is placed on
`grp:[@condition]filter:d1:…` — the aggregate's internal filter group — so it
filters the aggregate's INPUT, never the output row population, and FINAL never
re-asserts it.

The battery cannot catch it. Exposure requires a non-European supplier whose
`ps_supplycost` ties the European minimum for a qualifying part; TPC-H's
generator produces zero such rows at sf=0.1 **and** sf=1 (measured). Inject
one and the divergence is immediate:

```sql
INSERT INTO partsupp VALUES (18139, 855, 999, 418.71, 'synthetic tie row');
```
v3 → 44 rows (= `PRAGMA tpch(2)`), v4 → **45 rows**, the extra one an
INDONESIA supplier.

A census over both corpora finds 9 atoms hosted on a `[@condition]filter`
group: tpcds q04, q11 (×2), q30, q30-alt, q74, q81 and tpch q02. The eight
TPC-DS ones are `is not null` guards and subselect memberships that the battery
row-verifies against the references, so the shape is not wrong in itself — q02
is distinguished by the atom being a plain row property of an output-producing
source whose only chosen host is the aggregate's filter scope. Suggested home
for the fix: `_choose_groups` / `_upstream_most` in `condition_placement.py`.

## 4. Symptom counts across both corpora

Mechanical detector over the rendered SQL (`v4_sql_symptoms.py`); higher is
worse. `__final__` selects and window CTEs are excluded from `passthrough`.

| symptom | TPC-DS v3 | TPC-DS v4 | TPC-H v3 | TPC-H v4 |
|---|---:|---:|---:|---:|
| `repeat_scan` (same table scanned in ≥2 CTEs) | 113 | **66** | 11 | **9** |
| `dedup_group` (GROUP BY with no aggregate) | 83 | **59** | 1 | **4** |
| `passthrough` (bare projection off one CTE) | 24 | **36** | 1 | **2** |
| `split_agg` (aggregate CTE over a plain scan CTE) | 33 | **39** | 2 | **4** |

This is the migration in one table. v4's structural promise is real and
measured: **−47 repeated table scans** (the S3 twin-scan family dissolving, as
§0.1 of the design doc predicted) and **−24 dedup GROUP BYs**. Its debt is
equally clear: **+12 unfolded passthrough CTEs and +6 split aggregates** on
TPC-DS, and on TPC-H the debt outweighs the wins.

23 TPC-DS queries carry at least one unfolded passthrough; q77 alone has 9.

## 5. Optimization room, ranked by measured value

1. **Fold the passthrough CTEs (36 on TPC-DS, ~2 on TPC-H).** q77's
   `bewildered` is a literal `SELECT x as x, y as y, z as z FROM cool` — no
   filter, no group, no join, no computation. At ~150–300 chars each this is
   worth roughly 6–10k characters corpus-wide and costs nothing semantically.
   The audit's item 2 ("consolidate elision ownership" — one owner for
   passthrough removal instead of four) is the structural version of this fix,
   and this is the number that justifies it.
2. **Unblock the fold by not splitting the row stream (S2 remnant).** q20, q03,
   q02 and TPC-DS q29/q38/q30/q81 all show the same coupling: an aggregate and
   a same-parent dedup bucket each take the scan as a parent, the scan gets two
   consumers, and neither can fold. Fixing the split fixes `split_agg` and
   `passthrough` together. This is the single highest-value item and the one
   TPC-H makes unavoidable.
3. **Push predicates back onto their scans** (q20's CANADA, q02's EUROPE).
   Currently a filter that could sit in the scan's WHERE is deferred past a
   join. Size cost is modest; execution cost is not.
4. **Fold scalar derivations into the producing scan** (S4/S5, still open from
   the original audit): q72's `date_diff` CTE, q03/q05/q20's
   `extended_price * (1 - discount)` computed one layer above the scan that
   reads both columns.
5. **Narrow the row-level dedup GROUP BY** (TPC-H q05): v4 groups by 8 more
   columns than v3 at the same grain. Rows agree, but every extra grouping
   column is both characters and runtime.
6. **The provider-choice family** (recorded s44/s45, still open): q66's fresh
   `warehouse` scan, q28's row-grain `bucket_id` recompute, q83's final-select
   `item_id` re-join. All three are "read the value off the CTE that already
   has it instead of re-deriving it".

## 6. Generation cost — a regression that is not about size

| suite | v3 generation | v4 generation |
|---|---:|---:|
| TPC-DS 109 queries | 37.0 s | **125.5 s** |
| TPC-H 22 queries | 3.4 s | 0.7 s |

The census attributes **114.9 s of the 126 s to `search_sources` itself** over
400 ROOT requests — the obligation search is essentially the whole v4
generation budget. Three findings, each measured:

- **q05 truncates and says nothing.** Its single search visits `COVER_LIMIT`
  (4,096) covers, takes 36.7 s, and returns `SearchResult(truncated=True)`.
  Nothing in the production path reads that field — see the audit's finding
  A1. The plan is built from a truncated enumeration with no log line.
- **Identical searches are recomputed within one query.** Measured per query:
  q11 runs 7 searches of which only 4 are distinct (4.73 s of its 8.76 s is
  repeat work); q80 1 of 2 (5.37 s of 10.96 s); q23 5 of 7 (4.76 s of 13.80 s).
  A memo keyed on `(terminals, candidate set, conditions)` returning the same
  `SourceSolution` — pure data, no StrategyNodes — would roughly halve
  generation on the slow queries.
- **Branch-and-bound is still the right end state** for q05, and is still open
  from §0.2 of the design doc.

## 7. Reproduction artifacts

- `v4_size_tpcds.tsv`, `v4_size_tpch.tsv` — per-query numbers incl. reference
  SQL size and per-planner generation time.
- `tpcds_sql/`, `tpch_sql/` — `q<label>_v3.sql` / `q<label>_v4.sql` for every
  query, so any row in the tables above can be diffed directly.
- `census.log` — planner census; `dup.log` — duplicate-search measurement.

## 8. Full per-query tables

### TPC-DS (109 variants) - per query, sorted by ratio

| query | v3 | v4 | delta | ratio | CTEs v3->v4 | ref SQL | v4/ref |
|---|---:|---:|---:|---:|:--:|---:|---:|
| 28 | 88,000 | 19,233 | -68,767 | 0.22 | 1->5 | 2056 | 9.35 |
| 61 | 3,532 | 1,872 | -1,660 | 0.53 | 5->0 | 1279 | 1.46 |
| 37 | 1,461 | 1,032 | -429 | 0.71 | 2->1 | 592 | 1.74 |
| 11 | 5,784 | 4,222 | -1,562 | 0.73 | 4->2 | 3006 | 1.40 |
| 09 | 2,994 | 2,261 | -733 | 0.76 | 6->0 | 2446 | 0.92 |
| 16 | 3,627 | 2,765 | -862 | 0.76 | 6->5 | 821 | 3.37 |
| 04 | 8,908 | 6,936 | -1,972 | 0.78 | 4->3 | 4661 | 1.49 |
| 95 | 3,772 | 2,929 | -843 | 0.78 | 6->5 | 1010 | 2.90 |
| 17 | 8,384 | 6,627 | -1,757 | 0.79 | 5->5 | 1588 | 4.17 |
| 25 | 6,921 | 5,494 | -1,427 | 0.79 | 5->5 | 1067 | 5.15 |
| 33 | 4,307 | 3,498 | -809 | 0.81 | 3->2 | 1670 | 2.09 |
| 45 | 2,478 | 2,055 | -423 | 0.83 | 2->1 | 1159 | 1.77 |
| 65 | 2,418 | 2,003 | -415 | 0.83 | 3->2 | 1062 | 1.89 |
| 81 | 8,563 | 7,136 | -1,427 | 0.83 | 4->4 | 1459 | 4.89 |
| 30-alt | 6,669 | 5,572 | -1,097 | 0.84 | 4->4 | - | - |
| 30 | 6,669 | 5,572 | -1,097 | 0.84 | 4->4 | 1507 | 3.70 |
| 84 | 2,778 | 2,331 | -447 | 0.84 | 2->1 | 606 | 3.85 |
| 86 | 2,506 | 2,147 | -359 | 0.86 | 4->3 | 847 | 2.53 |
| 98 | 3,456 | 2,991 | -465 | 0.87 | 5->4 | 811 | 3.69 |
| 41 | 2,368 | 2,085 | -283 | 0.88 | 3->2 | 1166 | 1.79 |
| 69 | 4,451 | 3,895 | -556 | 0.88 | 4->3 | 1435 | 2.71 |
| 87 | 3,614 | 3,197 | -417 | 0.88 | 5->4 | 1163 | 2.75 |
| -1 | 2,435 | 2,174 | -261 | 0.89 | 4->3 | - | - |
| 01 | 1,807 | 1,611 | -196 | 0.89 | 3->2 | 689 | 2.34 |
| 64 | 20,041 | 18,074 | -1,967 | 0.90 | 17->14 | 3783 | 4.78 |
| 08 | 2,922 | 2,660 | -262 | 0.91 | 8->8 | 19155 | 0.14 |
| 36 | 2,587 | 2,365 | -222 | 0.91 | 4->3 | 1844 | 1.28 |
| 18 | 7,684 | 7,101 | -583 | 0.92 | 5->5 | 1593 | 4.46 |
| 94 | 3,433 | 3,240 | -193 | 0.94 | 6->9 | 795 | 4.08 |
| 97-one | 3,010 | 2,837 | -173 | 0.94 | 4->3 | - | - |
| 97-two | 3,026 | 2,853 | -173 | 0.94 | 4->3 | - | - |
| 32 | 1,276 | 1,228 | -48 | 0.96 | 2->2 | 527 | 2.33 |
| 92 | 1,380 | 1,334 | -46 | 0.97 | 2->2 | 555 | 2.40 |
| 67 | 3,741 | 3,662 | -79 | 0.98 | 3->3 | 1224 | 2.99 |
| 05 | 10,416 | 10,313 | -103 | 0.99 | 6->7 | 4238 | 2.43 |
| 23 | 7,673 | 7,586 | -87 | 0.99 | 11->13 | 2441 | 3.11 |
| 56 | 3,417 | 3,382 | -35 | 0.99 | 2->2 | 1857 | 1.82 |
| 60 | 3,343 | 3,308 | -35 | 0.99 | 2->2 | 1599 | 2.07 |
| 70 | 3,363 | 3,314 | -49 | 0.99 | 8->8 | 1258 | 2.63 |
| 76 | 5,340 | 5,295 | -45 | 0.99 | 4->4 | 1708 | 3.10 |
| 78 | 6,495 | 6,408 | -87 | 0.99 | 2->3 | 2512 | 2.55 |
| 01_agg | 973 | 973 | +0 | 1.00 | 0->0 | - | - |
| 02 | 4,861 | 4,861 | +0 | 1.00 | 4->4 | 2566 | 1.89 |
| 03 | 921 | 921 | +0 | 1.00 | 0->0 | 446 | 2.07 |
| 06 | 2,410 | 2,410 | +0 | 1.00 | 2->2 | 654 | 3.69 |
| 07 | 1,648 | 1,648 | +0 | 1.00 | 0->0 | 576 | 2.86 |
| 10 | 7,095 | 7,070 | -25 | 1.00 | 4->4 | 1858 | 3.81 |
| 12 | 2,075 | 2,075 | +0 | 1.00 | 2->2 | 749 | 2.77 |
| 13 | 3,168 | 3,168 | +0 | 1.00 | 0->0 | 2431 | 1.30 |
| 15 | 1,679 | 1,679 | +0 | 1.00 | 0->0 | 811 | 2.07 |
| 19 | 1,836 | 1,836 | +0 | 1.00 | 0->0 | 708 | 2.59 |
| 20 | 1,896 | 1,896 | +0 | 1.00 | 2->2 | 819 | 2.32 |
| 21 | 1,505 | 1,505 | +0 | 1.00 | 0->0 | 1014 | 1.48 |
| 22 | 1,510 | 1,510 | +0 | 1.00 | 1->1 | 486 | 3.11 |
| 26 | 1,400 | 1,400 | +0 | 1.00 | 0->0 | 583 | 2.40 |
| 27 | 2,302 | 2,302 | +0 | 1.00 | 1->1 | 1550 | 1.49 |
| 31 | 5,493 | 5,493 | +0 | 1.00 | 1->1 | 1994 | 2.75 |
| 34 | 2,857 | 2,857 | +0 | 1.00 | 1->1 | 1449 | 1.97 |
| 40 | 1,829 | 1,829 | +0 | 1.00 | 0->0 | 1047 | 1.75 |
| 42 | 1,052 | 1,052 | +0 | 1.00 | 0->0 | 554 | 1.90 |
| 43 | 1,962 | 1,962 | +0 | 1.00 | 0->0 | 1346 | 1.46 |
| 48 | 2,528 | 2,528 | +0 | 1.00 | 0->0 | 1425 | 1.77 |
| 52 | 971 | 971 | +0 | 1.00 | 0->0 | 469 | 2.07 |
| 53 | 2,672 | 2,672 | +0 | 1.00 | 2->2 | 1808 | 1.48 |
| 55 | 855 | 855 | +0 | 1.00 | 0->0 | 346 | 2.47 |
| 62 | 2,160 | 2,160 | +0 | 1.00 | 0->0 | 1421 | 1.52 |
| 63 | 2,617 | 2,617 | +0 | 1.00 | 2->2 | 1795 | 1.46 |
| 71 | 3,615 | 3,615 | +0 | 1.00 | 1->1 | 1399 | 2.58 |
| 73 | 2,905 | 2,905 | +0 | 1.00 | 1->1 | 1437 | 2.02 |
| 83 | 6,907 | 6,902 | -5 | 1.00 | 7->5 | 2195 | 3.14 |
| 85 | 4,239 | 4,239 | +0 | 1.00 | 0->0 | 2166 | 1.96 |
| 88 | 4,108 | 4,108 | +0 | 1.00 | 1->1 | 5658 | 0.73 |
| 89 | 3,964 | 3,964 | +0 | 1.00 | 2->2 | 965 | 4.11 |
| 90 | 1,197 | 1,197 | +0 | 1.00 | 0->0 | 982 | 1.22 |
| 91 | 2,748 | 2,748 | +0 | 1.00 | 0->0 | 982 | 2.80 |
| 93 | 923 | 923 | +0 | 1.00 | 0->0 | 669 | 1.38 |
| 96 | 1,030 | 1,030 | +0 | 1.00 | 0->0 | 382 | 2.70 |
| 97 | 2,313 | 2,313 | +0 | 1.00 | 2->2 | 1159 | 2.00 |
| 99 | 2,595 | 2,595 | +0 | 1.00 | 0->0 | 1484 | 1.75 |
| 35 | 9,853 | 9,911 | +58 | 1.01 | 7->8 | 1745 | 5.68 |
| 79 | 2,415 | 2,450 | +35 | 1.01 | 1->1 | 1188 | 2.06 |
| 39 | 2,086 | 2,135 | +49 | 1.02 | 0->1 | 1546 | 1.38 |
| 14 | 6,246 | 6,419 | +173 | 1.03 | 8->8 | 4229 | 1.52 |
| 24 | 3,940 | 4,046 | +106 | 1.03 | 3->5 | 1264 | 3.20 |
| 82.1 | 1,610 | 1,662 | +52 | 1.03 | 1->1 | - | - |
| 82 | 1,610 | 1,662 | +52 | 1.03 | 1->1 | 590 | 2.82 |
| 44 | 3,480 | 3,623 | +143 | 1.04 | 10->13 | 1462 | 2.48 |
| 58 | 5,105 | 5,358 | +253 | 1.05 | 3->4 | 2250 | 2.38 |
| 49 | 5,391 | 5,770 | +379 | 1.07 | 4->5 | 4352 | 1.33 |
| 02-one | 6,304 | 6,850 | +546 | 1.09 | 4->5 | - | - |
| 02-two | 6,304 | 6,850 | +546 | 1.09 | 4->5 | - | - |
| 57 | 5,081 | 5,557 | +476 | 1.09 | 3->4 | 2122 | 2.62 |
| 47 | 5,394 | 5,917 | +523 | 1.10 | 3->4 | 2473 | 2.39 |
| 54 | 3,757 | 4,155 | +398 | 1.11 | 6->6 | 1699 | 2.45 |
| 51 | 3,399 | 3,812 | +413 | 1.12 | 5->7 | 1999 | 1.91 |
| 66 | 20,318 | 22,778 | +2,460 | 1.12 | 2->4 | 7431 | 3.07 |
| 38 | 2,575 | 2,916 | +341 | 1.13 | 3->3 | 1032 | 2.83 |
| 75 | 6,948 | 7,824 | +876 | 1.13 | 6->7 | 2947 | 2.65 |
| 80 | 6,844 | 7,797 | +953 | 1.14 | 4->4 | 3674 | 2.12 |
| 50 | 4,526 | 5,277 | +751 | 1.17 | 3->3 | 1903 | 2.77 |
| 77 | 9,222 | 10,751 | +1,529 | 1.17 | 17->20 | 3255 | 3.30 |
| 46 | 3,270 | 3,865 | +595 | 1.18 | 2->3 | 1474 | 2.62 |
| 68 | 3,647 | 4,291 | +644 | 1.18 | 3->4 | 1453 | 2.95 |
| 59 | 6,237 | 7,397 | +1,160 | 1.19 | 5->8 | 2234 | 3.31 |
| -2 | 2,373 | 2,873 | +500 | 1.21 | 4->5 | - | - |
| -3 | 2,311 | 2,796 | +485 | 1.21 | 4->5 | - | - |
| 72 | 3,262 | 4,309 | +1,047 | 1.32 | 0->3 | 1360 | 3.17 |
| 74 | 4,091 | 5,451 | +1,360 | 1.33 | 4->5 | 2151 | 2.53 |
| 29 | 3,164 | 4,334 | +1,170 | 1.37 | 1->3 | 1089 | 3.98 |

### TPC-H (22) - per query, sorted by ratio

| query | v3 | v4 | delta | ratio | CTEs v3->v4 | ref SQL | v4/ref |
|---|---:|---:|---:|---:|:--:|---:|---:|
| 14 | 669 | 483 | -186 | 0.72 | 0->0 | 363 | 1.33 |
| 15 | 2,276 | 1,721 | -555 | 0.76 | 5->3 | 560 | 3.07 |
| 17 | 1,270 | 1,018 | -252 | 0.80 | 2->2 | 328 | 3.10 |
| 08 | 1,713 | 1,532 | -181 | 0.89 | 0->0 | 949 | 1.61 |
| 19 | 1,661 | 1,486 | -175 | 0.89 | 0->0 | 1042 | 1.43 |
| 11 | 2,065 | 1,959 | -106 | 0.95 | 5->5 | 581 | 3.37 |
| 18 | 1,329 | 1,278 | -51 | 0.96 | 2->2 | 543 | 2.35 |
| 22 | 1,787 | 1,731 | -56 | 0.97 | 3->3 | 806 | 2.15 |
| 01 | 792 | 792 | +0 | 1.00 | 0->0 | 544 | 1.46 |
| 06 | 314 | 314 | +0 | 1.00 | 0->0 | 242 | 1.30 |
| 07 | 1,450 | 1,450 | +0 | 1.00 | 0->0 | 953 | 1.52 |
| 09 | 1,111 | 1,111 | +0 | 1.00 | 0->0 | 677 | 1.64 |
| 10 | 2,115 | 2,121 | +6 | 1.00 | 2->2 | 579 | 3.66 |
| 12 | 909 | 909 | +0 | 1.00 | 0->0 | 703 | 1.29 |
| 13 | 683 | 683 | +0 | 1.00 | 1->1 | 365 | 1.87 |
| 16 | 1,049 | 1,049 | +0 | 1.00 | 1->1 | 552 | 1.90 |
| 21 | 1,623 | 1,623 | +0 | 1.00 | 3->3 | 802 | 2.02 |
| 03 | 1,304 | 1,544 | +240 | 1.18 | 2->3 | 467 | 3.31 |
| 04 | 682 | 809 | +127 | 1.19 | 1->2 | 407 | 1.99 |
| 05 | 1,482 | 1,849 | +367 | 1.25 | 1->1 | 521 | 3.55 |
| 02 | 2,698 | 3,412 | +714 | 1.26 | 2->5 | 840 | 4.06 |
| 20 | 1,807 | 2,701 | +894 | 1.49 | 2->5 | 908 | 2.97 |

## 9. Implementation quality audit (code-level)

Scope: `v4_helper/network_search.py` (1,658 lines) and `v4_helper/source_planning.py`
(1,497). Gates re-run at audit time: `test_v4_network_search.py` 25/25,
`mypy trilogy` clean over 313 files, `ruff --select E,F,I` clean on `v4_helper`.
The module is genuinely well-factored — one vocabulary (`Obligation`), pure
functions, memo tables that are declared `compare=False` on a frozen dataclass,
and every rule carries the defect that earned it. The findings below are debt,
not disorder.

**A1 — truncation is announced to nobody.** `search_sources` returns
`SearchResult.truncated`; the only reader in the tree is
`local_scripts/s32_network_shadow.py`. Production (`_network_source`) ignores
it. Measured: q05 truncates on every run. Two consequences of different
severity — a truncation *after* covers were found yields a possibly
non-optimal plan, but a truncation *before* the first cover is emitted returns
no solution at all, and the planner then silently falls through to
`_direct_source` / the unconditioned retry. The module docstring and both limit
constants promise "reported, never silent". Make it a log line at minimum.

**A2 — the Stage D contract is unfulfilled.** `SourceSolution` carries
`assignments`, `join_keys`, `partial_terminals` and `completions`; production
reads only `sources` and `connectors`. `_complete_partial_requested` still
*discovers* completions instead of rendering `solution.completions`, and
`_datasource_nodes_for_bridge` still re-derives assignments. So the cost model
prices a quantity the emitter computes independently — they can disagree with
no test noticing. Design §3 ("emit a solution, not a graph") is still open, and
those four fields are currently test-only surface.

**A3 — dead differentiation.** `SolutionCost.dominates` is defined and
documented ("compared as a partial order") but never called — `search_sources`
takes a lexicographic `min`, a leftover from the frontier-based stage C; the
class docstring is now false. `ConditionFit` computes five distinct labels but
only `SENSITIVE` (via `.disqualifying`) and `IMPLIED_EXACT` (via
`.partial_is_full`) are ever read; `APPLIES` / `UNAFFECTED` / `DEFERRED` /
`NEUTRAL` are indistinguishable to every consumer, though §3 of the design
promised them as dominance inputs and a unit test asserts `APPLIES` is
produced. Either wire them in or collapse the enum to the two bits that matter.

**A4 — search cost dominates generation.** See §6: 114.9 s of 126 s. The
enumeration is re-run for identical requests within one query (q11: 3 of 7
searches are exact repeats), and `_reduce` re-invokes `_pending_obligations`
once per drop candidate per cover on top of the enumeration's own per-state
call. A memo on `_pending_obligations` keyed by the chosen frozenset, and a
memo on `search_sources` keyed by `(terminals, candidates, conditions)`, are
both pure-data caches — note the standing rule that build-concept/build-query
caching is off-limits; these cache neither.

**A5 — test coverage gaps in the guardrail suite.** 25 tests, all meaningful,
but nothing covers: the `connected` obligation (the replacement for
`_connect`'s fabrication, and the newest rule), truncation behaviour at
`COVER_LIMIT` / `STATE_LIMIT`, `_row_complete`'s "row-partial may terminate but
not extend a chain" rule, or `_pin_unoffered_probes`. The first two are the
ones a regression would hide in.

**A6 — corpus-dead planners.** The census records zero firings across all 131
queries for `_plan_coalescing_axis`, `_plan_complete_where_source`,
`_plan_finer_filter_rollup` and `_cross_component_source`. That is NOT a
deletion argument — each was landed for a specific non-benchmark shape and the
full suite was not censused here — but it does mean the benchmark corpora
provide zero regression pressure on four pinned code paths. Worth a full-suite
census before any of them is touched.

**A7 — stale companion docs.** `local_scripts/s34_handoff.md` still describes
the post-s38 world (30-entry registry) and is referenced from the design doc as
the live failure list. Its partner `local_scripts/v4_shape_audit.md` — which
held the s39–s49 shape record and the S1–S5 symptom taxonomy — was deleted in
the working-set purge and survives only at
`git show 51cadbc48^:local_scripts/v4_shape_audit.md`. The design doc's §0.10
now carries the current state; the handoff should be retired or refreshed.
