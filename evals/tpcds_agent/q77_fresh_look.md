# q77 catalog arm — fresh-look problem statement

> Written for a fresh investigator. The previous investigation (me) may be
> anchored on a particular fix site; treat the **Hypothesis** section as
> unverified opinion and the **Established facts** as the ground truth. Re-derive
> freely.

## Goal

Make the v4 discovery planner generate a correct query for a TPC-DS-style select
that multiplies a per-group aggregate by a scalar from a separate rowset. The
canonical failing query is **TPC-DS q77** (`tests/modeling/tpc_ds_duckdb/query77.preql`),
but the bug reproduces in a much smaller **standalone single select** (below), so
ignore the multiselect/rollup wrapper for now.

## Reproduction harness

`local_scripts/_v3_v4_compare.py` — parses one preql string, generates SQL via
**v3** (`Dialects.DUCK_DB.default_executor(environment=env).generate_sql(preql)`)
and via **v4** (discovery: `_materialize_for_query` → `search_concepts` →
`compile_sql`), executes both on the duckdb `memory/` dataset, and prints row
counts + `ON 1=1` join counts. Run:

```
PYTHONIOENCODING=utf-8 .venv/Scripts/python.exe local_scripts/_v3_v4_compare.py
```

The embedded `CATALOG_ARM` preql (self-contained — two prereq rowsets + one select):

```
import catalog_sales as cs;
import catalog_returns as cr;
const period_start <- '2000-08-23'::date;
const period_end   <- '2000-09-22'::date;
rowset cr_grouped <- where cr.date.date between period_start and period_end
  select coalesce(cr.call_center.id, -1) as cr_cc_key,
         sum(cr.return_amount) as cr_returns_per_cc;
rowset cr_totals <- select count(cr_grouped.cr_cc_key) as cr_n_groups,
                           sum(cr_grouped.cr_returns_per_cc) as cr_total_returns;
where cs.date.date between period_start and period_end
select
  'catalog channel' as u_channel_c,
  cs.call_center.id as u_id_c,
  sum(cs.ext_sales_price) * cr_totals.cr_n_groups::numeric(15,2)                 as u_sales_c,
  cr_totals.cr_total_returns::numeric(15,2)                                       as u_returns_c,
  sum(cs.net_profit) * cr_totals.cr_n_groups - cr_totals.cr_total_returns::numeric(15,2) as u_profit_c
order by u_id_c asc nulls first;
```

Semantics: group catalog_sales by `cs.call_center.id`; per group, `u_sales_c =
sum(ext_sales) * N` and `u_profit_c = sum(net_profit)*N - total_returns`, where
`N = cr_n_groups` and `total_returns` are **scalars** (1-row aggregates of the
`cr_grouped` rowset). Expected: **one row per call center** (3 ids + a NULL group
= 4 rows), `u_returns_c` the same scalar on every row.

## Established facts (verified)

1. **v3 is correct, v4 is wrong, from the *same* parse/build.**
   ```
   v3:  4 rows, 1=1=2   (correct: ids None/1/2/5, returns=2008328.89 scalar)
   v4: 16 rows, 1=1=4   (wrong: cross-join fan-out, u_id_c decoupled from u_sales_c)
   ```
   Both engines consume the identical author/build concept objects. So whatever
   v4 does wrong, it is **not** caused by parse/build producing different output
   than v3 consumes.

2. **The build concept grains are "inaccurate" but v3 tolerates them.** At build,
   `u_id_c`, `u_sales_c`, `u_profit_c` all carry grain
   `Grain<cs.item.id, cs.order_number>` (the catalog_sales *row* grain), even
   though the select groups to `cs.call_center.id`. The grain of
   `sum(x)*scalar by call_center` is arguably `cs.call_center.id`, not row grain —
   but v3 produces the correct SQL **despite** this grain. v4 does not.

3. **v3's plan shape (target).** v3 builds the cs aggregate once (`abundant`, with
   the grouping-key column AND the sums), cross-joins the 1-row scalar rowset
   (`thoughtful`) **once**, and derives all of `u_id_c/u_sales_c/u_profit_c` in a
   single projection over that:
   ```sql
   abundant   AS (SELECT CS_CALL_CENTER_SK AS cs_call_center_id,
                         sum(CS_EXT_SALES_PRICE) AS _sum_ext,
                         sum(CS_NET_PROFIT)      AS _sum_profit
                  FROM catalog_sales JOIN date_dim ... GROUP BY 1),
   thoughtful AS (SELECT count(...) AS cr_n_groups, sum(...) AS cr_total_returns FROM wakeful),  -- 1 row
   yummy      AS (SELECT abundant._sum_ext * thoughtful.cr_n_groups AS u_sales_c,
                         abundant.cs_call_center_id                 AS u_id_c,
                         (abundant._sum_profit*thoughtful.cr_n_groups) - thoughtful.cr_total_returns AS u_profit_c
                  FROM thoughtful FULL JOIN abundant ON 1=1)            -- scalar broadcast, 1×N
   ```
   `u_id_c` is read from **`abundant`** (the aggregate's grouping-key column).

4. **v4's plan shape (broken).** v4 puts `u_id_c` in one CTE sourced from the raw
   `call_center` scan and `u_sales_c`/`u_profit_c` in another CTE sourced from the
   aggregate, then `FULL JOIN ... ON 1=1` between the two **multi-row** CTEs → N²
   fan-out. (Full SQL: run the harness, or
   `python local_scripts/discovery_v4_compare.py --query 77` and read
   `local_scripts/v4_compare/query77.md`.)

5. **v4 group graph for this select** (probe via `info.group_graph`/`group_attrs`
   after `search_concepts`):
   - `grp:aggregate:d0:cs.call_center.id` — outputs the two sums **and** `id`
     (`cs.call_center.id`, the GROUP BY key).
   - `grp:root:root` — raw scan; also outputs `id`.
   - `grp:basic:…:sig:A` grain `(cs.item.id, cs.order_number)` — outputs `u_id_c`.
   - `grp:basic:…:sig:B` grain `(cs.item.id, cs.order_number)` — outputs
     `u_sales_c`, `u_profit_c`.
   The two BASIC groups are at the **same grain** but in **different buckets**.
   Their stop-signatures (`_stop_signature`, `v4_helper/group_rules.py`):
   - `sig(u_id_c)    = {grp:root}`
   - `sig(u_sales_c) = {grp:aggregate, grp:rowset:cr_totals}`
   Buckets merge only when signatures are **equal**
   (`_partition_by_signature_and_grain`, group_rules.py:336-422), so these never
   merge → two co-grain row streams with no shared join key → `ON 1=1`.

## What's been tried (and reverted)

- **Author-side grain fix** (`models/author.py` `get_select_grain_and_keys`: make a
  row-op containing a bare aggregate resolve to the select grain instead of
  descending into the aggregate's input). This made `u_sales_c`'s grain
  `cs.call_center.id`, which re-bucketed it next to the aggregate and produced the
  correct catalog plan; full v4 `--test-set` was **98/98** and the v3 suite passed
  (**303**). It was **reverted** because of fact #1: v3 is correct from the
  unmodified grain, so changing the shared grain logic to satisfy v4 was judged to
  be patching the wrong layer (masking a v4-planner brittleness rather than fixing
  it). It remains a viable pragmatic fallback if the v4-side fix proves too costly.

- A pile of narrower grain tweaks (FK-key→self, empty-keys→grain, disjoint-grain
  clamp) — each fixed one column but regressed q36 or q49, or got undone at build.
  See git history / `project_q77_analysis` memory if useful, but they're dead ends.

## Hypothesis (UNVERIFIED — the previous investigator's opinion, may be wrong)

The divergence is that v4 sources the aggregate's grouping key (`cs.call_center.id`)
from the **root scan** rather than from the **aggregate group** that re-exposes it,
so a column that merely renames it (`u_id_c`) lands in a separate bucket from the
aggregate-derived columns and cross-joins. A v4-only fix *might* live in the
group-graph bucketing — e.g. attribute an aggregate's grain key to the aggregate
group in `_stop_signature`, and/or stop treating a broadcast (single-row) scalar
rowset as a bucket-splitting source, and/or relax the bucket-merge from
signature-equality to signature-subset. **But** this is planner-core (touches every
query's grouping; q49/q05/q46/q64 multiselect aligns rely on the current
signature-equality), so it carries real regression risk, and the previous
investigator could not find a version that fixed catalog+store without regressing
q49. A fresh investigator should feel free to reject this framing entirely — e.g.
the right fix might be in how the FINAL/merge assembly chooses join keys (it
currently emits `ON 1=1` when two contributors share no real key; cf. the q81 fix
which made a FULL merge INNER and pinned a merge's grain to its grouping
contributors), or somewhere else.

## Pointers

- v4 entry: `trilogy/core/processing/concept_strategies_v4.py` (`search_concepts`,
  `_resolve_multiselect`).
- group graph: `trilogy/core/processing/v4_helper/group_graph.py`
  (`_assign_groups`, `_attach_secondary_members`), `…/group_rules.py`
  (`partition_roots`, `_partition_by_signature_and_grain`, `_stop_signature`).
- strategy assembly: `…/v4_helper/strategy_builder.py` (`_assemble_final_node`).
- merge/join types: `trilogy/core/processing/nodes/merge_node.py`,
  `…/join_resolution.py`, `…/grain_utility.py`.
- grain derivation (shared by parse + build): `models/author.py`
  `get_select_grain_and_keys`; build call site `models/build.py:2543`
  (`__build_concept`).
- validate any change: `python local_scripts/discovery_v4_compare.py --test-set`
  (expect 98/98 baseline, q81 included) + `pytest tests/modeling/tpc_ds_duckdb
  tests/core/processing -m "not adventureworks_execution"`.

## Note: the full q77 also has a store-channel variant of "same shape"

Once the catalog arm is correct, the store arm (`ss_grouped`/`sr_grouped` joined by
a rowset `merge`) shows an analogous cross-join (`u_id_s` and `u_returns_s` not
sharing a join key). Worth confirming with the same v3-vs-v4 harness whether v3
gets the store arm right (it likely does) — if so it's the same class of v4 bug.
