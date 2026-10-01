# TPC-DS agent run 2026-10-01: engine bugs and repros

**Filed 2026-10-01**, from run `20261001-122722` (deepseek-v4-flash, sf=1, 99q, `ingest` +
`enriched` legs, concurrency 3) on branch `extension-row-null-semantics` at `41570e64c`.

| leg | this run | baseline |
|---|---|---|
| enriched | 95/99, 28.3M tokens (6.61M cache-adjusted) | 95/99, 22.0M (5.24M) - run 20260820-153007 |
| ingest | 87/99, 25.1M tokens (6.16M cache-adjusted) | 95/99, 42.6M (8.51M) - 08-19 funnel |

All 14 failures were triaged by re-running the candidate, diffing against
`tests/modeling/tpc_ds_duckdb/queryNN.sql`, and reading the generated SQL. 11 are agent
errors (listed at the bottom). The engine bugs are below, **regressions from this branch
first**. Each was checked on HEAD, `main` (`6da921434`) and `440caabc5` (2026-08-20).

## Running the repros

`model/` holds the ingest leg's auto-generated model (`root/`) and `model_enriched/` the
enriched leg's curated model (`raw/`); both `trilogy.toml`s point at the eval's cached sf=1
DuckDB, `evals/tpcds_agent/.cache/tpcds_sf1.duckdb` (built by any eval run; gitignored).

```bash
cd evals/tpcds_agent/bug_reports_20261001/model
../../../../.venv/Scripts/trilogy run repro_rollup_rank_subtotals.preql --timeout 60
```

Always pass `--timeout`: the runaway repro will otherwise run for many minutes. To compare
against another commit, use a detached worktree and run with `PYTHONPATH=<worktree>`
(confirm with `trilogy.__file__`). Each fix needs a regression test in `tests/` that
does not depend on this folder.

---

## Regressions from this branch

### R1. Rowset projection splits into CTEs rejoined on store only - runaway (q64 timeout)

`model/repro_rowset_runaway.preql`

```
import root.store_sales as ss;
rowset r <- select ss.ticket_number as a, ss.item.item_sk as i, ss.customer.customer_sk as b, ss.store.store_name as d;
select count(r.a) as n;
```

- Expected: 2,880,404 (the `store_sales` row count), one scan.
- HEAD: compiles in 0.1s, execution runs past 90s. The plan splits the projection into
  separate CTEs; `store_name` is read through a `(customer, store)`-grouped CTE and then
  joined back to the fact rows on `store_sk` alone, which fans out to billions of rows.
- main: one scan of `store_sales` FULL-joined to store and customer, 0.3s, correct.
- 440caabc5: compile error (keyless-join guard).
- The ingest model's partial `~?` foreign keys look like part of the trigger.
- In the eval this was q50's leftover probe; q64 ran it and hit the 900s agent budget
  (see H1).

### R2. Rowset projection INNER-joins store/customer, dropping fact rows

`model/repro_rowset_inner_drop.preql` - the same family as R1, with a date column:

```
rowset sale_lines <- select ss.ticket_number as ticket_number, ss.item.item_sk as item_sk,
    ss.customer.customer_sk as customer_sk, ss.date_dim.date as sold_date, ss.store.store_name as store_name;
select count(sale_lines.ticket_number) as n, count(sale_lines.sold_date) as d;
```

- Expected: `(240000, 2750554)` - `d` is the sales rows with a matching `date_dim` row.
- HEAD: `(240000, 2686024)`, 5-14s. Same split shape as R1; the dimension CTE INNER-joins
  `store` and `customer`, dropping sales with no store or customer.
- main: `(240000, 2821780)` - also wrong, but differently: see P4.
- Likely the same root cause as R1; fix together.

### R3. Rollup + rank re-reads a rollup key from its dimension, dropping subtotal rows (q67)

`model/repro_rollup_rank_subtotals.preql`

```
import root.store_sales as ss;
auto total <- coalesce(sum(ss.quantity), 0);
auto rnk <- rank(ss.item.category) over (partition by ss.item.category order by total desc);
select ss.item.category as cat, ss.date_dim.year as yr, total, rnk
where ss.date_dim.year = 2000
by rollup (ss.item.category, ss.date_dim.year);
```

- Expected: 23 rows (including the subtotal rows where `yr` is rolled up to NULL).
- HEAD: 11 rows. The rollup output is RIGHT OUTER JOINed to a `date_dim` subquery filtered
  on `d_year = 2000`, which drops every row where `yr` was rolled up.
- main and 440caabc5: 23 rows; `yr` is read straight off the rollup output.
- The full q67 candidate (8 rollup keys from item, date and store, ranked by
  `coalesce(sum(...))`) shows the same shape at a larger scale: the window CTE keeps only
  `category` and the grouping flags, and the select joins it back to the rollup CTE on
  those columns alone, cross-matching every rollup row at the same level and category.
  Without a `having rnk <= 100` that cross product is what ran away (59s locally vs 1.2s
  with plain `sum`). A plan with rollup keys from a single dimension (item only) is clean.
  Re-check the full q67 shape once the small repro is fixed; it was only checked on HEAD.

---

## Pre-existing on main (not caused by this branch)

### P1. `all_sales` filter applies to only one side of a merged fact (enriched q80)

`model_enriched/repro_all_sales_one_sided_filter.preql`

```
import raw.all_sales as s;
where s.item.current_price > 50 and s.channel_dim_id is not null
select s.channel, sum(s.ext_sales_price) as sales, sum(s.return_amount) as returns;
```

- HEAD: STORE sales 5,139,370,753.81 (every store sale); expected 293,062,856.58. Sales
  and profit about 17x too high, returns correct.
- `s.item.current_price > 50` is applied only to the returns side. The sales side never
  joins `item`, and returns attach to it by a LEFT JOIN, so every sale survives.
- Needs all three: the dimension filter, a filter on a sales-only key (`channel_dim_id`),
  and both a sales measure and a returns measure. Dropping either filter or the returns
  measure gives correct numbers.
- Bisected: 440caabc5 correct, first bad commit `2be34102b` (#659, "Query-generation
  simplification (waves 0-3), planner join fixes").

### P2. Filtered sum over a `union` rowset groups away duplicate lines (ingest q14)

`model/repro_union_filtered_sum.preql`

```
with u as union(
 (where ss.date_dim.year = 2001 and ss.date_dim.moy = 11 select ss.ticket_number as o, ss.item.item_sk as i, ss.quantity as q, ss.list_price as p),
 (where ws.sold_date.year = 2001 and ws.sold_date.moy = 11 select ws.order_number as o, ws.item.item_sk as i, ws.quantity as q, ws.list_price as p)
) -> (o, i, q, p);
select sum(u.q * u.p ? u.q > 0) as s_filtered, sum(u.q * u.p) as s_plain;
```

- Expected: both 441,241,543.45 (no row has `q <= 0`; checked in raw SQL). Actual: 429,907,531.17 vs
  441,241,543.45 on HEAD, main and 440caabc5.
- The filtered aggregate gets an intermediate CTE that `GROUP BY`s only the aggregate's
  inputs (grouping keys, quantity, filtered price), collapsing distinct lines that share
  quantity and price. Deleting that `GROUP BY` from the generated SQL by hand gives the
  correct answer.
- Needs both the union and the filter: the unfiltered sum is right, and the same filtered
  pattern over a single-source rowset is right. Related: the 08-20 fix for
  `count(key ? cond)` deduping to the key's grain (`v4_helper/projection.py`) - the union
  rowset probably lacks a row identity, so the dedup grain is just the inputs.

### P3. Joining on customer + item + ticket drops the customer condition (ingest q17)

`model/repro_join_drops_customer_equal.preql`, `model/repro_join_drops_customer_subset.preql`

```
import root.store_sales as ss;
import root.store_returns as sr;
select count(ss.quantity) as n, count(sr.return_quantity) as m
equal join ss.customer.customer_sk = sr.customer.customer_sk
equal join ss.item.item_sk = sr.item.item_sk
equal join ss.ticket_number = sr.ticket_number;
```

- The rendered join keeps only the item and ticket keys once both of `sr`'s grain keys are
  joined. The `equal join` form returns `n = 274,856`; with the customer key it should be
  204,459. 77,993 store sales in this data have a return carrying a different customer
  key, which is why it matters.
- `equal join` only exists on this branch, but the `subset join` spelling drops the
  customer key the same way on main. Check the rendered SQL's join condition for both.
- In the eval: the q17 candidate, with the agent's missing keys added, returns 29 rows vs
  23; removing the customer condition from the reference SQL also gives exactly 29.

### P4. A rowset projecting a date FULL-joins `date_dim`, adding dateless padding rows (main only)

Same repro as R2. main returns `d = 2,821,780`: one scan, but `date_dim` is FULL-joined, so
the 71,226 dates with no sales are padded in (2,750,554 + 71,226). HEAD replaced this with
R2's different wrong answer; both need fixing. A related shape on HEAD: a plain
`where ss.date_dim.year = ...` query renders `store_sales RIGHT OUTER JOIN date_dim`
(sums unaffected; row counts and date-level counts would be inflated). Also: an
unfiltered `select ss.item.category, ss.date_dim.year, sum(ss.quantity) by rollup(...)`
over the partial `~?` date key returns 284 rows vs 89 - every `date_dim` year is joined in.
That may be the intended region-domain behaviour; confirm or fix.

### P5. Rollup + rank with an aliased aggregate fails to compile

`model/repro_rollup_rank_alias_compile.preql`

```
auto total <- sum(ss.quantity);
auto rnk <- rank(ss.item.category) over (partition by ss.item.category order by total desc);
select ss.item.category as cat, ss.item.class as cls, total as total_q, rnk
by rollup (ss.item.category, ss.item.class) ...
```

Fails with `Undefined concept: _virt_agg_grouping_<hash>` on HEAD, main and 440caabc5.
Without the `as total_q` alias it compiles.

---

## Harness / tooling

### H1. A timed-out `--run-and-delete` probe is left behind in the shared worker directory

q50's agent ran `trilogy file write probe_rowset.preql --run-and-delete`; the run hit the
600s subprocess timeout, so the delete never happened. The file stayed in
`workspace/_worker_0`, the next query on that worker (q64) found it, ran it, and hung
until its own 900s budget ran out. Fix either side: `trilogy file write --run-and-delete`
(`trilogy/scripts/file.py`) should delete in a `finally`/on cancellation, and the eval
harness (`evals/common/main.py`) should clean a worker's directory between queries.

---

## Agent errors (no engine action, but doc candidates)

| query | leg(s) | error |
|---|---|---|
| q50, q59 (both), q64 | ingest, both, ingest | **Recurring trap**: a `union join` on a key is null-safe (pairs NULL with NULL) by design (`docs/subset_union_join_design.md`). q50/q59 kept a NULL-store/customer group the reference's inner join drops; q64 checked for a matching return with `having count(sr.item.item_sk) > 0`, which is never 0 because the merged key is always present. The `correlated-exists` / scoped-join agent docs should say: add `key is not null` to intersect, and count a non-key column of the other side to test existence. |
| q06 | ingest | filtered `category_id is not null` instead of `category is not null` |
| q11, q74 (both) | ingest, both | returned helper columns the prompt didn't ask for |
| q17 | ingest | joined sales to returns on customer only (then also hits P3) |
| q24 | ingest | used the sale's address, not the customer's current address |
| q28 | enriched | counted line items instead of non-null list prices |
| q31 | ingest | `coalesce(sum, 0)` kept a county with no Q3 store sales |
| q83 | ingest | percentage formula off by a factor of 9 |

Run artifacts (gitignored, local only):
`evals/tpcds_agent/results/20261001-122722_{ingest,enriched}_deepseek_deepseek-v4-flash/`.
