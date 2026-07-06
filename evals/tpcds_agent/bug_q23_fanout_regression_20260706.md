# q23 token sink 195k→1.08M — SILENT wrong-result: `sum(x) by k ? <off-grain filter>` ignores the filter on the aggregated rows (returns LIFETIME, not windowed)

**Run:** `evals/tpcds_agent/results/20260706-222300` (q23 FAIL, ref 4 rows / cand 16 rows, 1.08M tokens).
**Prior run:** `20260706-135542_enriched` (q23 FAIL, 100 rows, 195k tokens, 38 turns).
**Class:** REAL framework bug, **SILENT** (runs clean, exit 0, wrong rows). **Pre-existing — NOT a regression from `4e69c5547`.**

## TL;DR

- The candidate's 16 rows are largely **disjoint** from the reference's 4 (only `Collins/Gordon/2025.60`
  overlaps). This is NOT a fan-out / superset — it is a **wrong best-customer set**.
- Root: the candidate computes "best customers" with
  `store_total_by_cust <- sum(ss.quantity*ss.sales_price) by ss.customer.id ? ss.date.year in (2000..2003) and ss.customer.id is not null`.
  This parses as `FilterItem(content=AggregateWrapper(sum(...) by customer.id), where=date.year in (...))`.
  The engine computes the SUM over **all rows (lifetime)** and applies the `date.year` filter as a
  **post-aggregation CASE gate** at the (customer, year) grain — so the "windowed" total is really the
  customer's lifetime total. This inflates the global max **236266.51 → 282421.67**, which raises the
  `0.5*max` threshold, shrinking the best-customer set to a **wrong subset** (1679 ⊂ the correct 4703) →
  16 rows instead of 4.
- The `4e69c5547` commit is entirely EXCEPT/INTERSECT plumbing + value-list membership type-checks +
  ORDER BY substitution — **none on this codepath**. Verified by worktree A/B: the parent commit
  `1c7fed75a` produces the **identical** bug value 282421.67. The token jump is **agent query variation**,
  not a framework regression (see §5).

## Symptom / minimal repro (deterministic, `generate_sql` only)

Harness: `make_scoring_engine(ws/'tpcds.duckdb', ws, 'tpcds')` over the run workspace (copy the DB out first —
the live file is locked).

```
import raw.store_sales as ss;
auto t <- sum(ss.quantity*ss.sales_price) by ss.customer.id
          ? ss.date.year in (2000,2001,2002,2003) and ss.customer.id is not null;
auto m <- max(t) by *;
select m;
```
Returns **282421.67**. Correct windowed max (raw SQL, non-null customers) = **236266.51**.
282421.67 == the raw **lifetime** (all-years) max — proof the year filter was dropped from the SUM.

### The smoking-gun generated SQL

```sql
thoughtful as (                                    -- the aggregate node
  SELECT SS_CUSTOMER_SK as ss_customer_id,
         sum(SS_QUANTITY * SS_SALES_PRICE) as _virt_agg_sum  -- NO date filter, NO date join
  FROM store_sales GROUP BY 1),
cheerful as (                                      -- distinct (year, customer), joins date_dim
  SELECT D_YEAR as ss_date_year, ss_customer_id
  FROM wakeful LEFT JOIN date_dim ON ... GROUP BY 1,2),
cooperative as (                                   -- fan customer -> (customer, year), carrying LIFETIME sum
  SELECT ss_customer_id, ss_date_year, _virt_agg_sum
  FROM cheerful INNER JOIN thoughtful ON ss_customer_id is not distinct from ss_customer_id),
questionable as (                                  -- filter applied to the RESULT, not the rows
  SELECT ss_customer_id,
         CASE WHEN ss_date_year in (2000,2001,2002,2003) AND ss_customer_id is not null
              THEN _virt_agg_sum ELSE NULL END as t
  FROM cooperative)
-- final GROUP BY ss_customer_id, t
```
`thoughtful` sums lifetime; the `? year` predicate becomes a `CASE WHEN` on the aggregate *result* at a
finer (customer, year) grain. Any customer with ≥1 in-window row emits their **full lifetime** total.

## Trigger matrix (global max over customers; correct windowed non-null = 236266.51)

| # | form | result | correct? |
|---|---|---|---|
| A | `sum(x) by k ? <dim filter>` (filter OUTSIDE agg, needs date join) | **282421.67 (lifetime)** | ❌ **BUG** |
| F | `sum(x ? cond) by k` (filter INSIDE the aggregate) | 236266.51 | ✅ |
| B | rowset `where cond select k, sum(x)` (WHERE, pre-aggregation) | 236266.51 | ✅ |
| — | `? qty>0` (base-col filter, no join) via form A | still lifetime | ❌ (filter ignored) |

**Necessary trigger:** a `FilterItem` whose `content` is an `AggregateWrapper` with an explicit `by k`, and
whose `where` references a concept **not functionally determined by `k`** (here `date.year`, finer than
`customer.id`). The two equivalent idioms (F filter-inside, B rowset-WHERE) both compute the right windowed
value — so this is a silent footgun on one accepted spelling, not a missing feature.

## Why 16 vs 4 (end to end)

- Candidate `global_max_store <- max(store_total_by_cust) by *` = 282421.67 (should be 236266.51).
  The `by *` global-reduction form is **correct** (per `bug_q23_bare_max_sibling_rowset_having_tautology.md`),
  so that report's tautology bug is NOT what's firing here — the defect is upstream, in `store_total_by_cust`.
- `is_best_cust`: `store_total_by_cust > 0.5*global_max_store`. Threshold 0.5×282421=141210 (should be 118133).
  Candidate best set = **1679 customers**, a strict subset of the correct 4703 (overlap 1679, cand_only 0).
- Final Feb-2000 catalog+web sums over that wrong-narrower customer set → 16 (last,first) groups, only one of
  which (`Collins/Gordon`) coincides with the reference's 4. Removing the frequent-item filter gives 33 rows,
  removing the best-customer filter gives 100 (limit) — both filters ARE applied; the wrongness is entirely in
  the best-customer *definition* value, driven by the dropped year filter.
- The all-channel vs STORE-only frequent-items difference (candidate omits `channel='STORE'` on the triple
  count) is a real modeling deviation but is **not** the cause: forcing STORE-only leaves the result at 16.

## §5 — Causation: PRE-EXISTING, not `4e69c5547`

1. **Worktree A/B.** Ran form A against `git worktree` at `4e69c5547^` = `1c7fed75a` (parent): identical
   282421.67. The bug predates the commit.
2. **Diff is off-path.** Full read of `4e69c5547`: EXCEPT/INTERSECT set-operator plumbing
   (`union_node.py`, `union_select_node.py`, `_resolve_union_select`, `enums.SetOperator`,
   `query_processor.py:373 operator=set_operator.value`), the two `predicate_pushdown.py`/`union_dim_pushdown.py`
   guards `if parent_cte.operator != SetOperator.UNION_ALL.value: return False` (no-op for the `all_sales`
   merged-datasource union, whose `UnionCTE.operator == "UNION ALL"`), a value-list membership type-check
   (`author.py Comparison`, TupleWrapper branch — q23's `year in (...)` / `channel in (...)` are type-compatible,
   no error), and ORDER BY expression substitution (`select_finalize._substitute_order_by_outputs`). None touch
   filtered-aggregate / FilterItem-over-aggregate planning.
3. **The token jump is agent variation.** Old run (195k, 38 turns, **0** framework errors): agent used a
   **lifetime** best-customer sum (farewell: "lifetime sum(quantity×sales_price) > 50% of max"), matching the
   reference's own lifetime `best_ss_customer`, and stopped at 100 rows. New run (1.08M, 93 turns, only **3**
   syntax errors — `Syntax [211]`×2, `[223]`×1, no hard-error thrash): agent switched to a **windowed**
   `? year in (...)` filtered aggregate, silently got lifetime totals, and iterated for 90+ turns unable to
   diagnose a result that runs clean but is wrong. The churn = a long silent-wrong-result search × deepseek
   uncached-prefix replay (same replay dynamics documented in `bug_q23_churn_014126.md`), not a new sentinel.

## Root cause (file area)

`trilogy/core/processing/node_generators/filter_node.py` — `_aggregate_filter_parent_concepts` (L75-90) and
`pushdown_filter_to_parent` (L114-146). When a `FilterItem.content` is an `AggregateWrapper` (`sum(x) by k`),
the filter node materializes the aggregate at grain `k` and applies `where` at the filter node's (finer) row
grain — joining the off-grain dimension (`date.year`) back in and gating the *aggregate result*. The `where`
predicate is never pushed into the aggregate's **input** rows (which is what F/B do). `pushdown_filter_to_parent`
only pushes scalar predicates into the source WHERE and deliberately keeps a per-row CASE otherwise (the
`edge__function__filtered_aggregates` empty-group guard, memory `project_filtered_aggregate_disjoint_pushdown`);
that guard is correct for `sum(x ? cond) by k` but here the CASE lands on the wrong side of the aggregate
because the filter wraps the aggregate rather than its content. The build-time lowering of
`FilterItem(content=AggregateWrapper, where=...)` (`trilogy/core/models/build.py` filter/aggregate factory) should
either push the `where` onto the aggregate's source rows when `where` references concepts finer than the `by`
grain, or reject the construct — instead it silently mis-grains.

## Prior-report reconciliation (which still apply on the current engine)

- `bug_q23_regression_20260706_invalid_reference.md` (INVALID_REFERENCE + Ambiguous self-join): **FIXED**,
  did not recur (log has no such signatures).
- `bug_q23_rowsetnode_have_need_214830.md` (Have/need on filter-only rowset output): **not triggered** this run.
- `bug_q23_bare_max_sibling_rowset_having_tautology.md` / `...grainless_scalar_max...`: the candidate correctly
  uses `max(...) by *`, so the bare-max tautology is **not** the defect here.
- `bug_q23_churn_014126.md` (`--` not a comment; replay-cost churn): the replay-cost framing still holds for the
  token total; not the score-determining defect.
- **This filtered-aggregate silent bug is distinct from all of the above** and is the score-determining defect
  for this run.
