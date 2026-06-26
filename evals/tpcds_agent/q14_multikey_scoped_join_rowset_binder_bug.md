# Bug: multi-key scoped join onto a rowset → BinderException "Values list does not have a column named q_bid" (q14)

**Status:** FIXED 2026-06-25. Root cause was deeper than "missing column": for an
INNER scoped join, substitution collapsed the fact key onto the rowset canonical AND
attached the fact's datasource binding to it, so discovery sourced the rowset key from
that raw column and SKIPPED the rowset body (its WHERE filter) entirely. Single-key
emitted valid-but-WRONG SQL (filter silently dropped); multi-key sourced the keys
inconsistently → invalid SQL (`q_bid` BinderException). Fix: INNER-onto-rowset now uses
the identity+pseudonym path (like LEFT/FULL) so the rowset materializes and INNER-joins
as a restriction. `Factory.scoped_rowset_inner_sources` in build.py; excluded from the
`_build_concept` substitution swap and from `scoped_key_merge_map`. Regular fact/dim
INNER joins stay on substitution. Tests: tests/test_scoped_join_rowset_inner_filter.py
(single + multi key, row-level); the two `inner-k_*` rows in
tests/test_rowset_cross_datasource_outer_read.py corrected to the now-restricted answer.

---
*Original report below.*

**Status:** OPEN — confirmed, reproduced live (exact agent query + a minimal form).
**Surfaced by:** TPC-DS q14 enriched eval (run `20260625-183939`, trace line ~6853).
**Severity:** HIGH — generates invalid SQL; blocks the natural "filter to multi-column-qualifying
tuples via a scoped join" shape, forcing the agent onto worse fallbacks.

## Symptom

A scoped join that equates **two or more keys** between the outer query and a rowset:

```trilogy
inner join all_sales.item.brand_id = q.bid
inner join all_sales.item.class_id = q.cid
inner join all_sales.item.category_id = q.catid
```

→ `(_duckdb.BinderException) Binder Error: Values list "questionable" does not have a column
named "q_bid"` — the generated rowset CTE doesn't project the join-key column the join references.

## Reproducing query (exact, from the trace)

```trilogy
import raw.all_sales as all_sales;
auto in_store   <- sum(case when all_sales.channel = 'STORE'   then 1 else 0 end) by all_sales.item.brand_id, all_sales.item.class_id, all_sales.item.category_id;
auto in_catalog <- sum(case when all_sales.channel = 'CATALOG' then 1 else 0 end) by all_sales.item.brand_id, all_sales.item.class_id, all_sales.item.category_id;
auto in_web     <- sum(case when all_sales.channel = 'WEB'     then 1 else 0 end) by all_sales.item.brand_id, all_sales.item.class_id, all_sales.item.category_id;
with q as where all_sales.date.year between 1999 and 2001
  and in_store > 0 and in_catalog > 0 and in_web > 0
select all_sales.item.brand_id as bid, all_sales.item.class_id as cid, all_sales.item.category_id as catid;
select all_sales.channel, all_sales.item.brand_id, all_sales.item.class_id, all_sales.item.category_id,
  sum(all_sales.quantity * all_sales.list_price) as ts, count(all_sales.order_id) as tc
inner join all_sales.item.brand_id = q.bid
inner join all_sales.item.class_id = q.cid
inner join all_sales.item.category_id = q.catid
where all_sales.date.year = 2001 and all_sales.date.month_of_year = 11
limit 5;
```

→ `Binder Error: Values list "questionable" does not have a column named "q_bid"`.

## Likely root cause (family)

Same family as the q11/q23 hidden-column BinderExceptions (`project_outer_coalesce_dim_bridge_binder_bug`,
`project_filter_cte_dropped_metric_binder_bug`): a CTE references a column it doesn't project. Here
it's the **multi-key scoped-join** path — the rowset `q`'s CTE (aliased `questionable`) is joined on
`q.bid`/`q.cid`/`q.catid`, but the generated CTE doesn't carry one of those keys (`q_bid`) in its
output columns. Likely the multi-key join-key projection only registers a subset of the equated
keys when the join target is a rowset.

## Suggested fix

Ensure every key equated in a multi-key scoped join onto a rowset is projected in that rowset's CTE
`output_columns`. Compare against the single-key scoped-join path (which works) to find where the
2nd/3rd key is dropped.

## Repro harness

```python
import sys; sys.path.insert(0, 'evals')
from common import scoring
from pathlib import Path
ws = Path('evals/tpcds_agent/results/20260625-191717/workspace')
eng = scoring.make_scoring_engine(ws / 'tpcds.duckdb', ws, 'tpcds')
sql = eng.generate_sql(open('repro.preql').read())[-1]
eng.execute_raw_sql(sql)   # BinderException
```
