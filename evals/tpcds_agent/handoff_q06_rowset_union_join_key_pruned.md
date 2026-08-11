# Handoff: q06 prunes a rowset key required by `union join`

**Open as of 2026-08-10.** This is independent of aggregate-datasource
selection: the failing query selects only the ordinary `store_sales` and item
datasources.

## Summary

The q06 agent authored a category-average rowset and explicitly joined it back
to sales by category:

```preql
rowset cat_avg <-
where item.category is not null
select
    item.category as category,
    avg(item.current_price) as cat_avg_price;

select ...
union join ss.item.category = cat_avg.category;
```

Trilogy correctly groups the average CTE by item category, but prunes the
category projection before rendering the later join. The generated join then
loses its predicate entirely:

```sql
quizzical AS (
    SELECT avg(item.I_CURRENT_PRICE) AS cat_avg_price
    FROM item
    WHERE item.I_CATEGORY IS NOT NULL
    GROUP BY item.I_CATEGORY
),
concerned AS (
    ...
    FROM quizzical
    FULL JOIN yummy ON 1=1
)
```

Every qualifying sale is consequently compared with every category average.
On the SF=1 evaluation database this returns 51 states/state rows rather than
the reference's 46.

## Standalone reproduction

The reproduction uses the checked-in enriched semantic model and only
generates SQL:

```powershell
.venv\Scripts\python.exe evals\tpcds_agent\repro_q06_rowset_union_join_key_pruned.py
```

It asserts that the category-average grouping exists while the downstream
`FULL JOIN` has degraded to `ON 1=1`.

## Aggregate-path control

The original failing artifact is:

```text
evals/tpcds_agent/results/20260810-200843_enriched_aggregates/workspace/query06.preql
```

Its generated SQL references `store_sales`, not
`agg_store_sales_daily`. Running that exact program against the
`enriched_noise` workspace, which has no aggregate datasource bindings,
reproduces the same 51 rows. The failure is therefore not caused by aggregate
substitution.

The independently authored noise candidate avoids the defect by expressing
the category qualification as membership in a keyed item rowset; it returns
the correct 46 rows.

## Fresh-run confirmation

The renamed-table/cumulative-noise run reproduced the same defect through a
second authoring shape:

```text
evals/tpcds_agent/results/20260810-211903_enriched_noise/workspace/query06.preql
```

That candidate defines `auto cat_avg <- avg(item_dedup.cp) by
item_dedup.category` and relies on the grouped key to relate the category
average to each sale. Generated SQL again loses the key and emits `RIGHT OUTER
JOIN ... ON 1=1`, returning 51 rows. It reads `fact_store_sales` directly and
does not select a `fact_agg_*` datasource.

The independently authored `enriched_aggregates` candidate in the same run
uses an explicit keyed `subset join`; its SQL retains the category predicate
and returns the correct 46 rows. This fresh A/B is additional evidence that
aggregate materialization is not causal: candidate shape determines whether
the optimizer hits the key-liveness defect.

## Likely root area

The hidden-concept pass removes `cat_avg.category` from the grouped rowset even
though the scoped join still requires it. Inspect rowset output liveness and
join-key exposure around:

- `HideUnusedConcepts` (the debug trace reports the category aliases as hidden);
- scoped/coalescing join key propagation; and
- rendering of `union join` when one side's key has been pruned.

This resembles a liveness/dependency accounting issue more than join
inference: the parser retains the explicit join, but its right-hand key is no
longer available when the CTE graph is rendered.

## Desired behavior

`cat_avg.category` must remain projected from the grouped rowset and the final
SQL must join it to `ss.item.category`. If a required scoped-join key cannot be
rendered, generation should fail with a semantic error rather than silently
weakening the predicate to `ON 1=1`.

Regression coverage should assert both that the category key survives the
average CTE and that no predicate-bearing scoped join becomes an unconditional
join after optimization.
