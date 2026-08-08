# Q89 handoff: nested aggregate is computed outside the query's WHERE scope

## Status

Fresh investigation of the enriched TPC-DS run
`evals/tpcds_agent/results/20260806-224551` indicates a possible current Trilogy
correctness defect. Treat the diagnosis below as a thesis to confirm or reject,
not as evidence inherited from an older bug report.

The candidate executes successfully and returns 100 rows, but does not match the
100-row SQL reference. The first 50 rows match exactly. The first differing group
has the same dimensions and monthly total on both sides, but a different monthly
average.

## Prompt

> For store sales in year 1999, restricted to items in either
> ('Books','Electronics','Sports') with class in
> ('computers','stereo','football') or in ('Men','Jewelry','Women') with class in
> ('shirts','birdal','dresses'), report the total of the unit sale price (the
> per-unit price the item actually sold for - not its list price, and not the
> line/extended total) grouped by item category, class, brand, store name, store
> company name, and month of year. Alongside each row include the average of that
> monthly total computed across all months of the year for the same (category,
> brand, store name, store company name). Drop any row whose average is zero.
> Among the remaining rows, keep only those where the absolute difference
> between the row's monthly total and the average, divided by the average, is
> greater than 0.1. Order by (monthly total minus average), then store name, then
> category, class, brand, store company name, month of year, total, and average;
> limit 100 rows.

The exact persisted prompt should remain the source of truth if its wording has
changed since this handoff:
`evals/tpcds_agent/query_prompts.json`, query id 89.

## Full agent-authored Trilogy query

```preql
import raw.store_sales as sales;

auto monthly_total <- sum(sales.sales_price) by sales.item.category, sales.item.class, sales.item.brand_name, sales.store.name, sales.store.company_name, sales.sale_date.month_of_year;

auto monthly_avg <- avg(monthly_total) by sales.item.category, sales.item.brand_name, sales.store.name, sales.store.company_name;

where
  sales.sale_date.year = 1999
  and (
    (sales.item.category in ('Books', 'Electronics', 'Sports') and sales.item.class in ('computers', 'stereo', 'football'))
    or
    (sales.item.category in ('Men', 'Jewelry', 'Women') and sales.item.class in ('shirts', 'birdal', 'dresses'))
  )
  and monthly_avg != 0
  and abs(monthly_total - monthly_avg) / monthly_avg > 0.1
select
  sales.item.category,
  sales.item.class,
  sales.item.brand_name,
  sales.store.name,
  sales.store.company_name,
  sales.sale_date.month_of_year,
  monthly_total,
  monthly_avg,
order by
  (monthly_total - monthly_avg),
  sales.store.name,
  sales.item.category,
  sales.item.class,
  sales.item.brand_name,
  sales.store.company_name,
  sales.sale_date.month_of_year,
  monthly_total,
  monthly_avg
limit 100;
```

Original artifact:
`evals/tpcds_agent/results/20260806-224551/workspace/query89.preql`.

## Canonical SQL reference

```sql
SELECT * from
  (SELECT i_category, i_class, i_brand, s_store_name, s_company_name, d_moy,
          sum(ss_sales_price) sum_sales,
          avg(sum(ss_sales_price)) OVER
            (PARTITION BY i_category, i_brand, s_store_name, s_company_name)
            avg_monthly_sales
   FROM item, store_sales, date_dim, store
   WHERE ss_item_sk = i_item_sk
     AND ss_sold_date_sk = d_date_sk
     AND ss_store_sk = s_store_sk
     AND d_year = 1999
     AND ((i_category IN ('Books','Electronics','Sports')
           AND i_class IN ('computers','stereo','football'))
          OR (i_category IN ('Men','Jewelry','Women')
              AND i_class IN ('shirts','birdal','dresses')))
   GROUP BY i_category, i_class, i_brand, s_store_name, s_company_name, d_moy) tmp1
WHERE CASE
          WHEN (avg_monthly_sales <> 0)
          THEN (abs(sum_sales - avg_monthly_sales) / avg_monthly_sales)
          ELSE NULL
      END > 0.1
ORDER BY sum_sales - avg_monthly_sales,
         s_store_name, 1, 2, 3, 5, 6, 7, 8
LIMIT 100;
```

Reference artifact: `tests/modeling/tpc_ds_duckdb/query89.sql`.

## Concrete discrepancy

For this output key:

```text
category       Men
class          shirts
brand          importoimporto #2
store          bar
company        Unknown
month          7
monthly_total  793.38
```

the results are:

```text
Trilogy candidate monthly_avg: 2779.07125
SQL reference monthly_avg:      2296.6725
```

This is not merely a top-100 ordering difference: the same dimensional row and
monthly total exist in both results with different aggregate values. Across the
limited results, 89 rows are exactly common as a multiset.

## Thesis

The query-level row predicates are not being pushed below the first aggregate.
In particular, `monthly_total` is initially calculated from all years and all
category/class values. The generated plan later joins those totals back to raw
rows and applies the 1999/category predicates. It subsequently reconstructs the
monthly totals and averages through additional aggregate stages. This permits
out-of-scope input rows to affect `monthly_avg`.

The suspicious generated-plan sequence is:

1. `thoughtful` reads store sales with item, store, and date dimensions, with no
   year or category/class filter.
2. `cooperative` computes `sum(sales_price)` grouped by the monthly-total grain,
   still with no filter.
3. `abundant` joins `cooperative` back to `thoughtful` and only then applies
   `sales_sale_date_year = 1999` and the category/class predicate.
4. `questionable` averages the already out-of-scope aggregate from
   `cooperative`.
5. Later CTEs rebuild totals/averages, but the filter placement has already
   changed their population.

Representative generated SQL fragments:

```sql
cooperative as (
  SELECT ..., sum(thoughtful.sales_sales_price) AS monthly_sum
  FROM thoughtful
  GROUP BY category, class, brand, month, company, store
),
abundant as (
  SELECT cooperative.monthly_sum, thoughtful.*
  FROM cooperative
  INNER JOIN thoughtful ON ...
  WHERE thoughtful.sales_sale_date_year = 1999
    AND (...category/class predicate...)
),
questionable as (
  SELECT ..., avg(cooperative.monthly_sum) AS monthly_avg
  FROM cooperative
  GROUP BY category, brand, company, store
)
```

Under the documented query semantics, the ordinary row predicates in `where`
should restrict the rows feeding every aggregate in this query. If that contract
is correct, `cooperative` must not aggregate unfiltered years/categories.

## Reproduction

Use the repository virtual environment from the repository root. For direct
inspection of the persisted candidate, create an executor with:

```python
from pathlib import Path

from evals.common.scoring import make_scoring_engine
from trilogy.core.models.environment import Environment

workspace = Path(
    "evals/tpcds_agent/results/20260806-224551/workspace"
).resolve()
engine = make_scoring_engine(
    workspace / "tpcds.duckdb", workspace, "tpcds"
)
engine.environment = Environment(working_path=workspace)
query = (workspace / "query89.preql").read_text(encoding="utf-8")
sql = engine.generate_sql(query)[-1]
print(sql)
print(engine.execute_raw_sql(sql).fetchall())
```

The database is about 330 MB and is intentionally not copied into this handoff.

## Questions for the next investigator

1. Confirm whether query-level `where` is contractually required to scope both
   `monthly_total` and `monthly_avg` in this expression shape.
2. Reduce the issue to a small in-memory model containing at least two years and
   enough class/month rows for the incorrect average to be observable.
3. Test whether staging the filtered rows in a rowset produces the reference
   result. If it does, determine whether that is merely a workaround or the only
   supported way to express the query.
4. Identify the planner step that schedules `cooperative` before its applicable
   row conditions, and add a regression test around generated SQL and results.
5. Check inner-versus-left dimension joins independently. They may affect null
   dimension rows, but they do not explain the demonstrated non-null group's
   differing average.

## Falsification criteria

Reject the framework-bug thesis if the language contract explicitly says that a
query-level `where` does not scope named aggregates declared before the query,
or if a direct calculation proves `2779.07125` is the correct value for the
prompt's filtered population. In that case this should be reclassified as an
agent-authoring error requiring an explicitly filtered staging rowset.
