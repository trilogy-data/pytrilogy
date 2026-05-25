# Query 20

**Status:** `exec_fail`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | FAILED |
| reference execution | OK (100 rows) |

## Result comparison

_at least one side did not produce rows._

## SQL size

| Source | Chars | Lines |
| --- | --- | --- |
| v4 | 3721 | 97 |
| reference | 1984 | 48 |
| v4 / ref | 1.88x | 2.02x |

## Preql

```
import catalog_sales as cs;

where
    cs.item.category in ('Sports', 'Books', 'Home')
    and cs.sold_date.date between '1999-02-22'::date and '1999-03-24'::date
select
    cs.item.name,
    cs.item.desc,
    cs.item.category,
    cs.item.class,
    cs.item.current_price,
    sum(cs.ext_sales_price) as revenue,
    revenue * 100.0 / (sum(revenue) by cs.item.class) as revenue_ratio,
having
    cs.item.name is not null

order by
    cs.item.category asc nulls first,
    cs.item.class asc nulls first,
    cs.item.name asc nulls first,
    cs.item.desc asc nulls first,
    revenue_ratio asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
wakeful as (
SELECT
    "cs_sold_date_date"."D_DATE_SK" as "cs_sold_date_id",
    cast("cs_sold_date_date"."D_DATE" as date) as "cs_sold_date_date"
FROM
    "memory"."date_dim" as "cs_sold_date_date"),
highfalutin as (
SELECT
    "cs_item_items"."I_CATEGORY" as "cs_item_category",
    "cs_item_items"."I_CLASS" as "cs_item_class",
    "cs_item_items"."I_CURRENT_PRICE" as "cs_item_current_price",
    "cs_item_items"."I_ITEM_DESC" as "cs_item_desc",
    "cs_item_items"."I_ITEM_ID" as "cs_item_name",
    "cs_item_items"."I_ITEM_SK" as "cs_item_id"
FROM
    "memory"."item" as "cs_item_items"),
quizzical as (
SELECT
    "cs_catalog_sales"."CS_EXT_SALES_PRICE" as "cs_ext_sales_price",
    "cs_catalog_sales"."CS_ITEM_SK" as "cs_item_id",
    "cs_catalog_sales"."CS_SOLD_DATE_SK" as "cs_sold_date_id"
FROM
    "memory"."catalog_sales" as "cs_catalog_sales"),
cheerful as (
SELECT
    "highfalutin"."cs_item_category" as "cs_item_category",
    "highfalutin"."cs_item_class" as "cs_item_class",
    "highfalutin"."cs_item_current_price" as "cs_item_current_price",
    "highfalutin"."cs_item_desc" as "cs_item_desc",
    "highfalutin"."cs_item_name" as "cs_item_name",
    "quizzical"."cs_ext_sales_price" as "cs_ext_sales_price",
    "wakeful"."cs_sold_date_date" as "cs_sold_date_date"
FROM
    "quizzical"
    INNER JOIN "highfalutin" on "quizzical"."cs_item_id" = "highfalutin"."cs_item_id"
    LEFT OUTER JOIN "wakeful" on "quizzical"."cs_sold_date_id" = "wakeful"."cs_sold_date_id"
WHERE
    "highfalutin"."cs_item_category" in ('Sports','Books','Home') and "wakeful"."cs_sold_date_date" BETWEEN date '1999-02-22' AND date '1999-03-24'
),
thoughtful as (
SELECT
    "cheerful"."cs_item_category" as "cs_item_category",
    "cheerful"."cs_item_class" as "cs_item_class",
    "cheerful"."cs_item_current_price" as "cs_item_current_price",
    "cheerful"."cs_item_desc" as "cs_item_desc",
    "cheerful"."cs_item_name" as "cs_item_name",
    sum("cheerful"."cs_ext_sales_price") as "revenue"
FROM
    "cheerful"
GROUP BY
    1,
    2,
    3,
    4,
    5),
cooperative as (
SELECT
    "cheerful"."cs_item_class" as "cs_item_class",
    sum("thoughtful"."revenue") as "_virt_agg_sum_9832457364876792"
FROM
    "cheerful"
GROUP BY
    1),
questionable as (
SELECT
    "cooperative"."_virt_agg_sum_9832457364876792" as "_virt_agg_sum_9832457364876792",
    "thoughtful"."cs_item_category" as "cs_item_category",
    "thoughtful"."cs_item_current_price" as "cs_item_current_price",
    "thoughtful"."cs_item_desc" as "cs_item_desc",
    "thoughtful"."cs_item_name" as "cs_item_name",
    "thoughtful"."revenue" as "revenue",
    ( "thoughtful"."revenue" * 100.0 ) / ("cooperative"."_virt_agg_sum_9832457364876792") as "revenue_ratio",
    coalesce("cooperative"."cs_item_class","thoughtful"."cs_item_class") as "cs_item_class"
FROM
    "cooperative"
    FULL JOIN "thoughtful" on "cooperative"."cs_item_class" is not distinct from "thoughtful"."cs_item_class")
SELECT
    "questionable"."cs_item_name" as "cs_item_name",
    "questionable"."cs_item_desc" as "cs_item_desc",
    "questionable"."cs_item_category" as "cs_item_category",
    "questionable"."cs_item_class" as "cs_item_class",
    "questionable"."cs_item_current_price" as "cs_item_current_price",
    "questionable"."revenue" as "revenue",
    "questionable"."revenue_ratio" as "revenue_ratio"
FROM
    "questionable"
WHERE
    "questionable"."cs_item_name" is not null

ORDER BY 
    "questionable"."cs_item_category" asc nulls first,
    "questionable"."cs_item_class" asc nulls first,
    "questionable"."cs_item_name" asc nulls first,
    "questionable"."cs_item_desc" asc nulls first,
    "questionable"."revenue_ratio" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
cheerful as (
SELECT
    "cs_item_items"."I_CATEGORY" as "cs_item_category",
    "cs_item_items"."I_CLASS" as "cs_item_class",
    "cs_item_items"."I_CURRENT_PRICE" as "cs_item_current_price",
    "cs_item_items"."I_ITEM_DESC" as "cs_item_desc",
    "cs_item_items"."I_ITEM_ID" as "cs_item_name",
    sum("cs_catalog_sales"."CS_EXT_SALES_PRICE") as "revenue"
FROM
    "memory"."catalog_sales" as "cs_catalog_sales"
    INNER JOIN "memory"."item" as "cs_item_items" on "cs_catalog_sales"."CS_ITEM_SK" = "cs_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."date_dim" as "cs_sold_date_date" on "cs_catalog_sales"."CS_SOLD_DATE_SK" = "cs_sold_date_date"."D_DATE_SK"
WHERE
    "cs_item_items"."I_CATEGORY" in ('Sports','Books','Home') and cast("cs_sold_date_date"."D_DATE" as date) BETWEEN date '1999-02-22' AND date '1999-03-24' and "cs_item_items"."I_ITEM_ID" is not null

GROUP BY
    1,
    2,
    3,
    4,
    5),
cooperative as (
SELECT
    "cheerful"."cs_item_class" as "cs_item_class",
    sum("cheerful"."revenue") as "_virt_agg_sum_9832457364876792"
FROM
    "cheerful"
GROUP BY
    1)
SELECT
    "cheerful"."cs_item_name" as "cs_item_name",
    "cheerful"."cs_item_desc" as "cs_item_desc",
    "cheerful"."cs_item_category" as "cs_item_category",
    coalesce("cheerful"."cs_item_class","cooperative"."cs_item_class") as "cs_item_class",
    "cheerful"."cs_item_current_price" as "cs_item_current_price",
    "cheerful"."revenue" as "revenue",
    ( "cheerful"."revenue" * 100.0 ) / ("cooperative"."_virt_agg_sum_9832457364876792") as "revenue_ratio"
FROM
    "cooperative"
    INNER JOIN "cheerful" on "cooperative"."cs_item_class" is not distinct from "cheerful"."cs_item_class"
ORDER BY 
    "cheerful"."cs_item_category" asc nulls first,
    coalesce("cheerful"."cs_item_class","cooperative"."cs_item_class") asc nulls first,
    "cheerful"."cs_item_name" asc nulls first,
    "cheerful"."cs_item_desc" asc nulls first,
    "revenue_ratio" asc nulls first
LIMIT (100)
```

## v4 execution error

```
Traceback (most recent call last):
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 161, in run_one
    result.v4_rows = execute(con, v4_sql)
                     ~~~~~~~^^^^^^^^^^^^^
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 102, in execute
    cursor = con.execute(sql)
_duckdb.BinderException: Binder Error: Referenced table "thoughtful" not found!
Candidate tables: "cheerful"

LINE 60:     sum("thoughtful"."revenue") as "_virt_agg_sum_9832457364876792"
                 ^
```
