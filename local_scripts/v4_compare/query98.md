# Query 98

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (2521 rows) |
| reference execution | OK (2521 rows) |
| results identical | YES |

## Result comparison

v4 rows: 2521 (2521 distinct)
ref rows: 2521 (2521 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 4360 | 69 | 89.35 ms |
| reference | 2918 | 58 | 70.88 ms |
| v4 / ref | 1.49x | 1.19x | 1.26x |

## Preql

```
import store_sales as store_sales;

where
    store_sales.item.category in ('Sports', 'Books', 'Home')
    and store_sales.date.date between '1999-02-22'::date and '1999-03-24'::date
select
    store_sales.item.name,
    store_sales.item.desc,
    store_sales.item.category,
    store_sales.item.class,
    store_sales.item.current_price,
    sum(store_sales.ext_sales_price) as item_revenue,
    sum(store_sales.ext_sales_price) * 100.0 / (sum(store_sales.ext_sales_price) by store_sales.item.class) as revenueratio,
order by
    store_sales.item.category asc nulls first,
    store_sales.item.class asc nulls first,
    store_sales.item.name asc nulls first,
    store_sales.item.desc asc nulls first,
    revenueratio asc nulls first
;
```

## v4 generated SQL

```sql
WITH 
cheerful as (
SELECT
    "store_sales_item_items"."I_CATEGORY" as "store_sales_item_category",
    "store_sales_item_items"."I_CLASS" as "store_sales_item_class",
    "store_sales_item_items"."I_CURRENT_PRICE" as "store_sales_item_current_price",
    "store_sales_item_items"."I_ITEM_DESC" as "store_sales_item_desc",
    "store_sales_item_items"."I_ITEM_ID" as "store_sales_item_name",
    "store_sales_store_sales"."SS_EXT_SALES_PRICE" as "store_sales_ext_sales_price"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "store_sales_item_items" on "store_sales_store_sales"."SS_ITEM_SK" = "store_sales_item_items"."I_ITEM_SK"
WHERE
    "store_sales_item_items"."I_CATEGORY" in ('Sports','Books','Home') and cast("store_sales_date_date"."D_DATE" as date) BETWEEN date '1999-02-22' AND date '1999-03-24'
),
cooperative as (
SELECT
    "cheerful"."store_sales_item_class" as "store_sales_item_class",
    sum("cheerful"."store_sales_ext_sales_price") as "_virt_agg_sum_7595906549305205"
FROM
    "cheerful"
GROUP BY
    1),
thoughtful as (
SELECT
    "cheerful"."store_sales_item_category" as "store_sales_item_category",
    "cheerful"."store_sales_item_class" as "store_sales_item_class",
    "cheerful"."store_sales_item_current_price" as "store_sales_item_current_price",
    "cheerful"."store_sales_item_desc" as "store_sales_item_desc",
    "cheerful"."store_sales_item_name" as "store_sales_item_name",
    sum("cheerful"."store_sales_ext_sales_price") as "_virt_agg_sum_9873055619986236",
    sum("cheerful"."store_sales_ext_sales_price") as "item_revenue"
FROM
    "cheerful"
GROUP BY
    1,
    2,
    3,
    4,
    5),
questionable as (
SELECT
    "thoughtful"."store_sales_item_category" as "store_sales_item_category",
    "thoughtful"."store_sales_item_current_price" as "store_sales_item_current_price",
    "thoughtful"."store_sales_item_desc" as "store_sales_item_desc",
    "thoughtful"."store_sales_item_name" as "store_sales_item_name",
    ( "thoughtful"."_virt_agg_sum_9873055619986236" * 100.0 ) / ("cooperative"."_virt_agg_sum_7595906549305205") as "revenueratio",
    coalesce("cooperative"."store_sales_item_class","thoughtful"."store_sales_item_class") as "store_sales_item_class"
FROM
    "thoughtful"
    INNER JOIN "cooperative" on "thoughtful"."store_sales_item_class" is not distinct from "cooperative"."store_sales_item_class")
SELECT
    coalesce("questionable"."store_sales_item_name","thoughtful"."store_sales_item_name") as "store_sales_item_name",
    coalesce("questionable"."store_sales_item_desc","thoughtful"."store_sales_item_desc") as "store_sales_item_desc",
    coalesce("questionable"."store_sales_item_category","thoughtful"."store_sales_item_category") as "store_sales_item_category",
    coalesce("questionable"."store_sales_item_class","thoughtful"."store_sales_item_class") as "store_sales_item_class",
    coalesce("questionable"."store_sales_item_current_price","thoughtful"."store_sales_item_current_price") as "store_sales_item_current_price",
    "thoughtful"."item_revenue" as "item_revenue",
    "questionable"."revenueratio" as "revenueratio"
FROM
    "questionable"
    FULL JOIN "thoughtful" on "questionable"."store_sales_item_category" is not distinct from "thoughtful"."store_sales_item_category" AND "questionable"."store_sales_item_class" is not distinct from "thoughtful"."store_sales_item_class" AND "questionable"."store_sales_item_current_price" is not distinct from "thoughtful"."store_sales_item_current_price" AND "questionable"."store_sales_item_desc" is not distinct from "thoughtful"."store_sales_item_desc" AND "questionable"."store_sales_item_name" = "thoughtful"."store_sales_item_name"
ORDER BY 
    coalesce("questionable"."store_sales_item_category","thoughtful"."store_sales_item_category") asc nulls first,
    coalesce("questionable"."store_sales_item_class","thoughtful"."store_sales_item_class") asc nulls first,
    coalesce("questionable"."store_sales_item_name","thoughtful"."store_sales_item_name") asc nulls first,
    coalesce("questionable"."store_sales_item_desc","thoughtful"."store_sales_item_desc") asc nulls first,
    "questionable"."revenueratio" asc nulls first
```

## Reference SQL (zquery log)

```sql
WITH 
cheerful as (
SELECT
    "store_sales_item_items"."I_CATEGORY" as "store_sales_item_category",
    "store_sales_item_items"."I_CLASS" as "store_sales_item_class",
    "store_sales_item_items"."I_CURRENT_PRICE" as "store_sales_item_current_price",
    "store_sales_item_items"."I_ITEM_DESC" as "store_sales_item_desc",
    "store_sales_item_items"."I_ITEM_ID" as "store_sales_item_name",
    "store_sales_store_sales"."SS_EXT_SALES_PRICE" as "store_sales_ext_sales_price"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "store_sales_item_items" on "store_sales_store_sales"."SS_ITEM_SK" = "store_sales_item_items"."I_ITEM_SK"
WHERE
    "store_sales_item_items"."I_CATEGORY" in ('Sports','Books','Home') and cast("store_sales_date_date"."D_DATE" as date) BETWEEN date '1999-02-22' AND date '1999-03-24'
),
cooperative as (
SELECT
    "cheerful"."store_sales_item_class" as "store_sales_item_class",
    sum("cheerful"."store_sales_ext_sales_price") as "_virt_agg_sum_7595906549305205"
FROM
    "cheerful"
GROUP BY
    1),
thoughtful as (
SELECT
    "cheerful"."store_sales_item_category" as "store_sales_item_category",
    "cheerful"."store_sales_item_class" as "store_sales_item_class",
    "cheerful"."store_sales_item_current_price" as "store_sales_item_current_price",
    "cheerful"."store_sales_item_desc" as "store_sales_item_desc",
    "cheerful"."store_sales_item_name" as "store_sales_item_name",
    sum("cheerful"."store_sales_ext_sales_price") as "_virt_agg_sum_9873055619986236",
    sum("cheerful"."store_sales_ext_sales_price") as "item_revenue"
FROM
    "cheerful"
GROUP BY
    1,
    2,
    3,
    4,
    5)
SELECT
    "thoughtful"."store_sales_item_name" as "store_sales_item_name",
    "thoughtful"."store_sales_item_desc" as "store_sales_item_desc",
    "thoughtful"."store_sales_item_category" as "store_sales_item_category",
    coalesce("cooperative"."store_sales_item_class","thoughtful"."store_sales_item_class") as "store_sales_item_class",
    "thoughtful"."store_sales_item_current_price" as "store_sales_item_current_price",
    "thoughtful"."item_revenue" as "item_revenue",
    ( "thoughtful"."_virt_agg_sum_9873055619986236" * 100.0 ) / ("cooperative"."_virt_agg_sum_7595906549305205") as "revenueratio"
FROM
    "thoughtful"
    INNER JOIN "cooperative" on "thoughtful"."store_sales_item_class" is not distinct from "cooperative"."store_sales_item_class"
ORDER BY 
    "thoughtful"."store_sales_item_category" asc nulls first,
    coalesce("cooperative"."store_sales_item_class","thoughtful"."store_sales_item_class") asc nulls first,
    "thoughtful"."store_sales_item_name" asc nulls first,
    "thoughtful"."store_sales_item_desc" asc nulls first,
    "revenueratio" asc nulls first
```
