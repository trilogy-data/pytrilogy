# Query 98

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (24 rows) |
| reference execution | OK (24 rows) |
| results identical | YES |

## Result comparison

v4 rows: 24 (24 distinct)
ref rows: 24 (24 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 3006 | 59 | 13.67 ms |
| reference | 3517 | 70 | 14.00 ms |
| v4 / ref | 0.85x | 0.84x | 0.98x |

## Preql

```
import physical_sales as physical_sales;

where
    physical_sales.item.category in ('Sports', 'Books', 'Home')
    and physical_sales.date.date between '1999-02-22'::date and '1999-03-24'::date
select
    --physical_sales.item.id,
    physical_sales.item.text_id,
    physical_sales.item.desc,
    physical_sales.item.category,
    physical_sales.item.class,
    physical_sales.item.current_price,
    sum(physical_sales.ext_sales_price) as item_revenue,
    item_revenue * 100.0 / (sum(physical_sales.ext_sales_price) by physical_sales.item.class) as revenueratio,
order by
    physical_sales.item.category asc nulls first,
    physical_sales.item.class asc nulls first,
    physical_sales.item.text_id asc nulls first,
    physical_sales.item.desc asc nulls first,
    revenueratio asc nulls first
;
```

## v4 generated SQL

```sql
WITH 
cheerful as (
SELECT
    "physical_sales_item_items"."I_CATEGORY" as "physical_sales_item_category",
    "physical_sales_item_items"."I_CLASS" as "physical_sales_item_class",
    "physical_sales_item_items"."I_CURRENT_PRICE" as "physical_sales_item_current_price",
    "physical_sales_item_items"."I_ITEM_DESC" as "physical_sales_item_desc",
    "physical_sales_item_items"."I_ITEM_ID" as "physical_sales_item_text_id",
    "physical_sales_item_items"."I_ITEM_SK" as "physical_sales_item_id",
    "physical_sales_store_sales"."SS_EXT_SALES_PRICE" as "physical_sales_ext_sales_price"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "physical_sales_date_date" on "physical_sales_store_sales"."SS_SOLD_DATE_SK" = "physical_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "physical_sales_item_items" on "physical_sales_store_sales"."SS_ITEM_SK" = "physical_sales_item_items"."I_ITEM_SK"
WHERE
    "physical_sales_item_items"."I_CATEGORY" in ('Sports','Books','Home') and cast("physical_sales_date_date"."D_DATE" as date) BETWEEN date '1999-02-22' AND date '1999-03-24'
),
cooperative as (
SELECT
    "cheerful"."physical_sales_item_class" as "physical_sales_item_class",
    sum("cheerful"."physical_sales_ext_sales_price") as "_virt_agg_sum_9925573558789696"
FROM
    "cheerful"
GROUP BY
    1),
thoughtful as (
SELECT
    "cheerful"."physical_sales_item_category" as "physical_sales_item_category",
    "cheerful"."physical_sales_item_class" as "physical_sales_item_class",
    "cheerful"."physical_sales_item_current_price" as "physical_sales_item_current_price",
    "cheerful"."physical_sales_item_desc" as "physical_sales_item_desc",
    "cheerful"."physical_sales_item_text_id" as "physical_sales_item_text_id",
    sum("cheerful"."physical_sales_ext_sales_price") as "item_revenue"
FROM
    "cheerful"
GROUP BY
    1,
    2,
    3,
    4,
    5,
    "cheerful"."physical_sales_item_id")
SELECT
    "thoughtful"."item_revenue" as "item_revenue",
    ( "thoughtful"."item_revenue" * 100.0 ) / ("cooperative"."_virt_agg_sum_9925573558789696") as "revenueratio",
    "thoughtful"."physical_sales_item_category" as "physical_sales_item_category",
    "thoughtful"."physical_sales_item_class" as "physical_sales_item_class",
    "thoughtful"."physical_sales_item_current_price" as "physical_sales_item_current_price",
    "thoughtful"."physical_sales_item_desc" as "physical_sales_item_desc",
    "thoughtful"."physical_sales_item_text_id" as "physical_sales_item_text_id"
FROM
    "cooperative"
    RIGHT OUTER JOIN "thoughtful" on "cooperative"."physical_sales_item_class" is not distinct from "thoughtful"."physical_sales_item_class"
ORDER BY 
    "thoughtful"."physical_sales_item_category" asc nulls first,
    "thoughtful"."physical_sales_item_class" asc nulls first,
    "thoughtful"."physical_sales_item_text_id" asc nulls first,
    "thoughtful"."physical_sales_item_desc" asc nulls first,
    "revenueratio" asc nulls first
```

## Reference SQL (zquery log)

```sql
WITH 
cheerful as (
SELECT
    "physical_sales_item_items"."I_CLASS" as "physical_sales_item_class",
    "physical_sales_item_items"."I_ITEM_SK" as "physical_sales_item_id",
    "physical_sales_store_sales"."SS_EXT_SALES_PRICE" as "physical_sales_ext_sales_price"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "physical_sales_date_date" on "physical_sales_store_sales"."SS_SOLD_DATE_SK" = "physical_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "physical_sales_item_items" on "physical_sales_store_sales"."SS_ITEM_SK" = "physical_sales_item_items"."I_ITEM_SK"
WHERE
    "physical_sales_item_items"."I_CATEGORY" in ('Sports','Books','Home') and cast("physical_sales_date_date"."D_DATE" as date) BETWEEN date '1999-02-22' AND date '1999-03-24'
),
questionable as (
SELECT
    "physical_sales_item_items"."I_CATEGORY" as "physical_sales_item_category",
    "physical_sales_item_items"."I_CLASS" as "physical_sales_item_class",
    "physical_sales_item_items"."I_CURRENT_PRICE" as "physical_sales_item_current_price",
    "physical_sales_item_items"."I_ITEM_DESC" as "physical_sales_item_desc",
    "physical_sales_item_items"."I_ITEM_ID" as "physical_sales_item_text_id",
    "physical_sales_item_items"."I_ITEM_SK" as "physical_sales_item_id"
FROM
    "memory"."item" as "physical_sales_item_items"
WHERE
    "physical_sales_item_items"."I_CATEGORY" in ('Sports','Books','Home')
),
cooperative as (
SELECT
    "cheerful"."physical_sales_item_id" as "physical_sales_item_id",
    sum("cheerful"."physical_sales_ext_sales_price") as "item_revenue"
FROM
    "cheerful"
GROUP BY
    1),
thoughtful as (
SELECT
    "cheerful"."physical_sales_item_class" as "physical_sales_item_class",
    sum("cheerful"."physical_sales_ext_sales_price") as "_virt_agg_sum_9925573558789696"
FROM
    "cheerful"
GROUP BY
    1),
abundant as (
SELECT
    "cooperative"."item_revenue" as "item_revenue",
    "questionable"."physical_sales_item_category" as "physical_sales_item_category",
    "questionable"."physical_sales_item_class" as "physical_sales_item_class",
    "questionable"."physical_sales_item_current_price" as "physical_sales_item_current_price",
    "questionable"."physical_sales_item_desc" as "physical_sales_item_desc",
    "questionable"."physical_sales_item_text_id" as "physical_sales_item_text_id"
FROM
    "questionable"
    INNER JOIN "cooperative" on "questionable"."physical_sales_item_id" = "cooperative"."physical_sales_item_id")
SELECT
    "abundant"."physical_sales_item_text_id" as "physical_sales_item_text_id",
    "abundant"."physical_sales_item_desc" as "physical_sales_item_desc",
    "abundant"."physical_sales_item_category" as "physical_sales_item_category",
    "abundant"."physical_sales_item_class" as "physical_sales_item_class",
    "abundant"."physical_sales_item_current_price" as "physical_sales_item_current_price",
    "abundant"."item_revenue" as "item_revenue",
    ( "abundant"."item_revenue" * 100.0 ) / ("thoughtful"."_virt_agg_sum_9925573558789696") as "revenueratio"
FROM
    "abundant"
    LEFT OUTER JOIN "thoughtful" on "abundant"."physical_sales_item_class" is not distinct from "thoughtful"."physical_sales_item_class"
ORDER BY 
    "abundant"."physical_sales_item_category" asc nulls first,
    "abundant"."physical_sales_item_class" asc nulls first,
    "abundant"."physical_sales_item_text_id" asc nulls first,
    "abundant"."physical_sales_item_desc" asc nulls first,
    "revenueratio" asc nulls first
```
