# Query 20

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (100 rows) |
| reference execution | OK (100 rows) |
| results identical | YES |

## Result comparison

v4 rows: 100 (100 distinct)
ref rows: 100 (100 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 2457 | 59 | 47.46 ms |
| reference | 1996 | 48 | 46.03 ms |
| v4 / ref | 1.23x | 1.23x | 1.03x |

## Preql

```
import catalog_sales as cs;

where
    cs.item.category in ('Sports', 'Books', 'Home')
    and cs.sold_date.date between '1999-02-22'::date and '1999-03-24'::date
select
    cs.item.text_id,
    cs.item.desc,
    cs.item.category,
    cs.item.class,
    cs.item.current_price,
    sum(cs.ext_sales_price) as revenue,
    revenue * 100.0 / (sum(revenue) by cs.item.class) as revenue_ratio,
having
    cs.item.text_id is not null

order by
    cs.item.category asc nulls first,
    cs.item.class asc nulls first,
    cs.item.text_id asc nulls first,
    cs.item.desc asc nulls first,
    revenue_ratio asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
cheerful as (
SELECT
    "cs_item_items"."I_CATEGORY" as "cs_item_category",
    "cs_item_items"."I_CLASS" as "cs_item_class",
    "cs_item_items"."I_CURRENT_PRICE" as "cs_item_current_price",
    "cs_item_items"."I_ITEM_DESC" as "cs_item_desc",
    "cs_item_items"."I_ITEM_ID" as "cs_item_text_id",
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
    1),
questionable as (
SELECT
    "cheerful"."cs_item_category" as "cs_item_category",
    "cheerful"."cs_item_current_price" as "cs_item_current_price",
    "cheerful"."cs_item_desc" as "cs_item_desc",
    "cheerful"."cs_item_text_id" as "cs_item_text_id",
    "cheerful"."revenue" as "revenue",
    "cooperative"."_virt_agg_sum_9832457364876792" as "_virt_agg_sum_9832457364876792",
    coalesce("cheerful"."cs_item_class","cooperative"."cs_item_class") as "cs_item_class"
FROM
    "cheerful"
    INNER JOIN "cooperative" on "cheerful"."cs_item_class" is not distinct from "cooperative"."cs_item_class")
SELECT
    "questionable"."cs_item_text_id" as "cs_item_text_id",
    "questionable"."cs_item_desc" as "cs_item_desc",
    "questionable"."cs_item_category" as "cs_item_category",
    "questionable"."cs_item_class" as "cs_item_class",
    "questionable"."cs_item_current_price" as "cs_item_current_price",
    "questionable"."revenue" as "revenue",
    ( "questionable"."revenue" * 100.0 ) / ("questionable"."_virt_agg_sum_9832457364876792") as "revenue_ratio"
FROM
    "questionable"
ORDER BY 
    "questionable"."cs_item_category" asc nulls first,
    "questionable"."cs_item_class" asc nulls first,
    "questionable"."cs_item_text_id" asc nulls first,
    "questionable"."cs_item_desc" asc nulls first,
    "revenue_ratio" asc nulls first
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
    "cs_item_items"."I_ITEM_ID" as "cs_item_text_id",
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
    "cheerful"."cs_item_text_id" as "cs_item_text_id",
    "cheerful"."cs_item_desc" as "cs_item_desc",
    "cheerful"."cs_item_category" as "cs_item_category",
    coalesce("cheerful"."cs_item_class","cooperative"."cs_item_class") as "cs_item_class",
    "cheerful"."cs_item_current_price" as "cs_item_current_price",
    "cheerful"."revenue" as "revenue",
    ( "cheerful"."revenue" * 100.0 ) / ("cooperative"."_virt_agg_sum_9832457364876792") as "revenue_ratio"
FROM
    "cheerful"
    INNER JOIN "cooperative" on "cheerful"."cs_item_class" is not distinct from "cooperative"."cs_item_class"
ORDER BY 
    "cheerful"."cs_item_category" asc nulls first,
    coalesce("cheerful"."cs_item_class","cooperative"."cs_item_class") asc nulls first,
    "cheerful"."cs_item_text_id" asc nulls first,
    "cheerful"."cs_item_desc" asc nulls first,
    "revenue_ratio" asc nulls first
LIMIT (100)
```
