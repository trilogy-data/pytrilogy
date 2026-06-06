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
| v4 | 4217 | 96 | 53.26 ms |
| reference | 1872 | 48 | 20.35 ms |
| v4 / ref | 2.25x | 2.00x | 2.62x |

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
highfalutin as (
SELECT
    "cs_catalog_sales"."CS_ITEM_SK" as "cs_item_id",
    "cs_catalog_sales"."CS_SOLD_DATE_SK" as "cs_sold_date_id"
FROM
    "memory"."catalog_sales" as "cs_catalog_sales"
GROUP BY
    1,
    2),
cooperative as (
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
    "cs_item_items"."I_CATEGORY" in ('Sports','Books','Home') and cast("cs_sold_date_date"."D_DATE" as date) BETWEEN date '1999-02-22' AND date '1999-03-24'

GROUP BY
    1,
    2,
    3,
    4,
    5),
thoughtful as (
SELECT
    "cs_item_items"."I_CATEGORY" as "cs_item_category",
    "cs_item_items"."I_CLASS" as "cs_item_class",
    "cs_item_items"."I_CURRENT_PRICE" as "cs_item_current_price",
    "cs_item_items"."I_ITEM_DESC" as "cs_item_desc",
    "cs_item_items"."I_ITEM_ID" as "cs_item_text_id"
FROM
    "highfalutin"
    INNER JOIN "memory"."item" as "cs_item_items" on "highfalutin"."cs_item_id" = "cs_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."date_dim" as "cs_sold_date_date" on "highfalutin"."cs_sold_date_id" = "cs_sold_date_date"."D_DATE_SK"
WHERE
    "cs_item_items"."I_CATEGORY" in ('Sports','Books','Home') and cast("cs_sold_date_date"."D_DATE" as date) BETWEEN date '1999-02-22' AND date '1999-03-24'
),
uneven as (
SELECT
    "cooperative"."revenue" as "revenue",
    coalesce("cooperative"."cs_item_class","thoughtful"."cs_item_class") as "cs_item_class"
FROM
    "cooperative"
    FULL JOIN "thoughtful" on "cooperative"."cs_item_category" is not distinct from "thoughtful"."cs_item_category" AND "cooperative"."cs_item_class" is not distinct from "thoughtful"."cs_item_class" AND "cooperative"."cs_item_current_price" is not distinct from "thoughtful"."cs_item_current_price" AND "cooperative"."cs_item_desc" is not distinct from "thoughtful"."cs_item_desc" AND "cooperative"."cs_item_text_id" = "thoughtful"."cs_item_text_id"
GROUP BY
    1,
    2,
    coalesce("cooperative"."cs_item_category","thoughtful"."cs_item_category"),
    coalesce("cooperative"."cs_item_current_price","thoughtful"."cs_item_current_price"),
    coalesce("cooperative"."cs_item_desc","thoughtful"."cs_item_desc"),
    coalesce("cooperative"."cs_item_text_id","thoughtful"."cs_item_text_id")),
juicy as (
SELECT
    "uneven"."cs_item_class" as "cs_item_class",
    sum("uneven"."revenue") as "_virt_agg_sum_9832457364876792"
FROM
    "uneven"
GROUP BY
    1),
vacuous as (
SELECT
    "cooperative"."cs_item_category" as "cs_item_category",
    "cooperative"."cs_item_class" as "cs_item_class",
    "cooperative"."cs_item_current_price" as "cs_item_current_price",
    "cooperative"."cs_item_desc" as "cs_item_desc",
    "cooperative"."cs_item_text_id" as "cs_item_text_id",
    "cooperative"."revenue" as "revenue",
    "juicy"."_virt_agg_sum_9832457364876792" as "_virt_agg_sum_9832457364876792"
FROM
    "cooperative"
    INNER JOIN "juicy" on "cooperative"."cs_item_class" is not distinct from "juicy"."cs_item_class")
SELECT
    "vacuous"."cs_item_text_id" as "cs_item_text_id",
    "vacuous"."cs_item_desc" as "cs_item_desc",
    "vacuous"."cs_item_category" as "cs_item_category",
    "vacuous"."cs_item_class" as "cs_item_class",
    "vacuous"."cs_item_current_price" as "cs_item_current_price",
    "vacuous"."revenue" as "revenue",
    ( "vacuous"."revenue" * 100.0 ) / ("vacuous"."_virt_agg_sum_9832457364876792") as "revenue_ratio"
FROM
    "vacuous"
ORDER BY 
    "vacuous"."cs_item_category" asc nulls first,
    "vacuous"."cs_item_class" asc nulls first,
    "vacuous"."cs_item_text_id" asc nulls first,
    "vacuous"."cs_item_desc" asc nulls first,
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
    "cs_item_items"."I_CATEGORY" in ('Sports','Books','Home') and cast("cs_sold_date_date"."D_DATE" as date) BETWEEN date '1999-02-22' AND date '1999-03-24'

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
    "cheerful"."cs_item_class" as "cs_item_class",
    "cheerful"."cs_item_current_price" as "cs_item_current_price",
    "cheerful"."revenue" as "revenue",
    ( "cheerful"."revenue" * 100.0 ) / ("cooperative"."_virt_agg_sum_9832457364876792") as "revenue_ratio"
FROM
    "cheerful"
    INNER JOIN "cooperative" on "cheerful"."cs_item_class" is not distinct from "cooperative"."cs_item_class"
ORDER BY 
    "cheerful"."cs_item_category" asc nulls first,
    "cheerful"."cs_item_class" asc nulls first,
    "cheerful"."cs_item_text_id" asc nulls first,
    "cheerful"."cs_item_desc" asc nulls first,
    "revenue_ratio" asc nulls first
LIMIT (100)
```
