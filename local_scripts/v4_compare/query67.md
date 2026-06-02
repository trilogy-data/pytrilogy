# Query 67

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
| v4 | 3634 | 78 | 88.44 ms |
| reference | 3634 | 78 | 89.27 ms |
| v4 / ref | 1.00x | 1.00x | 0.99x |

## Preql

```
import physical_sales as ss;

where
    ss.date.month_seq between 1200 and 1211 and ss.store.id is not null
select
    ss.item.category,
    ss.item.class,
    ss.item.brand_name,
    ss.item.product_name,
    ss.date.year,
    ss.date.quarter,
    ss.date.month_of_year,
    ss.store.text_id,
    sum(coalesce(ss.sales_price * ss.quantity, 0)) by rollup() as sumsales,
    rank() over (partition by ss.item.category order by sumsales desc) as rk,
having
    rk <= 100

order by
    ss.item.category asc nulls first,
    ss.item.class asc nulls first,
    ss.item.brand_name asc nulls first,
    ss.item.product_name asc nulls first,
    ss.date.year asc nulls first,
    ss.date.quarter asc nulls first,
    ss.date.month_of_year asc nulls first,
    ss.store.text_id asc nulls first,
    sumsales asc nulls first,
    rk asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
thoughtful as (
SELECT
    "ss_date_date"."D_MOY" as "ss_date_month_of_year",
    "ss_date_date"."D_QOY" as "ss_date_quarter",
    "ss_date_date"."D_YEAR" as "ss_date_year",
    "ss_item_items"."I_BRAND" as "ss_item_brand_name",
    "ss_item_items"."I_CATEGORY" as "ss_item_category",
    "ss_item_items"."I_CLASS" as "ss_item_class",
    "ss_item_items"."I_PRODUCT_NAME" as "ss_item_product_name",
    "ss_store_sales"."SS_QUANTITY" as "ss_quantity",
    "ss_store_sales"."SS_SALES_PRICE" as "ss_sales_price",
    "ss_store_store"."S_STORE_ID" as "ss_store_text_id"
FROM
    "memory"."store_sales" as "ss_store_sales"
    INNER JOIN "memory"."date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "ss_item_items" on "ss_store_sales"."SS_ITEM_SK" = "ss_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."store" as "ss_store_store" on "ss_store_sales"."SS_STORE_SK" = "ss_store_store"."S_STORE_SK"
WHERE
    "ss_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1211 and "ss_store_sales"."SS_STORE_SK" is not null
),
cooperative as (
SELECT
    "thoughtful"."ss_date_month_of_year" as "ss_date_month_of_year",
    "thoughtful"."ss_date_quarter" as "ss_date_quarter",
    "thoughtful"."ss_date_year" as "ss_date_year",
    "thoughtful"."ss_item_brand_name" as "ss_item_brand_name",
    "thoughtful"."ss_item_category" as "ss_item_category",
    "thoughtful"."ss_item_class" as "ss_item_class",
    "thoughtful"."ss_item_product_name" as "ss_item_product_name",
    "thoughtful"."ss_store_text_id" as "ss_store_text_id",
    sum(coalesce("thoughtful"."ss_sales_price" * "thoughtful"."ss_quantity",0)) as "sumsales"
FROM
    "thoughtful"
GROUP BY
    ROLLUP (5, 6, 4, 7, 3, 2, 1, 8)),
questionable as (
SELECT
    "cooperative"."ss_date_month_of_year" as "ss_date_month_of_year",
    "cooperative"."ss_date_quarter" as "ss_date_quarter",
    "cooperative"."ss_date_year" as "ss_date_year",
    "cooperative"."ss_item_brand_name" as "ss_item_brand_name",
    "cooperative"."ss_item_category" as "ss_item_category",
    "cooperative"."ss_item_class" as "ss_item_class",
    "cooperative"."ss_item_product_name" as "ss_item_product_name",
    "cooperative"."ss_store_text_id" as "ss_store_text_id",
    "cooperative"."sumsales" as "sumsales",
    rank() over (partition by "cooperative"."ss_item_category" order by "cooperative"."sumsales" desc ) as "rk"
FROM
    "cooperative")
SELECT
    "questionable"."ss_item_category" as "ss_item_category",
    "questionable"."ss_item_class" as "ss_item_class",
    "questionable"."ss_item_brand_name" as "ss_item_brand_name",
    "questionable"."ss_item_product_name" as "ss_item_product_name",
    "questionable"."ss_date_year" as "ss_date_year",
    "questionable"."ss_date_quarter" as "ss_date_quarter",
    "questionable"."ss_date_month_of_year" as "ss_date_month_of_year",
    "questionable"."ss_store_text_id" as "ss_store_text_id",
    "questionable"."sumsales" as "sumsales",
    "questionable"."rk" as "rk"
FROM
    "questionable"
WHERE
    "questionable"."rk" <= 100

ORDER BY 
    "questionable"."ss_item_category" asc nulls first,
    "questionable"."ss_item_class" asc nulls first,
    "questionable"."ss_item_brand_name" asc nulls first,
    "questionable"."ss_item_product_name" asc nulls first,
    "questionable"."ss_date_year" asc nulls first,
    "questionable"."ss_date_quarter" asc nulls first,
    "questionable"."ss_date_month_of_year" asc nulls first,
    "questionable"."ss_store_text_id" asc nulls first,
    "questionable"."sumsales" asc nulls first,
    "questionable"."rk" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
thoughtful as (
SELECT
    "ss_date_date"."D_MOY" as "ss_date_month_of_year",
    "ss_date_date"."D_QOY" as "ss_date_quarter",
    "ss_date_date"."D_YEAR" as "ss_date_year",
    "ss_item_items"."I_BRAND" as "ss_item_brand_name",
    "ss_item_items"."I_CATEGORY" as "ss_item_category",
    "ss_item_items"."I_CLASS" as "ss_item_class",
    "ss_item_items"."I_PRODUCT_NAME" as "ss_item_product_name",
    "ss_store_sales"."SS_QUANTITY" as "ss_quantity",
    "ss_store_sales"."SS_SALES_PRICE" as "ss_sales_price",
    "ss_store_store"."S_STORE_ID" as "ss_store_text_id"
FROM
    "memory"."store_sales" as "ss_store_sales"
    INNER JOIN "memory"."date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "ss_item_items" on "ss_store_sales"."SS_ITEM_SK" = "ss_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."store" as "ss_store_store" on "ss_store_sales"."SS_STORE_SK" = "ss_store_store"."S_STORE_SK"
WHERE
    "ss_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1211 and "ss_store_sales"."SS_STORE_SK" is not null
),
cooperative as (
SELECT
    "thoughtful"."ss_date_month_of_year" as "ss_date_month_of_year",
    "thoughtful"."ss_date_quarter" as "ss_date_quarter",
    "thoughtful"."ss_date_year" as "ss_date_year",
    "thoughtful"."ss_item_brand_name" as "ss_item_brand_name",
    "thoughtful"."ss_item_category" as "ss_item_category",
    "thoughtful"."ss_item_class" as "ss_item_class",
    "thoughtful"."ss_item_product_name" as "ss_item_product_name",
    "thoughtful"."ss_store_text_id" as "ss_store_text_id",
    sum(coalesce("thoughtful"."ss_sales_price" * "thoughtful"."ss_quantity",0)) as "sumsales"
FROM
    "thoughtful"
GROUP BY
    ROLLUP (5, 6, 4, 7, 3, 2, 1, 8)),
questionable as (
SELECT
    "cooperative"."ss_date_month_of_year" as "ss_date_month_of_year",
    "cooperative"."ss_date_quarter" as "ss_date_quarter",
    "cooperative"."ss_date_year" as "ss_date_year",
    "cooperative"."ss_item_brand_name" as "ss_item_brand_name",
    "cooperative"."ss_item_category" as "ss_item_category",
    "cooperative"."ss_item_class" as "ss_item_class",
    "cooperative"."ss_item_product_name" as "ss_item_product_name",
    "cooperative"."ss_store_text_id" as "ss_store_text_id",
    "cooperative"."sumsales" as "sumsales",
    rank() over (partition by "cooperative"."ss_item_category" order by "cooperative"."sumsales" desc ) as "rk"
FROM
    "cooperative")
SELECT
    "questionable"."ss_item_category" as "ss_item_category",
    "questionable"."ss_item_class" as "ss_item_class",
    "questionable"."ss_item_brand_name" as "ss_item_brand_name",
    "questionable"."ss_item_product_name" as "ss_item_product_name",
    "questionable"."ss_date_year" as "ss_date_year",
    "questionable"."ss_date_quarter" as "ss_date_quarter",
    "questionable"."ss_date_month_of_year" as "ss_date_month_of_year",
    "questionable"."ss_store_text_id" as "ss_store_text_id",
    "questionable"."sumsales" as "sumsales",
    "questionable"."rk" as "rk"
FROM
    "questionable"
WHERE
    "questionable"."rk" <= 100

ORDER BY 
    "questionable"."ss_item_category" asc nulls first,
    "questionable"."ss_item_class" asc nulls first,
    "questionable"."ss_item_brand_name" asc nulls first,
    "questionable"."ss_item_product_name" asc nulls first,
    "questionable"."ss_date_year" asc nulls first,
    "questionable"."ss_date_quarter" asc nulls first,
    "questionable"."ss_date_month_of_year" asc nulls first,
    "questionable"."ss_store_text_id" asc nulls first,
    "questionable"."sumsales" asc nulls first,
    "questionable"."rk" asc nulls first
LIMIT (100)
```
