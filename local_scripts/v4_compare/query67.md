# Query 67

**Status:** `mismatch`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (100 rows) |
| reference execution | OK (100 rows) |
| results identical | NO |

## Result comparison

v4 rows: 100 (83 distinct)
ref rows: 100 (83 distinct)
only in v4 (showing up to 5 of 83):
  1x  (1, None, None, None, None, None, None, None, None, None)
  1x  (2, None, None, None, None, None, None, None, None, None)
  1x  (3, None, None, None, None, None, None, None, None, None)
  1x  (4, None, None, None, None, None, None, None, None, None)
  1x  (72, None, None, None, None, None, None, None, None, None)
only in ref (showing up to 5 of 83):
  1x  (72, None, None, None, None, None, None, None, None, Decimal('104996.99'))
  1x  (4, None, None, None, None, None, None, None, None, Decimal('582893.38'))
  1x  (3, None, None, None, None, None, None, None, None, Decimal('1641694.80'))
  1x  (2, None, None, None, None, None, None, None, None, Decimal('3304196.14'))
  1x  (1, None, None, None, None, None, None, None, None, Decimal('1018289131.65'))

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 4944 | 78 | 1.008 s |
| reference | 3634 | 78 | 1.224 s |
| v4 / ref | 1.36x | 1.00x | 0.82x |

## Preql

```
import store_sales as ss;

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
    rank() over (partition by "cooperative"."ss_item_category" order by "cooperative"."sumsales" desc ) as "rk"
FROM
    "cooperative")
SELECT
    coalesce("cooperative"."ss_item_category","questionable"."ss_item_category") as "ss_item_category",
    coalesce("cooperative"."ss_item_class","questionable"."ss_item_class") as "ss_item_class",
    coalesce("cooperative"."ss_item_brand_name","questionable"."ss_item_brand_name") as "ss_item_brand_name",
    coalesce("cooperative"."ss_item_product_name","questionable"."ss_item_product_name") as "ss_item_product_name",
    coalesce("cooperative"."ss_date_year","questionable"."ss_date_year") as "ss_date_year",
    coalesce("cooperative"."ss_date_quarter","questionable"."ss_date_quarter") as "ss_date_quarter",
    coalesce("cooperative"."ss_date_month_of_year","questionable"."ss_date_month_of_year") as "ss_date_month_of_year",
    coalesce("cooperative"."ss_store_text_id","questionable"."ss_store_text_id") as "ss_store_text_id",
    "cooperative"."sumsales" as "sumsales",
    "questionable"."rk" as "rk"
FROM
    "cooperative"
    RIGHT OUTER JOIN "questionable" on "cooperative"."ss_date_month_of_year" = "questionable"."ss_date_month_of_year" AND "cooperative"."ss_date_quarter" = "questionable"."ss_date_quarter" AND "cooperative"."ss_date_year" = "questionable"."ss_date_year" AND "cooperative"."ss_item_brand_name" = "questionable"."ss_item_brand_name" AND "cooperative"."ss_item_category" is not distinct from "questionable"."ss_item_category" AND "cooperative"."ss_item_class" is not distinct from "questionable"."ss_item_class" AND "cooperative"."ss_item_product_name" = "questionable"."ss_item_product_name" AND "cooperative"."ss_store_text_id" = "questionable"."ss_store_text_id"
WHERE
    "questionable"."rk" <= 100

ORDER BY 
    coalesce("cooperative"."ss_item_category","questionable"."ss_item_category") asc nulls first,
    coalesce("cooperative"."ss_item_class","questionable"."ss_item_class") asc nulls first,
    coalesce("cooperative"."ss_item_brand_name","questionable"."ss_item_brand_name") asc nulls first,
    coalesce("cooperative"."ss_item_product_name","questionable"."ss_item_product_name") asc nulls first,
    coalesce("cooperative"."ss_date_year","questionable"."ss_date_year") asc nulls first,
    coalesce("cooperative"."ss_date_quarter","questionable"."ss_date_quarter") asc nulls first,
    coalesce("cooperative"."ss_date_month_of_year","questionable"."ss_date_month_of_year") asc nulls first,
    coalesce("cooperative"."ss_store_text_id","questionable"."ss_store_text_id") asc nulls first,
    "cooperative"."sumsales" asc nulls first,
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
