# Query 52

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

## SQL size

| Source | Chars | Lines |
| --- | --- | --- |
| v4 | 2324 | 54 |
| reference | 938 | 21 |
| v4 / ref | 2.48x | 2.57x |

## Preql

```
import store_sales as store_sales;

where
    store_sales.item.manager_id = 1
    and store_sales.date.month_of_year = 11
    and store_sales.date.year = 2000
select
    store_sales.date.year,
    store_sales.item.brand_id,
    store_sales.item.brand_name,
    sum(store_sales.ext_sales_price) as ext_price,
order by
    store_sales.date.year asc,
    ext_price desc,
    store_sales.item.brand_id asc
limit 100
;
```

## v4 generated SQL

```sql
WITH 
wakeful as (
SELECT
    "store_sales_store_sales"."SS_EXT_SALES_PRICE" as "store_sales_ext_sales_price",
    "store_sales_store_sales"."SS_ITEM_SK" as "store_sales_item_id",
    "store_sales_store_sales"."SS_SOLD_DATE_SK" as "store_sales_date_id"
FROM
    "memory"."store_sales" as "store_sales_store_sales"),
highfalutin as (
SELECT
    "store_sales_item_items"."I_BRAND" as "store_sales_item_brand_name",
    "store_sales_item_items"."I_BRAND_ID" as "store_sales_item_brand_id",
    "store_sales_item_items"."I_ITEM_SK" as "store_sales_item_id",
    "store_sales_item_items"."I_MANAGER_ID" as "store_sales_item_manager_id"
FROM
    "memory"."item" as "store_sales_item_items"),
quizzical as (
SELECT
    "store_sales_date_date"."D_DATE_SK" as "store_sales_date_id",
    "store_sales_date_date"."D_MOY" as "store_sales_date_month_of_year",
    "store_sales_date_date"."D_YEAR" as "store_sales_date_year"
FROM
    "memory"."date_dim" as "store_sales_date_date"),
cheerful as (
SELECT
    "highfalutin"."store_sales_item_brand_id" as "store_sales_item_brand_id",
    "highfalutin"."store_sales_item_brand_name" as "store_sales_item_brand_name",
    "highfalutin"."store_sales_item_manager_id" as "store_sales_item_manager_id",
    "quizzical"."store_sales_date_month_of_year" as "store_sales_date_month_of_year",
    "quizzical"."store_sales_date_year" as "store_sales_date_year",
    "wakeful"."store_sales_ext_sales_price" as "store_sales_ext_sales_price"
FROM
    "wakeful"
    LEFT OUTER JOIN "quizzical" on "wakeful"."store_sales_date_id" = "quizzical"."store_sales_date_id"
    INNER JOIN "highfalutin" on "wakeful"."store_sales_item_id" = "highfalutin"."store_sales_item_id"
WHERE
    "highfalutin"."store_sales_item_manager_id" = 1 and "quizzical"."store_sales_date_month_of_year" = 11 and "quizzical"."store_sales_date_year" = 2000
)
SELECT
    sum("cheerful"."store_sales_ext_sales_price") as "ext_price",
    "cheerful"."store_sales_item_brand_id" as "store_sales_item_brand_id",
    "cheerful"."store_sales_item_brand_name" as "store_sales_item_brand_name",
    "cheerful"."store_sales_date_year" as "store_sales_date_year"
FROM
    "cheerful"
GROUP BY
    2,
    3,
    4
ORDER BY 
    "cheerful"."store_sales_date_year" asc,
    "ext_price" desc,
    "cheerful"."store_sales_item_brand_id" asc
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
SELECT
    "store_sales_date_date"."D_YEAR" as "store_sales_date_year",
    "store_sales_item_items"."I_BRAND_ID" as "store_sales_item_brand_id",
    "store_sales_item_items"."I_BRAND" as "store_sales_item_brand_name",
    sum("store_sales_store_sales"."SS_EXT_SALES_PRICE") as "ext_price"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "store_sales_item_items" on "store_sales_store_sales"."SS_ITEM_SK" = "store_sales_item_items"."I_ITEM_SK"
WHERE
    "store_sales_item_items"."I_MANAGER_ID" = 1 and "store_sales_date_date"."D_MOY" = 11 and "store_sales_date_date"."D_YEAR" = 2000

GROUP BY
    1,
    2,
    3
ORDER BY 
    "store_sales_date_date"."D_YEAR" asc,
    "ext_price" desc,
    "store_sales_item_items"."I_BRAND_ID" asc
LIMIT (100)
```
