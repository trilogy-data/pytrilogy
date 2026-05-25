# Query 42

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (10 rows) |
| reference execution | OK (10 rows) |
| results identical | YES |

## Result comparison

v4 rows: 10 (10 distinct)
ref rows: 10 (10 distinct)

## SQL size

| Source | Chars | Lines |
| --- | --- | --- |
| v4 | 2410 | 55 |
| reference | 1019 | 22 |
| v4 / ref | 2.37x | 2.50x |

## Preql

```
import store_sales as store_sales;

where
    store_sales.item.manager_id = 1
    and store_sales.date.month_of_year = 11
    and store_sales.date.year = 2000
select
    store_sales.date.year,
    store_sales.item.category_id,
    store_sales.item.category,
    sum(store_sales.ext_sales_price) as total_ext_sales_price,
order by
    total_ext_sales_price desc,
    store_sales.date.year asc,
    store_sales.item.category_id asc,
    store_sales.item.category asc
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
    "store_sales_item_items"."I_CATEGORY" as "store_sales_item_category",
    "store_sales_item_items"."I_CATEGORY_ID" as "store_sales_item_category_id",
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
    "highfalutin"."store_sales_item_category" as "store_sales_item_category",
    "highfalutin"."store_sales_item_category_id" as "store_sales_item_category_id",
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
    sum("cheerful"."store_sales_ext_sales_price") as "total_ext_sales_price",
    "cheerful"."store_sales_date_year" as "store_sales_date_year",
    "cheerful"."store_sales_item_category" as "store_sales_item_category",
    "cheerful"."store_sales_item_category_id" as "store_sales_item_category_id"
FROM
    "cheerful"
GROUP BY
    2,
    3,
    4
ORDER BY 
    "total_ext_sales_price" desc,
    "cheerful"."store_sales_date_year" asc,
    "cheerful"."store_sales_item_category_id" asc,
    "cheerful"."store_sales_item_category" asc
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
SELECT
    "store_sales_date_date"."D_YEAR" as "store_sales_date_year",
    "store_sales_item_items"."I_CATEGORY_ID" as "store_sales_item_category_id",
    "store_sales_item_items"."I_CATEGORY" as "store_sales_item_category",
    sum("store_sales_store_sales"."SS_EXT_SALES_PRICE") as "total_ext_sales_price"
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
    "total_ext_sales_price" desc,
    "store_sales_date_date"."D_YEAR" asc,
    "store_sales_item_items"."I_CATEGORY_ID" asc,
    "store_sales_item_items"."I_CATEGORY" asc
LIMIT (100)
```
