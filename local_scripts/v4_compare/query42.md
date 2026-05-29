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

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 1019 | 22 | 20.92 ms |
| reference | 1019 | 22 | 21.02 ms |
| v4 / ref | 1.00x | 1.00x | 1.00x |

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
SELECT
    sum("store_sales_store_sales"."SS_EXT_SALES_PRICE") as "total_ext_sales_price",
    "store_sales_date_date"."D_YEAR" as "store_sales_date_year",
    "store_sales_item_items"."I_CATEGORY" as "store_sales_item_category",
    "store_sales_item_items"."I_CATEGORY_ID" as "store_sales_item_category_id"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "store_sales_item_items" on "store_sales_store_sales"."SS_ITEM_SK" = "store_sales_item_items"."I_ITEM_SK"
WHERE
    "store_sales_item_items"."I_MANAGER_ID" = 1 and "store_sales_date_date"."D_MOY" = 11 and "store_sales_date_date"."D_YEAR" = 2000

GROUP BY
    2,
    3,
    4
ORDER BY 
    "total_ext_sales_price" desc,
    "store_sales_date_date"."D_YEAR" asc,
    "store_sales_item_items"."I_CATEGORY_ID" asc,
    "store_sales_item_items"."I_CATEGORY" asc
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
