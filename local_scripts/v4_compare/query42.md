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
| v4 | 1079 | 22 | 7.23 ms |
| reference | 1079 | 22 | 7.34 ms |
| v4 / ref | 1.00x | 1.00x | 0.98x |

## Preql

```
import physical_sales as physical_sales;

where
    physical_sales.item.manager_id = 1
    and physical_sales.date.month_of_year = 11
    and physical_sales.date.year = 2000
select
    physical_sales.date.year,
    physical_sales.item.category_id,
    physical_sales.item.category,
    sum(physical_sales.ext_sales_price) as total_ext_sales_price,
order by
    total_ext_sales_price desc,
    physical_sales.date.year asc,
    physical_sales.item.category_id asc,
    physical_sales.item.category asc
limit 100
;
```

## v4 generated SQL

```sql
SELECT
    "physical_sales_date_date"."D_YEAR" as "physical_sales_date_year",
    "physical_sales_item_items"."I_CATEGORY_ID" as "physical_sales_item_category_id",
    "physical_sales_item_items"."I_CATEGORY" as "physical_sales_item_category",
    sum("physical_sales_store_sales"."SS_EXT_SALES_PRICE") as "total_ext_sales_price"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "physical_sales_date_date" on "physical_sales_store_sales"."SS_SOLD_DATE_SK" = "physical_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "physical_sales_item_items" on "physical_sales_store_sales"."SS_ITEM_SK" = "physical_sales_item_items"."I_ITEM_SK"
WHERE
    "physical_sales_item_items"."I_MANAGER_ID" = 1 and "physical_sales_date_date"."D_MOY" = 11 and "physical_sales_date_date"."D_YEAR" = 2000

GROUP BY
    1,
    2,
    3
ORDER BY 
    "total_ext_sales_price" desc,
    "physical_sales_date_date"."D_YEAR" asc,
    "physical_sales_item_items"."I_CATEGORY_ID" asc,
    "physical_sales_item_items"."I_CATEGORY" asc
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
SELECT
    "physical_sales_date_date"."D_YEAR" as "physical_sales_date_year",
    "physical_sales_item_items"."I_CATEGORY_ID" as "physical_sales_item_category_id",
    "physical_sales_item_items"."I_CATEGORY" as "physical_sales_item_category",
    sum("physical_sales_store_sales"."SS_EXT_SALES_PRICE") as "total_ext_sales_price"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "physical_sales_date_date" on "physical_sales_store_sales"."SS_SOLD_DATE_SK" = "physical_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "physical_sales_item_items" on "physical_sales_store_sales"."SS_ITEM_SK" = "physical_sales_item_items"."I_ITEM_SK"
WHERE
    "physical_sales_item_items"."I_MANAGER_ID" = 1 and "physical_sales_date_date"."D_MOY" = 11 and "physical_sales_date_date"."D_YEAR" = 2000

GROUP BY
    1,
    2,
    3
ORDER BY 
    "total_ext_sales_price" desc,
    "physical_sales_date_date"."D_YEAR" asc,
    "physical_sales_item_items"."I_CATEGORY_ID" asc,
    "physical_sales_item_items"."I_CATEGORY" asc
LIMIT (100)
```
