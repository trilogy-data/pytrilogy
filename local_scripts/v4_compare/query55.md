# Query 55

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
| v4 | 885 | 18 | 60.97 ms |
| reference | 885 | 18 | 60.21 ms |
| v4 / ref | 1.00x | 1.00x | 1.01x |

## Preql

```
import physical_sales as physical_sales;

where
    physical_sales.item.manager_id = 28
    and physical_sales.date.year = 1999
    and physical_sales.date.month_of_year = 11
select
    physical_sales.item.brand_id,
    physical_sales.item.brand_name,
    sum(physical_sales.ext_sales_price) as total_ext_sales,
order by
    total_ext_sales desc,
    physical_sales.item.brand_id asc
limit 100
;
```

## v4 generated SQL

```sql
SELECT
    sum("physical_sales_store_sales"."SS_EXT_SALES_PRICE") as "total_ext_sales",
    "physical_sales_item_items"."I_BRAND_ID" as "physical_sales_item_brand_id",
    "physical_sales_item_items"."I_BRAND" as "physical_sales_item_brand_name"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "physical_sales_date_date" on "physical_sales_store_sales"."SS_SOLD_DATE_SK" = "physical_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "physical_sales_item_items" on "physical_sales_store_sales"."SS_ITEM_SK" = "physical_sales_item_items"."I_ITEM_SK"
WHERE
    "physical_sales_item_items"."I_MANAGER_ID" = 28 and "physical_sales_date_date"."D_YEAR" = 1999 and "physical_sales_date_date"."D_MOY" = 11

GROUP BY
    2,
    3
ORDER BY 
    "total_ext_sales" desc,
    "physical_sales_item_items"."I_BRAND_ID" asc
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
SELECT
    "physical_sales_item_items"."I_BRAND_ID" as "physical_sales_item_brand_id",
    "physical_sales_item_items"."I_BRAND" as "physical_sales_item_brand_name",
    sum("physical_sales_store_sales"."SS_EXT_SALES_PRICE") as "total_ext_sales"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "physical_sales_date_date" on "physical_sales_store_sales"."SS_SOLD_DATE_SK" = "physical_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "physical_sales_item_items" on "physical_sales_store_sales"."SS_ITEM_SK" = "physical_sales_item_items"."I_ITEM_SK"
WHERE
    "physical_sales_item_items"."I_MANAGER_ID" = 28 and "physical_sales_date_date"."D_YEAR" = 1999 and "physical_sales_date_date"."D_MOY" = 11

GROUP BY
    1,
    2
ORDER BY 
    "total_ext_sales" desc,
    "physical_sales_item_items"."I_BRAND_ID" asc
LIMIT (100)
```
