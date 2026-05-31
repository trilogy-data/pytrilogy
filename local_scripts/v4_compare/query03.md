# Query 03

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (89 rows) |
| reference execution | OK (89 rows) |
| results identical | YES |

## Result comparison

v4 rows: 89 (89 distinct)
ref rows: 89 (89 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 893 | 21 | 28.15 ms |
| reference | 893 | 21 | 27.56 ms |
| v4 / ref | 1.00x | 1.00x | 1.02x |

## Preql

```
import store_sales as store_sales;

#Report the total extended sales price per item brand of a specific manufacturer for all sales in a specific month
# of the year
where
    store_sales.date.month_of_year = 11 and store_sales.item.manufacturer_id = 128
select
    store_sales.date.year,
    store_sales.item.brand_id,
    store_sales.item.brand_name,
    sum(store_sales.ext_sales_price) as sum_agg,
order by
    store_sales.date.year asc,
    sum_agg desc,
    store_sales.item.brand_id asc
limit 100
;
```

## v4 generated SQL

```sql
SELECT
    sum("store_sales_store_sales"."SS_EXT_SALES_PRICE") as "sum_agg",
    "store_sales_date_date"."D_YEAR" as "store_sales_date_year",
    "store_sales_item_items"."I_BRAND_ID" as "store_sales_item_brand_id",
    "store_sales_item_items"."I_BRAND" as "store_sales_item_brand_name"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "store_sales_item_items" on "store_sales_store_sales"."SS_ITEM_SK" = "store_sales_item_items"."I_ITEM_SK"
WHERE
    "store_sales_date_date"."D_MOY" = 11 and "store_sales_item_items"."I_MANUFACT_ID" = 128

GROUP BY
    2,
    3,
    4
ORDER BY 
    "store_sales_date_date"."D_YEAR" asc,
    "sum_agg" desc,
    "store_sales_item_items"."I_BRAND_ID" asc
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
SELECT
    "store_sales_date_date"."D_YEAR" as "store_sales_date_year",
    "store_sales_item_items"."I_BRAND_ID" as "store_sales_item_brand_id",
    "store_sales_item_items"."I_BRAND" as "store_sales_item_brand_name",
    sum("store_sales_store_sales"."SS_EXT_SALES_PRICE") as "sum_agg"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "store_sales_item_items" on "store_sales_store_sales"."SS_ITEM_SK" = "store_sales_item_items"."I_ITEM_SK"
WHERE
    "store_sales_date_date"."D_MOY" = 11 and "store_sales_item_items"."I_MANUFACT_ID" = 128

GROUP BY
    1,
    2,
    3
ORDER BY 
    "store_sales_date_date"."D_YEAR" asc,
    "sum_agg" desc,
    "store_sales_item_items"."I_BRAND_ID" asc
LIMIT (100)
```
