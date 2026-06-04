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
| v4 | 947 | 21 | 36.21 ms |
| reference | 947 | 21 | 36.34 ms |
| v4 / ref | 1.00x | 1.00x | 1.00x |

## Preql

```
import physical_sales as physical_sales;

#Report the total extended sales price per item brand of a specific manufacturer for all sales in a specific month
# of the year
where
    physical_sales.date.month_of_year = 11 and physical_sales.item.manufacturer_id = 128
select
    physical_sales.date.year,
    physical_sales.item.brand_id,
    physical_sales.item.brand_name,
    sum(physical_sales.ext_sales_price) as sum_agg,
order by
    physical_sales.date.year asc,
    sum_agg desc,
    physical_sales.item.brand_id asc
limit 100
;
```

## v4 generated SQL

```sql
SELECT
    "physical_sales_date_date"."D_YEAR" as "physical_sales_date_year",
    "physical_sales_item_items"."I_BRAND_ID" as "physical_sales_item_brand_id",
    "physical_sales_item_items"."I_BRAND" as "physical_sales_item_brand_name",
    sum("physical_sales_store_sales"."SS_EXT_SALES_PRICE") as "sum_agg"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "physical_sales_date_date" on "physical_sales_store_sales"."SS_SOLD_DATE_SK" = "physical_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "physical_sales_item_items" on "physical_sales_store_sales"."SS_ITEM_SK" = "physical_sales_item_items"."I_ITEM_SK"
WHERE
    "physical_sales_date_date"."D_MOY" = 11 and "physical_sales_item_items"."I_MANUFACT_ID" = 128

GROUP BY
    1,
    2,
    3
ORDER BY 
    "physical_sales_date_date"."D_YEAR" asc,
    "sum_agg" desc,
    "physical_sales_item_items"."I_BRAND_ID" asc
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
SELECT
    "physical_sales_date_date"."D_YEAR" as "physical_sales_date_year",
    "physical_sales_item_items"."I_BRAND_ID" as "physical_sales_item_brand_id",
    "physical_sales_item_items"."I_BRAND" as "physical_sales_item_brand_name",
    sum("physical_sales_store_sales"."SS_EXT_SALES_PRICE") as "sum_agg"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "physical_sales_date_date" on "physical_sales_store_sales"."SS_SOLD_DATE_SK" = "physical_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "physical_sales_item_items" on "physical_sales_store_sales"."SS_ITEM_SK" = "physical_sales_item_items"."I_ITEM_SK"
WHERE
    "physical_sales_date_date"."D_MOY" = 11 and "physical_sales_item_items"."I_MANUFACT_ID" = 128

GROUP BY
    1,
    2,
    3
ORDER BY 
    "physical_sales_date_date"."D_YEAR" asc,
    "sum_agg" desc,
    "physical_sales_item_items"."I_BRAND_ID" asc
LIMIT (100)
```
