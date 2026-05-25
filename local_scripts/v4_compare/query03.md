# Query 03

**Status:** `mismatch`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (89 rows) |
| reference execution | OK (89 rows) |
| results identical | NO |

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
    "store_sales_item_items"."I_MANUFACT_ID" as "store_sales_item_manufacturer_id"
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
    "highfalutin"."store_sales_item_manufacturer_id" as "store_sales_item_manufacturer_id",
    "quizzical"."store_sales_date_month_of_year" as "store_sales_date_month_of_year",
    "quizzical"."store_sales_date_year" as "store_sales_date_year",
    "wakeful"."store_sales_ext_sales_price" as "store_sales_ext_sales_price"
FROM
    "wakeful"
    LEFT OUTER JOIN "quizzical" on "wakeful"."store_sales_date_id" = "quizzical"."store_sales_date_id"
    INNER JOIN "highfalutin" on "wakeful"."store_sales_item_id" = "highfalutin"."store_sales_item_id"
WHERE
    "quizzical"."store_sales_date_month_of_year" = 11 and "highfalutin"."store_sales_item_manufacturer_id" = 128
)
SELECT
    sum("cheerful"."store_sales_ext_sales_price") as "sum_agg",
    "cheerful"."store_sales_date_year" as "store_sales_date_year",
    "cheerful"."store_sales_item_brand_id" as "store_sales_item_brand_id",
    "cheerful"."store_sales_item_brand_name" as "store_sales_item_brand_name"
FROM
    "cheerful"
WHERE
    "cheerful"."store_sales_date_month_of_year" = 11 and "cheerful"."store_sales_item_manufacturer_id" = 128

GROUP BY
    2,
    3,
    4
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

## Result comparison

v4 rows: 89 (89 distinct)
ref rows: 89 (89 distinct)
only in v4 (showing up to 5 of 89):
  1x  (Decimal('50239.96'), 1999, 2001001, 'amalgimporto #1')
  1x  (Decimal('13937.28'), 1999, 1001002, 'amalgamalg #2')
  1x  (Decimal('10199.74'), 1999, 5003002, 'exportischolar #2')
  1x  (Decimal('17509.46'), 1998, 1004001, 'edu packamalg #1')
  1x  (Decimal('11056.90'), 1999, 8001009, 'amalgnameless #9')
only in ref (showing up to 5 of 89):
  1x  (1998, 2001001, 'amalgimporto #1', Decimal('60828.60'))
  1x  (1998, 5003001, 'exportischolar #1', Decimal('50182.63'))
  1x  (1998, 3004001, 'edu packexporti #1', Decimal('29036.38'))
  1x  (1998, 3001001, 'amalgexporti #1', Decimal('28596.63'))
  1x  (1998, 1003001, 'exportiamalg #1', Decimal('27087.35'))
