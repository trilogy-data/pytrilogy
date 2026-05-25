# Query 19

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
| v4 | 4966 | 113 |
| reference | 1794 | 28 |
| v4 / ref | 2.77x | 4.04x |

## Preql

```
import store_sales as store_sales;

where
    store_sales.item.manager_id = 8
    and store_sales.date.month_of_year = 11
    and store_sales.date.year = 1998
    and substring(store_sales.customer.address.zip, 1, 5) != substring(store_sales.store.zip, 1, 5)
select
    store_sales.item.brand_id,
    store_sales.item.brand_name,
    store_sales.item.manufacturer_id,
    store_sales.item.manufact,
    sum(store_sales.ext_sales_price) as ext_price,
order by
    ext_price desc,
    store_sales.item.brand_name asc,
    store_sales.item.brand_id asc,
    store_sales.item.manufacturer_id asc,
    store_sales.item.manufact asc
limit 100
;
```

## v4 generated SQL

```sql
WITH 
cooperative as (
SELECT
    "store_sales_store_sales"."SS_CUSTOMER_SK" as "store_sales_customer_id",
    "store_sales_store_sales"."SS_EXT_SALES_PRICE" as "store_sales_ext_sales_price",
    "store_sales_store_sales"."SS_ITEM_SK" as "store_sales_item_id",
    "store_sales_store_sales"."SS_SOLD_DATE_SK" as "store_sales_date_id",
    "store_sales_store_sales"."SS_STORE_SK" as "store_sales_store_id"
FROM
    "memory"."store_sales" as "store_sales_store_sales"),
questionable as (
SELECT
    "cooperative"."store_sales_customer_id" as "store_sales_customer_id",
    "cooperative"."store_sales_date_id" as "store_sales_date_id",
    "cooperative"."store_sales_ext_sales_price" as "store_sales_ext_sales_price",
    "cooperative"."store_sales_item_id" as "store_sales_item_id",
    "cooperative"."store_sales_store_id" as "store_sales_store_id"
FROM
    "cooperative"
GROUP BY
    1,
    2,
    3,
    4,
    5),
thoughtful as (
SELECT
    "store_sales_store_store"."S_STORE_SK" as "store_sales_store_id",
    "store_sales_store_store"."S_ZIP" as "store_sales_store_zip"
FROM
    "memory"."store" as "store_sales_store_store"),
cheerful as (
SELECT
    "store_sales_item_items"."I_BRAND" as "store_sales_item_brand_name",
    "store_sales_item_items"."I_BRAND_ID" as "store_sales_item_brand_id",
    "store_sales_item_items"."I_ITEM_SK" as "store_sales_item_id",
    "store_sales_item_items"."I_MANAGER_ID" as "store_sales_item_manager_id",
    "store_sales_item_items"."I_MANUFACT" as "store_sales_item_manufact",
    "store_sales_item_items"."I_MANUFACT_ID" as "store_sales_item_manufacturer_id"
FROM
    "memory"."item" as "store_sales_item_items"),
wakeful as (
SELECT
    "store_sales_date_date"."D_DATE_SK" as "store_sales_date_id",
    "store_sales_date_date"."D_MOY" as "store_sales_date_month_of_year",
    "store_sales_date_date"."D_YEAR" as "store_sales_date_year"
FROM
    "memory"."date_dim" as "store_sales_date_date"),
highfalutin as (
SELECT
    "store_sales_customer_customers"."C_CURRENT_ADDR_SK" as "store_sales_customer_address_id",
    "store_sales_customer_customers"."C_CUSTOMER_SK" as "store_sales_customer_id"
FROM
    "memory"."customer" as "store_sales_customer_customers"),
quizzical as (
SELECT
    "store_sales_customer_address_customer_address"."CA_ADDRESS_SK" as "store_sales_customer_address_id",
    "store_sales_customer_address_customer_address"."CA_ZIP" as "store_sales_customer_address_zip"
FROM
    "memory"."customer_address" as "store_sales_customer_address_customer_address"),
abundant as (
SELECT
    "cheerful"."store_sales_item_brand_id" as "store_sales_item_brand_id",
    "cheerful"."store_sales_item_brand_name" as "store_sales_item_brand_name",
    "cheerful"."store_sales_item_manager_id" as "store_sales_item_manager_id",
    "cheerful"."store_sales_item_manufact" as "store_sales_item_manufact",
    "cheerful"."store_sales_item_manufacturer_id" as "store_sales_item_manufacturer_id",
    "questionable"."store_sales_ext_sales_price" as "store_sales_ext_sales_price",
    "quizzical"."store_sales_customer_address_zip" as "store_sales_customer_address_zip",
    "thoughtful"."store_sales_store_zip" as "store_sales_store_zip",
    "wakeful"."store_sales_date_month_of_year" as "store_sales_date_month_of_year",
    "wakeful"."store_sales_date_year" as "store_sales_date_year"
FROM
    "questionable"
    LEFT OUTER JOIN "wakeful" on "questionable"."store_sales_date_id" = "wakeful"."store_sales_date_id"
    INNER JOIN "cheerful" on "questionable"."store_sales_item_id" = "cheerful"."store_sales_item_id"
    LEFT OUTER JOIN "thoughtful" on "questionable"."store_sales_store_id" = "thoughtful"."store_sales_store_id"
    LEFT OUTER JOIN "highfalutin" on "questionable"."store_sales_customer_id" = "highfalutin"."store_sales_customer_id"
    LEFT OUTER JOIN "quizzical" on "highfalutin"."store_sales_customer_address_id" = "quizzical"."store_sales_customer_address_id"
WHERE
    "cheerful"."store_sales_item_manager_id" = 8 and "wakeful"."store_sales_date_month_of_year" = 11 and "wakeful"."store_sales_date_year" = 1998 and SUBSTRING("quizzical"."store_sales_customer_address_zip",1,5) != SUBSTRING("thoughtful"."store_sales_store_zip",1,5)

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10)
SELECT
    sum("abundant"."store_sales_ext_sales_price") as "ext_price",
    "abundant"."store_sales_item_manufacturer_id" as "store_sales_item_manufacturer_id",
    "abundant"."store_sales_item_brand_id" as "store_sales_item_brand_id",
    "abundant"."store_sales_item_brand_name" as "store_sales_item_brand_name",
    "abundant"."store_sales_item_manufact" as "store_sales_item_manufact"
FROM
    "abundant"
GROUP BY
    2,
    3,
    4,
    5
ORDER BY 
    "ext_price" desc,
    "abundant"."store_sales_item_brand_name" asc,
    "abundant"."store_sales_item_brand_id" asc,
    "abundant"."store_sales_item_manufacturer_id" asc,
    "abundant"."store_sales_item_manufact" asc
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
SELECT
    "store_sales_item_items"."I_BRAND_ID" as "store_sales_item_brand_id",
    "store_sales_item_items"."I_BRAND" as "store_sales_item_brand_name",
    "store_sales_item_items"."I_MANUFACT_ID" as "store_sales_item_manufacturer_id",
    "store_sales_item_items"."I_MANUFACT" as "store_sales_item_manufact",
    sum("store_sales_store_sales"."SS_EXT_SALES_PRICE") as "ext_price"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "store_sales_item_items" on "store_sales_store_sales"."SS_ITEM_SK" = "store_sales_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."store" as "store_sales_store_store" on "store_sales_store_sales"."SS_STORE_SK" = "store_sales_store_store"."S_STORE_SK"
    INNER JOIN "memory"."customer" as "store_sales_customer_customers" on "store_sales_store_sales"."SS_CUSTOMER_SK" = "store_sales_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."customer_address" as "store_sales_customer_address_customer_address" on "store_sales_customer_customers"."C_CURRENT_ADDR_SK" = "store_sales_customer_address_customer_address"."CA_ADDRESS_SK"
WHERE
    "store_sales_item_items"."I_MANAGER_ID" = 8 and "store_sales_date_date"."D_MOY" = 11 and "store_sales_date_date"."D_YEAR" = 1998 and SUBSTRING("store_sales_customer_address_customer_address"."CA_ZIP",1,5) != SUBSTRING("store_sales_store_store"."S_ZIP",1,5)

GROUP BY
    1,
    2,
    3,
    4
ORDER BY 
    "ext_price" desc,
    "store_sales_item_items"."I_BRAND" asc,
    "store_sales_item_items"."I_BRAND_ID" asc,
    "store_sales_item_items"."I_MANUFACT_ID" asc,
    "store_sales_item_items"."I_MANUFACT" asc
LIMIT (100)
```
