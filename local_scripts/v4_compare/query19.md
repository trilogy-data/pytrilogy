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

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 1944 | 28 | 43.91 ms |
| reference | 1944 | 28 | 46.69 ms |
| v4 / ref | 1.00x | 1.00x | 0.94x |

## Preql

```
import physical_sales as physical_sales;

where
    physical_sales.item.manager_id = 8
    and physical_sales.date.month_of_year = 11
    and physical_sales.date.year = 1998
    and substring(physical_sales.billing_customer.address.zip, 1, 5) != substring(physical_sales.store.zip, 1, 5)
select
    physical_sales.item.brand_id,
    physical_sales.item.brand_name,
    physical_sales.item.manufacturer_id,
    physical_sales.item.manufact,
    sum(physical_sales.ext_sales_price) as ext_price,
order by
    ext_price desc,
    physical_sales.item.brand_name asc,
    physical_sales.item.brand_id asc,
    physical_sales.item.manufacturer_id asc,
    physical_sales.item.manufact asc
limit 100
;
```

## v4 generated SQL

```sql
SELECT
    "physical_sales_item_items"."I_BRAND_ID" as "physical_sales_item_brand_id",
    "physical_sales_item_items"."I_BRAND" as "physical_sales_item_brand_name",
    "physical_sales_item_items"."I_MANUFACT_ID" as "physical_sales_item_manufacturer_id",
    "physical_sales_item_items"."I_MANUFACT" as "physical_sales_item_manufact",
    sum("physical_sales_store_sales"."SS_EXT_SALES_PRICE") as "ext_price"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "physical_sales_date_date" on "physical_sales_store_sales"."SS_SOLD_DATE_SK" = "physical_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "physical_sales_item_items" on "physical_sales_store_sales"."SS_ITEM_SK" = "physical_sales_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."store" as "physical_sales_store_store" on "physical_sales_store_sales"."SS_STORE_SK" = "physical_sales_store_store"."S_STORE_SK"
    INNER JOIN "memory"."customer" as "physical_sales_billing_customer_customers" on "physical_sales_store_sales"."SS_CUSTOMER_SK" = "physical_sales_billing_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."customer_address" as "physical_sales_billing_customer_address_customer_address" on "physical_sales_billing_customer_customers"."C_CURRENT_ADDR_SK" = "physical_sales_billing_customer_address_customer_address"."CA_ADDRESS_SK"
WHERE
    "physical_sales_item_items"."I_MANAGER_ID" = 8 and "physical_sales_date_date"."D_MOY" = 11 and "physical_sales_date_date"."D_YEAR" = 1998 and SUBSTRING("physical_sales_billing_customer_address_customer_address"."CA_ZIP",1,5) != SUBSTRING("physical_sales_store_store"."S_ZIP",1,5)

GROUP BY
    1,
    2,
    3,
    4
ORDER BY 
    "ext_price" desc,
    "physical_sales_item_items"."I_BRAND" asc,
    "physical_sales_item_items"."I_BRAND_ID" asc,
    "physical_sales_item_items"."I_MANUFACT_ID" asc,
    "physical_sales_item_items"."I_MANUFACT" asc
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
SELECT
    "physical_sales_item_items"."I_BRAND_ID" as "physical_sales_item_brand_id",
    "physical_sales_item_items"."I_BRAND" as "physical_sales_item_brand_name",
    "physical_sales_item_items"."I_MANUFACT_ID" as "physical_sales_item_manufacturer_id",
    "physical_sales_item_items"."I_MANUFACT" as "physical_sales_item_manufact",
    sum("physical_sales_store_sales"."SS_EXT_SALES_PRICE") as "ext_price"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "physical_sales_date_date" on "physical_sales_store_sales"."SS_SOLD_DATE_SK" = "physical_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "physical_sales_item_items" on "physical_sales_store_sales"."SS_ITEM_SK" = "physical_sales_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."store" as "physical_sales_store_store" on "physical_sales_store_sales"."SS_STORE_SK" = "physical_sales_store_store"."S_STORE_SK"
    INNER JOIN "memory"."customer" as "physical_sales_billing_customer_customers" on "physical_sales_store_sales"."SS_CUSTOMER_SK" = "physical_sales_billing_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."customer_address" as "physical_sales_billing_customer_address_customer_address" on "physical_sales_billing_customer_customers"."C_CURRENT_ADDR_SK" = "physical_sales_billing_customer_address_customer_address"."CA_ADDRESS_SK"
WHERE
    "physical_sales_item_items"."I_MANAGER_ID" = 8 and "physical_sales_date_date"."D_MOY" = 11 and "physical_sales_date_date"."D_YEAR" = 1998 and SUBSTRING("physical_sales_billing_customer_address_customer_address"."CA_ZIP",1,5) != SUBSTRING("physical_sales_store_store"."S_ZIP",1,5)

GROUP BY
    1,
    2,
    3,
    4
ORDER BY 
    "ext_price" desc,
    "physical_sales_item_items"."I_BRAND" asc,
    "physical_sales_item_items"."I_BRAND_ID" asc,
    "physical_sales_item_items"."I_MANUFACT_ID" asc,
    "physical_sales_item_items"."I_MANUFACT" asc
LIMIT (100)
```
