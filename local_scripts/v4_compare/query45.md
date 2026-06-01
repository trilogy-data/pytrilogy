# Query 45

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (19 rows) |
| reference execution | OK (19 rows) |
| results identical | YES |

## Result comparison

v4 rows: 19 (19 distinct)
ref rows: 19 (19 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 2510 | 37 | 24.66 ms |
| reference | 2224 | 44 | 25.55 ms |
| v4 / ref | 1.13x | 0.84x | 0.97x |

## Preql

```
import web_sales as web_sales;
import item as item2;

auto special_item_ids <- item2.text_id ? item2.id in (2, 3, 5, 7, 11, 13, 17, 19, 23, 29);

where
    web_sales.date.quarter = 2
    and web_sales.date.year = 2001
    and (
        substring(web_sales.billing_customer.address.zip, 1, 5) in ('85669', '86197', '88274', '83405', '86475', '85392', '85460', '80348', '81792')
        or web_sales.item.text_id in special_item_ids
    )
select
    web_sales.billing_customer.address.zip,
    web_sales.billing_customer.address.city,
    sum(web_sales.sales_price) as total_sales,
order by
    web_sales.billing_customer.address.zip asc,
    web_sales.billing_customer.address.city asc
limit 100
;
```

## v4 generated SQL

```sql
WITH 
highfalutin as (
SELECT
    CASE WHEN "item2_items"."I_ITEM_SK" in (2,3,5,7,11,13,17,19,23,29) THEN "item2_items"."I_ITEM_ID" ELSE NULL END as "special_item_ids"
FROM
    "memory"."item" as "item2_items"),
abundant as (
SELECT
    "web_sales_billing_customer_address_customer_address"."CA_CITY" as "web_sales_billing_customer_address_city",
    "web_sales_billing_customer_address_customer_address"."CA_ZIP" as "web_sales_billing_customer_address_zip",
    "web_sales_item_items"."I_ITEM_ID" as "web_sales_item_text_id",
    "web_sales_web_sales"."WS_SALES_PRICE" as "web_sales_sales_price"
FROM
    "memory"."web_sales" as "web_sales_web_sales"
    INNER JOIN "memory"."date_dim" as "web_sales_date_date" on "web_sales_web_sales"."WS_SOLD_DATE_SK" = "web_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "web_sales_item_items" on "web_sales_web_sales"."WS_ITEM_SK" = "web_sales_item_items"."I_ITEM_SK"
    LEFT OUTER JOIN "memory"."customer" as "web_sales_billing_customer_customers" on "web_sales_web_sales"."WS_BILL_CUSTOMER_SK" = "web_sales_billing_customer_customers"."C_CUSTOMER_SK"
    LEFT OUTER JOIN "memory"."customer_address" as "web_sales_billing_customer_address_customer_address" on "web_sales_billing_customer_customers"."C_CURRENT_ADDR_SK" = "web_sales_billing_customer_address_customer_address"."CA_ADDRESS_SK"
WHERE
    "web_sales_date_date"."D_QOY" = 2 and "web_sales_date_date"."D_YEAR" = 2001 and ( SUBSTRING("web_sales_billing_customer_address_customer_address"."CA_ZIP",1,5) in ('85669','86197','88274','83405','86475','85392','85460','80348','81792') or "web_sales_item_items"."I_ITEM_ID" in (select highfalutin."special_item_ids" from highfalutin where highfalutin."special_item_ids" is not null) )
)
SELECT
    sum("abundant"."web_sales_sales_price") as "total_sales",
    "abundant"."web_sales_billing_customer_address_city" as "web_sales_billing_customer_address_city",
    "abundant"."web_sales_billing_customer_address_zip" as "web_sales_billing_customer_address_zip"
FROM
    "abundant"
WHERE
    ( SUBSTRING("abundant"."web_sales_billing_customer_address_zip",1,5) in ('85669','86197','88274','83405','86475','85392','85460','80348','81792') or "abundant"."web_sales_item_text_id" in (select highfalutin."special_item_ids" from highfalutin where highfalutin."special_item_ids" is not null) )

GROUP BY
    2,
    3
ORDER BY 
    "abundant"."web_sales_billing_customer_address_zip" asc,
    "abundant"."web_sales_billing_customer_address_city" asc
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
quizzical as (
SELECT
    "item2_items"."I_ITEM_ID" as "special_item_ids"
FROM
    "memory"."item" as "item2_items"
WHERE
    "item2_items"."I_ITEM_SK" in (2,3,5,7,11,13,17,19,23,29)

GROUP BY
    1),
uneven as (
SELECT
    "web_sales_billing_customer_address_customer_address"."CA_CITY" as "web_sales_billing_customer_address_city",
    "web_sales_billing_customer_address_customer_address"."CA_ZIP" as "web_sales_billing_customer_address_zip",
    "web_sales_web_sales"."WS_SALES_PRICE" as "web_sales_sales_price"
FROM
    "memory"."web_sales" as "web_sales_web_sales"
    INNER JOIN "memory"."date_dim" as "web_sales_date_date" on "web_sales_web_sales"."WS_SOLD_DATE_SK" = "web_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "web_sales_item_items" on "web_sales_web_sales"."WS_ITEM_SK" = "web_sales_item_items"."I_ITEM_SK"
    LEFT OUTER JOIN "memory"."customer" as "web_sales_billing_customer_customers" on "web_sales_web_sales"."WS_BILL_CUSTOMER_SK" = "web_sales_billing_customer_customers"."C_CUSTOMER_SK"
    LEFT OUTER JOIN "memory"."customer_address" as "web_sales_billing_customer_address_customer_address" on "web_sales_billing_customer_customers"."C_CURRENT_ADDR_SK" = "web_sales_billing_customer_address_customer_address"."CA_ADDRESS_SK"
WHERE
    "web_sales_date_date"."D_QOY" = 2 and "web_sales_date_date"."D_YEAR" = 2001 and ( SUBSTRING("web_sales_billing_customer_address_customer_address"."CA_ZIP",1,5) in ('85669','86197','88274','83405','86475','85392','85460','80348','81792') or "web_sales_item_items"."I_ITEM_ID" in (select quizzical."special_item_ids" from quizzical where quizzical."special_item_ids" is not null) )

GROUP BY
    1,
    2,
    3,
    "web_sales_item_items"."I_ITEM_SK",
    "web_sales_web_sales"."WS_ORDER_NUMBER")
SELECT
    "uneven"."web_sales_billing_customer_address_zip" as "web_sales_billing_customer_address_zip",
    "uneven"."web_sales_billing_customer_address_city" as "web_sales_billing_customer_address_city",
    sum("uneven"."web_sales_sales_price") as "total_sales"
FROM
    "uneven"
GROUP BY
    1,
    2
ORDER BY 
    "uneven"."web_sales_billing_customer_address_zip" asc,
    "uneven"."web_sales_billing_customer_address_city" asc
LIMIT (100)
```
