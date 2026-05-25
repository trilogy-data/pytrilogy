# Query 15

**Status:** `mismatch`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (100 rows) |
| reference execution | OK (100 rows) |
| results identical | NO |

## Result comparison

v4 rows: 100 (100 distinct)
ref rows: 100 (100 distinct)
only in v4 (showing up to 5 of 3):
  1x  (None, Decimal('3804.15'))
  1x  ('32812', Decimal('1563.14'))
  1x  ('38354', Decimal('2861.10'))
only in ref (showing up to 5 of 3):
  1x  (None, Decimal('3808.17'))
  1x  ('32812', Decimal('1567.58'))
  1x  ('38354', Decimal('2902.80'))

## SQL size

| Source | Chars | Lines |
| --- | --- | --- |
| v4 | 3318 | 70 |
| reference | 1372 | 16 |
| v4 / ref | 2.42x | 4.38x |

## Preql

```
import catalog_sales as catalog_sales;

where
    catalog_sales.date.quarter = 2
    and catalog_sales.date.year = 2001
    and (
        catalog_sales.bill_customer.address.state in ('CA', 'WA', 'GA')
        or catalog_sales.sales_price > 500
        or substring(catalog_sales.bill_customer.address.zip, 1, 5) in ('85669', '86197', '88274', '83405', '86475', '85392', '85460', '80348', '81792')
    )
select
    catalog_sales.bill_customer.address.zip,
    sum(catalog_sales.sales_price) as sales,
order by
    catalog_sales.bill_customer.address.zip asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
thoughtful as (
SELECT
    "catalog_sales_date_date"."D_DATE_SK" as "catalog_sales_date_id",
    "catalog_sales_date_date"."D_QOY" as "catalog_sales_date_quarter",
    "catalog_sales_date_date"."D_YEAR" as "catalog_sales_date_year"
FROM
    "memory"."date_dim" as "catalog_sales_date_date"),
wakeful as (
SELECT
    "catalog_sales_catalog_sales"."CS_BILL_CUSTOMER_SK" as "catalog_sales_bill_customer_id",
    "catalog_sales_catalog_sales"."CS_SALES_PRICE" as "catalog_sales_sales_price",
    "catalog_sales_catalog_sales"."CS_SOLD_DATE_SK" as "catalog_sales_date_id"
FROM
    "memory"."catalog_sales" as "catalog_sales_catalog_sales"),
cheerful as (
SELECT
    "wakeful"."catalog_sales_bill_customer_id" as "catalog_sales_bill_customer_id",
    "wakeful"."catalog_sales_date_id" as "catalog_sales_date_id",
    "wakeful"."catalog_sales_sales_price" as "catalog_sales_sales_price"
FROM
    "wakeful"
GROUP BY
    1,
    2,
    3),
highfalutin as (
SELECT
    "catalog_sales_bill_customer_customers"."C_CURRENT_ADDR_SK" as "catalog_sales_bill_customer_address_id",
    "catalog_sales_bill_customer_customers"."C_CUSTOMER_SK" as "catalog_sales_bill_customer_id"
FROM
    "memory"."customer" as "catalog_sales_bill_customer_customers"),
quizzical as (
SELECT
    "catalog_sales_bill_customer_address_customer_address"."CA_ADDRESS_SK" as "catalog_sales_bill_customer_address_id",
    "catalog_sales_bill_customer_address_customer_address"."CA_STATE" as "catalog_sales_bill_customer_address_state",
    "catalog_sales_bill_customer_address_customer_address"."CA_ZIP" as "catalog_sales_bill_customer_address_zip"
FROM
    "memory"."customer_address" as "catalog_sales_bill_customer_address_customer_address"),
cooperative as (
SELECT
    "cheerful"."catalog_sales_sales_price" as "catalog_sales_sales_price",
    "quizzical"."catalog_sales_bill_customer_address_state" as "catalog_sales_bill_customer_address_state",
    "quizzical"."catalog_sales_bill_customer_address_zip" as "catalog_sales_bill_customer_address_zip",
    "thoughtful"."catalog_sales_date_quarter" as "catalog_sales_date_quarter",
    "thoughtful"."catalog_sales_date_year" as "catalog_sales_date_year"
FROM
    "cheerful"
    LEFT OUTER JOIN "thoughtful" on "cheerful"."catalog_sales_date_id" = "thoughtful"."catalog_sales_date_id"
    LEFT OUTER JOIN "highfalutin" on "cheerful"."catalog_sales_bill_customer_id" = "highfalutin"."catalog_sales_bill_customer_id"
    LEFT OUTER JOIN "quizzical" on "highfalutin"."catalog_sales_bill_customer_address_id" = "quizzical"."catalog_sales_bill_customer_address_id"
WHERE
    "thoughtful"."catalog_sales_date_quarter" = 2 and "thoughtful"."catalog_sales_date_year" = 2001 and ( "quizzical"."catalog_sales_bill_customer_address_state" in ('CA','WA','GA') or "cheerful"."catalog_sales_sales_price" > 500 or SUBSTRING("quizzical"."catalog_sales_bill_customer_address_zip",1,5) in ('85669','86197','88274','83405','86475','85392','85460','80348','81792') )

GROUP BY
    1,
    2,
    3,
    4,
    5)
SELECT
    sum("cooperative"."catalog_sales_sales_price") as "sales",
    "cooperative"."catalog_sales_bill_customer_address_zip" as "catalog_sales_bill_customer_address_zip"
FROM
    "cooperative"
GROUP BY
    2
ORDER BY 
    "cooperative"."catalog_sales_bill_customer_address_zip" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
SELECT
    "catalog_sales_bill_customer_address_customer_address"."CA_ZIP" as "catalog_sales_bill_customer_address_zip",
    sum("catalog_sales_catalog_sales"."CS_SALES_PRICE") as "sales"
FROM
    "memory"."catalog_sales" as "catalog_sales_catalog_sales"
    INNER JOIN "memory"."date_dim" as "catalog_sales_date_date" on "catalog_sales_catalog_sales"."CS_SOLD_DATE_SK" = "catalog_sales_date_date"."D_DATE_SK"
    LEFT OUTER JOIN "memory"."customer" as "catalog_sales_bill_customer_customers" on "catalog_sales_catalog_sales"."CS_BILL_CUSTOMER_SK" = "catalog_sales_bill_customer_customers"."C_CUSTOMER_SK"
    LEFT OUTER JOIN "memory"."customer_address" as "catalog_sales_bill_customer_address_customer_address" on "catalog_sales_bill_customer_customers"."C_CURRENT_ADDR_SK" = "catalog_sales_bill_customer_address_customer_address"."CA_ADDRESS_SK"
WHERE
    "catalog_sales_date_date"."D_QOY" = 2 and "catalog_sales_date_date"."D_YEAR" = 2001 and ( "catalog_sales_bill_customer_address_customer_address"."CA_STATE" in ('CA','WA','GA') or "catalog_sales_catalog_sales"."CS_SALES_PRICE" > 500 or SUBSTRING("catalog_sales_bill_customer_address_customer_address"."CA_ZIP",1,5) in ('85669','86197','88274','83405','86475','85392','85460','80348','81792') )

GROUP BY
    1
ORDER BY 
    "catalog_sales_bill_customer_address_customer_address"."CA_ZIP" asc nulls first
LIMIT (100)
```
