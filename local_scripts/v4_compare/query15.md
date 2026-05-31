# Query 15

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
| v4 | 1372 | 16 | 16.42 ms |
| reference | 1372 | 16 | 15.53 ms |
| v4 / ref | 1.00x | 1.00x | 1.06x |

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
