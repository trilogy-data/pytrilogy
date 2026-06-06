# Query 48

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (1 rows) |
| reference execution | OK (1 rows) |
| results identical | YES |

## Result comparison

v4 rows: 1 (1 distinct)
ref rows: 1 (1 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 2366 | 9 | 4.50 ms |
| reference | 2366 | 9 | 4.26 ms |
| v4 / ref | 1.00x | 1.00x | 1.06x |

## Preql

```
import physical_sales as physical_sales;

where
    physical_sales.date.year = 2000
    and physical_sales.store.id is not null
    and (
        (
            physical_sales.customer_demographic.marital_status = 'M'
            and physical_sales.customer_demographic.education_status = '4 yr Degree'
            and physical_sales.sales_price between 100.0 and 150.0
        )
        or (
            physical_sales.customer_demographic.marital_status = 'D'
            and physical_sales.customer_demographic.education_status = '2 yr Degree'
            and physical_sales.sales_price between 50.0 and 100.0
        )
        or (
            physical_sales.customer_demographic.marital_status = 'S'
            and physical_sales.customer_demographic.education_status = 'College'
            and physical_sales.sales_price between 150.0 and 200.0
        )
    )
    and (
        (
            physical_sales.sale_address.country = 'United States'
            and physical_sales.sale_address.state in ('CO', 'OH', 'TX')
            and physical_sales.net_profit between 0 and 2000
        )
        or (
            physical_sales.sale_address.country = 'United States'
            and physical_sales.sale_address.state in ('OR', 'MN', 'KY')
            and physical_sales.net_profit between 150 and 3000
        )
        or (
            physical_sales.sale_address.country = 'United States'
            and physical_sales.sale_address.state in ('VA', 'CA', 'MS')
            and physical_sales.net_profit between 50 and 25000
        )
    )
select
    sum(physical_sales.quantity) as total_quantity,
;
```

## v4 generated SQL

```sql
SELECT
    sum("physical_sales_store_sales"."SS_QUANTITY") as "total_quantity"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "physical_sales_date_date" on "physical_sales_store_sales"."SS_SOLD_DATE_SK" = "physical_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer_address" as "physical_sales_sale_address_customer_address" on "physical_sales_store_sales"."SS_ADDR_SK" = "physical_sales_sale_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."customer_demographics" as "physical_sales_customer_demographic_customer_demographics" on "physical_sales_store_sales"."SS_CDEMO_SK" = "physical_sales_customer_demographic_customer_demographics"."CD_DEMO_SK"
WHERE
    "physical_sales_date_date"."D_YEAR" = 2000 and "physical_sales_store_sales"."SS_STORE_SK" is not null and ( ( "physical_sales_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" = 'M' and "physical_sales_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS" = '4 yr Degree' and "physical_sales_store_sales"."SS_SALES_PRICE" BETWEEN 100.0 AND 150.0 ) or ( "physical_sales_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" = 'D' and "physical_sales_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS" = '2 yr Degree' and "physical_sales_store_sales"."SS_SALES_PRICE" BETWEEN 50.0 AND 100.0 ) or ( "physical_sales_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" = 'S' and "physical_sales_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS" = 'College' and "physical_sales_store_sales"."SS_SALES_PRICE" BETWEEN 150.0 AND 200.0 ) ) and ( ( "physical_sales_sale_address_customer_address"."CA_COUNTRY" = 'United States' and "physical_sales_sale_address_customer_address"."CA_STATE" in ('CO','OH','TX') and "physical_sales_store_sales"."SS_NET_PROFIT" BETWEEN 0 AND 2000 ) or ( "physical_sales_sale_address_customer_address"."CA_COUNTRY" = 'United States' and "physical_sales_sale_address_customer_address"."CA_STATE" in ('OR','MN','KY') and "physical_sales_store_sales"."SS_NET_PROFIT" BETWEEN 150 AND 3000 ) or ( "physical_sales_sale_address_customer_address"."CA_COUNTRY" = 'United States' and "physical_sales_sale_address_customer_address"."CA_STATE" in ('VA','CA','MS') and "physical_sales_store_sales"."SS_NET_PROFIT" BETWEEN 50 AND 25000 ) )
```

## Reference SQL (zquery log)

```sql
SELECT
    sum("physical_sales_store_sales"."SS_QUANTITY") as "total_quantity"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "physical_sales_date_date" on "physical_sales_store_sales"."SS_SOLD_DATE_SK" = "physical_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer_address" as "physical_sales_sale_address_customer_address" on "physical_sales_store_sales"."SS_ADDR_SK" = "physical_sales_sale_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."customer_demographics" as "physical_sales_customer_demographic_customer_demographics" on "physical_sales_store_sales"."SS_CDEMO_SK" = "physical_sales_customer_demographic_customer_demographics"."CD_DEMO_SK"
WHERE
    "physical_sales_date_date"."D_YEAR" = 2000 and "physical_sales_store_sales"."SS_STORE_SK" is not null and ( ( "physical_sales_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" = 'M' and "physical_sales_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS" = '4 yr Degree' and "physical_sales_store_sales"."SS_SALES_PRICE" BETWEEN 100.0 AND 150.0 ) or ( "physical_sales_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" = 'D' and "physical_sales_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS" = '2 yr Degree' and "physical_sales_store_sales"."SS_SALES_PRICE" BETWEEN 50.0 AND 100.0 ) or ( "physical_sales_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" = 'S' and "physical_sales_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS" = 'College' and "physical_sales_store_sales"."SS_SALES_PRICE" BETWEEN 150.0 AND 200.0 ) ) and ( ( "physical_sales_sale_address_customer_address"."CA_COUNTRY" = 'United States' and "physical_sales_sale_address_customer_address"."CA_STATE" in ('CO','OH','TX') and "physical_sales_store_sales"."SS_NET_PROFIT" BETWEEN 0 AND 2000 ) or ( "physical_sales_sale_address_customer_address"."CA_COUNTRY" = 'United States' and "physical_sales_sale_address_customer_address"."CA_STATE" in ('OR','MN','KY') and "physical_sales_store_sales"."SS_NET_PROFIT" BETWEEN 150 AND 3000 ) or ( "physical_sales_sale_address_customer_address"."CA_COUNTRY" = 'United States' and "physical_sales_sale_address_customer_address"."CA_STATE" in ('VA','CA','MS') and "physical_sales_store_sales"."SS_NET_PROFIT" BETWEEN 50 AND 25000 ) )
```
