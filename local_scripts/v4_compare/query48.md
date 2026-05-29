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
| v4 | 2273 | 9 | 43.90 ms |
| reference | 2273 | 9 | 44.39 ms |
| v4 / ref | 1.00x | 1.00x | 0.99x |

## Preql

```
import store_sales as store_sales;

where
    store_sales.date.year = 2000
    and store_sales.store.id is not null
    and (
        (
            store_sales.customer_demographic.marital_status = 'M'
            and store_sales.customer_demographic.education_status = '4 yr Degree'
            and store_sales.sales_price between 100.0 and 150.0
        )
        or (
            store_sales.customer_demographic.marital_status = 'D'
            and store_sales.customer_demographic.education_status = '2 yr Degree'
            and store_sales.sales_price between 50.0 and 100.0
        )
        or (
            store_sales.customer_demographic.marital_status = 'S'
            and store_sales.customer_demographic.education_status = 'College'
            and store_sales.sales_price between 150.0 and 200.0
        )
    )
    and (
        (
            store_sales.sale_address.country = 'United States'
            and store_sales.sale_address.state in ('CO', 'OH', 'TX')
            and store_sales.net_profit between 0 and 2000
        )
        or (
            store_sales.sale_address.country = 'United States'
            and store_sales.sale_address.state in ('OR', 'MN', 'KY')
            and store_sales.net_profit between 150 and 3000
        )
        or (
            store_sales.sale_address.country = 'United States'
            and store_sales.sale_address.state in ('VA', 'CA', 'MS')
            and store_sales.net_profit between 50 and 25000
        )
    )
select
    sum(store_sales.quantity) as total_quantity,
;
```

## v4 generated SQL

```sql
SELECT
    sum("store_sales_store_sales"."SS_QUANTITY") as "total_quantity"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer_address" as "store_sales_sale_address_customer_address" on "store_sales_store_sales"."SS_ADDR_SK" = "store_sales_sale_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."customer_demographics" as "store_sales_customer_demographic_customer_demographics" on "store_sales_store_sales"."SS_CDEMO_SK" = "store_sales_customer_demographic_customer_demographics"."CD_DEMO_SK"
WHERE
    "store_sales_date_date"."D_YEAR" = 2000 and "store_sales_store_sales"."SS_STORE_SK" is not null and ( ( "store_sales_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" = 'M' and "store_sales_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS" = '4 yr Degree' and "store_sales_store_sales"."SS_SALES_PRICE" BETWEEN 100.0 AND 150.0 ) or ( "store_sales_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" = 'D' and "store_sales_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS" = '2 yr Degree' and "store_sales_store_sales"."SS_SALES_PRICE" BETWEEN 50.0 AND 100.0 ) or ( "store_sales_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" = 'S' and "store_sales_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS" = 'College' and "store_sales_store_sales"."SS_SALES_PRICE" BETWEEN 150.0 AND 200.0 ) ) and ( ( "store_sales_sale_address_customer_address"."CA_COUNTRY" = 'United States' and "store_sales_sale_address_customer_address"."CA_STATE" in ('CO','OH','TX') and "store_sales_store_sales"."SS_NET_PROFIT" BETWEEN 0 AND 2000 ) or ( "store_sales_sale_address_customer_address"."CA_COUNTRY" = 'United States' and "store_sales_sale_address_customer_address"."CA_STATE" in ('OR','MN','KY') and "store_sales_store_sales"."SS_NET_PROFIT" BETWEEN 150 AND 3000 ) or ( "store_sales_sale_address_customer_address"."CA_COUNTRY" = 'United States' and "store_sales_sale_address_customer_address"."CA_STATE" in ('VA','CA','MS') and "store_sales_store_sales"."SS_NET_PROFIT" BETWEEN 50 AND 25000 ) )
```

## Reference SQL (zquery log)

```sql
SELECT
    sum("store_sales_store_sales"."SS_QUANTITY") as "total_quantity"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer_address" as "store_sales_sale_address_customer_address" on "store_sales_store_sales"."SS_ADDR_SK" = "store_sales_sale_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."customer_demographics" as "store_sales_customer_demographic_customer_demographics" on "store_sales_store_sales"."SS_CDEMO_SK" = "store_sales_customer_demographic_customer_demographics"."CD_DEMO_SK"
WHERE
    "store_sales_date_date"."D_YEAR" = 2000 and "store_sales_store_sales"."SS_STORE_SK" is not null and ( ( "store_sales_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" = 'M' and "store_sales_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS" = '4 yr Degree' and "store_sales_store_sales"."SS_SALES_PRICE" BETWEEN 100.0 AND 150.0 ) or ( "store_sales_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" = 'D' and "store_sales_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS" = '2 yr Degree' and "store_sales_store_sales"."SS_SALES_PRICE" BETWEEN 50.0 AND 100.0 ) or ( "store_sales_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" = 'S' and "store_sales_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS" = 'College' and "store_sales_store_sales"."SS_SALES_PRICE" BETWEEN 150.0 AND 200.0 ) ) and ( ( "store_sales_sale_address_customer_address"."CA_COUNTRY" = 'United States' and "store_sales_sale_address_customer_address"."CA_STATE" in ('CO','OH','TX') and "store_sales_store_sales"."SS_NET_PROFIT" BETWEEN 0 AND 2000 ) or ( "store_sales_sale_address_customer_address"."CA_COUNTRY" = 'United States' and "store_sales_sale_address_customer_address"."CA_STATE" in ('OR','MN','KY') and "store_sales_store_sales"."SS_NET_PROFIT" BETWEEN 150 AND 3000 ) or ( "store_sales_sale_address_customer_address"."CA_COUNTRY" = 'United States' and "store_sales_sale_address_customer_address"."CA_STATE" in ('VA','CA','MS') and "store_sales_store_sales"."SS_NET_PROFIT" BETWEEN 50 AND 25000 ) )
```
