# Query 13

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
| v4 | 3010 | 13 | 37.96 ms |
| reference | 3010 | 13 | 38.79 ms |
| v4 / ref | 1.00x | 1.00x | 0.98x |

## Preql

```
import physical_sales as physical_sales;

where
    physical_sales.date.year = 2001
    and (
        (
            physical_sales.customer_demographic.marital_status = 'M'
            and physical_sales.customer_demographic.education_status = 'Advanced Degree'
            and physical_sales.sales_price between 100.0 and 150.0
            and physical_sales.household_demographic.dependent_count = 3
        )
        or (
            physical_sales.customer_demographic.marital_status = 'S'
            and physical_sales.customer_demographic.education_status = 'College'
            and physical_sales.sales_price between 50.0 and 100.0
            and physical_sales.household_demographic.dependent_count = 1
        )
        or (
            physical_sales.customer_demographic.marital_status = 'W'
            and physical_sales.customer_demographic.education_status = '2 yr Degree'
            and physical_sales.sales_price between 150.0 and 200.0
            and physical_sales.household_demographic.dependent_count = 1
        )
    )
    and (
        (
            physical_sales.sale_address.country = 'United States'
            and physical_sales.sale_address.state in ('TX', 'OH', 'TX')
            and physical_sales.net_profit between 100 and 200
        )
        or (
            physical_sales.sale_address.country = 'United States'
            and physical_sales.sale_address.state in ('OR', 'NM', 'KY')
            and physical_sales.net_profit between 150 and 300
        )
        or (
            physical_sales.sale_address.country = 'United States'
            and physical_sales.sale_address.state in ('VA', 'TX', 'MS')
            and physical_sales.net_profit between 50 and 250
        )
    )
select
    avg(physical_sales.quantity) as avg1,
    avg(physical_sales.ext_sales_price) as avg2,
    avg(physical_sales.ext_wholesale_cost) as avg3,
    sum(physical_sales.ext_wholesale_cost) as sum_ewc,
;
```

## v4 generated SQL

```sql
SELECT
    avg("physical_sales_store_sales"."SS_QUANTITY") as "avg1",
    avg("physical_sales_store_sales"."SS_EXT_SALES_PRICE") as "avg2",
    avg("physical_sales_store_sales"."SS_EXT_WHOLESALE_COST") as "avg3",
    sum("physical_sales_store_sales"."SS_EXT_WHOLESALE_COST") as "sum_ewc"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "physical_sales_date_date" on "physical_sales_store_sales"."SS_SOLD_DATE_SK" = "physical_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer_address" as "physical_sales_sale_address_customer_address" on "physical_sales_store_sales"."SS_ADDR_SK" = "physical_sales_sale_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."customer_demographics" as "physical_sales_customer_demographic_customer_demographics" on "physical_sales_store_sales"."SS_CDEMO_SK" = "physical_sales_customer_demographic_customer_demographics"."CD_DEMO_SK"
    INNER JOIN "memory"."household_demographics" as "physical_sales_household_demographic_household_demographics" on "physical_sales_store_sales"."SS_HDEMO_SK" = "physical_sales_household_demographic_household_demographics"."HD_DEMO_SK"
WHERE
    "physical_sales_date_date"."D_YEAR" = 2001 and ( ( "physical_sales_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" = 'M' and "physical_sales_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS" = 'Advanced Degree' and "physical_sales_store_sales"."SS_SALES_PRICE" BETWEEN 100.0 AND 150.0 and "physical_sales_household_demographic_household_demographics"."HD_DEP_COUNT" = 3 ) or ( "physical_sales_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" = 'S' and "physical_sales_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS" = 'College' and "physical_sales_store_sales"."SS_SALES_PRICE" BETWEEN 50.0 AND 100.0 and "physical_sales_household_demographic_household_demographics"."HD_DEP_COUNT" = 1 ) or ( "physical_sales_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" = 'W' and "physical_sales_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS" = '2 yr Degree' and "physical_sales_store_sales"."SS_SALES_PRICE" BETWEEN 150.0 AND 200.0 and "physical_sales_household_demographic_household_demographics"."HD_DEP_COUNT" = 1 ) ) and ( ( "physical_sales_sale_address_customer_address"."CA_COUNTRY" = 'United States' and "physical_sales_sale_address_customer_address"."CA_STATE" in ('TX','OH','TX') and "physical_sales_store_sales"."SS_NET_PROFIT" BETWEEN 100 AND 200 ) or ( "physical_sales_sale_address_customer_address"."CA_COUNTRY" = 'United States' and "physical_sales_sale_address_customer_address"."CA_STATE" in ('OR','NM','KY') and "physical_sales_store_sales"."SS_NET_PROFIT" BETWEEN 150 AND 300 ) or ( "physical_sales_sale_address_customer_address"."CA_COUNTRY" = 'United States' and "physical_sales_sale_address_customer_address"."CA_STATE" in ('VA','TX','MS') and "physical_sales_store_sales"."SS_NET_PROFIT" BETWEEN 50 AND 250 ) )
```

## Reference SQL (zquery log)

```sql
SELECT
    avg("physical_sales_store_sales"."SS_QUANTITY") as "avg1",
    avg("physical_sales_store_sales"."SS_EXT_SALES_PRICE") as "avg2",
    avg("physical_sales_store_sales"."SS_EXT_WHOLESALE_COST") as "avg3",
    sum("physical_sales_store_sales"."SS_EXT_WHOLESALE_COST") as "sum_ewc"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "physical_sales_date_date" on "physical_sales_store_sales"."SS_SOLD_DATE_SK" = "physical_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer_address" as "physical_sales_sale_address_customer_address" on "physical_sales_store_sales"."SS_ADDR_SK" = "physical_sales_sale_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."customer_demographics" as "physical_sales_customer_demographic_customer_demographics" on "physical_sales_store_sales"."SS_CDEMO_SK" = "physical_sales_customer_demographic_customer_demographics"."CD_DEMO_SK"
    INNER JOIN "memory"."household_demographics" as "physical_sales_household_demographic_household_demographics" on "physical_sales_store_sales"."SS_HDEMO_SK" = "physical_sales_household_demographic_household_demographics"."HD_DEMO_SK"
WHERE
    "physical_sales_date_date"."D_YEAR" = 2001 and ( ( "physical_sales_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" = 'M' and "physical_sales_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS" = 'Advanced Degree' and "physical_sales_store_sales"."SS_SALES_PRICE" BETWEEN 100.0 AND 150.0 and "physical_sales_household_demographic_household_demographics"."HD_DEP_COUNT" = 3 ) or ( "physical_sales_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" = 'S' and "physical_sales_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS" = 'College' and "physical_sales_store_sales"."SS_SALES_PRICE" BETWEEN 50.0 AND 100.0 and "physical_sales_household_demographic_household_demographics"."HD_DEP_COUNT" = 1 ) or ( "physical_sales_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" = 'W' and "physical_sales_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS" = '2 yr Degree' and "physical_sales_store_sales"."SS_SALES_PRICE" BETWEEN 150.0 AND 200.0 and "physical_sales_household_demographic_household_demographics"."HD_DEP_COUNT" = 1 ) ) and ( ( "physical_sales_sale_address_customer_address"."CA_COUNTRY" = 'United States' and "physical_sales_sale_address_customer_address"."CA_STATE" in ('TX','OH','TX') and "physical_sales_store_sales"."SS_NET_PROFIT" BETWEEN 100 AND 200 ) or ( "physical_sales_sale_address_customer_address"."CA_COUNTRY" = 'United States' and "physical_sales_sale_address_customer_address"."CA_STATE" in ('OR','NM','KY') and "physical_sales_store_sales"."SS_NET_PROFIT" BETWEEN 150 AND 300 ) or ( "physical_sales_sale_address_customer_address"."CA_COUNTRY" = 'United States' and "physical_sales_sale_address_customer_address"."CA_STATE" in ('VA','TX','MS') and "physical_sales_store_sales"."SS_NET_PROFIT" BETWEEN 50 AND 250 ) )
```
