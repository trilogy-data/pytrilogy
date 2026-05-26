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
| v4 | 4531 | 57 | 74.42 ms |
| reference | 2893 | 13 | 36.80 ms |
| v4 / ref | 1.57x | 4.38x | 2.02x |

## Preql

```
import store_sales as store_sales;

where
    store_sales.date.year = 2001
    and (
        (
            store_sales.customer_demographic.marital_status = 'M'
            and store_sales.customer_demographic.education_status = 'Advanced Degree'
            and store_sales.sales_price between 100.0 and 150.0
            and store_sales.household_demographic.dependent_count = 3
        )
        or (
            store_sales.customer_demographic.marital_status = 'S'
            and store_sales.customer_demographic.education_status = 'College'
            and store_sales.sales_price between 50.0 and 100.0
            and store_sales.household_demographic.dependent_count = 1
        )
        or (
            store_sales.customer_demographic.marital_status = 'W'
            and store_sales.customer_demographic.education_status = '2 yr Degree'
            and store_sales.sales_price between 150.0 and 200.0
            and store_sales.household_demographic.dependent_count = 1
        )
    )
    and (
        (
            store_sales.sale_address.country = 'United States'
            and store_sales.sale_address.state in ('TX', 'OH', 'TX')
            and store_sales.net_profit between 100 and 200
        )
        or (
            store_sales.sale_address.country = 'United States'
            and store_sales.sale_address.state in ('OR', 'NM', 'KY')
            and store_sales.net_profit between 150 and 300
        )
        or (
            store_sales.sale_address.country = 'United States'
            and store_sales.sale_address.state in ('VA', 'TX', 'MS')
            and store_sales.net_profit between 50 and 250
        )
    )
select
    avg(store_sales.quantity) as avg1,
    avg(store_sales.ext_sales_price) as avg2,
    avg(store_sales.ext_wholesale_cost) as avg3,
    sum(store_sales.ext_wholesale_cost) as sum_ewc,
;
```

## v4 generated SQL

```sql
WITH 
thoughtful as (
SELECT
    "store_sales_store_sales"."SS_ADDR_SK" as "store_sales_sale_address_id",
    "store_sales_store_sales"."SS_CDEMO_SK" as "store_sales_customer_demographic_id",
    "store_sales_store_sales"."SS_EXT_SALES_PRICE" as "store_sales_ext_sales_price",
    "store_sales_store_sales"."SS_EXT_WHOLESALE_COST" as "store_sales_ext_wholesale_cost",
    "store_sales_store_sales"."SS_HDEMO_SK" as "store_sales_household_demographic_id",
    "store_sales_store_sales"."SS_NET_PROFIT" as "store_sales_net_profit",
    "store_sales_store_sales"."SS_QUANTITY" as "store_sales_quantity",
    "store_sales_store_sales"."SS_SALES_PRICE" as "store_sales_sales_price",
    "store_sales_store_sales"."SS_SOLD_DATE_SK" as "store_sales_date_id"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9),
questionable as (
SELECT
    "thoughtful"."store_sales_ext_sales_price" as "store_sales_ext_sales_price",
    "thoughtful"."store_sales_ext_wholesale_cost" as "store_sales_ext_wholesale_cost",
    "thoughtful"."store_sales_quantity" as "store_sales_quantity"
FROM
    "thoughtful"
    INNER JOIN "memory"."date_dim" as "store_sales_date_date" on "thoughtful"."store_sales_date_id" = "store_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer_address" as "store_sales_sale_address_customer_address" on "thoughtful"."store_sales_sale_address_id" = "store_sales_sale_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."customer_demographics" as "store_sales_customer_demographic_customer_demographics" on "thoughtful"."store_sales_customer_demographic_id" = "store_sales_customer_demographic_customer_demographics"."CD_DEMO_SK"
    INNER JOIN "memory"."household_demographics" as "store_sales_household_demographic_household_demographics" on "thoughtful"."store_sales_household_demographic_id" = "store_sales_household_demographic_household_demographics"."HD_DEMO_SK"
WHERE
    "store_sales_date_date"."D_YEAR" = 2001 and ( ( "store_sales_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" = 'M' and "store_sales_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS" = 'Advanced Degree' and "thoughtful"."store_sales_sales_price" BETWEEN 100.0 AND 150.0 and "store_sales_household_demographic_household_demographics"."HD_DEP_COUNT" = 3 ) or ( "store_sales_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" = 'S' and "store_sales_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS" = 'College' and "thoughtful"."store_sales_sales_price" BETWEEN 50.0 AND 100.0 and "store_sales_household_demographic_household_demographics"."HD_DEP_COUNT" = 1 ) or ( "store_sales_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" = 'W' and "store_sales_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS" = '2 yr Degree' and "thoughtful"."store_sales_sales_price" BETWEEN 150.0 AND 200.0 and "store_sales_household_demographic_household_demographics"."HD_DEP_COUNT" = 1 ) ) and ( ( "store_sales_sale_address_customer_address"."CA_COUNTRY" = 'United States' and "store_sales_sale_address_customer_address"."CA_STATE" in ('TX','OH','TX') and "thoughtful"."store_sales_net_profit" BETWEEN 100 AND 200 ) or ( "store_sales_sale_address_customer_address"."CA_COUNTRY" = 'United States' and "store_sales_sale_address_customer_address"."CA_STATE" in ('OR','NM','KY') and "thoughtful"."store_sales_net_profit" BETWEEN 150 AND 300 ) or ( "store_sales_sale_address_customer_address"."CA_COUNTRY" = 'United States' and "store_sales_sale_address_customer_address"."CA_STATE" in ('VA','TX','MS') and "thoughtful"."store_sales_net_profit" BETWEEN 50 AND 250 ) )

GROUP BY
    1,
    2,
    3,
    "store_sales_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS",
    "store_sales_customer_demographic_customer_demographics"."CD_MARITAL_STATUS",
    "store_sales_date_date"."D_YEAR",
    "store_sales_household_demographic_household_demographics"."HD_DEP_COUNT",
    "store_sales_sale_address_customer_address"."CA_COUNTRY",
    "store_sales_sale_address_customer_address"."CA_STATE",
    "thoughtful"."store_sales_net_profit",
    "thoughtful"."store_sales_sales_price")
SELECT
    avg("questionable"."store_sales_quantity") as "avg1",
    avg("questionable"."store_sales_ext_sales_price") as "avg2",
    avg("questionable"."store_sales_ext_wholesale_cost") as "avg3",
    sum("questionable"."store_sales_ext_wholesale_cost") as "sum_ewc"
FROM
    "questionable"
```

## Reference SQL (zquery log)

```sql
SELECT
    avg("store_sales_store_sales"."SS_QUANTITY") as "avg1",
    avg("store_sales_store_sales"."SS_EXT_SALES_PRICE") as "avg2",
    avg("store_sales_store_sales"."SS_EXT_WHOLESALE_COST") as "avg3",
    sum("store_sales_store_sales"."SS_EXT_WHOLESALE_COST") as "sum_ewc"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer_address" as "store_sales_sale_address_customer_address" on "store_sales_store_sales"."SS_ADDR_SK" = "store_sales_sale_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."customer_demographics" as "store_sales_customer_demographic_customer_demographics" on "store_sales_store_sales"."SS_CDEMO_SK" = "store_sales_customer_demographic_customer_demographics"."CD_DEMO_SK"
    INNER JOIN "memory"."household_demographics" as "store_sales_household_demographic_household_demographics" on "store_sales_store_sales"."SS_HDEMO_SK" = "store_sales_household_demographic_household_demographics"."HD_DEMO_SK"
WHERE
    "store_sales_date_date"."D_YEAR" = 2001 and ( ( "store_sales_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" = 'M' and "store_sales_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS" = 'Advanced Degree' and "store_sales_store_sales"."SS_SALES_PRICE" BETWEEN 100.0 AND 150.0 and "store_sales_household_demographic_household_demographics"."HD_DEP_COUNT" = 3 ) or ( "store_sales_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" = 'S' and "store_sales_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS" = 'College' and "store_sales_store_sales"."SS_SALES_PRICE" BETWEEN 50.0 AND 100.0 and "store_sales_household_demographic_household_demographics"."HD_DEP_COUNT" = 1 ) or ( "store_sales_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" = 'W' and "store_sales_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS" = '2 yr Degree' and "store_sales_store_sales"."SS_SALES_PRICE" BETWEEN 150.0 AND 200.0 and "store_sales_household_demographic_household_demographics"."HD_DEP_COUNT" = 1 ) ) and ( ( "store_sales_sale_address_customer_address"."CA_COUNTRY" = 'United States' and "store_sales_sale_address_customer_address"."CA_STATE" in ('TX','OH','TX') and "store_sales_store_sales"."SS_NET_PROFIT" BETWEEN 100 AND 200 ) or ( "store_sales_sale_address_customer_address"."CA_COUNTRY" = 'United States' and "store_sales_sale_address_customer_address"."CA_STATE" in ('OR','NM','KY') and "store_sales_store_sales"."SS_NET_PROFIT" BETWEEN 150 AND 300 ) or ( "store_sales_sale_address_customer_address"."CA_COUNTRY" = 'United States' and "store_sales_sale_address_customer_address"."CA_STATE" in ('VA','TX','MS') and "store_sales_store_sales"."SS_NET_PROFIT" BETWEEN 50 AND 250 ) )
```
