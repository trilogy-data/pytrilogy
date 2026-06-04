# Query 79

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
| v4 | 2751 | 42 | 63.89 ms |
| reference | 2520 | 38 | 42.29 ms |
| v4 / ref | 1.09x | 1.11x | 1.51x |

## Preql

```
import physical_sales as physical_sales;

where
    (
        physical_sales.household_demographic.dependent_count = 6
        or physical_sales.household_demographic.vehicle_count > 2
    )
    and physical_sales.date.day_of_week = 1
    and physical_sales.date.year in (1999, 2000, 2001)
    and physical_sales.store.employees between 200 and 295
    and physical_sales.billing_customer.id is not null
select
    --physical_sales.sale_address.id,
    physical_sales.billing_customer.last_name,
    physical_sales.billing_customer.first_name,
    substring(physical_sales.store.city, 1, 30) as city_short,
    physical_sales.ticket_number,
    sum(physical_sales.coupon_amt)
            by physical_sales.ticket_number, physical_sales.billing_customer.id, physical_sales.sale_address.id, physical_sales.store.city as amt,
    sum(physical_sales.net_profit)
            by physical_sales.ticket_number, physical_sales.billing_customer.id, physical_sales.sale_address.id, physical_sales.store.city as profit,
order by
    physical_sales.billing_customer.last_name asc nulls first,
    physical_sales.billing_customer.first_name asc nulls first,
    city_short asc nulls first,
    profit asc nulls first,
    physical_sales.ticket_number asc
limit 100
;
```

## v4 generated SQL

```sql
WITH 
cooperative as (
SELECT
    "physical_sales_billing_customer_customers"."C_FIRST_NAME" as "physical_sales_billing_customer_first_name",
    "physical_sales_billing_customer_customers"."C_LAST_NAME" as "physical_sales_billing_customer_last_name",
    "physical_sales_store_sales"."SS_CUSTOMER_SK" as "physical_sales_billing_customer_id",
    "physical_sales_store_sales"."SS_TICKET_NUMBER" as "physical_sales_ticket_number",
    "physical_sales_store_store"."S_CITY" as "physical_sales_store_city",
    sum("physical_sales_store_sales"."SS_COUPON_AMT") as "amt",
    sum("physical_sales_store_sales"."SS_NET_PROFIT") as "profit"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "physical_sales_date_date" on "physical_sales_store_sales"."SS_SOLD_DATE_SK" = "physical_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."store" as "physical_sales_store_store" on "physical_sales_store_sales"."SS_STORE_SK" = "physical_sales_store_store"."S_STORE_SK"
    INNER JOIN "memory"."customer" as "physical_sales_billing_customer_customers" on "physical_sales_store_sales"."SS_CUSTOMER_SK" = "physical_sales_billing_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."household_demographics" as "physical_sales_household_demographic_household_demographics" on "physical_sales_store_sales"."SS_HDEMO_SK" = "physical_sales_household_demographic_household_demographics"."HD_DEMO_SK"
WHERE
    ( "physical_sales_household_demographic_household_demographics"."HD_DEP_COUNT" = 6 or "physical_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT" > 2 ) and "physical_sales_date_date"."D_DOW" = 1 and "physical_sales_date_date"."D_YEAR" in (1999,2000,2001) and "physical_sales_store_store"."S_NUMBER_EMPLOYEES" BETWEEN 200 AND 295 and "physical_sales_store_sales"."SS_CUSTOMER_SK" is not null

GROUP BY
    1,
    2,
    3,
    4,
    5,
    "physical_sales_store_sales"."SS_ADDR_SK")
SELECT
    "cooperative"."physical_sales_billing_customer_last_name" as "physical_sales_billing_customer_last_name",
    "cooperative"."physical_sales_billing_customer_first_name" as "physical_sales_billing_customer_first_name",
    SUBSTRING("cooperative"."physical_sales_store_city",1,30) as "city_short",
    "cooperative"."physical_sales_ticket_number" as "physical_sales_ticket_number",
    "cooperative"."amt" as "amt",
    "cooperative"."profit" as "profit"
FROM
    "cooperative"
ORDER BY 
    "cooperative"."physical_sales_billing_customer_last_name" asc nulls first,
    "cooperative"."physical_sales_billing_customer_first_name" asc nulls first,
    "city_short" asc nulls first,
    "cooperative"."profit" asc nulls first,
    "cooperative"."physical_sales_ticket_number" asc
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
cooperative as (
SELECT
    "physical_sales_store_sales"."SS_CUSTOMER_SK" as "physical_sales_billing_customer_id",
    "physical_sales_store_sales"."SS_TICKET_NUMBER" as "physical_sales_ticket_number",
    "physical_sales_store_store"."S_CITY" as "physical_sales_store_city",
    sum("physical_sales_store_sales"."SS_COUPON_AMT") as "amt",
    sum("physical_sales_store_sales"."SS_NET_PROFIT") as "profit"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "physical_sales_date_date" on "physical_sales_store_sales"."SS_SOLD_DATE_SK" = "physical_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."store" as "physical_sales_store_store" on "physical_sales_store_sales"."SS_STORE_SK" = "physical_sales_store_store"."S_STORE_SK"
    INNER JOIN "memory"."household_demographics" as "physical_sales_household_demographic_household_demographics" on "physical_sales_store_sales"."SS_HDEMO_SK" = "physical_sales_household_demographic_household_demographics"."HD_DEMO_SK"
WHERE
    ( "physical_sales_household_demographic_household_demographics"."HD_DEP_COUNT" = 6 or "physical_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT" > 2 ) and "physical_sales_date_date"."D_DOW" = 1 and "physical_sales_date_date"."D_YEAR" in (1999,2000,2001) and "physical_sales_store_store"."S_NUMBER_EMPLOYEES" BETWEEN 200 AND 295 and "physical_sales_store_sales"."SS_CUSTOMER_SK" is not null

GROUP BY
    1,
    2,
    3,
    "physical_sales_store_sales"."SS_ADDR_SK")
SELECT
    "physical_sales_billing_customer_customers"."C_LAST_NAME" as "physical_sales_billing_customer_last_name",
    "physical_sales_billing_customer_customers"."C_FIRST_NAME" as "physical_sales_billing_customer_first_name",
    SUBSTRING("cooperative"."physical_sales_store_city",1,30) as "city_short",
    "cooperative"."physical_sales_ticket_number" as "physical_sales_ticket_number",
    "cooperative"."amt" as "amt",
    "cooperative"."profit" as "profit"
FROM
    "cooperative"
    INNER JOIN "memory"."customer" as "physical_sales_billing_customer_customers" on "cooperative"."physical_sales_billing_customer_id" = "physical_sales_billing_customer_customers"."C_CUSTOMER_SK"
ORDER BY 
    "physical_sales_billing_customer_customers"."C_LAST_NAME" asc nulls first,
    "physical_sales_billing_customer_customers"."C_FIRST_NAME" asc nulls first,
    "city_short" asc nulls first,
    "cooperative"."profit" asc nulls first,
    "cooperative"."physical_sales_ticket_number" asc
LIMIT (100)
```
