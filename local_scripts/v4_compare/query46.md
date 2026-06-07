# Query 46

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
| v4 | 3752 | 71 | 62.89 ms |
| reference | 2951 | 51 | 60.24 ms |
| v4 / ref | 1.27x | 1.39x | 1.04x |

## Preql

```
import physical_sales as physical_sales;
import customer as customer;

select
    --customer.id,
    customer.last_name,
    customer.first_name,
    customer.address.city,
merge
where
    (
        physical_sales.household_demographic.dependent_count = 4
        or physical_sales.household_demographic.vehicle_count = 3
    )
    and physical_sales.date.day_of_week in (6, 0)
    and physical_sales.date.year in (1999, 2000, 2001)
    and physical_sales.store.city in ('Fairview', 'Midway')
select
    physical_sales.sale_address.city as bought_city,
    physical_sales.ticket_number,
    --physical_sales.customer.id,
    sum(physical_sales.coupon_amt) as amt,
    sum(physical_sales.net_profit) as profit,
align
    --customer_id: physical_sales.customer.id, customer.id
where
customer.address.city != bought_city
order by
    customer.last_name asc nulls first,
    customer.first_name asc nulls first,
    customer.address.city asc nulls first,
    bought_city asc nulls first,
    physical_sales.ticket_number asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
uneven as (
SELECT
    "physical_sales_sale_address_customer_address"."CA_CITY" as "physical_sales_sale_address_city",
    "physical_sales_store_sales"."SS_CUSTOMER_SK" as "physical_sales_customer_id",
    "physical_sales_store_sales"."SS_TICKET_NUMBER" as "physical_sales_ticket_number",
    sum("physical_sales_store_sales"."SS_COUPON_AMT") as "amt",
    sum("physical_sales_store_sales"."SS_NET_PROFIT") as "profit"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "physical_sales_date_date" on "physical_sales_store_sales"."SS_SOLD_DATE_SK" = "physical_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."store" as "physical_sales_store_store" on "physical_sales_store_sales"."SS_STORE_SK" = "physical_sales_store_store"."S_STORE_SK"
    LEFT OUTER JOIN "memory"."customer_address" as "physical_sales_sale_address_customer_address" on "physical_sales_store_sales"."SS_ADDR_SK" = "physical_sales_sale_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."household_demographics" as "physical_sales_household_demographic_household_demographics" on "physical_sales_store_sales"."SS_HDEMO_SK" = "physical_sales_household_demographic_household_demographics"."HD_DEMO_SK"
WHERE
    ( "physical_sales_household_demographic_household_demographics"."HD_DEP_COUNT" = 4 or "physical_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT" = 3 ) and "physical_sales_date_date"."D_DOW" in (6,0) and "physical_sales_date_date"."D_YEAR" in (1999,2000,2001) and "physical_sales_store_store"."S_CITY" in ('Fairview','Midway')

GROUP BY
    1,
    2,
    3),
wakeful as (
SELECT
    "customer_address_customer_address"."CA_CITY" as "customer_address_city",
    "customer_customers"."C_CUSTOMER_SK" as "customer_id",
    "customer_customers"."C_FIRST_NAME" as "customer_first_name",
    "customer_customers"."C_LAST_NAME" as "customer_last_name"
FROM
    "memory"."customer" as "customer_customers"
    INNER JOIN "memory"."customer_address" as "customer_address_customer_address" on "customer_customers"."C_CURRENT_ADDR_SK" = "customer_address_customer_address"."CA_ADDRESS_SK"),
vacuous as (
SELECT
    "uneven"."amt" as "amt",
    "uneven"."physical_sales_customer_id" as "customer_id",
    "uneven"."physical_sales_sale_address_city" as "bought_city",
    "uneven"."physical_sales_ticket_number" as "physical_sales_ticket_number",
    "uneven"."profit" as "profit"
FROM
    "uneven"),
concerned as (
SELECT
    "vacuous"."amt" as "amt",
    "vacuous"."bought_city" as "bought_city",
    "vacuous"."physical_sales_ticket_number" as "physical_sales_ticket_number",
    "vacuous"."profit" as "profit",
    "wakeful"."customer_address_city" as "customer_address_city",
    "wakeful"."customer_first_name" as "customer_first_name",
    "wakeful"."customer_last_name" as "customer_last_name"
FROM
    "wakeful"
    INNER JOIN "vacuous" on "wakeful"."customer_id" = "vacuous"."customer_id"
WHERE
    "wakeful"."customer_address_city" != "vacuous"."bought_city"
)
SELECT
    "concerned"."customer_last_name" as "customer_last_name",
    "concerned"."customer_first_name" as "customer_first_name",
    "concerned"."customer_address_city" as "customer_address_city",
    "concerned"."bought_city" as "bought_city",
    "concerned"."physical_sales_ticket_number" as "physical_sales_ticket_number",
    "concerned"."amt" as "amt",
    "concerned"."profit" as "profit"
FROM
    "concerned"
ORDER BY 
    "concerned"."customer_last_name" asc nulls first,
    "concerned"."customer_first_name" asc nulls first,
    "concerned"."customer_address_city" asc nulls first,
    "concerned"."bought_city" asc nulls first,
    "concerned"."physical_sales_ticket_number" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
wakeful as (
SELECT
    "customer_address_customer_address"."CA_CITY" as "customer_address_city",
    "customer_customers"."C_CUSTOMER_SK" as "customer_id",
    "customer_customers"."C_FIRST_NAME" as "customer_first_name",
    "customer_customers"."C_LAST_NAME" as "customer_last_name"
FROM
    "memory"."customer" as "customer_customers"
    INNER JOIN "memory"."customer_address" as "customer_address_customer_address" on "customer_customers"."C_CURRENT_ADDR_SK" = "customer_address_customer_address"."CA_ADDRESS_SK"),
uneven as (
SELECT
    "physical_sales_sale_address_customer_address"."CA_CITY" as "bought_city",
    "physical_sales_store_sales"."SS_CUSTOMER_SK" as "customer_id",
    "physical_sales_store_sales"."SS_TICKET_NUMBER" as "physical_sales_ticket_number",
    sum("physical_sales_store_sales"."SS_COUPON_AMT") as "amt",
    sum("physical_sales_store_sales"."SS_NET_PROFIT") as "profit"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "physical_sales_date_date" on "physical_sales_store_sales"."SS_SOLD_DATE_SK" = "physical_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."store" as "physical_sales_store_store" on "physical_sales_store_sales"."SS_STORE_SK" = "physical_sales_store_store"."S_STORE_SK"
    LEFT OUTER JOIN "memory"."customer_address" as "physical_sales_sale_address_customer_address" on "physical_sales_store_sales"."SS_ADDR_SK" = "physical_sales_sale_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."household_demographics" as "physical_sales_household_demographic_household_demographics" on "physical_sales_store_sales"."SS_HDEMO_SK" = "physical_sales_household_demographic_household_demographics"."HD_DEMO_SK"
WHERE
    ( "physical_sales_household_demographic_household_demographics"."HD_DEP_COUNT" = 4 or "physical_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT" = 3 ) and "physical_sales_date_date"."D_DOW" in (6,0) and "physical_sales_date_date"."D_YEAR" in (1999,2000,2001) and "physical_sales_store_store"."S_CITY" in ('Fairview','Midway')

GROUP BY
    1,
    2,
    3)
SELECT
    "wakeful"."customer_last_name" as "customer_last_name",
    "wakeful"."customer_first_name" as "customer_first_name",
    "wakeful"."customer_address_city" as "customer_address_city",
    "uneven"."bought_city" as "bought_city",
    "uneven"."physical_sales_ticket_number" as "physical_sales_ticket_number",
    "uneven"."amt" as "amt",
    "uneven"."profit" as "profit"
FROM
    "wakeful"
    INNER JOIN "uneven" on "wakeful"."customer_id" = "uneven"."customer_id"
WHERE
    "wakeful"."customer_address_city" != "uneven"."bought_city"

ORDER BY 
    "wakeful"."customer_last_name" asc nulls first,
    "wakeful"."customer_first_name" asc nulls first,
    "wakeful"."customer_address_city" asc nulls first,
    "uneven"."bought_city" asc nulls first,
    "uneven"."physical_sales_ticket_number" asc nulls first
LIMIT (100)
```
