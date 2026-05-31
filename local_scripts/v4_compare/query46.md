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
| v4 | 3603 | 71 | 75.71 ms |
| reference | 2870 | 51 | 70.76 ms |
| v4 / ref | 1.26x | 1.39x | 1.07x |

## Preql

```
import store_sales as store_sales;
import customer as customer;

select
    --customer.id,
    customer.last_name,
    customer.first_name,
    customer.address.city,
merge
where
    (
        store_sales.household_demographic.dependent_count = 4
        or store_sales.household_demographic.vehicle_count = 3
    )
    and store_sales.date.day_of_week in (6, 0)
    and store_sales.date.year in (1999, 2000, 2001)
    and store_sales.store.city in ('Fairview', 'Midway')
select
    store_sales.sale_address.city as bought_city,
    store_sales.ticket_number,
    --store_sales.customer.id,
    sum(store_sales.coupon_amt) as amt,
    sum(store_sales.net_profit) as profit,
align
    --customer_id: store_sales.customer.id, customer.id
where
customer.address.city != bought_city
order by
    customer.last_name asc nulls first,
    customer.first_name asc nulls first,
    customer.address.city asc nulls first,
    bought_city asc nulls first,
    store_sales.ticket_number asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
uneven as (
SELECT
    "store_sales_sale_address_customer_address"."CA_CITY" as "store_sales_sale_address_city",
    "store_sales_store_sales"."SS_CUSTOMER_SK" as "store_sales_customer_id",
    "store_sales_store_sales"."SS_TICKET_NUMBER" as "store_sales_ticket_number",
    sum("store_sales_store_sales"."SS_COUPON_AMT") as "amt",
    sum("store_sales_store_sales"."SS_NET_PROFIT") as "profit"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."store" as "store_sales_store_store" on "store_sales_store_sales"."SS_STORE_SK" = "store_sales_store_store"."S_STORE_SK"
    LEFT OUTER JOIN "memory"."customer_address" as "store_sales_sale_address_customer_address" on "store_sales_store_sales"."SS_ADDR_SK" = "store_sales_sale_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."household_demographics" as "store_sales_household_demographic_household_demographics" on "store_sales_store_sales"."SS_HDEMO_SK" = "store_sales_household_demographic_household_demographics"."HD_DEMO_SK"
WHERE
    ( "store_sales_household_demographic_household_demographics"."HD_DEP_COUNT" = 4 or "store_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT" = 3 ) and "store_sales_date_date"."D_DOW" in (6,0) and "store_sales_date_date"."D_YEAR" in (1999,2000,2001) and "store_sales_store_store"."S_CITY" in ('Fairview','Midway')

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
juicy as (
SELECT
    "uneven"."amt" as "amt",
    "uneven"."profit" as "profit",
    "uneven"."store_sales_customer_id" as "customer_id",
    "uneven"."store_sales_sale_address_city" as "bought_city",
    "uneven"."store_sales_ticket_number" as "store_sales_ticket_number"
FROM
    "uneven"),
vacuous as (
SELECT
    "juicy"."amt" as "amt",
    "juicy"."bought_city" as "bought_city",
    "juicy"."profit" as "profit",
    "juicy"."store_sales_ticket_number" as "store_sales_ticket_number",
    "wakeful"."customer_address_city" as "customer_address_city",
    "wakeful"."customer_first_name" as "customer_first_name",
    "wakeful"."customer_last_name" as "customer_last_name"
FROM
    "wakeful"
    INNER JOIN "juicy" on "wakeful"."customer_id" = "juicy"."customer_id"
WHERE
    "wakeful"."customer_address_city" != "juicy"."bought_city"
)
SELECT
    "vacuous"."customer_last_name" as "customer_last_name",
    "vacuous"."customer_first_name" as "customer_first_name",
    "vacuous"."customer_address_city" as "customer_address_city",
    "vacuous"."bought_city" as "bought_city",
    "vacuous"."store_sales_ticket_number" as "store_sales_ticket_number",
    "vacuous"."amt" as "amt",
    "vacuous"."profit" as "profit"
FROM
    "vacuous"
ORDER BY 
    "vacuous"."customer_last_name" asc nulls first,
    "vacuous"."customer_first_name" asc nulls first,
    "vacuous"."customer_address_city" asc nulls first,
    "vacuous"."bought_city" asc nulls first,
    "vacuous"."store_sales_ticket_number" asc nulls first
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
    "store_sales_sale_address_customer_address"."CA_CITY" as "bought_city",
    "store_sales_store_sales"."SS_CUSTOMER_SK" as "customer_id",
    "store_sales_store_sales"."SS_TICKET_NUMBER" as "store_sales_ticket_number",
    sum("store_sales_store_sales"."SS_COUPON_AMT") as "amt",
    sum("store_sales_store_sales"."SS_NET_PROFIT") as "profit"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."store" as "store_sales_store_store" on "store_sales_store_sales"."SS_STORE_SK" = "store_sales_store_store"."S_STORE_SK"
    LEFT OUTER JOIN "memory"."customer_address" as "store_sales_sale_address_customer_address" on "store_sales_store_sales"."SS_ADDR_SK" = "store_sales_sale_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."household_demographics" as "store_sales_household_demographic_household_demographics" on "store_sales_store_sales"."SS_HDEMO_SK" = "store_sales_household_demographic_household_demographics"."HD_DEMO_SK"
WHERE
    ( "store_sales_household_demographic_household_demographics"."HD_DEP_COUNT" = 4 or "store_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT" = 3 ) and "store_sales_date_date"."D_DOW" in (6,0) and "store_sales_date_date"."D_YEAR" in (1999,2000,2001) and "store_sales_store_store"."S_CITY" in ('Fairview','Midway')

GROUP BY
    1,
    2,
    3)
SELECT
    "wakeful"."customer_last_name" as "customer_last_name",
    "wakeful"."customer_first_name" as "customer_first_name",
    "wakeful"."customer_address_city" as "customer_address_city",
    "uneven"."bought_city" as "bought_city",
    "uneven"."store_sales_ticket_number" as "store_sales_ticket_number",
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
    "uneven"."store_sales_ticket_number" asc nulls first
LIMIT (100)
```
