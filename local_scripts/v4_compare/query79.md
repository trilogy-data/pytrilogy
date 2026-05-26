# Query 79

**Status:** `mismatch`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (100 rows) |
| reference execution | OK (100 rows) |
| results identical | NO |

## Result comparison

v4 rows: 100 (9 distinct)
ref rows: 100 (100 distinct)
only in v4 (showing up to 5 of 9):
  8x  (Decimal('3161.48'), 'Fairview', Decimal('-15790.23'), None, None, 213610)
  11x  (Decimal('82.87'), 'Fairview', Decimal('-14576.82'), None, None, 167006)
  11x  (Decimal('6766.29'), 'Fairview', Decimal('-13263.36'), None, None, 191089)
  13x  (Decimal('4396.37'), 'Fairview', Decimal('-13020.45'), None, None, 158139)
  12x  (Decimal('219.72'), 'Fairview', Decimal('-11749.39'), None, None, 7153)
only in ref (showing up to 5 of 91):
  1x  (Decimal('600.01'), 'Fairview', Decimal('-6430.56'), None, None, 129036)
  1x  (Decimal('1068.86'), 'Fairview', Decimal('-6405.03'), None, None, 146799)
  1x  (Decimal('2615.94'), 'Fairview', Decimal('-5642.11'), None, None, 228288)
  1x  (Decimal('704.60'), 'Fairview', Decimal('-5586.87'), None, None, 34621)
  1x  (Decimal('0.00'), 'Fairview', Decimal('-4215.78'), None, None, 142379)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 4124 | 63 | 78.54 ms |
| reference | 2326 | 38 | 45.79 ms |
| v4 / ref | 1.77x | 1.66x | 1.71x |

## Preql

```
import store_sales as store_sales;

where
    (
        store_sales.household_demographic.dependent_count = 6
        or store_sales.household_demographic.vehicle_count > 2
    )
    and store_sales.date.day_of_week = 1
    and store_sales.date.year in (1999, 2000, 2001)
    and store_sales.store.employees between 200 and 295
    and store_sales.customer.id is not null
select
    --store_sales.sale_address.id,
    store_sales.customer.last_name,
    store_sales.customer.first_name,
    substring(store_sales.store.city, 1, 30) as city_short,
    store_sales.ticket_number,
    sum(store_sales.coupon_amt)
            by store_sales.ticket_number, store_sales.customer.id, store_sales.sale_address.id, store_sales.store.city as amt,
    sum(store_sales.net_profit)
            by store_sales.ticket_number, store_sales.customer.id, store_sales.sale_address.id, store_sales.store.city as profit,
order by
    store_sales.customer.last_name asc nulls first,
    store_sales.customer.first_name asc nulls first,
    city_short asc nulls first,
    profit asc nulls first,
    store_sales.ticket_number asc
limit 100
;
```

## v4 generated SQL

```sql
WITH 
cooperative as (
SELECT
    "store_sales_customer_customers"."C_FIRST_NAME" as "store_sales_customer_first_name",
    "store_sales_customer_customers"."C_LAST_NAME" as "store_sales_customer_last_name",
    "store_sales_store_sales"."SS_ADDR_SK" as "store_sales_sale_address_id",
    "store_sales_store_sales"."SS_COUPON_AMT" as "store_sales_coupon_amt",
    "store_sales_store_sales"."SS_CUSTOMER_SK" as "store_sales_customer_id",
    "store_sales_store_sales"."SS_NET_PROFIT" as "store_sales_net_profit",
    "store_sales_store_sales"."SS_TICKET_NUMBER" as "store_sales_ticket_number",
    "store_sales_store_store"."S_CITY" as "store_sales_store_city"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."store" as "store_sales_store_store" on "store_sales_store_sales"."SS_STORE_SK" = "store_sales_store_store"."S_STORE_SK"
    INNER JOIN "memory"."customer" as "store_sales_customer_customers" on "store_sales_store_sales"."SS_CUSTOMER_SK" = "store_sales_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."household_demographics" as "store_sales_household_demographic_household_demographics" on "store_sales_store_sales"."SS_HDEMO_SK" = "store_sales_household_demographic_household_demographics"."HD_DEMO_SK"
WHERE
    ( "store_sales_household_demographic_household_demographics"."HD_DEP_COUNT" = 6 or "store_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT" > 2 ) and "store_sales_date_date"."D_DOW" = 1 and "store_sales_date_date"."D_YEAR" in (1999,2000,2001) and "store_sales_store_store"."S_NUMBER_EMPLOYEES" BETWEEN 200 AND 295 and "store_sales_store_sales"."SS_CUSTOMER_SK" is not null
),
abundant as (
SELECT
    "cooperative"."store_sales_customer_id" as "store_sales_customer_id",
    "cooperative"."store_sales_sale_address_id" as "store_sales_sale_address_id",
    "cooperative"."store_sales_store_city" as "store_sales_store_city",
    "cooperative"."store_sales_ticket_number" as "store_sales_ticket_number",
    sum("cooperative"."store_sales_coupon_amt") as "amt",
    sum("cooperative"."store_sales_net_profit") as "profit"
FROM
    "cooperative"
GROUP BY
    1,
    2,
    3,
    4),
questionable as (
SELECT
    "cooperative"."store_sales_customer_first_name" as "store_sales_customer_first_name",
    "cooperative"."store_sales_customer_id" as "store_sales_customer_id",
    "cooperative"."store_sales_customer_last_name" as "store_sales_customer_last_name",
    "cooperative"."store_sales_sale_address_id" as "store_sales_sale_address_id",
    "cooperative"."store_sales_store_city" as "store_sales_store_city",
    "cooperative"."store_sales_ticket_number" as "store_sales_ticket_number",
    SUBSTRING("cooperative"."store_sales_store_city",1,30) as "city_short"
FROM
    "cooperative")
SELECT
    "questionable"."store_sales_customer_last_name" as "store_sales_customer_last_name",
    "questionable"."store_sales_customer_first_name" as "store_sales_customer_first_name",
    "questionable"."city_short" as "city_short",
    coalesce("abundant"."store_sales_ticket_number","questionable"."store_sales_ticket_number") as "store_sales_ticket_number",
    "abundant"."amt" as "amt",
    "abundant"."profit" as "profit"
FROM
    "abundant"
    FULL JOIN "questionable" on "abundant"."store_sales_customer_id" = "questionable"."store_sales_customer_id" AND "abundant"."store_sales_sale_address_id" is not distinct from "questionable"."store_sales_sale_address_id" AND "abundant"."store_sales_store_city" = "questionable"."store_sales_store_city" AND "abundant"."store_sales_ticket_number" = "questionable"."store_sales_ticket_number"
ORDER BY 
    "questionable"."store_sales_customer_last_name" asc nulls first,
    "questionable"."store_sales_customer_first_name" asc nulls first,
    "questionable"."city_short" asc nulls first,
    "abundant"."profit" asc nulls first,
    coalesce("abundant"."store_sales_ticket_number","questionable"."store_sales_ticket_number") asc
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
cooperative as (
SELECT
    "store_sales_store_sales"."SS_CUSTOMER_SK" as "store_sales_customer_id",
    "store_sales_store_sales"."SS_TICKET_NUMBER" as "store_sales_ticket_number",
    "store_sales_store_store"."S_CITY" as "store_sales_store_city",
    sum("store_sales_store_sales"."SS_COUPON_AMT") as "amt",
    sum("store_sales_store_sales"."SS_NET_PROFIT") as "profit"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."store" as "store_sales_store_store" on "store_sales_store_sales"."SS_STORE_SK" = "store_sales_store_store"."S_STORE_SK"
    INNER JOIN "memory"."household_demographics" as "store_sales_household_demographic_household_demographics" on "store_sales_store_sales"."SS_HDEMO_SK" = "store_sales_household_demographic_household_demographics"."HD_DEMO_SK"
WHERE
    ( "store_sales_household_demographic_household_demographics"."HD_DEP_COUNT" = 6 or "store_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT" > 2 ) and "store_sales_date_date"."D_DOW" = 1 and "store_sales_date_date"."D_YEAR" in (1999,2000,2001) and "store_sales_store_store"."S_NUMBER_EMPLOYEES" BETWEEN 200 AND 295 and "store_sales_store_sales"."SS_CUSTOMER_SK" is not null

GROUP BY
    1,
    2,
    3,
    "store_sales_store_sales"."SS_ADDR_SK")
SELECT
    "store_sales_customer_customers"."C_LAST_NAME" as "store_sales_customer_last_name",
    "store_sales_customer_customers"."C_FIRST_NAME" as "store_sales_customer_first_name",
    SUBSTRING("cooperative"."store_sales_store_city",1,30) as "city_short",
    "cooperative"."store_sales_ticket_number" as "store_sales_ticket_number",
    "cooperative"."amt" as "amt",
    "cooperative"."profit" as "profit"
FROM
    "cooperative"
    INNER JOIN "memory"."customer" as "store_sales_customer_customers" on "cooperative"."store_sales_customer_id" = "store_sales_customer_customers"."C_CUSTOMER_SK"
ORDER BY 
    "store_sales_customer_customers"."C_LAST_NAME" asc nulls first,
    "store_sales_customer_customers"."C_FIRST_NAME" asc nulls first,
    "city_short" asc nulls first,
    "cooperative"."profit" asc nulls first,
    "cooperative"."store_sales_ticket_number" asc
LIMIT (100)
```
