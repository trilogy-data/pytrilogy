# Query 34

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (455 rows) |
| reference execution | OK (455 rows) |
| results identical | YES |

## Result comparison

v4 rows: 455 (455 distinct)
ref rows: 455 (455 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 2641 | 35 | 47.53 ms |
| reference | 2808 | 39 | 45.29 ms |
| v4 / ref | 0.94x | 0.90x | 1.05x |

## Preql

```
import store_sales as store_sales;

where
    (
        store_sales.date.day_of_month between 1 and 3
        or store_sales.date.day_of_month between 25 and 28
    )
    and (
        store_sales.household_demographic.buy_potential = '>10000'
        or store_sales.household_demographic.buy_potential = 'Unknown'
    )
    and store_sales.household_demographic.vehicle_count > 0
    and (case
            when store_sales.household_demographic.vehicle_count > 0 then (store_sales.household_demographic.dependent_count * 1.0) / store_sales.household_demographic.vehicle_count
            else null
        end) > 1.2
    and store_sales.date.year in (1999, 2000, 2001)
    and store_sales.store.county = 'Williamson County'
    and store_sales.customer.id is not null
    and store_sales.ticket_number is not null
select
    --store_sales.customer.id,
    store_sales.customer.last_name,
    store_sales.customer.first_name,
    store_sales.customer.salutation,
    store_sales.customer.preferred_cust_flag,
    store_sales.ticket_number,
    sum(store_sales.row_counter) by store_sales.customer.id, store_sales.ticket_number as cnt,
having
    cnt between 15 and 20

order by
    store_sales.customer.last_name asc nulls first,
    store_sales.customer.first_name asc nulls first,
    store_sales.customer.salutation asc nulls first,
    store_sales.customer.preferred_cust_flag desc nulls first,
    store_sales.ticket_number asc nulls first
;
```

## v4 generated SQL

```sql
SELECT
    "store_sales_customer_customers"."C_LAST_NAME" as "store_sales_customer_last_name",
    "store_sales_customer_customers"."C_FIRST_NAME" as "store_sales_customer_first_name",
    "store_sales_customer_customers"."C_SALUTATION" as "store_sales_customer_salutation",
    "store_sales_customer_customers"."C_PREFERRED_CUST_FLAG" as "store_sales_customer_preferred_cust_flag",
    "store_sales_store_sales"."SS_TICKET_NUMBER" as "store_sales_ticket_number",
    sum(1) as "cnt"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."store" as "store_sales_store_store" on "store_sales_store_sales"."SS_STORE_SK" = "store_sales_store_store"."S_STORE_SK"
    INNER JOIN "memory"."customer" as "store_sales_customer_customers" on "store_sales_store_sales"."SS_CUSTOMER_SK" = "store_sales_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."household_demographics" as "store_sales_household_demographic_household_demographics" on "store_sales_store_sales"."SS_HDEMO_SK" = "store_sales_household_demographic_household_demographics"."HD_DEMO_SK"
WHERE
    ( "store_sales_date_date"."D_DOM" BETWEEN 1 AND 3 or "store_sales_date_date"."D_DOM" BETWEEN 25 AND 28 ) and ( "store_sales_household_demographic_household_demographics"."HD_BUY_POTENTIAL" = '>10000' or "store_sales_household_demographic_household_demographics"."HD_BUY_POTENTIAL" = 'Unknown' ) and "store_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT" > 0 and ( CASE
	WHEN "store_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT" > 0 THEN ("store_sales_household_demographic_household_demographics"."HD_DEP_COUNT" * 1.0) / "store_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT"
	ELSE null
	END ) > 1.2 and "store_sales_date_date"."D_YEAR" in (1999,2000,2001) and "store_sales_store_store"."S_COUNTY" = 'Williamson County' and "store_sales_store_sales"."SS_CUSTOMER_SK" is not null and "store_sales_store_sales"."SS_TICKET_NUMBER" is not null

GROUP BY
    1,
    2,
    3,
    4,
    5,
    "store_sales_store_sales"."SS_CUSTOMER_SK"
HAVING
    "cnt" BETWEEN 15 AND 20

ORDER BY 
    "store_sales_customer_customers"."C_LAST_NAME" asc nulls first,
    "store_sales_customer_customers"."C_FIRST_NAME" asc nulls first,
    "store_sales_customer_customers"."C_SALUTATION" asc nulls first,
    "store_sales_customer_customers"."C_PREFERRED_CUST_FLAG" desc nulls first,
    "store_sales_store_sales"."SS_TICKET_NUMBER" asc nulls first
```

## Reference SQL (zquery log)

```sql
WITH 
cooperative as (
SELECT
    "store_sales_store_sales"."SS_CUSTOMER_SK" as "store_sales_customer_id",
    "store_sales_store_sales"."SS_TICKET_NUMBER" as "store_sales_ticket_number",
    sum(1) as "cnt"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."store" as "store_sales_store_store" on "store_sales_store_sales"."SS_STORE_SK" = "store_sales_store_store"."S_STORE_SK"
    INNER JOIN "memory"."household_demographics" as "store_sales_household_demographic_household_demographics" on "store_sales_store_sales"."SS_HDEMO_SK" = "store_sales_household_demographic_household_demographics"."HD_DEMO_SK"
WHERE
    ( "store_sales_date_date"."D_DOM" BETWEEN 1 AND 3 or "store_sales_date_date"."D_DOM" BETWEEN 25 AND 28 ) and ( "store_sales_household_demographic_household_demographics"."HD_BUY_POTENTIAL" = '>10000' or "store_sales_household_demographic_household_demographics"."HD_BUY_POTENTIAL" = 'Unknown' ) and "store_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT" > 0 and ( CASE
	WHEN "store_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT" > 0 THEN ("store_sales_household_demographic_household_demographics"."HD_DEP_COUNT" * 1.0) / "store_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT"
	ELSE null
	END ) > 1.2 and "store_sales_date_date"."D_YEAR" in (1999,2000,2001) and "store_sales_store_store"."S_COUNTY" = 'Williamson County' and "store_sales_store_sales"."SS_CUSTOMER_SK" is not null and "store_sales_store_sales"."SS_TICKET_NUMBER" is not null

GROUP BY
    1,
    2
HAVING
    "cnt" BETWEEN 15 AND 20
)
SELECT
    "store_sales_customer_customers"."C_LAST_NAME" as "store_sales_customer_last_name",
    "store_sales_customer_customers"."C_FIRST_NAME" as "store_sales_customer_first_name",
    "store_sales_customer_customers"."C_SALUTATION" as "store_sales_customer_salutation",
    "store_sales_customer_customers"."C_PREFERRED_CUST_FLAG" as "store_sales_customer_preferred_cust_flag",
    "cooperative"."store_sales_ticket_number" as "store_sales_ticket_number",
    "cooperative"."cnt" as "cnt"
FROM
    "cooperative"
    INNER JOIN "memory"."customer" as "store_sales_customer_customers" on "cooperative"."store_sales_customer_id" = "store_sales_customer_customers"."C_CUSTOMER_SK"
ORDER BY 
    "store_sales_customer_customers"."C_LAST_NAME" asc nulls first,
    "store_sales_customer_customers"."C_FIRST_NAME" asc nulls first,
    "store_sales_customer_customers"."C_SALUTATION" asc nulls first,
    "store_sales_customer_customers"."C_PREFERRED_CUST_FLAG" desc nulls first,
    "cooperative"."store_sales_ticket_number" asc nulls first
```
