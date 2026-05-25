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

## SQL size

| Source | Chars | Lines |
| --- | --- | --- |
| v4 | 7219 | 124 |
| reference | 2808 | 39 |
| v4 / ref | 2.57x | 3.18x |

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
WITH 
thoughtful as (
SELECT
    "store_sales_store_sales"."SS_CUSTOMER_SK" as "store_sales_customer_id",
    "store_sales_store_sales"."SS_HDEMO_SK" as "store_sales_household_demographic_id",
    "store_sales_store_sales"."SS_ITEM_SK" as "store_sales_item_id",
    "store_sales_store_sales"."SS_SOLD_DATE_SK" as "store_sales_date_id",
    "store_sales_store_sales"."SS_STORE_SK" as "store_sales_store_id",
    "store_sales_store_sales"."SS_TICKET_NUMBER" as "store_sales_ticket_number",
    1 as "store_sales_row_counter"
FROM
    "memory"."store_sales" as "store_sales_store_sales"),
cheerful as (
SELECT
    "store_sales_store_store"."S_COUNTY" as "store_sales_store_county",
    "store_sales_store_store"."S_STORE_SK" as "store_sales_store_id"
FROM
    "memory"."store" as "store_sales_store_store"),
wakeful as (
SELECT
    "store_sales_household_demographic_household_demographics"."HD_BUY_POTENTIAL" as "store_sales_household_demographic_buy_potential",
    "store_sales_household_demographic_household_demographics"."HD_DEMO_SK" as "store_sales_household_demographic_id",
    "store_sales_household_demographic_household_demographics"."HD_DEP_COUNT" as "store_sales_household_demographic_dependent_count",
    "store_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT" as "store_sales_household_demographic_vehicle_count"
FROM
    "memory"."household_demographics" as "store_sales_household_demographic_household_demographics"),
highfalutin as (
SELECT
    "store_sales_date_date"."D_DATE_SK" as "store_sales_date_id",
    "store_sales_date_date"."D_DOM" as "store_sales_date_day_of_month",
    "store_sales_date_date"."D_YEAR" as "store_sales_date_year"
FROM
    "memory"."date_dim" as "store_sales_date_date"),
quizzical as (
SELECT
    "store_sales_customer_customers"."C_CUSTOMER_SK" as "store_sales_customer_id",
    "store_sales_customer_customers"."C_FIRST_NAME" as "store_sales_customer_first_name",
    "store_sales_customer_customers"."C_LAST_NAME" as "store_sales_customer_last_name",
    "store_sales_customer_customers"."C_PREFERRED_CUST_FLAG" as "store_sales_customer_preferred_cust_flag",
    "store_sales_customer_customers"."C_SALUTATION" as "store_sales_customer_salutation"
FROM
    "memory"."customer" as "store_sales_customer_customers"),
cooperative as (
SELECT
    "cheerful"."store_sales_store_county" as "store_sales_store_county",
    "highfalutin"."store_sales_date_day_of_month" as "store_sales_date_day_of_month",
    "highfalutin"."store_sales_date_year" as "store_sales_date_year",
    "quizzical"."store_sales_customer_first_name" as "store_sales_customer_first_name",
    "quizzical"."store_sales_customer_last_name" as "store_sales_customer_last_name",
    "quizzical"."store_sales_customer_preferred_cust_flag" as "store_sales_customer_preferred_cust_flag",
    "quizzical"."store_sales_customer_salutation" as "store_sales_customer_salutation",
    "thoughtful"."store_sales_customer_id" as "store_sales_customer_id",
    "thoughtful"."store_sales_row_counter" as "store_sales_row_counter",
    "thoughtful"."store_sales_ticket_number" as "store_sales_ticket_number",
    "wakeful"."store_sales_household_demographic_buy_potential" as "store_sales_household_demographic_buy_potential",
    "wakeful"."store_sales_household_demographic_dependent_count" as "store_sales_household_demographic_dependent_count",
    "wakeful"."store_sales_household_demographic_vehicle_count" as "store_sales_household_demographic_vehicle_count"
FROM
    "thoughtful"
    LEFT OUTER JOIN "highfalutin" on "thoughtful"."store_sales_date_id" = "highfalutin"."store_sales_date_id"
    LEFT OUTER JOIN "cheerful" on "thoughtful"."store_sales_store_id" = "cheerful"."store_sales_store_id"
    LEFT OUTER JOIN "quizzical" on "thoughtful"."store_sales_customer_id" = "quizzical"."store_sales_customer_id"
    LEFT OUTER JOIN "wakeful" on "thoughtful"."store_sales_household_demographic_id" = "wakeful"."store_sales_household_demographic_id"
WHERE
    ( "highfalutin"."store_sales_date_day_of_month" BETWEEN 1 AND 3 or "highfalutin"."store_sales_date_day_of_month" BETWEEN 25 AND 28 ) and ( "wakeful"."store_sales_household_demographic_buy_potential" = '>10000' or "wakeful"."store_sales_household_demographic_buy_potential" = 'Unknown' ) and "wakeful"."store_sales_household_demographic_vehicle_count" > 0 and ( CASE
	WHEN "wakeful"."store_sales_household_demographic_vehicle_count" > 0 THEN ("wakeful"."store_sales_household_demographic_dependent_count" * 1.0) / "wakeful"."store_sales_household_demographic_vehicle_count"
	ELSE null
	END ) > 1.2 and "highfalutin"."store_sales_date_year" in (1999,2000,2001) and "cheerful"."store_sales_store_county" = 'Williamson County' and "thoughtful"."store_sales_customer_id" is not null and "thoughtful"."store_sales_ticket_number" is not null
),
abundant as (
SELECT
    "cooperative"."store_sales_customer_id" as "store_sales_customer_id",
    "cooperative"."store_sales_ticket_number" as "store_sales_ticket_number",
    sum("cooperative"."store_sales_row_counter") as "cnt"
FROM
    "cooperative"
GROUP BY
    1,
    2),
questionable as (
SELECT
    "cooperative"."store_sales_customer_first_name" as "store_sales_customer_first_name",
    "cooperative"."store_sales_customer_id" as "store_sales_customer_id",
    "cooperative"."store_sales_customer_last_name" as "store_sales_customer_last_name",
    "cooperative"."store_sales_customer_preferred_cust_flag" as "store_sales_customer_preferred_cust_flag",
    "cooperative"."store_sales_customer_salutation" as "store_sales_customer_salutation"
FROM
    "cooperative"
GROUP BY
    1,
    2,
    3,
    4,
    5),
uneven as (
SELECT
    "abundant"."cnt" as "cnt",
    "abundant"."store_sales_ticket_number" as "store_sales_ticket_number",
    "questionable"."store_sales_customer_first_name" as "store_sales_customer_first_name",
    "questionable"."store_sales_customer_id" as "store_sales_customer_id",
    "questionable"."store_sales_customer_last_name" as "store_sales_customer_last_name",
    "questionable"."store_sales_customer_preferred_cust_flag" as "store_sales_customer_preferred_cust_flag",
    "questionable"."store_sales_customer_salutation" as "store_sales_customer_salutation"
FROM
    "questionable"
    LEFT OUTER JOIN "abundant" on "questionable"."store_sales_customer_id" = "abundant"."store_sales_customer_id")
SELECT
    "uneven"."store_sales_customer_last_name" as "store_sales_customer_last_name",
    "uneven"."store_sales_customer_first_name" as "store_sales_customer_first_name",
    "uneven"."store_sales_customer_salutation" as "store_sales_customer_salutation",
    "uneven"."store_sales_customer_preferred_cust_flag" as "store_sales_customer_preferred_cust_flag",
    "uneven"."store_sales_ticket_number" as "store_sales_ticket_number",
    "uneven"."cnt" as "cnt"
FROM
    "uneven"
WHERE
    "uneven"."cnt" BETWEEN 15 AND 20

ORDER BY 
    "uneven"."store_sales_customer_last_name" asc nulls first,
    "uneven"."store_sales_customer_first_name" asc nulls first,
    "uneven"."store_sales_customer_salutation" asc nulls first,
    "uneven"."store_sales_customer_preferred_cust_flag" desc nulls first,
    "uneven"."store_sales_ticket_number" asc nulls first
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
