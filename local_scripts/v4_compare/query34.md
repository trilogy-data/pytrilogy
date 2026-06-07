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
| v4 | 4411 | 73 | 126.03 ms |
| reference | 2876 | 39 | 38.10 ms |
| v4 / ref | 1.53x | 1.87x | 3.31x |

## Preql

```
import physical_sales as physical_sales;

where
    (
        physical_sales.date.day_of_month between 1 and 3
        or physical_sales.date.day_of_month between 25 and 28
    )
    and (
        physical_sales.household_demographic.buy_potential = '>10000'
        or physical_sales.household_demographic.buy_potential = 'Unknown'
    )
    and physical_sales.household_demographic.vehicle_count > 0
    and (case
            when physical_sales.household_demographic.vehicle_count > 0 then (physical_sales.household_demographic.dependent_count * 1.0) / physical_sales.household_demographic.vehicle_count
            else null
        end) > 1.2
    and physical_sales.date.year in (1999, 2000, 2001)
    and physical_sales.store.county = 'Williamson County'
    and physical_sales.customer.id is not null
    and physical_sales.ticket_number is not null
select
    --physical_sales.customer.id,
    physical_sales.customer.last_name,
    physical_sales.customer.first_name,
    physical_sales.customer.salutation,
    physical_sales.customer.preferred_cust_flag,
    physical_sales.ticket_number,
    sum(physical_sales.row_counter) by physical_sales.customer.id, physical_sales.ticket_number as cnt,
having
    cnt between 15 and 20

order by
    physical_sales.customer.last_name asc nulls first,
    physical_sales.customer.first_name asc nulls first,
    physical_sales.customer.salutation asc nulls first,
    physical_sales.customer.preferred_cust_flag desc nulls first,
    physical_sales.ticket_number asc nulls first
;
```

## v4 generated SQL

```sql
WITH 
wakeful as (
SELECT
    "physical_sales_customer_customers"."C_FIRST_NAME" as "physical_sales_customer_first_name",
    "physical_sales_customer_customers"."C_LAST_NAME" as "physical_sales_customer_last_name",
    "physical_sales_customer_customers"."C_PREFERRED_CUST_FLAG" as "physical_sales_customer_preferred_cust_flag",
    "physical_sales_customer_customers"."C_SALUTATION" as "physical_sales_customer_salutation",
    "physical_sales_store_sales"."SS_CUSTOMER_SK" as "physical_sales_customer_id",
    "physical_sales_store_sales"."SS_TICKET_NUMBER" as "physical_sales_ticket_number"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    LEFT OUTER JOIN "memory"."customer" as "physical_sales_customer_customers" on "physical_sales_store_sales"."SS_CUSTOMER_SK" = "physical_sales_customer_customers"."C_CUSTOMER_SK"),
uneven as (
SELECT
    "physical_sales_store_sales"."SS_CUSTOMER_SK" as "physical_sales_customer_id",
    "physical_sales_store_sales"."SS_TICKET_NUMBER" as "physical_sales_ticket_number",
    sum(1) as "cnt"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "physical_sales_date_date" on "physical_sales_store_sales"."SS_SOLD_DATE_SK" = "physical_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."store" as "physical_sales_store_store" on "physical_sales_store_sales"."SS_STORE_SK" = "physical_sales_store_store"."S_STORE_SK"
    INNER JOIN "memory"."household_demographics" as "physical_sales_household_demographic_household_demographics" on "physical_sales_store_sales"."SS_HDEMO_SK" = "physical_sales_household_demographic_household_demographics"."HD_DEMO_SK"
WHERE
    ( "physical_sales_date_date"."D_DOM" BETWEEN 1 AND 3 or "physical_sales_date_date"."D_DOM" BETWEEN 25 AND 28 ) and ( "physical_sales_household_demographic_household_demographics"."HD_BUY_POTENTIAL" = '>10000' or "physical_sales_household_demographic_household_demographics"."HD_BUY_POTENTIAL" = 'Unknown' ) and "physical_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT" > 0 and ( CASE
	WHEN "physical_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT" > 0 THEN ("physical_sales_household_demographic_household_demographics"."HD_DEP_COUNT" * 1.0) / "physical_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT"
	ELSE null
	END ) > 1.2 and "physical_sales_date_date"."D_YEAR" in (1999,2000,2001) and "physical_sales_store_store"."S_COUNTY" = 'Williamson County' and "physical_sales_store_sales"."SS_CUSTOMER_SK" is not null

GROUP BY
    1,
    2
HAVING
    "cnt" BETWEEN 15 AND 20
),
thoughtful as (
SELECT
    "wakeful"."physical_sales_ticket_number" as "physical_sales_ticket_number"
FROM
    "wakeful"
GROUP BY
    1),
cheerful as (
SELECT
    "wakeful"."physical_sales_customer_first_name" as "physical_sales_customer_first_name",
    "wakeful"."physical_sales_customer_id" as "physical_sales_customer_id",
    "wakeful"."physical_sales_customer_last_name" as "physical_sales_customer_last_name",
    "wakeful"."physical_sales_customer_preferred_cust_flag" as "physical_sales_customer_preferred_cust_flag",
    "wakeful"."physical_sales_customer_salutation" as "physical_sales_customer_salutation"
FROM
    "wakeful"
GROUP BY
    1,
    2,
    3,
    4,
    5)
SELECT
    "cheerful"."physical_sales_customer_last_name" as "physical_sales_customer_last_name",
    "cheerful"."physical_sales_customer_first_name" as "physical_sales_customer_first_name",
    "cheerful"."physical_sales_customer_salutation" as "physical_sales_customer_salutation",
    "cheerful"."physical_sales_customer_preferred_cust_flag" as "physical_sales_customer_preferred_cust_flag",
    "thoughtful"."physical_sales_ticket_number" as "physical_sales_ticket_number",
    "uneven"."cnt" as "cnt"
FROM
    "cheerful"
    INNER JOIN "uneven" on "cheerful"."physical_sales_customer_id" = "uneven"."physical_sales_customer_id"
    LEFT OUTER JOIN "thoughtful" on "uneven"."physical_sales_ticket_number" = "thoughtful"."physical_sales_ticket_number"
ORDER BY 
    "cheerful"."physical_sales_customer_last_name" asc nulls first,
    "cheerful"."physical_sales_customer_first_name" asc nulls first,
    "cheerful"."physical_sales_customer_salutation" asc nulls first,
    "cheerful"."physical_sales_customer_preferred_cust_flag" desc nulls first,
    "thoughtful"."physical_sales_ticket_number" asc nulls first
```

## Reference SQL (zquery log)

```sql
WITH 
cooperative as (
SELECT
    "physical_sales_store_sales"."SS_CUSTOMER_SK" as "physical_sales_customer_id",
    "physical_sales_store_sales"."SS_TICKET_NUMBER" as "physical_sales_ticket_number",
    sum(1) as "cnt"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "physical_sales_date_date" on "physical_sales_store_sales"."SS_SOLD_DATE_SK" = "physical_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."store" as "physical_sales_store_store" on "physical_sales_store_sales"."SS_STORE_SK" = "physical_sales_store_store"."S_STORE_SK"
    INNER JOIN "memory"."household_demographics" as "physical_sales_household_demographic_household_demographics" on "physical_sales_store_sales"."SS_HDEMO_SK" = "physical_sales_household_demographic_household_demographics"."HD_DEMO_SK"
WHERE
    ( "physical_sales_date_date"."D_DOM" BETWEEN 1 AND 3 or "physical_sales_date_date"."D_DOM" BETWEEN 25 AND 28 ) and ( "physical_sales_household_demographic_household_demographics"."HD_BUY_POTENTIAL" = '>10000' or "physical_sales_household_demographic_household_demographics"."HD_BUY_POTENTIAL" = 'Unknown' ) and "physical_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT" > 0 and ( CASE
	WHEN "physical_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT" > 0 THEN ("physical_sales_household_demographic_household_demographics"."HD_DEP_COUNT" * 1.0) / "physical_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT"
	ELSE null
	END ) > 1.2 and "physical_sales_date_date"."D_YEAR" in (1999,2000,2001) and "physical_sales_store_store"."S_COUNTY" = 'Williamson County' and "physical_sales_store_sales"."SS_CUSTOMER_SK" is not null

GROUP BY
    1,
    2
HAVING
    "cnt" BETWEEN 15 AND 20
)
SELECT
    "physical_sales_customer_customers"."C_LAST_NAME" as "physical_sales_customer_last_name",
    "physical_sales_customer_customers"."C_FIRST_NAME" as "physical_sales_customer_first_name",
    "physical_sales_customer_customers"."C_SALUTATION" as "physical_sales_customer_salutation",
    "physical_sales_customer_customers"."C_PREFERRED_CUST_FLAG" as "physical_sales_customer_preferred_cust_flag",
    "cooperative"."physical_sales_ticket_number" as "physical_sales_ticket_number",
    "cooperative"."cnt" as "cnt"
FROM
    "cooperative"
    INNER JOIN "memory"."customer" as "physical_sales_customer_customers" on "cooperative"."physical_sales_customer_id" = "physical_sales_customer_customers"."C_CUSTOMER_SK"
ORDER BY 
    "physical_sales_customer_customers"."C_LAST_NAME" asc nulls first,
    "physical_sales_customer_customers"."C_FIRST_NAME" asc nulls first,
    "physical_sales_customer_customers"."C_SALUTATION" asc nulls first,
    "physical_sales_customer_customers"."C_PREFERRED_CUST_FLAG" desc nulls first,
    "cooperative"."physical_sales_ticket_number" asc nulls first
```
