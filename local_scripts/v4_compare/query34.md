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
| v4 | 2876 | 35 | 36.50 ms |
| reference | 4843 | 70 | 49.65 ms |
| v4 / ref | 0.59x | 0.50x | 0.74x |

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
    and physical_sales.billing_customer.id is not null
    and physical_sales.ticket_number is not null
select
    --physical_sales.billing_customer.id,
    physical_sales.billing_customer.last_name,
    physical_sales.billing_customer.first_name,
    physical_sales.billing_customer.salutation,
    physical_sales.billing_customer.preferred_cust_flag,
    physical_sales.ticket_number,
    sum(physical_sales.row_counter) by physical_sales.billing_customer.id, physical_sales.ticket_number as cnt,
having
    cnt between 15 and 20

order by
    physical_sales.billing_customer.last_name asc nulls first,
    physical_sales.billing_customer.first_name asc nulls first,
    physical_sales.billing_customer.salutation asc nulls first,
    physical_sales.billing_customer.preferred_cust_flag desc nulls first,
    physical_sales.ticket_number asc nulls first
;
```

## v4 generated SQL

```sql
SELECT
    "physical_sales_billing_customer_customers"."C_LAST_NAME" as "physical_sales_billing_customer_last_name",
    "physical_sales_billing_customer_customers"."C_FIRST_NAME" as "physical_sales_billing_customer_first_name",
    "physical_sales_billing_customer_customers"."C_SALUTATION" as "physical_sales_billing_customer_salutation",
    "physical_sales_billing_customer_customers"."C_PREFERRED_CUST_FLAG" as "physical_sales_billing_customer_preferred_cust_flag",
    "physical_sales_store_sales"."SS_TICKET_NUMBER" as "physical_sales_ticket_number",
    sum(1) as "cnt"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "physical_sales_date_date" on "physical_sales_store_sales"."SS_SOLD_DATE_SK" = "physical_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."store" as "physical_sales_store_store" on "physical_sales_store_sales"."SS_STORE_SK" = "physical_sales_store_store"."S_STORE_SK"
    INNER JOIN "memory"."customer" as "physical_sales_billing_customer_customers" on "physical_sales_store_sales"."SS_CUSTOMER_SK" = "physical_sales_billing_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."household_demographics" as "physical_sales_household_demographic_household_demographics" on "physical_sales_store_sales"."SS_HDEMO_SK" = "physical_sales_household_demographic_household_demographics"."HD_DEMO_SK"
WHERE
    ( "physical_sales_date_date"."D_DOM" BETWEEN 1 AND 3 or "physical_sales_date_date"."D_DOM" BETWEEN 25 AND 28 ) and ( "physical_sales_household_demographic_household_demographics"."HD_BUY_POTENTIAL" = '>10000' or "physical_sales_household_demographic_household_demographics"."HD_BUY_POTENTIAL" = 'Unknown' ) and "physical_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT" > 0 and ( CASE
	WHEN "physical_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT" > 0 THEN ("physical_sales_household_demographic_household_demographics"."HD_DEP_COUNT" * 1.0) / "physical_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT"
	ELSE null
	END ) > 1.2 and "physical_sales_date_date"."D_YEAR" in (1999,2000,2001) and "physical_sales_store_store"."S_COUNTY" = 'Williamson County' and "physical_sales_store_sales"."SS_CUSTOMER_SK" is not null and "physical_sales_store_sales"."SS_TICKET_NUMBER" is not null

GROUP BY
    1,
    2,
    3,
    4,
    5,
    "physical_sales_store_sales"."SS_CUSTOMER_SK"
HAVING
    "cnt" BETWEEN 15 AND 20

ORDER BY 
    "physical_sales_billing_customer_customers"."C_LAST_NAME" asc nulls first,
    "physical_sales_billing_customer_customers"."C_FIRST_NAME" asc nulls first,
    "physical_sales_billing_customer_customers"."C_SALUTATION" asc nulls first,
    "physical_sales_billing_customer_customers"."C_PREFERRED_CUST_FLAG" desc nulls first,
    "physical_sales_store_sales"."SS_TICKET_NUMBER" asc nulls first
```

## Reference SQL (zquery log)

```sql
WITH 
cooperative as (
SELECT
    "physical_sales_billing_customer_customers"."C_FIRST_NAME" as "physical_sales_billing_customer_first_name",
    "physical_sales_billing_customer_customers"."C_LAST_NAME" as "physical_sales_billing_customer_last_name",
    "physical_sales_billing_customer_customers"."C_PREFERRED_CUST_FLAG" as "physical_sales_billing_customer_preferred_cust_flag",
    "physical_sales_billing_customer_customers"."C_SALUTATION" as "physical_sales_billing_customer_salutation",
    "physical_sales_store_sales"."SS_CUSTOMER_SK" as "physical_sales_billing_customer_id",
    "physical_sales_store_sales"."SS_TICKET_NUMBER" as "physical_sales_ticket_number",
    1 as "physical_sales_row_counter"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "physical_sales_date_date" on "physical_sales_store_sales"."SS_SOLD_DATE_SK" = "physical_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."store" as "physical_sales_store_store" on "physical_sales_store_sales"."SS_STORE_SK" = "physical_sales_store_store"."S_STORE_SK"
    INNER JOIN "memory"."customer" as "physical_sales_billing_customer_customers" on "physical_sales_store_sales"."SS_CUSTOMER_SK" = "physical_sales_billing_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."household_demographics" as "physical_sales_household_demographic_household_demographics" on "physical_sales_store_sales"."SS_HDEMO_SK" = "physical_sales_household_demographic_household_demographics"."HD_DEMO_SK"
WHERE
    ( "physical_sales_date_date"."D_DOM" BETWEEN 1 AND 3 or "physical_sales_date_date"."D_DOM" BETWEEN 25 AND 28 ) and ( "physical_sales_household_demographic_household_demographics"."HD_BUY_POTENTIAL" = '>10000' or "physical_sales_household_demographic_household_demographics"."HD_BUY_POTENTIAL" = 'Unknown' ) and "physical_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT" > 0 and ( CASE
	WHEN "physical_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT" > 0 THEN ("physical_sales_household_demographic_household_demographics"."HD_DEP_COUNT" * 1.0) / "physical_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT"
	ELSE null
	END ) > 1.2 and "physical_sales_date_date"."D_YEAR" in (1999,2000,2001) and "physical_sales_store_store"."S_COUNTY" = 'Williamson County' and "physical_sales_store_sales"."SS_CUSTOMER_SK" is not null and "physical_sales_store_sales"."SS_TICKET_NUMBER" is not null
),
uneven as (
SELECT
    "cooperative"."physical_sales_billing_customer_first_name" as "physical_sales_billing_customer_first_name",
    "cooperative"."physical_sales_billing_customer_id" as "physical_sales_billing_customer_id",
    "cooperative"."physical_sales_billing_customer_last_name" as "physical_sales_billing_customer_last_name",
    "cooperative"."physical_sales_billing_customer_preferred_cust_flag" as "physical_sales_billing_customer_preferred_cust_flag",
    "cooperative"."physical_sales_billing_customer_salutation" as "physical_sales_billing_customer_salutation"
FROM
    "cooperative"
GROUP BY
    1,
    2,
    3,
    4,
    5),
questionable as (
SELECT
    "cooperative"."physical_sales_billing_customer_id" as "physical_sales_billing_customer_id",
    "cooperative"."physical_sales_ticket_number" as "physical_sales_ticket_number",
    sum("cooperative"."physical_sales_row_counter") as "cnt"
FROM
    "cooperative"
GROUP BY
    1,
    2,
    "cooperative"."physical_sales_billing_customer_first_name",
    "cooperative"."physical_sales_billing_customer_last_name",
    "cooperative"."physical_sales_billing_customer_preferred_cust_flag",
    "cooperative"."physical_sales_billing_customer_salutation"
HAVING
    "cnt" BETWEEN 15 AND 20
)
SELECT
    "uneven"."physical_sales_billing_customer_last_name" as "physical_sales_billing_customer_last_name",
    "uneven"."physical_sales_billing_customer_first_name" as "physical_sales_billing_customer_first_name",
    "uneven"."physical_sales_billing_customer_salutation" as "physical_sales_billing_customer_salutation",
    "uneven"."physical_sales_billing_customer_preferred_cust_flag" as "physical_sales_billing_customer_preferred_cust_flag",
    "questionable"."physical_sales_ticket_number" as "physical_sales_ticket_number",
    "questionable"."cnt" as "cnt"
FROM
    "uneven"
    RIGHT OUTER JOIN "questionable" on "uneven"."physical_sales_billing_customer_id" = "questionable"."physical_sales_billing_customer_id"
ORDER BY 
    "uneven"."physical_sales_billing_customer_last_name" asc nulls first,
    "uneven"."physical_sales_billing_customer_first_name" asc nulls first,
    "uneven"."physical_sales_billing_customer_salutation" asc nulls first,
    "uneven"."physical_sales_billing_customer_preferred_cust_flag" desc nulls first,
    "questionable"."physical_sales_ticket_number" asc nulls first
```
