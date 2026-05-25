# Query 73

**Status:** `exec_fail`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | FAILED |
| reference execution | OK (1 rows) |

## Result comparison

_at least one side did not produce rows._

## SQL size

| Source | Chars | Lines |
| --- | --- | --- |
| v4 | 7223 | 127 |
| reference | 2700 | 38 |
| v4 / ref | 2.68x | 3.34x |

## Preql

```
import store_sales as store_sales;

# Force the count to a strict per-ticket grain via the `by` clause; the rest of
# the ticket-level customer enrichment now derives from the same filtered rowset
# rather than going through an unfiltered cooperative dedup of all store_sales.
auto ticket_cnt <- count(
    store_sales.item.id
        ? store_sales.date.day_of_month >= 1
and store_sales.date.day_of_month <= 2
and (
    store_sales.household_demographic.buy_potential = '>10000'
    or store_sales.household_demographic.buy_potential = 'Unknown'
)
and store_sales.household_demographic.vehicle_count > 0
and (case
        when store_sales.household_demographic.vehicle_count > 0 then (store_sales.household_demographic.dependent_count * 1.0) / store_sales.household_demographic.vehicle_count
        else null
    end) > 1
and store_sales.date.year in (1999, 2000, 2001)
and store_sales.store.county in ('Orange County', 'Bronx County', 'Franklin Parish', 'Williamson County')
)
    by store_sales.ticket_number, store_sales.customer.id;

where
    store_sales.customer.id is not null
select
    --store_sales.customer.id,
    store_sales.customer.last_name,
    store_sales.customer.first_name,
    store_sales.customer.salutation,
    store_sales.customer.preferred_cust_flag,
    store_sales.ticket_number,
    ticket_cnt,
having
    ticket_cnt >= 1 and ticket_cnt <= 5

order by
    ticket_cnt desc,
    store_sales.customer.last_name asc,
    store_sales.ticket_number asc,
    store_sales.customer.id asc
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
    "store_sales_store_sales"."SS_TICKET_NUMBER" as "store_sales_ticket_number"
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
    "thoughtful"."store_sales_item_id" as "store_sales_item_id",
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
    "thoughtful"."store_sales_customer_id" is not null
),
abundant as (
SELECT
    CASE WHEN "cooperative"."store_sales_date_day_of_month" >= 1 and "cooperative"."store_sales_date_day_of_month" <= 2 and ( "cooperative"."store_sales_household_demographic_buy_potential" = '>10000' or "cooperative"."store_sales_household_demographic_buy_potential" = 'Unknown' ) and "cooperative"."store_sales_household_demographic_vehicle_count" > 0 and ( CASE
	WHEN "cooperative"."store_sales_household_demographic_vehicle_count" > 0 THEN ("cooperative"."store_sales_household_demographic_dependent_count" * 1.0) / "cooperative"."store_sales_household_demographic_vehicle_count"
	ELSE null
	END ) > 1 and "cooperative"."store_sales_date_year" in (1999,2000,2001) and "cooperative"."store_sales_store_county" in ('Orange County','Bronx County','Franklin Parish','Williamson County') THEN "cooperative"."store_sales_item_id" ELSE NULL END as "_virt_filter_id_4484877027926973"
FROM
    "cooperative"),
uneven as (
SELECT
    "cooperative"."store_sales_customer_id" as "store_sales_customer_id",
    "cooperative"."store_sales_ticket_number" as "store_sales_ticket_number",
    count("abundant"."_virt_filter_id_4484877027926973") as "ticket_cnt"
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
yummy as (
SELECT
    "questionable"."store_sales_customer_first_name" as "store_sales_customer_first_name",
    "questionable"."store_sales_customer_id" as "store_sales_customer_id",
    "questionable"."store_sales_customer_last_name" as "store_sales_customer_last_name",
    "questionable"."store_sales_customer_preferred_cust_flag" as "store_sales_customer_preferred_cust_flag",
    "questionable"."store_sales_customer_salutation" as "store_sales_customer_salutation",
    "uneven"."store_sales_ticket_number" as "store_sales_ticket_number",
    coalesce("uneven"."ticket_cnt",0) as "ticket_cnt"
FROM
    "questionable"
    LEFT OUTER JOIN "uneven" on "questionable"."store_sales_customer_id" = "uneven"."store_sales_customer_id")
SELECT
    "yummy"."store_sales_customer_last_name" as "store_sales_customer_last_name",
    "yummy"."store_sales_customer_first_name" as "store_sales_customer_first_name",
    "yummy"."store_sales_customer_salutation" as "store_sales_customer_salutation",
    "yummy"."store_sales_customer_preferred_cust_flag" as "store_sales_customer_preferred_cust_flag",
    "yummy"."store_sales_ticket_number" as "store_sales_ticket_number",
    "yummy"."ticket_cnt" as "ticket_cnt"
FROM
    "yummy"
WHERE
    "yummy"."ticket_cnt" >= 1 and "yummy"."ticket_cnt" <= 5

ORDER BY 
    "yummy"."ticket_cnt" desc,
    "yummy"."store_sales_customer_last_name" asc,
    "yummy"."store_sales_ticket_number" asc,
    "yummy"."store_sales_customer_id" asc
```

## Reference SQL (zquery log)

```sql
WITH 
cooperative as (
SELECT
    "store_sales_store_sales"."SS_CUSTOMER_SK" as "store_sales_customer_id",
    "store_sales_store_sales"."SS_TICKET_NUMBER" as "store_sales_ticket_number",
    count("store_sales_store_sales"."SS_ITEM_SK") as "ticket_cnt"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."store" as "store_sales_store_store" on "store_sales_store_sales"."SS_STORE_SK" = "store_sales_store_store"."S_STORE_SK"
    INNER JOIN "memory"."household_demographics" as "store_sales_household_demographic_household_demographics" on "store_sales_store_sales"."SS_HDEMO_SK" = "store_sales_household_demographic_household_demographics"."HD_DEMO_SK"
WHERE
    "store_sales_store_sales"."SS_CUSTOMER_SK" is not null and "store_sales_date_date"."D_DOM" >= 1 and "store_sales_date_date"."D_DOM" <= 2 and ( "store_sales_household_demographic_household_demographics"."HD_BUY_POTENTIAL" = '>10000' or "store_sales_household_demographic_household_demographics"."HD_BUY_POTENTIAL" = 'Unknown' ) and "store_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT" > 0 and ( CASE
	WHEN "store_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT" > 0 THEN ("store_sales_household_demographic_household_demographics"."HD_DEP_COUNT" * 1.0) / "store_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT"
	ELSE null
	END ) > 1 and "store_sales_date_date"."D_YEAR" in (1999,2000,2001) and "store_sales_store_store"."S_COUNTY" in ('Orange County','Bronx County','Franklin Parish','Williamson County')

GROUP BY
    1,
    2
HAVING
    "ticket_cnt" >= 1 and "ticket_cnt" <= 5
)
SELECT
    "store_sales_customer_customers"."C_LAST_NAME" as "store_sales_customer_last_name",
    "store_sales_customer_customers"."C_FIRST_NAME" as "store_sales_customer_first_name",
    "store_sales_customer_customers"."C_SALUTATION" as "store_sales_customer_salutation",
    "store_sales_customer_customers"."C_PREFERRED_CUST_FLAG" as "store_sales_customer_preferred_cust_flag",
    "cooperative"."store_sales_ticket_number" as "store_sales_ticket_number",
    "cooperative"."ticket_cnt" as "ticket_cnt"
FROM
    "cooperative"
    INNER JOIN "memory"."customer" as "store_sales_customer_customers" on "cooperative"."store_sales_customer_id" = "store_sales_customer_customers"."C_CUSTOMER_SK"
ORDER BY 
    "cooperative"."ticket_cnt" desc,
    "store_sales_customer_customers"."C_LAST_NAME" asc,
    "cooperative"."store_sales_ticket_number" asc,
    "store_sales_customer_customers"."C_CUSTOMER_SK" asc
```

## v4 execution error

```
Traceback (most recent call last):
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 161, in run_one
    result.v4_rows = execute(con, v4_sql)
                     ~~~~~~~^^^^^^^^^^^^^
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 102, in execute
    cursor = con.execute(sql)
_duckdb.BinderException: Binder Error: Referenced table "abundant" not found!
Candidate tables: "cooperative"

LINE 78:     count("abundant"."_virt_filter_id_4484877027926973") as "ticket_cnt...
                   ^
```
