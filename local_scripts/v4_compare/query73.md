# Query 73

**Status:** `mismatch`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (7 rows) |
| reference execution | OK (1 rows) |
| results identical | NO |

## Result comparison

v4 rows: 7 (1 distinct)
ref rows: 1 (1 distinct)
only in v4 (showing up to 5 of 1):
  6x  ('Maribel', 'Robinson', 'N', 'Miss', 153104, 5)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 5411 | 81 | 172.82 ms |
| reference | 2700 | 38 | 40.28 ms |
| v4 / ref | 2.00x | 2.13x | 4.29x |

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
cooperative as (
SELECT
    "store_sales_customer_customers"."C_FIRST_NAME" as "store_sales_customer_first_name",
    "store_sales_customer_customers"."C_LAST_NAME" as "store_sales_customer_last_name",
    "store_sales_customer_customers"."C_PREFERRED_CUST_FLAG" as "store_sales_customer_preferred_cust_flag",
    "store_sales_customer_customers"."C_SALUTATION" as "store_sales_customer_salutation",
    "store_sales_date_date"."D_DOM" as "store_sales_date_day_of_month",
    "store_sales_date_date"."D_YEAR" as "store_sales_date_year",
    "store_sales_household_demographic_household_demographics"."HD_BUY_POTENTIAL" as "store_sales_household_demographic_buy_potential",
    "store_sales_household_demographic_household_demographics"."HD_DEP_COUNT" as "store_sales_household_demographic_dependent_count",
    "store_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT" as "store_sales_household_demographic_vehicle_count",
    "store_sales_store_sales"."SS_CUSTOMER_SK" as "store_sales_customer_id",
    "store_sales_store_sales"."SS_ITEM_SK" as "store_sales_item_id",
    "store_sales_store_sales"."SS_TICKET_NUMBER" as "store_sales_ticket_number",
    "store_sales_store_store"."S_COUNTY" as "store_sales_store_county"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    LEFT OUTER JOIN "memory"."date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
    LEFT OUTER JOIN "memory"."store" as "store_sales_store_store" on "store_sales_store_sales"."SS_STORE_SK" = "store_sales_store_store"."S_STORE_SK"
    INNER JOIN "memory"."customer" as "store_sales_customer_customers" on "store_sales_store_sales"."SS_CUSTOMER_SK" = "store_sales_customer_customers"."C_CUSTOMER_SK"
    LEFT OUTER JOIN "memory"."household_demographics" as "store_sales_household_demographic_household_demographics" on "store_sales_store_sales"."SS_HDEMO_SK" = "store_sales_household_demographic_household_demographics"."HD_DEMO_SK"
WHERE
    "store_sales_store_sales"."SS_CUSTOMER_SK" is not null
),
questionable as (
SELECT
    "cooperative"."store_sales_customer_first_name" as "store_sales_customer_first_name",
    "cooperative"."store_sales_customer_id" as "store_sales_customer_id",
    "cooperative"."store_sales_customer_last_name" as "store_sales_customer_last_name",
    "cooperative"."store_sales_customer_preferred_cust_flag" as "store_sales_customer_preferred_cust_flag",
    "cooperative"."store_sales_customer_salutation" as "store_sales_customer_salutation",
    "cooperative"."store_sales_ticket_number" as "store_sales_ticket_number",
    CASE WHEN "cooperative"."store_sales_date_day_of_month" >= 1 and "cooperative"."store_sales_date_day_of_month" <= 2 and ( "cooperative"."store_sales_household_demographic_buy_potential" = '>10000' or "cooperative"."store_sales_household_demographic_buy_potential" = 'Unknown' ) and "cooperative"."store_sales_household_demographic_vehicle_count" > 0 and ( CASE
	WHEN "cooperative"."store_sales_household_demographic_vehicle_count" > 0 THEN ("cooperative"."store_sales_household_demographic_dependent_count" * 1.0) / "cooperative"."store_sales_household_demographic_vehicle_count"
	ELSE null
	END ) > 1 and "cooperative"."store_sales_date_year" in (1999,2000,2001) and "cooperative"."store_sales_store_county" in ('Orange County','Bronx County','Franklin Parish','Williamson County') THEN "cooperative"."store_sales_item_id" ELSE NULL END as "_virt_filter_id_4484877027926973"
FROM
    "cooperative"),
abundant as (
SELECT
    "questionable"."store_sales_customer_id" as "store_sales_customer_id",
    "questionable"."store_sales_ticket_number" as "store_sales_ticket_number",
    count("questionable"."_virt_filter_id_4484877027926973") as "ticket_cnt"
FROM
    "questionable"
GROUP BY
    1,
    2),
uneven as (
SELECT
    "abundant"."ticket_cnt" as "ticket_cnt",
    "questionable"."store_sales_customer_first_name" as "store_sales_customer_first_name",
    "questionable"."store_sales_customer_id" as "store_sales_customer_id",
    "questionable"."store_sales_customer_last_name" as "store_sales_customer_last_name",
    "questionable"."store_sales_customer_preferred_cust_flag" as "store_sales_customer_preferred_cust_flag",
    "questionable"."store_sales_customer_salutation" as "store_sales_customer_salutation",
    "questionable"."store_sales_ticket_number" as "store_sales_ticket_number"
FROM
    "abundant"
    INNER JOIN "questionable" on "abundant"."store_sales_customer_id" = "questionable"."store_sales_customer_id" AND "abundant"."store_sales_ticket_number" = "questionable"."store_sales_ticket_number"
WHERE
    "abundant"."ticket_cnt" >= 1
)
SELECT
    "uneven"."store_sales_customer_last_name" as "store_sales_customer_last_name",
    "uneven"."store_sales_customer_first_name" as "store_sales_customer_first_name",
    "uneven"."store_sales_customer_salutation" as "store_sales_customer_salutation",
    "uneven"."store_sales_customer_preferred_cust_flag" as "store_sales_customer_preferred_cust_flag",
    "uneven"."store_sales_ticket_number" as "store_sales_ticket_number",
    "uneven"."ticket_cnt" as "ticket_cnt"
FROM
    "uneven"
WHERE
    "uneven"."ticket_cnt" <= 5

ORDER BY 
    "uneven"."ticket_cnt" desc,
    "uneven"."store_sales_customer_last_name" asc,
    "uneven"."store_sales_ticket_number" asc,
    "uneven"."store_sales_customer_id" asc
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
