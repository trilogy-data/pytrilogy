# Query 73

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (0 rows) |
| reference execution | OK (0 rows) |
| results identical | YES |

## Result comparison

v4 rows: 0 (0 distinct)
ref rows: 0 (0 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 2656 | 34 | 14.02 ms |
| reference | 2826 | 38 | 4.60 ms |
| v4 / ref | 0.94x | 0.89x | 3.05x |

## Preql

```
import physical_sales as physical_sales;

# Force the count to a strict per-ticket grain via the `by` clause; the rest of
# the ticket-level customer enrichment now derives from the same filtered rowset
# rather than going through an unfiltered cooperative dedup of all physical_sales.
auto ticket_cnt <- count(
    physical_sales.item.id
        ? physical_sales.date.day_of_month >= 1
and physical_sales.date.day_of_month <= 2
and (
    physical_sales.household_demographic.buy_potential = '>10000'
    or physical_sales.household_demographic.buy_potential = 'Unknown'
)
and physical_sales.household_demographic.vehicle_count > 0
and (case
        when physical_sales.household_demographic.vehicle_count > 0 then (physical_sales.household_demographic.dependent_count * 1.0) / physical_sales.household_demographic.vehicle_count
        else null
    end) > 1
and physical_sales.date.year in (1999, 2000, 2001)
and physical_sales.store.county in ('Orange County', 'Bronx County', 'Franklin Parish', 'Williamson County')
)
    by physical_sales.ticket_number, physical_sales.customer.id;

where
    physical_sales.customer.id is not null
select
    --physical_sales.customer.id,
    physical_sales.customer.last_name,
    physical_sales.customer.first_name,
    physical_sales.customer.salutation,
    physical_sales.customer.preferred_cust_flag,
    physical_sales.ticket_number,
    ticket_cnt,
having
    ticket_cnt >= 1 and ticket_cnt <= 5

order by
    ticket_cnt desc,
    physical_sales.customer.last_name asc,
    physical_sales.ticket_number asc,
    physical_sales.customer.id asc
;
```

## v4 generated SQL

```sql
SELECT
    "physical_sales_customer_customers"."C_LAST_NAME" as "physical_sales_customer_last_name",
    "physical_sales_customer_customers"."C_FIRST_NAME" as "physical_sales_customer_first_name",
    "physical_sales_customer_customers"."C_SALUTATION" as "physical_sales_customer_salutation",
    "physical_sales_customer_customers"."C_PREFERRED_CUST_FLAG" as "physical_sales_customer_preferred_cust_flag",
    "physical_sales_store_sales"."SS_TICKET_NUMBER" as "physical_sales_ticket_number",
    count(CASE WHEN "physical_sales_date_date"."D_DOM" >= 1 and "physical_sales_date_date"."D_DOM" <= 2 and ( "physical_sales_household_demographic_household_demographics"."HD_BUY_POTENTIAL" = '>10000' or "physical_sales_household_demographic_household_demographics"."HD_BUY_POTENTIAL" = 'Unknown' ) and "physical_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT" > 0 and ( CASE
	WHEN "physical_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT" > 0 THEN ("physical_sales_household_demographic_household_demographics"."HD_DEP_COUNT" * 1.0) / "physical_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT"
	ELSE null
	END ) > 1 and "physical_sales_date_date"."D_YEAR" in (1999,2000,2001) and "physical_sales_store_store"."S_COUNTY" in ('Orange County','Bronx County','Franklin Parish','Williamson County') THEN "physical_sales_store_sales"."SS_ITEM_SK" ELSE NULL END) as "ticket_cnt"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    LEFT OUTER JOIN "memory"."date_dim" as "physical_sales_date_date" on "physical_sales_store_sales"."SS_SOLD_DATE_SK" = "physical_sales_date_date"."D_DATE_SK"
    LEFT OUTER JOIN "memory"."store" as "physical_sales_store_store" on "physical_sales_store_sales"."SS_STORE_SK" = "physical_sales_store_store"."S_STORE_SK"
    INNER JOIN "memory"."customer" as "physical_sales_customer_customers" on "physical_sales_store_sales"."SS_CUSTOMER_SK" = "physical_sales_customer_customers"."C_CUSTOMER_SK"
    LEFT OUTER JOIN "memory"."household_demographics" as "physical_sales_household_demographic_household_demographics" on "physical_sales_store_sales"."SS_HDEMO_SK" = "physical_sales_household_demographic_household_demographics"."HD_DEMO_SK"
WHERE
    "physical_sales_store_sales"."SS_CUSTOMER_SK" is not null

GROUP BY
    1,
    2,
    3,
    4,
    5,
    "physical_sales_store_sales"."SS_CUSTOMER_SK"
HAVING
    "ticket_cnt" >= 1 and "ticket_cnt" <= 5

ORDER BY 
    "ticket_cnt" desc,
    "physical_sales_customer_customers"."C_LAST_NAME" asc,
    "physical_sales_store_sales"."SS_TICKET_NUMBER" asc,
    "physical_sales_store_sales"."SS_CUSTOMER_SK" asc
```

## Reference SQL (zquery log)

```sql
WITH 
cooperative as (
SELECT
    "physical_sales_store_sales"."SS_CUSTOMER_SK" as "physical_sales_customer_id",
    "physical_sales_store_sales"."SS_TICKET_NUMBER" as "physical_sales_ticket_number",
    count("physical_sales_store_sales"."SS_ITEM_SK") as "ticket_cnt"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "physical_sales_date_date" on "physical_sales_store_sales"."SS_SOLD_DATE_SK" = "physical_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."store" as "physical_sales_store_store" on "physical_sales_store_sales"."SS_STORE_SK" = "physical_sales_store_store"."S_STORE_SK"
    INNER JOIN "memory"."household_demographics" as "physical_sales_household_demographic_household_demographics" on "physical_sales_store_sales"."SS_HDEMO_SK" = "physical_sales_household_demographic_household_demographics"."HD_DEMO_SK"
WHERE
    "physical_sales_store_sales"."SS_CUSTOMER_SK" is not null and "physical_sales_date_date"."D_DOM" >= 1 and "physical_sales_date_date"."D_DOM" <= 2 and ( "physical_sales_household_demographic_household_demographics"."HD_BUY_POTENTIAL" = '>10000' or "physical_sales_household_demographic_household_demographics"."HD_BUY_POTENTIAL" = 'Unknown' ) and "physical_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT" > 0 and ( CASE
	WHEN "physical_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT" > 0 THEN ("physical_sales_household_demographic_household_demographics"."HD_DEP_COUNT" * 1.0) / "physical_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT"
	ELSE null
	END ) > 1 and "physical_sales_date_date"."D_YEAR" in (1999,2000,2001) and "physical_sales_store_store"."S_COUNTY" in ('Orange County','Bronx County','Franklin Parish','Williamson County')

GROUP BY
    1,
    2
HAVING
    "ticket_cnt" >= 1 and "ticket_cnt" <= 5
)
SELECT
    "physical_sales_customer_customers"."C_LAST_NAME" as "physical_sales_customer_last_name",
    "physical_sales_customer_customers"."C_FIRST_NAME" as "physical_sales_customer_first_name",
    "physical_sales_customer_customers"."C_SALUTATION" as "physical_sales_customer_salutation",
    "physical_sales_customer_customers"."C_PREFERRED_CUST_FLAG" as "physical_sales_customer_preferred_cust_flag",
    "cooperative"."physical_sales_ticket_number" as "physical_sales_ticket_number",
    "cooperative"."ticket_cnt" as "ticket_cnt"
FROM
    "cooperative"
    INNER JOIN "memory"."customer" as "physical_sales_customer_customers" on "cooperative"."physical_sales_customer_id" = "physical_sales_customer_customers"."C_CUSTOMER_SK"
ORDER BY 
    "cooperative"."ticket_cnt" desc,
    "physical_sales_customer_customers"."C_LAST_NAME" asc,
    "cooperative"."physical_sales_ticket_number" asc,
    "physical_sales_customer_customers"."C_CUSTOMER_SK" asc
```
