# Query 50

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (6 rows) |
| reference execution | OK (6 rows) |
| results identical | YES |

## Result comparison

v4 rows: 6 (6 distinct)
ref rows: 6 (6 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 7110 | 125 | 126.98 ms |
| reference | 4728 | 75 | 37.74 ms |
| v4 / ref | 1.50x | 1.67x | 3.36x |

## Preql

```
import physical_sales as physical_sales;

auto days_to_return <- physical_sales.return_date.id - physical_sales.date.id;

def bucket_count(low, high) -> sum(
    count(physical_sales.item.id ? days_to_return > low and days_to_return <= high)
        by physical_sales.ticket_number, physical_sales.store.id
)
    by physical_sales.store.id;

where
    physical_sales.return_date.year = 2001
    and physical_sales.return_date.month_of_year = 8
    and physical_sales.billing_customer.id = physical_sales.return_customer.id
    and physical_sales.store.id is not null
select
    --physical_sales.store.id,
    physical_sales.store.name,
    physical_sales.store.company_id,
    physical_sales.store.street_number,
    physical_sales.store.street_name,
    physical_sales.store.street_type,
    physical_sales.store.suite_number,
    physical_sales.store.city,
    physical_sales.store.county,
    physical_sales.store.state,
    physical_sales.store.zip,
    @bucket_count(-1, 30) as days_30,
    @bucket_count(30, 60) as days_31_60,
    @bucket_count(60, 90) as days_61_90,
    @bucket_count(90, 120) as days_91_120,
    @bucket_count(120, 99999) as days_120_plus,
order by
    physical_sales.store.name asc nulls first,
    physical_sales.store.company_id asc nulls first,
    physical_sales.store.street_number asc nulls first,
    physical_sales.store.street_name asc nulls first,
    physical_sales.store.street_type asc nulls first,
    physical_sales.store.suite_number asc nulls first,
    physical_sales.store.city asc nulls first,
    physical_sales.store.county asc nulls first,
    physical_sales.store.state asc nulls first,
    physical_sales.store.zip asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
thoughtful as (
SELECT
    "physical_sales_return_date_date"."D_DATE_SK" as "physical_sales_return_date_id",
    "physical_sales_store_sales"."SS_ITEM_SK" as "physical_sales_item_id",
    "physical_sales_store_sales"."SS_SOLD_DATE_SK" as "physical_sales_date_id",
    "physical_sales_store_sales"."SS_STORE_SK" as "physical_sales_store_id",
    "physical_sales_store_sales"."SS_TICKET_NUMBER" as "physical_sales_ticket_number",
    "physical_sales_store_store"."S_CITY" as "physical_sales_store_city",
    "physical_sales_store_store"."S_COMPANY_ID" as "physical_sales_store_company_id",
    "physical_sales_store_store"."S_COUNTY" as "physical_sales_store_county",
    "physical_sales_store_store"."S_STATE" as "physical_sales_store_state",
    "physical_sales_store_store"."S_STORE_NAME" as "physical_sales_store_name",
    "physical_sales_store_store"."S_STREET_NAME" as "physical_sales_store_street_name",
    "physical_sales_store_store"."S_STREET_NUMBER" as "physical_sales_store_street_number",
    "physical_sales_store_store"."S_STREET_TYPE" as "physical_sales_store_street_type",
    "physical_sales_store_store"."S_SUITE_NUMBER" as "physical_sales_store_suite_number",
    "physical_sales_store_store"."S_ZIP" as "physical_sales_store_zip"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."store_returns" as "physical_sales_store_returns" on "physical_sales_store_sales"."SS_ITEM_SK" = "physical_sales_store_returns"."SR_ITEM_SK" AND "physical_sales_store_sales"."SS_TICKET_NUMBER" = "physical_sales_store_returns"."SR_TICKET_NUMBER"
    INNER JOIN "memory"."store" as "physical_sales_store_store" on "physical_sales_store_sales"."SS_STORE_SK" = "physical_sales_store_store"."S_STORE_SK"
    INNER JOIN "memory"."date_dim" as "physical_sales_return_date_date" on "physical_sales_store_returns"."SR_RETURNED_DATE_SK" = "physical_sales_return_date_date"."D_DATE_SK"
WHERE
    "physical_sales_return_date_date"."D_YEAR" = 2001 and "physical_sales_return_date_date"."D_MOY" = 8 and "physical_sales_store_sales"."SS_CUSTOMER_SK" = "physical_sales_store_returns"."SR_CUSTOMER_SK" and "physical_sales_store_sales"."SS_STORE_SK" is not null

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14,
    15),
yummy as (
SELECT
    "thoughtful"."physical_sales_store_city" as "physical_sales_store_city",
    "thoughtful"."physical_sales_store_company_id" as "physical_sales_store_company_id",
    "thoughtful"."physical_sales_store_county" as "physical_sales_store_county",
    "thoughtful"."physical_sales_store_id" as "physical_sales_store_id",
    "thoughtful"."physical_sales_store_name" as "physical_sales_store_name",
    "thoughtful"."physical_sales_store_state" as "physical_sales_store_state",
    "thoughtful"."physical_sales_store_street_name" as "physical_sales_store_street_name",
    "thoughtful"."physical_sales_store_street_number" as "physical_sales_store_street_number",
    "thoughtful"."physical_sales_store_street_type" as "physical_sales_store_street_type",
    "thoughtful"."physical_sales_store_suite_number" as "physical_sales_store_suite_number",
    "thoughtful"."physical_sales_store_zip" as "physical_sales_store_zip"
FROM
    "thoughtful"
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11),
cooperative as (
SELECT
    "thoughtful"."physical_sales_store_id" as "physical_sales_store_id",
    count(CASE WHEN "thoughtful"."physical_sales_return_date_id" - "thoughtful"."physical_sales_date_id" > -1 and "thoughtful"."physical_sales_return_date_id" - "thoughtful"."physical_sales_date_id" <= 30 THEN "thoughtful"."physical_sales_item_id" ELSE NULL END) as "_virt_agg_count_6396225769465204",
    count(CASE WHEN "thoughtful"."physical_sales_return_date_id" - "thoughtful"."physical_sales_date_id" > 120 and "thoughtful"."physical_sales_return_date_id" - "thoughtful"."physical_sales_date_id" <= 99999 THEN "thoughtful"."physical_sales_item_id" ELSE NULL END) as "_virt_agg_count_6261479071707891",
    count(CASE WHEN "thoughtful"."physical_sales_return_date_id" - "thoughtful"."physical_sales_date_id" > 30 and "thoughtful"."physical_sales_return_date_id" - "thoughtful"."physical_sales_date_id" <= 60 THEN "thoughtful"."physical_sales_item_id" ELSE NULL END) as "_virt_agg_count_1754169376042040",
    count(CASE WHEN "thoughtful"."physical_sales_return_date_id" - "thoughtful"."physical_sales_date_id" > 60 and "thoughtful"."physical_sales_return_date_id" - "thoughtful"."physical_sales_date_id" <= 90 THEN "thoughtful"."physical_sales_item_id" ELSE NULL END) as "_virt_agg_count_9524432267113409",
    count(CASE WHEN "thoughtful"."physical_sales_return_date_id" - "thoughtful"."physical_sales_date_id" > 90 and "thoughtful"."physical_sales_return_date_id" - "thoughtful"."physical_sales_date_id" <= 120 THEN "thoughtful"."physical_sales_item_id" ELSE NULL END) as "_virt_agg_count_5608833957360839"
FROM
    "thoughtful"
GROUP BY
    1,
    "thoughtful"."physical_sales_ticket_number")
SELECT
    "yummy"."physical_sales_store_name" as "physical_sales_store_name",
    "yummy"."physical_sales_store_company_id" as "physical_sales_store_company_id",
    "yummy"."physical_sales_store_street_number" as "physical_sales_store_street_number",
    "yummy"."physical_sales_store_street_name" as "physical_sales_store_street_name",
    "yummy"."physical_sales_store_street_type" as "physical_sales_store_street_type",
    "yummy"."physical_sales_store_suite_number" as "physical_sales_store_suite_number",
    "yummy"."physical_sales_store_city" as "physical_sales_store_city",
    "yummy"."physical_sales_store_county" as "physical_sales_store_county",
    "yummy"."physical_sales_store_state" as "physical_sales_store_state",
    "yummy"."physical_sales_store_zip" as "physical_sales_store_zip",
    sum("cooperative"."_virt_agg_count_6396225769465204") as "days_30",
    sum("cooperative"."_virt_agg_count_1754169376042040") as "days_31_60",
    sum("cooperative"."_virt_agg_count_9524432267113409") as "days_61_90",
    sum("cooperative"."_virt_agg_count_5608833957360839") as "days_91_120",
    sum("cooperative"."_virt_agg_count_6261479071707891") as "days_120_plus"
FROM
    "yummy"
    INNER JOIN "cooperative" on "yummy"."physical_sales_store_id" = "cooperative"."physical_sales_store_id"
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    "yummy"."physical_sales_store_id"
ORDER BY 
    "yummy"."physical_sales_store_name" asc nulls first,
    "yummy"."physical_sales_store_company_id" asc nulls first,
    "yummy"."physical_sales_store_street_number" asc nulls first,
    "yummy"."physical_sales_store_street_name" asc nulls first,
    "yummy"."physical_sales_store_street_type" asc nulls first,
    "yummy"."physical_sales_store_suite_number" asc nulls first,
    "yummy"."physical_sales_store_city" asc nulls first,
    "yummy"."physical_sales_store_county" asc nulls first,
    "yummy"."physical_sales_store_state" asc nulls first,
    "yummy"."physical_sales_store_zip" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
cheerful as (
SELECT
    "physical_sales_return_date_date"."D_DATE_SK" - "physical_sales_store_sales"."SS_SOLD_DATE_SK" as "days_to_return",
    "physical_sales_store_sales"."SS_ITEM_SK" as "physical_sales_item_id",
    "physical_sales_store_sales"."SS_STORE_SK" as "physical_sales_store_id",
    "physical_sales_store_sales"."SS_TICKET_NUMBER" as "physical_sales_ticket_number"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."store_returns" as "physical_sales_store_returns" on "physical_sales_store_sales"."SS_ITEM_SK" = "physical_sales_store_returns"."SR_ITEM_SK" AND "physical_sales_store_sales"."SS_TICKET_NUMBER" = "physical_sales_store_returns"."SR_TICKET_NUMBER"
    INNER JOIN "memory"."date_dim" as "physical_sales_return_date_date" on "physical_sales_store_returns"."SR_RETURNED_DATE_SK" = "physical_sales_return_date_date"."D_DATE_SK"
WHERE
    "physical_sales_return_date_date"."D_YEAR" = 2001 and "physical_sales_return_date_date"."D_MOY" = 8 and "physical_sales_store_sales"."SS_CUSTOMER_SK" = "physical_sales_store_returns"."SR_CUSTOMER_SK" and "physical_sales_store_sales"."SS_STORE_SK" is not null

GROUP BY
    1,
    2,
    3,
    4),
thoughtful as (
SELECT
    "cheerful"."physical_sales_store_id" as "physical_sales_store_id",
    count(CASE WHEN "cheerful"."days_to_return" > -1 and "cheerful"."days_to_return" <= 30 THEN "cheerful"."physical_sales_item_id" ELSE NULL END) as "_virt_agg_count_6396225769465204",
    count(CASE WHEN "cheerful"."days_to_return" > 120 and "cheerful"."days_to_return" <= 99999 THEN "cheerful"."physical_sales_item_id" ELSE NULL END) as "_virt_agg_count_6261479071707891",
    count(CASE WHEN "cheerful"."days_to_return" > 30 and "cheerful"."days_to_return" <= 60 THEN "cheerful"."physical_sales_item_id" ELSE NULL END) as "_virt_agg_count_1754169376042040",
    count(CASE WHEN "cheerful"."days_to_return" > 60 and "cheerful"."days_to_return" <= 90 THEN "cheerful"."physical_sales_item_id" ELSE NULL END) as "_virt_agg_count_9524432267113409",
    count(CASE WHEN "cheerful"."days_to_return" > 90 and "cheerful"."days_to_return" <= 120 THEN "cheerful"."physical_sales_item_id" ELSE NULL END) as "_virt_agg_count_5608833957360839"
FROM
    "cheerful"
GROUP BY
    1,
    "cheerful"."physical_sales_ticket_number"),
questionable as (
SELECT
    "thoughtful"."physical_sales_store_id" as "physical_sales_store_id",
    sum("thoughtful"."_virt_agg_count_1754169376042040") as "days_31_60",
    sum("thoughtful"."_virt_agg_count_5608833957360839") as "days_91_120",
    sum("thoughtful"."_virt_agg_count_6261479071707891") as "days_120_plus",
    sum("thoughtful"."_virt_agg_count_6396225769465204") as "days_30",
    sum("thoughtful"."_virt_agg_count_9524432267113409") as "days_61_90"
FROM
    "thoughtful"
GROUP BY
    1)
SELECT
    "physical_sales_store_store"."S_STORE_NAME" as "physical_sales_store_name",
    "physical_sales_store_store"."S_COMPANY_ID" as "physical_sales_store_company_id",
    "physical_sales_store_store"."S_STREET_NUMBER" as "physical_sales_store_street_number",
    "physical_sales_store_store"."S_STREET_NAME" as "physical_sales_store_street_name",
    "physical_sales_store_store"."S_STREET_TYPE" as "physical_sales_store_street_type",
    "physical_sales_store_store"."S_SUITE_NUMBER" as "physical_sales_store_suite_number",
    "physical_sales_store_store"."S_CITY" as "physical_sales_store_city",
    "physical_sales_store_store"."S_COUNTY" as "physical_sales_store_county",
    "physical_sales_store_store"."S_STATE" as "physical_sales_store_state",
    "physical_sales_store_store"."S_ZIP" as "physical_sales_store_zip",
    "questionable"."days_30" as "days_30",
    "questionable"."days_31_60" as "days_31_60",
    "questionable"."days_61_90" as "days_61_90",
    "questionable"."days_91_120" as "days_91_120",
    "questionable"."days_120_plus" as "days_120_plus"
FROM
    "memory"."store" as "physical_sales_store_store"
    INNER JOIN "questionable" on "physical_sales_store_store"."S_STORE_SK" = "questionable"."physical_sales_store_id"
ORDER BY 
    "physical_sales_store_store"."S_STORE_NAME" asc nulls first,
    "physical_sales_store_store"."S_COMPANY_ID" asc nulls first,
    "physical_sales_store_store"."S_STREET_NUMBER" asc nulls first,
    "physical_sales_store_store"."S_STREET_NAME" asc nulls first,
    "physical_sales_store_store"."S_STREET_TYPE" asc nulls first,
    "physical_sales_store_store"."S_SUITE_NUMBER" asc nulls first,
    "physical_sales_store_store"."S_CITY" asc nulls first,
    "physical_sales_store_store"."S_COUNTY" asc nulls first,
    "physical_sales_store_store"."S_STATE" asc nulls first,
    "physical_sales_store_store"."S_ZIP" asc nulls first
LIMIT (100)
```
