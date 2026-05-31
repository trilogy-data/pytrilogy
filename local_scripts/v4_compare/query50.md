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
| v4 | 6632 | 117 | 141.23 ms |
| reference | 4533 | 75 | 43.04 ms |
| v4 / ref | 1.46x | 1.56x | 3.28x |

## Preql

```
import store_sales as store_sales;

auto days_to_return <- store_sales.return_date.id - store_sales.date.id;

def bucket_count(low, high) -> sum(
    count(store_sales.item.id ? days_to_return > low and days_to_return <= high)
        by store_sales.ticket_number, store_sales.store.id
)
    by store_sales.store.id;

where
    store_sales.return_date.year = 2001
    and store_sales.return_date.month_of_year = 8
    and store_sales.customer.id = store_sales.return_customer.id
    and store_sales.store.id is not null
select
    --store_sales.store.id,
    store_sales.store.name,
    store_sales.store.company_id,
    store_sales.store.street_number,
    store_sales.store.street_name,
    store_sales.store.street_type,
    store_sales.store.suite_number,
    store_sales.store.city,
    store_sales.store.county,
    store_sales.store.state,
    store_sales.store.zip,
    @bucket_count(-1, 30) as days_30,
    @bucket_count(30, 60) as days_31_60,
    @bucket_count(60, 90) as days_61_90,
    @bucket_count(90, 120) as days_91_120,
    @bucket_count(120, 99999) as days_120_plus,
order by
    store_sales.store.name asc nulls first,
    store_sales.store.company_id asc nulls first,
    store_sales.store.street_number asc nulls first,
    store_sales.store.street_name asc nulls first,
    store_sales.store.street_type asc nulls first,
    store_sales.store.suite_number asc nulls first,
    store_sales.store.city asc nulls first,
    store_sales.store.county asc nulls first,
    store_sales.store.state asc nulls first,
    store_sales.store.zip asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
thoughtful as (
SELECT
    "store_sales_return_date_date"."D_DATE_SK" as "store_sales_return_date_id",
    "store_sales_store_sales"."SS_ITEM_SK" as "store_sales_item_id",
    "store_sales_store_sales"."SS_SOLD_DATE_SK" as "store_sales_date_id",
    "store_sales_store_sales"."SS_STORE_SK" as "store_sales_store_id",
    "store_sales_store_sales"."SS_TICKET_NUMBER" as "store_sales_ticket_number",
    "store_sales_store_store"."S_CITY" as "store_sales_store_city",
    "store_sales_store_store"."S_COMPANY_ID" as "store_sales_store_company_id",
    "store_sales_store_store"."S_COUNTY" as "store_sales_store_county",
    "store_sales_store_store"."S_STATE" as "store_sales_store_state",
    "store_sales_store_store"."S_STORE_NAME" as "store_sales_store_name",
    "store_sales_store_store"."S_STREET_NAME" as "store_sales_store_street_name",
    "store_sales_store_store"."S_STREET_NUMBER" as "store_sales_store_street_number",
    "store_sales_store_store"."S_STREET_TYPE" as "store_sales_store_street_type",
    "store_sales_store_store"."S_SUITE_NUMBER" as "store_sales_store_suite_number",
    "store_sales_store_store"."S_ZIP" as "store_sales_store_zip"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    INNER JOIN "memory"."store_returns" as "store_sales_store_returns" on "store_sales_store_sales"."SS_ITEM_SK" = "store_sales_store_returns"."SR_ITEM_SK" AND "store_sales_store_sales"."SS_TICKET_NUMBER" = "store_sales_store_returns"."SR_TICKET_NUMBER"
    INNER JOIN "memory"."store" as "store_sales_store_store" on "store_sales_store_sales"."SS_STORE_SK" = "store_sales_store_store"."S_STORE_SK"
    INNER JOIN "memory"."date_dim" as "store_sales_return_date_date" on "store_sales_store_returns"."SR_RETURNED_DATE_SK" = "store_sales_return_date_date"."D_DATE_SK"
WHERE
    "store_sales_return_date_date"."D_YEAR" = 2001 and "store_sales_return_date_date"."D_MOY" = 8 and "store_sales_store_sales"."SS_CUSTOMER_SK" = "store_sales_store_returns"."SR_CUSTOMER_SK" and "store_sales_store_sales"."SS_STORE_SK" is not null

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
cooperative as (
SELECT
    "thoughtful"."store_sales_store_city" as "store_sales_store_city",
    "thoughtful"."store_sales_store_company_id" as "store_sales_store_company_id",
    "thoughtful"."store_sales_store_county" as "store_sales_store_county",
    "thoughtful"."store_sales_store_id" as "store_sales_store_id",
    "thoughtful"."store_sales_store_name" as "store_sales_store_name",
    "thoughtful"."store_sales_store_state" as "store_sales_store_state",
    "thoughtful"."store_sales_store_street_name" as "store_sales_store_street_name",
    "thoughtful"."store_sales_store_street_number" as "store_sales_store_street_number",
    "thoughtful"."store_sales_store_street_type" as "store_sales_store_street_type",
    "thoughtful"."store_sales_store_suite_number" as "store_sales_store_suite_number",
    "thoughtful"."store_sales_store_zip" as "store_sales_store_zip",
    count(CASE WHEN "thoughtful"."store_sales_return_date_id" - "thoughtful"."store_sales_date_id" > -1 and "thoughtful"."store_sales_return_date_id" - "thoughtful"."store_sales_date_id" <= 30 THEN "thoughtful"."store_sales_item_id" ELSE NULL END) as "_virt_agg_count_7691116690045464",
    count(CASE WHEN "thoughtful"."store_sales_return_date_id" - "thoughtful"."store_sales_date_id" > 120 and "thoughtful"."store_sales_return_date_id" - "thoughtful"."store_sales_date_id" <= 99999 THEN "thoughtful"."store_sales_item_id" ELSE NULL END) as "_virt_agg_count_7969998780980378",
    count(CASE WHEN "thoughtful"."store_sales_return_date_id" - "thoughtful"."store_sales_date_id" > 30 and "thoughtful"."store_sales_return_date_id" - "thoughtful"."store_sales_date_id" <= 60 THEN "thoughtful"."store_sales_item_id" ELSE NULL END) as "_virt_agg_count_3393740962845140",
    count(CASE WHEN "thoughtful"."store_sales_return_date_id" - "thoughtful"."store_sales_date_id" > 60 and "thoughtful"."store_sales_return_date_id" - "thoughtful"."store_sales_date_id" <= 90 THEN "thoughtful"."store_sales_item_id" ELSE NULL END) as "_virt_agg_count_4020156712075239",
    count(CASE WHEN "thoughtful"."store_sales_return_date_id" - "thoughtful"."store_sales_date_id" > 90 and "thoughtful"."store_sales_return_date_id" - "thoughtful"."store_sales_date_id" <= 120 THEN "thoughtful"."store_sales_item_id" ELSE NULL END) as "_virt_agg_count_5623669394588902"
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
    11,
    "thoughtful"."store_sales_ticket_number")
SELECT
    sum("cooperative"."_virt_agg_count_7969998780980378") as "days_120_plus",
    sum("cooperative"."_virt_agg_count_7691116690045464") as "days_30",
    sum("cooperative"."_virt_agg_count_3393740962845140") as "days_31_60",
    sum("cooperative"."_virt_agg_count_4020156712075239") as "days_61_90",
    sum("cooperative"."_virt_agg_count_5623669394588902") as "days_91_120",
    "cooperative"."store_sales_store_city" as "store_sales_store_city",
    "cooperative"."store_sales_store_company_id" as "store_sales_store_company_id",
    "cooperative"."store_sales_store_county" as "store_sales_store_county",
    "cooperative"."store_sales_store_name" as "store_sales_store_name",
    "cooperative"."store_sales_store_state" as "store_sales_store_state",
    "cooperative"."store_sales_store_street_name" as "store_sales_store_street_name",
    "cooperative"."store_sales_store_street_number" as "store_sales_store_street_number",
    "cooperative"."store_sales_store_street_type" as "store_sales_store_street_type",
    "cooperative"."store_sales_store_suite_number" as "store_sales_store_suite_number",
    "cooperative"."store_sales_store_zip" as "store_sales_store_zip"
FROM
    "cooperative"
GROUP BY
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14,
    15,
    "cooperative"."store_sales_store_id"
ORDER BY 
    "cooperative"."store_sales_store_name" asc nulls first,
    "cooperative"."store_sales_store_company_id" asc nulls first,
    "cooperative"."store_sales_store_street_number" asc nulls first,
    "cooperative"."store_sales_store_street_name" asc nulls first,
    "cooperative"."store_sales_store_street_type" asc nulls first,
    "cooperative"."store_sales_store_suite_number" asc nulls first,
    "cooperative"."store_sales_store_city" asc nulls first,
    "cooperative"."store_sales_store_county" asc nulls first,
    "cooperative"."store_sales_store_state" asc nulls first,
    "cooperative"."store_sales_store_zip" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
cheerful as (
SELECT
    "store_sales_return_date_date"."D_DATE_SK" - "store_sales_store_sales"."SS_SOLD_DATE_SK" as "days_to_return",
    "store_sales_store_sales"."SS_ITEM_SK" as "store_sales_item_id",
    "store_sales_store_sales"."SS_STORE_SK" as "store_sales_store_id",
    "store_sales_store_sales"."SS_TICKET_NUMBER" as "store_sales_ticket_number"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    INNER JOIN "memory"."store_returns" as "store_sales_store_returns" on "store_sales_store_sales"."SS_ITEM_SK" = "store_sales_store_returns"."SR_ITEM_SK" AND "store_sales_store_sales"."SS_TICKET_NUMBER" = "store_sales_store_returns"."SR_TICKET_NUMBER"
    INNER JOIN "memory"."date_dim" as "store_sales_return_date_date" on "store_sales_store_returns"."SR_RETURNED_DATE_SK" = "store_sales_return_date_date"."D_DATE_SK"
WHERE
    "store_sales_return_date_date"."D_YEAR" = 2001 and "store_sales_return_date_date"."D_MOY" = 8 and "store_sales_store_sales"."SS_CUSTOMER_SK" = "store_sales_store_returns"."SR_CUSTOMER_SK" and "store_sales_store_sales"."SS_STORE_SK" is not null

GROUP BY
    1,
    2,
    3,
    4),
thoughtful as (
SELECT
    "cheerful"."store_sales_store_id" as "store_sales_store_id",
    count(CASE WHEN "cheerful"."days_to_return" > -1 and "cheerful"."days_to_return" <= 30 THEN "cheerful"."store_sales_item_id" ELSE NULL END) as "_virt_agg_count_7691116690045464",
    count(CASE WHEN "cheerful"."days_to_return" > 120 and "cheerful"."days_to_return" <= 99999 THEN "cheerful"."store_sales_item_id" ELSE NULL END) as "_virt_agg_count_7969998780980378",
    count(CASE WHEN "cheerful"."days_to_return" > 30 and "cheerful"."days_to_return" <= 60 THEN "cheerful"."store_sales_item_id" ELSE NULL END) as "_virt_agg_count_3393740962845140",
    count(CASE WHEN "cheerful"."days_to_return" > 60 and "cheerful"."days_to_return" <= 90 THEN "cheerful"."store_sales_item_id" ELSE NULL END) as "_virt_agg_count_4020156712075239",
    count(CASE WHEN "cheerful"."days_to_return" > 90 and "cheerful"."days_to_return" <= 120 THEN "cheerful"."store_sales_item_id" ELSE NULL END) as "_virt_agg_count_5623669394588902"
FROM
    "cheerful"
GROUP BY
    1,
    "cheerful"."store_sales_ticket_number"),
questionable as (
SELECT
    "thoughtful"."store_sales_store_id" as "store_sales_store_id",
    sum("thoughtful"."_virt_agg_count_3393740962845140") as "days_31_60",
    sum("thoughtful"."_virt_agg_count_4020156712075239") as "days_61_90",
    sum("thoughtful"."_virt_agg_count_5623669394588902") as "days_91_120",
    sum("thoughtful"."_virt_agg_count_7691116690045464") as "days_30",
    sum("thoughtful"."_virt_agg_count_7969998780980378") as "days_120_plus"
FROM
    "thoughtful"
GROUP BY
    1)
SELECT
    "store_sales_store_store"."S_STORE_NAME" as "store_sales_store_name",
    "store_sales_store_store"."S_COMPANY_ID" as "store_sales_store_company_id",
    "store_sales_store_store"."S_STREET_NUMBER" as "store_sales_store_street_number",
    "store_sales_store_store"."S_STREET_NAME" as "store_sales_store_street_name",
    "store_sales_store_store"."S_STREET_TYPE" as "store_sales_store_street_type",
    "store_sales_store_store"."S_SUITE_NUMBER" as "store_sales_store_suite_number",
    "store_sales_store_store"."S_CITY" as "store_sales_store_city",
    "store_sales_store_store"."S_COUNTY" as "store_sales_store_county",
    "store_sales_store_store"."S_STATE" as "store_sales_store_state",
    "store_sales_store_store"."S_ZIP" as "store_sales_store_zip",
    "questionable"."days_30" as "days_30",
    "questionable"."days_31_60" as "days_31_60",
    "questionable"."days_61_90" as "days_61_90",
    "questionable"."days_91_120" as "days_91_120",
    "questionable"."days_120_plus" as "days_120_plus"
FROM
    "memory"."store" as "store_sales_store_store"
    INNER JOIN "questionable" on "store_sales_store_store"."S_STORE_SK" = "questionable"."store_sales_store_id"
ORDER BY 
    "store_sales_store_store"."S_STORE_NAME" asc nulls first,
    "store_sales_store_store"."S_COMPANY_ID" asc nulls first,
    "store_sales_store_store"."S_STREET_NUMBER" asc nulls first,
    "store_sales_store_store"."S_STREET_NAME" asc nulls first,
    "store_sales_store_store"."S_STREET_TYPE" asc nulls first,
    "store_sales_store_store"."S_SUITE_NUMBER" asc nulls first,
    "store_sales_store_store"."S_CITY" asc nulls first,
    "store_sales_store_store"."S_COUNTY" asc nulls first,
    "store_sales_store_store"."S_STATE" asc nulls first,
    "store_sales_store_store"."S_ZIP" asc nulls first
LIMIT (100)
```
