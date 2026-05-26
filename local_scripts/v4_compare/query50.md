# Query 50

**Status:** `exec_fail`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | FAILED |
| reference execution | OK (6 rows) |

## Result comparison

_at least one side did not produce rows._

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 7331 | 127 | — |
| reference | 6312 | 108 | 110.01 ms |
| v4 / ref | 1.16x | 1.18x | — |

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
    15,
    "store_sales_return_date_date"."D_MOY",
    "store_sales_return_date_date"."D_YEAR",
    "store_sales_store_returns"."SR_CUSTOMER_SK",
    "store_sales_store_sales"."SS_CUSTOMER_SK"),
cooperative as (
SELECT
    "thoughtful"."store_sales_item_id" as "store_sales_item_id",
    "thoughtful"."store_sales_return_date_id" - "thoughtful"."store_sales_date_id" as "days_to_return",
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
    "thoughtful"."store_sales_store_zip" as "store_sales_store_zip"
FROM
    "thoughtful"),
questionable as (
SELECT
    CASE WHEN "cooperative"."days_to_return" > -1 and "cooperative"."days_to_return" <= 30 THEN "cooperative"."store_sales_item_id" ELSE NULL END as "_virt_filter_id_9423286833555721",
    CASE WHEN "cooperative"."days_to_return" > 120 and "cooperative"."days_to_return" <= 99999 THEN "cooperative"."store_sales_item_id" ELSE NULL END as "_virt_filter_id_5952815899712229",
    CASE WHEN "cooperative"."days_to_return" > 30 and "cooperative"."days_to_return" <= 60 THEN "cooperative"."store_sales_item_id" ELSE NULL END as "_virt_filter_id_7510659595786166",
    CASE WHEN "cooperative"."days_to_return" > 60 and "cooperative"."days_to_return" <= 90 THEN "cooperative"."store_sales_item_id" ELSE NULL END as "_virt_filter_id_6450764138736494",
    CASE WHEN "cooperative"."days_to_return" > 90 and "cooperative"."days_to_return" <= 120 THEN "cooperative"."store_sales_item_id" ELSE NULL END as "_virt_filter_id_4994347099701481"
FROM
    "cooperative"),
abundant as (
SELECT
    count("questionable"."_virt_filter_id_4994347099701481") as "_virt_agg_count_5623669394588902",
    count("questionable"."_virt_filter_id_5952815899712229") as "_virt_agg_count_7969998780980378",
    count("questionable"."_virt_filter_id_6450764138736494") as "_virt_agg_count_4020156712075239",
    count("questionable"."_virt_filter_id_7510659595786166") as "_virt_agg_count_3393740962845140",
    count("questionable"."_virt_filter_id_9423286833555721") as "_virt_agg_count_7691116690045464"
FROM
    "questionable"
GROUP BY
    "thoughtful"."store_sales_store_id",
    "thoughtful"."store_sales_ticket_number"),
uneven as (
SELECT
    "thoughtful"."store_sales_store_id" as "store_sales_store_id",
    sum("abundant"."_virt_agg_count_3393740962845140") as "days_31_60",
    sum("abundant"."_virt_agg_count_4020156712075239") as "days_61_90",
    sum("abundant"."_virt_agg_count_5623669394588902") as "days_91_120",
    sum("abundant"."_virt_agg_count_7691116690045464") as "days_30",
    sum("abundant"."_virt_agg_count_7969998780980378") as "days_120_plus"
FROM
    "abundant"
GROUP BY
    1)
SELECT
    "cooperative"."store_sales_store_name" as "store_sales_store_name",
    "cooperative"."store_sales_store_company_id" as "store_sales_store_company_id",
    "cooperative"."store_sales_store_street_number" as "store_sales_store_street_number",
    "cooperative"."store_sales_store_street_name" as "store_sales_store_street_name",
    "cooperative"."store_sales_store_street_type" as "store_sales_store_street_type",
    "cooperative"."store_sales_store_suite_number" as "store_sales_store_suite_number",
    "cooperative"."store_sales_store_city" as "store_sales_store_city",
    "cooperative"."store_sales_store_county" as "store_sales_store_county",
    "cooperative"."store_sales_store_state" as "store_sales_store_state",
    "cooperative"."store_sales_store_zip" as "store_sales_store_zip",
    "uneven"."days_30" as "days_30",
    "uneven"."days_31_60" as "days_31_60",
    "uneven"."days_61_90" as "days_61_90",
    "uneven"."days_91_120" as "days_91_120",
    "uneven"."days_120_plus" as "days_120_plus"
FROM
    "uneven"
    INNER JOIN "cooperative" on "uneven"."store_sales_store_id" = "cooperative"."store_sales_store_id"
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
questionable as (
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
    "store_sales_store_sales"."SS_STORE_SK" as "store_sales_store_id",
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
    INNER JOIN "memory"."customer" as "store_sales_return_customer_customers" on "store_sales_store_returns"."SR_CUSTOMER_SK" = "store_sales_return_customer_customers"."C_CUSTOMER_SK"
WHERE
    "store_sales_store_sales"."SS_CUSTOMER_SK" = "store_sales_return_customer_customers"."C_CUSTOMER_SK" and "store_sales_store_sales"."SS_STORE_SK" is not null

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
abundant as (
SELECT
    "questionable"."store_sales_store_id" as "store_sales_store_id",
    count(CASE WHEN "questionable"."days_to_return" > -1 and "questionable"."days_to_return" <= 30 THEN "questionable"."store_sales_item_id" ELSE NULL END) as "_virt_agg_count_7691116690045464",
    count(CASE WHEN "questionable"."days_to_return" > 120 and "questionable"."days_to_return" <= 99999 THEN "questionable"."store_sales_item_id" ELSE NULL END) as "_virt_agg_count_7969998780980378",
    count(CASE WHEN "questionable"."days_to_return" > 30 and "questionable"."days_to_return" <= 60 THEN "questionable"."store_sales_item_id" ELSE NULL END) as "_virt_agg_count_3393740962845140",
    count(CASE WHEN "questionable"."days_to_return" > 60 and "questionable"."days_to_return" <= 90 THEN "questionable"."store_sales_item_id" ELSE NULL END) as "_virt_agg_count_4020156712075239",
    count(CASE WHEN "questionable"."days_to_return" > 90 and "questionable"."days_to_return" <= 120 THEN "questionable"."store_sales_item_id" ELSE NULL END) as "_virt_agg_count_5623669394588902"
FROM
    "questionable"
GROUP BY
    1,
    "questionable"."store_sales_ticket_number"),
yummy as (
SELECT
    "abundant"."store_sales_store_id" as "store_sales_store_id",
    sum("abundant"."_virt_agg_count_3393740962845140") as "days_31_60",
    sum("abundant"."_virt_agg_count_4020156712075239") as "days_61_90",
    sum("abundant"."_virt_agg_count_5623669394588902") as "days_91_120",
    sum("abundant"."_virt_agg_count_7691116690045464") as "days_30",
    sum("abundant"."_virt_agg_count_7969998780980378") as "days_120_plus"
FROM
    "abundant"
GROUP BY
    1)
SELECT
    "thoughtful"."store_sales_store_name" as "store_sales_store_name",
    "thoughtful"."store_sales_store_company_id" as "store_sales_store_company_id",
    "thoughtful"."store_sales_store_street_number" as "store_sales_store_street_number",
    "thoughtful"."store_sales_store_street_name" as "store_sales_store_street_name",
    "thoughtful"."store_sales_store_street_type" as "store_sales_store_street_type",
    "thoughtful"."store_sales_store_suite_number" as "store_sales_store_suite_number",
    "thoughtful"."store_sales_store_city" as "store_sales_store_city",
    "thoughtful"."store_sales_store_county" as "store_sales_store_county",
    "thoughtful"."store_sales_store_state" as "store_sales_store_state",
    "thoughtful"."store_sales_store_zip" as "store_sales_store_zip",
    "yummy"."days_30" as "days_30",
    "yummy"."days_31_60" as "days_31_60",
    "yummy"."days_61_90" as "days_61_90",
    "yummy"."days_91_120" as "days_91_120",
    "yummy"."days_120_plus" as "days_120_plus"
FROM
    "thoughtful"
    LEFT OUTER JOIN "yummy" on "thoughtful"."store_sales_store_id" = "yummy"."store_sales_store_id"
ORDER BY 
    "thoughtful"."store_sales_store_name" asc nulls first,
    "thoughtful"."store_sales_store_company_id" asc nulls first,
    "thoughtful"."store_sales_store_street_number" asc nulls first,
    "thoughtful"."store_sales_store_street_name" asc nulls first,
    "thoughtful"."store_sales_store_street_type" asc nulls first,
    "thoughtful"."store_sales_store_suite_number" asc nulls first,
    "thoughtful"."store_sales_store_city" asc nulls first,
    "thoughtful"."store_sales_store_county" asc nulls first,
    "thoughtful"."store_sales_store_state" asc nulls first,
    "thoughtful"."store_sales_store_zip" asc nulls first
LIMIT (100)
```

## v4 execution error

```
Traceback (most recent call last):
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 179, in run_one
    result.v4_exec_seconds, result.v4_rows = _time(
                                             ~~~~~^
        lambda: execute(con, v4_sql)
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    )
    ^
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 45, in _time
    value = fn()
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 180, in <lambda>
    lambda: execute(con, v4_sql)
            ~~~~~~~^^^^^^^^^^^^^
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 120, in execute
    cursor = con.execute(sql)
_duckdb.BinderException: Binder Error: Referenced table "thoughtful" not found!
Candidate tables: "questionable"

LINE 83:     "thoughtful"."store_sales_store_id",
             ^
```
