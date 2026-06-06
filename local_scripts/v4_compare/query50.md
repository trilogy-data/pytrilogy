# Query 50

**Status:** `gen_fail`

| Stage | Result |
| --- | --- |
| v4 SQL generation | FAILED |
| reference execution | OK (1 rows) |

## Result comparison

_at least one side did not produce rows._

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 0 | 0 | — |
| reference | 4728 | 75 | 5.20 ms |

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
    and physical_sales.customer.id = physical_sales.return_customer.id
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

_v4 did not produce SQL._

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

## v4 generation error

```
Traceback (most recent call last):
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 256, in generate_v4_sql
    statements = eng.generate_sql(preql_path.read_text())
  File "C:\Program Files\Python313\Lib\functools.py", line 983, in _method
    return dispatch(args[0].__class__).__get__(obj, cls)(*args, **kwargs)
           ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\executor.py", line 663, in _
    compiled_sql = self.generator.compile_statement(statement)
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\dialect\base.py", line 2315, in compile_statement
    raise ValueError(
    ...<2 lines>...
    )
ValueError: Invalid reference string found in query: 
WITH 
quizzical as (
SELECT
    INVALID_REFERENCE_BUG as "physical_sales_store_id",
    count(CASE WHEN INVALID_REFERENCE_BUG - INVALID_REFERENCE_BUG > -1 and INVALID_REFERENCE_BUG - INVALID_REFERENCE_BUG <= 30 THEN INVALID_REFERENCE_BUG ELSE NULL END) as "_virt_agg_count_6396225769465204",
    count(CASE WHEN INVALID_REFERENCE_BUG - INVALID_REFERENCE_BUG > 120 and INVALID_REFERENCE_BUG - INVALID_REFERENCE_BUG <= 99999 THEN INVALID_REFERENCE_BUG ELSE NULL END) as "_virt_agg_count_6261479071707891",
    count(CASE WHEN INVALID_REFERENCE_BUG - INVALID_REFERENCE_BUG > 30 and INVALID_REFERENCE_BUG - INVALID_REFERENCE_BUG <= 60 THEN INVALID_REFERENCE_BUG ELSE NULL END) as "_virt_agg_count_1754169376042040",
    count(CASE WHEN INVALID_REFERENCE_BUG - INVALID_REFERENCE_BUG > 60 and INVALID_REFERENCE_BUG - INVALID_REFERENCE_BUG <= 90 THEN INVALID_REFERENCE_BUG ELSE NULL END) as "_virt_agg_count_9524432267113409",
    count(CASE WHEN INVALID_REFERENCE_BUG - INVALID_REFERENCE_BUG > 90 and INVALID_REFERENCE_BUG - INVALID_REFERENCE_BUG <= 120 THEN INVALID_REFERENCE_BUG ELSE NULL END) as "_virt_agg_count_5608833957360839"

GROUP BY
    1)
SELECT
    sum("quizzical"."_virt_agg_count_6396225769465204") as "days_30",
    sum("quizzical"."_virt_agg_count_1754169376042040") as "days_31_60",
    sum("quizzical"."_virt_agg_count_9524432267113409") as "days_61_90",
    sum("quizzical"."_virt_agg_count_5608833957360839") as "days_91_120",
    sum("quizzical"."_virt_agg_count_6261479071707891") as "days_120_plus"
FROM
    "quizzical"
GROUP BY
    "quizzical"."physical_sales_store_id"
ORDER BY 
    INVALID_REFERENCE_BUG asc nulls first,
    INVALID_REFERENCE_BUG asc nulls first,
    INVALID_REFERENCE_BUG asc nulls first,
    INVALID_REFERENCE_BUG asc nulls first,
    INVALID_REFERENCE_BUG asc nulls first,
    INVALID_REFERENCE_BUG asc nulls first,
    INVALID_REFERENCE_BUG asc nulls first,
    INVALID_REFERENCE_BUG asc nulls first,
    INVALID_REFERENCE_BUG asc nulls first,
    INVALID_REFERENCE_BUG asc nulls first
LIMIT (100), this should never occur. Please create an issue to report this.
```
