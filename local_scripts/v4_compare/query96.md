# Query 96

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (1 rows) |
| reference execution | OK (1 rows) |
| results identical | YES |

## Result comparison

v4 rows: 1 (1 distinct)
ref rows: 1 (1 distinct)

## SQL size

| Source | Chars | Lines |
| --- | --- | --- |
| v4 | 2635 | 52 |
| reference | 998 | 13 |
| v4 / ref | 2.64x | 4.00x |

## Preql

```
import store_sales as store_sales;

where
    store_sales.time.hour = 20
    and store_sales.time.minute >= 30
    and store_sales.household_demographic.dependent_count = 7
    and store_sales.store.name = 'ese'
select
    count(concat(store_sales.ticket_number::string, '-', store_sales.item.id::string)) as ct,
order by
    ct asc
limit 100
;
```

## v4 generated SQL

```sql
WITH 
cheerful as (
SELECT
    "store_sales_time_time"."T_HOUR" as "store_sales_time_hour",
    "store_sales_time_time"."T_MINUTE" as "store_sales_time_minute",
    "store_sales_time_time"."T_TIME_SK" as "store_sales_time_id"
FROM
    "memory"."time_dim" as "store_sales_time_time"),
wakeful as (
SELECT
    "store_sales_store_sales"."SS_HDEMO_SK" as "store_sales_household_demographic_id",
    "store_sales_store_sales"."SS_ITEM_SK" as "store_sales_item_id",
    "store_sales_store_sales"."SS_SOLD_TIME_SK" as "store_sales_time_id",
    "store_sales_store_sales"."SS_STORE_SK" as "store_sales_store_id",
    "store_sales_store_sales"."SS_TICKET_NUMBER" as "store_sales_ticket_number"
FROM
    "memory"."store_sales" as "store_sales_store_sales"),
highfalutin as (
SELECT
    "store_sales_store_store"."S_STORE_NAME" as "store_sales_store_name",
    "store_sales_store_store"."S_STORE_SK" as "store_sales_store_id"
FROM
    "memory"."store" as "store_sales_store_store"),
quizzical as (
SELECT
    "store_sales_household_demographic_household_demographics"."HD_DEMO_SK" as "store_sales_household_demographic_id",
    "store_sales_household_demographic_household_demographics"."HD_DEP_COUNT" as "store_sales_household_demographic_dependent_count"
FROM
    "memory"."household_demographics" as "store_sales_household_demographic_household_demographics"),
thoughtful as (
SELECT
    "cheerful"."store_sales_time_hour" as "store_sales_time_hour",
    "cheerful"."store_sales_time_minute" as "store_sales_time_minute",
    "highfalutin"."store_sales_store_name" as "store_sales_store_name",
    "quizzical"."store_sales_household_demographic_dependent_count" as "store_sales_household_demographic_dependent_count",
    "wakeful"."store_sales_item_id" as "store_sales_item_id",
    "wakeful"."store_sales_ticket_number" as "store_sales_ticket_number"
FROM
    "wakeful"
    LEFT OUTER JOIN "cheerful" on "wakeful"."store_sales_time_id" = "cheerful"."store_sales_time_id"
    LEFT OUTER JOIN "highfalutin" on "wakeful"."store_sales_store_id" = "highfalutin"."store_sales_store_id"
    LEFT OUTER JOIN "quizzical" on "wakeful"."store_sales_household_demographic_id" = "quizzical"."store_sales_household_demographic_id"
WHERE
    "cheerful"."store_sales_time_hour" = 20 and "cheerful"."store_sales_time_minute" >= 30 and "quizzical"."store_sales_household_demographic_dependent_count" = 7 and "highfalutin"."store_sales_store_name" = 'ese'
)
SELECT
    count((cast("thoughtful"."store_sales_ticket_number" as string) || '-' || cast("thoughtful"."store_sales_item_id" as string))) as "ct"
FROM
    "thoughtful"
ORDER BY 
    "ct" asc
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
SELECT
    count((cast("store_sales_store_sales"."SS_TICKET_NUMBER" as string) || '-' || cast("store_sales_store_sales"."SS_ITEM_SK" as string))) as "ct"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    INNER JOIN "memory"."time_dim" as "store_sales_time_time" on "store_sales_store_sales"."SS_SOLD_TIME_SK" = "store_sales_time_time"."T_TIME_SK"
    INNER JOIN "memory"."store" as "store_sales_store_store" on "store_sales_store_sales"."SS_STORE_SK" = "store_sales_store_store"."S_STORE_SK"
    INNER JOIN "memory"."household_demographics" as "store_sales_household_demographic_household_demographics" on "store_sales_store_sales"."SS_HDEMO_SK" = "store_sales_household_demographic_household_demographics"."HD_DEMO_SK"
WHERE
    "store_sales_time_time"."T_HOUR" = 20 and "store_sales_time_time"."T_MINUTE" >= 30 and "store_sales_household_demographic_household_demographics"."HD_DEP_COUNT" = 7 and "store_sales_store_store"."S_STORE_NAME" = 'ese'

ORDER BY 
    "ct" asc
LIMIT (100)
```
