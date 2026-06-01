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

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 1046 | 13 | 6.30 ms |
| reference | 1046 | 13 | 6.31 ms |
| v4 / ref | 1.00x | 1.00x | 1.00x |

## Preql

```
import physical_sales as physical_sales;

where
    physical_sales.time.hour = 20
    and physical_sales.time.minute >= 30
    and physical_sales.household_demographic.dependent_count = 7
    and physical_sales.store.name = 'ese'
select
    count(concat(physical_sales.ticket_number::string, '-', physical_sales.item.id::string)) as ct,
order by
    ct asc
limit 100
;
```

## v4 generated SQL

```sql
SELECT
    count((cast("physical_sales_store_sales"."SS_TICKET_NUMBER" as string) || '-' || cast("physical_sales_store_sales"."SS_ITEM_SK" as string))) as "ct"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."time_dim" as "physical_sales_time_time" on "physical_sales_store_sales"."SS_SOLD_TIME_SK" = "physical_sales_time_time"."T_TIME_SK"
    INNER JOIN "memory"."store" as "physical_sales_store_store" on "physical_sales_store_sales"."SS_STORE_SK" = "physical_sales_store_store"."S_STORE_SK"
    INNER JOIN "memory"."household_demographics" as "physical_sales_household_demographic_household_demographics" on "physical_sales_store_sales"."SS_HDEMO_SK" = "physical_sales_household_demographic_household_demographics"."HD_DEMO_SK"
WHERE
    "physical_sales_time_time"."T_HOUR" = 20 and "physical_sales_time_time"."T_MINUTE" >= 30 and "physical_sales_household_demographic_household_demographics"."HD_DEP_COUNT" = 7 and "physical_sales_store_store"."S_STORE_NAME" = 'ese'

ORDER BY 
    "ct" asc
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
SELECT
    count((cast("physical_sales_store_sales"."SS_TICKET_NUMBER" as string) || '-' || cast("physical_sales_store_sales"."SS_ITEM_SK" as string))) as "ct"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."time_dim" as "physical_sales_time_time" on "physical_sales_store_sales"."SS_SOLD_TIME_SK" = "physical_sales_time_time"."T_TIME_SK"
    INNER JOIN "memory"."store" as "physical_sales_store_store" on "physical_sales_store_sales"."SS_STORE_SK" = "physical_sales_store_store"."S_STORE_SK"
    INNER JOIN "memory"."household_demographics" as "physical_sales_household_demographic_household_demographics" on "physical_sales_store_sales"."SS_HDEMO_SK" = "physical_sales_household_demographic_household_demographics"."HD_DEMO_SK"
WHERE
    "physical_sales_time_time"."T_HOUR" = 20 and "physical_sales_time_time"."T_MINUTE" >= 30 and "physical_sales_household_demographic_household_demographics"."HD_DEP_COUNT" = 7 and "physical_sales_store_store"."S_STORE_NAME" = 'ese'

ORDER BY 
    "ct" asc
LIMIT (100)
```
