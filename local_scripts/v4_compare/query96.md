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
| v4 | 998 | 13 | 5.42 ms |
| reference | 998 | 13 | 5.45 ms |
| v4 / ref | 1.00x | 1.00x | 1.00x |

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
