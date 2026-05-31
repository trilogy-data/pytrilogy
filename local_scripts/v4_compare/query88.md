# Query 88

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
| v4 | 3887 | 32 | 29.90 ms |
| reference | 3887 | 32 | 29.99 ms |
| v4 / ref | 1.00x | 1.00x | 1.00x |

## Preql

```
import store_sales as store_sales;

def bucket_count(h, min_low, min_high) -> sum(
    count(
        store_sales.item.id
            ? store_sales.time.hour = h
and store_sales.time.minute >= min_low
and store_sales.time.minute < min_high
    )
        by store_sales.ticket_number
);

where
    store_sales.store.name = 'ese'
    and store_sales.time.hour in (8, 9, 10, 11, 12)
    and (
        (
            store_sales.household_demographic.dependent_count = 4
            and store_sales.household_demographic.vehicle_count <= 6
        )
        or (
            store_sales.household_demographic.dependent_count = 2
            and store_sales.household_demographic.vehicle_count <= 4
        )
        or (
            store_sales.household_demographic.dependent_count = 0
            and store_sales.household_demographic.vehicle_count <= 2
        )
    )
select
    @bucket_count(8, 30, 60) as h8_30_to_9,
    @bucket_count(9, 0, 30) as h9_to_9_30,
    @bucket_count(9, 30, 60) as h9_30_to_10,
    @bucket_count(10, 0, 30) as h10_to_10_30,
    @bucket_count(10, 30, 60) as h10_30_to_11,
    @bucket_count(11, 0, 30) as h11_to_11_30,
    @bucket_count(11, 30, 60) as h11_30_to_12,
    @bucket_count(12, 0, 30) as h12_to_12_30,
;
```

## v4 generated SQL

```sql
WITH 
thoughtful as (
SELECT
    count(CASE WHEN "store_sales_time_time"."T_HOUR" = 10 and "store_sales_time_time"."T_MINUTE" >= 0 and "store_sales_time_time"."T_MINUTE" < 30 THEN "store_sales_store_sales"."SS_ITEM_SK" ELSE NULL END) as "_virt_agg_count_7911186439813521",
    count(CASE WHEN "store_sales_time_time"."T_HOUR" = 10 and "store_sales_time_time"."T_MINUTE" >= 30 and "store_sales_time_time"."T_MINUTE" < 60 THEN "store_sales_store_sales"."SS_ITEM_SK" ELSE NULL END) as "_virt_agg_count_4915320083864949",
    count(CASE WHEN "store_sales_time_time"."T_HOUR" = 11 and "store_sales_time_time"."T_MINUTE" >= 0 and "store_sales_time_time"."T_MINUTE" < 30 THEN "store_sales_store_sales"."SS_ITEM_SK" ELSE NULL END) as "_virt_agg_count_9208026921280603",
    count(CASE WHEN "store_sales_time_time"."T_HOUR" = 11 and "store_sales_time_time"."T_MINUTE" >= 30 and "store_sales_time_time"."T_MINUTE" < 60 THEN "store_sales_store_sales"."SS_ITEM_SK" ELSE NULL END) as "_virt_agg_count_7914196801151291",
    count(CASE WHEN "store_sales_time_time"."T_HOUR" = 12 and "store_sales_time_time"."T_MINUTE" >= 0 and "store_sales_time_time"."T_MINUTE" < 30 THEN "store_sales_store_sales"."SS_ITEM_SK" ELSE NULL END) as "_virt_agg_count_6874762517186813",
    count(CASE WHEN "store_sales_time_time"."T_HOUR" = 8 and "store_sales_time_time"."T_MINUTE" >= 30 and "store_sales_time_time"."T_MINUTE" < 60 THEN "store_sales_store_sales"."SS_ITEM_SK" ELSE NULL END) as "_virt_agg_count_6255323248253146",
    count(CASE WHEN "store_sales_time_time"."T_HOUR" = 9 and "store_sales_time_time"."T_MINUTE" >= 0 and "store_sales_time_time"."T_MINUTE" < 30 THEN "store_sales_store_sales"."SS_ITEM_SK" ELSE NULL END) as "_virt_agg_count_2910789853377884",
    count(CASE WHEN "store_sales_time_time"."T_HOUR" = 9 and "store_sales_time_time"."T_MINUTE" >= 30 and "store_sales_time_time"."T_MINUTE" < 60 THEN "store_sales_store_sales"."SS_ITEM_SK" ELSE NULL END) as "_virt_agg_count_2219364601882723"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    INNER JOIN "memory"."time_dim" as "store_sales_time_time" on "store_sales_store_sales"."SS_SOLD_TIME_SK" = "store_sales_time_time"."T_TIME_SK"
    INNER JOIN "memory"."store" as "store_sales_store_store" on "store_sales_store_sales"."SS_STORE_SK" = "store_sales_store_store"."S_STORE_SK"
    INNER JOIN "memory"."household_demographics" as "store_sales_household_demographic_household_demographics" on "store_sales_store_sales"."SS_HDEMO_SK" = "store_sales_household_demographic_household_demographics"."HD_DEMO_SK"
WHERE
    "store_sales_store_store"."S_STORE_NAME" = 'ese' and "store_sales_time_time"."T_HOUR" in (8,9,10,11,12) and ( ( "store_sales_household_demographic_household_demographics"."HD_DEP_COUNT" = 4 and "store_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT" <= 6 ) or ( "store_sales_household_demographic_household_demographics"."HD_DEP_COUNT" = 2 and "store_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT" <= 4 ) or ( "store_sales_household_demographic_household_demographics"."HD_DEP_COUNT" = 0 and "store_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT" <= 2 ) )

GROUP BY
    "store_sales_store_sales"."SS_TICKET_NUMBER")
SELECT
    sum("thoughtful"."_virt_agg_count_4915320083864949") as "h10_30_to_11",
    sum("thoughtful"."_virt_agg_count_7911186439813521") as "h10_to_10_30",
    sum("thoughtful"."_virt_agg_count_7914196801151291") as "h11_30_to_12",
    sum("thoughtful"."_virt_agg_count_9208026921280603") as "h11_to_11_30",
    sum("thoughtful"."_virt_agg_count_6874762517186813") as "h12_to_12_30",
    sum("thoughtful"."_virt_agg_count_6255323248253146") as "h8_30_to_9",
    sum("thoughtful"."_virt_agg_count_2219364601882723") as "h9_30_to_10",
    sum("thoughtful"."_virt_agg_count_2910789853377884") as "h9_to_9_30"
FROM
    "thoughtful"
```

## Reference SQL (zquery log)

```sql
WITH 
thoughtful as (
SELECT
    count(CASE WHEN "store_sales_time_time"."T_HOUR" = 10 and "store_sales_time_time"."T_MINUTE" >= 0 and "store_sales_time_time"."T_MINUTE" < 30 THEN "store_sales_store_sales"."SS_ITEM_SK" ELSE NULL END) as "_virt_agg_count_7911186439813521",
    count(CASE WHEN "store_sales_time_time"."T_HOUR" = 10 and "store_sales_time_time"."T_MINUTE" >= 30 and "store_sales_time_time"."T_MINUTE" < 60 THEN "store_sales_store_sales"."SS_ITEM_SK" ELSE NULL END) as "_virt_agg_count_4915320083864949",
    count(CASE WHEN "store_sales_time_time"."T_HOUR" = 11 and "store_sales_time_time"."T_MINUTE" >= 0 and "store_sales_time_time"."T_MINUTE" < 30 THEN "store_sales_store_sales"."SS_ITEM_SK" ELSE NULL END) as "_virt_agg_count_9208026921280603",
    count(CASE WHEN "store_sales_time_time"."T_HOUR" = 11 and "store_sales_time_time"."T_MINUTE" >= 30 and "store_sales_time_time"."T_MINUTE" < 60 THEN "store_sales_store_sales"."SS_ITEM_SK" ELSE NULL END) as "_virt_agg_count_7914196801151291",
    count(CASE WHEN "store_sales_time_time"."T_HOUR" = 12 and "store_sales_time_time"."T_MINUTE" >= 0 and "store_sales_time_time"."T_MINUTE" < 30 THEN "store_sales_store_sales"."SS_ITEM_SK" ELSE NULL END) as "_virt_agg_count_6874762517186813",
    count(CASE WHEN "store_sales_time_time"."T_HOUR" = 8 and "store_sales_time_time"."T_MINUTE" >= 30 and "store_sales_time_time"."T_MINUTE" < 60 THEN "store_sales_store_sales"."SS_ITEM_SK" ELSE NULL END) as "_virt_agg_count_6255323248253146",
    count(CASE WHEN "store_sales_time_time"."T_HOUR" = 9 and "store_sales_time_time"."T_MINUTE" >= 0 and "store_sales_time_time"."T_MINUTE" < 30 THEN "store_sales_store_sales"."SS_ITEM_SK" ELSE NULL END) as "_virt_agg_count_2910789853377884",
    count(CASE WHEN "store_sales_time_time"."T_HOUR" = 9 and "store_sales_time_time"."T_MINUTE" >= 30 and "store_sales_time_time"."T_MINUTE" < 60 THEN "store_sales_store_sales"."SS_ITEM_SK" ELSE NULL END) as "_virt_agg_count_2219364601882723"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    INNER JOIN "memory"."time_dim" as "store_sales_time_time" on "store_sales_store_sales"."SS_SOLD_TIME_SK" = "store_sales_time_time"."T_TIME_SK"
    INNER JOIN "memory"."store" as "store_sales_store_store" on "store_sales_store_sales"."SS_STORE_SK" = "store_sales_store_store"."S_STORE_SK"
    INNER JOIN "memory"."household_demographics" as "store_sales_household_demographic_household_demographics" on "store_sales_store_sales"."SS_HDEMO_SK" = "store_sales_household_demographic_household_demographics"."HD_DEMO_SK"
WHERE
    "store_sales_store_store"."S_STORE_NAME" = 'ese' and "store_sales_time_time"."T_HOUR" in (8,9,10,11,12) and ( ( "store_sales_household_demographic_household_demographics"."HD_DEP_COUNT" = 4 and "store_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT" <= 6 ) or ( "store_sales_household_demographic_household_demographics"."HD_DEP_COUNT" = 2 and "store_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT" <= 4 ) or ( "store_sales_household_demographic_household_demographics"."HD_DEP_COUNT" = 0 and "store_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT" <= 2 ) )

GROUP BY
    "store_sales_store_sales"."SS_TICKET_NUMBER")
SELECT
    sum("thoughtful"."_virt_agg_count_6255323248253146") as "h8_30_to_9",
    sum("thoughtful"."_virt_agg_count_2910789853377884") as "h9_to_9_30",
    sum("thoughtful"."_virt_agg_count_2219364601882723") as "h9_30_to_10",
    sum("thoughtful"."_virt_agg_count_7911186439813521") as "h10_to_10_30",
    sum("thoughtful"."_virt_agg_count_4915320083864949") as "h10_30_to_11",
    sum("thoughtful"."_virt_agg_count_9208026921280603") as "h11_to_11_30",
    sum("thoughtful"."_virt_agg_count_7914196801151291") as "h11_30_to_12",
    sum("thoughtful"."_virt_agg_count_6874762517186813") as "h12_to_12_30"
FROM
    "thoughtful"
```
