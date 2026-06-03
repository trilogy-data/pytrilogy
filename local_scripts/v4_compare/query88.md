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
| v4 | 4040 | 32 | 5.52 ms |
| reference | 4040 | 32 | 4.74 ms |
| v4 / ref | 1.00x | 1.00x | 1.16x |

## Preql

```
import physical_sales as physical_sales;

def bucket_count(h, min_low, min_high) -> sum(
    count(
        physical_sales.item.id
            ? physical_sales.time.hour = h
and physical_sales.time.minute >= min_low
and physical_sales.time.minute < min_high
    )
        by physical_sales.ticket_number
);

where
    physical_sales.store.name = 'ese'
    and physical_sales.time.hour in (8, 9, 10, 11, 12)
    and (
        (
            physical_sales.household_demographic.dependent_count = 4
            and physical_sales.household_demographic.vehicle_count <= 6
        )
        or (
            physical_sales.household_demographic.dependent_count = 2
            and physical_sales.household_demographic.vehicle_count <= 4
        )
        or (
            physical_sales.household_demographic.dependent_count = 0
            and physical_sales.household_demographic.vehicle_count <= 2
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
    count(CASE WHEN "physical_sales_time_time"."T_HOUR" = 10 and "physical_sales_time_time"."T_MINUTE" >= 0 and "physical_sales_time_time"."T_MINUTE" < 30 THEN "physical_sales_store_sales"."SS_ITEM_SK" ELSE NULL END) as "_virt_agg_count_6212958414412857",
    count(CASE WHEN "physical_sales_time_time"."T_HOUR" = 10 and "physical_sales_time_time"."T_MINUTE" >= 30 and "physical_sales_time_time"."T_MINUTE" < 60 THEN "physical_sales_store_sales"."SS_ITEM_SK" ELSE NULL END) as "_virt_agg_count_2564949507987795",
    count(CASE WHEN "physical_sales_time_time"."T_HOUR" = 11 and "physical_sales_time_time"."T_MINUTE" >= 0 and "physical_sales_time_time"."T_MINUTE" < 30 THEN "physical_sales_store_sales"."SS_ITEM_SK" ELSE NULL END) as "_virt_agg_count_4724975346229381",
    count(CASE WHEN "physical_sales_time_time"."T_HOUR" = 11 and "physical_sales_time_time"."T_MINUTE" >= 30 and "physical_sales_time_time"."T_MINUTE" < 60 THEN "physical_sales_store_sales"."SS_ITEM_SK" ELSE NULL END) as "_virt_agg_count_7932453718511052",
    count(CASE WHEN "physical_sales_time_time"."T_HOUR" = 12 and "physical_sales_time_time"."T_MINUTE" >= 0 and "physical_sales_time_time"."T_MINUTE" < 30 THEN "physical_sales_store_sales"."SS_ITEM_SK" ELSE NULL END) as "_virt_agg_count_2820652540022982",
    count(CASE WHEN "physical_sales_time_time"."T_HOUR" = 8 and "physical_sales_time_time"."T_MINUTE" >= 30 and "physical_sales_time_time"."T_MINUTE" < 60 THEN "physical_sales_store_sales"."SS_ITEM_SK" ELSE NULL END) as "_virt_agg_count_7631409747863814",
    count(CASE WHEN "physical_sales_time_time"."T_HOUR" = 9 and "physical_sales_time_time"."T_MINUTE" >= 0 and "physical_sales_time_time"."T_MINUTE" < 30 THEN "physical_sales_store_sales"."SS_ITEM_SK" ELSE NULL END) as "_virt_agg_count_4000376457083059",
    count(CASE WHEN "physical_sales_time_time"."T_HOUR" = 9 and "physical_sales_time_time"."T_MINUTE" >= 30 and "physical_sales_time_time"."T_MINUTE" < 60 THEN "physical_sales_store_sales"."SS_ITEM_SK" ELSE NULL END) as "_virt_agg_count_8039435418428577"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."time_dim" as "physical_sales_time_time" on "physical_sales_store_sales"."SS_SOLD_TIME_SK" = "physical_sales_time_time"."T_TIME_SK"
    INNER JOIN "memory"."store" as "physical_sales_store_store" on "physical_sales_store_sales"."SS_STORE_SK" = "physical_sales_store_store"."S_STORE_SK"
    INNER JOIN "memory"."household_demographics" as "physical_sales_household_demographic_household_demographics" on "physical_sales_store_sales"."SS_HDEMO_SK" = "physical_sales_household_demographic_household_demographics"."HD_DEMO_SK"
WHERE
    "physical_sales_store_store"."S_STORE_NAME" = 'ese' and "physical_sales_time_time"."T_HOUR" in (8,9,10,11,12) and ( ( "physical_sales_household_demographic_household_demographics"."HD_DEP_COUNT" = 4 and "physical_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT" <= 6 ) or ( "physical_sales_household_demographic_household_demographics"."HD_DEP_COUNT" = 2 and "physical_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT" <= 4 ) or ( "physical_sales_household_demographic_household_demographics"."HD_DEP_COUNT" = 0 and "physical_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT" <= 2 ) )

GROUP BY
    "physical_sales_store_sales"."SS_TICKET_NUMBER")
SELECT
    sum("thoughtful"."_virt_agg_count_7631409747863814") as "h8_30_to_9",
    sum("thoughtful"."_virt_agg_count_4000376457083059") as "h9_to_9_30",
    sum("thoughtful"."_virt_agg_count_8039435418428577") as "h9_30_to_10",
    sum("thoughtful"."_virt_agg_count_6212958414412857") as "h10_to_10_30",
    sum("thoughtful"."_virt_agg_count_2564949507987795") as "h10_30_to_11",
    sum("thoughtful"."_virt_agg_count_4724975346229381") as "h11_to_11_30",
    sum("thoughtful"."_virt_agg_count_7932453718511052") as "h11_30_to_12",
    sum("thoughtful"."_virt_agg_count_2820652540022982") as "h12_to_12_30"
FROM
    "thoughtful"
```

## Reference SQL (zquery log)

```sql
WITH 
thoughtful as (
SELECT
    count(CASE WHEN "physical_sales_time_time"."T_HOUR" = 10 and "physical_sales_time_time"."T_MINUTE" >= 0 and "physical_sales_time_time"."T_MINUTE" < 30 THEN "physical_sales_store_sales"."SS_ITEM_SK" ELSE NULL END) as "_virt_agg_count_6212958414412857",
    count(CASE WHEN "physical_sales_time_time"."T_HOUR" = 10 and "physical_sales_time_time"."T_MINUTE" >= 30 and "physical_sales_time_time"."T_MINUTE" < 60 THEN "physical_sales_store_sales"."SS_ITEM_SK" ELSE NULL END) as "_virt_agg_count_2564949507987795",
    count(CASE WHEN "physical_sales_time_time"."T_HOUR" = 11 and "physical_sales_time_time"."T_MINUTE" >= 0 and "physical_sales_time_time"."T_MINUTE" < 30 THEN "physical_sales_store_sales"."SS_ITEM_SK" ELSE NULL END) as "_virt_agg_count_4724975346229381",
    count(CASE WHEN "physical_sales_time_time"."T_HOUR" = 11 and "physical_sales_time_time"."T_MINUTE" >= 30 and "physical_sales_time_time"."T_MINUTE" < 60 THEN "physical_sales_store_sales"."SS_ITEM_SK" ELSE NULL END) as "_virt_agg_count_7932453718511052",
    count(CASE WHEN "physical_sales_time_time"."T_HOUR" = 12 and "physical_sales_time_time"."T_MINUTE" >= 0 and "physical_sales_time_time"."T_MINUTE" < 30 THEN "physical_sales_store_sales"."SS_ITEM_SK" ELSE NULL END) as "_virt_agg_count_2820652540022982",
    count(CASE WHEN "physical_sales_time_time"."T_HOUR" = 8 and "physical_sales_time_time"."T_MINUTE" >= 30 and "physical_sales_time_time"."T_MINUTE" < 60 THEN "physical_sales_store_sales"."SS_ITEM_SK" ELSE NULL END) as "_virt_agg_count_7631409747863814",
    count(CASE WHEN "physical_sales_time_time"."T_HOUR" = 9 and "physical_sales_time_time"."T_MINUTE" >= 0 and "physical_sales_time_time"."T_MINUTE" < 30 THEN "physical_sales_store_sales"."SS_ITEM_SK" ELSE NULL END) as "_virt_agg_count_4000376457083059",
    count(CASE WHEN "physical_sales_time_time"."T_HOUR" = 9 and "physical_sales_time_time"."T_MINUTE" >= 30 and "physical_sales_time_time"."T_MINUTE" < 60 THEN "physical_sales_store_sales"."SS_ITEM_SK" ELSE NULL END) as "_virt_agg_count_8039435418428577"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."time_dim" as "physical_sales_time_time" on "physical_sales_store_sales"."SS_SOLD_TIME_SK" = "physical_sales_time_time"."T_TIME_SK"
    INNER JOIN "memory"."store" as "physical_sales_store_store" on "physical_sales_store_sales"."SS_STORE_SK" = "physical_sales_store_store"."S_STORE_SK"
    INNER JOIN "memory"."household_demographics" as "physical_sales_household_demographic_household_demographics" on "physical_sales_store_sales"."SS_HDEMO_SK" = "physical_sales_household_demographic_household_demographics"."HD_DEMO_SK"
WHERE
    "physical_sales_store_store"."S_STORE_NAME" = 'ese' and "physical_sales_time_time"."T_HOUR" in (8,9,10,11,12) and ( ( "physical_sales_household_demographic_household_demographics"."HD_DEP_COUNT" = 4 and "physical_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT" <= 6 ) or ( "physical_sales_household_demographic_household_demographics"."HD_DEP_COUNT" = 2 and "physical_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT" <= 4 ) or ( "physical_sales_household_demographic_household_demographics"."HD_DEP_COUNT" = 0 and "physical_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT" <= 2 ) )

GROUP BY
    "physical_sales_store_sales"."SS_TICKET_NUMBER")
SELECT
    sum("thoughtful"."_virt_agg_count_7631409747863814") as "h8_30_to_9",
    sum("thoughtful"."_virt_agg_count_4000376457083059") as "h9_to_9_30",
    sum("thoughtful"."_virt_agg_count_8039435418428577") as "h9_30_to_10",
    sum("thoughtful"."_virt_agg_count_6212958414412857") as "h10_to_10_30",
    sum("thoughtful"."_virt_agg_count_2564949507987795") as "h10_30_to_11",
    sum("thoughtful"."_virt_agg_count_4724975346229381") as "h11_to_11_30",
    sum("thoughtful"."_virt_agg_count_7932453718511052") as "h11_30_to_12",
    sum("thoughtful"."_virt_agg_count_2820652540022982") as "h12_to_12_30"
FROM
    "thoughtful"
```
