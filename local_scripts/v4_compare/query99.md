# Query 99

**Status:** `exec_fail`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | FAILED |
| reference execution | OK (90 rows) |

## Result comparison

_at least one side did not produce rows._

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 3774 | 55 | — |
| reference | 2597 | 28 | 45.78 ms |
| v4 / ref | 1.45x | 1.96x | — |

## Preql

```
#For catalog sales, create a report showing the counts of orders shipped within 30 days, from 31 to 60 days, from
#61 to 90 days, from 91 to 120 days and over 120 days within a given year, grouped by warehouse, call center
# and shipping mode.
import catalog_sales;

auto warehouse_short_name <- substring(warehouse.name, 1, 20);

# we can statically evaluate the case statement to prune to
def catalog_in_range(start, end) -> case
    when start is null then count(row_counter ? days_to_ship <= end)
    when end is not null then count(row_counter ? days_to_ship > start and days_to_ship <= end)
    else count(row_counter ? days_to_ship > start)
end;

where
    ship_date.month_seq between 1200 and 1211
    and order_number is not null
    and call_center.id is not null
    and warehouse.id is not null
    and ship_mode.id is not null
select
    warehouse_short_name,
    ship_mode.type,
    lower(call_center.name) as cc_name_lower,
    @catalog_in_range(null, 30) as less_than_30_days,
    @catalog_in_range(30, 60) as between_31_and_60_days,
    @catalog_in_range(60, 90) as between_61_and_90_days,
    @catalog_in_range(90, 120) as between_91_and_120_days,
    @catalog_in_range(120, null) as over_120_days,
order by
    warehouse_short_name asc nulls first,
    ship_mode.type asc nulls first,
    cc_name_lower asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
questionable as (
SELECT
    "call_center_call_center"."CC_NAME" as "call_center_name",
    "ship_mode_ship_mode"."SM_TYPE" as "ship_mode_type",
    "warehouse_warehouse"."w_warehouse_name" as "warehouse_name",
    1 as "row_counter",
    cast("ship_date_date"."D_DATE" as date) as "ship_date_date",
    cast("sold_date_date"."D_DATE" as date) as "sold_date_date"
FROM
    "memory"."catalog_sales" as "catalog_sales"
    INNER JOIN "memory"."date_dim" as "ship_date_date" on "catalog_sales"."CS_SHIP_DATE_SK" = "ship_date_date"."D_DATE_SK"
    INNER JOIN "memory"."ship_mode" as "ship_mode_ship_mode" on "catalog_sales"."CS_SHIP_MODE_SK" = "ship_mode_ship_mode"."SM_SHIP_MODE_SK"
    LEFT OUTER JOIN "memory"."date_dim" as "sold_date_date" on "catalog_sales"."CS_SOLD_DATE_SK" = "sold_date_date"."D_DATE_SK"
    INNER JOIN "memory"."warehouse" as "warehouse_warehouse" on "catalog_sales"."CS_WAREHOUSE_SK" = "warehouse_warehouse"."w_warehouse_sk"
    INNER JOIN "memory"."call_center" as "call_center_call_center" on "catalog_sales"."CS_CALL_CENTER_SK" = "call_center_call_center"."CC_CALL_CENTER_SK"
WHERE
    "ship_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1211 and "catalog_sales"."CS_ORDER_NUMBER" is not null and "catalog_sales"."CS_CALL_CENTER_SK" is not null and "catalog_sales"."CS_WAREHOUSE_SK" is not null and "catalog_sales"."CS_SHIP_MODE_SK" is not null
),
yummy as (
SELECT
    "questionable"."ship_mode_type" as "ship_mode_type",
    LOWER("questionable"."call_center_name")  as "cc_name_lower",
    SUBSTRING("questionable"."warehouse_name",1,20) as "warehouse_short_name"
FROM
    "questionable"),
abundant as (
SELECT
    CASE WHEN date_diff('day', "questionable"."sold_date_date", "questionable"."ship_date_date") <= 30 THEN "questionable"."row_counter" ELSE NULL END as "_virt_filter_row_counter_5011928028596288",
    CASE WHEN date_diff('day', "questionable"."sold_date_date", "questionable"."ship_date_date") > 120 THEN "questionable"."row_counter" ELSE NULL END as "_virt_filter_row_counter_3600395140186427",
    CASE WHEN date_diff('day', "questionable"."sold_date_date", "questionable"."ship_date_date") > 30 and date_diff('day', "questionable"."sold_date_date", "questionable"."ship_date_date") <= 60 THEN "questionable"."row_counter" ELSE NULL END as "_virt_filter_row_counter_3995177617069933",
    CASE WHEN date_diff('day', "questionable"."sold_date_date", "questionable"."ship_date_date") > 60 and date_diff('day', "questionable"."sold_date_date", "questionable"."ship_date_date") <= 90 THEN "questionable"."row_counter" ELSE NULL END as "_virt_filter_row_counter_2542054096360490",
    CASE WHEN date_diff('day', "questionable"."sold_date_date", "questionable"."ship_date_date") > 90 and date_diff('day', "questionable"."sold_date_date", "questionable"."ship_date_date") <= 120 THEN "questionable"."row_counter" ELSE NULL END as "_virt_filter_row_counter_8267453838305074"
FROM
    "questionable")
SELECT
    count("abundant"."_virt_filter_row_counter_5011928028596288") as "less_than_30_days",
    count("abundant"."_virt_filter_row_counter_3995177617069933") as "between_31_and_60_days",
    count("abundant"."_virt_filter_row_counter_2542054096360490") as "between_61_and_90_days",
    count("abundant"."_virt_filter_row_counter_8267453838305074") as "between_91_and_120_days",
    count("abundant"."_virt_filter_row_counter_3600395140186427") as "over_120_days",
    "yummy"."cc_name_lower" as "cc_name_lower",
    "yummy"."ship_mode_type" as "ship_mode_type",
    "yummy"."warehouse_short_name" as "warehouse_short_name"
FROM
    "yummy"
GROUP BY
    6,
    7,
    8
ORDER BY 
    "yummy"."warehouse_short_name" asc nulls first,
    "yummy"."ship_mode_type" asc nulls first,
    "yummy"."cc_name_lower" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
SELECT
    SUBSTRING("warehouse_warehouse"."w_warehouse_name",1,20) as "warehouse_short_name",
    "ship_mode_ship_mode"."SM_TYPE" as "ship_mode_type",
    LOWER("call_center_call_center"."CC_NAME")  as "cc_name_lower",
    count(CASE WHEN date_diff('day', cast("sold_date_date"."D_DATE" as date), cast("ship_date_date"."D_DATE" as date)) <= 30 THEN 1 ELSE NULL END) as "less_than_30_days",
    count(CASE WHEN date_diff('day', cast("sold_date_date"."D_DATE" as date), cast("ship_date_date"."D_DATE" as date)) > 30 and date_diff('day', cast("sold_date_date"."D_DATE" as date), cast("ship_date_date"."D_DATE" as date)) <= 60 THEN 1 ELSE NULL END) as "between_31_and_60_days",
    count(CASE WHEN date_diff('day', cast("sold_date_date"."D_DATE" as date), cast("ship_date_date"."D_DATE" as date)) > 60 and date_diff('day', cast("sold_date_date"."D_DATE" as date), cast("ship_date_date"."D_DATE" as date)) <= 90 THEN 1 ELSE NULL END) as "between_61_and_90_days",
    count(CASE WHEN date_diff('day', cast("sold_date_date"."D_DATE" as date), cast("ship_date_date"."D_DATE" as date)) > 90 and date_diff('day', cast("sold_date_date"."D_DATE" as date), cast("ship_date_date"."D_DATE" as date)) <= 120 THEN 1 ELSE NULL END) as "between_91_and_120_days",
    count(CASE WHEN date_diff('day', cast("sold_date_date"."D_DATE" as date), cast("ship_date_date"."D_DATE" as date)) > 120 THEN 1 ELSE NULL END) as "over_120_days"
FROM
    "memory"."catalog_sales" as "catalog_sales"
    INNER JOIN "memory"."date_dim" as "ship_date_date" on "catalog_sales"."CS_SHIP_DATE_SK" = "ship_date_date"."D_DATE_SK"
    INNER JOIN "memory"."ship_mode" as "ship_mode_ship_mode" on "catalog_sales"."CS_SHIP_MODE_SK" = "ship_mode_ship_mode"."SM_SHIP_MODE_SK"
    LEFT OUTER JOIN "memory"."date_dim" as "sold_date_date" on "catalog_sales"."CS_SOLD_DATE_SK" = "sold_date_date"."D_DATE_SK"
    INNER JOIN "memory"."warehouse" as "warehouse_warehouse" on "catalog_sales"."CS_WAREHOUSE_SK" = "warehouse_warehouse"."w_warehouse_sk"
    INNER JOIN "memory"."call_center" as "call_center_call_center" on "catalog_sales"."CS_CALL_CENTER_SK" = "call_center_call_center"."CC_CALL_CENTER_SK"
WHERE
    "ship_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1211 and "catalog_sales"."CS_ORDER_NUMBER" is not null and "catalog_sales"."CS_CALL_CENTER_SK" is not null and "catalog_sales"."CS_WAREHOUSE_SK" is not null and "catalog_sales"."CS_SHIP_MODE_SK" is not null

GROUP BY
    1,
    2,
    3
ORDER BY 
    "warehouse_short_name" asc nulls first,
    "ship_mode_ship_mode"."SM_TYPE" asc nulls first,
    "cc_name_lower" asc nulls first
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
_duckdb.BinderException: Binder Error: Referenced table "abundant" not found!
Candidate tables: "yummy"

LINE 37:     count("abundant"."_virt_filter_row_counter_5011928028596288")...
                   ^
```
