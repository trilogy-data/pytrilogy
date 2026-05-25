# Query 99

**Status:** `exec_fail`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | FAILED |
| reference execution | OK (90 rows) |

## Result comparison

_at least one side did not produce rows._

## SQL size

| Source | Chars | Lines |
| --- | --- | --- |
| v4 | 6483 | 159 |
| reference | 2597 | 28 |
| v4 / ref | 2.50x | 5.68x |

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
    "warehouse_warehouse"."w_warehouse_name" as "warehouse_name",
    "warehouse_warehouse"."w_warehouse_sk" as "warehouse_id"
FROM
    "memory"."warehouse" as "warehouse_warehouse"),
cooperative as (
SELECT
    "sold_date_date"."D_DATE_SK" as "sold_date_id",
    cast("sold_date_date"."D_DATE" as date) as "sold_date_date"
FROM
    "memory"."date_dim" as "sold_date_date"),
thoughtful as (
SELECT
    "ship_mode_ship_mode"."SM_SHIP_MODE_SK" as "ship_mode_id",
    "ship_mode_ship_mode"."SM_TYPE" as "ship_mode_type"
FROM
    "memory"."ship_mode" as "ship_mode_ship_mode"),
cheerful as (
SELECT
    "ship_date_date"."D_DATE_SK" as "ship_date_id",
    "ship_date_date"."D_MONTH_SEQ" as "ship_date_month_seq",
    cast("ship_date_date"."D_DATE" as date) as "ship_date_date"
FROM
    "memory"."date_dim" as "ship_date_date"),
highfalutin as (
SELECT
    "catalog_sales"."CS_CALL_CENTER_SK" as "call_center_id",
    "catalog_sales"."CS_ORDER_NUMBER" as "order_number",
    "catalog_sales"."CS_SHIP_DATE_SK" as "ship_date_id",
    "catalog_sales"."CS_SHIP_MODE_SK" as "ship_mode_id",
    "catalog_sales"."CS_SOLD_DATE_SK" as "sold_date_id",
    "catalog_sales"."CS_WAREHOUSE_SK" as "warehouse_id",
    1 as "row_counter"
FROM
    "memory"."catalog_sales" as "catalog_sales"),
wakeful as (
SELECT
    "highfalutin"."call_center_id" as "call_center_id",
    "highfalutin"."order_number" as "order_number",
    "highfalutin"."row_counter" as "row_counter",
    "highfalutin"."ship_date_id" as "ship_date_id",
    "highfalutin"."ship_mode_id" as "ship_mode_id",
    "highfalutin"."sold_date_id" as "sold_date_id",
    "highfalutin"."warehouse_id" as "warehouse_id"
FROM
    "highfalutin"
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7),
quizzical as (
SELECT
    "call_center_call_center"."CC_CALL_CENTER_SK" as "call_center_id",
    "call_center_call_center"."CC_NAME" as "call_center_name"
FROM
    "memory"."call_center" as "call_center_call_center"),
abundant as (
SELECT
    "cheerful"."ship_date_date" as "ship_date_date",
    "cheerful"."ship_date_month_seq" as "ship_date_month_seq",
    "cooperative"."sold_date_date" as "sold_date_date",
    "questionable"."warehouse_name" as "warehouse_name",
    "quizzical"."call_center_name" as "call_center_name",
    "thoughtful"."ship_mode_type" as "ship_mode_type",
    "wakeful"."call_center_id" as "call_center_id",
    "wakeful"."order_number" as "order_number",
    "wakeful"."row_counter" as "row_counter",
    "wakeful"."ship_mode_id" as "ship_mode_id",
    "wakeful"."warehouse_id" as "warehouse_id"
FROM
    "wakeful"
    LEFT OUTER JOIN "cheerful" on "wakeful"."ship_date_id" = "cheerful"."ship_date_id"
    LEFT OUTER JOIN "thoughtful" on "wakeful"."ship_mode_id" = "thoughtful"."ship_mode_id"
    LEFT OUTER JOIN "cooperative" on "wakeful"."sold_date_id" = "cooperative"."sold_date_id"
    LEFT OUTER JOIN "questionable" on "wakeful"."warehouse_id" = "questionable"."warehouse_id"
    LEFT OUTER JOIN "quizzical" on "wakeful"."call_center_id" = "quizzical"."call_center_id"
WHERE
    "cheerful"."ship_date_month_seq" BETWEEN 1200 AND 1211 and "wakeful"."order_number" is not null and "wakeful"."call_center_id" is not null and "wakeful"."warehouse_id" is not null and "wakeful"."ship_mode_id" is not null

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
yummy as (
SELECT
    "abundant"."call_center_id" as "call_center_id",
    "abundant"."call_center_name" as "call_center_name",
    "abundant"."order_number" as "order_number",
    "abundant"."row_counter" as "row_counter",
    "abundant"."ship_date_date" as "ship_date_date",
    "abundant"."ship_date_month_seq" as "ship_date_month_seq",
    "abundant"."ship_mode_id" as "ship_mode_id",
    "abundant"."ship_mode_type" as "ship_mode_type",
    "abundant"."sold_date_date" as "sold_date_date",
    "abundant"."warehouse_id" as "warehouse_id",
    "abundant"."warehouse_name" as "warehouse_name",
    date_diff('day', "abundant"."sold_date_date", "abundant"."ship_date_date") as "days_to_ship"
FROM
    "abundant"),
juicy as (
SELECT
    CASE WHEN "yummy"."days_to_ship" <= 30 THEN "yummy"."row_counter" ELSE NULL END as "_virt_filter_row_counter_5011928028596288",
    CASE WHEN "yummy"."days_to_ship" > 120 THEN "yummy"."row_counter" ELSE NULL END as "_virt_filter_row_counter_3600395140186427",
    CASE WHEN "yummy"."days_to_ship" > 30 and "yummy"."days_to_ship" <= 60 THEN "yummy"."row_counter" ELSE NULL END as "_virt_filter_row_counter_3995177617069933",
    CASE WHEN "yummy"."days_to_ship" > 60 and "yummy"."days_to_ship" <= 90 THEN "yummy"."row_counter" ELSE NULL END as "_virt_filter_row_counter_2542054096360490",
    CASE WHEN "yummy"."days_to_ship" > 90 and "yummy"."days_to_ship" <= 120 THEN "yummy"."row_counter" ELSE NULL END as "_virt_filter_row_counter_8267453838305074"
FROM
    "yummy"),
uneven as (
SELECT
    "abundant"."call_center_id" as "call_center_id",
    "abundant"."call_center_name" as "call_center_name",
    "abundant"."order_number" as "order_number",
    "abundant"."row_counter" as "row_counter",
    "abundant"."ship_date_date" as "ship_date_date",
    "abundant"."ship_date_month_seq" as "ship_date_month_seq",
    "abundant"."ship_mode_id" as "ship_mode_id",
    "abundant"."ship_mode_type" as "ship_mode_type",
    "abundant"."sold_date_date" as "sold_date_date",
    "abundant"."warehouse_id" as "warehouse_id",
    "abundant"."warehouse_name" as "warehouse_name",
    LOWER("abundant"."call_center_name")  as "cc_name_lower",
    SUBSTRING("abundant"."warehouse_name",1,20) as "warehouse_short_name"
FROM
    "abundant")
SELECT
    count("juicy"."_virt_filter_row_counter_5011928028596288") as "less_than_30_days",
    count("juicy"."_virt_filter_row_counter_3995177617069933") as "between_31_and_60_days",
    count("juicy"."_virt_filter_row_counter_2542054096360490") as "between_61_and_90_days",
    count("juicy"."_virt_filter_row_counter_8267453838305074") as "between_91_and_120_days",
    count("juicy"."_virt_filter_row_counter_3600395140186427") as "over_120_days",
    "uneven"."cc_name_lower" as "cc_name_lower",
    "uneven"."ship_mode_type" as "ship_mode_type",
    "uneven"."warehouse_short_name" as "warehouse_short_name"
FROM
    "uneven"
GROUP BY
    6,
    7,
    8
ORDER BY 
    "uneven"."warehouse_short_name" asc nulls first,
    "uneven"."ship_mode_type" asc nulls first,
    "uneven"."cc_name_lower" asc nulls first
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
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 161, in run_one
    result.v4_rows = execute(con, v4_sql)
                     ~~~~~~~^^^^^^^^^^^^^
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 102, in execute
    cursor = con.execute(sql)
_duckdb.BinderException: Binder Error: Referenced table "juicy" not found!
Candidate tables: "uneven"

LINE 141:     count("juicy"."_virt_filter_row_counter_5011928028596288") as...
                    ^
```
