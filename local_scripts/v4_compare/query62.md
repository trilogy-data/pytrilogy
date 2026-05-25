# Query 62

**Status:** `exec_fail`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | FAILED |
| reference execution | OK (100 rows) |

## Result comparison

_at least one side did not produce rows._

## SQL size

| Source | Chars | Lines |
| --- | --- | --- |
| v4 | 4640 | 120 |
| reference | 2102 | 42 |
| v4 / ref | 2.21x | 2.86x |

## Preql

```
import web_sales as ws;

auto days_to_ship <- ws.ship_date.id - ws.date.id;

where
    ws.ship_date.month_seq between 1200 and 1211
    and ws.warehouse.id is not null
    and ws.ship_mode.id is not null
    and ws.web_site.id is not null
select
    substring(ws.warehouse.name, 1, 20) as w_substr,
    ws.ship_mode.type,
    ws.web_site.name,
    sum(
            case
                when days_to_ship <= 30 then 1
                else 0
            end
        ) as days_30,
    sum(
            case
                when days_to_ship > 30 and days_to_ship <= 60 then 1
                else 0
            end
        ) as days_31_60,
    sum(
            case
                when days_to_ship > 60 and days_to_ship <= 90 then 1
                else 0
            end
        ) as days_61_90,
    sum(
            case
                when days_to_ship > 90 and days_to_ship <= 120 then 1
                else 0
            end
        ) as days_91_120,
    sum(
            case
                when days_to_ship > 120 then 1
                else 0
            end
        ) as days_120_plus,
order by
    w_substr asc nulls first,
    ws.ship_mode.type asc nulls first,
    ws.web_site.name asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
thoughtful as (
SELECT
    "ws_web_site_web_site"."web_name" as "ws_web_site_name",
    "ws_web_site_web_site"."web_site_sk" as "ws_web_site_id"
FROM
    "memory"."web_site" as "ws_web_site_web_site"),
cheerful as (
SELECT
    "ws_web_sales"."WS_SHIP_DATE_SK" as "ws_ship_date_id",
    "ws_web_sales"."WS_SHIP_MODE_SK" as "ws_ship_mode_id",
    "ws_web_sales"."WS_SOLD_DATE_SK" as "ws_date_id",
    "ws_web_sales"."WS_WAREHOUSE_SK" as "ws_warehouse_id",
    "ws_web_sales"."WS_WEB_SITE_SK" as "ws_web_site_id"
FROM
    "memory"."web_sales" as "ws_web_sales"
WHERE
    "ws_web_sales"."WS_WAREHOUSE_SK" is not null and "ws_web_sales"."WS_SHIP_MODE_SK" is not null and "ws_web_sales"."WS_WEB_SITE_SK" is not null
),
wakeful as (
SELECT
    "ws_warehouse_warehouse"."w_warehouse_name" as "ws_warehouse_name",
    "ws_warehouse_warehouse"."w_warehouse_sk" as "ws_warehouse_id"
FROM
    "memory"."warehouse" as "ws_warehouse_warehouse"),
highfalutin as (
SELECT
    "ws_ship_mode_ship_mode"."SM_SHIP_MODE_SK" as "ws_ship_mode_id",
    "ws_ship_mode_ship_mode"."SM_TYPE" as "ws_ship_mode_type"
FROM
    "memory"."ship_mode" as "ws_ship_mode_ship_mode"),
quizzical as (
SELECT
    "ws_ship_date_date"."D_DATE_SK" as "ws_ship_date_id",
    "ws_ship_date_date"."D_MONTH_SEQ" as "ws_ship_date_month_seq"
FROM
    "memory"."date_dim" as "ws_ship_date_date"),
cooperative as (
SELECT
    "cheerful"."ws_date_id" as "ws_date_id",
    "cheerful"."ws_ship_date_id" as "ws_ship_date_id",
    "cheerful"."ws_ship_mode_id" as "ws_ship_mode_id",
    "cheerful"."ws_warehouse_id" as "ws_warehouse_id",
    "cheerful"."ws_web_site_id" as "ws_web_site_id",
    "highfalutin"."ws_ship_mode_type" as "ws_ship_mode_type",
    "quizzical"."ws_ship_date_month_seq" as "ws_ship_date_month_seq",
    "thoughtful"."ws_web_site_name" as "ws_web_site_name",
    "wakeful"."ws_warehouse_name" as "ws_warehouse_name"
FROM
    "cheerful"
    LEFT OUTER JOIN "thoughtful" on "cheerful"."ws_web_site_id" = "thoughtful"."ws_web_site_id"
    LEFT OUTER JOIN "quizzical" on "cheerful"."ws_ship_date_id" = "quizzical"."ws_ship_date_id"
    LEFT OUTER JOIN "highfalutin" on "cheerful"."ws_ship_mode_id" = "highfalutin"."ws_ship_mode_id"
    LEFT OUTER JOIN "wakeful" on "cheerful"."ws_warehouse_id" = "wakeful"."ws_warehouse_id"
WHERE
    "quizzical"."ws_ship_date_month_seq" BETWEEN 1200 AND 1211
),
abundant as (
SELECT
    "cooperative"."ws_date_id" as "ws_date_id",
    "cooperative"."ws_ship_date_id" as "ws_ship_date_id",
    "cooperative"."ws_ship_date_month_seq" as "ws_ship_date_month_seq",
    "cooperative"."ws_ship_mode_id" as "ws_ship_mode_id",
    "cooperative"."ws_ship_mode_type" as "ws_ship_mode_type",
    "cooperative"."ws_warehouse_id" as "ws_warehouse_id",
    "cooperative"."ws_warehouse_name" as "ws_warehouse_name",
    "cooperative"."ws_web_site_id" as "ws_web_site_id",
    "cooperative"."ws_web_site_name" as "ws_web_site_name",
    SUBSTRING("cooperative"."ws_warehouse_name",1,20) as "w_substr"
FROM
    "cooperative"),
questionable as (
SELECT
    "cooperative"."ws_date_id" as "ws_date_id",
    "cooperative"."ws_ship_date_id" - "cooperative"."ws_date_id" as "days_to_ship",
    "cooperative"."ws_ship_date_id" as "ws_ship_date_id",
    "cooperative"."ws_ship_date_month_seq" as "ws_ship_date_month_seq",
    "cooperative"."ws_ship_mode_id" as "ws_ship_mode_id",
    "cooperative"."ws_ship_mode_type" as "ws_ship_mode_type",
    "cooperative"."ws_warehouse_id" as "ws_warehouse_id",
    "cooperative"."ws_warehouse_name" as "ws_warehouse_name",
    "cooperative"."ws_web_site_id" as "ws_web_site_id",
    "cooperative"."ws_web_site_name" as "ws_web_site_name"
FROM
    "cooperative")
SELECT
    sum(CASE
	WHEN "questionable"."days_to_ship" <= 30 THEN 1
	ELSE 0
	END) as "days_30",
    sum(CASE
	WHEN "questionable"."days_to_ship" > 30 and "questionable"."days_to_ship" <= 60 THEN 1
	ELSE 0
	END) as "days_31_60",
    sum(CASE
	WHEN "questionable"."days_to_ship" > 60 and "questionable"."days_to_ship" <= 90 THEN 1
	ELSE 0
	END) as "days_61_90",
    sum(CASE
	WHEN "questionable"."days_to_ship" > 90 and "questionable"."days_to_ship" <= 120 THEN 1
	ELSE 0
	END) as "days_91_120",
    sum(CASE
	WHEN "questionable"."days_to_ship" > 120 THEN 1
	ELSE 0
	END) as "days_120_plus",
    "abundant"."w_substr" as "w_substr",
    "abundant"."ws_web_site_name" as "ws_web_site_name",
    "abundant"."ws_ship_mode_type" as "ws_ship_mode_type"
FROM
    "abundant"
GROUP BY
    6,
    7,
    8
ORDER BY 
    "abundant"."w_substr" asc nulls first,
    "abundant"."ws_ship_mode_type" asc nulls first,
    "abundant"."ws_web_site_name" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
SELECT
    SUBSTRING("ws_warehouse_warehouse"."w_warehouse_name",1,20) as "w_substr",
    "ws_ship_mode_ship_mode"."SM_TYPE" as "ws_ship_mode_type",
    "ws_web_site_web_site"."web_name" as "ws_web_site_name",
    sum(CASE
	WHEN "ws_web_sales"."WS_SHIP_DATE_SK" - "ws_web_sales"."WS_SOLD_DATE_SK" <= 30 THEN 1
	ELSE 0
	END) as "days_30",
    sum(CASE
	WHEN "ws_web_sales"."WS_SHIP_DATE_SK" - "ws_web_sales"."WS_SOLD_DATE_SK" > 30 and "ws_web_sales"."WS_SHIP_DATE_SK" - "ws_web_sales"."WS_SOLD_DATE_SK" <= 60 THEN 1
	ELSE 0
	END) as "days_31_60",
    sum(CASE
	WHEN "ws_web_sales"."WS_SHIP_DATE_SK" - "ws_web_sales"."WS_SOLD_DATE_SK" > 60 and "ws_web_sales"."WS_SHIP_DATE_SK" - "ws_web_sales"."WS_SOLD_DATE_SK" <= 90 THEN 1
	ELSE 0
	END) as "days_61_90",
    sum(CASE
	WHEN "ws_web_sales"."WS_SHIP_DATE_SK" - "ws_web_sales"."WS_SOLD_DATE_SK" > 90 and "ws_web_sales"."WS_SHIP_DATE_SK" - "ws_web_sales"."WS_SOLD_DATE_SK" <= 120 THEN 1
	ELSE 0
	END) as "days_91_120",
    sum(CASE
	WHEN "ws_web_sales"."WS_SHIP_DATE_SK" - "ws_web_sales"."WS_SOLD_DATE_SK" > 120 THEN 1
	ELSE 0
	END) as "days_120_plus"
FROM
    "memory"."web_sales" as "ws_web_sales"
    INNER JOIN "memory"."web_site" as "ws_web_site_web_site" on "ws_web_sales"."WS_WEB_SITE_SK" = "ws_web_site_web_site"."web_site_sk"
    INNER JOIN "memory"."date_dim" as "ws_ship_date_date" on "ws_web_sales"."WS_SHIP_DATE_SK" = "ws_ship_date_date"."D_DATE_SK"
    INNER JOIN "memory"."ship_mode" as "ws_ship_mode_ship_mode" on "ws_web_sales"."WS_SHIP_MODE_SK" = "ws_ship_mode_ship_mode"."SM_SHIP_MODE_SK"
    INNER JOIN "memory"."warehouse" as "ws_warehouse_warehouse" on "ws_web_sales"."WS_WAREHOUSE_SK" = "ws_warehouse_warehouse"."w_warehouse_sk"
WHERE
    "ws_ship_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1211 and "ws_web_sales"."WS_WAREHOUSE_SK" is not null and "ws_web_sales"."WS_SHIP_MODE_SK" is not null and "ws_web_sales"."WS_WEB_SITE_SK" is not null

GROUP BY
    1,
    2,
    3
ORDER BY 
    "w_substr" asc nulls first,
    "ws_ship_mode_ship_mode"."SM_TYPE" asc nulls first,
    "ws_web_site_web_site"."web_name" asc nulls first
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
_duckdb.BinderException: Binder Error: Referenced table "questionable" not found!
Candidate tables: "abundant"

LINE 88: 	WHEN "questionable"."days_to_ship" <= 30 THEN 1
              ^
```
