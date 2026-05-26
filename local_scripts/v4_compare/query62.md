# Query 62

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (100 rows) |
| reference execution | OK (100 rows) |
| results identical | YES |

## Result comparison

v4 rows: 100 (100 distinct)
ref rows: 100 (100 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 2112 | 51 | 34.58 ms |
| reference | 2102 | 42 | 32.92 ms |
| v4 / ref | 1.00x | 1.21x | 1.05x |

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
cooperative as (
SELECT
    "ws_ship_mode_ship_mode"."SM_TYPE" as "ws_ship_mode_type",
    "ws_web_sales"."WS_SHIP_DATE_SK" - "ws_web_sales"."WS_SOLD_DATE_SK" as "days_to_ship",
    "ws_web_site_web_site"."web_name" as "ws_web_site_name",
    SUBSTRING("ws_warehouse_warehouse"."w_warehouse_name",1,20) as "w_substr"
FROM
    "memory"."web_sales" as "ws_web_sales"
    INNER JOIN "memory"."web_site" as "ws_web_site_web_site" on "ws_web_sales"."WS_WEB_SITE_SK" = "ws_web_site_web_site"."web_site_sk"
    INNER JOIN "memory"."date_dim" as "ws_ship_date_date" on "ws_web_sales"."WS_SHIP_DATE_SK" = "ws_ship_date_date"."D_DATE_SK"
    INNER JOIN "memory"."ship_mode" as "ws_ship_mode_ship_mode" on "ws_web_sales"."WS_SHIP_MODE_SK" = "ws_ship_mode_ship_mode"."SM_SHIP_MODE_SK"
    INNER JOIN "memory"."warehouse" as "ws_warehouse_warehouse" on "ws_web_sales"."WS_WAREHOUSE_SK" = "ws_warehouse_warehouse"."w_warehouse_sk"
WHERE
    "ws_ship_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1211 and "ws_web_sales"."WS_WAREHOUSE_SK" is not null and "ws_web_sales"."WS_SHIP_MODE_SK" is not null and "ws_web_sales"."WS_WEB_SITE_SK" is not null
)
SELECT
    sum(CASE
	WHEN "cooperative"."days_to_ship" <= 30 THEN 1
	ELSE 0
	END) as "days_30",
    sum(CASE
	WHEN "cooperative"."days_to_ship" > 30 and "cooperative"."days_to_ship" <= 60 THEN 1
	ELSE 0
	END) as "days_31_60",
    sum(CASE
	WHEN "cooperative"."days_to_ship" > 60 and "cooperative"."days_to_ship" <= 90 THEN 1
	ELSE 0
	END) as "days_61_90",
    sum(CASE
	WHEN "cooperative"."days_to_ship" > 90 and "cooperative"."days_to_ship" <= 120 THEN 1
	ELSE 0
	END) as "days_91_120",
    sum(CASE
	WHEN "cooperative"."days_to_ship" > 120 THEN 1
	ELSE 0
	END) as "days_120_plus",
    "cooperative"."ws_web_site_name" as "ws_web_site_name",
    "cooperative"."ws_ship_mode_type" as "ws_ship_mode_type",
    "cooperative"."w_substr" as "w_substr"
FROM
    "cooperative"
GROUP BY
    6,
    7,
    8
ORDER BY 
    "cooperative"."w_substr" asc nulls first,
    "cooperative"."ws_ship_mode_type" asc nulls first,
    "cooperative"."ws_web_site_name" asc nulls first
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
