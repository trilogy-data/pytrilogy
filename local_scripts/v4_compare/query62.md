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
| v4 | 2831 | 40 | 53.23 ms |
| reference | 2162 | 27 | 56.20 ms |
| v4 / ref | 1.31x | 1.48x | 0.95x |

## Preql

```
import web_sales as ws;
property days_to_ship <- ws.ship_date.id - ws.date.id;
WHERE
    ws.ship_date.month_seq between 1200 and  1211 and ws.warehouse.id is not null and ws.ship_mode.id is not null and ws.web_site.id is not null
SELECT
    substring(ws.warehouse.name,1,20) -> w_substr,
    ws.ship_mode.type,
    ws.web_site.name,
    coalesce(sum(filter ws.row_counter where days_to_ship <= 30),0) -> days_30,
    coalesce(sum(filter ws.row_counter where days_to_ship > 30 and days_to_ship <= 60),0) -> days_31_60,
    coalesce(sum(filter ws.row_counter where days_to_ship > 60 and days_to_ship <= 90),0) -> days_61_90,
    coalesce(sum(filter ws.row_counter where days_to_ship > 90 and days_to_ship <= 120),0) -> days_91_120,
    coalesce(sum(filter ws.row_counter where days_to_ship > 120),0) -> days_120_plus,
ORDER BY
    w_substr asc nulls first,
    ws.ship_mode.type asc nulls first,
    ws.web_site.name asc nulls first
LIMIT 100
;
```

## v4 generated SQL

```sql
WITH 
cooperative as (
SELECT
    "ws_ship_mode_ship_mode"."SM_TYPE" as "ws_ship_mode_type",
    "ws_web_site_web_site"."web_name" as "ws_web_site_name",
    SUBSTRING("ws_warehouse_warehouse"."w_warehouse_name",1,20) as "w_substr",
    sum(CASE WHEN "ws_web_sales"."WS_SHIP_DATE_SK" - "ws_web_sales"."WS_SOLD_DATE_SK" <= 30 THEN 1 ELSE NULL END) as "_virt_agg_sum_532447299563042",
    sum(CASE WHEN "ws_web_sales"."WS_SHIP_DATE_SK" - "ws_web_sales"."WS_SOLD_DATE_SK" > 120 THEN 1 ELSE NULL END) as "_virt_agg_sum_1591163926846168",
    sum(CASE WHEN "ws_web_sales"."WS_SHIP_DATE_SK" - "ws_web_sales"."WS_SOLD_DATE_SK" > 30 and "ws_web_sales"."WS_SHIP_DATE_SK" - "ws_web_sales"."WS_SOLD_DATE_SK" <= 60 THEN 1 ELSE NULL END) as "_virt_agg_sum_3972236993249717",
    sum(CASE WHEN "ws_web_sales"."WS_SHIP_DATE_SK" - "ws_web_sales"."WS_SOLD_DATE_SK" > 60 and "ws_web_sales"."WS_SHIP_DATE_SK" - "ws_web_sales"."WS_SOLD_DATE_SK" <= 90 THEN 1 ELSE NULL END) as "_virt_agg_sum_1757493792246964",
    sum(CASE WHEN "ws_web_sales"."WS_SHIP_DATE_SK" - "ws_web_sales"."WS_SOLD_DATE_SK" > 90 and "ws_web_sales"."WS_SHIP_DATE_SK" - "ws_web_sales"."WS_SOLD_DATE_SK" <= 120 THEN 1 ELSE NULL END) as "_virt_agg_sum_9896727465284234"
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
    3)
SELECT
    "cooperative"."w_substr" as "w_substr",
    "cooperative"."ws_ship_mode_type" as "ws_ship_mode_type",
    "cooperative"."ws_web_site_name" as "ws_web_site_name",
    coalesce("cooperative"."_virt_agg_sum_532447299563042",0) as "days_30",
    coalesce("cooperative"."_virt_agg_sum_3972236993249717",0) as "days_31_60",
    coalesce("cooperative"."_virt_agg_sum_1757493792246964",0) as "days_61_90",
    coalesce("cooperative"."_virt_agg_sum_9896727465284234",0) as "days_91_120",
    coalesce("cooperative"."_virt_agg_sum_1591163926846168",0) as "days_120_plus"
FROM
    "cooperative"
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
    coalesce(sum(CASE WHEN "ws_web_sales"."WS_SHIP_DATE_SK" - "ws_web_sales"."WS_SOLD_DATE_SK" <= 30 THEN 1 ELSE NULL END),0) as "days_30",
    coalesce(sum(CASE WHEN "ws_web_sales"."WS_SHIP_DATE_SK" - "ws_web_sales"."WS_SOLD_DATE_SK" > 30 and "ws_web_sales"."WS_SHIP_DATE_SK" - "ws_web_sales"."WS_SOLD_DATE_SK" <= 60 THEN 1 ELSE NULL END),0) as "days_31_60",
    coalesce(sum(CASE WHEN "ws_web_sales"."WS_SHIP_DATE_SK" - "ws_web_sales"."WS_SOLD_DATE_SK" > 60 and "ws_web_sales"."WS_SHIP_DATE_SK" - "ws_web_sales"."WS_SOLD_DATE_SK" <= 90 THEN 1 ELSE NULL END),0) as "days_61_90",
    coalesce(sum(CASE WHEN "ws_web_sales"."WS_SHIP_DATE_SK" - "ws_web_sales"."WS_SOLD_DATE_SK" > 90 and "ws_web_sales"."WS_SHIP_DATE_SK" - "ws_web_sales"."WS_SOLD_DATE_SK" <= 120 THEN 1 ELSE NULL END),0) as "days_91_120",
    coalesce(sum(CASE WHEN "ws_web_sales"."WS_SHIP_DATE_SK" - "ws_web_sales"."WS_SOLD_DATE_SK" > 120 THEN 1 ELSE NULL END),0) as "days_120_plus"
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
