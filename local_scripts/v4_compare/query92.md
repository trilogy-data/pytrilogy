# Query 92

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
| v4 | 1471 | 38 | 2.62 ms |
| reference | 1318 | 35 | 2.43 ms |
| v4 / ref | 1.12x | 1.09x | 1.08x |

## Preql

```
import web_sales as ws;

const start_date <- '2000-01-27'::date;
const end_date <- '2000-04-26'::date;
auto avg_item_disc <- 1.3 * avg(ws.ext_discount_amount ? ws.date.date between start_date and end_date) by ws.item.id;

where
    ws.item.manufacturer_id = 350
    and ws.date.date between start_date and end_date
    and ws.ext_discount_amount > avg_item_disc
select
    sum(ws.ext_discount_amount) as excess_discount_amount,
order by
    excess_discount_amount asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
thoughtful as (
SELECT
    "ws_web_sales"."WS_ITEM_SK" as "ws_item_id",
    avg(CASE WHEN cast("ws_date_date"."D_DATE" as date) BETWEEN :start_date AND :end_date THEN "ws_web_sales"."WS_EXT_DISCOUNT_AMT" ELSE NULL END) as "_virt_agg_avg_5364249642270353"
FROM
    "memory"."web_sales" as "ws_web_sales"
    LEFT OUTER JOIN "memory"."date_dim" as "ws_date_date" on "ws_web_sales"."WS_SOLD_DATE_SK" = "ws_date_date"."D_DATE_SK"
GROUP BY
    1),
cheerful as (
SELECT
    "ws_item_items"."I_ITEM_SK" as "ws_item_id",
    "ws_web_sales"."WS_EXT_DISCOUNT_AMT" as "ws_ext_discount_amount"
FROM
    "memory"."web_sales" as "ws_web_sales"
    INNER JOIN "memory"."date_dim" as "ws_date_date" on "ws_web_sales"."WS_SOLD_DATE_SK" = "ws_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "ws_item_items" on "ws_web_sales"."WS_ITEM_SK" = "ws_item_items"."I_ITEM_SK"
WHERE
    "ws_item_items"."I_MANUFACT_ID" = 350 and cast("ws_date_date"."D_DATE" as date) BETWEEN :start_date AND :end_date
),
uneven as (
SELECT
    "thoughtful"."ws_item_id" as "ws_item_id",
    1.3 * "thoughtful"."_virt_agg_avg_5364249642270353" as "avg_item_disc"
FROM
    "thoughtful")
SELECT
    sum("cheerful"."ws_ext_discount_amount") as "excess_discount_amount"
FROM
    "cheerful"
    INNER JOIN "uneven" on "cheerful"."ws_item_id" = "uneven"."ws_item_id"
WHERE
    "cheerful"."ws_ext_discount_amount" > "uneven"."avg_item_disc"

ORDER BY 
    "excess_discount_amount" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
cheerful as (
SELECT
    "ws_item_items"."I_ITEM_SK" as "ws_item_id",
    "ws_web_sales"."WS_EXT_DISCOUNT_AMT" as "ws_ext_discount_amount"
FROM
    "memory"."web_sales" as "ws_web_sales"
    INNER JOIN "memory"."date_dim" as "ws_date_date" on "ws_web_sales"."WS_SOLD_DATE_SK" = "ws_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "ws_item_items" on "ws_web_sales"."WS_ITEM_SK" = "ws_item_items"."I_ITEM_SK"
WHERE
    "ws_item_items"."I_MANUFACT_ID" = 350 and cast("ws_date_date"."D_DATE" as date) BETWEEN :start_date AND :end_date
),
thoughtful as (
SELECT
    "ws_web_sales"."WS_ITEM_SK" as "ws_item_id",
    avg("ws_web_sales"."WS_EXT_DISCOUNT_AMT") as "_virt_agg_avg_5364249642270353"
FROM
    "memory"."web_sales" as "ws_web_sales"
    INNER JOIN "memory"."date_dim" as "ws_date_date" on "ws_web_sales"."WS_SOLD_DATE_SK" = "ws_date_date"."D_DATE_SK"
WHERE
    cast("ws_date_date"."D_DATE" as date) BETWEEN :start_date AND :end_date

GROUP BY
    1)
SELECT
    sum("cheerful"."ws_ext_discount_amount") as "excess_discount_amount"
FROM
    "cheerful"
    INNER JOIN "thoughtful" on "cheerful"."ws_item_id" = "thoughtful"."ws_item_id"
WHERE
    "cheerful"."ws_ext_discount_amount" > 1.3 * "thoughtful"."_virt_agg_avg_5364249642270353"

ORDER BY 
    "excess_discount_amount" asc nulls first
LIMIT (100)
```
