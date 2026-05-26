# Query 94

**Status:** `exec_fail`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | FAILED |
| reference execution | OK (1 rows) |

## Result comparison

_at least one side did not produce rows._

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 371 | 8 | — |
| reference | 3548 | 83 | 72.65 ms |
| v4 / ref | 0.10x | 0.10x | — |

## Preql

```
import web_sales as ws;
import web_returns as wr;

auto multi_warehouse_orders <- ws.order_number ? count(ws.warehouse.id) by ws.order_number > 1;

where
    ws.ship_date.date between '1999-02-01'::date and '1999-04-02'::date
    and ws.ship_address.state = 'IL'
    and ws.web_site.company_name = 'pri'
    and ws.order_number in multi_warehouse_orders
    and ws.order_number not in wr.web_sales.order_number
select
    count_distinct(ws.order_number) as order_count,
    sum(ws.ext_ship_cost) as total_shipping_cost,
    sum(ws.net_profit) as total_net_profit,
order by
    order_count asc
limit 100
;
```

## v4 generated SQL

```sql
SELECT
    CASE WHENINVALID_REFERENCE_BUG_<Missing source reference to ws.order_number> IS NOT NULL THEN 1 ELSE 0 END as "order_count",
    INVALID_REFERENCE_BUG_<Missing source reference to ws.ext_ship_cost> as "total_shipping_cost",
    INVALID_REFERENCE_BUG_<Missing source reference to ws.net_profit> as "total_net_profit"

ORDER BY 
    "order_count" asc
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
questionable as (
SELECT
    "ws_web_sales"."WS_ORDER_NUMBER" as "ws_order_number",
    "ws_web_sales"."WS_WAREHOUSE_SK" as "ws_warehouse_id"
FROM
    "memory"."web_sales" as "ws_web_sales"
GROUP BY
    1,
    2),
quizzical as (
SELECT
    "wr_web_returns"."WR_ORDER_NUMBER" as "wr_web_sales_order_number"
FROM
    "memory"."web_returns" as "wr_web_returns"
GROUP BY
    1),
abundant as (
SELECT
    "questionable"."ws_order_number" as "multi_warehouse_orders"
FROM
    "questionable"
GROUP BY
    1
HAVING
    count("questionable"."ws_warehouse_id") > 1
),
uneven as (
SELECT
    "abundant"."multi_warehouse_orders" as "multi_warehouse_orders"
FROM
    "abundant"),
cooperative as (
SELECT
    "ws_web_sales"."WS_ORDER_NUMBER" as "ws_order_number"
FROM
    "memory"."web_sales" as "ws_web_sales"
    INNER JOIN "memory"."web_site" as "ws_web_site_web_site" on "ws_web_sales"."WS_WEB_SITE_SK" = "ws_web_site_web_site"."web_site_sk"
    INNER JOIN "memory"."date_dim" as "ws_ship_date_date" on "ws_web_sales"."WS_SHIP_DATE_SK" = "ws_ship_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer_address" as "ws_ship_address_customer_address" on "ws_web_sales"."WS_SHIP_ADDR_SK" = "ws_ship_address_customer_address"."CA_ADDRESS_SK"
WHERE
    "ws_web_site_web_site"."web_company_name" = 'pri' and cast("ws_ship_date_date"."D_DATE" as date) BETWEEN date '1999-02-01' AND date '1999-04-02' and "ws_ship_address_customer_address"."CA_STATE" = 'IL' and "ws_web_sales"."WS_ORDER_NUMBER" in (select uneven."multi_warehouse_orders" from uneven where uneven."multi_warehouse_orders" is not null) and "ws_web_sales"."WS_ORDER_NUMBER" not in (select quizzical."wr_web_sales_order_number" from quizzical where quizzical."wr_web_sales_order_number" is not null)

GROUP BY
    1),
concerned as (
SELECT
    "ws_web_sales"."WS_EXT_SHIP_COST" as "ws_ext_ship_cost",
    "ws_web_sales"."WS_NET_PROFIT" as "ws_net_profit"
FROM
    "memory"."web_sales" as "ws_web_sales"
    INNER JOIN "memory"."web_site" as "ws_web_site_web_site" on "ws_web_sales"."WS_WEB_SITE_SK" = "ws_web_site_web_site"."web_site_sk"
    INNER JOIN "memory"."date_dim" as "ws_ship_date_date" on "ws_web_sales"."WS_SHIP_DATE_SK" = "ws_ship_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer_address" as "ws_ship_address_customer_address" on "ws_web_sales"."WS_SHIP_ADDR_SK" = "ws_ship_address_customer_address"."CA_ADDRESS_SK"
WHERE
    cast("ws_ship_date_date"."D_DATE" as date) BETWEEN date '1999-02-01' AND date '1999-04-02' and "ws_ship_address_customer_address"."CA_STATE" = 'IL' and "ws_web_site_web_site"."web_company_name" = 'pri' and "ws_web_sales"."WS_ORDER_NUMBER" in (select uneven."multi_warehouse_orders" from uneven where uneven."multi_warehouse_orders" is not null) and "ws_web_sales"."WS_ORDER_NUMBER" not in (select quizzical."wr_web_sales_order_number" from quizzical where quizzical."wr_web_sales_order_number" is not null)

GROUP BY
    1,
    2,
    "ws_web_sales"."WS_ITEM_SK",
    "ws_web_sales"."WS_ORDER_NUMBER"),
vacuous as (
SELECT
    count(distinct "cooperative"."ws_order_number") as "order_count"
FROM
    "cooperative"),
young as (
SELECT
    sum("concerned"."ws_ext_ship_cost") as "total_shipping_cost",
    sum("concerned"."ws_net_profit") as "total_net_profit"
FROM
    "concerned")
SELECT
    "vacuous"."order_count" as "order_count",
    "young"."total_shipping_cost" as "total_shipping_cost",
    "young"."total_net_profit" as "total_net_profit"
FROM
    "vacuous"
    FULL JOIN "young" on 1=1
ORDER BY 
    "vacuous"."order_count" asc
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
_duckdb.ParserException: Parser Error: syntax error at or near "source"

LINE 2:     CASE WHENINVALID_REFERENCE_BUG_<Missing source reference to ws.order_number> IS NOT NULL THEN 1...
                                                    ^
```
