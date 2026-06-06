# Query 94

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
| v4 | 5270 | 119 | 69.29 ms |
| reference | 3548 | 83 | 39.85 ms |
| v4 / ref | 1.49x | 1.43x | 1.74x |

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
WITH 
uneven as (
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
yummy as (
SELECT
    "uneven"."ws_order_number" as "ws_order_number",
    count("uneven"."ws_warehouse_id") as "_virt_agg_count_9309405360138092"
FROM
    "uneven"
GROUP BY
    1
HAVING
    "_virt_agg_count_9309405360138092" > 1
),
vacuous as (
SELECT
    CASE WHEN "yummy"."_virt_agg_count_9309405360138092" > 1 THEN "yummy"."ws_order_number" ELSE NULL END as "multi_warehouse_orders"
FROM
    "yummy"),
cooperative as (
SELECT
    "ws_web_sales"."WS_ORDER_NUMBER" as "ws_order_number",
    "ws_web_sales"."WS_SHIP_ADDR_SK" as "ws_ship_address_id",
    "ws_web_sales"."WS_SHIP_DATE_SK" as "ws_ship_date_id",
    "ws_web_sales"."WS_WEB_SITE_SK" as "ws_web_site_id"
FROM
    "memory"."web_sales" as "ws_web_sales"
WHERE
    "ws_web_sales"."WS_ORDER_NUMBER" in (select vacuous."multi_warehouse_orders" from vacuous where vacuous."multi_warehouse_orders" is not null) and "ws_web_sales"."WS_ORDER_NUMBER" not in (select quizzical."wr_web_sales_order_number" from quizzical where quizzical."wr_web_sales_order_number" is not null)

GROUP BY
    1,
    2,
    3,
    4),
abhorrent as (
SELECT
    "ws_web_sales"."WS_EXT_SHIP_COST" as "ws_ext_ship_cost",
    "ws_web_sales"."WS_ITEM_SK" as "ws_item_id",
    "ws_web_sales"."WS_NET_PROFIT" as "ws_net_profit",
    "ws_web_sales"."WS_ORDER_NUMBER" as "ws_order_number"
FROM
    "memory"."web_sales" as "ws_web_sales"
    INNER JOIN "memory"."web_site" as "ws_web_site_web_site" on "ws_web_sales"."WS_WEB_SITE_SK" = "ws_web_site_web_site"."web_site_sk"
    INNER JOIN "memory"."date_dim" as "ws_ship_date_date" on "ws_web_sales"."WS_SHIP_DATE_SK" = "ws_ship_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer_address" as "ws_ship_address_customer_address" on "ws_web_sales"."WS_SHIP_ADDR_SK" = "ws_ship_address_customer_address"."CA_ADDRESS_SK"
WHERE
    cast("ws_ship_date_date"."D_DATE" as date) BETWEEN date '1999-02-01' AND date '1999-04-02' and "ws_ship_address_customer_address"."CA_STATE" = 'IL' and "ws_web_site_web_site"."web_company_name" = 'pri' and "ws_web_sales"."WS_ORDER_NUMBER" in (select vacuous."multi_warehouse_orders" from vacuous where vacuous."multi_warehouse_orders" is not null) and "ws_web_sales"."WS_ORDER_NUMBER" not in (select quizzical."wr_web_sales_order_number" from quizzical where quizzical."wr_web_sales_order_number" is not null)
),
abundant as (
SELECT
    "cooperative"."ws_order_number" as "ws_order_number"
FROM
    "cooperative"
    INNER JOIN "memory"."web_site" as "ws_web_site_web_site" on "cooperative"."ws_web_site_id" = "ws_web_site_web_site"."web_site_sk"
    INNER JOIN "memory"."date_dim" as "ws_ship_date_date" on "cooperative"."ws_ship_date_id" = "ws_ship_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer_address" as "ws_ship_address_customer_address" on "cooperative"."ws_ship_address_id" = "ws_ship_address_customer_address"."CA_ADDRESS_SK"
WHERE
    cast("ws_ship_date_date"."D_DATE" as date) BETWEEN date '1999-02-01' AND date '1999-04-02' and "ws_ship_address_customer_address"."CA_STATE" = 'IL' and "ws_web_site_web_site"."web_company_name" = 'pri' and "cooperative"."ws_order_number" in (select vacuous."multi_warehouse_orders" from vacuous where vacuous."multi_warehouse_orders" is not null) and "cooperative"."ws_order_number" not in (select quizzical."wr_web_sales_order_number" from quizzical where quizzical."wr_web_sales_order_number" is not null)
),
sweltering as (
SELECT
    "abhorrent"."ws_ext_ship_cost" as "ws_ext_ship_cost",
    "abhorrent"."ws_net_profit" as "ws_net_profit"
FROM
    "abhorrent"
WHERE
    "abhorrent"."ws_order_number" in (select vacuous."multi_warehouse_orders" from vacuous where vacuous."multi_warehouse_orders" is not null) and "abhorrent"."ws_order_number" not in (select quizzical."wr_web_sales_order_number" from quizzical where quizzical."wr_web_sales_order_number" is not null)

GROUP BY
    1,
    2,
    "abhorrent"."ws_item_id",
    "abhorrent"."ws_order_number"),
concerned as (
SELECT
    "abundant"."ws_order_number" as "ws_order_number"
FROM
    "abundant"
WHERE
    "abundant"."ws_order_number" in (select vacuous."multi_warehouse_orders" from vacuous where vacuous."multi_warehouse_orders" is not null) and "abundant"."ws_order_number" not in (select quizzical."wr_web_sales_order_number" from quizzical where quizzical."wr_web_sales_order_number" is not null)

GROUP BY
    1),
macho as (
SELECT
    sum("sweltering"."ws_ext_ship_cost") as "total_shipping_cost",
    sum("sweltering"."ws_net_profit") as "total_net_profit"
FROM
    "sweltering"),
sparkling as (
SELECT
    count(distinct "concerned"."ws_order_number") as "order_count"
FROM
    "concerned")
SELECT
    "sparkling"."order_count" as "order_count",
    "macho"."total_shipping_cost" as "total_shipping_cost",
    "macho"."total_net_profit" as "total_net_profit"
FROM
    "sparkling"
    FULL JOIN "macho" on 1=1
ORDER BY 
    "sparkling"."order_count" asc
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
