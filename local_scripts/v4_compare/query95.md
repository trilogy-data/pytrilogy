# Query 95

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
| v4 | 3932 | 91 | 61.50 ms |
| reference | 4030 | 76 | 56.30 ms |
| v4 / ref | 0.98x | 1.20x | 1.09x |

## Preql

```
import web_sales as web_sales;

auto multi_warehouse_order <- web_sales.order_number ? count(web_sales.warehouse.id) by web_sales.order_number > 1;
auto returned_orders <- web_sales.order_number ? web_sales.is_returned is True;

where
    web_sales.ship_date.date between '1999-02-01'::date and '1999-04-02'::date
    and web_sales.ship_address.state = 'IL'
    and web_sales.web_site.company_name = 'pri'
    and web_sales.order_number in multi_warehouse_order
    and web_sales.order_number in returned_orders
select
    count(web_sales.order_number) as order_count,
    sum(web_sales.ext_ship_cost) as total_shipping_cost,
    sum(web_sales.net_profit) as total_net_profit,
order by
    order_count desc
limit 100
;
```

## v4 generated SQL

```sql
WITH 
questionable as (
SELECT
    "web_sales_web_sales"."WS_ORDER_NUMBER" as "web_sales_order_number",
    "web_sales_web_sales"."WS_WAREHOUSE_SK" as "web_sales_warehouse_id",
    CASE WHEN WR_ORDER_NUMBER IS NOT NULL THEN 1 else 0 END as "web_sales_is_returned"
FROM
    "memory"."web_sales" as "web_sales_web_sales"
    LEFT OUTER JOIN "memory"."web_returns" as "web_sales_web_returns" on "web_sales_web_sales"."WS_ITEM_SK" = "web_sales_web_returns"."WR_ITEM_SK" AND "web_sales_web_sales"."WS_ORDER_NUMBER" = "web_sales_web_returns"."WR_ORDER_NUMBER"
GROUP BY
    1,
    2,
    3),
uneven as (
SELECT
    "questionable"."web_sales_order_number" as "web_sales_order_number",
    "questionable"."web_sales_warehouse_id" as "web_sales_warehouse_id"
FROM
    "questionable"
GROUP BY
    1,
    2),
abundant as (
SELECT
    CASE WHEN "questionable"."web_sales_is_returned" is True THEN "questionable"."web_sales_order_number" ELSE NULL END as "returned_orders"
FROM
    "questionable"),
yummy as (
SELECT
    "uneven"."web_sales_order_number" as "web_sales_order_number",
    count("uneven"."web_sales_warehouse_id") as "_virt_agg_count_2435454530783120"
FROM
    "uneven"
GROUP BY
    1),
juicy as (
SELECT
    CASE WHEN "yummy"."_virt_agg_count_2435454530783120" > 1 THEN "yummy"."web_sales_order_number" ELSE NULL END as "multi_warehouse_order"
FROM
    "yummy"),
thoughtful as (
SELECT
    "web_sales_web_sales"."WS_EXT_SHIP_COST" as "web_sales_ext_ship_cost",
    "web_sales_web_sales"."WS_NET_PROFIT" as "web_sales_net_profit",
    "web_sales_web_sales"."WS_ORDER_NUMBER" as "web_sales_order_number"
FROM
    "memory"."web_sales" as "web_sales_web_sales"
    INNER JOIN "memory"."web_site" as "web_sales_web_site_web_site" on "web_sales_web_sales"."WS_WEB_SITE_SK" = "web_sales_web_site_web_site"."web_site_sk"
    INNER JOIN "memory"."date_dim" as "web_sales_ship_date_date" on "web_sales_web_sales"."WS_SHIP_DATE_SK" = "web_sales_ship_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer_address" as "web_sales_ship_address_customer_address" on "web_sales_web_sales"."WS_SHIP_ADDR_SK" = "web_sales_ship_address_customer_address"."CA_ADDRESS_SK"
WHERE
    cast("web_sales_ship_date_date"."D_DATE" as date) BETWEEN date '1999-02-01' AND date '1999-04-02' and "web_sales_ship_address_customer_address"."CA_STATE" = 'IL' and "web_sales_web_site_web_site"."web_company_name" = 'pri' and "web_sales_web_sales"."WS_ORDER_NUMBER" in (select juicy."multi_warehouse_order" from juicy where juicy."multi_warehouse_order" is not null) and "web_sales_web_sales"."WS_ORDER_NUMBER" in (select abundant."returned_orders" from abundant where abundant."returned_orders" is not null)
),
vacuous as (
SELECT
    "thoughtful"."web_sales_ext_ship_cost" as "web_sales_ext_ship_cost",
    "thoughtful"."web_sales_net_profit" as "web_sales_net_profit",
    "thoughtful"."web_sales_order_number" as "web_sales_order_number"
FROM
    "thoughtful"
WHERE
    "thoughtful"."web_sales_order_number" in (select juicy."multi_warehouse_order" from juicy where juicy."multi_warehouse_order" is not null) and "thoughtful"."web_sales_order_number" in (select abundant."returned_orders" from abundant where abundant."returned_orders" is not null)
),
young as (
SELECT
    "vacuous"."web_sales_order_number" as "web_sales_order_number"
FROM
    "vacuous"
GROUP BY
    1),
concerned as (
SELECT
    sum("vacuous"."web_sales_ext_ship_cost") as "total_shipping_cost",
    sum("vacuous"."web_sales_net_profit") as "total_net_profit"
FROM
    "vacuous"),
sparkling as (
SELECT
    count("young"."web_sales_order_number") as "order_count"
FROM
    "young")
SELECT
    coalesce("sparkling"."order_count",0) as "order_count",
    "concerned"."total_shipping_cost" as "total_shipping_cost",
    "concerned"."total_net_profit" as "total_net_profit"
FROM
    "concerned"
    FULL JOIN "sparkling" on 1=1
ORDER BY 
    coalesce("sparkling"."order_count",0) desc
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
questionable as (
SELECT
    "web_sales_web_sales"."WS_ORDER_NUMBER" as "web_sales_order_number",
    "web_sales_web_sales"."WS_WAREHOUSE_SK" as "web_sales_warehouse_id"
FROM
    "memory"."web_sales" as "web_sales_web_sales"
GROUP BY
    1,
    2),
abundant as (
SELECT
    "questionable"."web_sales_order_number" as "multi_warehouse_order"
FROM
    "questionable"
GROUP BY
    1
HAVING
    count("questionable"."web_sales_warehouse_id") > 1
),
uneven as (
SELECT
    "abundant"."multi_warehouse_order" as "multi_warehouse_order"
FROM
    "abundant"),
thoughtful as (
SELECT
    "web_sales_web_sales"."WS_ORDER_NUMBER" as "returned_orders"
FROM
    "memory"."web_sales" as "web_sales_web_sales"
    INNER JOIN "memory"."web_returns" as "web_sales_web_returns" on "web_sales_web_sales"."WS_ITEM_SK" = "web_sales_web_returns"."WR_ITEM_SK" AND "web_sales_web_sales"."WS_ORDER_NUMBER" = "web_sales_web_returns"."WR_ORDER_NUMBER"
WHERE
    CASE WHEN WR_ORDER_NUMBER IS NOT NULL THEN 1 else 0 END is True and "web_sales_web_sales"."WS_ORDER_NUMBER" in (select uneven."multi_warehouse_order" from uneven where uneven."multi_warehouse_order" is not null)

GROUP BY
    1),
cooperative as (
SELECT
    "web_sales_web_sales"."WS_ORDER_NUMBER" as "web_sales_order_number"
FROM
    "memory"."web_sales" as "web_sales_web_sales"
    INNER JOIN "memory"."web_site" as "web_sales_web_site_web_site" on "web_sales_web_sales"."WS_WEB_SITE_SK" = "web_sales_web_site_web_site"."web_site_sk"
    INNER JOIN "memory"."date_dim" as "web_sales_ship_date_date" on "web_sales_web_sales"."WS_SHIP_DATE_SK" = "web_sales_ship_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer_address" as "web_sales_ship_address_customer_address" on "web_sales_web_sales"."WS_SHIP_ADDR_SK" = "web_sales_ship_address_customer_address"."CA_ADDRESS_SK"
WHERE
    "web_sales_web_site_web_site"."web_company_name" = 'pri' and cast("web_sales_ship_date_date"."D_DATE" as date) BETWEEN date '1999-02-01' AND date '1999-04-02' and "web_sales_ship_address_customer_address"."CA_STATE" = 'IL' and "web_sales_web_sales"."WS_ORDER_NUMBER" in (select uneven."multi_warehouse_order" from uneven where uneven."multi_warehouse_order" is not null) and "web_sales_web_sales"."WS_ORDER_NUMBER" in (select thoughtful."returned_orders" from thoughtful where thoughtful."returned_orders" is not null)

GROUP BY
    1),
concerned as (
SELECT
    sum("web_sales_web_sales"."WS_EXT_SHIP_COST") as "total_shipping_cost",
    sum("web_sales_web_sales"."WS_NET_PROFIT") as "total_net_profit"
FROM
    "memory"."web_sales" as "web_sales_web_sales"
    INNER JOIN "memory"."web_site" as "web_sales_web_site_web_site" on "web_sales_web_sales"."WS_WEB_SITE_SK" = "web_sales_web_site_web_site"."web_site_sk"
    INNER JOIN "memory"."date_dim" as "web_sales_ship_date_date" on "web_sales_web_sales"."WS_SHIP_DATE_SK" = "web_sales_ship_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer_address" as "web_sales_ship_address_customer_address" on "web_sales_web_sales"."WS_SHIP_ADDR_SK" = "web_sales_ship_address_customer_address"."CA_ADDRESS_SK"
WHERE
    cast("web_sales_ship_date_date"."D_DATE" as date) BETWEEN date '1999-02-01' AND date '1999-04-02' and "web_sales_ship_address_customer_address"."CA_STATE" = 'IL' and "web_sales_web_site_web_site"."web_company_name" = 'pri' and "web_sales_web_sales"."WS_ORDER_NUMBER" in (select uneven."multi_warehouse_order" from uneven where uneven."multi_warehouse_order" is not null) and "web_sales_web_sales"."WS_ORDER_NUMBER" in (select thoughtful."returned_orders" from thoughtful where thoughtful."returned_orders" is not null)
),
vacuous as (
SELECT
    count("cooperative"."web_sales_order_number") as "order_count"
FROM
    "cooperative")
SELECT
    coalesce("vacuous"."order_count",0) as "order_count",
    "concerned"."total_shipping_cost" as "total_shipping_cost",
    "concerned"."total_net_profit" as "total_net_profit"
FROM
    "vacuous"
    FULL JOIN "concerned" on 1=1
ORDER BY 
    coalesce("vacuous"."order_count",0) desc
LIMIT (100)
```
