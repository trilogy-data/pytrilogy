# Query 95

**Status:** `mismatch`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (1 rows) |
| reference execution | OK (1 rows) |
| results identical | NO |

## Result comparison

v4 rows: 1 (1 distinct)
ref rows: 1 (1 distinct)
only in v4 (showing up to 5 of 1):
  1x  (95, Decimal('-18202.90'), Decimal('100592.32'))
only in ref (showing up to 5 of 1):
  1x  (68, Decimal('-18202.90'), Decimal('100592.32'))

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 3058 | 56 | 37.98 ms |
| reference | 4030 | 76 | 58.35 ms |
| v4 / ref | 0.76x | 0.74x | 0.65x |

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
    count("questionable"."web_sales_warehouse_id") as "_virt_agg_count_2435454530783120"
FROM
    "questionable"
GROUP BY
    1),
abundant as (
SELECT
    CASE WHEN "questionable"."web_sales_is_returned" is True THEN "questionable"."web_sales_order_number" ELSE NULL END as "returned_orders"
FROM
    "questionable"),
yummy as (
SELECT
    CASE WHEN "uneven"."_virt_agg_count_2435454530783120" > 1 THEN "uneven"."web_sales_order_number" ELSE NULL END as "multi_warehouse_order"
FROM
    "uneven"),
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
    cast("web_sales_ship_date_date"."D_DATE" as date) BETWEEN date '1999-02-01' AND date '1999-04-02' and "web_sales_ship_address_customer_address"."CA_STATE" = 'IL' and "web_sales_web_site_web_site"."web_company_name" = 'pri' and "web_sales_web_sales"."WS_ORDER_NUMBER" in (select yummy."multi_warehouse_order" from yummy where yummy."multi_warehouse_order" is not null) and "web_sales_web_sales"."WS_ORDER_NUMBER" in (select abundant."returned_orders" from abundant where abundant."returned_orders" is not null)
)
SELECT
    count("thoughtful"."web_sales_order_number") as "order_count",
    sum("thoughtful"."web_sales_net_profit") as "total_net_profit",
    sum("thoughtful"."web_sales_ext_ship_cost") as "total_shipping_cost"
FROM
    "thoughtful"
WHERE
    "thoughtful"."web_sales_order_number" in (select yummy."multi_warehouse_order" from yummy where yummy."multi_warehouse_order" is not null) and "thoughtful"."web_sales_order_number" in (select abundant."returned_orders" from abundant where abundant."returned_orders" is not null)

ORDER BY 
    "order_count" desc
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
