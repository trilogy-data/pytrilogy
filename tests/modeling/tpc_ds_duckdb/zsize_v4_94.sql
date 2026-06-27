
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
juicy as (
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
    "ws_web_sales"."WS_ORDER_NUMBER" in (select juicy."multi_warehouse_orders" from juicy where juicy."multi_warehouse_orders" is not null) and "ws_web_sales"."WS_ORDER_NUMBER" not in (select quizzical."wr_web_sales_order_number" from quizzical where quizzical."wr_web_sales_order_number" is not null)

GROUP BY
    1,
    2,
    3,
    4),
young as (
SELECT
    "ws_web_sales"."WS_EXT_SHIP_COST" as "ws_ext_ship_cost",
    "ws_web_sales"."WS_NET_PROFIT" as "ws_net_profit",
    "ws_web_sales"."WS_ORDER_NUMBER" as "ws_order_number"
FROM
    "memory"."web_sales" as "ws_web_sales"
    INNER JOIN "memory"."web_site" as "ws_web_site_web_site" on "ws_web_sales"."WS_WEB_SITE_SK" = "ws_web_site_web_site"."web_site_sk"
    INNER JOIN "memory"."date_dim" as "ws_ship_date_date" on "ws_web_sales"."WS_SHIP_DATE_SK" = "ws_ship_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer_address" as "ws_ship_address_customer_address" on "ws_web_sales"."WS_SHIP_ADDR_SK" = "ws_ship_address_customer_address"."CA_ADDRESS_SK"
WHERE
    cast("ws_ship_date_date"."D_DATE" as date) BETWEEN date '1999-02-01' AND date '1999-04-02' and "ws_ship_address_customer_address"."CA_STATE" = 'IL' and "ws_web_site_web_site"."web_company_name" = 'pri' and "ws_web_sales"."WS_ORDER_NUMBER" in (select juicy."multi_warehouse_orders" from juicy where juicy."multi_warehouse_orders" is not null) and "ws_web_sales"."WS_ORDER_NUMBER" not in (select quizzical."wr_web_sales_order_number" from quizzical where quizzical."wr_web_sales_order_number" is not null)
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
    cast("ws_ship_date_date"."D_DATE" as date) BETWEEN date '1999-02-01' AND date '1999-04-02' and "ws_ship_address_customer_address"."CA_STATE" = 'IL' and "ws_web_site_web_site"."web_company_name" = 'pri' and "cooperative"."ws_order_number" in (select juicy."multi_warehouse_orders" from juicy where juicy."multi_warehouse_orders" is not null) and "cooperative"."ws_order_number" not in (select quizzical."wr_web_sales_order_number" from quizzical where quizzical."wr_web_sales_order_number" is not null)
),
sparkling as (
SELECT
    sum("young"."ws_ext_ship_cost") as "total_shipping_cost",
    sum("young"."ws_net_profit") as "total_net_profit"
FROM
    "young"
WHERE
    "young"."ws_order_number" in (select juicy."multi_warehouse_orders" from juicy where juicy."multi_warehouse_orders" is not null) and "young"."ws_order_number" not in (select quizzical."wr_web_sales_order_number" from quizzical where quizzical."wr_web_sales_order_number" is not null)
),
vacuous as (
SELECT
    count(distinct "abundant"."ws_order_number") as "order_count"
FROM
    "abundant"
WHERE
    "abundant"."ws_order_number" in (select juicy."multi_warehouse_orders" from juicy where juicy."multi_warehouse_orders" is not null) and "abundant"."ws_order_number" not in (select quizzical."wr_web_sales_order_number" from quizzical where quizzical."wr_web_sales_order_number" is not null)
)
SELECT
    "vacuous"."order_count" as "order_count",
    "sparkling"."total_shipping_cost" as "total_shipping_cost",
    "sparkling"."total_net_profit" as "total_net_profit"
FROM
    "vacuous"
    FULL JOIN "sparkling" on 1=1
ORDER BY 
    "vacuous"."order_count" asc
LIMIT (100)