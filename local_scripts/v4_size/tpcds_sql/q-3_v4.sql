
WITH 
uneven as (
SELECT
    "ws_web_sales"."WS_WEB_PAGE_SK" as "_ws_grouped_ws_wp_id",
    sum("ws_web_sales"."WS_EXT_SALES_PRICE") as "_ws_grouped_ws_sales",
    sum("ws_web_sales"."WS_NET_PROFIT") as "_ws_grouped_ws_profit"
FROM
    "memory"."web_sales" as "ws_web_sales"
    INNER JOIN "memory"."date_dim" as "ws_sale_date_date" on "ws_web_sales"."WS_SOLD_DATE_SK" = "ws_sale_date_date"."D_DATE_SK"
WHERE
    cast("ws_sale_date_date"."D_DATE" as date) BETWEEN :period_start AND :period_end

GROUP BY
    1),
vacuous as (
SELECT
    "uneven"."_ws_grouped_ws_profit" as "ws_grouped_ws_profit",
    "uneven"."_ws_grouped_ws_sales" as "ws_grouped_ws_sales",
    "uneven"."_ws_grouped_ws_wp_id" as "ws_grouped_ws_wp_id"
FROM
    "uneven"),
cheerful as (
SELECT
    "ws_web_returns"."WR_WEB_PAGE_SK" as "_wr_grouped_wr_wp_id",
    sum("ws_web_returns"."WR_NET_LOSS") as "_wr_grouped_wr_loss",
    sum("ws_web_returns"."WR_RETURN_AMT") as "_wr_grouped_wr_returns"
FROM
    "memory"."web_sales" as "ws_web_sales"
    LEFT OUTER JOIN "memory"."web_returns" as "ws_web_returns" on "ws_web_sales"."WS_ITEM_SK" = "ws_web_returns"."WR_ITEM_SK" AND "ws_web_sales"."WS_ORDER_NUMBER" = "ws_web_returns"."WR_ORDER_NUMBER"
    RIGHT OUTER JOIN "memory"."date_dim" as "ws_return_date_date" on "ws_web_returns"."WR_RETURNED_DATE_SK" = "ws_return_date_date"."D_DATE_SK"
WHERE
    cast("ws_return_date_date"."D_DATE" as date) BETWEEN :period_start AND :period_end

GROUP BY
    1),
questionable as (
SELECT
    "cheerful"."_wr_grouped_wr_loss" as "wr_grouped_wr_loss",
    "cheerful"."_wr_grouped_wr_returns" as "wr_grouped_wr_returns",
    "cheerful"."_wr_grouped_wr_wp_id" as "wr_grouped_wr_wp_id"
FROM
    "cheerful"),
concerned as (
SELECT
    "questionable"."wr_grouped_wr_loss" as "wr_grouped_wr_loss",
    "questionable"."wr_grouped_wr_returns" as "wr_grouped_wr_returns",
    "vacuous"."ws_grouped_ws_profit" as "ws_grouped_ws_profit",
    "vacuous"."ws_grouped_ws_sales" as "ws_grouped_ws_sales",
    coalesce("questionable"."wr_grouped_wr_wp_id","vacuous"."ws_grouped_ws_wp_id") as "wr_grouped_wr_wp_id",
    coalesce("questionable"."wr_grouped_wr_wp_id","vacuous"."ws_grouped_ws_wp_id") as "ws_grouped_ws_wp_id"
FROM
    "vacuous"
    LEFT OUTER JOIN "questionable" on "vacuous"."ws_grouped_ws_wp_id" is not distinct from "questionable"."wr_grouped_wr_wp_id")
SELECT
    :u_channel_w as "u_channel_w",
    "concerned"."ws_grouped_ws_wp_id" as "u_id_w",
    cast("concerned"."ws_grouped_ws_sales" as numeric(15,2)) as "u_sales_w",
    cast(coalesce("concerned"."wr_grouped_wr_returns",0) as numeric(15,2)) as "u_returns_w",
    "concerned"."ws_grouped_ws_profit" - cast(coalesce("concerned"."wr_grouped_wr_loss",0) as numeric(15,2)) as "u_profit_w"
FROM
    "concerned"
ORDER BY 
    "u_id_w" asc nulls first