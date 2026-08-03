
WITH 
abundant as (
SELECT
    "ss_store_sales"."SS_STORE_SK" as "_ss_grouped_ss_store_id",
    sum("ss_store_sales"."SS_EXT_SALES_PRICE") as "_ss_grouped_ss_sales",
    sum("ss_store_sales"."SS_NET_PROFIT") as "_ss_grouped_ss_profit"
FROM
    "memory"."store_sales" as "ss_store_sales"
    INNER JOIN "memory"."date_dim" as "ss_sale_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_sale_date_date"."D_DATE_SK"
WHERE
    cast("ss_sale_date_date"."D_DATE" as date) BETWEEN :period_start AND :period_end

GROUP BY
    1),
yummy as (
SELECT
    "abundant"."_ss_grouped_ss_profit" as "ss_grouped_ss_profit",
    "abundant"."_ss_grouped_ss_sales" as "ss_grouped_ss_sales",
    "abundant"."_ss_grouped_ss_store_id" as "ss_grouped_ss_store_id"
FROM
    "abundant"),
cheerful as (
SELECT
    "ss_store_returns"."SR_STORE_SK" as "_sr_grouped_sr_store_id",
    sum("ss_store_returns"."SR_NET_LOSS") as "_sr_grouped_sr_loss",
    sum("ss_store_returns"."SR_RETURN_AMT") as "_sr_grouped_sr_returns"
FROM
    "memory"."store_sales" as "ss_store_sales"
    LEFT OUTER JOIN "memory"."store_returns" as "ss_store_returns" on "ss_store_sales"."SS_ITEM_SK" = "ss_store_returns"."SR_ITEM_SK" AND "ss_store_sales"."SS_TICKET_NUMBER" = "ss_store_returns"."SR_TICKET_NUMBER"
    RIGHT OUTER JOIN "memory"."date_dim" as "ss_return_date_date" on "ss_store_returns"."SR_RETURNED_DATE_SK" = "ss_return_date_date"."D_DATE_SK"
WHERE
    cast("ss_return_date_date"."D_DATE" as date) BETWEEN :period_start AND :period_end

GROUP BY
    1),
cooperative as (
SELECT
    "cheerful"."_sr_grouped_sr_loss" as "sr_grouped_sr_loss",
    "cheerful"."_sr_grouped_sr_returns" as "sr_grouped_sr_returns",
    "cheerful"."_sr_grouped_sr_store_id" as "sr_grouped_sr_store_id"
FROM
    "cheerful")
SELECT
    :u_channel_s as "u_channel_s",
    coalesce("cooperative"."sr_grouped_sr_store_id","yummy"."ss_grouped_ss_store_id") as "u_id_s",
    cast("yummy"."ss_grouped_ss_sales" as numeric(15,2)) as "u_sales_s",
    cast(coalesce("cooperative"."sr_grouped_sr_returns",0) as numeric(15,2)) as "u_returns_s",
    "yummy"."ss_grouped_ss_profit" - cast(coalesce("cooperative"."sr_grouped_sr_loss",0) as numeric(15,2)) as "u_profit_s"
FROM
    "yummy"
    LEFT OUTER JOIN "cooperative" on "yummy"."ss_grouped_ss_store_id" is not distinct from "cooperative"."sr_grouped_sr_store_id"
ORDER BY 
    "u_id_s" asc nulls first