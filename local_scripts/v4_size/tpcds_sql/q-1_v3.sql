
WITH 
yummy as (
SELECT
    "cs_catalog_sales"."CS_CALL_CENTER_SK" as "cs_call_center_sk",
    sum("cs_catalog_sales"."CS_EXT_SALES_PRICE") as "_virt_agg_sum_6520591768854391",
    sum("cs_catalog_sales"."CS_NET_PROFIT") as "_virt_agg_sum_6226990944561419"
FROM
    "memory"."catalog_sales" as "cs_catalog_sales"
    INNER JOIN "memory"."date_dim" as "cs_sale_date_date" on "cs_catalog_sales"."CS_SOLD_DATE_SK" = "cs_sale_date_date"."D_DATE_SK"
WHERE
    cast("cs_sale_date_date"."D_DATE" as date) BETWEEN :period_start AND :period_end

GROUP BY
    1),
cheerful as (
SELECT
    coalesce("cs_catalog_returns"."CR_CALL_CENTER_SK",-1) as "cr_grouped_cr_cc_key",
    sum("cs_catalog_returns"."CR_NET_LOSS") as "cr_grouped_cr_loss_per_cc",
    sum("cs_catalog_returns"."CR_RETURN_AMOUNT") as "cr_grouped_cr_returns_per_cc"
FROM
    "memory"."catalog_sales" as "cs_catalog_sales"
    LEFT OUTER JOIN "memory"."catalog_returns" as "cs_catalog_returns" on "cs_catalog_sales"."CS_ITEM_SK" = "cs_catalog_returns"."CR_ITEM_SK" AND "cs_catalog_sales"."CS_ORDER_NUMBER" = "cs_catalog_returns"."CR_ORDER_NUMBER"
    RIGHT OUTER JOIN "memory"."date_dim" as "cs_return_date_date" on "cs_catalog_returns"."CR_RETURNED_DATE_SK" = "cs_return_date_date"."D_DATE_SK"
WHERE
    cast("cs_return_date_date"."D_DATE" as date) BETWEEN :period_start AND :period_end

GROUP BY
    1),
questionable as (
SELECT
    cast(sum("cheerful"."cr_grouped_cr_returns_per_cc") as numeric(15,2)) as "u_returns_c",
    count("cheerful"."cr_grouped_cr_cc_key") as "cr_totals_cr_n_groups",
    sum("cheerful"."cr_grouped_cr_loss_per_cc") as "cr_totals_cr_total_loss"
FROM
    "cheerful"),
vacuous as (
SELECT
    "yummy"."_virt_agg_sum_6520591768854391" * cast("questionable"."cr_totals_cr_n_groups" as numeric(15,2)) as "u_sales_c",
    "yummy"."cs_call_center_sk" as "u_id_c",
    ( "yummy"."_virt_agg_sum_6226990944561419" * "questionable"."cr_totals_cr_n_groups" ) - cast("questionable"."cr_totals_cr_total_loss" as numeric(15,2)) as "u_profit_c",
    :u_channel_c as "u_channel_c"
FROM
    "questionable"
    RIGHT OUTER JOIN "yummy" on 1=1)
SELECT
    "vacuous"."u_channel_c" as "u_channel_c",
    "vacuous"."u_id_c" as "u_id_c",
    "vacuous"."u_sales_c" as "u_sales_c",
    "questionable"."u_returns_c" as "u_returns_c",
    "vacuous"."u_profit_c" as "u_profit_c"
FROM
    "vacuous"
    LEFT OUTER JOIN "questionable" on 1=1
ORDER BY 
    "vacuous"."u_id_c" asc nulls first