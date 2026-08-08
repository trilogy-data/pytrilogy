
WITH 
cool as (
SELECT
    "ws_web_sales"."WS_WEB_PAGE_SK" as "_ws_grouped_ws_wp_id",
    sum("ws_web_sales"."WS_EXT_SALES_PRICE") as "_ws_grouped_ws_sales",
    sum("ws_web_sales"."WS_NET_PROFIT") as "_ws_grouped_ws_profit"
FROM
    "memory"."web_sales" as "ws_web_sales"
    INNER JOIN "memory"."date_dim" as "ws_sale_date_date" on "ws_web_sales"."WS_SOLD_DATE_SK" = "ws_sale_date_date"."D_DATE_SK"
WHERE
    cast("ws_sale_date_date"."D_DATE" as date) BETWEEN :period_start AND :period_end and "ws_web_sales"."WS_WEB_PAGE_SK" is not null

GROUP BY
    1),
bewildered as (
SELECT
    "cool"."_ws_grouped_ws_profit" as "ws_grouped_ws_profit",
    "cool"."_ws_grouped_ws_sales" as "ws_grouped_ws_sales",
    "cool"."_ws_grouped_ws_wp_id" as "ws_grouped_ws_wp_id"
FROM
    "cool"),
yellow as (
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
courageous as (
SELECT
    "yellow"."_wr_grouped_wr_loss" as "wr_grouped_wr_loss",
    "yellow"."_wr_grouped_wr_returns" as "wr_grouped_wr_returns",
    "yellow"."_wr_grouped_wr_wp_id" as "wr_grouped_wr_wp_id"
FROM
    "yellow"),
level as (
SELECT
    "bewildered"."ws_grouped_ws_profit" as "ws_grouped_ws_profit",
    "bewildered"."ws_grouped_ws_sales" as "ws_grouped_ws_sales",
    "courageous"."wr_grouped_wr_loss" as "wr_grouped_wr_loss",
    "courageous"."wr_grouped_wr_returns" as "wr_grouped_wr_returns",
    coalesce("bewildered"."ws_grouped_ws_wp_id","courageous"."wr_grouped_wr_wp_id") as "wr_grouped_wr_wp_id",
    coalesce("bewildered"."ws_grouped_ws_wp_id","courageous"."wr_grouped_wr_wp_id") as "ws_grouped_ws_wp_id"
FROM
    "bewildered"
    FULL JOIN "courageous" on "bewildered"."ws_grouped_ws_wp_id" is not distinct from "courageous"."wr_grouped_wr_wp_id"),
wooden as (
SELECT
    "level"."ws_grouped_ws_profit" - cast(coalesce("level"."wr_grouped_wr_loss",0) as numeric(15,2)) as "___tvf_arm_2_u_profit",
    "level"."ws_grouped_ws_sales" as "ws_grouped_ws_sales",
    "level"."ws_grouped_ws_wp_id" as "___tvf_arm_2_u_id",
    :___tvf_arm_2_u_channel as "___tvf_arm_2_u_channel",
    cast("level"."ws_grouped_ws_sales" as numeric(15,2)) as "___tvf_arm_2_u_sales",
    cast(coalesce("level"."wr_grouped_wr_returns",0) as numeric(15,2)) as "___tvf_arm_2_u_returns"
FROM
    "level"),
busy as (
SELECT
    "ss_store_sales"."SS_STORE_SK" as "_ss_grouped_ss_store_id",
    sum("ss_store_sales"."SS_EXT_SALES_PRICE") as "_ss_grouped_ss_sales",
    sum("ss_store_sales"."SS_NET_PROFIT") as "_ss_grouped_ss_profit"
FROM
    "memory"."store_sales" as "ss_store_sales"
    INNER JOIN "memory"."date_dim" as "ss_sale_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_sale_date_date"."D_DATE_SK"
WHERE
    cast("ss_sale_date_date"."D_DATE" as date) BETWEEN :period_start AND :period_end and "ss_store_sales"."SS_STORE_SK" is not null

GROUP BY
    1),
premium as (
SELECT
    "busy"."_ss_grouped_ss_profit" as "ss_grouped_ss_profit",
    "busy"."_ss_grouped_ss_sales" as "ss_grouped_ss_sales",
    "busy"."_ss_grouped_ss_store_id" as "ss_grouped_ss_store_id"
FROM
    "busy"),
macho as (
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
kaput as (
SELECT
    "macho"."_sr_grouped_sr_loss" as "sr_grouped_sr_loss",
    "macho"."_sr_grouped_sr_returns" as "sr_grouped_sr_returns",
    "macho"."_sr_grouped_sr_store_id" as "sr_grouped_sr_store_id"
FROM
    "macho"),
puzzled as (
SELECT
    "kaput"."sr_grouped_sr_loss" as "sr_grouped_sr_loss",
    "kaput"."sr_grouped_sr_returns" as "sr_grouped_sr_returns",
    "premium"."ss_grouped_ss_profit" as "ss_grouped_ss_profit",
    "premium"."ss_grouped_ss_sales" as "ss_grouped_ss_sales",
    coalesce("kaput"."sr_grouped_sr_store_id","premium"."ss_grouped_ss_store_id") as "sr_grouped_sr_store_id",
    coalesce("kaput"."sr_grouped_sr_store_id","premium"."ss_grouped_ss_store_id") as "ss_grouped_ss_store_id"
FROM
    "premium"
    FULL JOIN "kaput" on "premium"."ss_grouped_ss_store_id" is not distinct from "kaput"."sr_grouped_sr_store_id"),
waggish as (
SELECT
    "puzzled"."ss_grouped_ss_profit" - cast(coalesce("puzzled"."sr_grouped_sr_loss",0) as numeric(15,2)) as "___tvf_arm_1_u_profit",
    "puzzled"."ss_grouped_ss_sales" as "ss_grouped_ss_sales",
    "puzzled"."ss_grouped_ss_store_id" as "___tvf_arm_1_u_id",
    :___tvf_arm_1_u_channel as "___tvf_arm_1_u_channel",
    cast("puzzled"."ss_grouped_ss_sales" as numeric(15,2)) as "___tvf_arm_1_u_sales",
    cast(coalesce("puzzled"."sr_grouped_sr_returns",0) as numeric(15,2)) as "___tvf_arm_1_u_returns"
FROM
    "puzzled"),
uneven as (
SELECT
    "cs_catalog_sales"."CS_CALL_CENTER_SK" as "_cs_grouped_cs_cc_id",
    sum("cs_catalog_sales"."CS_EXT_SALES_PRICE") as "_cs_grouped_cs_sales",
    sum("cs_catalog_sales"."CS_NET_PROFIT") as "_cs_grouped_cs_profit"
FROM
    "memory"."catalog_sales" as "cs_catalog_sales"
    INNER JOIN "memory"."date_dim" as "cs_sale_date_date" on "cs_catalog_sales"."CS_SOLD_DATE_SK" = "cs_sale_date_date"."D_DATE_SK"
WHERE
    cast("cs_sale_date_date"."D_DATE" as date) BETWEEN :period_start AND :period_end and "cs_catalog_sales"."CS_CALL_CENTER_SK" is not null

GROUP BY
    1),
vacuous as (
SELECT
    "uneven"."_cs_grouped_cs_cc_id" as "cs_grouped_cs_cc_id",
    "uneven"."_cs_grouped_cs_profit" as "cs_grouped_cs_profit",
    "uneven"."_cs_grouped_cs_sales" as "cs_grouped_cs_sales"
FROM
    "uneven"),
cheerful as (
SELECT
    "cs_catalog_returns"."CR_CALL_CENTER_SK" as "_cr_grouped_cr_cc_id",
    sum("cs_catalog_returns"."CR_NET_LOSS") as "_cr_grouped_cr_loss",
    sum("cs_catalog_returns"."CR_RETURN_AMOUNT") as "_cr_grouped_cr_returns"
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
    "cheerful"."_cr_grouped_cr_cc_id" as "cr_grouped_cr_cc_id",
    "cheerful"."_cr_grouped_cr_loss" as "cr_grouped_cr_loss",
    "cheerful"."_cr_grouped_cr_returns" as "cr_grouped_cr_returns"
FROM
    "cheerful"),
concerned as (
SELECT
    "questionable"."cr_grouped_cr_loss" as "cr_grouped_cr_loss",
    "questionable"."cr_grouped_cr_returns" as "cr_grouped_cr_returns",
    "vacuous"."cs_grouped_cs_profit" as "cs_grouped_cs_profit",
    "vacuous"."cs_grouped_cs_sales" as "cs_grouped_cs_sales",
    coalesce("questionable"."cr_grouped_cr_cc_id","vacuous"."cs_grouped_cs_cc_id") as "cr_grouped_cr_cc_id",
    coalesce("questionable"."cr_grouped_cr_cc_id","vacuous"."cs_grouped_cs_cc_id") as "cs_grouped_cs_cc_id"
FROM
    "vacuous"
    FULL JOIN "questionable" on "vacuous"."cs_grouped_cs_cc_id" is not distinct from "questionable"."cr_grouped_cr_cc_id"),
young as (
SELECT
    "concerned"."cs_grouped_cs_cc_id" as "___tvf_arm_0_u_id",
    "concerned"."cs_grouped_cs_profit" - cast(coalesce("concerned"."cr_grouped_cr_loss",0) as numeric(15,2)) as "___tvf_arm_0_u_profit",
    "concerned"."cs_grouped_cs_sales" as "cs_grouped_cs_sales",
    :___tvf_arm_0_u_channel as "___tvf_arm_0_u_channel",
    cast("concerned"."cs_grouped_cs_sales" as numeric(15,2)) as "___tvf_arm_0_u_sales",
    cast(coalesce("concerned"."cr_grouped_cr_returns",0) as numeric(15,2)) as "___tvf_arm_0_u_returns"
FROM
    "concerned"),
tearful as (
SELECT
    "young"."___tvf_arm_0_u_channel" as "_l0_union_u_channel",
    "young"."___tvf_arm_0_u_id" as "_l0_union_u_id",
    "young"."___tvf_arm_0_u_sales" as "_l0_union_u_sales",
    "young"."___tvf_arm_0_u_returns" as "_l0_union_u_returns",
    "young"."___tvf_arm_0_u_profit" as "_l0_union_u_profit"
FROM
    "young"
WHERE
    "young"."cs_grouped_cs_sales" is not null

UNION ALL
SELECT
    "waggish"."___tvf_arm_1_u_channel" as "_l0_union_u_channel",
    "waggish"."___tvf_arm_1_u_id" as "_l0_union_u_id",
    "waggish"."___tvf_arm_1_u_sales" as "_l0_union_u_sales",
    "waggish"."___tvf_arm_1_u_returns" as "_l0_union_u_returns",
    "waggish"."___tvf_arm_1_u_profit" as "_l0_union_u_profit"
FROM
    "waggish"
WHERE
    "waggish"."ss_grouped_ss_sales" is not null

UNION ALL
SELECT
    "wooden"."___tvf_arm_2_u_channel" as "_l0_union_u_channel",
    "wooden"."___tvf_arm_2_u_id" as "_l0_union_u_id",
    "wooden"."___tvf_arm_2_u_sales" as "_l0_union_u_sales",
    "wooden"."___tvf_arm_2_u_returns" as "_l0_union_u_returns",
    "wooden"."___tvf_arm_2_u_profit" as "_l0_union_u_profit"
FROM
    "wooden"
WHERE
    "wooden"."ws_grouped_ws_sales" is not null
),
cloudy as (
SELECT
    "tearful"."_l0_union_u_channel" as "l0_union_u_channel",
    "tearful"."_l0_union_u_id" as "l0_union_u_id",
    "tearful"."_l0_union_u_profit" as "l0_union_u_profit",
    "tearful"."_l0_union_u_returns" as "l0_union_u_returns",
    "tearful"."_l0_union_u_sales" as "l0_union_u_sales"
FROM
    "tearful")
SELECT
    "cloudy"."l0_union_u_channel" as "channel",
    "cloudy"."l0_union_u_id" as "sk",
    sum("cloudy"."l0_union_u_sales") as "sales",
    sum("cloudy"."l0_union_u_returns") as "returns_",
    sum("cloudy"."l0_union_u_profit") as "profit"
FROM
    "cloudy"
GROUP BY
    ROLLUP (1, 2)
ORDER BY 
    "channel" asc nulls first,
    "sk" asc nulls first,
    "returns_" desc
LIMIT (100)