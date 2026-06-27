
WITH 
sedate as (
SELECT
    "ws_web_sales"."WS_ITEM_SK" as "ws_item_id",
    "ws_web_sales"."WS_ORDER_NUMBER" as "ws_order_number",
    "ws_web_sales"."WS_SHIP_CUSTOMER_SK" as "ws_ship_customer_id"
FROM
    "memory"."web_sales" as "ws_web_sales"
WHERE
    "ws_web_sales"."WS_SHIP_CUSTOMER_SK" is null and "ws_web_sales"."WS_SOLD_DATE_SK" is not null
),
scrawny as (
SELECT
    "ss_store_sales"."SS_ITEM_SK" as "ss_item_id",
    "ss_store_sales"."SS_STORE_SK" as "ss_store_id",
    "ss_store_sales"."SS_TICKET_NUMBER" as "ss_ticket_number"
FROM
    "memory"."store_sales" as "ss_store_sales"
WHERE
    "ss_store_sales"."SS_STORE_SK" is null and "ss_store_sales"."SS_SOLD_DATE_SK" is not null
),
quizzical as (
SELECT
    "cs_catalog_sales"."CS_ITEM_SK" as "cs_item_id",
    "cs_catalog_sales"."CS_ORDER_NUMBER" as "cs_order_number",
    "cs_catalog_sales"."CS_SHIP_ADDR_SK" as "cs_customer_address_id"
FROM
    "memory"."catalog_sales" as "cs_catalog_sales"
WHERE
    "cs_catalog_sales"."CS_SHIP_ADDR_SK" is null and "cs_catalog_sales"."CS_SOLD_DATE_SK" is not null
),
waggish as (
SELECT
    "ws_date_date"."D_QOY" as "ws_date_quarter",
    "ws_date_date"."D_YEAR" as "ws_date_year",
    "ws_web_sales"."WS_EXT_SALES_PRICE" as "ws_ext_sales_price",
    "ws_web_sales"."WS_ITEM_SK" as "ws_item_id",
    "ws_web_sales"."WS_ORDER_NUMBER" as "ws_order_number"
FROM
    "memory"."web_sales" as "ws_web_sales"
    INNER JOIN "memory"."date_dim" as "ws_date_date" on "ws_web_sales"."WS_SOLD_DATE_SK" = "ws_date_date"."D_DATE_SK"
WHERE
    "ws_web_sales"."WS_SHIP_CUSTOMER_SK" is null and "ws_web_sales"."WS_SOLD_DATE_SK" is not null
),
abhorrent as (
SELECT
    "ss_date_date"."D_QOY" as "ss_date_quarter",
    "ss_date_date"."D_YEAR" as "ss_date_year",
    "ss_store_sales"."SS_EXT_SALES_PRICE" as "ss_ext_sales_price",
    "ss_store_sales"."SS_ITEM_SK" as "ss_item_id",
    "ss_store_sales"."SS_TICKET_NUMBER" as "ss_ticket_number"
FROM
    "memory"."store_sales" as "ss_store_sales"
    INNER JOIN "memory"."date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
WHERE
    "ss_store_sales"."SS_STORE_SK" is null and "ss_store_sales"."SS_SOLD_DATE_SK" is not null
),
thoughtful as (
SELECT
    "cs_catalog_sales"."CS_EXT_SALES_PRICE" as "cs_ext_sales_price",
    "cs_catalog_sales"."CS_ITEM_SK" as "cs_item_id",
    "cs_catalog_sales"."CS_ORDER_NUMBER" as "cs_order_number",
    "cs_date_date"."D_QOY" as "cs_date_quarter",
    "cs_date_date"."D_YEAR" as "cs_date_year"
FROM
    "memory"."catalog_sales" as "cs_catalog_sales"
    INNER JOIN "memory"."date_dim" as "cs_date_date" on "cs_catalog_sales"."CS_SOLD_DATE_SK" = "cs_date_date"."D_DATE_SK"
WHERE
    "cs_catalog_sales"."CS_SHIP_ADDR_SK" is null and "cs_catalog_sales"."CS_SOLD_DATE_SK" is not null
),
yellow as (
SELECT
    "sedate"."ws_item_id" as "ws_item_id",
    "sedate"."ws_order_number" as "ws_order_number",
    CASE
	WHEN "sedate"."ws_ship_customer_id" is null THEN 1
	ELSE 0
	END as "ws_row_flag"
FROM
    "sedate"),
friendly as (
SELECT
    "scrawny"."ss_item_id" as "ss_item_id",
    "scrawny"."ss_ticket_number" as "ss_ticket_number",
    CASE
	WHEN "scrawny"."ss_store_id" is null THEN 1
	ELSE 0
	END as "ss_row_flag"
FROM
    "scrawny"),
highfalutin as (
SELECT
    "quizzical"."cs_item_id" as "cs_item_id",
    "quizzical"."cs_order_number" as "cs_order_number",
    CASE
	WHEN "quizzical"."cs_customer_address_id" is null THEN 1
	ELSE 0
	END as "cs_row_flag"
FROM
    "quizzical"),
rambunctious as (
SELECT
    "waggish"."ws_date_quarter" as "ws_date_quarter",
    "waggish"."ws_date_year" as "ws_date_year",
    "waggish"."ws_ext_sales_price" as "ws_ext_sales_price",
    "waggish"."ws_item_id" as "ws_item_id",
    "waggish"."ws_order_number" as "ws_order_number"
FROM
    "waggish"),
sweltering as (
SELECT
    "abhorrent"."ss_date_quarter" as "ss_date_quarter",
    "abhorrent"."ss_date_year" as "ss_date_year",
    "abhorrent"."ss_ext_sales_price" as "ss_ext_sales_price",
    "abhorrent"."ss_item_id" as "ss_item_id",
    "abhorrent"."ss_ticket_number" as "ss_ticket_number"
FROM
    "abhorrent"),
cooperative as (
SELECT
    "thoughtful"."cs_date_quarter" as "cs_date_quarter",
    "thoughtful"."cs_date_year" as "cs_date_year",
    "thoughtful"."cs_ext_sales_price" as "cs_ext_sales_price",
    "thoughtful"."cs_item_id" as "cs_item_id",
    "thoughtful"."cs_order_number" as "cs_order_number"
FROM
    "thoughtful"),
puffy as (
SELECT
    "rambunctious"."ws_date_quarter" as "ws_date_quarter",
    "rambunctious"."ws_date_year" as "ws_date_year",
    "rambunctious"."ws_ext_sales_price" as "ws_ext_sales_price",
    "rambunctious"."ws_item_id" as "ws_item_id",
    "rambunctious"."ws_order_number" as "ws_order_number"
FROM
    "rambunctious"),
late as (
SELECT
    "sweltering"."ss_date_quarter" as "ss_date_quarter",
    "sweltering"."ss_date_year" as "ss_date_year",
    "sweltering"."ss_ext_sales_price" as "ss_ext_sales_price",
    "sweltering"."ss_item_id" as "ss_item_id",
    "sweltering"."ss_ticket_number" as "ss_ticket_number"
FROM
    "sweltering"),
questionable as (
SELECT
    "cooperative"."cs_date_quarter" as "cs_date_quarter",
    "cooperative"."cs_date_year" as "cs_date_year",
    "cooperative"."cs_ext_sales_price" as "cs_ext_sales_price",
    "cooperative"."cs_item_id" as "cs_item_id",
    "cooperative"."cs_order_number" as "cs_order_number"
FROM
    "cooperative"),
resonant as (
SELECT
    "puffy"."ws_date_quarter" as "___tvf_arm_1_d_qoy",
    "puffy"."ws_date_year" as "___tvf_arm_1_d_year",
    "ws_item_items"."I_CATEGORY" as "___tvf_arm_1_i_category",
    :___tvf_arm_1_channel as "___tvf_arm_1_channel",
    :___tvf_arm_1_col_name as "___tvf_arm_1_col_name",
    sum("puffy"."ws_ext_sales_price") as "___tvf_arm_1_sales_amt",
    sum("yellow"."ws_row_flag") as "___tvf_arm_1_sales_cnt"
FROM
    "yellow"
    INNER JOIN "puffy" on "yellow"."ws_item_id" = "puffy"."ws_item_id" AND "yellow"."ws_order_number" = "puffy"."ws_order_number"
    INNER JOIN "memory"."item" as "ws_item_items" on "yellow"."ws_item_id" = "ws_item_items"."I_ITEM_SK"
GROUP BY
    1,
    2,
    3),
kaput as (
SELECT
    "late"."ss_date_quarter" as "___tvf_arm_0_d_qoy",
    "late"."ss_date_year" as "___tvf_arm_0_d_year",
    "ss_item_items"."I_CATEGORY" as "___tvf_arm_0_i_category",
    :___tvf_arm_0_channel as "___tvf_arm_0_channel",
    :___tvf_arm_0_col_name as "___tvf_arm_0_col_name",
    sum("friendly"."ss_row_flag") as "___tvf_arm_0_sales_cnt",
    sum("late"."ss_ext_sales_price") as "___tvf_arm_0_sales_amt"
FROM
    "friendly"
    INNER JOIN "late" on "friendly"."ss_item_id" = "late"."ss_item_id" AND "friendly"."ss_ticket_number" = "late"."ss_ticket_number"
    INNER JOIN "memory"."item" as "ss_item_items" on "friendly"."ss_item_id" = "ss_item_items"."I_ITEM_SK"
GROUP BY
    1,
    2,
    3),
uneven as (
SELECT
    "cs_item_items"."I_CATEGORY" as "___tvf_arm_2_i_category",
    "questionable"."cs_date_quarter" as "___tvf_arm_2_d_qoy",
    "questionable"."cs_date_year" as "___tvf_arm_2_d_year",
    :___tvf_arm_2_channel as "___tvf_arm_2_channel",
    :___tvf_arm_2_col_name as "___tvf_arm_2_col_name",
    sum("highfalutin"."cs_row_flag") as "___tvf_arm_2_sales_cnt",
    sum("questionable"."cs_ext_sales_price") as "___tvf_arm_2_sales_amt"
FROM
    "questionable"
    INNER JOIN "highfalutin" on "questionable"."cs_item_id" = "highfalutin"."cs_item_id" AND "questionable"."cs_order_number" = "highfalutin"."cs_order_number"
    INNER JOIN "memory"."item" as "cs_item_items" on "questionable"."cs_item_id" = "cs_item_items"."I_ITEM_SK"
GROUP BY
    1,
    2,
    3),
elated as (
SELECT
    "uneven"."___tvf_arm_2_channel" as "channel",
    "uneven"."___tvf_arm_2_col_name" as "col_name",
    "uneven"."___tvf_arm_2_d_year" as "d_year",
    "uneven"."___tvf_arm_2_d_qoy" as "d_qoy",
    "uneven"."___tvf_arm_2_i_category" as "i_category",
    "uneven"."___tvf_arm_2_sales_cnt" as "sales_cnt",
    "uneven"."___tvf_arm_2_sales_amt" as "sales_amt"
FROM
    "uneven"
UNION ALL
SELECT
    "kaput"."___tvf_arm_0_channel" as "channel",
    "kaput"."___tvf_arm_0_col_name" as "col_name",
    "kaput"."___tvf_arm_0_d_year" as "d_year",
    "kaput"."___tvf_arm_0_d_qoy" as "d_qoy",
    "kaput"."___tvf_arm_0_i_category" as "i_category",
    "kaput"."___tvf_arm_0_sales_cnt" as "sales_cnt",
    "kaput"."___tvf_arm_0_sales_amt" as "sales_amt"
FROM
    "kaput"
UNION ALL
SELECT
    "resonant"."___tvf_arm_1_channel" as "channel",
    "resonant"."___tvf_arm_1_col_name" as "col_name",
    "resonant"."___tvf_arm_1_d_year" as "d_year",
    "resonant"."___tvf_arm_1_d_qoy" as "d_qoy",
    "resonant"."___tvf_arm_1_i_category" as "i_category",
    "resonant"."___tvf_arm_1_sales_cnt" as "sales_cnt",
    "resonant"."___tvf_arm_1_sales_amt" as "sales_amt"
FROM
    "resonant")
SELECT
    "elated"."channel" as "q76_results_channel",
    "elated"."col_name" as "q76_results_col_name",
    "elated"."d_year" as "q76_results_d_year",
    "elated"."d_qoy" as "q76_results_d_qoy",
    "elated"."i_category" as "q76_results_i_category",
    "elated"."sales_cnt" as "q76_results_sales_cnt",
    "elated"."sales_amt" as "q76_results_sales_amt"
FROM
    "elated"
ORDER BY 
    "q76_results_channel" asc nulls first,
    "q76_results_col_name" asc nulls first,
    "q76_results_d_year" asc nulls first,
    "q76_results_d_qoy" asc nulls first,
    "q76_results_i_category" asc nulls first
LIMIT (100)