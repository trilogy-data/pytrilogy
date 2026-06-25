
WITH 
courageous as (
SELECT
    "ws_web_sales"."WS_ITEM_SK" as "ws_item_id",
    "ws_web_sales"."WS_ORDER_NUMBER" as "ws_order_number",
    "ws_web_sales"."WS_SHIP_CUSTOMER_SK" as "ws_ship_customer_id"
FROM
    "memory"."web_sales" as "ws_web_sales"
WHERE
    "ws_web_sales"."WS_SHIP_CUSTOMER_SK" is null and "ws_web_sales"."WS_SOLD_DATE_SK" is not null
),
kaput as (
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
yellow as (
SELECT
    "ws_date_date"."D_QOY" as "ws_date_quarter",
    "ws_date_date"."D_YEAR" as "ws_date_year",
    "ws_item_items"."I_CATEGORY" as "ws_item_category",
    "ws_item_items"."I_ITEM_SK" as "ws_item_id",
    "ws_web_sales"."WS_EXT_SALES_PRICE" as "ws_ext_sales_price",
    "ws_web_sales"."WS_ORDER_NUMBER" as "ws_order_number"
FROM
    "memory"."web_sales" as "ws_web_sales"
    INNER JOIN "memory"."date_dim" as "ws_date_date" on "ws_web_sales"."WS_SOLD_DATE_SK" = "ws_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "ws_item_items" on "ws_web_sales"."WS_ITEM_SK" = "ws_item_items"."I_ITEM_SK"
WHERE
    "ws_web_sales"."WS_SHIP_CUSTOMER_SK" is null and "ws_web_sales"."WS_SOLD_DATE_SK" is not null
),
macho as (
SELECT
    "ss_date_date"."D_QOY" as "ss_date_quarter",
    "ss_date_date"."D_YEAR" as "ss_date_year",
    "ss_item_items"."I_CATEGORY" as "ss_item_category",
    "ss_item_items"."I_ITEM_SK" as "ss_item_id",
    "ss_store_sales"."SS_EXT_SALES_PRICE" as "ss_ext_sales_price",
    "ss_store_sales"."SS_TICKET_NUMBER" as "ss_ticket_number"
FROM
    "memory"."store_sales" as "ss_store_sales"
    INNER JOIN "memory"."date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "ss_item_items" on "ss_store_sales"."SS_ITEM_SK" = "ss_item_items"."I_ITEM_SK"
WHERE
    "ss_store_sales"."SS_STORE_SK" is null and "ss_store_sales"."SS_SOLD_DATE_SK" is not null
),
abundant as (
SELECT
    "cs_catalog_sales"."CS_EXT_SALES_PRICE" as "cs_ext_sales_price",
    "cs_catalog_sales"."CS_ITEM_SK" as "cs_item_id",
    "cs_catalog_sales"."CS_ORDER_NUMBER" as "cs_order_number",
    "cs_date_date"."D_QOY" as "cs_date_quarter",
    "cs_date_date"."D_YEAR" as "cs_date_year",
    "cs_item_items"."I_CATEGORY" as "cs_item_category"
FROM
    "memory"."catalog_sales" as "cs_catalog_sales"
    INNER JOIN "memory"."date_dim" as "cs_date_date" on "cs_catalog_sales"."CS_SOLD_DATE_SK" = "cs_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "cs_item_items" on "cs_catalog_sales"."CS_ITEM_SK" = "cs_item_items"."I_ITEM_SK"
WHERE
    "cs_catalog_sales"."CS_SHIP_ADDR_SK" is null and "cs_catalog_sales"."CS_SOLD_DATE_SK" is not null
),
vast as (
SELECT
    "courageous"."ws_item_id" as "ws_item_id",
    "courageous"."ws_order_number" as "ws_order_number",
    "courageous"."ws_ship_customer_id" as "ws_ship_customer_id"
FROM
    "courageous"),
divergent as (
SELECT
    "kaput"."ss_item_id" as "ss_item_id",
    "kaput"."ss_store_id" as "ss_store_id",
    "kaput"."ss_ticket_number" as "ss_ticket_number"
FROM
    "kaput"),
highfalutin as (
SELECT
    "quizzical"."cs_customer_address_id" as "cs_customer_address_id",
    "quizzical"."cs_item_id" as "cs_item_id",
    "quizzical"."cs_order_number" as "cs_order_number"
FROM
    "quizzical"),
resonant as (
SELECT
    "yellow"."ws_date_quarter" as "ws_date_quarter",
    "yellow"."ws_date_year" as "ws_date_year",
    "yellow"."ws_ext_sales_price" as "ws_ext_sales_price",
    "yellow"."ws_item_category" as "ws_item_category",
    "yellow"."ws_item_id" as "ws_item_id",
    "yellow"."ws_order_number" as "ws_order_number"
FROM
    "yellow"),
scrawny as (
SELECT
    "macho"."ss_date_quarter" as "ss_date_quarter",
    "macho"."ss_date_year" as "ss_date_year",
    "macho"."ss_ext_sales_price" as "ss_ext_sales_price",
    "macho"."ss_item_category" as "ss_item_category",
    "macho"."ss_item_id" as "ss_item_id",
    "macho"."ss_ticket_number" as "ss_ticket_number"
FROM
    "macho"),
uneven as (
SELECT
    "abundant"."cs_date_quarter" as "cs_date_quarter",
    "abundant"."cs_date_year" as "cs_date_year",
    "abundant"."cs_ext_sales_price" as "cs_ext_sales_price",
    "abundant"."cs_item_category" as "cs_item_category",
    "abundant"."cs_item_id" as "cs_item_id",
    "abundant"."cs_order_number" as "cs_order_number"
FROM
    "abundant"),
cool as (
SELECT
    "vast"."ws_item_id" as "ws_item_id",
    "vast"."ws_order_number" as "ws_order_number",
    "vast"."ws_ship_customer_id" as "ws_ship_customer_id"
FROM
    "vast"),
busy as (
SELECT
    "divergent"."ss_item_id" as "ss_item_id",
    "divergent"."ss_store_id" as "ss_store_id",
    "divergent"."ss_ticket_number" as "ss_ticket_number"
FROM
    "divergent"),
wakeful as (
SELECT
    "highfalutin"."cs_customer_address_id" as "cs_customer_address_id",
    "highfalutin"."cs_item_id" as "cs_item_id",
    "highfalutin"."cs_order_number" as "cs_order_number"
FROM
    "highfalutin"),
dapper as (
SELECT
    "resonant"."ws_date_quarter" as "ws_date_quarter",
    "resonant"."ws_date_year" as "ws_date_year",
    "resonant"."ws_ext_sales_price" as "ws_ext_sales_price",
    "resonant"."ws_item_category" as "ws_item_category",
    "resonant"."ws_item_id" as "ws_item_id",
    "resonant"."ws_order_number" as "ws_order_number"
FROM
    "resonant"),
friendly as (
SELECT
    "scrawny"."ss_date_quarter" as "ss_date_quarter",
    "scrawny"."ss_date_year" as "ss_date_year",
    "scrawny"."ss_ext_sales_price" as "ss_ext_sales_price",
    "scrawny"."ss_item_category" as "ss_item_category",
    "scrawny"."ss_item_id" as "ss_item_id",
    "scrawny"."ss_ticket_number" as "ss_ticket_number"
FROM
    "scrawny"),
yummy as (
SELECT
    "uneven"."cs_date_quarter" as "cs_date_quarter",
    "uneven"."cs_date_year" as "cs_date_year",
    "uneven"."cs_ext_sales_price" as "cs_ext_sales_price",
    "uneven"."cs_item_category" as "cs_item_category",
    "uneven"."cs_item_id" as "cs_item_id",
    "uneven"."cs_order_number" as "cs_order_number"
FROM
    "uneven"),
elated as (
SELECT
    "cool"."ws_item_id" as "ws_item_id",
    "cool"."ws_order_number" as "ws_order_number",
    CASE
	WHEN "cool"."ws_ship_customer_id" is null THEN 1
	ELSE 0
	END as "ws_row_flag"
FROM
    "cool"),
charming as (
SELECT
    "busy"."ss_item_id" as "ss_item_id",
    "busy"."ss_ticket_number" as "ss_ticket_number",
    CASE
	WHEN "busy"."ss_store_id" is null THEN 1
	ELSE 0
	END as "ss_row_flag"
FROM
    "busy"),
cheerful as (
SELECT
    "wakeful"."cs_item_id" as "cs_item_id",
    "wakeful"."cs_order_number" as "cs_order_number",
    CASE
	WHEN "wakeful"."cs_customer_address_id" is null THEN 1
	ELSE 0
	END as "cs_row_flag"
FROM
    "wakeful"),
wary as (
SELECT
    "dapper"."ws_date_quarter" as "___tvf_arm_1_d_qoy",
    "dapper"."ws_date_year" as "___tvf_arm_1_d_year",
    "dapper"."ws_item_category" as "___tvf_arm_1_i_category",
    :___tvf_arm_1_channel as "___tvf_arm_1_channel",
    :___tvf_arm_1_col_name as "___tvf_arm_1_col_name",
    sum("dapper"."ws_ext_sales_price") as "___tvf_arm_1_sales_amt",
    sum("elated"."ws_row_flag") as "___tvf_arm_1_sales_cnt"
FROM
    "elated"
    INNER JOIN "dapper" on "elated"."ws_item_id" = "dapper"."ws_item_id" AND "elated"."ws_order_number" = "dapper"."ws_order_number"
GROUP BY
    1,
    2,
    3),
protective as (
SELECT
    "friendly"."ss_date_quarter" as "___tvf_arm_0_d_qoy",
    "friendly"."ss_date_year" as "___tvf_arm_0_d_year",
    "friendly"."ss_item_category" as "___tvf_arm_0_i_category",
    :___tvf_arm_0_channel as "___tvf_arm_0_channel",
    :___tvf_arm_0_col_name as "___tvf_arm_0_col_name",
    sum("charming"."ss_row_flag") as "___tvf_arm_0_sales_cnt",
    sum("friendly"."ss_ext_sales_price") as "___tvf_arm_0_sales_amt"
FROM
    "charming"
    INNER JOIN "friendly" on "charming"."ss_item_id" = "friendly"."ss_item_id" AND "charming"."ss_ticket_number" = "friendly"."ss_ticket_number"
GROUP BY
    1,
    2,
    3),
juicy as (
SELECT
    "yummy"."cs_date_quarter" as "___tvf_arm_2_d_qoy",
    "yummy"."cs_date_year" as "___tvf_arm_2_d_year",
    "yummy"."cs_item_category" as "___tvf_arm_2_i_category",
    :___tvf_arm_2_channel as "___tvf_arm_2_channel",
    :___tvf_arm_2_col_name as "___tvf_arm_2_col_name",
    sum("cheerful"."cs_row_flag") as "___tvf_arm_2_sales_cnt",
    sum("yummy"."cs_ext_sales_price") as "___tvf_arm_2_sales_amt"
FROM
    "yummy"
    INNER JOIN "cheerful" on "yummy"."cs_item_id" = "cheerful"."cs_item_id" AND "yummy"."cs_order_number" = "cheerful"."cs_order_number"
GROUP BY
    1,
    2,
    3),
tearful as (
SELECT
    "juicy"."___tvf_arm_2_channel" as "channel",
    "juicy"."___tvf_arm_2_col_name" as "col_name",
    "juicy"."___tvf_arm_2_d_year" as "d_year",
    "juicy"."___tvf_arm_2_d_qoy" as "d_qoy",
    "juicy"."___tvf_arm_2_i_category" as "i_category",
    "juicy"."___tvf_arm_2_sales_cnt" as "sales_cnt",
    "juicy"."___tvf_arm_2_sales_amt" as "sales_amt"
FROM
    "juicy"
UNION ALL
SELECT
    "protective"."___tvf_arm_0_channel" as "channel",
    "protective"."___tvf_arm_0_col_name" as "col_name",
    "protective"."___tvf_arm_0_d_year" as "d_year",
    "protective"."___tvf_arm_0_d_qoy" as "d_qoy",
    "protective"."___tvf_arm_0_i_category" as "i_category",
    "protective"."___tvf_arm_0_sales_cnt" as "sales_cnt",
    "protective"."___tvf_arm_0_sales_amt" as "sales_amt"
FROM
    "protective"
UNION ALL
SELECT
    "wary"."___tvf_arm_1_channel" as "channel",
    "wary"."___tvf_arm_1_col_name" as "col_name",
    "wary"."___tvf_arm_1_d_year" as "d_year",
    "wary"."___tvf_arm_1_d_qoy" as "d_qoy",
    "wary"."___tvf_arm_1_i_category" as "i_category",
    "wary"."___tvf_arm_1_sales_cnt" as "sales_cnt",
    "wary"."___tvf_arm_1_sales_amt" as "sales_amt"
FROM
    "wary")
SELECT
    "tearful"."channel" as "q76_results_channel",
    "tearful"."col_name" as "q76_results_col_name",
    "tearful"."d_year" as "q76_results_d_year",
    "tearful"."d_qoy" as "q76_results_d_qoy",
    "tearful"."i_category" as "q76_results_i_category",
    "tearful"."sales_cnt" as "q76_results_sales_cnt",
    "tearful"."sales_amt" as "q76_results_sales_amt"
FROM
    "tearful"
ORDER BY 
    "q76_results_channel" asc nulls first,
    "q76_results_col_name" asc nulls first,
    "q76_results_d_year" asc nulls first,
    "q76_results_d_qoy" asc nulls first,
    "q76_results_i_category" asc nulls first
LIMIT (100)