
WITH 
cheerful as (
SELECT
    "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" as "sales_customer_id",
    "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "sales_date_id",
    "sales_catalog_sales_unified"."CS_EXT_DISCOUNT_AMT" as "sales_ext_discount_amount",
    "sales_catalog_sales_unified"."CS_EXT_LIST_PRICE" as "sales_ext_list_price",
    "sales_catalog_sales_unified"."CS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_catalog_sales_unified"."CS_EXT_WHOLESALE_COST" as "sales_ext_wholesale_cost",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
UNION ALL
SELECT
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "sales_customer_id",
    "sales_store_sales_unified"."SS_SOLD_DATE_SK" as "sales_date_id",
    "sales_store_sales_unified"."SS_EXT_DISCOUNT_AMT" as "sales_ext_discount_amount",
    "sales_store_sales_unified"."SS_EXT_LIST_PRICE" as "sales_ext_list_price",
    "sales_store_sales_unified"."SS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_store_sales_unified"."SS_EXT_WHOLESALE_COST" as "sales_ext_wholesale_cost",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
UNION ALL
SELECT
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" as "sales_customer_id",
    "sales_web_sales_unified"."WS_SOLD_DATE_SK" as "sales_date_id",
    "sales_web_sales_unified"."WS_EXT_DISCOUNT_AMT" as "sales_ext_discount_amount",
    "sales_web_sales_unified"."WS_EXT_LIST_PRICE" as "sales_ext_list_price",
    "sales_web_sales_unified"."WS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_web_sales_unified"."WS_EXT_WHOLESALE_COST" as "sales_ext_wholesale_cost",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"),
cooperative as (
SELECT
    "cheerful"."sales_customer_id" as "sales_customer_id",
    sum((( ( "cheerful"."sales_ext_list_price" - "cheerful"."sales_ext_wholesale_cost" ) - "cheerful"."sales_ext_discount_amount" ) + "cheerful"."sales_ext_sales_price") / CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_YEAR" = 2001 THEN 2 ELSE NULL END) as "catalog_first_year",
    sum((( ( "cheerful"."sales_ext_list_price" - "cheerful"."sales_ext_wholesale_cost" ) - "cheerful"."sales_ext_discount_amount" ) + "cheerful"."sales_ext_sales_price") / CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_YEAR" = 2002 THEN 2 ELSE NULL END) as "catalog_second_year",
    sum((( ( "cheerful"."sales_ext_list_price" - "cheerful"."sales_ext_wholesale_cost" ) - "cheerful"."sales_ext_discount_amount" ) + "cheerful"."sales_ext_sales_price") / CASE WHEN "cheerful"."sales_sales_channel" = 'STORE' and "sales_date_date"."D_YEAR" = 2001 THEN 2 ELSE NULL END) as "store_first_year",
    sum((( ( "cheerful"."sales_ext_list_price" - "cheerful"."sales_ext_wholesale_cost" ) - "cheerful"."sales_ext_discount_amount" ) + "cheerful"."sales_ext_sales_price") / CASE WHEN "cheerful"."sales_sales_channel" = 'STORE' and "sales_date_date"."D_YEAR" = 2002 THEN 2 ELSE NULL END) as "store_second_year",
    sum((( ( "cheerful"."sales_ext_list_price" - "cheerful"."sales_ext_wholesale_cost" ) - "cheerful"."sales_ext_discount_amount" ) + "cheerful"."sales_ext_sales_price") / CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_YEAR" = 2001 THEN 2 ELSE NULL END) as "web_first_year",
    sum((( ( "cheerful"."sales_ext_list_price" - "cheerful"."sales_ext_wholesale_cost" ) - "cheerful"."sales_ext_discount_amount" ) + "cheerful"."sales_ext_sales_price") / CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_YEAR" = 2002 THEN 2 ELSE NULL END) as "web_second_year"
FROM
    "cheerful"
    LEFT OUTER JOIN "memory"."date_dim" as "sales_date_date" on "cheerful"."sales_date_id" = "sales_date_date"."D_DATE_SK"
GROUP BY
    1),
yummy as (
SELECT
    "cooperative"."catalog_first_year" as "catalog_first_year",
    "cooperative"."catalog_second_year" as "catalog_second_year",
    "cooperative"."store_first_year" as "store_first_year",
    "cooperative"."store_second_year" as "store_second_year",
    "cooperative"."web_first_year" as "web_first_year",
    "cooperative"."web_second_year" as "web_second_year",
    "sales_customer_customers"."C_CUSTOMER_ID" as "sales_customer_text_id",
    "sales_customer_customers"."C_FIRST_NAME" as "sales_customer_first_name",
    "sales_customer_customers"."C_LAST_NAME" as "sales_customer_last_name",
    "sales_customer_customers"."C_PREFERRED_CUST_FLAG" as "sales_customer_preferred_cust_flag"
FROM
    "cooperative"
    LEFT OUTER JOIN "memory"."customer" as "sales_customer_customers" on "cooperative"."sales_customer_id" = "sales_customer_customers"."C_CUSTOMER_SK"),
young as (
SELECT
    "yummy"."sales_customer_preferred_cust_flag" as "customer_preferred_cust_flag"
FROM
    "yummy"),
concerned as (
SELECT
    "yummy"."sales_customer_last_name" as "customer_last_name"
FROM
    "yummy"),
vacuous as (
SELECT
    "yummy"."sales_customer_text_id" as "customer_id"
FROM
    "yummy"
WHERE
    "yummy"."store_first_year" > 0 and "yummy"."catalog_first_year" > 0 and "yummy"."web_first_year" > 0 and ( CASE
	WHEN "yummy"."catalog_first_year" > 0 THEN "yummy"."catalog_second_year" / "yummy"."catalog_first_year"
	ELSE null
	END ) > ( CASE
	WHEN "yummy"."store_first_year" > 0 THEN "yummy"."store_second_year" / "yummy"."store_first_year"
	ELSE null
	END ) and ( CASE
	WHEN "yummy"."catalog_first_year" > 0 THEN "yummy"."catalog_second_year" / "yummy"."catalog_first_year"
	ELSE null
	END ) > ( CASE
	WHEN "yummy"."web_first_year" > 0 THEN "yummy"."web_second_year" / "yummy"."web_first_year"
	ELSE null
	END )
),
juicy as (
SELECT
    "yummy"."sales_customer_first_name" as "customer_first_name"
FROM
    "yummy")
SELECT
    "vacuous"."customer_id" as "customer_id",
    "juicy"."customer_first_name" as "customer_first_name",
    "concerned"."customer_last_name" as "customer_last_name",
    "young"."customer_preferred_cust_flag" as "customer_preferred_cust_flag"
FROM
    "juicy"
    FULL JOIN "vacuous" on 1=1
    FULL JOIN "concerned" on 1=1
    FULL JOIN "young" on 1=1
ORDER BY 
    "vacuous"."customer_id" asc nulls first,
    "juicy"."customer_first_name" asc nulls first,
    "concerned"."customer_last_name" asc nulls first,
    "young"."customer_preferred_cust_flag" asc nulls first
LIMIT (100)