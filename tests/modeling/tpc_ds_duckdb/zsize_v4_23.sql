
WITH 
late as (
SELECT
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "sales_billing_customer_id",
    "sales_store_sales_unified"."SS_QUANTITY" as "sales_quantity",
    "sales_store_sales_unified"."SS_SALES_PRICE" as "sales_sales_price",
    "sales_store_sales_unified"."SS_SOLD_DATE_SK" as "sales_date_id"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
WHERE
    "sales_store_sales_unified"."SS_CUSTOMER_SK" is not null and  'STORE'  = 'STORE'
),
uneven as (
SELECT
    cast("sales_date_date"."D_DATE" as date) as "sales_date_date",
    coalesce("sales_store_returns_unified"."SR_ITEM_SK","sales_store_sales_unified"."SS_ITEM_SK") as "sales_item_id",
    coalesce("sales_store_returns_unified"."SR_TICKET_NUMBER","sales_store_sales_unified"."SS_TICKET_NUMBER") as "sales_order_id"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    LEFT OUTER JOIN "memory"."store_returns" as "sales_store_returns_unified" on  'STORE'  =  'STORE'  AND "sales_store_sales_unified"."SS_ITEM_SK" = "sales_store_returns_unified"."SR_ITEM_SK" AND "sales_store_sales_unified"."SS_TICKET_NUMBER" = "sales_store_returns_unified"."SR_TICKET_NUMBER"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
     'STORE'  = 'STORE' and "sales_date_date"."D_YEAR" in (2000,2001,2002,2003)
),
yummy as (
SELECT
    "sales_item_items"."I_ITEM_SK" as "_frequent_items_frequent_item_id",
    SUBSTRING("sales_item_items"."I_ITEM_DESC",1,30) as "sales_item_desc_truncated"
FROM
    "memory"."item" as "sales_item_items"),
charming as (
SELECT
    "late"."sales_billing_customer_id" as "_best_customers_best_customer_id",
    sum("late"."sales_quantity" * "late"."sales_sales_price") as "customer_total_overall"
FROM
    "late"
GROUP BY
    1),
macho as (
SELECT
    sum("late"."sales_quantity" * "late"."sales_sales_price") as "customer_total_in_window"
FROM
    "late"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "late"."sales_date_id" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_date_date"."D_YEAR" in (2000,2001,2002,2003)

GROUP BY
    "late"."sales_billing_customer_id"),
vacuous as (
SELECT
    "uneven"."sales_date_date" as "sales_date_date",
    "uneven"."sales_item_id" as "sales_item_id",
    "uneven"."sales_order_id" as "sales_order_id",
    "yummy"."sales_item_desc_truncated" as "sales_item_desc_truncated"
FROM
    "uneven"
    LEFT OUTER JOIN "yummy" on "uneven"."sales_item_id" = "yummy"."_frequent_items_frequent_item_id"
GROUP BY
    1,
    2,
    3,
    4),
kaput as (
SELECT
    max("macho"."customer_total_in_window") as "_max_total_cmax"
FROM
    "macho"),
young as (
SELECT
    "vacuous"."sales_item_id" as "sales_item_id"
FROM
    "vacuous"
GROUP BY
    1,
    "vacuous"."sales_date_date",
    "vacuous"."sales_item_desc_truncated"
HAVING
    count("vacuous"."sales_order_id") > 4
),
busy as (
SELECT
    "kaput"."_max_total_cmax" as "max_total_cmax"
FROM
    "kaput"),
sparkling as (
SELECT
    "yummy"."_frequent_items_frequent_item_id" as "_frequent_items_frequent_item_id"
FROM
    "young"
    LEFT OUTER JOIN "yummy" on "young"."sales_item_id" = "yummy"."_frequent_items_frequent_item_id"),
waggish as (
SELECT
    "charming"."_best_customers_best_customer_id" as "_best_customers_best_customer_id"
FROM
    "charming"
    INNER JOIN "busy" on 1=1
WHERE
    "charming"."customer_total_overall" > 0.5 * "busy"."max_total_cmax"
),
abhorrent as (
SELECT
    "sparkling"."_frequent_items_frequent_item_id" as "_frequent_items_frequent_item_id"
FROM
    "sparkling"),
rambunctious as (
SELECT
    "waggish"."_best_customers_best_customer_id" as "_best_customers_best_customer_id"
FROM
    "waggish"),
sweltering as (
SELECT
    "abhorrent"."_frequent_items_frequent_item_id" as "frequent_items_frequent_item_id"
FROM
    "abhorrent"),
puffy as (
SELECT
    "rambunctious"."_best_customers_best_customer_id" as "best_customers_best_customer_id"
FROM
    "rambunctious"),
thoughtful as (
SELECT
    "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" as "sales_billing_customer_id",
     'CATALOG'  as "sales_channel",
    "sales_catalog_sales_unified"."CS_LIST_PRICE" as "sales_list_price",
    "sales_catalog_sales_unified"."CS_QUANTITY" as "sales_quantity"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
    INNER JOIN "memory"."customer" as "sales_billing_customer_customers" on "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" = "sales_billing_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_catalog_sales_unified"."CS_ITEM_SK" in (select sweltering."frequent_items_frequent_item_id" from sweltering where sweltering."frequent_items_frequent_item_id" is not null) and "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" in (select puffy."best_customers_best_customer_id" from puffy where puffy."best_customers_best_customer_id" is not null) and "sales_date_date"."D_YEAR" = 2000 and "sales_date_date"."D_MOY" = 2

UNION ALL
SELECT
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "sales_billing_customer_id",
     'STORE'  as "sales_channel",
    "sales_store_sales_unified"."SS_LIST_PRICE" as "sales_list_price",
    "sales_store_sales_unified"."SS_QUANTITY" as "sales_quantity"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "memory"."customer" as "sales_billing_customer_customers" on "sales_store_sales_unified"."SS_CUSTOMER_SK" = "sales_billing_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_store_sales_unified"."SS_ITEM_SK" in (select sweltering."frequent_items_frequent_item_id" from sweltering where sweltering."frequent_items_frequent_item_id" is not null) and "sales_store_sales_unified"."SS_CUSTOMER_SK" in (select puffy."best_customers_best_customer_id" from puffy where puffy."best_customers_best_customer_id" is not null) and "sales_date_date"."D_YEAR" = 2000 and "sales_date_date"."D_MOY" = 2

UNION ALL
SELECT
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" as "sales_billing_customer_id",
     'WEB'  as "sales_channel",
    "sales_web_sales_unified"."WS_LIST_PRICE" as "sales_list_price",
    "sales_web_sales_unified"."WS_QUANTITY" as "sales_quantity"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "memory"."customer" as "sales_billing_customer_customers" on "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" = "sales_billing_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_web_sales_unified"."WS_ITEM_SK" in (select sweltering."frequent_items_frequent_item_id" from sweltering where sweltering."frequent_items_frequent_item_id" is not null) and "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" in (select puffy."best_customers_best_customer_id" from puffy where puffy."best_customers_best_customer_id" is not null) and "sales_date_date"."D_YEAR" = 2000 and "sales_date_date"."D_MOY" = 2
),
questionable as (
SELECT
    "sales_billing_customer_customers"."C_FIRST_NAME" as "sales_billing_customer_first_name",
    "sales_billing_customer_customers"."C_LAST_NAME" as "sales_billing_customer_last_name",
    "thoughtful"."sales_channel" as "sales_channel",
    "thoughtful"."sales_list_price" as "sales_list_price",
    "thoughtful"."sales_quantity" as "sales_quantity"
FROM
    "thoughtful"
    LEFT OUTER JOIN "memory"."customer" as "sales_billing_customer_customers" on "thoughtful"."sales_billing_customer_id" = "sales_billing_customer_customers"."C_CUSTOMER_SK"),
hard as (
SELECT
    "questionable"."sales_billing_customer_first_name" as "sales_billing_customer_first_name",
    "questionable"."sales_billing_customer_last_name" as "sales_billing_customer_last_name",
    sum(CASE WHEN "questionable"."sales_channel" in ('WEB','CATALOG') THEN "questionable"."sales_quantity" * "questionable"."sales_list_price" ELSE NULL END) as "sales_total"
FROM
    "questionable"
GROUP BY
    1,
    2
HAVING
    "sales_total" > 0
)
SELECT
    "hard"."sales_billing_customer_last_name" as "c_last_name",
    "hard"."sales_billing_customer_first_name" as "c_first_name",
    "hard"."sales_total" as "sales_total"
FROM
    "hard"
ORDER BY 
    "c_last_name" asc nulls first,
    "c_first_name" asc nulls first,
    "hard"."sales_total" asc nulls first
LIMIT (100)