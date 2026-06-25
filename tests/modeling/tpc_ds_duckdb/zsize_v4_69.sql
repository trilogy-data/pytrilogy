
WITH 
late as (
SELECT
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" as "_web_buyers_web_cust_id"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" is not null and "sales_date_date"."D_YEAR" = 2001 and "sales_date_date"."D_MOY" BETWEEN 4 AND 6 and  'WEB'  = 'WEB'

GROUP BY
    1),
vacuous as (
SELECT
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "_store_buyers_store_cust_id"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_store_sales_unified"."SS_CUSTOMER_SK" is not null and "sales_date_date"."D_YEAR" = 2001 and "sales_date_date"."D_MOY" BETWEEN 4 AND 6 and  'STORE'  = 'STORE'

GROUP BY
    1),
thoughtful as (
SELECT
    "sales_catalog_sales_unified"."CS_SHIP_CUSTOMER_SK" as "_catalog_buyers_cat_cust_id"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_catalog_sales_unified"."CS_SHIP_CUSTOMER_SK" is not null and "sales_date_date"."D_YEAR" = 2001 and "sales_date_date"."D_MOY" BETWEEN 4 AND 6 and  'CATALOG'  = 'CATALOG'

GROUP BY
    1),
divergent as (
SELECT
    "late"."_web_buyers_web_cust_id" as "web_buyers_web_cust_id"
FROM
    "late"),
sweltering as (
SELECT
    "vacuous"."_store_buyers_store_cust_id" as "store_buyers_store_cust_id"
FROM
    "vacuous"),
juicy as (
SELECT
    "thoughtful"."_catalog_buyers_cat_cust_id" as "catalog_buyers_cat_cust_id"
FROM
    "thoughtful"),
cheerful as (
SELECT
    "customer_customers"."C_CUSTOMER_SK" as "customer_id",
    "customer_demographics_customer_demographics"."CD_CREDIT_RATING" as "customer_demographics_credit_rating",
    "customer_demographics_customer_demographics"."CD_EDUCATION_STATUS" as "customer_demographics_education_status",
    "customer_demographics_customer_demographics"."CD_GENDER" as "customer_demographics_gender",
    "customer_demographics_customer_demographics"."CD_MARITAL_STATUS" as "customer_demographics_marital_status",
    "customer_demographics_customer_demographics"."CD_PURCHASE_ESTIMATE" as "customer_demographics_purchase_estimate"
FROM
    "memory"."customer" as "customer_customers"
    INNER JOIN "memory"."customer_address" as "customer_address_customer_address" on "customer_customers"."C_CURRENT_ADDR_SK" = "customer_address_customer_address"."CA_ADDRESS_SK"
    LEFT OUTER JOIN "memory"."customer_demographics" as "customer_demographics_customer_demographics" on "customer_customers"."C_CURRENT_CDEMO_SK" = "customer_demographics_customer_demographics"."CD_DEMO_SK"
WHERE
    "customer_address_customer_address"."CA_STATE" in ('KY','GA','NM') and "customer_customers"."C_CUSTOMER_SK" in (select sweltering."store_buyers_store_cust_id" from sweltering where sweltering."store_buyers_store_cust_id" is not null) and "customer_customers"."C_CUSTOMER_SK" not in (select divergent."web_buyers_web_cust_id" from divergent where divergent."web_buyers_web_cust_id" is not null) and "customer_customers"."C_CUSTOMER_SK" not in (select juicy."catalog_buyers_cat_cust_id" from juicy where juicy."catalog_buyers_cat_cust_id" is not null)
)
SELECT
    "cheerful"."customer_demographics_gender" as "customer_demographics_gender",
    "cheerful"."customer_demographics_marital_status" as "customer_demographics_marital_status",
    "cheerful"."customer_demographics_education_status" as "customer_demographics_education_status",
    count("cheerful"."customer_id") as "cnt1",
    "cheerful"."customer_demographics_purchase_estimate" as "customer_demographics_purchase_estimate",
    count("cheerful"."customer_id") as "cnt2",
    "cheerful"."customer_demographics_credit_rating" as "customer_demographics_credit_rating",
    count("cheerful"."customer_id") as "cnt3"
FROM
    "cheerful"
GROUP BY
    1,
    2,
    3,
    5,
    7
ORDER BY 
    "cheerful"."customer_demographics_gender" asc,
    "cheerful"."customer_demographics_marital_status" asc,
    "cheerful"."customer_demographics_education_status" asc,
    "cheerful"."customer_demographics_purchase_estimate" asc,
    "cheerful"."customer_demographics_credit_rating" asc
LIMIT (100)