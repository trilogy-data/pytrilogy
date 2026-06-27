
WITH 
cheerful as (
SELECT
     'CATALOG'  as "sales_channel",
    "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "sales_date_id",
    "sales_catalog_sales_unified"."CS_SHIP_CUSTOMER_SK" as "sales_purchasing_customer_id"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
UNION ALL
SELECT
     'STORE'  as "sales_channel",
    "sales_store_sales_unified"."SS_SOLD_DATE_SK" as "sales_date_id",
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "sales_purchasing_customer_id"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
UNION ALL
SELECT
     'WEB'  as "sales_channel",
    "sales_web_sales_unified"."WS_SOLD_DATE_SK" as "sales_date_id",
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" as "sales_purchasing_customer_id"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"),
yummy as (
SELECT
    "cheerful"."sales_channel" as "sales_channel",
    "cheerful"."sales_purchasing_customer_id" as "sales_purchasing_customer_id",
    "sales_date_date"."D_MOY" as "sales_date_month_of_year",
    "sales_date_date"."D_YEAR" as "sales_date_year"
FROM
    "cheerful"
    LEFT OUTER JOIN "memory"."date_dim" as "sales_date_date" on "cheerful"."sales_date_id" = "sales_date_date"."D_DATE_SK"
    LEFT OUTER JOIN "memory"."customer" as "sales_purchasing_customer_customers" on "cheerful"."sales_purchasing_customer_id" = "sales_purchasing_customer_customers"."C_CUSTOMER_SK"),
juicy as (
SELECT
    "yummy"."sales_purchasing_customer_id" as "sales_purchasing_customer_id",
    CASE WHEN "yummy"."sales_channel" = 'STORE' and "yummy"."sales_date_year" = 2002 and "yummy"."sales_date_month_of_year" in (1,2,3,4) THEN "yummy"."sales_purchasing_customer_id" ELSE NULL END as "store_buyers",
    CASE WHEN "yummy"."sales_channel" in ('WEB','CATALOG') and "yummy"."sales_date_year" = 2002 and "yummy"."sales_date_month_of_year" in (1,2,3,4) THEN "yummy"."sales_purchasing_customer_id" ELSE NULL END as "webcat_buyers"
FROM
    "yummy"),
vacuous as (
SELECT
    "juicy"."store_buyers" as "store_buyers"
FROM
    "juicy"
GROUP BY
    1),
concerned as (
SELECT
    "juicy"."webcat_buyers" as "webcat_buyers"
FROM
    "juicy"
WHERE
    "juicy"."sales_purchasing_customer_id" in (select vacuous."store_buyers" from vacuous where vacuous."store_buyers" is not null)

GROUP BY
    1),
abhorrent as (
SELECT
    "sales_purchasing_customer_customers"."C_CUSTOMER_SK" as "sales_purchasing_customer_id",
    "sales_purchasing_customer_demographics_customer_demographics"."CD_CREDIT_RATING" as "sales_purchasing_customer_demographics_credit_rating",
    "sales_purchasing_customer_demographics_customer_demographics"."CD_DEP_COLLEGE_COUNT" as "sales_purchasing_customer_demographics_college_dependent_count",
    "sales_purchasing_customer_demographics_customer_demographics"."CD_DEP_COUNT" as "sales_purchasing_customer_demographics_dependent_count",
    "sales_purchasing_customer_demographics_customer_demographics"."CD_DEP_EMPLOYED_COUNT" as "sales_purchasing_customer_demographics_employed_dependent_count",
    "sales_purchasing_customer_demographics_customer_demographics"."CD_EDUCATION_STATUS" as "sales_purchasing_customer_demographics_education_status",
    "sales_purchasing_customer_demographics_customer_demographics"."CD_GENDER" as "sales_purchasing_customer_demographics_gender",
    "sales_purchasing_customer_demographics_customer_demographics"."CD_MARITAL_STATUS" as "sales_purchasing_customer_demographics_marital_status",
    "sales_purchasing_customer_demographics_customer_demographics"."CD_PURCHASE_ESTIMATE" as "sales_purchasing_customer_demographics_purchase_estimate"
FROM
    "memory"."customer" as "sales_purchasing_customer_customers"
    INNER JOIN "memory"."customer_address" as "sales_purchasing_customer_address_customer_address" on "sales_purchasing_customer_customers"."C_CURRENT_ADDR_SK" = "sales_purchasing_customer_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."customer_demographics" as "sales_purchasing_customer_demographics_customer_demographics" on "sales_purchasing_customer_customers"."C_CURRENT_CDEMO_SK" = "sales_purchasing_customer_demographics_customer_demographics"."CD_DEMO_SK"
WHERE
    "sales_purchasing_customer_address_customer_address"."CA_COUNTY" in ('Rush County','Toole County','Jefferson County','Dona Ana County','La Porte County') and "sales_purchasing_customer_demographics_customer_demographics"."CD_GENDER" is not null and "sales_purchasing_customer_customers"."C_CUSTOMER_SK" in (select vacuous."store_buyers" from vacuous where vacuous."store_buyers" is not null) and "sales_purchasing_customer_customers"."C_CUSTOMER_SK" in (select concerned."webcat_buyers" from concerned where concerned."webcat_buyers" is not null)
),
uneven as (
SELECT
    "cheerful"."sales_purchasing_customer_id" as "sales_purchasing_customer_id"
FROM
    "cheerful"
    LEFT OUTER JOIN "memory"."date_dim" as "sales_date_date" on "cheerful"."sales_date_id" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer" as "sales_purchasing_customer_customers" on "cheerful"."sales_purchasing_customer_id" = "sales_purchasing_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."customer_address" as "sales_purchasing_customer_address_customer_address" on "sales_purchasing_customer_customers"."C_CURRENT_ADDR_SK" = "sales_purchasing_customer_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."customer_demographics" as "sales_purchasing_customer_demographics_customer_demographics" on "sales_purchasing_customer_customers"."C_CURRENT_CDEMO_SK" = "sales_purchasing_customer_demographics_customer_demographics"."CD_DEMO_SK"
WHERE
    "sales_purchasing_customer_address_customer_address"."CA_COUNTY" in ('Rush County','Toole County','Jefferson County','Dona Ana County','La Porte County') and "sales_purchasing_customer_demographics_customer_demographics"."CD_GENDER" is not null and "cheerful"."sales_purchasing_customer_id" in (select vacuous."store_buyers" from vacuous where vacuous."store_buyers" is not null) and "cheerful"."sales_purchasing_customer_id" in (select concerned."webcat_buyers" from concerned where concerned."webcat_buyers" is not null)
),
young as (
SELECT
    "uneven"."sales_purchasing_customer_id" as "sales_purchasing_customer_id"
FROM
    "uneven"),
sparkling as (
SELECT
    "young"."sales_purchasing_customer_id" as "sales_purchasing_customer_id"
FROM
    "young"),
sweltering as (
SELECT
    "abhorrent"."sales_purchasing_customer_demographics_college_dependent_count" as "sales_purchasing_customer_demographics_college_dependent_count",
    "abhorrent"."sales_purchasing_customer_demographics_credit_rating" as "sales_purchasing_customer_demographics_credit_rating",
    "abhorrent"."sales_purchasing_customer_demographics_dependent_count" as "sales_purchasing_customer_demographics_dependent_count",
    "abhorrent"."sales_purchasing_customer_demographics_education_status" as "sales_purchasing_customer_demographics_education_status",
    "abhorrent"."sales_purchasing_customer_demographics_employed_dependent_count" as "sales_purchasing_customer_demographics_employed_dependent_count",
    "abhorrent"."sales_purchasing_customer_demographics_gender" as "sales_purchasing_customer_demographics_gender",
    "abhorrent"."sales_purchasing_customer_demographics_marital_status" as "sales_purchasing_customer_demographics_marital_status",
    "abhorrent"."sales_purchasing_customer_demographics_purchase_estimate" as "sales_purchasing_customer_demographics_purchase_estimate",
    "abhorrent"."sales_purchasing_customer_id" as "sales_purchasing_customer_id"
FROM
    "abhorrent"
WHERE
    "abhorrent"."sales_purchasing_customer_id" in (select vacuous."store_buyers" from vacuous where vacuous."store_buyers" is not null) and "abhorrent"."sales_purchasing_customer_id" in (select concerned."webcat_buyers" from concerned where concerned."webcat_buyers" is not null)

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9)
SELECT
    "sweltering"."sales_purchasing_customer_demographics_gender" as "sales_purchasing_customer_demographics_gender",
    "sweltering"."sales_purchasing_customer_demographics_marital_status" as "sales_purchasing_customer_demographics_marital_status",
    "sweltering"."sales_purchasing_customer_demographics_education_status" as "sales_purchasing_customer_demographics_education_status",
    count("sweltering"."sales_purchasing_customer_id") as "cnt1",
    "sweltering"."sales_purchasing_customer_demographics_purchase_estimate" as "sales_purchasing_customer_demographics_purchase_estimate",
    count("sweltering"."sales_purchasing_customer_id") as "cnt2",
    "sweltering"."sales_purchasing_customer_demographics_credit_rating" as "sales_purchasing_customer_demographics_credit_rating",
    count("sweltering"."sales_purchasing_customer_id") as "cnt3",
    "sweltering"."sales_purchasing_customer_demographics_dependent_count" as "sales_purchasing_customer_demographics_dependent_count",
    count("sweltering"."sales_purchasing_customer_id") as "cnt4",
    "sweltering"."sales_purchasing_customer_demographics_employed_dependent_count" as "sales_purchasing_customer_demographics_employed_dependent_count",
    count("sweltering"."sales_purchasing_customer_id") as "cnt5",
    "sweltering"."sales_purchasing_customer_demographics_college_dependent_count" as "sales_purchasing_customer_demographics_college_dependent_count",
    count("sweltering"."sales_purchasing_customer_id") as "cnt6"
FROM
    "sweltering"
GROUP BY
    1,
    2,
    3,
    5,
    7,
    9,
    11,
    13
ORDER BY 
    "sweltering"."sales_purchasing_customer_demographics_gender" asc,
    "sweltering"."sales_purchasing_customer_demographics_marital_status" asc,
    "sweltering"."sales_purchasing_customer_demographics_education_status" asc,
    "sweltering"."sales_purchasing_customer_demographics_purchase_estimate" asc,
    "sweltering"."sales_purchasing_customer_demographics_credit_rating" asc,
    "sweltering"."sales_purchasing_customer_demographics_dependent_count" asc,
    "sweltering"."sales_purchasing_customer_demographics_employed_dependent_count" asc,
    "sweltering"."sales_purchasing_customer_demographics_college_dependent_count" asc