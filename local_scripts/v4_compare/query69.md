# Query 69

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (100 rows) |
| reference execution | OK (100 rows) |
| results identical | YES |

## Result comparison

v4 rows: 100 (100 distinct)
ref rows: 100 (100 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 5240 | 118 | 62.93 ms |
| reference | 4137 | 80 | 72.93 ms |
| v4 / ref | 1.27x | 1.48x | 0.86x |

## Preql

```
import customer as customer;
import all_sales as sales;

# TPC-DS q69 uses ss_customer_sk for the store EXISTS branch,
# ws_bill_customer_sk for web, and cs_ship_customer_sk for catalog, so:
#   - store/web filters use sales.billing_customer.id (= SS_CUSTOMER_SK / WS_BILL_CUSTOMER_SK)
#   - catalog filter uses sales.ship_customer.id (= CS_SHIP_CUSTOMER_SK)
rowset store_buyers <- where
    sales.sales_channel = 'STORE'
    and sales.date.year = 2001
    and sales.date.month_of_year between 4 and 6
    and sales.billing_customer.id is not null
select
    sales.billing_customer.id as store_cust_id,
;

rowset web_buyers <- where
    sales.sales_channel = 'WEB'
    and sales.date.year = 2001
    and sales.date.month_of_year between 4 and 6
    and sales.billing_customer.id is not null
select
    sales.billing_customer.id as web_cust_id,
;

rowset catalog_buyers <- where
    sales.sales_channel = 'CATALOG'
    and sales.date.year = 2001
    and sales.date.month_of_year between 4 and 6
    and sales.ship_customer.id is not null
select
    sales.ship_customer.id as cat_cust_id,
;

where
    customer.address.state in ('KY', 'GA', 'NM')
    and customer.id in store_buyers.store_cust_id
    and customer.id not in web_buyers.web_cust_id
    and customer.id not in catalog_buyers.cat_cust_id
select
    customer.demographics.gender,
    customer.demographics.marital_status,
    customer.demographics.education_status,
    count(customer.id) as cnt1,
    customer.demographics.purchase_estimate,
    count(customer.id) as cnt2,
    customer.demographics.credit_rating,
    count(customer.id) as cnt3,
order by
    customer.demographics.gender asc,
    customer.demographics.marital_status asc,
    customer.demographics.education_status asc,
    customer.demographics.purchase_estimate asc,
    customer.demographics.credit_rating asc
limit 100
;
```

## v4 generated SQL

```sql
WITH 
abundant as (
SELECT
    "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" as "sales_billing_customer_id",
     'CATALOG'  as "sales_sales_channel",
    "sales_catalog_sales_unified"."CS_SHIP_CUSTOMER_SK" as "sales_ship_customer_id"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_date_date"."D_YEAR" = 2001 and "sales_date_date"."D_MOY" BETWEEN 4 AND 6

UNION ALL
SELECT
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "sales_billing_customer_id",
     'STORE'  as "sales_sales_channel",
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "sales_ship_customer_id"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_date_date"."D_YEAR" = 2001 and "sales_date_date"."D_MOY" BETWEEN 4 AND 6

UNION ALL
SELECT
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" as "sales_billing_customer_id",
     'WEB'  as "sales_sales_channel",
    "sales_web_sales_unified"."WS_SHIP_CUSTOMER_SK" as "sales_ship_customer_id"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_date_date"."D_YEAR" = 2001 and "sales_date_date"."D_MOY" BETWEEN 4 AND 6
),
late as (
SELECT
    "abundant"."sales_billing_customer_id" as "_store_buyers_store_cust_id"
FROM
    "abundant"
WHERE
    "abundant"."sales_sales_channel" = 'STORE' and "abundant"."sales_billing_customer_id" is not null

GROUP BY
    1),
young as (
SELECT
    "abundant"."sales_ship_customer_id" as "_catalog_buyers_cat_cust_id"
FROM
    "abundant"
WHERE
    "abundant"."sales_sales_channel" = 'CATALOG' and "abundant"."sales_ship_customer_id" is not null

GROUP BY
    1),
yummy as (
SELECT
    "abundant"."sales_billing_customer_id" as "_web_buyers_web_cust_id"
FROM
    "abundant"
WHERE
    "abundant"."sales_sales_channel" = 'WEB' and "abundant"."sales_billing_customer_id" is not null

GROUP BY
    1),
friendly as (
SELECT
    "late"."_store_buyers_store_cust_id" as "store_buyers_store_cust_id"
FROM
    "late"),
sweltering as (
SELECT
    "young"."_catalog_buyers_cat_cust_id" as "catalog_buyers_cat_cust_id"
FROM
    "young"),
concerned as (
SELECT
    "yummy"."_web_buyers_web_cust_id" as "web_buyers_web_cust_id"
FROM
    "yummy"),
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
    "customer_address_customer_address"."CA_STATE" in ('KY','GA','NM') and "customer_customers"."C_CUSTOMER_SK" in (select friendly."store_buyers_store_cust_id" from friendly where friendly."store_buyers_store_cust_id" is not null) and "customer_customers"."C_CUSTOMER_SK" not in (select concerned."web_buyers_web_cust_id" from concerned where concerned."web_buyers_web_cust_id" is not null) and "customer_customers"."C_CUSTOMER_SK" not in (select sweltering."catalog_buyers_cat_cust_id" from sweltering where sweltering."catalog_buyers_cat_cust_id" is not null)
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
```

## Reference SQL (zquery log)

```sql
WITH 
cheerful as (
SELECT
    "sales_catalog_sales_unified"."CS_SHIP_CUSTOMER_SK" as "catalog_buyers_cat_cust_id"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_date_date"."D_YEAR" = 2001 and "sales_date_date"."D_MOY" BETWEEN 4 AND 6 and  'CATALOG'  = 'CATALOG' and "sales_catalog_sales_unified"."CS_SHIP_CUSTOMER_SK" is not null

GROUP BY
    1),
abundant as (
SELECT
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "store_buyers_store_cust_id"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_date_date"."D_YEAR" = 2001 and "sales_date_date"."D_MOY" BETWEEN 4 AND 6 and  'STORE'  = 'STORE' and "sales_store_sales_unified"."SS_CUSTOMER_SK" is not null

GROUP BY
    1),
juicy as (
SELECT
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" as "web_buyers_web_cust_id"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_date_date"."D_YEAR" = 2001 and "sales_date_date"."D_MOY" BETWEEN 4 AND 6 and  'WEB'  = 'WEB' and "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" is not null

GROUP BY
    1),
young as (
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
    "customer_address_customer_address"."CA_STATE" in ('KY','GA','NM') and "customer_customers"."C_CUSTOMER_SK" in (select abundant."store_buyers_store_cust_id" from abundant where abundant."store_buyers_store_cust_id" is not null) and "customer_customers"."C_CUSTOMER_SK" not in (select juicy."web_buyers_web_cust_id" from juicy where juicy."web_buyers_web_cust_id" is not null) and "customer_customers"."C_CUSTOMER_SK" not in (select cheerful."catalog_buyers_cat_cust_id" from cheerful where cheerful."catalog_buyers_cat_cust_id" is not null)

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6)
SELECT
    "young"."customer_demographics_gender" as "customer_demographics_gender",
    "young"."customer_demographics_marital_status" as "customer_demographics_marital_status",
    "young"."customer_demographics_education_status" as "customer_demographics_education_status",
    count("young"."customer_id") as "cnt1",
    "young"."customer_demographics_purchase_estimate" as "customer_demographics_purchase_estimate",
    count("young"."customer_id") as "cnt2",
    "young"."customer_demographics_credit_rating" as "customer_demographics_credit_rating",
    count("young"."customer_id") as "cnt3"
FROM
    "young"
GROUP BY
    1,
    2,
    3,
    5,
    7
ORDER BY 
    "young"."customer_demographics_gender" asc,
    "young"."customer_demographics_marital_status" asc,
    "young"."customer_demographics_education_status" asc,
    "young"."customer_demographics_purchase_estimate" asc,
    "young"."customer_demographics_credit_rating" asc
LIMIT (100)
```
