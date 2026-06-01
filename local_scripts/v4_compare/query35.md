# Query 35

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
| v4 | 9370 | 167 | 92.68 ms |
| reference | 10103 | 157 | 120.28 ms |
| v4 / ref | 0.93x | 1.06x | 0.77x |

## Preql

```
import customer as customer;
import all_sales as sales;

# TPC-DS q35 uses cs_ship_customer_sk for the catalog EXISTS branch and
# ws_bill_customer_sk for web, so:
#   - store/web filters use sales.billing_customer.id (= SS_CUSTOMER_SK / WS_BILL_CUSTOMER_SK)
#   - catalog filter uses sales.ship_customer.id (= CS_SHIP_CUSTOMER_SK)
rowset store_buyers <- where
    sales.sales_channel = 'STORE'
    and sales.date.year = 2002
    and sales.date.quarter < 4
    and sales.billing_customer.id is not null
select
    sales.billing_customer.id as store_cust_id,
;

rowset web_buyers <- where
    sales.sales_channel = 'WEB'
    and sales.date.year = 2002
    and sales.date.quarter < 4
    and sales.billing_customer.id is not null
select
    sales.billing_customer.id as web_cust_id,
;

rowset catalog_buyers <- where
    sales.sales_channel = 'CATALOG'
    and sales.date.year = 2002
    and sales.date.quarter < 4
    and sales.ship_customer.id is not null
select
    sales.ship_customer.id as cat_cust_id,
;

where
    customer.id in store_buyers.store_cust_id
    and (customer.id in web_buyers.web_cust_id or customer.id in catalog_buyers.cat_cust_id)
    and customer.demographics.id is not null
select
    customer.address.state,
    customer.demographics.gender,
    customer.demographics.marital_status,
    customer.demographics.dependent_count,
    count(customer.id) as cnt1,
    min(customer.demographics.dependent_count) as min1,
    max(customer.demographics.dependent_count) as max1,
    avg(customer.demographics.dependent_count) as avg1,
    customer.demographics.employed_dependent_count,
    count(customer.id) as cnt2,
    min(customer.demographics.employed_dependent_count) as min2,
    max(customer.demographics.employed_dependent_count) as max2,
    avg(customer.demographics.employed_dependent_count) as avg2,
    customer.demographics.college_dependent_count,
    count(customer.id) as cnt3,
    min(customer.demographics.college_dependent_count) as min3,
    max(customer.demographics.college_dependent_count) as max3,
    avg(customer.demographics.college_dependent_count) as avg3,
order by
    customer.address.state asc nulls first,
    customer.demographics.gender asc nulls first,
    customer.demographics.marital_status asc nulls first,
    customer.demographics.dependent_count asc nulls first,
    customer.demographics.employed_dependent_count asc nulls first,
    customer.demographics.college_dependent_count asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
abhorrent as (
SELECT
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" as "sales_billing_customer_id"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_date_date"."D_YEAR" = 2002 and "sales_date_date"."D_QOY" < 4 and  'WEB'  = 'WEB' and "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" is not null

GROUP BY
    1),
juicy as (
SELECT
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "sales_billing_customer_id"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_date_date"."D_YEAR" = 2002 and "sales_date_date"."D_QOY" < 4 and  'STORE'  = 'STORE' and "sales_store_sales_unified"."SS_CUSTOMER_SK" is not null

GROUP BY
    1),
thoughtful as (
SELECT
    "sales_catalog_sales_unified"."CS_SHIP_CUSTOMER_SK" as "sales_ship_customer_id"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_date_date"."D_YEAR" = 2002 and "sales_date_date"."D_QOY" < 4 and  'CATALOG'  = 'CATALOG' and "sales_catalog_sales_unified"."CS_SHIP_CUSTOMER_SK" is not null

GROUP BY
    1),
macho as (
SELECT
    "abhorrent"."sales_billing_customer_id" as "_web_buyers_web_cust_id"
FROM
    "abhorrent"),
young as (
SELECT
    "juicy"."sales_billing_customer_id" as "_store_buyers_store_cust_id"
FROM
    "juicy"),
uneven as (
SELECT
    "thoughtful"."sales_ship_customer_id" as "_catalog_buyers_cat_cust_id"
FROM
    "thoughtful"),
scrawny as (
SELECT
    "macho"."_web_buyers_web_cust_id" as "web_buyers_web_cust_id"
FROM
    "macho"),
sparkling as (
SELECT
    "young"."_store_buyers_store_cust_id" as "store_buyers_store_cust_id"
FROM
    "young"),
yummy as (
SELECT
    "uneven"."_catalog_buyers_cat_cust_id" as "catalog_buyers_cat_cust_id"
FROM
    "uneven"),
cheerful as (
SELECT
    "customer_address_customer_address"."CA_STATE" as "customer_address_state",
    "customer_customers"."C_CUSTOMER_SK" as "customer_id",
    "customer_demographics_customer_demographics"."CD_DEP_COLLEGE_COUNT" as "customer_demographics_college_dependent_count",
    "customer_demographics_customer_demographics"."CD_DEP_COUNT" as "customer_demographics_dependent_count",
    "customer_demographics_customer_demographics"."CD_DEP_EMPLOYED_COUNT" as "customer_demographics_employed_dependent_count",
    "customer_demographics_customer_demographics"."CD_GENDER" as "customer_demographics_gender",
    "customer_demographics_customer_demographics"."CD_MARITAL_STATUS" as "customer_demographics_marital_status"
FROM
    "memory"."customer" as "customer_customers"
    INNER JOIN "memory"."customer_address" as "customer_address_customer_address" on "customer_customers"."C_CURRENT_ADDR_SK" = "customer_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."customer_demographics" as "customer_demographics_customer_demographics" on "customer_customers"."C_CURRENT_CDEMO_SK" = "customer_demographics_customer_demographics"."CD_DEMO_SK"
WHERE
    "customer_customers"."C_CURRENT_CDEMO_SK" is not null and "customer_customers"."C_CUSTOMER_SK" in (select sparkling."store_buyers_store_cust_id" from sparkling where sparkling."store_buyers_store_cust_id" is not null) and ( "customer_customers"."C_CUSTOMER_SK" in (select scrawny."web_buyers_web_cust_id" from scrawny where scrawny."web_buyers_web_cust_id" is not null) or "customer_customers"."C_CUSTOMER_SK" in (select yummy."catalog_buyers_cat_cust_id" from yummy where yummy."catalog_buyers_cat_cust_id" is not null) )
),
friendly as (
SELECT
    "cheerful"."customer_address_state" as "customer_address_state",
    "cheerful"."customer_demographics_college_dependent_count" as "customer_demographics_college_dependent_count",
    "cheerful"."customer_demographics_dependent_count" as "customer_demographics_dependent_count",
    "cheerful"."customer_demographics_employed_dependent_count" as "customer_demographics_employed_dependent_count",
    "cheerful"."customer_demographics_gender" as "customer_demographics_gender",
    "cheerful"."customer_demographics_marital_status" as "customer_demographics_marital_status",
    "cheerful"."customer_id" as "customer_id"
FROM
    "cheerful"),
busy as (
SELECT
    "friendly"."customer_address_state" as "customer_address_state",
    "friendly"."customer_demographics_college_dependent_count" as "customer_demographics_college_dependent_count",
    "friendly"."customer_demographics_dependent_count" as "customer_demographics_dependent_count",
    "friendly"."customer_demographics_employed_dependent_count" as "customer_demographics_employed_dependent_count",
    "friendly"."customer_demographics_gender" as "customer_demographics_gender",
    "friendly"."customer_demographics_marital_status" as "customer_demographics_marital_status",
    avg("friendly"."customer_demographics_college_dependent_count") as "avg3",
    avg("friendly"."customer_demographics_dependent_count") as "avg1",
    avg("friendly"."customer_demographics_employed_dependent_count") as "avg2",
    max("friendly"."customer_demographics_college_dependent_count") as "max3",
    max("friendly"."customer_demographics_dependent_count") as "max1",
    max("friendly"."customer_demographics_employed_dependent_count") as "max2",
    min("friendly"."customer_demographics_college_dependent_count") as "min3",
    min("friendly"."customer_demographics_dependent_count") as "min1",
    min("friendly"."customer_demographics_employed_dependent_count") as "min2"
FROM
    "friendly"
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6),
kaput as (
SELECT
    "friendly"."customer_address_state" as "customer_address_state",
    "friendly"."customer_demographics_college_dependent_count" as "customer_demographics_college_dependent_count",
    "friendly"."customer_demographics_dependent_count" as "customer_demographics_dependent_count",
    "friendly"."customer_demographics_employed_dependent_count" as "customer_demographics_employed_dependent_count",
    "friendly"."customer_demographics_gender" as "customer_demographics_gender",
    "friendly"."customer_demographics_marital_status" as "customer_demographics_marital_status",
    count("friendly"."customer_id") as "cnt1",
    count("friendly"."customer_id") as "cnt2",
    count("friendly"."customer_id") as "cnt3"
FROM
    "friendly"
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6)
SELECT
    coalesce("busy"."customer_address_state","kaput"."customer_address_state") as "customer_address_state",
    coalesce("busy"."customer_demographics_gender","kaput"."customer_demographics_gender") as "customer_demographics_gender",
    coalesce("busy"."customer_demographics_marital_status","kaput"."customer_demographics_marital_status") as "customer_demographics_marital_status",
    coalesce("busy"."customer_demographics_dependent_count","kaput"."customer_demographics_dependent_count") as "customer_demographics_dependent_count",
    coalesce("kaput"."cnt1",0) as "cnt1",
    "busy"."min1" as "min1",
    "busy"."max1" as "max1",
    "busy"."avg1" as "avg1",
    coalesce("busy"."customer_demographics_employed_dependent_count","kaput"."customer_demographics_employed_dependent_count") as "customer_demographics_employed_dependent_count",
    coalesce("kaput"."cnt2",0) as "cnt2",
    "busy"."min2" as "min2",
    "busy"."max2" as "max2",
    "busy"."avg2" as "avg2",
    coalesce("busy"."customer_demographics_college_dependent_count","kaput"."customer_demographics_college_dependent_count") as "customer_demographics_college_dependent_count",
    coalesce("kaput"."cnt3",0) as "cnt3",
    "busy"."min3" as "min3",
    "busy"."max3" as "max3",
    "busy"."avg3" as "avg3"
FROM
    "busy"
    INNER JOIN "kaput" on "busy"."customer_address_state" is not distinct from "kaput"."customer_address_state" AND "busy"."customer_demographics_college_dependent_count" = "kaput"."customer_demographics_college_dependent_count" AND "busy"."customer_demographics_dependent_count" = "kaput"."customer_demographics_dependent_count" AND "busy"."customer_demographics_employed_dependent_count" = "kaput"."customer_demographics_employed_dependent_count" AND "busy"."customer_demographics_gender" = "kaput"."customer_demographics_gender" AND "busy"."customer_demographics_marital_status" = "kaput"."customer_demographics_marital_status"
ORDER BY 
    coalesce("busy"."customer_address_state","kaput"."customer_address_state") asc nulls first,
    coalesce("busy"."customer_demographics_gender","kaput"."customer_demographics_gender") asc nulls first,
    coalesce("busy"."customer_demographics_marital_status","kaput"."customer_demographics_marital_status") asc nulls first,
    coalesce("busy"."customer_demographics_dependent_count","kaput"."customer_demographics_dependent_count") asc nulls first,
    coalesce("busy"."customer_demographics_employed_dependent_count","kaput"."customer_demographics_employed_dependent_count") asc nulls first,
    coalesce("busy"."customer_demographics_college_dependent_count","kaput"."customer_demographics_college_dependent_count") asc nulls first
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
    "sales_date_date"."D_YEAR" = 2002 and "sales_date_date"."D_QOY" < 4 and  'CATALOG'  = 'CATALOG' and "sales_catalog_sales_unified"."CS_SHIP_CUSTOMER_SK" is not null

GROUP BY
    1),
abundant as (
SELECT
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "store_buyers_store_cust_id"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_date_date"."D_YEAR" = 2002 and "sales_date_date"."D_QOY" < 4 and  'STORE'  = 'STORE' and "sales_store_sales_unified"."SS_CUSTOMER_SK" is not null

GROUP BY
    1),
juicy as (
SELECT
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" as "web_buyers_web_cust_id"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_date_date"."D_YEAR" = 2002 and "sales_date_date"."D_QOY" < 4 and  'WEB'  = 'WEB' and "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" is not null

GROUP BY
    1),
abhorrent as (
SELECT
    "customer_address_customer_address"."CA_STATE" as "customer_address_state",
    "customer_customers"."C_CUSTOMER_SK" as "customer_id",
    "customer_demographics_customer_demographics"."CD_DEP_COLLEGE_COUNT" as "customer_demographics_college_dependent_count",
    "customer_demographics_customer_demographics"."CD_DEP_COUNT" as "customer_demographics_dependent_count",
    "customer_demographics_customer_demographics"."CD_DEP_EMPLOYED_COUNT" as "customer_demographics_employed_dependent_count",
    "customer_demographics_customer_demographics"."CD_GENDER" as "customer_demographics_gender",
    "customer_demographics_customer_demographics"."CD_MARITAL_STATUS" as "customer_demographics_marital_status"
FROM
    "memory"."customer" as "customer_customers"
    INNER JOIN "memory"."customer_address" as "customer_address_customer_address" on "customer_customers"."C_CURRENT_ADDR_SK" = "customer_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."customer_demographics" as "customer_demographics_customer_demographics" on "customer_customers"."C_CURRENT_CDEMO_SK" = "customer_demographics_customer_demographics"."CD_DEMO_SK"
WHERE
    "customer_customers"."C_CUSTOMER_SK" in (select abundant."store_buyers_store_cust_id" from abundant where abundant."store_buyers_store_cust_id" is not null) and ( "customer_customers"."C_CUSTOMER_SK" in (select juicy."web_buyers_web_cust_id" from juicy where juicy."web_buyers_web_cust_id" is not null) or "customer_customers"."C_CUSTOMER_SK" in (select cheerful."catalog_buyers_cat_cust_id" from cheerful where cheerful."catalog_buyers_cat_cust_id" is not null) ) and "customer_customers"."C_CURRENT_CDEMO_SK" is not null

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7),
young as (
SELECT
    "customer_address_customer_address"."CA_STATE" as "customer_address_state",
    "customer_demographics_customer_demographics"."CD_DEP_COLLEGE_COUNT" as "customer_demographics_college_dependent_count",
    "customer_demographics_customer_demographics"."CD_DEP_COUNT" as "customer_demographics_dependent_count",
    "customer_demographics_customer_demographics"."CD_DEP_EMPLOYED_COUNT" as "customer_demographics_employed_dependent_count",
    "customer_demographics_customer_demographics"."CD_GENDER" as "customer_demographics_gender",
    "customer_demographics_customer_demographics"."CD_MARITAL_STATUS" as "customer_demographics_marital_status"
FROM
    "memory"."customer" as "customer_customers"
    INNER JOIN "memory"."customer_address" as "customer_address_customer_address" on "customer_customers"."C_CURRENT_ADDR_SK" = "customer_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."customer_demographics" as "customer_demographics_customer_demographics" on "customer_customers"."C_CURRENT_CDEMO_SK" = "customer_demographics_customer_demographics"."CD_DEMO_SK"
WHERE
    "customer_customers"."C_CUSTOMER_SK" in (select abundant."store_buyers_store_cust_id" from abundant where abundant."store_buyers_store_cust_id" is not null) and ( "customer_customers"."C_CUSTOMER_SK" in (select juicy."web_buyers_web_cust_id" from juicy where juicy."web_buyers_web_cust_id" is not null) or "customer_customers"."C_CUSTOMER_SK" in (select cheerful."catalog_buyers_cat_cust_id" from cheerful where cheerful."catalog_buyers_cat_cust_id" is not null) ) and "customer_customers"."C_CURRENT_CDEMO_SK" is not null

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    "customer_customers"."C_CURRENT_CDEMO_SK"),
sweltering as (
SELECT
    "abhorrent"."customer_address_state" as "customer_address_state",
    "abhorrent"."customer_demographics_college_dependent_count" as "customer_demographics_college_dependent_count",
    "abhorrent"."customer_demographics_dependent_count" as "customer_demographics_dependent_count",
    "abhorrent"."customer_demographics_employed_dependent_count" as "customer_demographics_employed_dependent_count",
    "abhorrent"."customer_demographics_gender" as "customer_demographics_gender",
    "abhorrent"."customer_demographics_marital_status" as "customer_demographics_marital_status",
    count("abhorrent"."customer_id") as "cnt1",
    count("abhorrent"."customer_id") as "cnt2",
    count("abhorrent"."customer_id") as "cnt3"
FROM
    "abhorrent"
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6),
sparkling as (
SELECT
    "young"."customer_address_state" as "customer_address_state",
    "young"."customer_demographics_college_dependent_count" as "customer_demographics_college_dependent_count",
    "young"."customer_demographics_dependent_count" as "customer_demographics_dependent_count",
    "young"."customer_demographics_employed_dependent_count" as "customer_demographics_employed_dependent_count",
    "young"."customer_demographics_gender" as "customer_demographics_gender",
    "young"."customer_demographics_marital_status" as "customer_demographics_marital_status",
    avg("young"."customer_demographics_college_dependent_count") as "avg3",
    avg("young"."customer_demographics_dependent_count") as "avg1",
    avg("young"."customer_demographics_employed_dependent_count") as "avg2",
    max("young"."customer_demographics_college_dependent_count") as "max3",
    max("young"."customer_demographics_dependent_count") as "max1",
    max("young"."customer_demographics_employed_dependent_count") as "max2",
    min("young"."customer_demographics_college_dependent_count") as "min3",
    min("young"."customer_demographics_dependent_count") as "min1",
    min("young"."customer_demographics_employed_dependent_count") as "min2"
FROM
    "young"
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6)
SELECT
    coalesce("sparkling"."customer_address_state","sweltering"."customer_address_state") as "customer_address_state",
    coalesce("sparkling"."customer_demographics_gender","sweltering"."customer_demographics_gender") as "customer_demographics_gender",
    coalesce("sparkling"."customer_demographics_marital_status","sweltering"."customer_demographics_marital_status") as "customer_demographics_marital_status",
    coalesce("sparkling"."customer_demographics_dependent_count","sweltering"."customer_demographics_dependent_count") as "customer_demographics_dependent_count",
    "sweltering"."cnt1" as "cnt1",
    "sparkling"."min1" as "min1",
    "sparkling"."max1" as "max1",
    "sparkling"."avg1" as "avg1",
    coalesce("sparkling"."customer_demographics_employed_dependent_count","sweltering"."customer_demographics_employed_dependent_count") as "customer_demographics_employed_dependent_count",
    "sweltering"."cnt2" as "cnt2",
    "sparkling"."min2" as "min2",
    "sparkling"."max2" as "max2",
    "sparkling"."avg2" as "avg2",
    coalesce("sparkling"."customer_demographics_college_dependent_count","sweltering"."customer_demographics_college_dependent_count") as "customer_demographics_college_dependent_count",
    "sweltering"."cnt3" as "cnt3",
    "sparkling"."min3" as "min3",
    "sparkling"."max3" as "max3",
    "sparkling"."avg3" as "avg3"
FROM
    "sweltering"
    INNER JOIN "sparkling" on "sweltering"."customer_address_state" is not distinct from "sparkling"."customer_address_state" AND "sweltering"."customer_demographics_college_dependent_count" is not distinct from "sparkling"."customer_demographics_college_dependent_count" AND "sweltering"."customer_demographics_dependent_count" is not distinct from "sparkling"."customer_demographics_dependent_count" AND "sweltering"."customer_demographics_employed_dependent_count" is not distinct from "sparkling"."customer_demographics_employed_dependent_count" AND "sweltering"."customer_demographics_gender" is not distinct from "sparkling"."customer_demographics_gender" AND "sweltering"."customer_demographics_marital_status" is not distinct from "sparkling"."customer_demographics_marital_status"
ORDER BY 
    coalesce("sparkling"."customer_address_state","sweltering"."customer_address_state") asc nulls first,
    coalesce("sparkling"."customer_demographics_gender","sweltering"."customer_demographics_gender") asc nulls first,
    coalesce("sparkling"."customer_demographics_marital_status","sweltering"."customer_demographics_marital_status") asc nulls first,
    coalesce("sparkling"."customer_demographics_dependent_count","sweltering"."customer_demographics_dependent_count") asc nulls first,
    coalesce("sparkling"."customer_demographics_employed_dependent_count","sweltering"."customer_demographics_employed_dependent_count") asc nulls first,
    coalesce("sparkling"."customer_demographics_college_dependent_count","sweltering"."customer_demographics_college_dependent_count") asc nulls first
LIMIT (100)
```
