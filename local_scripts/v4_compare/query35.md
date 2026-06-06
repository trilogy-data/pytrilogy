# Query 35

**Status:** `mismatch`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (0 rows) |
| reference execution | OK (12 rows) |
| results identical | NO |

## Result comparison

v4 rows: 0 (0 distinct)
ref rows: 12 (12 distinct)
only in ref (showing up to 5 of 12):
  1x  (2.0, 0.0, 0.0, 1, 1, 1, 'GA', 0, 2, 0, 'F', 'M', 2, 0, 0, 2, 0, 0)
  1x  (0.0, 0.0, 0.0, 1, 1, 1, 'GA', 0, 0, 0, 'F', 'U', 0, 0, 0, 0, 0, 0)
  1x  (0.0, 0.0, 0.0, 1, 1, 1, 'GA', 0, 0, 0, 'M', 'D', 0, 0, 0, 0, 0, 0)
  1x  (3.0, 0.0, 0.0, 1, 1, 1, 'IN', 0, 3, 0, 'F', 'D', 3, 0, 0, 3, 0, 0)
  1x  (2.0, 0.0, 0.0, 1, 1, 1, 'IN', 0, 2, 0, 'F', 'S', 2, 0, 0, 2, 0, 0)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 10071 | 197 | 6.96 ms |
| reference | 10103 | 157 | 13.07 ms |
| v4 / ref | 1.00x | 1.25x | 0.53x |

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
abundant as (
SELECT
    "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "sales_date_id",
     'CATALOG'  as "sales_sales_channel",
    "sales_catalog_sales_unified"."CS_SHIP_CUSTOMER_SK" as "sales_ship_customer_id",
    "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" as "sales_billing_customer_id"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_date_date"."D_YEAR" = 2002 and "sales_date_date"."D_QOY" < 4
),
sweltering as (
SELECT
    "abundant"."sales_ship_customer_id" as "sales_ship_customer_id"
FROM
    "abundant"
WHERE
    "abundant"."sales_ship_customer_id" is not null

GROUP BY
    1,
    "abundant"."sales_date_id",
    "abundant"."sales_sales_channel"),
uneven as (
SELECT
    "abundant"."sales_billing_customer_id" as "sales_billing_customer_id",
    "abundant"."sales_sales_channel" as "sales_sales_channel"
FROM
    "abundant"
WHERE
    "abundant"."sales_billing_customer_id" is not null

GROUP BY
    1,
    2,
    "abundant"."sales_date_id"),
late as (
SELECT
    "sweltering"."sales_ship_customer_id" as "_catalog_buyers_cat_cust_id"
FROM
    "sweltering"),
young as (
SELECT
    "uneven"."sales_billing_customer_id" as "_store_buyers_store_cust_id"
FROM
    "uneven"
WHERE
    "uneven"."sales_sales_channel" = 'STORE'
),
juicy as (
SELECT
    "uneven"."sales_billing_customer_id" as "_web_buyers_web_cust_id"
FROM
    "uneven"
WHERE
    "uneven"."sales_sales_channel" = 'WEB'
),
scrawny as (
SELECT
    "late"."_catalog_buyers_cat_cust_id" as "catalog_buyers_cat_cust_id"
FROM
    "late"),
abhorrent as (
SELECT
    "young"."_store_buyers_store_cust_id" as "store_buyers_store_cust_id"
FROM
    "young"),
concerned as (
SELECT
    "juicy"."_web_buyers_web_cust_id" as "web_buyers_web_cust_id"
FROM
    "juicy"),
cheerful as (
SELECT
    "customer_address_customer_address"."CA_STATE" as "customer_address_state",
    "customer_customers"."C_CURRENT_CDEMO_SK" as "customer_demographics_id",
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
    "customer_customers"."C_CURRENT_CDEMO_SK" is not null and "customer_customers"."C_CUSTOMER_SK" in (select abhorrent."store_buyers_store_cust_id" from abhorrent where abhorrent."store_buyers_store_cust_id" is not null) and ( "customer_customers"."C_CUSTOMER_SK" in (select concerned."web_buyers_web_cust_id" from concerned where concerned."web_buyers_web_cust_id" is not null) or "customer_customers"."C_CUSTOMER_SK" in (select scrawny."catalog_buyers_cat_cust_id" from scrawny where scrawny."catalog_buyers_cat_cust_id" is not null) )
),
friendly as (
SELECT
    "cheerful"."customer_address_state" as "customer_address_state",
    "cheerful"."customer_demographics_college_dependent_count" as "customer_demographics_college_dependent_count",
    "cheerful"."customer_demographics_dependent_count" as "customer_demographics_dependent_count",
    "cheerful"."customer_demographics_employed_dependent_count" as "customer_demographics_employed_dependent_count",
    "cheerful"."customer_demographics_gender" as "customer_demographics_gender",
    "cheerful"."customer_demographics_id" as "customer_demographics_id",
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
    "friendly"."customer_demographics_marital_status" as "customer_demographics_marital_status"
FROM
    "friendly"
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    "friendly"."customer_demographics_id"),
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
    6),
charming as (
SELECT
    "busy"."customer_address_state" as "customer_address_state",
    "busy"."customer_demographics_college_dependent_count" as "customer_demographics_college_dependent_count",
    "busy"."customer_demographics_dependent_count" as "customer_demographics_dependent_count",
    "busy"."customer_demographics_employed_dependent_count" as "customer_demographics_employed_dependent_count",
    "busy"."customer_demographics_gender" as "customer_demographics_gender",
    "busy"."customer_demographics_marital_status" as "customer_demographics_marital_status",
    avg("busy"."customer_demographics_college_dependent_count") as "avg3",
    avg("busy"."customer_demographics_dependent_count") as "avg1",
    avg("busy"."customer_demographics_employed_dependent_count") as "avg2",
    max("busy"."customer_demographics_college_dependent_count") as "max3",
    max("busy"."customer_demographics_dependent_count") as "max1",
    max("busy"."customer_demographics_employed_dependent_count") as "max2",
    min("busy"."customer_demographics_college_dependent_count") as "min3",
    min("busy"."customer_demographics_dependent_count") as "min1",
    min("busy"."customer_demographics_employed_dependent_count") as "min2"
FROM
    "busy"
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6)
SELECT
    coalesce("charming"."customer_address_state","kaput"."customer_address_state") as "customer_address_state",
    coalesce("charming"."customer_demographics_gender","kaput"."customer_demographics_gender") as "customer_demographics_gender",
    coalesce("charming"."customer_demographics_marital_status","kaput"."customer_demographics_marital_status") as "customer_demographics_marital_status",
    coalesce("charming"."customer_demographics_dependent_count","kaput"."customer_demographics_dependent_count") as "customer_demographics_dependent_count",
    coalesce("kaput"."cnt1",0) as "cnt1",
    "charming"."min1" as "min1",
    "charming"."max1" as "max1",
    "charming"."avg1" as "avg1",
    coalesce("charming"."customer_demographics_employed_dependent_count","kaput"."customer_demographics_employed_dependent_count") as "customer_demographics_employed_dependent_count",
    coalesce("kaput"."cnt2",0) as "cnt2",
    "charming"."min2" as "min2",
    "charming"."max2" as "max2",
    "charming"."avg2" as "avg2",
    coalesce("charming"."customer_demographics_college_dependent_count","kaput"."customer_demographics_college_dependent_count") as "customer_demographics_college_dependent_count",
    coalesce("kaput"."cnt3",0) as "cnt3",
    "charming"."min3" as "min3",
    "charming"."max3" as "max3",
    "charming"."avg3" as "avg3"
FROM
    "charming"
    INNER JOIN "kaput" on "charming"."customer_address_state" is not distinct from "kaput"."customer_address_state" AND "charming"."customer_demographics_college_dependent_count" = "kaput"."customer_demographics_college_dependent_count" AND "charming"."customer_demographics_dependent_count" = "kaput"."customer_demographics_dependent_count" AND "charming"."customer_demographics_employed_dependent_count" = "kaput"."customer_demographics_employed_dependent_count" AND "charming"."customer_demographics_gender" = "kaput"."customer_demographics_gender" AND "charming"."customer_demographics_marital_status" = "kaput"."customer_demographics_marital_status"
ORDER BY 
    coalesce("charming"."customer_address_state","kaput"."customer_address_state") asc nulls first,
    coalesce("charming"."customer_demographics_gender","kaput"."customer_demographics_gender") asc nulls first,
    coalesce("charming"."customer_demographics_marital_status","kaput"."customer_demographics_marital_status") asc nulls first,
    coalesce("charming"."customer_demographics_dependent_count","kaput"."customer_demographics_dependent_count") asc nulls first,
    coalesce("charming"."customer_demographics_employed_dependent_count","kaput"."customer_demographics_employed_dependent_count") asc nulls first,
    coalesce("charming"."customer_demographics_college_dependent_count","kaput"."customer_demographics_college_dependent_count") asc nulls first
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
