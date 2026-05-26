# Query 69

**Status:** `exec_fail`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | FAILED |
| reference execution | OK (100 rows) |

## Result comparison

_at least one side did not produce rows._

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 1236 | 23 | — |
| reference | 4137 | 80 | 91.56 ms |
| v4 / ref | 0.30x | 0.29x | — |

## Preql

```
import customer as customer;
import unified_sales as sales;

# TPC-DS q69 uses ss_customer_sk for the store EXISTS branch,
# ws_bill_customer_sk for web, and cs_ship_customer_sk for catalog, so:
#   - store/web filters use sales.customer.id (= SS_CUSTOMER_SK / WS_BILL_CUSTOMER_SK)
#   - catalog filter uses sales.ship_customer.id (= CS_SHIP_CUSTOMER_SK)
rowset store_buyers <- where
    sales.sales_channel = 'STORE'
    and sales.date.year = 2001
    and sales.date.month_of_year between 4 and 6
    and sales.customer.id is not null
select
    sales.customer.id as store_cust_id,
;

rowset web_buyers <- where
    sales.sales_channel = 'WEB'
    and sales.date.year = 2001
    and sales.date.month_of_year between 4 and 6
    and sales.customer.id is not null
select
    sales.customer.id as web_cust_id,
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
SELECT
    count(INVALID_REFERENCE_BUG_<Missing source reference to customer.id>) as "cnt1",
    count(INVALID_REFERENCE_BUG_<Missing source reference to customer.id>) as "cnt2",
    count(INVALID_REFERENCE_BUG_<Missing source reference to customer.id>) as "cnt3",
    INVALID_REFERENCE_BUG_<Missing source reference to customer.demographics.purchase_estimate> as "customer_demographics_purchase_estimate",
    INVALID_REFERENCE_BUG_<Missing source reference to customer.demographics.education_status> as "customer_demographics_education_status",
    INVALID_REFERENCE_BUG_<Missing source reference to customer.demographics.marital_status> as "customer_demographics_marital_status",
    INVALID_REFERENCE_BUG_<Missing source reference to customer.demographics.gender> as "customer_demographics_gender",
    INVALID_REFERENCE_BUG_<Missing source reference to customer.demographics.credit_rating> as "customer_demographics_credit_rating"

GROUP BY
    4,
    5,
    6,
    7,
    8
ORDER BY 
    "customer_demographics_gender" asc,
    "customer_demographics_marital_status" asc,
    "customer_demographics_education_status" asc,
    "customer_demographics_purchase_estimate" asc,
    "customer_demographics_credit_rating" asc
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
    "memory"."customer_address" as "customer_address_customer_address"
    INNER JOIN "memory"."customer" as "customer_customers" on "customer_address_customer_address"."CA_ADDRESS_SK" = "customer_customers"."C_CURRENT_ADDR_SK"
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

## v4 execution error

```
Traceback (most recent call last):
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 179, in run_one
    result.v4_exec_seconds, result.v4_rows = _time(
                                             ~~~~~^
        lambda: execute(con, v4_sql)
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    )
    ^
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 45, in _time
    value = fn()
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 180, in <lambda>
    lambda: execute(con, v4_sql)
            ~~~~~~~^^^^^^^^^^^^^
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 120, in execute
    cursor = con.execute(sql)
_duckdb.ParserException: Parser Error: syntax error at or near "source"

LINE 2:     count(INVALID_REFERENCE_BUG_<Missing source reference to customer.id>) as "cnt1",
                                                 ^
```
