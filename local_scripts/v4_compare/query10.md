# Query 10

**Status:** `mismatch`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (94215 rows) |
| reference execution | OK (6 rows) |
| results identical | NO |

## Result comparison

v4 rows: 94215 (94215 distinct)
ref rows: 6 (6 distinct)
only in v4 (showing up to 5 of 94215):
  1x  (10, 10, 10, 10, 10, 10, 6, 'Good', 0, '2 yr Degree', 2, 'F', 'D', 500)
  1x  (4, 4, 4, 4, 4, 4, 0, 'Good', 0, '2 yr Degree', 3, 'F', 'D', 500)
  1x  (2, 2, 2, 2, 2, 2, 2, 'Good', 0, '2 yr Degree', 3, 'F', 'D', 500)
  1x  (3, 3, 3, 3, 3, 3, 2, 'Good', 0, '2 yr Degree', 5, 'F', 'D', 500)
  1x  (8, 8, 8, 8, 8, 8, 4, 'Good', 1, '2 yr Degree', 0, 'F', 'D', 500)
only in ref (showing up to 5 of 6):
  1x  (1, 1, 1, 1, 1, 1, 5, 'High Risk', 2, 'Advanced Degree', 4, 'F', 'D', 3000)
  1x  (1, 1, 1, 1, 1, 1, 4, 'Good', 6, 'Unknown', 5, 'F', 'D', 1500)
  1x  (1, 1, 1, 1, 1, 1, 5, 'Good', 4, '2 yr Degree', 0, 'F', 'W', 8500)
  1x  (1, 1, 1, 1, 1, 1, 1, 'Low Risk', 3, 'College', 0, 'M', 'D', 8500)
  1x  (1, 1, 1, 1, 1, 1, 1, 'Unknown', 2, 'Primary', 1, 'M', 'D', 7000)

## SQL size

| Source | Chars | Lines |
| --- | --- | --- |
| v4 | 8031 | 176 |
| reference | 4839 | 84 |
| v4 / ref | 1.66x | 2.10x |

## Preql

```
import customer as customer;
import store_sales as store_sales;
import web_sales as web_sales;
import catalog_sales as catalog_sales;

#Count the customers with the same gender, marital status, education status, purchase estimate, credit rating,
#dependent count, employed dependent count and college dependent count who live in certain counties and who
# have purchased from both stores and another sales channel during a three month time period of a given year.
merge catalog_sales.customer.id into ~customer.id;
merge web_sales.customer.id into ~customer.id;
merge store_sales.customer.id into ~customer.id;

auto relevant_customers <- customer.id
    ? store_sales.date.year = 2002
and store_sales.date.month_of_year in (1, 2, 3, 4)
and customer.address.county in ('Rush County', 'Toole County', 'Jefferson County', 'Dona Ana County', 'La Porte County')
and (
    (web_sales.date.year = 2002 and web_sales.date.month_of_year in (1, 2, 3, 4))
    or (catalog_sales.date.year = 2002 and catalog_sales.date.month_of_year in (1, 2, 3, 4))
);

where
    customer.id in relevant_customers and customer.demographics.gender is not null
select
    customer.demographics.gender,
    customer.demographics.marital_status,
    customer.demographics.education_status,
    count(customer.id) as cnt1,
    customer.demographics.purchase_estimate,
    count(customer.id) as cnt2,
    customer.demographics.credit_rating,
    count(customer.id) as cnt3,
    customer.demographics.dependent_count,
    count(customer.id) as cnt4,
    customer.demographics.employed_dependent_count,
    count(customer.id) as cnt5,
    customer.demographics.college_dependent_count,
    count(customer.id) as cnt6,
order by
    customer.demographics.gender asc,
    customer.demographics.marital_status asc,
    customer.demographics.education_status asc,
    customer.demographics.purchase_estimate asc,
    customer.demographics.credit_rating asc,
    customer.demographics.dependent_count asc,
    customer.demographics.employed_dependent_count asc,
    customer.demographics.college_dependent_count asc
;
```

## v4 generated SQL

```sql
WITH 
juicy as (
SELECT
    "web_sales_web_sales"."WS_BILL_CUSTOMER_SK" as "customer_id",
    "web_sales_web_sales"."WS_SOLD_DATE_SK" as "web_sales_date_id"
FROM
    "memory"."web_sales" as "web_sales_web_sales"),
vacuous as (
SELECT
    "juicy"."customer_id" as "customer_id",
    "juicy"."web_sales_date_id" as "web_sales_date_id"
FROM
    "juicy"
GROUP BY
    1,
    2),
yummy as (
SELECT
    "web_sales_date_date"."D_DATE_SK" as "web_sales_date_id",
    "web_sales_date_date"."D_MOY" as "web_sales_date_month_of_year",
    "web_sales_date_date"."D_YEAR" as "web_sales_date_year"
FROM
    "memory"."date_dim" as "web_sales_date_date"),
abundant as (
SELECT
    "store_sales_store_sales"."SS_CUSTOMER_SK" as "customer_id",
    "store_sales_store_sales"."SS_SOLD_DATE_SK" as "store_sales_date_id"
FROM
    "memory"."store_sales" as "store_sales_store_sales"),
uneven as (
SELECT
    "abundant"."customer_id" as "customer_id",
    "abundant"."store_sales_date_id" as "store_sales_date_id"
FROM
    "abundant"
GROUP BY
    1,
    2),
questionable as (
SELECT
    "store_sales_date_date"."D_DATE_SK" as "store_sales_date_id",
    "store_sales_date_date"."D_MOY" as "store_sales_date_month_of_year",
    "store_sales_date_date"."D_YEAR" as "store_sales_date_year"
FROM
    "memory"."date_dim" as "store_sales_date_date"),
cooperative as (
SELECT
    "customer_demographics_customer_demographics"."CD_CREDIT_RATING" as "customer_demographics_credit_rating",
    "customer_demographics_customer_demographics"."CD_DEMO_SK" as "customer_demographics_id",
    "customer_demographics_customer_demographics"."CD_DEP_COLLEGE_COUNT" as "customer_demographics_college_dependent_count",
    "customer_demographics_customer_demographics"."CD_DEP_COUNT" as "customer_demographics_dependent_count",
    "customer_demographics_customer_demographics"."CD_DEP_EMPLOYED_COUNT" as "customer_demographics_employed_dependent_count",
    "customer_demographics_customer_demographics"."CD_EDUCATION_STATUS" as "customer_demographics_education_status",
    "customer_demographics_customer_demographics"."CD_GENDER" as "customer_demographics_gender",
    "customer_demographics_customer_demographics"."CD_MARITAL_STATUS" as "customer_demographics_marital_status",
    "customer_demographics_customer_demographics"."CD_PURCHASE_ESTIMATE" as "customer_demographics_purchase_estimate"
FROM
    "memory"."customer_demographics" as "customer_demographics_customer_demographics"),
thoughtful as (
SELECT
    "customer_customers"."C_CURRENT_ADDR_SK" as "customer_address_id",
    "customer_customers"."C_CURRENT_CDEMO_SK" as "customer_demographics_id",
    "customer_customers"."C_CUSTOMER_SK" as "customer_id"
FROM
    "memory"."customer" as "customer_customers"),
cheerful as (
SELECT
    "customer_address_customer_address"."CA_ADDRESS_SK" as "customer_address_id",
    "customer_address_customer_address"."CA_COUNTY" as "customer_address_county"
FROM
    "memory"."customer_address" as "customer_address_customer_address"),
wakeful as (
SELECT
    "catalog_sales_date_date"."D_DATE_SK" as "catalog_sales_date_id",
    "catalog_sales_date_date"."D_MOY" as "catalog_sales_date_month_of_year",
    "catalog_sales_date_date"."D_YEAR" as "catalog_sales_date_year"
FROM
    "memory"."date_dim" as "catalog_sales_date_date"),
quizzical as (
SELECT
    "catalog_sales_catalog_sales"."CS_SHIP_CUSTOMER_SK" as "customer_id",
    "catalog_sales_catalog_sales"."CS_SOLD_DATE_SK" as "catalog_sales_date_id"
FROM
    "memory"."catalog_sales" as "catalog_sales_catalog_sales"),
highfalutin as (
SELECT
    "quizzical"."catalog_sales_date_id" as "catalog_sales_date_id",
    "quizzical"."customer_id" as "customer_id"
FROM
    "quizzical"
GROUP BY
    1,
    2),
concerned as (
SELECT
    "cheerful"."customer_address_county" as "customer_address_county",
    "cooperative"."customer_demographics_college_dependent_count" as "customer_demographics_college_dependent_count",
    "cooperative"."customer_demographics_credit_rating" as "customer_demographics_credit_rating",
    "cooperative"."customer_demographics_dependent_count" as "customer_demographics_dependent_count",
    "cooperative"."customer_demographics_education_status" as "customer_demographics_education_status",
    "cooperative"."customer_demographics_employed_dependent_count" as "customer_demographics_employed_dependent_count",
    "cooperative"."customer_demographics_gender" as "customer_demographics_gender",
    "cooperative"."customer_demographics_marital_status" as "customer_demographics_marital_status",
    "cooperative"."customer_demographics_purchase_estimate" as "customer_demographics_purchase_estimate",
    "questionable"."store_sales_date_month_of_year" as "store_sales_date_month_of_year",
    "questionable"."store_sales_date_year" as "store_sales_date_year",
    "wakeful"."catalog_sales_date_month_of_year" as "catalog_sales_date_month_of_year",
    "wakeful"."catalog_sales_date_year" as "catalog_sales_date_year",
    "yummy"."web_sales_date_month_of_year" as "web_sales_date_month_of_year",
    "yummy"."web_sales_date_year" as "web_sales_date_year",
    coalesce("highfalutin"."customer_id","thoughtful"."customer_id","uneven"."customer_id","vacuous"."customer_id") as "customer_id"
FROM
    "vacuous"
    LEFT OUTER JOIN "yummy" on "vacuous"."web_sales_date_id" = "yummy"."web_sales_date_id"
    FULL JOIN "highfalutin" on "vacuous"."customer_id" is not distinct from "highfalutin"."customer_id"
    FULL JOIN "uneven" on coalesce("highfalutin"."customer_id", "vacuous"."customer_id") = "uneven"."customer_id"
    FULL JOIN "thoughtful" on coalesce("highfalutin"."customer_id", "uneven"."customer_id", "vacuous"."customer_id") = "thoughtful"."customer_id"
    LEFT OUTER JOIN "cheerful" on "thoughtful"."customer_address_id" = "cheerful"."customer_address_id"
    FULL JOIN "questionable" on "uneven"."store_sales_date_id" = "questionable"."store_sales_date_id"
    FULL JOIN "wakeful" on "highfalutin"."catalog_sales_date_id" = "wakeful"."catalog_sales_date_id"
    LEFT OUTER JOIN "cooperative" on "thoughtful"."customer_demographics_id" = "cooperative"."customer_demographics_id"
WHERE
    "cooperative"."customer_demographics_gender" is not null

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14,
    15,
    16)
SELECT
    count("concerned"."customer_id") as "cnt1",
    count("concerned"."customer_id") as "cnt2",
    count("concerned"."customer_id") as "cnt3",
    count("concerned"."customer_id") as "cnt4",
    count("concerned"."customer_id") as "cnt5",
    count("concerned"."customer_id") as "cnt6",
    "concerned"."customer_demographics_college_dependent_count" as "customer_demographics_college_dependent_count",
    "concerned"."customer_demographics_credit_rating" as "customer_demographics_credit_rating",
    "concerned"."customer_demographics_dependent_count" as "customer_demographics_dependent_count",
    "concerned"."customer_demographics_employed_dependent_count" as "customer_demographics_employed_dependent_count",
    "concerned"."customer_demographics_purchase_estimate" as "customer_demographics_purchase_estimate",
    "concerned"."customer_demographics_gender" as "customer_demographics_gender",
    "concerned"."customer_demographics_marital_status" as "customer_demographics_marital_status",
    "concerned"."customer_demographics_education_status" as "customer_demographics_education_status"
FROM
    "concerned"
GROUP BY
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14
ORDER BY 
    "concerned"."customer_demographics_gender" asc,
    "concerned"."customer_demographics_marital_status" asc,
    "concerned"."customer_demographics_education_status" asc,
    "concerned"."customer_demographics_purchase_estimate" asc,
    "concerned"."customer_demographics_credit_rating" asc,
    "concerned"."customer_demographics_dependent_count" asc,
    "concerned"."customer_demographics_employed_dependent_count" asc,
    "concerned"."customer_demographics_college_dependent_count" asc
```

## Reference SQL (zquery log)

```sql
WITH 
quizzical as (
SELECT
    "catalog_sales_catalog_sales"."CS_SHIP_CUSTOMER_SK" as "customer_id",
    "catalog_sales_catalog_sales"."CS_SOLD_DATE_SK" as "catalog_sales_date_id"
FROM
    "memory"."catalog_sales" as "catalog_sales_catalog_sales"
GROUP BY
    1,
    2),
questionable as (
SELECT
    "store_sales_store_sales"."SS_CUSTOMER_SK" as "customer_id",
    "store_sales_store_sales"."SS_SOLD_DATE_SK" as "store_sales_date_id"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
GROUP BY
    1,
    2),
yummy as (
SELECT
    "web_sales_web_sales"."WS_BILL_CUSTOMER_SK" as "customer_id",
    "web_sales_web_sales"."WS_SOLD_DATE_SK" as "web_sales_date_id"
FROM
    "memory"."web_sales" as "web_sales_web_sales"
GROUP BY
    1,
    2),
vacuous as (
SELECT
    coalesce("customer_customers"."C_CUSTOMER_SK","questionable"."customer_id","quizzical"."customer_id","yummy"."customer_id") as "relevant_customers"
FROM
    "yummy"
    LEFT OUTER JOIN "memory"."date_dim" as "web_sales_date_date" on "yummy"."web_sales_date_id" = "web_sales_date_date"."D_DATE_SK"
    FULL JOIN "quizzical" on "yummy"."customer_id" is not distinct from "quizzical"."customer_id"
    RIGHT OUTER JOIN "questionable" on coalesce("quizzical"."customer_id", "yummy"."customer_id") = "questionable"."customer_id"
    INNER JOIN "memory"."customer" as "customer_customers" on coalesce("quizzical"."customer_id", "questionable"."customer_id", "yummy"."customer_id") = "customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."customer_address" as "customer_address_customer_address" on "customer_customers"."C_CURRENT_ADDR_SK" = "customer_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."date_dim" as "store_sales_date_date" on "questionable"."store_sales_date_id" = "store_sales_date_date"."D_DATE_SK"
    LEFT OUTER JOIN "memory"."date_dim" as "catalog_sales_date_date" on "quizzical"."catalog_sales_date_id" = "catalog_sales_date_date"."D_DATE_SK"
WHERE
    "store_sales_date_date"."D_YEAR" = 2002 and "store_sales_date_date"."D_MOY" in (1,2,3,4) and "customer_address_customer_address"."CA_COUNTY" in ('Rush County','Toole County','Jefferson County','Dona Ana County','La Porte County') and ( ( "web_sales_date_date"."D_YEAR" = 2002 and "web_sales_date_date"."D_MOY" in (1,2,3,4) ) or ( "catalog_sales_date_date"."D_YEAR" = 2002 and "catalog_sales_date_date"."D_MOY" in (1,2,3,4) ) )

GROUP BY
    1)
SELECT
    "customer_demographics_customer_demographics"."CD_GENDER" as "customer_demographics_gender",
    "customer_demographics_customer_demographics"."CD_MARITAL_STATUS" as "customer_demographics_marital_status",
    "customer_demographics_customer_demographics"."CD_EDUCATION_STATUS" as "customer_demographics_education_status",
    count("customer_customers"."C_CUSTOMER_SK") as "cnt1",
    "customer_demographics_customer_demographics"."CD_PURCHASE_ESTIMATE" as "customer_demographics_purchase_estimate",
    count("customer_customers"."C_CUSTOMER_SK") as "cnt2",
    "customer_demographics_customer_demographics"."CD_CREDIT_RATING" as "customer_demographics_credit_rating",
    count("customer_customers"."C_CUSTOMER_SK") as "cnt3",
    "customer_demographics_customer_demographics"."CD_DEP_COUNT" as "customer_demographics_dependent_count",
    count("customer_customers"."C_CUSTOMER_SK") as "cnt4",
    "customer_demographics_customer_demographics"."CD_DEP_EMPLOYED_COUNT" as "customer_demographics_employed_dependent_count",
    count("customer_customers"."C_CUSTOMER_SK") as "cnt5",
    "customer_demographics_customer_demographics"."CD_DEP_COLLEGE_COUNT" as "customer_demographics_college_dependent_count",
    count("customer_customers"."C_CUSTOMER_SK") as "cnt6"
FROM
    "memory"."customer" as "customer_customers"
    INNER JOIN "memory"."customer_demographics" as "customer_demographics_customer_demographics" on "customer_customers"."C_CURRENT_CDEMO_SK" = "customer_demographics_customer_demographics"."CD_DEMO_SK"
WHERE
    "customer_customers"."C_CUSTOMER_SK" in (select vacuous."relevant_customers" from vacuous where vacuous."relevant_customers" is not null) and "customer_demographics_customer_demographics"."CD_GENDER" is not null

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
    "customer_demographics_customer_demographics"."CD_GENDER" asc,
    "customer_demographics_customer_demographics"."CD_MARITAL_STATUS" asc,
    "customer_demographics_customer_demographics"."CD_EDUCATION_STATUS" asc,
    "customer_demographics_customer_demographics"."CD_PURCHASE_ESTIMATE" asc,
    "customer_demographics_customer_demographics"."CD_CREDIT_RATING" asc,
    "customer_demographics_customer_demographics"."CD_DEP_COUNT" asc,
    "customer_demographics_customer_demographics"."CD_DEP_EMPLOYED_COUNT" asc,
    "customer_demographics_customer_demographics"."CD_DEP_COLLEGE_COUNT" asc
```
