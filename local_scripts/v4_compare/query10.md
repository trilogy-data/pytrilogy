# Query 10

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (6 rows) |
| reference execution | OK (6 rows) |
| results identical | YES |

## Result comparison

v4 rows: 6 (6 distinct)
ref rows: 6 (6 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 7473 | 137 | 117.67 ms |
| reference | 4875 | 84 | 73.60 ms |
| v4 / ref | 1.53x | 1.63x | 1.60x |

## Preql

```
import customer as customer;
import physical_sales as physical_sales;
import web_sales as web_sales;
import catalog_sales as catalog_sales;

#Count the customers with the same gender, marital status, education status, purchase estimate, credit rating,
#dependent count, employed dependent count and college dependent count who live in certain counties and who
# have purchased from both stores and another sales channel during a three month time period of a given year.
merge catalog_sales.ship_customer.id into ~customer.id;
merge web_sales.billing_customer.id into ~customer.id;
merge physical_sales.customer.id into ~customer.id;

auto relevant_customers <- customer.id
    ? physical_sales.date.year = 2002
and physical_sales.date.month_of_year in (1, 2, 3, 4)
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
    "physical_sales_store_sales"."SS_CUSTOMER_SK" as "customer_id",
    "physical_sales_store_sales"."SS_SOLD_DATE_SK" as "physical_sales_date_id"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
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
    "catalog_sales_date_date"."D_MOY" as "catalog_sales_date_month_of_year",
    "catalog_sales_date_date"."D_YEAR" as "catalog_sales_date_year",
    "customer_address_customer_address"."CA_COUNTY" as "customer_address_county",
    "physical_sales_date_date"."D_MOY" as "physical_sales_date_month_of_year",
    "physical_sales_date_date"."D_YEAR" as "physical_sales_date_year",
    "web_sales_date_date"."D_MOY" as "web_sales_date_month_of_year",
    "web_sales_date_date"."D_YEAR" as "web_sales_date_year",
    coalesce("customer_customers"."C_CUSTOMER_SK","questionable"."customer_id","quizzical"."customer_id","yummy"."customer_id") as "customer_id"
FROM
    "yummy"
    LEFT OUTER JOIN "memory"."date_dim" as "web_sales_date_date" on "yummy"."web_sales_date_id" = "web_sales_date_date"."D_DATE_SK"
    FULL JOIN "questionable" on "yummy"."customer_id" is not distinct from "questionable"."customer_id"
    FULL JOIN "quizzical" on coalesce("yummy"."customer_id", "questionable"."customer_id") = "quizzical"."customer_id"
    FULL JOIN "memory"."customer" as "customer_customers" on coalesce("yummy"."customer_id", "questionable"."customer_id", "quizzical"."customer_id") = "customer_customers"."C_CUSTOMER_SK"
    FULL JOIN "memory"."customer_address" as "customer_address_customer_address" on "customer_customers"."C_CURRENT_ADDR_SK" = "customer_address_customer_address"."CA_ADDRESS_SK"
    FULL JOIN "memory"."date_dim" as "catalog_sales_date_date" on "quizzical"."catalog_sales_date_id" = "catalog_sales_date_date"."D_DATE_SK"
    FULL JOIN "memory"."date_dim" as "physical_sales_date_date" on "questionable"."physical_sales_date_id" = "physical_sales_date_date"."D_DATE_SK"),
concerned as (
SELECT
    CASE WHEN "vacuous"."physical_sales_date_year" = 2002 and "vacuous"."physical_sales_date_month_of_year" in (1,2,3,4) and "vacuous"."customer_address_county" in ('Rush County','Toole County','Jefferson County','Dona Ana County','La Porte County') and ( ( "vacuous"."web_sales_date_year" = 2002 and "vacuous"."web_sales_date_month_of_year" in (1,2,3,4) ) or ( "vacuous"."catalog_sales_date_year" = 2002 and "vacuous"."catalog_sales_date_month_of_year" in (1,2,3,4) ) ) THEN "vacuous"."customer_id" ELSE NULL END as "relevant_customers"
FROM
    "vacuous"),
young as (
SELECT
    "concerned"."relevant_customers" as "relevant_customers"
FROM
    "concerned"
GROUP BY
    1),
abhorrent as (
SELECT
    "customer_customers"."C_CUSTOMER_SK" as "customer_id",
    "customer_demographics_customer_demographics"."CD_CREDIT_RATING" as "customer_demographics_credit_rating",
    "customer_demographics_customer_demographics"."CD_DEP_COLLEGE_COUNT" as "customer_demographics_college_dependent_count",
    "customer_demographics_customer_demographics"."CD_DEP_COUNT" as "customer_demographics_dependent_count",
    "customer_demographics_customer_demographics"."CD_DEP_EMPLOYED_COUNT" as "customer_demographics_employed_dependent_count",
    "customer_demographics_customer_demographics"."CD_EDUCATION_STATUS" as "customer_demographics_education_status",
    "customer_demographics_customer_demographics"."CD_GENDER" as "customer_demographics_gender",
    "customer_demographics_customer_demographics"."CD_MARITAL_STATUS" as "customer_demographics_marital_status",
    "customer_demographics_customer_demographics"."CD_PURCHASE_ESTIMATE" as "customer_demographics_purchase_estimate"
FROM
    "memory"."customer" as "customer_customers"
    INNER JOIN "memory"."customer_demographics" as "customer_demographics_customer_demographics" on "customer_customers"."C_CURRENT_CDEMO_SK" = "customer_demographics_customer_demographics"."CD_DEMO_SK"
WHERE
    "customer_demographics_customer_demographics"."CD_GENDER" is not null and "customer_customers"."C_CUSTOMER_SK" in (select young."relevant_customers" from young where young."relevant_customers" is not null)
),
sweltering as (
SELECT
    "abhorrent"."customer_demographics_college_dependent_count" as "customer_demographics_college_dependent_count",
    "abhorrent"."customer_demographics_credit_rating" as "customer_demographics_credit_rating",
    "abhorrent"."customer_demographics_dependent_count" as "customer_demographics_dependent_count",
    "abhorrent"."customer_demographics_education_status" as "customer_demographics_education_status",
    "abhorrent"."customer_demographics_employed_dependent_count" as "customer_demographics_employed_dependent_count",
    "abhorrent"."customer_demographics_gender" as "customer_demographics_gender",
    "abhorrent"."customer_demographics_marital_status" as "customer_demographics_marital_status",
    "abhorrent"."customer_demographics_purchase_estimate" as "customer_demographics_purchase_estimate",
    "abhorrent"."customer_id" as "customer_id"
FROM
    "abhorrent"
WHERE
    "abhorrent"."customer_id" in (select young."relevant_customers" from young where young."relevant_customers" is not null)

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
    "sweltering"."customer_demographics_gender" as "customer_demographics_gender",
    "sweltering"."customer_demographics_marital_status" as "customer_demographics_marital_status",
    "sweltering"."customer_demographics_education_status" as "customer_demographics_education_status",
    count("sweltering"."customer_id") as "cnt1",
    "sweltering"."customer_demographics_purchase_estimate" as "customer_demographics_purchase_estimate",
    count("sweltering"."customer_id") as "cnt2",
    "sweltering"."customer_demographics_credit_rating" as "customer_demographics_credit_rating",
    count("sweltering"."customer_id") as "cnt3",
    "sweltering"."customer_demographics_dependent_count" as "customer_demographics_dependent_count",
    count("sweltering"."customer_id") as "cnt4",
    "sweltering"."customer_demographics_employed_dependent_count" as "customer_demographics_employed_dependent_count",
    count("sweltering"."customer_id") as "cnt5",
    "sweltering"."customer_demographics_college_dependent_count" as "customer_demographics_college_dependent_count",
    count("sweltering"."customer_id") as "cnt6"
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
    "sweltering"."customer_demographics_gender" asc,
    "sweltering"."customer_demographics_marital_status" asc,
    "sweltering"."customer_demographics_education_status" asc,
    "sweltering"."customer_demographics_purchase_estimate" asc,
    "sweltering"."customer_demographics_credit_rating" asc,
    "sweltering"."customer_demographics_dependent_count" asc,
    "sweltering"."customer_demographics_employed_dependent_count" asc,
    "sweltering"."customer_demographics_college_dependent_count" asc
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
    "physical_sales_store_sales"."SS_CUSTOMER_SK" as "customer_id",
    "physical_sales_store_sales"."SS_SOLD_DATE_SK" as "physical_sales_date_id"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
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
    RIGHT OUTER JOIN "questionable" on "yummy"."customer_id" is not distinct from "questionable"."customer_id"
    LEFT OUTER JOIN "quizzical" on coalesce("yummy"."customer_id", "questionable"."customer_id") = "quizzical"."customer_id"
    INNER JOIN "memory"."customer" as "customer_customers" on coalesce("yummy"."customer_id", "questionable"."customer_id", "quizzical"."customer_id") = "customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."customer_address" as "customer_address_customer_address" on "customer_customers"."C_CURRENT_ADDR_SK" = "customer_address_customer_address"."CA_ADDRESS_SK"
    LEFT OUTER JOIN "memory"."date_dim" as "catalog_sales_date_date" on "quizzical"."catalog_sales_date_id" = "catalog_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."date_dim" as "physical_sales_date_date" on "questionable"."physical_sales_date_id" = "physical_sales_date_date"."D_DATE_SK"
WHERE
    "physical_sales_date_date"."D_YEAR" = 2002 and "physical_sales_date_date"."D_MOY" in (1,2,3,4) and "customer_address_customer_address"."CA_COUNTY" in ('Rush County','Toole County','Jefferson County','Dona Ana County','La Porte County') and ( ( "web_sales_date_date"."D_YEAR" = 2002 and "web_sales_date_date"."D_MOY" in (1,2,3,4) ) or ( "catalog_sales_date_date"."D_YEAR" = 2002 and "catalog_sales_date_date"."D_MOY" in (1,2,3,4) ) )

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
