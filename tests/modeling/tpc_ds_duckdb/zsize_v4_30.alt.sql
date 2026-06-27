
WITH 
juicy as (
SELECT
    "web_returns_return_address_customer_address"."CA_STATE" as "web_returns_return_address_state",
    "web_returns_web_returns"."WR_RETURNING_CUSTOMER_SK" as "web_returns_billing_customer_id"
FROM
    "memory"."web_returns" as "web_returns_web_returns"
    INNER JOIN "memory"."date_dim" as "web_returns_return_date_date" on "web_returns_web_returns"."WR_RETURNED_DATE_SK" = "web_returns_return_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer_address" as "web_returns_return_address_customer_address" on "web_returns_web_returns"."WR_RETURNING_ADDR_SK" = "web_returns_return_address_customer_address"."CA_ADDRESS_SK"
WHERE
    "web_returns_return_address_customer_address"."CA_STATE" is not null
),
thoughtful as (
SELECT
    "web_returns_return_address_customer_address"."CA_STATE" as "web_returns_return_address_state",
    "web_returns_web_returns"."WR_RETURNING_CUSTOMER_SK" as "web_returns_billing_customer_id",
    sum(CASE WHEN "web_returns_return_date_date"."D_YEAR" = 2002 and "web_returns_return_address_customer_address"."CA_STATE" is not null THEN "web_returns_web_returns"."WR_RETURN_AMT" ELSE NULL END) as "customer_state_returns_2002"
FROM
    "memory"."web_returns" as "web_returns_web_returns"
    INNER JOIN "memory"."date_dim" as "web_returns_return_date_date" on "web_returns_web_returns"."WR_RETURNED_DATE_SK" = "web_returns_return_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer_address" as "web_returns_return_address_customer_address" on "web_returns_web_returns"."WR_RETURNING_ADDR_SK" = "web_returns_return_address_customer_address"."CA_ADDRESS_SK"
WHERE
    "web_returns_return_address_customer_address"."CA_STATE" is not null

GROUP BY
    1,
    2),
vacuous as (
SELECT
    "juicy"."web_returns_billing_customer_id" as "web_returns_billing_customer_id",
    "juicy"."web_returns_return_address_state" as "web_returns_return_address_state"
FROM
    "juicy"),
abundant as (
SELECT
    "thoughtful"."web_returns_return_address_state" as "web_returns_return_address_state",
    1.2 * avg("thoughtful"."customer_state_returns_2002") as "scaled_state_returns_2002"
FROM
    "thoughtful"
GROUP BY
    1),
concerned as (
SELECT
    "thoughtful"."customer_state_returns_2002" as "customer_state_returns_2002",
    coalesce("thoughtful"."web_returns_billing_customer_id","vacuous"."web_returns_billing_customer_id") as "web_returns_billing_customer_id"
FROM
    "vacuous"
    RIGHT OUTER JOIN "thoughtful" on "vacuous"."web_returns_billing_customer_id" = "thoughtful"."web_returns_billing_customer_id" AND "vacuous"."web_returns_return_address_state" is not distinct from "thoughtful"."web_returns_return_address_state"
    INNER JOIN "abundant" on "vacuous"."web_returns_return_address_state" is not distinct from "abundant"."web_returns_return_address_state"
WHERE
    "thoughtful"."customer_state_returns_2002" > "abundant"."scaled_state_returns_2002"

GROUP BY
    1,
    2,
    coalesce("abundant"."web_returns_return_address_state","thoughtful"."web_returns_return_address_state","vacuous"."web_returns_return_address_state"))
SELECT
    "web_returns_billing_customer_customers"."C_CUSTOMER_ID" as "web_returns_billing_customer_text_id",
    "web_returns_billing_customer_customers"."C_SALUTATION" as "web_returns_billing_customer_salutation",
    "web_returns_billing_customer_customers"."C_FIRST_NAME" as "web_returns_billing_customer_first_name",
    "web_returns_billing_customer_customers"."C_LAST_NAME" as "web_returns_billing_customer_last_name",
    "web_returns_billing_customer_customers"."C_PREFERRED_CUST_FLAG" as "web_returns_billing_customer_preferred_cust_flag",
    "web_returns_billing_customer_customers"."C_BIRTH_DAY" as "web_returns_billing_customer_birth_day",
    "web_returns_billing_customer_customers"."C_BIRTH_MONTH" as "web_returns_billing_customer_birth_month",
    "web_returns_billing_customer_customers"."C_BIRTH_YEAR" as "web_returns_billing_customer_birth_year",
    "web_returns_billing_customer_customers"."C_BIRTH_COUNTRY" as "web_returns_billing_customer_birth_country",
    "web_returns_billing_customer_customers"."C_LOGIN" as "web_returns_billing_customer_login",
    "web_returns_billing_customer_customers"."C_EMAIL_ADDRESS" as "web_returns_billing_customer_email_address",
    "web_returns_billing_customer_customers"."C_LAST_REVIEW_DATE_SK" as "web_returns_billing_customer_last_review_date",
    "concerned"."customer_state_returns_2002" as "customer_state_returns_2002"
FROM
    "concerned"
    INNER JOIN "memory"."customer" as "web_returns_billing_customer_customers" on "concerned"."web_returns_billing_customer_id" = "web_returns_billing_customer_customers"."C_CUSTOMER_SK"
ORDER BY 
    "web_returns_billing_customer_customers"."C_CUSTOMER_ID" asc nulls first,
    "web_returns_billing_customer_customers"."C_SALUTATION" asc nulls first,
    "web_returns_billing_customer_customers"."C_FIRST_NAME" asc nulls first,
    "web_returns_billing_customer_customers"."C_LAST_NAME" asc nulls first,
    "web_returns_billing_customer_customers"."C_PREFERRED_CUST_FLAG" asc nulls first,
    "web_returns_billing_customer_customers"."C_BIRTH_DAY" asc nulls first,
    "web_returns_billing_customer_customers"."C_BIRTH_MONTH" asc nulls first,
    "web_returns_billing_customer_customers"."C_BIRTH_YEAR" asc nulls first,
    "web_returns_billing_customer_customers"."C_BIRTH_COUNTRY" asc nulls first,
    "web_returns_billing_customer_customers"."C_LOGIN" asc nulls first,
    "web_returns_billing_customer_customers"."C_EMAIL_ADDRESS" asc nulls first,
    "web_returns_billing_customer_customers"."C_LAST_REVIEW_DATE_SK" asc nulls first,
    "concerned"."customer_state_returns_2002" asc nulls first
LIMIT (100)