
WITH 
cooperative as (
SELECT
    "web_returns_customer_customers"."C_BIRTH_COUNTRY" as "web_returns_customer_birth_country",
    "web_returns_customer_customers"."C_BIRTH_DAY" as "web_returns_customer_birth_day",
    "web_returns_customer_customers"."C_BIRTH_MONTH" as "web_returns_customer_birth_month",
    "web_returns_customer_customers"."C_BIRTH_YEAR" as "web_returns_customer_birth_year",
    "web_returns_customer_customers"."C_CUSTOMER_ID" as "web_returns_customer_text_id",
    "web_returns_customer_customers"."C_CUSTOMER_SK" as "web_returns_customer_id",
    "web_returns_customer_customers"."C_EMAIL_ADDRESS" as "web_returns_customer_email_address",
    "web_returns_customer_customers"."C_FIRST_NAME" as "web_returns_customer_first_name",
    "web_returns_customer_customers"."C_LAST_NAME" as "web_returns_customer_last_name",
    "web_returns_customer_customers"."C_LAST_REVIEW_DATE_SK" as "web_returns_customer_last_review_date",
    "web_returns_customer_customers"."C_LOGIN" as "web_returns_customer_login",
    "web_returns_customer_customers"."C_PREFERRED_CUST_FLAG" as "web_returns_customer_preferred_cust_flag",
    "web_returns_customer_customers"."C_SALUTATION" as "web_returns_customer_salutation",
    "web_returns_return_address_customer_address"."CA_STATE" as "web_returns_return_address_state"
FROM
    "memory"."web_returns" as "web_returns_web_returns"
    INNER JOIN "memory"."customer" as "web_returns_customer_customers" on "web_returns_web_returns"."WR_RETURNING_CUSTOMER_SK" = "web_returns_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."date_dim" as "web_returns_return_date_date" on "web_returns_web_returns"."WR_RETURNED_DATE_SK" = "web_returns_return_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer_address" as "web_returns_return_address_customer_address" on "web_returns_web_returns"."WR_RETURNING_ADDR_SK" = "web_returns_return_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."customer_address" as "web_returns_customer_address_customer_address" on "web_returns_customer_customers"."C_CURRENT_ADDR_SK" = "web_returns_customer_address_customer_address"."CA_ADDRESS_SK"
WHERE
    "web_returns_return_date_date"."D_YEAR" = 2002 and "web_returns_return_address_customer_address"."CA_STATE" is not null and "web_returns_customer_address_customer_address"."CA_STATE" = 'GA'
),
questionable as (
SELECT
    "web_returns_return_address_customer_address"."CA_STATE" as "web_returns_return_address_state",
    "web_returns_web_returns"."WR_RETURNING_CUSTOMER_SK" as "web_returns_customer_id",
    sum("web_returns_web_returns"."WR_RETURN_AMT") as "customer_state_returns_2002"
FROM
    "memory"."web_returns" as "web_returns_web_returns"
    INNER JOIN "memory"."customer_address" as "web_returns_return_address_customer_address" on "web_returns_web_returns"."WR_RETURNING_ADDR_SK" = "web_returns_return_address_customer_address"."CA_ADDRESS_SK"
GROUP BY
    1,
    2)
SELECT
    "cooperative"."web_returns_customer_text_id" as "web_returns_customer_text_id",
    "cooperative"."web_returns_customer_salutation" as "web_returns_customer_salutation",
    "cooperative"."web_returns_customer_first_name" as "web_returns_customer_first_name",
    "cooperative"."web_returns_customer_last_name" as "web_returns_customer_last_name",
    "cooperative"."web_returns_customer_preferred_cust_flag" as "web_returns_customer_preferred_cust_flag",
    "cooperative"."web_returns_customer_birth_day" as "web_returns_customer_birth_day",
    "cooperative"."web_returns_customer_birth_month" as "web_returns_customer_birth_month",
    "cooperative"."web_returns_customer_birth_year" as "web_returns_customer_birth_year",
    "cooperative"."web_returns_customer_birth_country" as "web_returns_customer_birth_country",
    "cooperative"."web_returns_customer_login" as "web_returns_customer_login",
    "cooperative"."web_returns_customer_email_address" as "web_returns_customer_email_address",
    "cooperative"."web_returns_customer_last_review_date" as "web_returns_customer_last_review_date",
    "questionable"."customer_state_returns_2002" as "customer_state_returns_2002"
FROM
    "cooperative"
    RIGHT OUTER JOIN "questionable" on "cooperative"."web_returns_customer_id" = "questionable"."web_returns_customer_id" AND "cooperative"."web_returns_return_address_state" is not distinct from "questionable"."web_returns_return_address_state"
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
    "cooperative"."web_returns_customer_id"
HAVING
    "customer_state_returns_2002" > 1.2 * avg("customer_state_returns_2002")

ORDER BY 
    "cooperative"."web_returns_customer_text_id" asc nulls first,
    "cooperative"."web_returns_customer_salutation" asc nulls first,
    "cooperative"."web_returns_customer_first_name" asc nulls first,
    "cooperative"."web_returns_customer_last_name" asc nulls first,
    "cooperative"."web_returns_customer_preferred_cust_flag" asc nulls first,
    "cooperative"."web_returns_customer_birth_day" asc nulls first,
    "cooperative"."web_returns_customer_birth_month" asc nulls first,
    "cooperative"."web_returns_customer_birth_year" asc nulls first,
    "cooperative"."web_returns_customer_birth_country" asc nulls first,
    "cooperative"."web_returns_customer_login" asc nulls first,
    "cooperative"."web_returns_customer_email_address" asc nulls first,
    "cooperative"."web_returns_customer_last_review_date" asc nulls first,
    "questionable"."customer_state_returns_2002" asc nulls first
LIMIT (100)
