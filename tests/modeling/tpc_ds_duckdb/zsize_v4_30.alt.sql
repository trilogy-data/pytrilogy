
WITH 
concerned as (
SELECT
    "web_returns_web_returns"."WR_RETURNING_ADDR_SK" as "web_returns_return_address_id",
    "web_returns_web_returns"."WR_RETURNING_CUSTOMER_SK" as "web_returns_billing_customer_id"
FROM
    "memory"."web_returns" as "web_returns_web_returns"
GROUP BY
    1,
    2),
cooperative as (
SELECT
    "web_returns_billing_customer_customers"."C_BIRTH_COUNTRY" as "web_returns_billing_customer_birth_country",
    "web_returns_billing_customer_customers"."C_BIRTH_DAY" as "web_returns_billing_customer_birth_day",
    "web_returns_billing_customer_customers"."C_BIRTH_MONTH" as "web_returns_billing_customer_birth_month",
    "web_returns_billing_customer_customers"."C_BIRTH_YEAR" as "web_returns_billing_customer_birth_year",
    "web_returns_billing_customer_customers"."C_CUSTOMER_ID" as "web_returns_billing_customer_text_id",
    "web_returns_billing_customer_customers"."C_CUSTOMER_SK" as "web_returns_billing_customer_id",
    "web_returns_billing_customer_customers"."C_EMAIL_ADDRESS" as "web_returns_billing_customer_email_address",
    "web_returns_billing_customer_customers"."C_FIRST_NAME" as "web_returns_billing_customer_first_name",
    "web_returns_billing_customer_customers"."C_LAST_NAME" as "web_returns_billing_customer_last_name",
    "web_returns_billing_customer_customers"."C_LAST_REVIEW_DATE_SK" as "web_returns_billing_customer_last_review_date",
    "web_returns_billing_customer_customers"."C_LOGIN" as "web_returns_billing_customer_login",
    "web_returns_billing_customer_customers"."C_PREFERRED_CUST_FLAG" as "web_returns_billing_customer_preferred_cust_flag",
    "web_returns_billing_customer_customers"."C_SALUTATION" as "web_returns_billing_customer_salutation",
    "web_returns_return_address_customer_address"."CA_STATE" as "web_returns_return_address_state"
FROM
    "memory"."web_returns" as "web_returns_web_returns"
    INNER JOIN "memory"."date_dim" as "web_returns_return_date_date" on "web_returns_web_returns"."WR_RETURNED_DATE_SK" = "web_returns_return_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer_address" as "web_returns_return_address_customer_address" on "web_returns_web_returns"."WR_RETURNING_ADDR_SK" = "web_returns_return_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."customer" as "web_returns_billing_customer_customers" on "web_returns_web_returns"."WR_RETURNING_CUSTOMER_SK" = "web_returns_billing_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."customer_address" as "web_returns_billing_customer_address_customer_address" on "web_returns_billing_customer_customers"."C_CURRENT_ADDR_SK" = "web_returns_billing_customer_address_customer_address"."CA_ADDRESS_SK"
WHERE
    "web_returns_billing_customer_address_customer_address"."CA_STATE" = 'GA' and "web_returns_return_address_customer_address"."CA_STATE" is not null
),
uneven as (
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
young as (
SELECT
    "concerned"."web_returns_billing_customer_id" as "web_returns_billing_customer_id",
    "web_returns_return_address_customer_address"."CA_STATE" as "web_returns_return_address_state"
FROM
    "concerned"
    INNER JOIN "memory"."customer_address" as "web_returns_return_address_customer_address" on "concerned"."web_returns_return_address_id" = "web_returns_return_address_customer_address"."CA_ADDRESS_SK"),
questionable as (
SELECT
    "cooperative"."web_returns_billing_customer_birth_country" as "web_returns_billing_customer_birth_country",
    "cooperative"."web_returns_billing_customer_birth_day" as "web_returns_billing_customer_birth_day",
    "cooperative"."web_returns_billing_customer_birth_month" as "web_returns_billing_customer_birth_month",
    "cooperative"."web_returns_billing_customer_birth_year" as "web_returns_billing_customer_birth_year",
    "cooperative"."web_returns_billing_customer_email_address" as "web_returns_billing_customer_email_address",
    "cooperative"."web_returns_billing_customer_first_name" as "web_returns_billing_customer_first_name",
    "cooperative"."web_returns_billing_customer_id" as "web_returns_billing_customer_id",
    "cooperative"."web_returns_billing_customer_last_name" as "web_returns_billing_customer_last_name",
    "cooperative"."web_returns_billing_customer_last_review_date" as "web_returns_billing_customer_last_review_date",
    "cooperative"."web_returns_billing_customer_login" as "web_returns_billing_customer_login",
    "cooperative"."web_returns_billing_customer_preferred_cust_flag" as "web_returns_billing_customer_preferred_cust_flag",
    "cooperative"."web_returns_billing_customer_salutation" as "web_returns_billing_customer_salutation",
    "cooperative"."web_returns_billing_customer_text_id" as "web_returns_billing_customer_text_id",
    "cooperative"."web_returns_return_address_state" as "web_returns_return_address_state"
FROM
    "cooperative"),
sparkling as (
SELECT
    "uneven"."customer_state_returns_2002" as "customer_state_returns_2002",
    coalesce("uneven"."web_returns_return_address_state","young"."web_returns_return_address_state") as "web_returns_return_address_state"
FROM
    "young"
    FULL JOIN "uneven" on "young"."web_returns_billing_customer_id" = "uneven"."web_returns_billing_customer_id" AND "young"."web_returns_return_address_state" is not distinct from "uneven"."web_returns_return_address_state"
GROUP BY
    1,
    2,
    coalesce("uneven"."web_returns_billing_customer_id","young"."web_returns_billing_customer_id")),
abundant as (
SELECT
    "questionable"."web_returns_billing_customer_birth_country" as "web_returns_billing_customer_birth_country",
    "questionable"."web_returns_billing_customer_birth_day" as "web_returns_billing_customer_birth_day",
    "questionable"."web_returns_billing_customer_birth_month" as "web_returns_billing_customer_birth_month",
    "questionable"."web_returns_billing_customer_birth_year" as "web_returns_billing_customer_birth_year",
    "questionable"."web_returns_billing_customer_email_address" as "web_returns_billing_customer_email_address",
    "questionable"."web_returns_billing_customer_first_name" as "web_returns_billing_customer_first_name",
    "questionable"."web_returns_billing_customer_id" as "web_returns_billing_customer_id",
    "questionable"."web_returns_billing_customer_last_name" as "web_returns_billing_customer_last_name",
    "questionable"."web_returns_billing_customer_last_review_date" as "web_returns_billing_customer_last_review_date",
    "questionable"."web_returns_billing_customer_login" as "web_returns_billing_customer_login",
    "questionable"."web_returns_billing_customer_preferred_cust_flag" as "web_returns_billing_customer_preferred_cust_flag",
    "questionable"."web_returns_billing_customer_salutation" as "web_returns_billing_customer_salutation",
    "questionable"."web_returns_billing_customer_text_id" as "web_returns_billing_customer_text_id",
    "questionable"."web_returns_return_address_state" as "web_returns_return_address_state"
FROM
    "questionable"
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
    14),
sweltering as (
SELECT
    "sparkling"."web_returns_return_address_state" as "web_returns_return_address_state",
    1.2 * avg("sparkling"."customer_state_returns_2002") as "scaled_state_returns_2002"
FROM
    "sparkling"
GROUP BY
    1),
macho as (
SELECT
    "questionable"."web_returns_billing_customer_id" as "web_returns_billing_customer_id",
    "questionable"."web_returns_return_address_state" as "web_returns_return_address_state",
    "uneven"."customer_state_returns_2002" as "customer_state_returns_2002"
FROM
    "questionable"
    INNER JOIN "uneven" on "questionable"."web_returns_billing_customer_id" = "uneven"."web_returns_billing_customer_id" AND "questionable"."web_returns_return_address_state" is not distinct from "uneven"."web_returns_return_address_state"
    INNER JOIN "sweltering" on "questionable"."web_returns_return_address_state" is not distinct from "sweltering"."web_returns_return_address_state"
WHERE
    "uneven"."customer_state_returns_2002" > "sweltering"."scaled_state_returns_2002"

GROUP BY
    1,
    2,
    3)
SELECT
    "abundant"."web_returns_billing_customer_text_id" as "web_returns_billing_customer_text_id",
    "abundant"."web_returns_billing_customer_salutation" as "web_returns_billing_customer_salutation",
    "abundant"."web_returns_billing_customer_first_name" as "web_returns_billing_customer_first_name",
    "abundant"."web_returns_billing_customer_last_name" as "web_returns_billing_customer_last_name",
    "abundant"."web_returns_billing_customer_preferred_cust_flag" as "web_returns_billing_customer_preferred_cust_flag",
    "abundant"."web_returns_billing_customer_birth_day" as "web_returns_billing_customer_birth_day",
    "abundant"."web_returns_billing_customer_birth_month" as "web_returns_billing_customer_birth_month",
    "abundant"."web_returns_billing_customer_birth_year" as "web_returns_billing_customer_birth_year",
    "abundant"."web_returns_billing_customer_birth_country" as "web_returns_billing_customer_birth_country",
    "abundant"."web_returns_billing_customer_login" as "web_returns_billing_customer_login",
    "abundant"."web_returns_billing_customer_email_address" as "web_returns_billing_customer_email_address",
    "abundant"."web_returns_billing_customer_last_review_date" as "web_returns_billing_customer_last_review_date",
    "macho"."customer_state_returns_2002" as "customer_state_returns_2002"
FROM
    "macho"
    LEFT OUTER JOIN "abundant" on "macho"."web_returns_billing_customer_id" = "abundant"."web_returns_billing_customer_id" AND "macho"."web_returns_return_address_state" is not distinct from "abundant"."web_returns_return_address_state"
ORDER BY 
    "abundant"."web_returns_billing_customer_text_id" asc nulls first,
    "abundant"."web_returns_billing_customer_salutation" asc nulls first,
    "abundant"."web_returns_billing_customer_first_name" asc nulls first,
    "abundant"."web_returns_billing_customer_last_name" asc nulls first,
    "abundant"."web_returns_billing_customer_preferred_cust_flag" asc nulls first,
    "abundant"."web_returns_billing_customer_birth_day" asc nulls first,
    "abundant"."web_returns_billing_customer_birth_month" asc nulls first,
    "abundant"."web_returns_billing_customer_birth_year" asc nulls first,
    "abundant"."web_returns_billing_customer_birth_country" asc nulls first,
    "abundant"."web_returns_billing_customer_login" asc nulls first,
    "abundant"."web_returns_billing_customer_email_address" asc nulls first,
    "abundant"."web_returns_billing_customer_last_review_date" asc nulls first,
    "macho"."customer_state_returns_2002" asc nulls first
LIMIT (100)