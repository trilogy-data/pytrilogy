
WITH 
young as (
SELECT
    "cr_catalog_returns"."CR_RETURNING_CUSTOMER_SK" as "cr_billing_customer_id",
    "cr_return_address_customer_address"."CA_STATE" as "cr_return_address_state"
FROM
    "memory"."catalog_returns" as "cr_catalog_returns"
    INNER JOIN "memory"."date_dim" as "cr_date_date" on "cr_catalog_returns"."CR_RETURNED_DATE_SK" = "cr_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer_address" as "cr_return_address_customer_address" on "cr_catalog_returns"."CR_RETURNING_ADDR_SK" = "cr_return_address_customer_address"."CA_ADDRESS_SK"
WHERE
    "cr_return_address_customer_address"."CA_STATE" is not null
),
abundant as (
SELECT
    "cr_catalog_returns"."CR_RETURNING_CUSTOMER_SK" as "cr_billing_customer_id",
    "cr_return_address_customer_address"."CA_STATE" as "cr_return_address_state",
    sum(CASE WHEN "cr_date_date"."D_YEAR" = 2000 and "cr_return_address_customer_address"."CA_STATE" is not null THEN "cr_catalog_returns"."CR_RETURN_AMT_INC_TAX" ELSE NULL END) as "customer_state"
FROM
    "memory"."catalog_returns" as "cr_catalog_returns"
    INNER JOIN "memory"."date_dim" as "cr_date_date" on "cr_catalog_returns"."CR_RETURNED_DATE_SK" = "cr_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer_address" as "cr_return_address_customer_address" on "cr_catalog_returns"."CR_RETURNING_ADDR_SK" = "cr_return_address_customer_address"."CA_ADDRESS_SK"
WHERE
    "cr_return_address_customer_address"."CA_STATE" is not null

GROUP BY
    1,
    2),
wakeful as (
SELECT
    "cr_billing_customer_address_customer_address"."CA_CITY" as "cr_billing_customer_address_city",
    "cr_billing_customer_address_customer_address"."CA_COUNTRY" as "cr_billing_customer_address_country",
    "cr_billing_customer_address_customer_address"."CA_COUNTY" as "cr_billing_customer_address_county",
    "cr_billing_customer_address_customer_address"."CA_GMT_OFFSET" as "cr_billing_customer_address_gmt_offset",
    "cr_billing_customer_address_customer_address"."CA_LOCATION_TYPE" as "cr_billing_customer_address_location_type",
    "cr_billing_customer_address_customer_address"."CA_STATE" as "cr_billing_customer_address_state",
    "cr_billing_customer_address_customer_address"."CA_STREET_NAME" as "cr_billing_customer_address_street_name",
    "cr_billing_customer_address_customer_address"."CA_STREET_NUMBER" as "cr_billing_customer_address_street_number",
    "cr_billing_customer_address_customer_address"."CA_STREET_TYPE" as "cr_billing_customer_address_street_type",
    "cr_billing_customer_address_customer_address"."CA_SUITE_NUMBER" as "cr_billing_customer_address_suite_number",
    "cr_billing_customer_address_customer_address"."CA_ZIP" as "cr_billing_customer_address_zip",
    "cr_billing_customer_customers"."C_CUSTOMER_ID" as "cr_billing_customer_text_id",
    "cr_billing_customer_customers"."C_CUSTOMER_SK" as "cr_billing_customer_id",
    "cr_billing_customer_customers"."C_FIRST_NAME" as "cr_billing_customer_first_name",
    "cr_billing_customer_customers"."C_LAST_NAME" as "cr_billing_customer_last_name",
    "cr_billing_customer_customers"."C_SALUTATION" as "cr_billing_customer_salutation"
FROM
    "memory"."customer" as "cr_billing_customer_customers"
    INNER JOIN "memory"."customer_address" as "cr_billing_customer_address_customer_address" on "cr_billing_customer_customers"."C_CURRENT_ADDR_SK" = "cr_billing_customer_address_customer_address"."CA_ADDRESS_SK"
WHERE
    "cr_billing_customer_address_customer_address"."CA_STATE" = 'GA'
),
sparkling as (
SELECT
    "young"."cr_billing_customer_id" as "cr_billing_customer_id",
    "young"."cr_return_address_state" as "cr_return_address_state"
FROM
    "young"),
juicy as (
SELECT
    "abundant"."cr_return_address_state" as "cr_return_address_state",
    1.2 * avg("abundant"."customer_state") as "scaled_state"
FROM
    "abundant"
GROUP BY
    1),
cheerful as (
SELECT
    "wakeful"."cr_billing_customer_address_city" as "cr_billing_customer_address_city",
    "wakeful"."cr_billing_customer_address_country" as "cr_billing_customer_address_country",
    "wakeful"."cr_billing_customer_address_county" as "cr_billing_customer_address_county",
    "wakeful"."cr_billing_customer_address_gmt_offset" as "cr_billing_customer_address_gmt_offset",
    "wakeful"."cr_billing_customer_address_location_type" as "cr_billing_customer_address_location_type",
    "wakeful"."cr_billing_customer_address_state" as "cr_billing_customer_address_state",
    "wakeful"."cr_billing_customer_address_street_name" as "cr_billing_customer_address_street_name",
    "wakeful"."cr_billing_customer_address_street_number" as "cr_billing_customer_address_street_number",
    "wakeful"."cr_billing_customer_address_street_type" as "cr_billing_customer_address_street_type",
    "wakeful"."cr_billing_customer_address_suite_number" as "cr_billing_customer_address_suite_number",
    "wakeful"."cr_billing_customer_address_zip" as "cr_billing_customer_address_zip",
    "wakeful"."cr_billing_customer_first_name" as "cr_billing_customer_first_name",
    "wakeful"."cr_billing_customer_id" as "cr_billing_customer_id",
    "wakeful"."cr_billing_customer_last_name" as "cr_billing_customer_last_name",
    "wakeful"."cr_billing_customer_salutation" as "cr_billing_customer_salutation",
    "wakeful"."cr_billing_customer_text_id" as "cr_billing_customer_text_id"
FROM
    "wakeful"),
abhorrent as (
SELECT
    "abundant"."customer_state" as "customer_state",
    coalesce("abundant"."cr_billing_customer_id","sparkling"."cr_billing_customer_id") as "cr_billing_customer_id"
FROM
    "sparkling"
    RIGHT OUTER JOIN "abundant" on "sparkling"."cr_billing_customer_id" = "abundant"."cr_billing_customer_id" AND "sparkling"."cr_return_address_state" is not distinct from "abundant"."cr_return_address_state"
    INNER JOIN "juicy" on "sparkling"."cr_return_address_state" is not distinct from "juicy"."cr_return_address_state"
WHERE
    "abundant"."customer_state" > "juicy"."scaled_state"

GROUP BY
    1,
    2,
    coalesce("abundant"."cr_return_address_state","juicy"."cr_return_address_state","sparkling"."cr_return_address_state"))
SELECT
    "cheerful"."cr_billing_customer_text_id" as "cr_billing_customer_text_id",
    "cheerful"."cr_billing_customer_salutation" as "cr_billing_customer_salutation",
    "cheerful"."cr_billing_customer_first_name" as "cr_billing_customer_first_name",
    "cheerful"."cr_billing_customer_last_name" as "cr_billing_customer_last_name",
    "cheerful"."cr_billing_customer_address_street_number" as "cr_billing_customer_address_street_number",
    "cheerful"."cr_billing_customer_address_street_name" as "cr_billing_customer_address_street_name",
    "cheerful"."cr_billing_customer_address_street_type" as "cr_billing_customer_address_street_type",
    "cheerful"."cr_billing_customer_address_suite_number" as "cr_billing_customer_address_suite_number",
    "cheerful"."cr_billing_customer_address_city" as "cr_billing_customer_address_city",
    "cheerful"."cr_billing_customer_address_county" as "cr_billing_customer_address_county",
    "cheerful"."cr_billing_customer_address_state" as "cr_billing_customer_address_state",
    "cheerful"."cr_billing_customer_address_zip" as "cr_billing_customer_address_zip",
    "cheerful"."cr_billing_customer_address_country" as "cr_billing_customer_address_country",
    "cheerful"."cr_billing_customer_address_gmt_offset" as "cr_billing_customer_address_gmt_offset",
    "cheerful"."cr_billing_customer_address_location_type" as "cr_billing_customer_address_location_type",
    "abhorrent"."customer_state" as "customer_state"
FROM
    "abhorrent"
    INNER JOIN "cheerful" on "abhorrent"."cr_billing_customer_id" = "cheerful"."cr_billing_customer_id"
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
    16,
    "cheerful"."cr_billing_customer_id"
ORDER BY 
    "cheerful"."cr_billing_customer_text_id" asc nulls first,
    "cheerful"."cr_billing_customer_salutation" asc nulls first,
    "cheerful"."cr_billing_customer_first_name" asc nulls first,
    "cheerful"."cr_billing_customer_last_name" asc nulls first,
    "cheerful"."cr_billing_customer_address_street_number" asc nulls first,
    "cheerful"."cr_billing_customer_address_street_name" asc nulls first,
    "cheerful"."cr_billing_customer_address_street_type" asc nulls first,
    "cheerful"."cr_billing_customer_address_suite_number" asc nulls first,
    "cheerful"."cr_billing_customer_address_city" asc nulls first,
    "cheerful"."cr_billing_customer_address_county" asc nulls first,
    "cheerful"."cr_billing_customer_address_state" asc nulls first,
    "cheerful"."cr_billing_customer_address_zip" asc nulls first,
    "cheerful"."cr_billing_customer_address_country" asc nulls first,
    "cheerful"."cr_billing_customer_address_gmt_offset" asc nulls first,
    "cheerful"."cr_billing_customer_address_location_type" asc nulls first,
    "abhorrent"."customer_state" asc nulls first
LIMIT (100)