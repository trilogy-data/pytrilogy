
WITH 
abundant as (
SELECT
    "cr_catalog_returns"."CR_RETURNING_ADDR_SK" as "cr_return_address_id",
    "cr_catalog_returns"."CR_RETURNING_CUSTOMER_SK" as "cr_billing_customer_id"
FROM
    "memory"."catalog_returns" as "cr_catalog_returns"
GROUP BY
    1,
    2),
cooperative as (
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
    "cr_billing_customer_customers"."C_SALUTATION" as "cr_billing_customer_salutation",
    "cr_return_address_customer_address"."CA_STATE" as "cr_return_address_state"
FROM
    "memory"."catalog_returns" as "cr_catalog_returns"
    INNER JOIN "memory"."date_dim" as "cr_date_date" on "cr_catalog_returns"."CR_RETURNED_DATE_SK" = "cr_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer_address" as "cr_return_address_customer_address" on "cr_catalog_returns"."CR_RETURNING_ADDR_SK" = "cr_return_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."customer" as "cr_billing_customer_customers" on "cr_catalog_returns"."CR_RETURNING_CUSTOMER_SK" = "cr_billing_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."customer_address" as "cr_billing_customer_address_customer_address" on "cr_billing_customer_customers"."C_CURRENT_ADDR_SK" = "cr_billing_customer_address_customer_address"."CA_ADDRESS_SK"
WHERE
    "cr_billing_customer_address_customer_address"."CA_STATE" = 'GA' and "cr_return_address_customer_address"."CA_STATE" is not null
),
yummy as (
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
uneven as (
SELECT
    "abundant"."cr_billing_customer_id" as "cr_billing_customer_id",
    "cr_return_address_customer_address"."CA_STATE" as "cr_return_address_state"
FROM
    "abundant"
    INNER JOIN "memory"."customer_address" as "cr_return_address_customer_address" on "abundant"."cr_return_address_id" = "cr_return_address_customer_address"."CA_ADDRESS_SK"),
questionable as (
SELECT
    "cooperative"."cr_billing_customer_address_city" as "cr_billing_customer_address_city",
    "cooperative"."cr_billing_customer_address_country" as "cr_billing_customer_address_country",
    "cooperative"."cr_billing_customer_address_county" as "cr_billing_customer_address_county",
    "cooperative"."cr_billing_customer_address_gmt_offset" as "cr_billing_customer_address_gmt_offset",
    "cooperative"."cr_billing_customer_address_location_type" as "cr_billing_customer_address_location_type",
    "cooperative"."cr_billing_customer_address_state" as "cr_billing_customer_address_state",
    "cooperative"."cr_billing_customer_address_street_name" as "cr_billing_customer_address_street_name",
    "cooperative"."cr_billing_customer_address_street_number" as "cr_billing_customer_address_street_number",
    "cooperative"."cr_billing_customer_address_street_type" as "cr_billing_customer_address_street_type",
    "cooperative"."cr_billing_customer_address_suite_number" as "cr_billing_customer_address_suite_number",
    "cooperative"."cr_billing_customer_address_zip" as "cr_billing_customer_address_zip",
    "cooperative"."cr_billing_customer_first_name" as "cr_billing_customer_first_name",
    "cooperative"."cr_billing_customer_id" as "cr_billing_customer_id",
    "cooperative"."cr_billing_customer_last_name" as "cr_billing_customer_last_name",
    "cooperative"."cr_billing_customer_salutation" as "cr_billing_customer_salutation",
    "cooperative"."cr_billing_customer_text_id" as "cr_billing_customer_text_id",
    "cooperative"."cr_return_address_state" as "cr_return_address_state"
FROM
    "cooperative"),
concerned as (
SELECT
    "yummy"."customer_state" as "customer_state",
    coalesce("uneven"."cr_return_address_state","yummy"."cr_return_address_state") as "cr_return_address_state"
FROM
    "yummy"
    FULL JOIN "uneven" on "yummy"."cr_billing_customer_id" = "uneven"."cr_billing_customer_id" AND "yummy"."cr_return_address_state" is not distinct from "uneven"."cr_return_address_state"
GROUP BY
    1,
    2,
    coalesce("uneven"."cr_billing_customer_id","yummy"."cr_billing_customer_id")),
sparkling as (
SELECT
    "concerned"."cr_return_address_state" as "cr_return_address_state",
    1.2 * avg("concerned"."customer_state") as "scaled_state"
FROM
    "concerned"
GROUP BY
    1),
sweltering as (
SELECT
    "cooperative"."cr_billing_customer_id" as "cr_billing_customer_id",
    "yummy"."customer_state" as "customer_state",
    coalesce("cooperative"."cr_return_address_state","sparkling"."cr_return_address_state","yummy"."cr_return_address_state") as "cr_return_address_state"
FROM
    "yummy"
    INNER JOIN "cooperative" on "yummy"."cr_billing_customer_id" = "cooperative"."cr_billing_customer_id" AND "yummy"."cr_return_address_state" is not distinct from "cooperative"."cr_return_address_state"
    INNER JOIN "sparkling" on "yummy"."cr_return_address_state" is not distinct from "sparkling"."cr_return_address_state"
WHERE
    "yummy"."customer_state" > "sparkling"."scaled_state"

GROUP BY
    1,
    2,
    3)
SELECT
    "questionable"."cr_billing_customer_text_id" as "cr_billing_customer_text_id",
    "questionable"."cr_billing_customer_salutation" as "cr_billing_customer_salutation",
    "questionable"."cr_billing_customer_first_name" as "cr_billing_customer_first_name",
    "questionable"."cr_billing_customer_last_name" as "cr_billing_customer_last_name",
    "questionable"."cr_billing_customer_address_street_number" as "cr_billing_customer_address_street_number",
    "questionable"."cr_billing_customer_address_street_name" as "cr_billing_customer_address_street_name",
    "questionable"."cr_billing_customer_address_street_type" as "cr_billing_customer_address_street_type",
    "questionable"."cr_billing_customer_address_suite_number" as "cr_billing_customer_address_suite_number",
    "questionable"."cr_billing_customer_address_city" as "cr_billing_customer_address_city",
    "questionable"."cr_billing_customer_address_county" as "cr_billing_customer_address_county",
    "questionable"."cr_billing_customer_address_state" as "cr_billing_customer_address_state",
    "questionable"."cr_billing_customer_address_zip" as "cr_billing_customer_address_zip",
    "questionable"."cr_billing_customer_address_country" as "cr_billing_customer_address_country",
    "questionable"."cr_billing_customer_address_gmt_offset" as "cr_billing_customer_address_gmt_offset",
    "questionable"."cr_billing_customer_address_location_type" as "cr_billing_customer_address_location_type",
    "sweltering"."customer_state" as "customer_state"
FROM
    "questionable"
    INNER JOIN "sweltering" on "questionable"."cr_billing_customer_id" = "sweltering"."cr_billing_customer_id" AND "questionable"."cr_return_address_state" is not distinct from "sweltering"."cr_return_address_state"
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
    "questionable"."cr_billing_customer_id"
ORDER BY 
    "questionable"."cr_billing_customer_text_id" asc nulls first,
    "questionable"."cr_billing_customer_salutation" asc nulls first,
    "questionable"."cr_billing_customer_first_name" asc nulls first,
    "questionable"."cr_billing_customer_last_name" asc nulls first,
    "questionable"."cr_billing_customer_address_street_number" asc nulls first,
    "questionable"."cr_billing_customer_address_street_name" asc nulls first,
    "questionable"."cr_billing_customer_address_street_type" asc nulls first,
    "questionable"."cr_billing_customer_address_suite_number" asc nulls first,
    "questionable"."cr_billing_customer_address_city" asc nulls first,
    "questionable"."cr_billing_customer_address_county" asc nulls first,
    "questionable"."cr_billing_customer_address_state" asc nulls first,
    "questionable"."cr_billing_customer_address_zip" asc nulls first,
    "questionable"."cr_billing_customer_address_country" asc nulls first,
    "questionable"."cr_billing_customer_address_gmt_offset" asc nulls first,
    "questionable"."cr_billing_customer_address_location_type" asc nulls first,
    "sweltering"."customer_state" asc nulls first
LIMIT (100)