
WITH 
thoughtful as (
SELECT
    "physical_sales_return_date_date"."D_DATE_SK" as "physical_sales_return_date_id",
    "physical_sales_store_sales"."SS_ITEM_SK" as "physical_sales_item_id",
    "physical_sales_store_sales"."SS_SOLD_DATE_SK" as "physical_sales_date_id",
    "physical_sales_store_sales"."SS_STORE_SK" as "physical_sales_store_id",
    "physical_sales_store_sales"."SS_TICKET_NUMBER" as "physical_sales_ticket_number",
    "physical_sales_store_store"."S_CITY" as "physical_sales_store_city",
    "physical_sales_store_store"."S_COMPANY_ID" as "physical_sales_store_company_id",
    "physical_sales_store_store"."S_COUNTY" as "physical_sales_store_county",
    "physical_sales_store_store"."S_STATE" as "physical_sales_store_state",
    "physical_sales_store_store"."S_STORE_NAME" as "physical_sales_store_name",
    "physical_sales_store_store"."S_STREET_NAME" as "physical_sales_store_street_name",
    "physical_sales_store_store"."S_STREET_NUMBER" as "physical_sales_store_street_number",
    "physical_sales_store_store"."S_STREET_TYPE" as "physical_sales_store_street_type",
    "physical_sales_store_store"."S_SUITE_NUMBER" as "physical_sales_store_suite_number",
    "physical_sales_store_store"."S_ZIP" as "physical_sales_store_zip"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."store_returns" as "physical_sales_store_returns" on "physical_sales_store_sales"."SS_ITEM_SK" = "physical_sales_store_returns"."SR_ITEM_SK" AND "physical_sales_store_sales"."SS_TICKET_NUMBER" = "physical_sales_store_returns"."SR_TICKET_NUMBER"
    INNER JOIN "memory"."store" as "physical_sales_store_store" on "physical_sales_store_sales"."SS_STORE_SK" = "physical_sales_store_store"."S_STORE_SK"
    INNER JOIN "memory"."date_dim" as "physical_sales_return_date_date" on "physical_sales_store_returns"."SR_RETURNED_DATE_SK" = "physical_sales_return_date_date"."D_DATE_SK"
WHERE
    "physical_sales_return_date_date"."D_YEAR" = 2001 and "physical_sales_return_date_date"."D_MOY" = 8 and "physical_sales_store_sales"."SS_CUSTOMER_SK" = "physical_sales_store_returns"."SR_CUSTOMER_SK" and "physical_sales_store_sales"."SS_STORE_SK" is not null
),
cooperative as (
SELECT
    "thoughtful"."physical_sales_item_id" as "physical_sales_item_id",
    "thoughtful"."physical_sales_return_date_id" - "thoughtful"."physical_sales_date_id" as "days_to_return",
    "thoughtful"."physical_sales_store_city" as "physical_sales_store_city",
    "thoughtful"."physical_sales_store_company_id" as "physical_sales_store_company_id",
    "thoughtful"."physical_sales_store_county" as "physical_sales_store_county",
    "thoughtful"."physical_sales_store_id" as "physical_sales_store_id",
    "thoughtful"."physical_sales_store_name" as "physical_sales_store_name",
    "thoughtful"."physical_sales_store_state" as "physical_sales_store_state",
    "thoughtful"."physical_sales_store_street_name" as "physical_sales_store_street_name",
    "thoughtful"."physical_sales_store_street_number" as "physical_sales_store_street_number",
    "thoughtful"."physical_sales_store_street_type" as "physical_sales_store_street_type",
    "thoughtful"."physical_sales_store_suite_number" as "physical_sales_store_suite_number",
    "thoughtful"."physical_sales_store_zip" as "physical_sales_store_zip",
    "thoughtful"."physical_sales_ticket_number" as "physical_sales_ticket_number"
FROM
    "thoughtful"),
yummy as (
SELECT
    "cooperative"."physical_sales_store_id" as "physical_sales_store_id",
    count(CASE WHEN "cooperative"."days_to_return" > -1 and "cooperative"."days_to_return" <= 30 THEN "cooperative"."physical_sales_item_id" ELSE NULL END) as "_virt_agg_count_6396225769465204",
    count(CASE WHEN "cooperative"."days_to_return" > 120 and "cooperative"."days_to_return" <= 99999 THEN "cooperative"."physical_sales_item_id" ELSE NULL END) as "_virt_agg_count_6261479071707891",
    count(CASE WHEN "cooperative"."days_to_return" > 30 and "cooperative"."days_to_return" <= 60 THEN "cooperative"."physical_sales_item_id" ELSE NULL END) as "_virt_agg_count_1754169376042040",
    count(CASE WHEN "cooperative"."days_to_return" > 60 and "cooperative"."days_to_return" <= 90 THEN "cooperative"."physical_sales_item_id" ELSE NULL END) as "_virt_agg_count_9524432267113409",
    count(CASE WHEN "cooperative"."days_to_return" > 90 and "cooperative"."days_to_return" <= 120 THEN "cooperative"."physical_sales_item_id" ELSE NULL END) as "_virt_agg_count_5608833957360839"
FROM
    "cooperative"
GROUP BY
    1,
    "cooperative"."physical_sales_ticket_number"),
abundant as (
SELECT
    "cooperative"."physical_sales_store_city" as "physical_sales_store_city",
    "cooperative"."physical_sales_store_company_id" as "physical_sales_store_company_id",
    "cooperative"."physical_sales_store_county" as "physical_sales_store_county",
    "cooperative"."physical_sales_store_id" as "physical_sales_store_id",
    "cooperative"."physical_sales_store_name" as "physical_sales_store_name",
    "cooperative"."physical_sales_store_state" as "physical_sales_store_state",
    "cooperative"."physical_sales_store_street_name" as "physical_sales_store_street_name",
    "cooperative"."physical_sales_store_street_number" as "physical_sales_store_street_number",
    "cooperative"."physical_sales_store_street_type" as "physical_sales_store_street_type",
    "cooperative"."physical_sales_store_suite_number" as "physical_sales_store_suite_number",
    "cooperative"."physical_sales_store_zip" as "physical_sales_store_zip"
FROM
    "cooperative"
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
    11),
juicy as (
SELECT
    "yummy"."physical_sales_store_id" as "physical_sales_store_id",
    sum("yummy"."_virt_agg_count_1754169376042040") as "days_31_60",
    sum("yummy"."_virt_agg_count_5608833957360839") as "days_91_120",
    sum("yummy"."_virt_agg_count_6261479071707891") as "days_120_plus",
    sum("yummy"."_virt_agg_count_6396225769465204") as "days_30",
    sum("yummy"."_virt_agg_count_9524432267113409") as "days_61_90"
FROM
    "yummy"
GROUP BY
    1)
SELECT
    "abundant"."physical_sales_store_name" as "physical_sales_store_name",
    "abundant"."physical_sales_store_company_id" as "physical_sales_store_company_id",
    "abundant"."physical_sales_store_street_number" as "physical_sales_store_street_number",
    "abundant"."physical_sales_store_street_name" as "physical_sales_store_street_name",
    "abundant"."physical_sales_store_street_type" as "physical_sales_store_street_type",
    "abundant"."physical_sales_store_suite_number" as "physical_sales_store_suite_number",
    "abundant"."physical_sales_store_city" as "physical_sales_store_city",
    "abundant"."physical_sales_store_county" as "physical_sales_store_county",
    "abundant"."physical_sales_store_state" as "physical_sales_store_state",
    "abundant"."physical_sales_store_zip" as "physical_sales_store_zip",
    "juicy"."days_30" as "days_30",
    "juicy"."days_31_60" as "days_31_60",
    "juicy"."days_61_90" as "days_61_90",
    "juicy"."days_91_120" as "days_91_120",
    "juicy"."days_120_plus" as "days_120_plus"
FROM
    "juicy"
    INNER JOIN "abundant" on "juicy"."physical_sales_store_id" = "abundant"."physical_sales_store_id"
ORDER BY 
    "abundant"."physical_sales_store_name" asc nulls first,
    "abundant"."physical_sales_store_company_id" asc nulls first,
    "abundant"."physical_sales_store_street_number" asc nulls first,
    "abundant"."physical_sales_store_street_name" asc nulls first,
    "abundant"."physical_sales_store_street_type" asc nulls first,
    "abundant"."physical_sales_store_suite_number" asc nulls first,
    "abundant"."physical_sales_store_city" asc nulls first,
    "abundant"."physical_sales_store_county" asc nulls first,
    "abundant"."physical_sales_store_state" asc nulls first,
    "abundant"."physical_sales_store_zip" asc nulls first
LIMIT (100)