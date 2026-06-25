
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
    "thoughtful"."physical_sales_ticket_number" as "physical_sales_ticket_number",
    CASE WHEN "thoughtful"."physical_sales_return_date_id" - "thoughtful"."physical_sales_date_id" > -1 and "thoughtful"."physical_sales_return_date_id" - "thoughtful"."physical_sales_date_id" <= 30 THEN "thoughtful"."physical_sales_item_id" ELSE NULL END as "_virt_filter_id_3290865527698864",
    CASE WHEN "thoughtful"."physical_sales_return_date_id" - "thoughtful"."physical_sales_date_id" > 120 and "thoughtful"."physical_sales_return_date_id" - "thoughtful"."physical_sales_date_id" <= 99999 THEN "thoughtful"."physical_sales_item_id" ELSE NULL END as "_virt_filter_id_250001838041135",
    CASE WHEN "thoughtful"."physical_sales_return_date_id" - "thoughtful"."physical_sales_date_id" > 30 and "thoughtful"."physical_sales_return_date_id" - "thoughtful"."physical_sales_date_id" <= 60 THEN "thoughtful"."physical_sales_item_id" ELSE NULL END as "_virt_filter_id_9710315153806452",
    CASE WHEN "thoughtful"."physical_sales_return_date_id" - "thoughtful"."physical_sales_date_id" > 60 and "thoughtful"."physical_sales_return_date_id" - "thoughtful"."physical_sales_date_id" <= 90 THEN "thoughtful"."physical_sales_item_id" ELSE NULL END as "_virt_filter_id_8521335378550187",
    CASE WHEN "thoughtful"."physical_sales_return_date_id" - "thoughtful"."physical_sales_date_id" > 90 and "thoughtful"."physical_sales_return_date_id" - "thoughtful"."physical_sales_date_id" <= 120 THEN "thoughtful"."physical_sales_item_id" ELSE NULL END as "_virt_filter_id_917686748108553"
FROM
    "thoughtful"),
concerned as (
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
uneven as (
SELECT
    "cooperative"."physical_sales_store_id" as "physical_sales_store_id",
    count("cooperative"."_virt_filter_id_250001838041135") as "_virt_agg_count_6261479071707891",
    count("cooperative"."_virt_filter_id_3290865527698864") as "_virt_agg_count_6396225769465204",
    count("cooperative"."_virt_filter_id_8521335378550187") as "_virt_agg_count_9524432267113409",
    count("cooperative"."_virt_filter_id_917686748108553") as "_virt_agg_count_5608833957360839",
    count("cooperative"."_virt_filter_id_9710315153806452") as "_virt_agg_count_1754169376042040"
FROM
    "cooperative"
GROUP BY
    1,
    "cooperative"."physical_sales_ticket_number"),
juicy as (
SELECT
    "uneven"."physical_sales_store_id" as "physical_sales_store_id",
    sum("uneven"."_virt_agg_count_1754169376042040") as "days_31_60",
    sum("uneven"."_virt_agg_count_5608833957360839") as "days_91_120",
    sum("uneven"."_virt_agg_count_6261479071707891") as "days_120_plus",
    sum("uneven"."_virt_agg_count_6396225769465204") as "days_30",
    sum("uneven"."_virt_agg_count_9524432267113409") as "days_61_90"
FROM
    "uneven"
GROUP BY
    1)
SELECT
    "concerned"."physical_sales_store_name" as "physical_sales_store_name",
    "concerned"."physical_sales_store_company_id" as "physical_sales_store_company_id",
    "concerned"."physical_sales_store_street_number" as "physical_sales_store_street_number",
    "concerned"."physical_sales_store_street_name" as "physical_sales_store_street_name",
    "concerned"."physical_sales_store_street_type" as "physical_sales_store_street_type",
    "concerned"."physical_sales_store_suite_number" as "physical_sales_store_suite_number",
    "concerned"."physical_sales_store_city" as "physical_sales_store_city",
    "concerned"."physical_sales_store_county" as "physical_sales_store_county",
    "concerned"."physical_sales_store_state" as "physical_sales_store_state",
    "concerned"."physical_sales_store_zip" as "physical_sales_store_zip",
    "juicy"."days_30" as "days_30",
    "juicy"."days_31_60" as "days_31_60",
    "juicy"."days_61_90" as "days_61_90",
    "juicy"."days_91_120" as "days_91_120",
    "juicy"."days_120_plus" as "days_120_plus"
FROM
    "concerned"
    INNER JOIN "juicy" on "concerned"."physical_sales_store_id" = "juicy"."physical_sales_store_id"
ORDER BY 
    "concerned"."physical_sales_store_name" asc nulls first,
    "concerned"."physical_sales_store_company_id" asc nulls first,
    "concerned"."physical_sales_store_street_number" asc nulls first,
    "concerned"."physical_sales_store_street_name" asc nulls first,
    "concerned"."physical_sales_store_street_type" asc nulls first,
    "concerned"."physical_sales_store_suite_number" asc nulls first,
    "concerned"."physical_sales_store_city" asc nulls first,
    "concerned"."physical_sales_store_county" asc nulls first,
    "concerned"."physical_sales_store_state" asc nulls first,
    "concerned"."physical_sales_store_zip" asc nulls first
LIMIT (100)