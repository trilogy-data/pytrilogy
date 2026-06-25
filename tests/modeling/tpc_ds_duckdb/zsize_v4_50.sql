
WITH 
yummy as (
SELECT
    "physical_sales_store_sales"."SS_STORE_SK" as "physical_sales_store_id",
    count(CASE WHEN "physical_sales_return_date_date"."D_DATE_SK" - "physical_sales_store_sales"."SS_SOLD_DATE_SK" > -1 and "physical_sales_return_date_date"."D_DATE_SK" - "physical_sales_store_sales"."SS_SOLD_DATE_SK" <= 30 THEN "physical_sales_store_sales"."SS_ITEM_SK" ELSE NULL END) as "_virt_agg_count_6396225769465204",
    count(CASE WHEN "physical_sales_return_date_date"."D_DATE_SK" - "physical_sales_store_sales"."SS_SOLD_DATE_SK" > 120 and "physical_sales_return_date_date"."D_DATE_SK" - "physical_sales_store_sales"."SS_SOLD_DATE_SK" <= 99999 THEN "physical_sales_store_sales"."SS_ITEM_SK" ELSE NULL END) as "_virt_agg_count_6261479071707891",
    count(CASE WHEN "physical_sales_return_date_date"."D_DATE_SK" - "physical_sales_store_sales"."SS_SOLD_DATE_SK" > 30 and "physical_sales_return_date_date"."D_DATE_SK" - "physical_sales_store_sales"."SS_SOLD_DATE_SK" <= 60 THEN "physical_sales_store_sales"."SS_ITEM_SK" ELSE NULL END) as "_virt_agg_count_1754169376042040",
    count(CASE WHEN "physical_sales_return_date_date"."D_DATE_SK" - "physical_sales_store_sales"."SS_SOLD_DATE_SK" > 60 and "physical_sales_return_date_date"."D_DATE_SK" - "physical_sales_store_sales"."SS_SOLD_DATE_SK" <= 90 THEN "physical_sales_store_sales"."SS_ITEM_SK" ELSE NULL END) as "_virt_agg_count_9524432267113409",
    count(CASE WHEN "physical_sales_return_date_date"."D_DATE_SK" - "physical_sales_store_sales"."SS_SOLD_DATE_SK" > 90 and "physical_sales_return_date_date"."D_DATE_SK" - "physical_sales_store_sales"."SS_SOLD_DATE_SK" <= 120 THEN "physical_sales_store_sales"."SS_ITEM_SK" ELSE NULL END) as "_virt_agg_count_5608833957360839"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."store_returns" as "physical_sales_store_returns" on "physical_sales_store_sales"."SS_ITEM_SK" = "physical_sales_store_returns"."SR_ITEM_SK" AND "physical_sales_store_sales"."SS_TICKET_NUMBER" = "physical_sales_store_returns"."SR_TICKET_NUMBER"
    INNER JOIN "memory"."date_dim" as "physical_sales_return_date_date" on "physical_sales_store_returns"."SR_RETURNED_DATE_SK" = "physical_sales_return_date_date"."D_DATE_SK"
WHERE
    "physical_sales_return_date_date"."D_YEAR" = 2001 and "physical_sales_return_date_date"."D_MOY" = 8 and "physical_sales_store_sales"."SS_CUSTOMER_SK" = "physical_sales_store_returns"."SR_CUSTOMER_SK" and "physical_sales_store_sales"."SS_STORE_SK" is not null

GROUP BY
    1,
    "physical_sales_store_sales"."SS_TICKET_NUMBER"),
thoughtful as (
SELECT
    "physical_sales_store_sales"."SS_STORE_SK" as "physical_sales_store_id",
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
concerned as (
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
    "thoughtful"."physical_sales_store_name" as "physical_sales_store_name",
    "thoughtful"."physical_sales_store_company_id" as "physical_sales_store_company_id",
    "thoughtful"."physical_sales_store_street_number" as "physical_sales_store_street_number",
    "thoughtful"."physical_sales_store_street_name" as "physical_sales_store_street_name",
    "thoughtful"."physical_sales_store_street_type" as "physical_sales_store_street_type",
    "thoughtful"."physical_sales_store_suite_number" as "physical_sales_store_suite_number",
    "thoughtful"."physical_sales_store_city" as "physical_sales_store_city",
    "thoughtful"."physical_sales_store_county" as "physical_sales_store_county",
    "thoughtful"."physical_sales_store_state" as "physical_sales_store_state",
    "thoughtful"."physical_sales_store_zip" as "physical_sales_store_zip",
    "concerned"."days_30" as "days_30",
    "concerned"."days_31_60" as "days_31_60",
    "concerned"."days_61_90" as "days_61_90",
    "concerned"."days_91_120" as "days_91_120",
    "concerned"."days_120_plus" as "days_120_plus"
FROM
    "concerned"
    INNER JOIN "thoughtful" on "concerned"."physical_sales_store_id" = "thoughtful"."physical_sales_store_id"
ORDER BY 
    "thoughtful"."physical_sales_store_name" asc nulls first,
    "thoughtful"."physical_sales_store_company_id" asc nulls first,
    "thoughtful"."physical_sales_store_street_number" asc nulls first,
    "thoughtful"."physical_sales_store_street_name" asc nulls first,
    "thoughtful"."physical_sales_store_street_type" asc nulls first,
    "thoughtful"."physical_sales_store_suite_number" asc nulls first,
    "thoughtful"."physical_sales_store_city" asc nulls first,
    "thoughtful"."physical_sales_store_county" asc nulls first,
    "thoughtful"."physical_sales_store_state" asc nulls first,
    "thoughtful"."physical_sales_store_zip" asc nulls first
LIMIT (100)