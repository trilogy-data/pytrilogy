
WITH 
cheerful as (
SELECT
    "store_sales_store_sales"."SS_STORE_SK" as "store_sales_store_id",
    count(CASE WHEN "store_sales_return_date_date"."D_DATE_SK" - "store_sales_store_sales"."SS_SOLD_DATE_SK" > -1 and "store_sales_return_date_date"."D_DATE_SK" - "store_sales_store_sales"."SS_SOLD_DATE_SK" <= 30 THEN "store_sales_store_sales"."SS_ITEM_SK" ELSE NULL END) as "_virt_agg_count_7691116690045464",
    count(CASE WHEN "store_sales_return_date_date"."D_DATE_SK" - "store_sales_store_sales"."SS_SOLD_DATE_SK" > 120 and "store_sales_return_date_date"."D_DATE_SK" - "store_sales_store_sales"."SS_SOLD_DATE_SK" <= 99999 THEN "store_sales_store_sales"."SS_ITEM_SK" ELSE NULL END) as "_virt_agg_count_7969998780980378",
    count(CASE WHEN "store_sales_return_date_date"."D_DATE_SK" - "store_sales_store_sales"."SS_SOLD_DATE_SK" > 30 and "store_sales_return_date_date"."D_DATE_SK" - "store_sales_store_sales"."SS_SOLD_DATE_SK" <= 60 THEN "store_sales_store_sales"."SS_ITEM_SK" ELSE NULL END) as "_virt_agg_count_3393740962845140",
    count(CASE WHEN "store_sales_return_date_date"."D_DATE_SK" - "store_sales_store_sales"."SS_SOLD_DATE_SK" > 60 and "store_sales_return_date_date"."D_DATE_SK" - "store_sales_store_sales"."SS_SOLD_DATE_SK" <= 90 THEN "store_sales_store_sales"."SS_ITEM_SK" ELSE NULL END) as "_virt_agg_count_4020156712075239",
    count(CASE WHEN "store_sales_return_date_date"."D_DATE_SK" - "store_sales_store_sales"."SS_SOLD_DATE_SK" > 90 and "store_sales_return_date_date"."D_DATE_SK" - "store_sales_store_sales"."SS_SOLD_DATE_SK" <= 120 THEN "store_sales_store_sales"."SS_ITEM_SK" ELSE NULL END) as "_virt_agg_count_5623669394588902"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    INNER JOIN "memory"."store_returns" as "store_sales_store_returns" on "store_sales_store_sales"."SS_ITEM_SK" = "store_sales_store_returns"."SR_ITEM_SK" AND "store_sales_store_sales"."SS_TICKET_NUMBER" = "store_sales_store_returns"."SR_TICKET_NUMBER"
    INNER JOIN "memory"."date_dim" as "store_sales_return_date_date" on "store_sales_store_returns"."SR_RETURNED_DATE_SK" = "store_sales_return_date_date"."D_DATE_SK"
WHERE
    "store_sales_return_date_date"."D_YEAR" = 2001 and "store_sales_return_date_date"."D_MOY" = 8 and "store_sales_store_sales"."SS_CUSTOMER_SK" = "store_sales_store_returns"."SR_CUSTOMER_SK" and "store_sales_store_sales"."SS_STORE_SK" is not null

GROUP BY
    1,
    "store_sales_store_sales"."SS_TICKET_NUMBER"),
questionable as (
SELECT
    "cheerful"."store_sales_store_id" as "store_sales_store_id",
    sum("cheerful"."_virt_agg_count_3393740962845140") as "days_31_60",
    sum("cheerful"."_virt_agg_count_4020156712075239") as "days_61_90",
    sum("cheerful"."_virt_agg_count_5623669394588902") as "days_91_120",
    sum("cheerful"."_virt_agg_count_7691116690045464") as "days_30",
    sum("cheerful"."_virt_agg_count_7969998780980378") as "days_120_plus"
FROM
    "cheerful"
GROUP BY
    1)
SELECT
    "store_sales_store_store"."S_STORE_NAME" as "store_sales_store_name",
    "store_sales_store_store"."S_COMPANY_ID" as "store_sales_store_company_id",
    "store_sales_store_store"."S_STREET_NUMBER" as "store_sales_store_street_number",
    "store_sales_store_store"."S_STREET_NAME" as "store_sales_store_street_name",
    "store_sales_store_store"."S_STREET_TYPE" as "store_sales_store_street_type",
    "store_sales_store_store"."S_SUITE_NUMBER" as "store_sales_store_suite_number",
    "store_sales_store_store"."S_CITY" as "store_sales_store_city",
    "store_sales_store_store"."S_COUNTY" as "store_sales_store_county",
    "store_sales_store_store"."S_STATE" as "store_sales_store_state",
    "store_sales_store_store"."S_ZIP" as "store_sales_store_zip",
    "questionable"."days_30" as "days_30",
    "questionable"."days_31_60" as "days_31_60",
    "questionable"."days_61_90" as "days_61_90",
    "questionable"."days_91_120" as "days_91_120",
    "questionable"."days_120_plus" as "days_120_plus"
FROM
    "memory"."store" as "store_sales_store_store"
    INNER JOIN "questionable" on "store_sales_store_store"."S_STORE_SK" = "questionable"."store_sales_store_id"
ORDER BY 
    "store_sales_store_store"."S_STORE_NAME" asc nulls first,
    "store_sales_store_store"."S_COMPANY_ID" asc nulls first,
    "store_sales_store_store"."S_STREET_NUMBER" asc nulls first,
    "store_sales_store_store"."S_STREET_NAME" asc nulls first,
    "store_sales_store_store"."S_STREET_TYPE" asc nulls first,
    "store_sales_store_store"."S_SUITE_NUMBER" asc nulls first,
    "store_sales_store_store"."S_CITY" asc nulls first,
    "store_sales_store_store"."S_COUNTY" asc nulls first,
    "store_sales_store_store"."S_STATE" asc nulls first,
    "store_sales_store_store"."S_ZIP" asc nulls first
LIMIT (100)