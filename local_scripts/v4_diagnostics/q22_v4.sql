
WITH 
cooperative as (
SELECT
    "date_date"."D_WEEK_SEQ" as "relevent_week_seq"
FROM
    "memory"."date_dim" as "catalog_sales_date_date"
    LEFT OUTER JOIN "memory"."date_dim" as "date_date" on "catalog_sales_date_date"."D_DATE_SK" = "date_date"."D_DATE_SK"
WHERE
    "date_date"."D_YEAR" in (2001,2002)

GROUP BY
    1),
juicy as (
SELECT
    "web_sales_web_sales"."WS_EXT_SALES_PRICE" as "web_sales_ext_sales_price",
    "web_sales_web_sales"."WS_SOLD_DATE_SK" as "date_id",
    "web_sales_web_sales"."WS_SOLD_DATE_SK" as "web_sales_date_id"
FROM
    "memory"."web_sales" as "web_sales_web_sales"),
quizzical as (
SELECT
    "catalog_sales_catalog_sales"."CS_EXT_SALES_PRICE" as "catalog_sales_ext_sales_price",
    "catalog_sales_catalog_sales"."CS_SOLD_DATE_SK" as "date_id",
    "catalog_sales_catalog_sales"."CS_SOLD_DATE_SK" as "web_sales_date_id"
FROM
    "memory"."catalog_sales" as "catalog_sales_catalog_sales"),
highfalutin as (
SELECT
    "date_date"."D_DATE_SK" as "date_id",
    "date_date"."D_DATE_SK" as "web_sales_date_id",
    "date_date"."D_DOW" as "date_day_of_week",
    "date_date"."D_WEEK_SEQ" as "date_week_seq"
FROM
    "memory"."date_dim" as "date_date"
WHERE
    "date_date"."D_WEEK_SEQ" in (select cooperative."relevent_week_seq" from cooperative where cooperative."relevent_week_seq" is not null)

GROUP BY
    1,
    3,
    4),
vacuous as (
SELECT
    "highfalutin"."date_week_seq" as "date_week_seq",
    sum(CASE
	WHEN "highfalutin"."date_day_of_week" = 0 THEN "juicy"."web_sales_ext_sales_price"
	ELSE cast(0.0 as numeric(15,2))
	END) as "_virt_agg_sum_9238538473336606",
    sum(CASE
	WHEN "highfalutin"."date_day_of_week" = 1 THEN "juicy"."web_sales_ext_sales_price"
	ELSE cast(0.0 as numeric(15,2))
	END) as "_virt_agg_sum_8302396398525202",
    sum(CASE
	WHEN "highfalutin"."date_day_of_week" = 2 THEN "juicy"."web_sales_ext_sales_price"
	ELSE cast(0.0 as numeric(15,2))
	END) as "_virt_agg_sum_6823422966658347",
    sum(CASE
	WHEN "highfalutin"."date_day_of_week" = 3 THEN "juicy"."web_sales_ext_sales_price"
	ELSE cast(0.0 as numeric(15,2))
	END) as "_virt_agg_sum_2293560889657522",
    sum(CASE
	WHEN "highfalutin"."date_day_of_week" = 4 THEN "juicy"."web_sales_ext_sales_price"
	ELSE cast(0.0 as numeric(15,2))
	END) as "_virt_agg_sum_8086371301050807",
    sum(CASE
	WHEN "highfalutin"."date_day_of_week" = 5 THEN "juicy"."web_sales_ext_sales_price"
	ELSE cast(0.0 as numeric(15,2))
	END) as "_virt_agg_sum_8833754379564371",
    sum(CASE
	WHEN "highfalutin"."date_day_of_week" = 6 THEN "juicy"."web_sales_ext_sales_price"
	ELSE cast(0.0 as numeric(15,2))
	END) as "_virt_agg_sum_5332872165953971"
FROM
    "juicy"
    RIGHT OUTER JOIN "highfalutin" on "juicy"."date_id" = "highfalutin"."date_id"
WHERE
    "highfalutin"."date_week_seq" in (select cooperative."relevent_week_seq" from cooperative where cooperative."relevent_week_seq" is not null)

GROUP BY
    1),
cheerful as (
SELECT
    "highfalutin"."date_week_seq" as "date_week_seq",
    sum(CASE
	WHEN "highfalutin"."date_day_of_week" = 0 THEN "quizzical"."catalog_sales_ext_sales_price"
	ELSE cast(0.0 as numeric(15,2))
	END) as "_virt_agg_sum_5446384850356435",
    sum(CASE
	WHEN "highfalutin"."date_day_of_week" = 1 THEN "quizzical"."catalog_sales_ext_sales_price"
	ELSE cast(0.0 as numeric(15,2))
	END) as "_virt_agg_sum_7224794219444244",
    sum(CASE
	WHEN "highfalutin"."date_day_of_week" = 2 THEN "quizzical"."catalog_sales_ext_sales_price"
	ELSE cast(0.0 as numeric(15,2))
	END) as "_virt_agg_sum_733654448721027",
    sum(CASE
	WHEN "highfalutin"."date_day_of_week" = 3 THEN "quizzical"."catalog_sales_ext_sales_price"
	ELSE cast(0.0 as numeric(15,2))
	END) as "_virt_agg_sum_3727603509126659",
    sum(CASE
	WHEN "highfalutin"."date_day_of_week" = 4 THEN "quizzical"."catalog_sales_ext_sales_price"
	ELSE cast(0.0 as numeric(15,2))
	END) as "_virt_agg_sum_2131371712943644",
    sum(CASE
	WHEN "highfalutin"."date_day_of_week" = 5 THEN "quizzical"."catalog_sales_ext_sales_price"
	ELSE cast(0.0 as numeric(15,2))
	END) as "_virt_agg_sum_4518379598128005",
    sum(CASE
	WHEN "highfalutin"."date_day_of_week" = 6 THEN "quizzical"."catalog_sales_ext_sales_price"
	ELSE cast(0.0 as numeric(15,2))
	END) as "_virt_agg_sum_7662941318754865"
FROM
    "quizzical"
    RIGHT OUTER JOIN "highfalutin" on "quizzical"."date_id" = "highfalutin"."date_id"
WHERE
    "highfalutin"."date_week_seq" in (select cooperative."relevent_week_seq" from cooperative where cooperative."relevent_week_seq" is not null)

GROUP BY
    1),
sparkling as (
SELECT
    "cheerful"."_virt_agg_sum_2131371712943644" as "_virt_agg_sum_2131371712943644",
    "cheerful"."_virt_agg_sum_3727603509126659" as "_virt_agg_sum_3727603509126659",
    "cheerful"."_virt_agg_sum_4518379598128005" as "_virt_agg_sum_4518379598128005",
    "cheerful"."_virt_agg_sum_5446384850356435" as "_virt_agg_sum_5446384850356435",
    "cheerful"."_virt_agg_sum_7224794219444244" as "_virt_agg_sum_7224794219444244",
    "cheerful"."_virt_agg_sum_733654448721027" as "_virt_agg_sum_733654448721027",
    "cheerful"."_virt_agg_sum_7662941318754865" as "_virt_agg_sum_7662941318754865",
    "cheerful"."date_week_seq" as "date_week_seq",
    "vacuous"."_virt_agg_sum_2293560889657522" as "_virt_agg_sum_2293560889657522",
    "vacuous"."_virt_agg_sum_5332872165953971" as "_virt_agg_sum_5332872165953971",
    "vacuous"."_virt_agg_sum_6823422966658347" as "_virt_agg_sum_6823422966658347",
    "vacuous"."_virt_agg_sum_8086371301050807" as "_virt_agg_sum_8086371301050807",
    "vacuous"."_virt_agg_sum_8302396398525202" as "_virt_agg_sum_8302396398525202",
    "vacuous"."_virt_agg_sum_8833754379564371" as "_virt_agg_sum_8833754379564371",
    "vacuous"."_virt_agg_sum_9238538473336606" as "_virt_agg_sum_9238538473336606",
    lead("vacuous"."_virt_agg_sum_2293560889657522" + "cheerful"."_virt_agg_sum_3727603509126659", 53) over (order by "cheerful"."date_week_seq" asc ) as "_virt_window_lead_9386088415621209",
    lead("vacuous"."_virt_agg_sum_5332872165953971" + "cheerful"."_virt_agg_sum_7662941318754865", 53) over (order by "cheerful"."date_week_seq" asc ) as "_virt_window_lead_1615489443759951",
    lead("vacuous"."_virt_agg_sum_6823422966658347" + "cheerful"."_virt_agg_sum_733654448721027", 53) over (order by "cheerful"."date_week_seq" asc ) as "_virt_window_lead_1732363590168359",
    lead("vacuous"."_virt_agg_sum_8086371301050807" + "cheerful"."_virt_agg_sum_2131371712943644", 53) over (order by "cheerful"."date_week_seq" asc ) as "_virt_window_lead_9762136461725141",
    lead("vacuous"."_virt_agg_sum_8302396398525202" + "cheerful"."_virt_agg_sum_7224794219444244", 53) over (order by "cheerful"."date_week_seq" asc ) as "_virt_window_lead_4790424210530227",
    lead("vacuous"."_virt_agg_sum_8833754379564371" + "cheerful"."_virt_agg_sum_4518379598128005", 53) over (order by "cheerful"."date_week_seq" asc ) as "_virt_window_lead_9976629776715537",
    lead("vacuous"."_virt_agg_sum_9238538473336606" + "cheerful"."_virt_agg_sum_5446384850356435", 53) over (order by "cheerful"."date_week_seq" asc ) as "_virt_window_lead_7125692363367989"
FROM
    "vacuous"
    INNER JOIN "cheerful" on "vacuous"."date_week_seq" = "cheerful"."date_week_seq")
SELECT
    "sparkling"."date_week_seq" as "date_week_seq",
    round(( "sparkling"."_virt_agg_sum_9238538473336606" + "sparkling"."_virt_agg_sum_5446384850356435" ) / ("sparkling"."_virt_window_lead_7125692363367989"),2) as "sunday_increase",
    round(( "sparkling"."_virt_agg_sum_8302396398525202" + "sparkling"."_virt_agg_sum_7224794219444244" ) / ("sparkling"."_virt_window_lead_4790424210530227"),2) as "monday_increase",
    round(( "sparkling"."_virt_agg_sum_6823422966658347" + "sparkling"."_virt_agg_sum_733654448721027" ) / ("sparkling"."_virt_window_lead_1732363590168359"),2) as "tuesday_increase",
    round(( "sparkling"."_virt_agg_sum_2293560889657522" + "sparkling"."_virt_agg_sum_3727603509126659" ) / ("sparkling"."_virt_window_lead_9386088415621209"),2) as "wednesday_increase",
    round(( "sparkling"."_virt_agg_sum_8086371301050807" + "sparkling"."_virt_agg_sum_2131371712943644" ) / ("sparkling"."_virt_window_lead_9762136461725141"),2) as "thursday_increase",
    round(( "sparkling"."_virt_agg_sum_8833754379564371" + "sparkling"."_virt_agg_sum_4518379598128005" ) / ("sparkling"."_virt_window_lead_9976629776715537"),2) as "friday_increase",
    round(( "sparkling"."_virt_agg_sum_5332872165953971" + "sparkling"."_virt_agg_sum_7662941318754865" ) / ("sparkling"."_virt_window_lead_1615489443759951"),2) as "saturday_increase"
FROM
    "sparkling"
WHERE
    round(( "sparkling"."_virt_agg_sum_9238538473336606" + "sparkling"."_virt_agg_sum_5446384850356435" ) / ("sparkling"."_virt_window_lead_7125692363367989"),2) is not null

ORDER BY 
    "sparkling"."date_week_seq" asc nulls first
LIMIT (100)