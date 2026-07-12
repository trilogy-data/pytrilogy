
WITH 
questionable as (
SELECT
    CASE WHEN "date_date"."D_YEAR" in (2001,2002) THEN "date_date"."D_WEEK_SEQ" ELSE NULL END as "relevent_week_seq"
FROM
    "memory"."date_dim" as "catalog_sales_date_date"
    LEFT OUTER JOIN "memory"."date_dim" as "date_date" on "catalog_sales_date_date"."D_DATE_SK" = "date_date"."D_DATE_SK"
GROUP BY
    1),
thoughtful as (
SELECT
    "web_sales_web_sales"."WS_ORDER_NUMBER" as "web_sales_order_number"
FROM
    "memory"."web_sales" as "web_sales_web_sales"
    LEFT OUTER JOIN "memory"."catalog_sales" as "catalog_sales_catalog_sales" on "web_sales_web_sales"."WS_SOLD_DATE_SK" is not distinct from "catalog_sales_catalog_sales"."CS_SOLD_DATE_SK"
    INNER JOIN "memory"."date_dim" as "date_date" on "web_sales_web_sales"."WS_SOLD_DATE_SK" = "date_date"."D_DATE_SK"
WHERE
    "date_date"."D_WEEK_SEQ" in (select questionable."relevent_week_seq" from questionable where questionable."relevent_week_seq" is not null)
),
sparkling as (
SELECT
    "date_date"."D_DOW" as "date_day_of_week",
    "date_date"."D_WEEK_SEQ" as "date_week_seq",
    "web_sales_web_sales"."WS_EXT_SALES_PRICE" as "web_sales_ext_sales_price",
    "web_sales_web_sales"."WS_ITEM_SK" as "web_sales_item_id",
    "web_sales_web_sales"."WS_ORDER_NUMBER" as "web_sales_order_number"
FROM
    "memory"."web_sales" as "web_sales_web_sales"
    INNER JOIN "memory"."date_dim" as "date_date" on "web_sales_web_sales"."WS_SOLD_DATE_SK" = "date_date"."D_DATE_SK"
WHERE
    "date_date"."D_WEEK_SEQ" in (select questionable."relevent_week_seq" from questionable where questionable."relevent_week_seq" is not null)
),
wakeful as (
SELECT
    "catalog_sales_catalog_sales"."CS_EXT_SALES_PRICE" as "catalog_sales_ext_sales_price",
    "catalog_sales_catalog_sales"."CS_ITEM_SK" as "catalog_sales_item_id",
    "catalog_sales_catalog_sales"."CS_ORDER_NUMBER" as "catalog_sales_order_number",
    "date_date"."D_DOW" as "date_day_of_week",
    "date_date"."D_WEEK_SEQ" as "date_week_seq"
FROM
    "memory"."catalog_sales" as "catalog_sales_catalog_sales"
    INNER JOIN "memory"."date_dim" as "date_date" on "catalog_sales_catalog_sales"."CS_SOLD_DATE_SK" = "date_date"."D_DATE_SK"
WHERE
    "date_date"."D_WEEK_SEQ" in (select questionable."relevent_week_seq" from questionable where questionable."relevent_week_seq" is not null)
),
yummy as (
SELECT
    "thoughtful"."web_sales_order_number" as "web_sales_order_number"
FROM
    "thoughtful"),
juicy as (
SELECT
    "yummy"."web_sales_order_number" as "web_sales_order_number"
FROM
    "yummy"),
abhorrent as (
SELECT
    "sparkling"."date_day_of_week" as "date_day_of_week",
    "sparkling"."date_week_seq" as "date_week_seq",
    "sparkling"."web_sales_ext_sales_price" as "web_sales_ext_sales_price"
FROM
    "sparkling"
WHERE
    "sparkling"."date_week_seq" in (select questionable."relevent_week_seq" from questionable where questionable."relevent_week_seq" is not null)

GROUP BY
    1,
    2,
    3,
    "sparkling"."web_sales_item_id",
    "sparkling"."web_sales_order_number"),
vacuous as (
SELECT
    "wakeful"."catalog_sales_ext_sales_price" as "catalog_sales_ext_sales_price",
    "wakeful"."date_day_of_week" as "date_day_of_week",
    "wakeful"."date_week_seq" as "date_week_seq"
FROM
    "wakeful"
WHERE
    "wakeful"."date_week_seq" in (select questionable."relevent_week_seq" from questionable where questionable."relevent_week_seq" is not null)

GROUP BY
    1,
    2,
    3,
    "wakeful"."catalog_sales_item_id",
    "wakeful"."catalog_sales_order_number"),
late as (
SELECT
    "abhorrent"."date_week_seq" as "date_week_seq",
    sum(CASE
	WHEN "abhorrent"."date_day_of_week" = 0 THEN "abhorrent"."web_sales_ext_sales_price"
	ELSE cast(0.0 as numeric(15,2))
	END) as "_virt_agg_sum_9238538473336606",
    sum(CASE
	WHEN "abhorrent"."date_day_of_week" = 1 THEN "abhorrent"."web_sales_ext_sales_price"
	ELSE cast(0.0 as numeric(15,2))
	END) as "_virt_agg_sum_8302396398525202",
    sum(CASE
	WHEN "abhorrent"."date_day_of_week" = 2 THEN "abhorrent"."web_sales_ext_sales_price"
	ELSE cast(0.0 as numeric(15,2))
	END) as "_virt_agg_sum_6823422966658347",
    sum(CASE
	WHEN "abhorrent"."date_day_of_week" = 3 THEN "abhorrent"."web_sales_ext_sales_price"
	ELSE cast(0.0 as numeric(15,2))
	END) as "_virt_agg_sum_2293560889657522",
    sum(CASE
	WHEN "abhorrent"."date_day_of_week" = 4 THEN "abhorrent"."web_sales_ext_sales_price"
	ELSE cast(0.0 as numeric(15,2))
	END) as "_virt_agg_sum_8086371301050807",
    sum(CASE
	WHEN "abhorrent"."date_day_of_week" = 5 THEN "abhorrent"."web_sales_ext_sales_price"
	ELSE cast(0.0 as numeric(15,2))
	END) as "_virt_agg_sum_8833754379564371",
    sum(CASE
	WHEN "abhorrent"."date_day_of_week" = 6 THEN "abhorrent"."web_sales_ext_sales_price"
	ELSE cast(0.0 as numeric(15,2))
	END) as "_virt_agg_sum_5332872165953971"
FROM
    "abhorrent"
GROUP BY
    1),
young as (
SELECT
    "vacuous"."date_week_seq" as "date_week_seq",
    sum(CASE
	WHEN "vacuous"."date_day_of_week" = 0 THEN "vacuous"."catalog_sales_ext_sales_price"
	ELSE cast(0.0 as numeric(15,2))
	END) as "_virt_agg_sum_5446384850356435",
    sum(CASE
	WHEN "vacuous"."date_day_of_week" = 1 THEN "vacuous"."catalog_sales_ext_sales_price"
	ELSE cast(0.0 as numeric(15,2))
	END) as "_virt_agg_sum_7224794219444244",
    sum(CASE
	WHEN "vacuous"."date_day_of_week" = 2 THEN "vacuous"."catalog_sales_ext_sales_price"
	ELSE cast(0.0 as numeric(15,2))
	END) as "_virt_agg_sum_733654448721027",
    sum(CASE
	WHEN "vacuous"."date_day_of_week" = 3 THEN "vacuous"."catalog_sales_ext_sales_price"
	ELSE cast(0.0 as numeric(15,2))
	END) as "_virt_agg_sum_3727603509126659",
    sum(CASE
	WHEN "vacuous"."date_day_of_week" = 4 THEN "vacuous"."catalog_sales_ext_sales_price"
	ELSE cast(0.0 as numeric(15,2))
	END) as "_virt_agg_sum_2131371712943644",
    sum(CASE
	WHEN "vacuous"."date_day_of_week" = 5 THEN "vacuous"."catalog_sales_ext_sales_price"
	ELSE cast(0.0 as numeric(15,2))
	END) as "_virt_agg_sum_4518379598128005",
    sum(CASE
	WHEN "vacuous"."date_day_of_week" = 6 THEN "vacuous"."catalog_sales_ext_sales_price"
	ELSE cast(0.0 as numeric(15,2))
	END) as "_virt_agg_sum_7662941318754865"
FROM
    "vacuous"
GROUP BY
    1),
macho as (
SELECT
    "late"."_virt_agg_sum_2293560889657522" as "_virt_agg_sum_2293560889657522",
    "late"."_virt_agg_sum_5332872165953971" as "_virt_agg_sum_5332872165953971",
    "late"."_virt_agg_sum_6823422966658347" as "_virt_agg_sum_6823422966658347",
    "late"."_virt_agg_sum_8086371301050807" as "_virt_agg_sum_8086371301050807",
    "late"."_virt_agg_sum_8302396398525202" as "_virt_agg_sum_8302396398525202",
    "late"."_virt_agg_sum_8833754379564371" as "_virt_agg_sum_8833754379564371",
    "late"."_virt_agg_sum_9238538473336606" as "_virt_agg_sum_9238538473336606",
    "young"."_virt_agg_sum_2131371712943644" as "_virt_agg_sum_2131371712943644",
    "young"."_virt_agg_sum_3727603509126659" as "_virt_agg_sum_3727603509126659",
    "young"."_virt_agg_sum_4518379598128005" as "_virt_agg_sum_4518379598128005",
    "young"."_virt_agg_sum_5446384850356435" as "_virt_agg_sum_5446384850356435",
    "young"."_virt_agg_sum_7224794219444244" as "_virt_agg_sum_7224794219444244",
    "young"."_virt_agg_sum_733654448721027" as "_virt_agg_sum_733654448721027",
    "young"."_virt_agg_sum_7662941318754865" as "_virt_agg_sum_7662941318754865",
    "young"."date_week_seq" as "date_week_seq",
    lead("late"."_virt_agg_sum_2293560889657522" + "young"."_virt_agg_sum_3727603509126659", 53) over (order by "young"."date_week_seq" asc ) as "_virt_window_lead_9386088415621209",
    lead("late"."_virt_agg_sum_5332872165953971" + "young"."_virt_agg_sum_7662941318754865", 53) over (order by "young"."date_week_seq" asc ) as "_virt_window_lead_1615489443759951",
    lead("late"."_virt_agg_sum_6823422966658347" + "young"."_virt_agg_sum_733654448721027", 53) over (order by "young"."date_week_seq" asc ) as "_virt_window_lead_1732363590168359",
    lead("late"."_virt_agg_sum_8086371301050807" + "young"."_virt_agg_sum_2131371712943644", 53) over (order by "young"."date_week_seq" asc ) as "_virt_window_lead_9762136461725141",
    lead("late"."_virt_agg_sum_8302396398525202" + "young"."_virt_agg_sum_7224794219444244", 53) over (order by "young"."date_week_seq" asc ) as "_virt_window_lead_4790424210530227",
    lead("late"."_virt_agg_sum_8833754379564371" + "young"."_virt_agg_sum_4518379598128005", 53) over (order by "young"."date_week_seq" asc ) as "_virt_window_lead_9976629776715537",
    lead("late"."_virt_agg_sum_9238538473336606" + "young"."_virt_agg_sum_5446384850356435", 53) over (order by "young"."date_week_seq" asc ) as "_virt_window_lead_7125692363367989"
FROM
    "late"
    INNER JOIN "young" on "late"."date_week_seq" = "young"."date_week_seq")
SELECT
    "macho"."date_week_seq" as "date_week_seq",
    round(( "macho"."_virt_agg_sum_9238538473336606" + "macho"."_virt_agg_sum_5446384850356435" ) / ("macho"."_virt_window_lead_7125692363367989"),2) as "sunday_increase",
    round(( "macho"."_virt_agg_sum_8302396398525202" + "macho"."_virt_agg_sum_7224794219444244" ) / ("macho"."_virt_window_lead_4790424210530227"),2) as "monday_increase",
    round(( "macho"."_virt_agg_sum_6823422966658347" + "macho"."_virt_agg_sum_733654448721027" ) / ("macho"."_virt_window_lead_1732363590168359"),2) as "tuesday_increase",
    round(( "macho"."_virt_agg_sum_2293560889657522" + "macho"."_virt_agg_sum_3727603509126659" ) / ("macho"."_virt_window_lead_9386088415621209"),2) as "wednesday_increase",
    round(( "macho"."_virt_agg_sum_8086371301050807" + "macho"."_virt_agg_sum_2131371712943644" ) / ("macho"."_virt_window_lead_9762136461725141"),2) as "thursday_increase",
    round(( "macho"."_virt_agg_sum_8833754379564371" + "macho"."_virt_agg_sum_4518379598128005" ) / ("macho"."_virt_window_lead_9976629776715537"),2) as "friday_increase",
    round(( "macho"."_virt_agg_sum_5332872165953971" + "macho"."_virt_agg_sum_7662941318754865" ) / ("macho"."_virt_window_lead_1615489443759951"),2) as "saturday_increase"
FROM
    "macho"
WHERE
    round(( "macho"."_virt_agg_sum_9238538473336606" + "macho"."_virt_agg_sum_5446384850356435" ) / ("macho"."_virt_window_lead_7125692363367989"),2) is not null

ORDER BY 
    "macho"."date_week_seq" asc nulls first
LIMIT (100)