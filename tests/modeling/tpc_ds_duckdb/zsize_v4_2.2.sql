
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
young as (
SELECT
    "date_date"."D_DOW" as "date_day_of_week",
    "date_date"."D_WEEK_SEQ" as "date_week_seq",
    "web_sales_web_sales"."WS_EXT_SALES_PRICE" as "web_sales_ext_sales_price"
FROM
    "memory"."web_sales" as "web_sales_web_sales"
    INNER JOIN "memory"."date_dim" as "date_date" on "web_sales_web_sales"."WS_SOLD_DATE_SK" = "date_date"."D_DATE_SK"
WHERE
    "date_date"."D_WEEK_SEQ" in (select questionable."relevent_week_seq" from questionable where questionable."relevent_week_seq" is not null)
),
wakeful as (
SELECT
    "catalog_sales_catalog_sales"."CS_EXT_SALES_PRICE" as "catalog_sales_ext_sales_price",
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
sparkling as (
SELECT
    "young"."date_week_seq" as "date_week_seq",
    sum(CASE
	WHEN "young"."date_day_of_week" = 0 THEN "young"."web_sales_ext_sales_price"
	ELSE cast(0.0 as numeric(15,2))
	END) as "_virt_agg_sum_9238538473336606",
    sum(CASE
	WHEN "young"."date_day_of_week" = 1 THEN "young"."web_sales_ext_sales_price"
	ELSE cast(0.0 as numeric(15,2))
	END) as "_virt_agg_sum_8302396398525202",
    sum(CASE
	WHEN "young"."date_day_of_week" = 2 THEN "young"."web_sales_ext_sales_price"
	ELSE cast(0.0 as numeric(15,2))
	END) as "_virt_agg_sum_6823422966658347",
    sum(CASE
	WHEN "young"."date_day_of_week" = 3 THEN "young"."web_sales_ext_sales_price"
	ELSE cast(0.0 as numeric(15,2))
	END) as "_virt_agg_sum_2293560889657522",
    sum(CASE
	WHEN "young"."date_day_of_week" = 4 THEN "young"."web_sales_ext_sales_price"
	ELSE cast(0.0 as numeric(15,2))
	END) as "_virt_agg_sum_8086371301050807",
    sum(CASE
	WHEN "young"."date_day_of_week" = 5 THEN "young"."web_sales_ext_sales_price"
	ELSE cast(0.0 as numeric(15,2))
	END) as "_virt_agg_sum_8833754379564371",
    sum(CASE
	WHEN "young"."date_day_of_week" = 6 THEN "young"."web_sales_ext_sales_price"
	ELSE cast(0.0 as numeric(15,2))
	END) as "_virt_agg_sum_5332872165953971"
FROM
    "young"
WHERE
    "young"."date_week_seq" in (select questionable."relevent_week_seq" from questionable where questionable."relevent_week_seq" is not null)

GROUP BY
    1),
vacuous as (
SELECT
    "wakeful"."date_week_seq" as "date_week_seq",
    sum(CASE
	WHEN "wakeful"."date_day_of_week" = 0 THEN "wakeful"."catalog_sales_ext_sales_price"
	ELSE cast(0.0 as numeric(15,2))
	END) as "_virt_agg_sum_5446384850356435",
    sum(CASE
	WHEN "wakeful"."date_day_of_week" = 1 THEN "wakeful"."catalog_sales_ext_sales_price"
	ELSE cast(0.0 as numeric(15,2))
	END) as "_virt_agg_sum_7224794219444244",
    sum(CASE
	WHEN "wakeful"."date_day_of_week" = 2 THEN "wakeful"."catalog_sales_ext_sales_price"
	ELSE cast(0.0 as numeric(15,2))
	END) as "_virt_agg_sum_733654448721027",
    sum(CASE
	WHEN "wakeful"."date_day_of_week" = 3 THEN "wakeful"."catalog_sales_ext_sales_price"
	ELSE cast(0.0 as numeric(15,2))
	END) as "_virt_agg_sum_3727603509126659",
    sum(CASE
	WHEN "wakeful"."date_day_of_week" = 4 THEN "wakeful"."catalog_sales_ext_sales_price"
	ELSE cast(0.0 as numeric(15,2))
	END) as "_virt_agg_sum_2131371712943644",
    sum(CASE
	WHEN "wakeful"."date_day_of_week" = 5 THEN "wakeful"."catalog_sales_ext_sales_price"
	ELSE cast(0.0 as numeric(15,2))
	END) as "_virt_agg_sum_4518379598128005",
    sum(CASE
	WHEN "wakeful"."date_day_of_week" = 6 THEN "wakeful"."catalog_sales_ext_sales_price"
	ELSE cast(0.0 as numeric(15,2))
	END) as "_virt_agg_sum_7662941318754865"
FROM
    "wakeful"
WHERE
    "wakeful"."date_week_seq" in (select questionable."relevent_week_seq" from questionable where questionable."relevent_week_seq" is not null)

GROUP BY
    1),
sweltering as (
SELECT
    "sparkling"."_virt_agg_sum_2293560889657522" as "_virt_agg_sum_2293560889657522",
    "sparkling"."_virt_agg_sum_5332872165953971" as "_virt_agg_sum_5332872165953971",
    "sparkling"."_virt_agg_sum_6823422966658347" as "_virt_agg_sum_6823422966658347",
    "sparkling"."_virt_agg_sum_8086371301050807" as "_virt_agg_sum_8086371301050807",
    "sparkling"."_virt_agg_sum_8302396398525202" as "_virt_agg_sum_8302396398525202",
    "sparkling"."_virt_agg_sum_8833754379564371" as "_virt_agg_sum_8833754379564371",
    "sparkling"."_virt_agg_sum_9238538473336606" as "_virt_agg_sum_9238538473336606",
    "vacuous"."_virt_agg_sum_2131371712943644" as "_virt_agg_sum_2131371712943644",
    "vacuous"."_virt_agg_sum_3727603509126659" as "_virt_agg_sum_3727603509126659",
    "vacuous"."_virt_agg_sum_4518379598128005" as "_virt_agg_sum_4518379598128005",
    "vacuous"."_virt_agg_sum_5446384850356435" as "_virt_agg_sum_5446384850356435",
    "vacuous"."_virt_agg_sum_7224794219444244" as "_virt_agg_sum_7224794219444244",
    "vacuous"."_virt_agg_sum_733654448721027" as "_virt_agg_sum_733654448721027",
    "vacuous"."_virt_agg_sum_7662941318754865" as "_virt_agg_sum_7662941318754865",
    "vacuous"."date_week_seq" as "date_week_seq",
    lead("sparkling"."_virt_agg_sum_2293560889657522" + "vacuous"."_virt_agg_sum_3727603509126659", 53) over (order by "vacuous"."date_week_seq" asc ) as "_virt_window_lead_9386088415621209",
    lead("sparkling"."_virt_agg_sum_5332872165953971" + "vacuous"."_virt_agg_sum_7662941318754865", 53) over (order by "vacuous"."date_week_seq" asc ) as "_virt_window_lead_1615489443759951",
    lead("sparkling"."_virt_agg_sum_6823422966658347" + "vacuous"."_virt_agg_sum_733654448721027", 53) over (order by "vacuous"."date_week_seq" asc ) as "_virt_window_lead_1732363590168359",
    lead("sparkling"."_virt_agg_sum_8086371301050807" + "vacuous"."_virt_agg_sum_2131371712943644", 53) over (order by "vacuous"."date_week_seq" asc ) as "_virt_window_lead_9762136461725141",
    lead("sparkling"."_virt_agg_sum_8302396398525202" + "vacuous"."_virt_agg_sum_7224794219444244", 53) over (order by "vacuous"."date_week_seq" asc ) as "_virt_window_lead_4790424210530227",
    lead("sparkling"."_virt_agg_sum_8833754379564371" + "vacuous"."_virt_agg_sum_4518379598128005", 53) over (order by "vacuous"."date_week_seq" asc ) as "_virt_window_lead_9976629776715537",
    lead("sparkling"."_virt_agg_sum_9238538473336606" + "vacuous"."_virt_agg_sum_5446384850356435", 53) over (order by "vacuous"."date_week_seq" asc ) as "_virt_window_lead_7125692363367989"
FROM
    "sparkling"
    INNER JOIN "vacuous" on "sparkling"."date_week_seq" = "vacuous"."date_week_seq")
SELECT
    "sweltering"."date_week_seq" as "date_week_seq",
    round(( "sweltering"."_virt_agg_sum_9238538473336606" + "sweltering"."_virt_agg_sum_5446384850356435" ) / ("sweltering"."_virt_window_lead_7125692363367989"),2) as "sunday_increase",
    round(( "sweltering"."_virt_agg_sum_8302396398525202" + "sweltering"."_virt_agg_sum_7224794219444244" ) / ("sweltering"."_virt_window_lead_4790424210530227"),2) as "monday_increase",
    round(( "sweltering"."_virt_agg_sum_6823422966658347" + "sweltering"."_virt_agg_sum_733654448721027" ) / ("sweltering"."_virt_window_lead_1732363590168359"),2) as "tuesday_increase",
    round(( "sweltering"."_virt_agg_sum_2293560889657522" + "sweltering"."_virt_agg_sum_3727603509126659" ) / ("sweltering"."_virt_window_lead_9386088415621209"),2) as "wednesday_increase",
    round(( "sweltering"."_virt_agg_sum_8086371301050807" + "sweltering"."_virt_agg_sum_2131371712943644" ) / ("sweltering"."_virt_window_lead_9762136461725141"),2) as "thursday_increase",
    round(( "sweltering"."_virt_agg_sum_8833754379564371" + "sweltering"."_virt_agg_sum_4518379598128005" ) / ("sweltering"."_virt_window_lead_9976629776715537"),2) as "friday_increase",
    round(( "sweltering"."_virt_agg_sum_5332872165953971" + "sweltering"."_virt_agg_sum_7662941318754865" ) / ("sweltering"."_virt_window_lead_1615489443759951"),2) as "saturday_increase"
FROM
    "sweltering"
WHERE
    round(( "sweltering"."_virt_agg_sum_9238538473336606" + "sweltering"."_virt_agg_sum_5446384850356435" ) / ("sweltering"."_virt_window_lead_7125692363367989"),2) is not null

ORDER BY 
    "sweltering"."date_week_seq" asc nulls first
LIMIT (100)