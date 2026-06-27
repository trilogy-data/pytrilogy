
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
    "sparkling"."_virt_agg_sum_2293560889657522" + "vacuous"."_virt_agg_sum_3727603509126659" as "wednesday_sales",
    "sparkling"."_virt_agg_sum_5332872165953971" + "vacuous"."_virt_agg_sum_7662941318754865" as "saturday_sales",
    "sparkling"."_virt_agg_sum_6823422966658347" + "vacuous"."_virt_agg_sum_733654448721027" as "tuesday_sales",
    "sparkling"."_virt_agg_sum_8086371301050807" + "vacuous"."_virt_agg_sum_2131371712943644" as "thursday_sales",
    "sparkling"."_virt_agg_sum_8302396398525202" + "vacuous"."_virt_agg_sum_7224794219444244" as "monday_sales",
    "sparkling"."_virt_agg_sum_8833754379564371" + "vacuous"."_virt_agg_sum_4518379598128005" as "friday_sales",
    "sparkling"."_virt_agg_sum_9238538473336606" + "vacuous"."_virt_agg_sum_5446384850356435" as "sunday_sales",
    "vacuous"."date_week_seq" as "date_week_seq"
FROM
    "sparkling"
    INNER JOIN "vacuous" on "sparkling"."date_week_seq" = "vacuous"."date_week_seq"),
macho as (
SELECT
    "sweltering"."date_week_seq" as "date_week_seq",
    lead("sweltering"."friday_sales", 53) over (order by "sweltering"."date_week_seq" asc ) as "_virt_window_lead_6651551761445993",
    lead("sweltering"."monday_sales", 53) over (order by "sweltering"."date_week_seq" asc ) as "_virt_window_lead_9809563217267406",
    lead("sweltering"."saturday_sales", 53) over (order by "sweltering"."date_week_seq" asc ) as "_virt_window_lead_5767709179323528",
    lead("sweltering"."sunday_sales", 53) over (order by "sweltering"."date_week_seq" asc ) as "_virt_window_lead_3949607449893123",
    lead("sweltering"."thursday_sales", 53) over (order by "sweltering"."date_week_seq" asc ) as "_virt_window_lead_2497781736791521",
    lead("sweltering"."tuesday_sales", 53) over (order by "sweltering"."date_week_seq" asc ) as "_virt_window_lead_5067641372653397",
    lead("sweltering"."wednesday_sales", 53) over (order by "sweltering"."date_week_seq" asc ) as "_virt_window_lead_315641373767519"
FROM
    "sweltering"),
scrawny as (
SELECT
    "macho"."_virt_window_lead_2497781736791521" as "_virt_window_lead_2497781736791521",
    "macho"."_virt_window_lead_315641373767519" as "_virt_window_lead_315641373767519",
    "macho"."_virt_window_lead_3949607449893123" as "_virt_window_lead_3949607449893123",
    "macho"."_virt_window_lead_5067641372653397" as "_virt_window_lead_5067641372653397",
    "macho"."_virt_window_lead_5767709179323528" as "_virt_window_lead_5767709179323528",
    "macho"."_virt_window_lead_6651551761445993" as "_virt_window_lead_6651551761445993",
    "macho"."_virt_window_lead_9809563217267406" as "_virt_window_lead_9809563217267406",
    "sweltering"."date_week_seq" as "date_week_seq",
    "sweltering"."friday_sales" as "friday_sales",
    "sweltering"."monday_sales" as "monday_sales",
    "sweltering"."saturday_sales" as "saturday_sales",
    "sweltering"."sunday_sales" as "sunday_sales",
    "sweltering"."thursday_sales" as "thursday_sales",
    "sweltering"."tuesday_sales" as "tuesday_sales",
    "sweltering"."wednesday_sales" as "wednesday_sales"
FROM
    "macho"
    INNER JOIN "sweltering" on "macho"."date_week_seq" = "sweltering"."date_week_seq")
SELECT
    "scrawny"."date_week_seq" as "date_week_seq",
    round(( "scrawny"."sunday_sales" ) / ("scrawny"."_virt_window_lead_3949607449893123"),2) as "sunday_increase",
    round(( "scrawny"."monday_sales" ) / ("scrawny"."_virt_window_lead_9809563217267406"),2) as "monday_increase",
    round(( "scrawny"."tuesday_sales" ) / ("scrawny"."_virt_window_lead_5067641372653397"),2) as "tuesday_increase",
    round(( "scrawny"."wednesday_sales" ) / ("scrawny"."_virt_window_lead_315641373767519"),2) as "wednesday_increase",
    round(( "scrawny"."thursday_sales" ) / ("scrawny"."_virt_window_lead_2497781736791521"),2) as "thursday_increase",
    round(( "scrawny"."friday_sales" ) / ("scrawny"."_virt_window_lead_6651551761445993"),2) as "friday_increase",
    round(( "scrawny"."saturday_sales" ) / ("scrawny"."_virt_window_lead_5767709179323528"),2) as "saturday_increase"
FROM
    "scrawny"
WHERE
    round(( "scrawny"."sunday_sales" ) / ("scrawny"."_virt_window_lead_3949607449893123"),2) is not null

ORDER BY 
    "scrawny"."date_week_seq" asc nulls first
LIMIT (100)