
WITH 
cheerful as (
SELECT
    "ws_web_sales"."WS_SHIP_DATE_SK" as "ws_ship_date_id",
    "ws_web_sales"."WS_SHIP_MODE_SK" as "ws_ship_mode_id",
    "ws_web_sales"."WS_SOLD_DATE_SK" as "ws_date_id",
    "ws_web_sales"."WS_WAREHOUSE_SK" as "ws_warehouse_id",
    "ws_web_sales"."WS_WEB_SITE_SK" as "ws_web_site_id"
FROM
    "memory"."web_sales" as "ws_web_sales"
WHERE
    "ws_web_sales"."WS_WAREHOUSE_SK" is not null and "ws_web_sales"."WS_SHIP_MODE_SK" is not null and "ws_web_sales"."WS_WEB_SITE_SK" is not null

GROUP BY
    1,
    2,
    3,
    4,
    5),
questionable as (
SELECT
    "cheerful"."ws_date_id" as "ws_date_id",
    "cheerful"."ws_ship_date_id" as "ws_ship_date_id",
    "cheerful"."ws_warehouse_id" as "ws_warehouse_id",
    "ws_ship_mode_ship_mode"."SM_TYPE" as "ws_ship_mode_type",
    "ws_warehouse_warehouse"."w_warehouse_name" as "ws_warehouse_name",
    "ws_web_site_web_site"."web_name" as "ws_web_site_name"
FROM
    "cheerful"
    LEFT OUTER JOIN "memory"."web_site" as "ws_web_site_web_site" on "cheerful"."ws_web_site_id" = "ws_web_site_web_site"."web_site_sk"
    INNER JOIN "memory"."date_dim" as "ws_ship_date_date" on "cheerful"."ws_ship_date_id" = "ws_ship_date_date"."D_DATE_SK"
    LEFT OUTER JOIN "memory"."ship_mode" as "ws_ship_mode_ship_mode" on "cheerful"."ws_ship_mode_id" = "ws_ship_mode_ship_mode"."SM_SHIP_MODE_SK"
    LEFT OUTER JOIN "memory"."warehouse" as "ws_warehouse_warehouse" on "cheerful"."ws_warehouse_id" = "ws_warehouse_warehouse"."w_warehouse_sk"
WHERE
    "ws_ship_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1211
),
uneven as (
SELECT
    "questionable"."ws_warehouse_id" as "ws_warehouse_id",
    SUBSTRING("questionable"."ws_warehouse_name",1,20) as "w_substr"
FROM
    "questionable"),
abundant as (
SELECT
    "questionable"."ws_ship_date_id" - "questionable"."ws_date_id" as "days_to_ship"
FROM
    "questionable")
SELECT
    sum(CASE
	WHEN "abundant"."days_to_ship" > 120 THEN 1
	ELSE 0
	END) as "days_120_plus",
    sum(CASE
	WHEN "abundant"."days_to_ship" <= 30 THEN 1
	ELSE 0
	END) as "days_30",
    sum(CASE
	WHEN "abundant"."days_to_ship" > 30 and "abundant"."days_to_ship" <= 60 THEN 1
	ELSE 0
	END) as "days_31_60",
    sum(CASE
	WHEN "abundant"."days_to_ship" > 60 and "abundant"."days_to_ship" <= 90 THEN 1
	ELSE 0
	END) as "days_61_90",
    sum(CASE
	WHEN "abundant"."days_to_ship" > 90 and "abundant"."days_to_ship" <= 120 THEN 1
	ELSE 0
	END) as "days_91_120",
    "uneven"."w_substr" as "w_substr",
    "questionable"."ws_ship_mode_type" as "ws_ship_mode_type",
    "questionable"."ws_web_site_name" as "ws_web_site_name"
FROM
    "questionable"
    LEFT OUTER JOIN "uneven" on "questionable"."ws_warehouse_id" = "uneven"."ws_warehouse_id"
    FULL JOIN "abundant" on 1=1
GROUP BY
    6,
    7,
    8
ORDER BY 
    "uneven"."w_substr" asc nulls first,
    "questionable"."ws_ship_mode_type" asc nulls first,
    "questionable"."ws_web_site_name" asc nulls first
LIMIT (100)