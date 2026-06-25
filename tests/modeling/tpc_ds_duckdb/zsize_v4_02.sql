
WITH 
questionable as (
SELECT
    CASE WHEN "sales_date_date"."D_YEAR" in (2001,2002) THEN "sales_date_date"."D_WEEK_SEQ" ELSE NULL END as "relevent_week_seq"
FROM
    "memory"."date_dim" as "sales_date_date"
GROUP BY
    1),
cheerful as (
SELECT
    "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "sales_date_id",
    "sales_catalog_sales_unified"."CS_EXT_SALES_PRICE" as "sales_ext_sales_price"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
    INNER JOIN "questionable" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "questionable"."sales_date_id"
WHERE
    "questionable"."INVALID_ALIAS: [MODELS_EXECUTE] Concept sales"."date"."week_seq@Grain<sales"."date"."id> not found on sales"."catalog_sales_unified_join_sales"."date"."date_at_sales_channel_sales_item_id_sales_order_id; have ['sales"."channel@Grain<sales"."channel,sales"."item"."id,sales"."order_id>', 'sales"."date"."id@Grain<sales"."channel,sales"."item"."id,sales"."order_id>', 'sales"."ext_sales_price@Grain<sales"."channel,sales"."item"."id,sales"."order_id>', 'sales"."item"."id@Grain<sales"."channel,sales"."item"."id,sales"."order_id>', 'sales"."order_id@Grain<sales"."channel,sales"."item"."id,sales"."order_id>', 'sales"."date"."day_of_week@Grain<sales"."channel,sales"."item"."id,sales"."order_id>', 'sales"."date"."week_seq@Grain<sales"."channel,sales"."item"."id,sales"."order_id>', 'sales"."date"."year@Grain<sales"."channel,sales"."item"."id,sales"."order_id>'] from ['sales"."catalog_sales_unified', 'sales"."date"."date']"."" in (select questionable."relevent_week_seq" from questionable where questionable."relevent_week_seq" is not null)

UNION ALL
SELECT
    "sales_web_sales_unified"."WS_SOLD_DATE_SK" as "sales_date_id",
    "sales_web_sales_unified"."WS_EXT_SALES_PRICE" as "sales_ext_sales_price"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "questionable" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "questionable"."sales_date_id"
WHERE
    "questionable"."INVALID_ALIAS: [MODELS_EXECUTE] Concept sales"."date"."week_seq@Grain<sales"."date"."id> not found on sales"."date"."date_join_sales"."web_sales_unified_at_sales_channel_sales_item_id_sales_order_id; have ['sales"."channel@Grain<sales"."channel,sales"."item"."id,sales"."order_id>', 'sales"."date"."id@Grain<sales"."channel,sales"."item"."id,sales"."order_id>', 'sales"."ext_sales_price@Grain<sales"."channel,sales"."item"."id,sales"."order_id>', 'sales"."item"."id@Grain<sales"."channel,sales"."item"."id,sales"."order_id>', 'sales"."order_id@Grain<sales"."channel,sales"."item"."id,sales"."order_id>', 'sales"."date"."day_of_week@Grain<sales"."channel,sales"."item"."id,sales"."order_id>', 'sales"."date"."week_seq@Grain<sales"."channel,sales"."item"."id,sales"."order_id>', 'sales"."date"."year@Grain<sales"."channel,sales"."item"."id,sales"."order_id>'] from ['sales"."date"."date', 'sales"."web_sales_unified']"."" in (select questionable."relevent_week_seq" from questionable where questionable."relevent_week_seq" is not null)
),
cooperative as (
SELECT
    "cheerful"."sales_ext_sales_price" as "sales_ext_sales_price",
    "sales_date_date"."D_DOW" as "sales_date_day_of_week",
    "sales_date_date"."D_WEEK_SEQ" as "sales_date_week_seq"
FROM
    "cheerful"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "cheerful"."sales_date_id" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_date_date"."D_WEEK_SEQ" in (select questionable."relevent_week_seq" from questionable where questionable."relevent_week_seq" is not null)
),
uneven as (
SELECT
    "cooperative"."sales_date_week_seq" as "sales_date_week_seq",
    sum(CASE WHEN "cooperative"."sales_date_day_of_week" = 0 THEN "cooperative"."sales_ext_sales_price" ELSE NULL END) as "_virt_agg_sum_5898269946212687",
    sum(CASE WHEN "cooperative"."sales_date_day_of_week" = 1 THEN "cooperative"."sales_ext_sales_price" ELSE NULL END) as "_virt_agg_sum_1215995592885356",
    sum(CASE WHEN "cooperative"."sales_date_day_of_week" = 2 THEN "cooperative"."sales_ext_sales_price" ELSE NULL END) as "_virt_agg_sum_5503961012463124",
    sum(CASE WHEN "cooperative"."sales_date_day_of_week" = 3 THEN "cooperative"."sales_ext_sales_price" ELSE NULL END) as "_virt_agg_sum_6232287870778562",
    sum(CASE WHEN "cooperative"."sales_date_day_of_week" = 4 THEN "cooperative"."sales_ext_sales_price" ELSE NULL END) as "_virt_agg_sum_3226984322777641",
    sum(CASE WHEN "cooperative"."sales_date_day_of_week" = 5 THEN "cooperative"."sales_ext_sales_price" ELSE NULL END) as "_virt_agg_sum_1755492547499297",
    sum(CASE WHEN "cooperative"."sales_date_day_of_week" = 6 THEN "cooperative"."sales_ext_sales_price" ELSE NULL END) as "_virt_agg_sum_3160525683686265"
FROM
    "cooperative"
GROUP BY
    1),
concerned as (
SELECT
    "uneven"."_virt_agg_sum_1215995592885356" as "_virt_agg_sum_1215995592885356",
    "uneven"."_virt_agg_sum_1755492547499297" as "_virt_agg_sum_1755492547499297",
    "uneven"."_virt_agg_sum_3160525683686265" as "_virt_agg_sum_3160525683686265",
    "uneven"."_virt_agg_sum_3226984322777641" as "_virt_agg_sum_3226984322777641",
    "uneven"."_virt_agg_sum_5503961012463124" as "_virt_agg_sum_5503961012463124",
    "uneven"."_virt_agg_sum_5898269946212687" as "_virt_agg_sum_5898269946212687",
    "uneven"."_virt_agg_sum_6232287870778562" as "_virt_agg_sum_6232287870778562",
    "uneven"."sales_date_week_seq" as "sales_date_week_seq",
    lead("uneven"."_virt_agg_sum_1215995592885356", 53) over (order by "uneven"."sales_date_week_seq" asc ) as "_virt_window_lead_8434916643189094",
    lead("uneven"."_virt_agg_sum_1755492547499297", 53) over (order by "uneven"."sales_date_week_seq" asc ) as "_virt_window_lead_1513977696668684",
    lead("uneven"."_virt_agg_sum_3160525683686265", 53) over (order by "uneven"."sales_date_week_seq" asc ) as "_virt_window_lead_6726398054446491",
    lead("uneven"."_virt_agg_sum_3226984322777641", 53) over (order by "uneven"."sales_date_week_seq" asc ) as "_virt_window_lead_7589933802981203",
    lead("uneven"."_virt_agg_sum_5503961012463124", 53) over (order by "uneven"."sales_date_week_seq" asc ) as "_virt_window_lead_5402686874923245",
    lead("uneven"."_virt_agg_sum_5898269946212687", 53) over (order by "uneven"."sales_date_week_seq" asc ) as "_virt_window_lead_3355739386573542",
    lead("uneven"."_virt_agg_sum_6232287870778562", 53) over (order by "uneven"."sales_date_week_seq" asc ) as "_virt_window_lead_8846802885933861"
FROM
    "uneven")
SELECT
    "concerned"."sales_date_week_seq" as "sales_date_week_seq",
    round("concerned"."_virt_agg_sum_5898269946212687" / ("concerned"."_virt_window_lead_3355739386573542"),2) as "sunday_increase",
    round("concerned"."_virt_agg_sum_1215995592885356" / ("concerned"."_virt_window_lead_8434916643189094"),2) as "monday_increase",
    round("concerned"."_virt_agg_sum_5503961012463124" / ("concerned"."_virt_window_lead_5402686874923245"),2) as "tuesday_increase",
    round("concerned"."_virt_agg_sum_6232287870778562" / ("concerned"."_virt_window_lead_8846802885933861"),2) as "wednesday_increase",
    round("concerned"."_virt_agg_sum_3226984322777641" / ("concerned"."_virt_window_lead_7589933802981203"),2) as "thursday_increase",
    round("concerned"."_virt_agg_sum_1755492547499297" / ("concerned"."_virt_window_lead_1513977696668684"),2) as "friday_increase",
    round("concerned"."_virt_agg_sum_3160525683686265" / ("concerned"."_virt_window_lead_6726398054446491"),2) as "saturday_increase"
FROM
    "concerned"
WHERE
    round("concerned"."_virt_agg_sum_5898269946212687" / ("concerned"."_virt_window_lead_3355739386573542"),2) is not null

ORDER BY 
    "concerned"."sales_date_week_seq" asc nulls first
LIMIT (100)