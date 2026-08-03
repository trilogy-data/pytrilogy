
WITH 
quizzical as (
SELECT
    "date_date"."D_WEEK_SEQ" as "target_week_seq"
FROM
    "memory"."date_dim" as "date_date"
WHERE
    "date_date"."D_DATE" = '2000-01-03'

GROUP BY
    1),
questionable as (
SELECT
     'CATALOG'  as "sales_channel",
    "sales_catalog_sales_unified"."CS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_catalog_sales_unified"."CS_ITEM_SK" as "sales_item_sk",
    "sales_sale_date_date"."D_WEEK_SEQ" as "sales_sale_date_week_seq"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_sale_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_sale_date_date"."D_DATE_SK"
WHERE
    exists (select 1 from quizzical where quizzical."target_week_seq" is not distinct from "sales_sale_date_date"."D_WEEK_SEQ")

UNION ALL
SELECT
     'STORE'  as "sales_channel",
    "sales_store_sales_unified"."SS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_sk",
    "sales_sale_date_date"."D_WEEK_SEQ" as "sales_sale_date_week_seq"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_sale_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_sale_date_date"."D_DATE_SK"
WHERE
    exists (select 1 from quizzical where quizzical."target_week_seq" is not distinct from "sales_sale_date_date"."D_WEEK_SEQ")

UNION ALL
SELECT
     'WEB'  as "sales_channel",
    "sales_web_sales_unified"."WS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_sk",
    "sales_sale_date_date"."D_WEEK_SEQ" as "sales_sale_date_week_seq"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_sale_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_sale_date_date"."D_DATE_SK"
WHERE
    exists (select 1 from quizzical where quizzical."target_week_seq" is not distinct from "sales_sale_date_date"."D_WEEK_SEQ")
),
yummy as (
SELECT
    "sales_item_items"."I_ITEM_ID" as "item_id",
    "sales_item_items"."I_ITEM_ID" as "sales_item_id",
    CASE WHEN "questionable"."sales_channel" = 'CATALOG' THEN "questionable"."sales_ext_sales_price" ELSE NULL END as "_virt_filter_ext_sales_price_2613615356807588",
    CASE WHEN "questionable"."sales_channel" = 'STORE' THEN "questionable"."sales_ext_sales_price" ELSE NULL END as "_virt_filter_ext_sales_price_5511141461727120",
    CASE WHEN "questionable"."sales_channel" = 'WEB' THEN "questionable"."sales_ext_sales_price" ELSE NULL END as "_virt_filter_ext_sales_price_2201881228813271"
FROM
    "questionable"
    INNER JOIN "memory"."item" as "sales_item_items" on "questionable"."sales_item_sk" = "sales_item_items"."I_ITEM_SK"
WHERE
    exists (select 1 from quizzical where quizzical."target_week_seq" is not distinct from "questionable"."sales_sale_date_week_seq")
),
young as (
SELECT
    "yummy"."sales_item_id" as "sales_item_id",
    ( sum("yummy"."_virt_filter_ext_sales_price_2201881228813271") / ( (( sum("yummy"."_virt_filter_ext_sales_price_5511141461727120") + sum("yummy"."_virt_filter_ext_sales_price_2613615356807588") ) + sum("yummy"."_virt_filter_ext_sales_price_2201881228813271")) / 3 ) ) * 100 as "ws_dev",
    ( sum("yummy"."_virt_filter_ext_sales_price_2613615356807588") / ( (( sum("yummy"."_virt_filter_ext_sales_price_5511141461727120") + sum("yummy"."_virt_filter_ext_sales_price_2613615356807588") ) + sum("yummy"."_virt_filter_ext_sales_price_2201881228813271")) / 3 ) ) * 100 as "cs_dev",
    ( sum("yummy"."_virt_filter_ext_sales_price_5511141461727120") / ( (( sum("yummy"."_virt_filter_ext_sales_price_5511141461727120") + sum("yummy"."_virt_filter_ext_sales_price_2613615356807588") ) + sum("yummy"."_virt_filter_ext_sales_price_2201881228813271")) / 3 ) ) * 100 as "ss_dev",
    (( sum("yummy"."_virt_filter_ext_sales_price_5511141461727120") + sum("yummy"."_virt_filter_ext_sales_price_2613615356807588") ) + sum("yummy"."_virt_filter_ext_sales_price_2201881228813271")) / 3 as "avg_rev",
    sum("yummy"."_virt_filter_ext_sales_price_2201881228813271") as "ws_item_rev",
    sum("yummy"."_virt_filter_ext_sales_price_2613615356807588") as "cs_item_rev",
    sum("yummy"."_virt_filter_ext_sales_price_5511141461727120") as "ss_item_rev"
FROM
    "yummy"
GROUP BY
    1
HAVING
    "ss_item_rev" BETWEEN 0.9 * "cs_item_rev" AND 1.1 * "cs_item_rev" and "ss_item_rev" BETWEEN 0.9 * "ws_item_rev" AND 1.1 * "ws_item_rev" and "cs_item_rev" BETWEEN 0.9 * "ss_item_rev" AND 1.1 * "ss_item_rev" and "cs_item_rev" BETWEEN 0.9 * "ws_item_rev" AND 1.1 * "ws_item_rev" and "ws_item_rev" BETWEEN 0.9 * "ss_item_rev" AND 1.1 * "ss_item_rev" and "ws_item_rev" BETWEEN 0.9 * "cs_item_rev" AND 1.1 * "cs_item_rev"
)
SELECT
    "yummy"."item_id" as "item_id",
    "young"."ss_item_rev" as "ss_item_rev",
    "young"."ss_dev" as "ss_dev",
    "young"."cs_item_rev" as "cs_item_rev",
    "young"."cs_dev" as "cs_dev",
    "young"."ws_item_rev" as "ws_item_rev",
    "young"."ws_dev" as "ws_dev",
    "young"."avg_rev" as "avg_rev"
FROM
    "young"
    INNER JOIN "yummy" on "young"."sales_item_id" = "yummy"."item_id"
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8
ORDER BY 
    "yummy"."item_id" asc nulls first,
    "young"."ss_item_rev" asc nulls first
LIMIT (100)