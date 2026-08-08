
WITH 
juicy as (
SELECT
     'CATALOG'  as "sales_channel",
    "sales_catalog_sales_unified"."CS_ITEM_SK" as "sales_item_sk",
    "sales_catalog_sales_unified"."CS_ORDER_NUMBER" as "sales_order_id"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
UNION ALL
SELECT
     'STORE'  as "sales_channel",
    "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_sk",
    "sales_store_sales_unified"."SS_TICKET_NUMBER" as "sales_order_id"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
UNION ALL
SELECT
     'WEB'  as "sales_channel",
    "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_sk",
    "sales_web_sales_unified"."WS_ORDER_NUMBER" as "sales_order_id"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"),
questionable as (
SELECT
     'CATALOG'  as "sales_channel",
    "sales_catalog_returns_unified"."CR_ITEM_SK" as "sales_item_sk",
    "sales_catalog_returns_unified"."CR_ORDER_NUMBER" as "sales_order_id",
    "sales_catalog_returns_unified"."CR_RETURNED_DATE_SK" as "sales_return_date_sk",
    "sales_catalog_returns_unified"."CR_RETURN_QUANTITY" as "sales_return_quantity"
FROM
    "memory"."catalog_returns" as "sales_catalog_returns_unified"
UNION ALL
SELECT
     'STORE'  as "sales_channel",
    "sales_store_returns_unified"."SR_ITEM_SK" as "sales_item_sk",
    "sales_store_returns_unified"."SR_TICKET_NUMBER" as "sales_order_id",
    "sales_store_returns_unified"."SR_RETURNED_DATE_SK" as "sales_return_date_sk",
    "sales_store_returns_unified"."SR_RETURN_QUANTITY" as "sales_return_quantity"
FROM
    "memory"."store_returns" as "sales_store_returns_unified"
UNION ALL
SELECT
     'WEB'  as "sales_channel",
    "sales_web_returns_unified"."WR_ITEM_SK" as "sales_item_sk",
    "sales_web_returns_unified"."WR_ORDER_NUMBER" as "sales_order_id",
    "sales_web_returns_unified"."WR_RETURNED_DATE_SK" as "sales_return_date_sk",
    "sales_web_returns_unified"."WR_RETURN_QUANTITY" as "sales_return_quantity"
FROM
    "memory"."web_returns" as "sales_web_returns_unified"),
quizzical as (
SELECT
    "date_date"."D_WEEK_SEQ" as "target_week_seqs"
FROM
    "memory"."date_dim" as "date_date"
WHERE
    (date '2000-06-30' is not distinct from cast("date_date"."D_DATE" as date) or date '2000-09-27' is not distinct from cast("date_date"."D_DATE" as date) or date '2000-11-17' is not distinct from cast("date_date"."D_DATE" as date))

GROUP BY
    1),
young as (
SELECT
    "sales_item_items"."I_ITEM_ID" as "item_id",
    "sales_item_items"."I_ITEM_ID" as "sales_item_id",
    CASE WHEN coalesce("juicy"."sales_channel","questionable"."sales_channel") = 'CATALOG' THEN "questionable"."sales_return_quantity" ELSE NULL END as "_virt_filter_return_quantity_5779532101154568",
    CASE WHEN coalesce("juicy"."sales_channel","questionable"."sales_channel") = 'CATALOG' THEN coalesce("juicy"."sales_order_id","questionable"."sales_order_id") ELSE NULL END as "_virt_filter_order_id_9747092284480774",
    CASE WHEN coalesce("juicy"."sales_channel","questionable"."sales_channel") = 'STORE' THEN "questionable"."sales_return_quantity" ELSE NULL END as "_virt_filter_return_quantity_1772282805685931",
    CASE WHEN coalesce("juicy"."sales_channel","questionable"."sales_channel") = 'STORE' THEN coalesce("juicy"."sales_order_id","questionable"."sales_order_id") ELSE NULL END as "_virt_filter_order_id_5221868426357137",
    CASE WHEN coalesce("juicy"."sales_channel","questionable"."sales_channel") = 'WEB' THEN "questionable"."sales_return_quantity" ELSE NULL END as "_virt_filter_return_quantity_3080706904930962",
    CASE WHEN coalesce("juicy"."sales_channel","questionable"."sales_channel") = 'WEB' THEN coalesce("juicy"."sales_order_id","questionable"."sales_order_id") ELSE NULL END as "_virt_filter_order_id_4509973400283298"
FROM
    "juicy"
    FULL JOIN "questionable" on "juicy"."sales_channel" = "questionable"."sales_channel" AND "juicy"."sales_item_sk" = "questionable"."sales_item_sk" AND "juicy"."sales_order_id" = "questionable"."sales_order_id"
    RIGHT OUTER JOIN "memory"."date_dim" as "sales_return_date_date" on "questionable"."sales_return_date_sk" = "sales_return_date_date"."D_DATE_SK"
    LEFT OUTER JOIN "memory"."item" as "sales_item_items" on "juicy"."sales_item_sk" = "sales_item_items"."I_ITEM_SK"
WHERE
    exists (select 1 from quizzical where quizzical."target_week_seqs" is not distinct from "sales_return_date_date"."D_WEEK_SEQ")
),
late as (
SELECT
    "young"."sales_item_id" as "sales_item_id",
    ( ( (sum("young"."_virt_filter_return_quantity_1772282805685931") * 1.0) / ( ( sum("young"."_virt_filter_return_quantity_1772282805685931") + sum("young"."_virt_filter_return_quantity_5779532101154568") ) + sum("young"."_virt_filter_return_quantity_3080706904930962") ) ) / 3.0 ) * 100 as "sr_dev",
    ( ( (sum("young"."_virt_filter_return_quantity_3080706904930962") * 1.0) / ( ( sum("young"."_virt_filter_return_quantity_1772282805685931") + sum("young"."_virt_filter_return_quantity_5779532101154568") ) + sum("young"."_virt_filter_return_quantity_3080706904930962") ) ) / 3.0 ) * 100 as "wr_dev",
    ( ( (sum("young"."_virt_filter_return_quantity_5779532101154568") * 1.0) / ( ( sum("young"."_virt_filter_return_quantity_1772282805685931") + sum("young"."_virt_filter_return_quantity_5779532101154568") ) + sum("young"."_virt_filter_return_quantity_3080706904930962") ) ) / 3.0 ) * 100 as "cr_dev",
    ( ( sum("young"."_virt_filter_return_quantity_1772282805685931") + sum("young"."_virt_filter_return_quantity_5779532101154568") ) + sum("young"."_virt_filter_return_quantity_3080706904930962") ) / 3.0 as "average",
    count(distinct "young"."_virt_filter_order_id_4509973400283298") as "wr_item_present",
    count(distinct "young"."_virt_filter_order_id_5221868426357137") as "sr_item_present",
    count(distinct "young"."_virt_filter_order_id_9747092284480774") as "cr_item_present",
    sum("young"."_virt_filter_return_quantity_1772282805685931") as "sr_item_qty",
    sum("young"."_virt_filter_return_quantity_3080706904930962") as "wr_item_qty",
    sum("young"."_virt_filter_return_quantity_5779532101154568") as "cr_item_qty"
FROM
    "young"
GROUP BY
    1
HAVING
    "sr_item_present" > 0 and "cr_item_present" > 0 and "wr_item_present" > 0
)
SELECT
    "young"."item_id" as "item_id",
    "late"."sr_item_qty" as "sr_item_qty",
    "late"."sr_dev" as "sr_dev",
    "late"."cr_item_qty" as "cr_item_qty",
    "late"."cr_dev" as "cr_dev",
    "late"."wr_item_qty" as "wr_item_qty",
    "late"."wr_dev" as "wr_dev",
    "late"."average" as "average"
FROM
    "young"
    INNER JOIN "late" on "young"."item_id" = "late"."sales_item_id"
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    "late"."cr_item_present",
    "late"."sr_item_present",
    "late"."wr_item_present"
ORDER BY 
    "young"."item_id" asc nulls first,
    "late"."sr_item_qty" asc nulls first
LIMIT (100)