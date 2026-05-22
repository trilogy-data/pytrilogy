
WITH 
questionable as (
SELECT
    "sales_catalog_returns_unified"."CR_ITEM_SK" as "sales_item_id",
    "sales_catalog_returns_unified"."CR_ORDER_NUMBER" as "sales_order_id",
     'CATALOG'  as "sales_sales_channel",
    "sales_catalog_returns_unified"."CR_RETURN_QUANTITY" as "sales_return_quantity",
    "sales_catalog_returns_unified"."CR_RETURNED_DATE_SK" as "sales_return_date_id"
FROM
    "memory"."catalog_returns" as "sales_catalog_returns_unified"
UNION ALL
SELECT
    "sales_store_returns_unified"."SR_ITEM_SK" as "sales_item_id",
    "sales_store_returns_unified"."SR_TICKET_NUMBER" as "sales_order_id",
     'STORE'  as "sales_sales_channel",
    "sales_store_returns_unified"."SR_RETURN_QUANTITY" as "sales_return_quantity",
    "sales_store_returns_unified"."SR_RETURNED_DATE_SK" as "sales_return_date_id"
FROM
    "memory"."store_returns" as "sales_store_returns_unified"
UNION ALL
SELECT
    "sales_web_returns_unified"."WR_ITEM_SK" as "sales_item_id",
    "sales_web_returns_unified"."WR_ORDER_NUMBER" as "sales_order_id",
     'WEB'  as "sales_sales_channel",
    "sales_web_returns_unified"."WR_RETURN_QUANTITY" as "sales_return_quantity",
    "sales_web_returns_unified"."WR_RETURNED_DATE_SK" as "sales_return_date_id"
FROM
    "memory"."web_returns" as "sales_web_returns_unified"),
quizzical as (
SELECT
    CASE WHEN cast("date_date"."D_DATE" as date) in (date '2000-06-30',date '2000-09-27',date '2000-11-17') THEN "date_date"."D_WEEK_SEQ" ELSE NULL END as "target_week_seqs"
FROM
    "memory"."date_dim" as "date_date"
GROUP BY
    1),
scrawny as (
SELECT
    "questionable"."sales_item_id" as "sales_item_id",
    "questionable"."sales_return_date_id" as "sales_return_date_id"
FROM
    "questionable"
GROUP BY
    1,
    2),
protective as (
SELECT
    "sales_item_items"."I_ITEM_ID" as "sales_item_name",
    CASE WHEN "questionable"."sales_sales_channel" = 'CATALOG' THEN "questionable"."sales_order_id" ELSE NULL END as "_virt_filter_order_id_7518965045904948",
    CASE WHEN "questionable"."sales_sales_channel" = 'STORE' THEN "questionable"."sales_order_id" ELSE NULL END as "_virt_filter_order_id_5282889778133979",
    CASE WHEN "questionable"."sales_sales_channel" = 'WEB' THEN "questionable"."sales_order_id" ELSE NULL END as "_virt_filter_order_id_4128599423878258"
FROM
    "memory"."item" as "sales_item_items"
    LEFT OUTER JOIN "questionable" on "sales_item_items"."I_ITEM_SK" = "questionable"."sales_item_id"
GROUP BY
    1,
    2,
    3,
    4,
    "questionable"."sales_order_id"),
kaput as (
SELECT
    "sales_item_items"."I_ITEM_ID" as "sales_item_name",
    sum(CASE WHEN "questionable"."sales_sales_channel" = 'STORE' THEN "questionable"."sales_return_quantity" ELSE NULL END) as "sr_item_qty",
    sum(CASE WHEN "questionable"."sales_sales_channel" = 'WEB' THEN "questionable"."sales_return_quantity" ELSE NULL END) as "wr_item_qty"
FROM
    "memory"."item" as "sales_item_items"
    LEFT OUTER JOIN "questionable" on "sales_item_items"."I_ITEM_SK" = "questionable"."sales_item_id"
GROUP BY
    1),
abhorrent as (
SELECT
    "sales_item_items"."I_ITEM_ID" as "sales_item_name"
FROM
    "memory"."item" as "sales_item_items"
    INNER JOIN "questionable" on "sales_item_items"."I_ITEM_SK" = "questionable"."sales_item_id"
    INNER JOIN "memory"."date_dim" as "sales_return_date_date" on "questionable"."sales_return_date_id" = "sales_return_date_date"."D_DATE_SK"
WHERE
    "sales_return_date_date"."D_WEEK_SEQ" in (select quizzical."target_week_seqs" from quizzical where quizzical."target_week_seqs" is not null)

GROUP BY
    1,
    "questionable"."sales_order_id",
    CASE WHEN "questionable"."sales_sales_channel" = 'CATALOG' THEN "questionable"."sales_order_id" ELSE NULL END,
    CASE WHEN "questionable"."sales_sales_channel" = 'STORE' THEN "questionable"."sales_order_id" ELSE NULL END,
    CASE WHEN "questionable"."sales_sales_channel" = 'WEB' THEN "questionable"."sales_order_id" ELSE NULL END),
vacuous as (
SELECT
    "questionable"."sales_order_id" as "sales_order_id",
    "questionable"."sales_return_quantity" as "sales_return_quantity",
    "questionable"."sales_sales_channel" as "sales_sales_channel",
    "sales_item_items"."I_ITEM_SK" as "sales_item_id"
FROM
    "memory"."item" as "sales_item_items"
    INNER JOIN "questionable" on "sales_item_items"."I_ITEM_SK" = "questionable"."sales_item_id"
    INNER JOIN "memory"."date_dim" as "sales_return_date_date" on "questionable"."sales_return_date_id" = "sales_return_date_date"."D_DATE_SK"
WHERE
    "sales_return_date_date"."D_WEEK_SEQ" in (select quizzical."target_week_seqs" from quizzical where quizzical."target_week_seqs" is not null)

GROUP BY
    1,
    2,
    3,
    4,
    "sales_item_items"."I_ITEM_ID"),
yummy as (
SELECT
    "questionable"."sales_order_id" as "sales_order_id",
    "questionable"."sales_return_quantity" as "sales_return_quantity",
    "questionable"."sales_sales_channel" as "sales_sales_channel",
    "sales_item_items"."I_ITEM_ID" as "sales_item_name",
    "sales_item_items"."I_ITEM_SK" as "sales_item_id",
    "sales_return_date_date"."D_WEEK_SEQ" as "sales_return_date_week_seq"
FROM
    "memory"."item" as "sales_item_items"
    LEFT OUTER JOIN "questionable" on "sales_item_items"."I_ITEM_SK" = "questionable"."sales_item_id"
    LEFT OUTER JOIN "memory"."date_dim" as "sales_return_date_date" on "questionable"."sales_return_date_id" = "sales_return_date_date"."D_DATE_SK"
    FULL JOIN "quizzical" on 1=1
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6),
friendly as (
SELECT
    "sales_item_items"."I_ITEM_ID" as "sales_item_name",
    "sales_return_date_date"."D_WEEK_SEQ" as "sales_return_date_week_seq"
FROM
    "memory"."item" as "sales_item_items"
    INNER JOIN "scrawny" on "sales_item_items"."I_ITEM_SK" = "scrawny"."sales_item_id"
    INNER JOIN "memory"."date_dim" as "sales_return_date_date" on "scrawny"."sales_return_date_id" = "sales_return_date_date"."D_DATE_SK"
WHERE
    "sales_return_date_date"."D_WEEK_SEQ" in (select quizzical."target_week_seqs" from quizzical where quizzical."target_week_seqs" is not null)

GROUP BY
    1,
    2),
waggish as (
SELECT
    "protective"."sales_item_name" as "sales_item_name",
    count("protective"."_virt_filter_order_id_4128599423878258") as "wr_item_present",
    count("protective"."_virt_filter_order_id_5282889778133979") as "sr_item_present",
    count("protective"."_virt_filter_order_id_7518965045904948") as "cr_item_present"
FROM
    "protective"
GROUP BY
    1
HAVING
    "sr_item_present" > 0
),
macho as (
SELECT
    "abhorrent"."sales_item_name" as "sales_item_name"
FROM
    "abhorrent"
GROUP BY
    1),
concerned as (
SELECT
    "vacuous"."sales_item_id" as "sales_item_id",
    "vacuous"."sales_order_id" as "sales_order_id",
    "vacuous"."sales_sales_channel" as "sales_sales_channel",
    CASE WHEN "vacuous"."sales_sales_channel" = 'CATALOG' THEN "vacuous"."sales_return_quantity" ELSE NULL END as "_virt_filter_return_quantity_1904161637839137"
FROM
    "vacuous"),
juicy as (
SELECT
    "yummy"."sales_item_id" as "sales_item_id",
    "yummy"."sales_item_name" as "sales_item_name",
    "yummy"."sales_order_id" as "sales_order_id",
    "yummy"."sales_return_date_week_seq" as "sales_return_date_week_seq",
    "yummy"."sales_sales_channel" as "sales_sales_channel",
    CASE WHEN "yummy"."sales_sales_channel" = 'STORE' THEN "yummy"."sales_return_quantity" ELSE NULL END as "_virt_filter_return_quantity_6293408465554798",
    CASE WHEN "yummy"."sales_sales_channel" = 'WEB' THEN "yummy"."sales_return_quantity" ELSE NULL END as "_virt_filter_return_quantity_6234128225083739"
FROM
    "yummy"),
charming as (
SELECT
    "friendly"."sales_item_name" as "sales_item_name",
    "friendly"."sales_return_date_week_seq" as "sales_return_date_week_seq",
    "kaput"."sr_item_qty" as "sr_item_qty",
    "kaput"."wr_item_qty" as "wr_item_qty"
FROM
    "friendly"
    INNER JOIN "kaput" on "friendly"."sales_item_name" = "kaput"."sales_item_name"
WHERE
    "friendly"."sales_return_date_week_seq" in (select quizzical."target_week_seqs" from quizzical where quizzical."target_week_seqs" is not null)
),
young as (
SELECT
    "concerned"."_virt_filter_return_quantity_1904161637839137" as "_virt_filter_return_quantity_1904161637839137",
    "juicy"."_virt_filter_return_quantity_6234128225083739" as "_virt_filter_return_quantity_6234128225083739",
    "juicy"."_virt_filter_return_quantity_6293408465554798" as "_virt_filter_return_quantity_6293408465554798",
    "juicy"."sales_item_name" as "sales_item_name"
FROM
    "juicy"
    LEFT OUTER JOIN "concerned" on "juicy"."sales_item_id" = "concerned"."sales_item_id" AND "juicy"."sales_order_id" = "concerned"."sales_order_id" AND "juicy"."sales_sales_channel" = "concerned"."sales_sales_channel"
WHERE
    "juicy"."sales_return_date_week_seq" in (select quizzical."target_week_seqs" from quizzical where quizzical."target_week_seqs" is not null)

GROUP BY
    1,
    2,
    3,
    4,
    "juicy"."sales_item_id",
    "juicy"."sales_order_id",
    "juicy"."sales_sales_channel"),
rambunctious as (
SELECT
    "charming"."sales_item_name" as "sales_item_name",
    "charming"."sr_item_qty" as "sr_item_qty",
    "charming"."wr_item_qty" as "wr_item_qty",
    "waggish"."cr_item_present" as "cr_item_present",
    "waggish"."sr_item_present" as "sr_item_present",
    "waggish"."wr_item_present" as "wr_item_present"
FROM
    "charming"
    INNER JOIN "waggish" on "charming"."sales_item_name" = "waggish"."sales_item_name"
WHERE
    "charming"."sales_return_date_week_seq" in (select quizzical."target_week_seqs" from quizzical where quizzical."target_week_seqs" is not null)
),
sparkling as (
SELECT
    "young"."sales_item_name" as "sales_item_name",
    sum("young"."_virt_filter_return_quantity_1904161637839137") as "cr_item_qty",
    sum("young"."_virt_filter_return_quantity_6234128225083739") as "wr_item_qty",
    sum("young"."_virt_filter_return_quantity_6293408465554798") as "sr_item_qty"
FROM
    "young"
GROUP BY
    1)
SELECT
    "sparkling"."sales_item_name" as "item_id",
    "sparkling"."sr_item_qty" as "sr_item_qty",
    ( ( ("sparkling"."sr_item_qty" * 1.0) / ( ( "sparkling"."sr_item_qty" + "sparkling"."cr_item_qty" ) + "sparkling"."wr_item_qty" ) ) / 3.0 ) * 100 as "sr_dev",
    "sparkling"."cr_item_qty" as "cr_item_qty",
    ( ( ("sparkling"."cr_item_qty" * 1.0) / ( ( "sparkling"."sr_item_qty" + "sparkling"."cr_item_qty" ) + "sparkling"."wr_item_qty" ) ) / 3.0 ) * 100 as "cr_dev",
    "sparkling"."wr_item_qty" as "wr_item_qty",
    ( ( ("sparkling"."wr_item_qty" * 1.0) / ( ( "sparkling"."sr_item_qty" + "sparkling"."cr_item_qty" ) + "sparkling"."wr_item_qty" ) ) / 3.0 ) * 100 as "wr_dev",
    ( ( "sparkling"."sr_item_qty" + "sparkling"."cr_item_qty" ) + "sparkling"."wr_item_qty" ) / 3.0 as "average"
FROM
    "sparkling"
    RIGHT OUTER JOIN "rambunctious" on "sparkling"."sales_item_name" = "rambunctious"."sales_item_name" AND "sparkling"."sr_item_qty" is not distinct from "rambunctious"."sr_item_qty" AND "sparkling"."wr_item_qty" is not distinct from "rambunctious"."wr_item_qty"
    INNER JOIN "macho" on "sparkling"."sales_item_name" = "macho"."sales_item_name"
WHERE
    "rambunctious"."sr_item_present" > 0 and "rambunctious"."cr_item_present" > 0 and "rambunctious"."wr_item_present" > 0

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    "rambunctious"."cr_item_present",
    "rambunctious"."sr_item_present",
    "rambunctious"."wr_item_present"
ORDER BY 
    "item_id" asc nulls first,
    "sparkling"."sr_item_qty" asc nulls first
LIMIT (100)