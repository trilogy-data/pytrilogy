
WITH 
highfalutin as (
SELECT
    "cs_catalog_sales"."CS_ITEM_SK" as "cs_item_id",
    "cs_catalog_sales"."CS_SOLD_DATE_SK" as "cs_sold_date_id"
FROM
    "memory"."catalog_sales" as "cs_catalog_sales"
GROUP BY
    1,
    2),
cooperative as (
SELECT
    "cs_item_items"."I_CATEGORY" as "cs_item_category",
    "cs_item_items"."I_CLASS" as "cs_item_class",
    "cs_item_items"."I_CURRENT_PRICE" as "cs_item_current_price",
    "cs_item_items"."I_ITEM_DESC" as "cs_item_desc",
    "cs_item_items"."I_ITEM_ID" as "cs_item_text_id",
    sum("cs_catalog_sales"."CS_EXT_SALES_PRICE") as "revenue"
FROM
    "memory"."catalog_sales" as "cs_catalog_sales"
    INNER JOIN "memory"."item" as "cs_item_items" on "cs_catalog_sales"."CS_ITEM_SK" = "cs_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."date_dim" as "cs_sold_date_date" on "cs_catalog_sales"."CS_SOLD_DATE_SK" = "cs_sold_date_date"."D_DATE_SK"
WHERE
    "cs_item_items"."I_CATEGORY" in ('Sports','Books','Home') and cast("cs_sold_date_date"."D_DATE" as date) BETWEEN date '1999-02-22' AND date '1999-03-24'

GROUP BY
    1,
    2,
    3,
    4,
    5),
thoughtful as (
SELECT
    "cs_item_items"."I_CATEGORY" as "cs_item_category",
    "cs_item_items"."I_CLASS" as "cs_item_class",
    "cs_item_items"."I_CURRENT_PRICE" as "cs_item_current_price",
    "cs_item_items"."I_ITEM_DESC" as "cs_item_desc",
    "cs_item_items"."I_ITEM_ID" as "cs_item_text_id"
FROM
    "highfalutin"
    INNER JOIN "memory"."item" as "cs_item_items" on "highfalutin"."cs_item_id" = "cs_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."date_dim" as "cs_sold_date_date" on "highfalutin"."cs_sold_date_id" = "cs_sold_date_date"."D_DATE_SK"
WHERE
    "cs_item_items"."I_CATEGORY" in ('Sports','Books','Home') and cast("cs_sold_date_date"."D_DATE" as date) BETWEEN date '1999-02-22' AND date '1999-03-24'
),
abundant as (
SELECT
    "cooperative"."revenue" as "revenue",
    coalesce("cooperative"."cs_item_class","thoughtful"."cs_item_class") as "cs_item_class"
FROM
    "cooperative"
    FULL JOIN "thoughtful" on "cooperative"."cs_item_category" is not distinct from "thoughtful"."cs_item_category" AND "cooperative"."cs_item_class" is not distinct from "thoughtful"."cs_item_class" AND "cooperative"."cs_item_current_price" is not distinct from "thoughtful"."cs_item_current_price" AND "cooperative"."cs_item_desc" is not distinct from "thoughtful"."cs_item_desc" AND "cooperative"."cs_item_text_id" = "thoughtful"."cs_item_text_id"
GROUP BY
    1,
    2,
    coalesce("cooperative"."cs_item_category","thoughtful"."cs_item_category"),
    coalesce("cooperative"."cs_item_current_price","thoughtful"."cs_item_current_price"),
    coalesce("cooperative"."cs_item_desc","thoughtful"."cs_item_desc"),
    coalesce("cooperative"."cs_item_text_id","thoughtful"."cs_item_text_id")),
yummy as (
SELECT
    "abundant"."cs_item_class" as "cs_item_class",
    sum("abundant"."revenue") as "_virt_agg_sum_9832457364876792"
FROM
    "abundant"
GROUP BY
    1),
juicy as (
SELECT
    "cooperative"."cs_item_category" as "cs_item_category",
    "cooperative"."cs_item_class" as "cs_item_class",
    "cooperative"."cs_item_current_price" as "cs_item_current_price",
    "cooperative"."cs_item_desc" as "cs_item_desc",
    "cooperative"."cs_item_text_id" as "cs_item_text_id",
    "cooperative"."revenue" as "revenue",
    "yummy"."_virt_agg_sum_9832457364876792" as "_virt_agg_sum_9832457364876792"
FROM
    "cooperative"
    INNER JOIN "yummy" on "cooperative"."cs_item_class" is not distinct from "yummy"."cs_item_class")
SELECT
    "juicy"."cs_item_text_id" as "cs_item_text_id",
    "juicy"."cs_item_desc" as "cs_item_desc",
    "juicy"."cs_item_category" as "cs_item_category",
    "juicy"."cs_item_class" as "cs_item_class",
    "juicy"."cs_item_current_price" as "cs_item_current_price",
    "juicy"."revenue" as "revenue",
    ( "juicy"."revenue" * 100.0 ) / ("juicy"."_virt_agg_sum_9832457364876792") as "revenue_ratio"
FROM
    "juicy"
ORDER BY 
    "juicy"."cs_item_category" asc nulls first,
    "juicy"."cs_item_class" asc nulls first,
    "juicy"."cs_item_text_id" asc nulls first,
    "juicy"."cs_item_desc" asc nulls first,
    "revenue_ratio" asc nulls first
LIMIT (100)