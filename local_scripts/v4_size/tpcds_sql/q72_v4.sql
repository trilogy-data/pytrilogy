
WITH 
yummy as (
SELECT
    "cs_catalog_sales"."CS_ORDER_NUMBER" as "cs_order_number",
    "cs_catalog_sales"."CS_PROMO_SK" as "cs_promotion_sk",
    "cs_item_items"."I_ITEM_DESC" as "cs_item_desc",
    "cs_sale_date_date"."D_WEEK_SEQ" as "cs_sale_date_week_seq",
    "inv_date_date"."D_DATE_SK" as "inv_date_sk",
    "inv_warehouse_inventory"."inv_item_sk" as "inv_item_sk",
    "inv_warehouse_inventory"."inv_warehouse_sk" as "inv_warehouse_sk",
    "inv_warehouse_warehouse"."w_warehouse_name" as "inv_warehouse_name",
    cast("cs_sale_date_date"."D_DATE" as date) as "cs_sale_date_date",
    cast("cs_ship_date_date"."D_DATE" as date) as "cs_ship_date_date"
FROM
    "memory"."inventory" as "inv_warehouse_inventory"
    INNER JOIN "memory"."catalog_sales" as "cs_catalog_sales" on "inv_warehouse_inventory"."inv_item_sk" = "cs_catalog_sales"."CS_ITEM_SK"
    INNER JOIN "memory"."date_dim" as "inv_date_date" on "inv_warehouse_inventory"."inv_date_sk" = "inv_date_date"."D_DATE_SK"
    INNER JOIN "memory"."date_dim" as "cs_sale_date_date" on "cs_catalog_sales"."CS_SOLD_DATE_SK" = "cs_sale_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer_demographics" as "cs_pos_customer_demographic_customer_demographics" on "cs_catalog_sales"."CS_BILL_CDEMO_SK" = "cs_pos_customer_demographic_customer_demographics"."CD_DEMO_SK"
    INNER JOIN "memory"."household_demographics" as "cs_pos_household_demographic_household_demographics" on "cs_catalog_sales"."CS_BILL_HDEMO_SK" = "cs_pos_household_demographic_household_demographics"."HD_DEMO_SK"
    LEFT OUTER JOIN "memory"."item" as "cs_item_items" on "inv_warehouse_inventory"."inv_item_sk" = "cs_item_items"."I_ITEM_SK"
    LEFT OUTER JOIN "memory"."date_dim" as "cs_ship_date_date" on "cs_catalog_sales"."CS_SHIP_DATE_SK" = "cs_ship_date_date"."D_DATE_SK"
    LEFT OUTER JOIN "memory"."warehouse" as "inv_warehouse_warehouse" on "inv_warehouse_inventory"."inv_warehouse_sk" = "inv_warehouse_warehouse"."w_warehouse_sk"
WHERE
    "cs_sale_date_date"."D_YEAR" = 1999 and "cs_pos_household_demographic_household_demographics"."HD_BUY_POTENTIAL" = '>10000' and "cs_pos_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" = 'D' and "inv_warehouse_inventory"."inv_quantity_on_hand" < "cs_catalog_sales"."CS_QUANTITY" and "inv_date_date"."D_WEEK_SEQ" = "cs_sale_date_date"."D_WEEK_SEQ"
),
young as (
SELECT
    "yummy"."cs_sale_date_week_seq" as "cs_sale_date_week_seq",
    date_diff('day', "yummy"."cs_sale_date_date", "yummy"."cs_ship_date_date") as "cs_days_to_ship"
FROM
    "yummy"),
juicy as (
SELECT
    "yummy"."cs_item_desc" as "item_desc",
    "yummy"."cs_sale_date_week_seq" as "week_seq",
    "yummy"."inv_warehouse_name" as "warehouse_name",
    count(CASE WHEN "yummy"."cs_promotion_sk" is not null THEN md5(CONCAT_WS('', coalesce(cast("yummy"."cs_order_number" as string),''), coalesce(cast("yummy"."inv_item_sk" as string),''), coalesce(cast("yummy"."inv_date_sk" as string),''), coalesce(cast("yummy"."inv_warehouse_sk" as string),''))) ELSE NULL END) as "promo",
    count(CASE WHEN "yummy"."cs_promotion_sk" is null THEN md5(CONCAT_WS('', coalesce(cast("yummy"."cs_order_number" as string),''), coalesce(cast("yummy"."inv_item_sk" as string),''), coalesce(cast("yummy"."inv_date_sk" as string),''), coalesce(cast("yummy"."inv_warehouse_sk" as string),''))) ELSE NULL END) as "no_promo",
    count(md5(CONCAT_WS('', coalesce(cast("yummy"."cs_order_number" as string),''), coalesce(cast("yummy"."inv_item_sk" as string),''), coalesce(cast("yummy"."inv_date_sk" as string),''), coalesce(cast("yummy"."inv_warehouse_sk" as string),'')))) as "total_cnt"
FROM
    "yummy"
GROUP BY
    1,
    2,
    3)
SELECT
    "juicy"."item_desc" as "item_desc",
    "juicy"."warehouse_name" as "warehouse_name",
    "juicy"."week_seq" as "week_seq",
    "juicy"."no_promo" as "no_promo",
    "juicy"."promo" as "promo",
    "juicy"."total_cnt" as "total_cnt"
FROM
    "juicy"
    INNER JOIN "young" on "juicy"."week_seq" = "young"."cs_sale_date_week_seq"
WHERE
    "young"."cs_days_to_ship" > 5

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6
ORDER BY 
    "juicy"."total_cnt" desc nulls first,
    "juicy"."item_desc" asc nulls first,
    "juicy"."warehouse_name" asc nulls first,
    "juicy"."week_seq" asc nulls first
LIMIT (100)