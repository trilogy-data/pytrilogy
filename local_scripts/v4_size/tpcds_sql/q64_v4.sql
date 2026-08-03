
WITH 
vast as (
SELECT
    "ss_store_sales"."SS_ADDR_SK" as "ss_pos_address_sk",
    "ss_store_sales"."SS_CUSTOMER_SK" as "ss_customer_sk",
    "ss_store_sales"."SS_ITEM_SK" as "ss_item_sk",
    "ss_store_sales"."SS_STORE_SK" as "ss_store_sk"
FROM
    "memory"."store_sales" as "ss_store_sales"
GROUP BY
    1,
    2,
    3,
    4),
uneven as (
SELECT
    "ss_customer_customers"."C_CURRENT_ADDR_SK" as "ss_customer_current_address_sk",
    "ss_customer_customers"."C_CURRENT_CDEMO_SK" as "ss_customer_current_demographics_sk",
    "ss_customer_customers"."C_CUSTOMER_SK" as "ss_customer_sk",
    "ss_customer_customers"."C_FIRST_SALES_DATE_SK" as "ss_customer_first_sales_date_sk",
    "ss_customer_customers"."C_FIRST_SHIPTO_DATE_SK" as "ss_customer_first_shipto_date_sk",
    coalesce("ss_customer_customers"."C_CURRENT_ADDR_SK") as "_virt_presence_9232186781707329"
FROM
    "memory"."customer" as "ss_customer_customers"),
cool as (
SELECT
    "ss_customer_current_address_customer_address"."CA_CITY" as "c_city",
    "ss_customer_current_address_customer_address"."CA_STREET_NAME" as "c_str",
    "ss_customer_current_address_customer_address"."CA_STREET_NUMBER" as "c_sn",
    "ss_customer_current_address_customer_address"."CA_ZIP" as "c_zip",
    "ss_item_items"."I_PRODUCT_NAME" as "p_name",
    "ss_pos_address_customer_address"."CA_CITY" as "b_city",
    "ss_pos_address_customer_address"."CA_STREET_NAME" as "b_str",
    "ss_pos_address_customer_address"."CA_STREET_NUMBER" as "b_sn",
    "ss_pos_address_customer_address"."CA_ZIP" as "b_zip",
    "ss_store_store"."S_STORE_NAME" as "s_name",
    "ss_store_store"."S_ZIP" as "s_zip",
    coalesce("ss_customer_current_address_customer_address"."CA_ADDRESS_SK","ss_customer_customers"."C_CURRENT_ADDR_SK") as "agg_99_c_addr_99",
    coalesce("ss_customer_current_address_customer_address"."CA_ADDRESS_SK","ss_customer_customers"."C_CURRENT_ADDR_SK") as "ss_customer_current_address_sk",
    coalesce("ss_item_items"."I_ITEM_SK","vast"."ss_item_sk") as "agg_00_item_sk_00",
    coalesce("ss_item_items"."I_ITEM_SK","vast"."ss_item_sk") as "agg_99_item_sk_99",
    coalesce("ss_item_items"."I_ITEM_SK","vast"."ss_item_sk") as "ss_item_sk",
    coalesce("ss_pos_address_customer_address"."CA_ADDRESS_SK","vast"."ss_pos_address_sk") as "agg_99_b_addr_99",
    coalesce("ss_pos_address_customer_address"."CA_ADDRESS_SK","vast"."ss_pos_address_sk") as "ss_pos_address_sk",
    coalesce("ss_store_store"."S_STORE_SK","vast"."ss_store_sk") as "agg_00_store_sk_00",
    coalesce("ss_store_store"."S_STORE_SK","vast"."ss_store_sk") as "agg_99_store_sk_99",
    coalesce("ss_store_store"."S_STORE_SK","vast"."ss_store_sk") as "ss_store_sk"
FROM
    "vast"
    FULL JOIN "memory"."item" as "ss_item_items" on "vast"."ss_item_sk" = "ss_item_items"."I_ITEM_SK"
    FULL JOIN "memory"."store" as "ss_store_store" on "vast"."ss_store_sk" = "ss_store_store"."S_STORE_SK"
    FULL JOIN "memory"."customer" as "ss_customer_customers" on "vast"."ss_customer_sk" = "ss_customer_customers"."C_CUSTOMER_SK"
    FULL JOIN "memory"."customer_address" as "ss_pos_address_customer_address" on "vast"."ss_pos_address_sk" = "ss_pos_address_customer_address"."CA_ADDRESS_SK"
    FULL JOIN "memory"."customer_address" as "ss_customer_current_address_customer_address" on "ss_customer_customers"."C_CURRENT_ADDR_SK" = "ss_customer_current_address_customer_address"."CA_ADDRESS_SK"),
wakeful as (
SELECT
    coalesce("cs_catalog_returns"."CR_ITEM_SK","cs_catalog_sales"."CS_ITEM_SK") as "cs_item_sk"
FROM
    "memory"."catalog_sales" as "cs_catalog_sales"
    LEFT OUTER JOIN "memory"."catalog_returns" as "cs_catalog_returns" on "cs_catalog_sales"."CS_ITEM_SK" = "cs_catalog_returns"."CR_ITEM_SK" AND "cs_catalog_sales"."CS_ORDER_NUMBER" = "cs_catalog_returns"."CR_ORDER_NUMBER"
GROUP BY
    1
HAVING
    sum(CASE WHEN ("cs_catalog_returns"."CR_ORDER_NUMBER" is not null) = True THEN "cs_catalog_sales"."CS_EXT_LIST_PRICE" ELSE NULL END) > 2 * sum(CASE WHEN ("cs_catalog_returns"."CR_ORDER_NUMBER" is not null) = True THEN ( coalesce("cs_catalog_returns"."CR_REFUNDED_CASH",0) + coalesce("cs_catalog_returns"."CR_REVERSED_CHARGE",0) ) + coalesce("cs_catalog_returns"."CR_STORE_CREDIT",0) ELSE NULL END)
),
cooperative as (
SELECT
    "wakeful"."cs_item_sk" as "cs_ui_cs_ui_item_id"
FROM
    "wakeful"),
sparkling as (
SELECT
    "ss_store_sales"."SS_ADDR_SK" as "ss_pos_address_sk",
    "ss_store_sales"."SS_CDEMO_SK" as "ss_pos_customer_demographic_sk",
    "ss_store_sales"."SS_COUPON_AMT" as "ss_coupon_amt",
    "ss_store_sales"."SS_CUSTOMER_SK" as "ss_customer_sk",
    "ss_store_sales"."SS_ITEM_SK" as "ss_item_sk",
    "ss_store_sales"."SS_LIST_PRICE" as "ss_list_price",
    "ss_store_sales"."SS_SOLD_DATE_SK" as "ss_sale_date_sk",
    "ss_store_sales"."SS_STORE_SK" as "ss_store_sk",
    "ss_store_sales"."SS_TICKET_NUMBER" as "ss_ticket_number",
    "ss_store_sales"."SS_WHOLESALE_COST" as "ss_wholesale_cost",
    coalesce("ss_store_sales"."SS_ADDR_SK") as "_virt_presence_9866050310038537",
    coalesce("ss_store_sales"."SS_STORE_SK") as "_virt_presence_334607598686241"
FROM
    "memory"."store_sales" as "ss_store_sales"
WHERE
    exists (select 1 from cooperative where cooperative."cs_ui_cs_ui_item_id" is not distinct from "ss_store_sales"."SS_ITEM_SK") and "ss_store_sales"."SS_CUSTOMER_SK" is not null
),
vacuous as (
SELECT
    "ss_item_items"."I_COLOR" as "ss_item_color",
    "ss_item_items"."I_CURRENT_PRICE" as "ss_item_current_price",
    "ss_item_items"."I_ITEM_SK" as "ss_item_sk"
FROM
    "memory"."item" as "ss_item_items"
WHERE
    exists (select 1 from cooperative where cooperative."cs_ui_cs_ui_item_id" is not distinct from "ss_item_items"."I_ITEM_SK") and ("ss_item_items"."I_COLOR" is not null and "ss_item_items"."I_COLOR" in ('purple','burlywood','indian','spring','floral','medium')) and "ss_item_items"."I_CURRENT_PRICE" BETWEEN 65 AND 74
),
late as (
SELECT
    ("ss_store_returns"."SR_TICKET_NUMBER" is not null) as "ss_is_returned",
    coalesce("ss_store_returns"."SR_ITEM_SK","ss_store_sales"."SS_ITEM_SK") as "agg_00_item_sk_00",
    coalesce("ss_store_returns"."SR_ITEM_SK","ss_store_sales"."SS_ITEM_SK") as "agg_99_item_sk_99",
    coalesce("ss_store_returns"."SR_ITEM_SK","ss_store_sales"."SS_ITEM_SK") as "ss_item_sk",
    coalesce("ss_store_returns"."SR_TICKET_NUMBER","ss_store_sales"."SS_TICKET_NUMBER") as "ss_ticket_number"
FROM
    "memory"."store_sales" as "ss_store_sales"
    LEFT OUTER JOIN "memory"."store_returns" as "ss_store_returns" on "ss_store_sales"."SS_ITEM_SK" = "ss_store_returns"."SR_ITEM_SK" AND "ss_store_sales"."SS_TICKET_NUMBER" = "ss_store_returns"."SR_TICKET_NUMBER"
WHERE
    exists (select 1 from cooperative where cooperative."cs_ui_cs_ui_item_id" is not distinct from coalesce("ss_store_returns"."SR_ITEM_SK","ss_store_sales"."SS_ITEM_SK")) and ("ss_store_returns"."SR_TICKET_NUMBER" is not null) and "ss_store_sales"."SS_CUSTOMER_SK" is not null
),
premium as (
SELECT
    "sparkling"."ss_coupon_amt" as "ss_coupon_amt",
    "sparkling"."ss_list_price" as "ss_list_price",
    "sparkling"."ss_store_sk" as "ss_store_sk",
    "sparkling"."ss_ticket_number" as "ss_ticket_number",
    "sparkling"."ss_wholesale_cost" as "ss_wholesale_cost",
    "ss_customer_current_demographics_customer_demographics"."CD_MARITAL_STATUS" as "ss_customer_current_demographics_marital_status",
    "ss_pos_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" as "ss_pos_customer_demographic_marital_status",
    "ss_sale_date_date"."D_YEAR" as "ss_sale_date_year",
    coalesce("sparkling"."ss_item_sk","vacuous"."ss_item_sk") as "agg_00_item_sk_00",
    coalesce("sparkling"."ss_item_sk","vacuous"."ss_item_sk") as "agg_99_item_sk_99",
    coalesce("sparkling"."ss_item_sk","vacuous"."ss_item_sk") as "ss_item_sk"
FROM
    "sparkling"
    INNER JOIN "vacuous" on "sparkling"."ss_item_sk" = "vacuous"."ss_item_sk"
    INNER JOIN "uneven" on "sparkling"."ss_customer_sk" = "uneven"."ss_customer_sk"
    INNER JOIN "memory"."date_dim" as "ss_sale_date_date" on "sparkling"."ss_sale_date_sk" = "ss_sale_date_date"."D_DATE_SK"
    LEFT OUTER JOIN "memory"."customer_demographics" as "ss_pos_customer_demographic_customer_demographics" on "sparkling"."ss_pos_customer_demographic_sk" = "ss_pos_customer_demographic_customer_demographics"."CD_DEMO_SK"
    LEFT OUTER JOIN "memory"."customer_demographics" as "ss_customer_current_demographics_customer_demographics" on "uneven"."ss_customer_current_demographics_sk" = "ss_customer_current_demographics_customer_demographics"."CD_DEMO_SK"
WHERE
    exists (select 1 from cooperative where cooperative."cs_ui_cs_ui_item_id" is not distinct from coalesce("sparkling"."ss_item_sk","vacuous"."ss_item_sk")) and "ss_sale_date_date"."D_YEAR" = 2000 and ("vacuous"."ss_item_color" is not null and "vacuous"."ss_item_color" in ('purple','burlywood','indian','spring','floral','medium')) and "vacuous"."ss_item_current_price" BETWEEN 65 AND 74 and coalesce("sparkling"."ss_customer_sk","uneven"."ss_customer_sk") is not null and "sparkling"."_virt_presence_334607598686241" is not null and "sparkling"."_virt_presence_9866050310038537" is not null and "uneven"."_virt_presence_9232186781707329" is not null
),
puzzled as (
SELECT
    "premium"."ss_coupon_amt" as "ss_coupon_amt",
    "premium"."ss_customer_current_demographics_marital_status" as "ss_customer_current_demographics_marital_status",
    "premium"."ss_item_sk" as "ss_item_sk",
    "premium"."ss_list_price" as "ss_list_price",
    "premium"."ss_pos_customer_demographic_marital_status" as "ss_pos_customer_demographic_marital_status",
    "premium"."ss_sale_date_year" as "ss_sale_date_year",
    "premium"."ss_store_sk" as "ss_store_sk",
    "premium"."ss_ticket_number" as "ss_ticket_number",
    "premium"."ss_wholesale_cost" as "ss_wholesale_cost"
FROM
    "late"
    RIGHT OUTER JOIN "premium" on "late"."ss_item_sk" is not distinct from "premium"."ss_item_sk" AND "late"."ss_ticket_number" is not distinct from "premium"."ss_ticket_number"
WHERE
    "late"."ss_is_returned"

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9),
rambunctious as (
SELECT
    "puzzled"."ss_item_sk" as "agg_00_item_sk_00",
    "puzzled"."ss_sale_date_year" as "agg_00_syear_00",
    "puzzled"."ss_store_sk" as "agg_00_store_sk_00",
    count("puzzled"."ss_ticket_number") as "agg_00_cnt_00",
    sum("puzzled"."ss_coupon_amt") as "agg_00_s3_00",
    sum("puzzled"."ss_list_price") as "agg_00_s2_00",
    sum("puzzled"."ss_wholesale_cost") as "agg_00_s1_00"
FROM
    "puzzled"
WHERE
    "puzzled"."ss_pos_customer_demographic_marital_status" != "puzzled"."ss_customer_current_demographics_marital_status"

GROUP BY
    1,
    2,
    3),
abhorrent as (
SELECT
    "sparkling"."ss_coupon_amt" as "ss_coupon_amt",
    "sparkling"."ss_list_price" as "ss_list_price",
    "sparkling"."ss_pos_address_sk" as "ss_pos_address_sk",
    "sparkling"."ss_store_sk" as "ss_store_sk",
    "sparkling"."ss_ticket_number" as "ss_ticket_number",
    "sparkling"."ss_wholesale_cost" as "ss_wholesale_cost",
    "ss_customer_current_demographics_customer_demographics"."CD_MARITAL_STATUS" as "ss_customer_current_demographics_marital_status",
    "ss_customer_first_sales_date_date"."D_YEAR" as "ss_customer_first_sales_date_year",
    "ss_customer_first_shipto_date_date"."D_YEAR" as "ss_customer_first_shipto_date_year",
    "ss_pos_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" as "ss_pos_customer_demographic_marital_status",
    "ss_sale_date_date"."D_YEAR" as "ss_sale_date_year",
    "uneven"."ss_customer_current_address_sk" as "ss_customer_current_address_sk",
    coalesce("sparkling"."ss_item_sk","vacuous"."ss_item_sk") as "agg_00_item_sk_00",
    coalesce("sparkling"."ss_item_sk","vacuous"."ss_item_sk") as "agg_99_item_sk_99",
    coalesce("sparkling"."ss_item_sk","vacuous"."ss_item_sk") as "ss_item_sk"
FROM
    "sparkling"
    INNER JOIN "vacuous" on "sparkling"."ss_item_sk" = "vacuous"."ss_item_sk"
    INNER JOIN "uneven" on "sparkling"."ss_customer_sk" = "uneven"."ss_customer_sk"
    INNER JOIN "memory"."date_dim" as "ss_sale_date_date" on "sparkling"."ss_sale_date_sk" = "ss_sale_date_date"."D_DATE_SK"
    LEFT OUTER JOIN "memory"."customer_demographics" as "ss_pos_customer_demographic_customer_demographics" on "sparkling"."ss_pos_customer_demographic_sk" = "ss_pos_customer_demographic_customer_demographics"."CD_DEMO_SK"
    LEFT OUTER JOIN "memory"."date_dim" as "ss_customer_first_sales_date_date" on "uneven"."ss_customer_first_sales_date_sk" = "ss_customer_first_sales_date_date"."D_DATE_SK"
    LEFT OUTER JOIN "memory"."date_dim" as "ss_customer_first_shipto_date_date" on "uneven"."ss_customer_first_shipto_date_sk" = "ss_customer_first_shipto_date_date"."D_DATE_SK"
    LEFT OUTER JOIN "memory"."customer_demographics" as "ss_customer_current_demographics_customer_demographics" on "uneven"."ss_customer_current_demographics_sk" = "ss_customer_current_demographics_customer_demographics"."CD_DEMO_SK"
WHERE
    exists (select 1 from cooperative where cooperative."cs_ui_cs_ui_item_id" is not distinct from coalesce("sparkling"."ss_item_sk","vacuous"."ss_item_sk")) and "ss_sale_date_date"."D_YEAR" = 1999 and ("vacuous"."ss_item_color" is not null and "vacuous"."ss_item_color" in ('purple','burlywood','indian','spring','floral','medium')) and "vacuous"."ss_item_current_price" BETWEEN 65 AND 74 and coalesce("sparkling"."ss_customer_sk","uneven"."ss_customer_sk") is not null and "sparkling"."_virt_presence_334607598686241" is not null and "sparkling"."_virt_presence_9866050310038537" is not null and "uneven"."_virt_presence_9232186781707329" is not null
),
scrawny as (
SELECT
    "abhorrent"."ss_coupon_amt" as "ss_coupon_amt",
    "abhorrent"."ss_customer_current_address_sk" as "ss_customer_current_address_sk",
    "abhorrent"."ss_customer_current_demographics_marital_status" as "ss_customer_current_demographics_marital_status",
    "abhorrent"."ss_customer_first_sales_date_year" as "ss_customer_first_sales_date_year",
    "abhorrent"."ss_customer_first_shipto_date_year" as "ss_customer_first_shipto_date_year",
    "abhorrent"."ss_item_sk" as "ss_item_sk",
    "abhorrent"."ss_list_price" as "ss_list_price",
    "abhorrent"."ss_pos_address_sk" as "ss_pos_address_sk",
    "abhorrent"."ss_pos_customer_demographic_marital_status" as "ss_pos_customer_demographic_marital_status",
    "abhorrent"."ss_sale_date_year" as "ss_sale_date_year",
    "abhorrent"."ss_store_sk" as "ss_store_sk",
    "abhorrent"."ss_ticket_number" as "ss_ticket_number",
    "abhorrent"."ss_wholesale_cost" as "ss_wholesale_cost"
FROM
    "late"
    RIGHT OUTER JOIN "abhorrent" on "late"."ss_item_sk" is not distinct from "abhorrent"."ss_item_sk" AND "late"."ss_ticket_number" is not distinct from "abhorrent"."ss_ticket_number"
WHERE
    "late"."ss_is_returned"

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13),
kaput as (
SELECT
    "scrawny"."ss_customer_current_address_sk" as "agg_99_c_addr_99",
    "scrawny"."ss_item_sk" as "agg_99_item_sk_99",
    "scrawny"."ss_pos_address_sk" as "agg_99_b_addr_99",
    "scrawny"."ss_sale_date_year" as "agg_99_syear_99",
    "scrawny"."ss_store_sk" as "agg_99_store_sk_99",
    count("scrawny"."ss_ticket_number") as "agg_99_cnt_99",
    sum("scrawny"."ss_coupon_amt") as "agg_99_s3_99",
    sum("scrawny"."ss_list_price") as "agg_99_s2_99",
    sum("scrawny"."ss_wholesale_cost") as "agg_99_s1_99"
FROM
    "scrawny"
WHERE
    "scrawny"."ss_pos_customer_demographic_marital_status" != "scrawny"."ss_customer_current_demographics_marital_status"

GROUP BY
    1,
    2,
    3,
    4,
    5,
    "scrawny"."ss_customer_first_sales_date_year",
    "scrawny"."ss_customer_first_shipto_date_year")
SELECT
    "cool"."p_name" as "p_name",
    "cool"."s_name" as "s_name",
    "cool"."s_zip" as "s_zip",
    "cool"."b_sn" as "b_sn",
    "cool"."b_str" as "b_str",
    "cool"."b_city" as "b_city",
    "cool"."b_zip" as "b_zip",
    "cool"."c_sn" as "c_sn",
    "cool"."c_str" as "c_str",
    "cool"."c_city" as "c_city",
    "cool"."c_zip" as "c_zip",
    "kaput"."agg_99_syear_99" as "agg_99_syear_99",
    "kaput"."agg_99_cnt_99" as "agg_99_cnt_99",
    "kaput"."agg_99_s1_99" as "agg_99_s1_99",
    "kaput"."agg_99_s2_99" as "agg_99_s2_99",
    "kaput"."agg_99_s3_99" as "agg_99_s3_99",
    "rambunctious"."agg_00_s1_00" as "agg_00_s1_00",
    "rambunctious"."agg_00_s2_00" as "agg_00_s2_00",
    "rambunctious"."agg_00_s3_00" as "agg_00_s3_00",
    "rambunctious"."agg_00_syear_00" as "agg_00_syear_00",
    "rambunctious"."agg_00_cnt_00" as "agg_00_cnt_00"
FROM
    "kaput"
    INNER JOIN "cool" on "kaput"."agg_99_b_addr_99" = "cool"."ss_pos_address_sk" AND "kaput"."agg_99_c_addr_99" = "cool"."ss_customer_current_address_sk" AND "kaput"."agg_99_item_sk_99" = "cool"."ss_item_sk" AND "kaput"."agg_99_store_sk_99" = "cool"."ss_store_sk"
    INNER JOIN "rambunctious" on "cool"."ss_item_sk" = "rambunctious"."agg_00_item_sk_00" AND "cool"."ss_store_sk" = "rambunctious"."agg_00_store_sk_00" AND "kaput"."agg_99_item_sk_99" = "rambunctious"."agg_00_item_sk_00" AND "kaput"."agg_99_store_sk_99" = "rambunctious"."agg_00_store_sk_00"
WHERE
    "rambunctious"."agg_00_cnt_00" <= "kaput"."agg_99_cnt_99"

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14,
    15,
    16,
    17,
    18,
    19,
    20,
    21,
    coalesce("cool"."ss_customer_current_address_sk","kaput"."agg_99_c_addr_99"),
    coalesce("cool"."ss_item_sk","kaput"."agg_99_item_sk_99","rambunctious"."agg_00_item_sk_00"),
    coalesce("cool"."ss_pos_address_sk","kaput"."agg_99_b_addr_99"),
    coalesce("cool"."ss_store_sk","kaput"."agg_99_store_sk_99","rambunctious"."agg_00_store_sk_00")
ORDER BY 
    "cool"."p_name" asc nulls first,
    "cool"."s_name" asc nulls first,
    "rambunctious"."agg_00_cnt_00" asc nulls first,
    "kaput"."agg_99_s1_99" asc nulls first,
    "rambunctious"."agg_00_s1_00" asc nulls first