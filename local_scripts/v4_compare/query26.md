# Query 26

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (100 rows) |
| reference execution | OK (100 rows) |
| results identical | YES |

## Result comparison

v4 rows: 100 (100 distinct)
ref rows: 100 (100 distinct)

## SQL size

| Source | Chars | Lines |
| --- | --- | --- |
| v4 | 4395 | 108 |
| reference | 1394 | 20 |
| v4 / ref | 3.15x | 5.40x |

## Preql

```
#Computes the average quantity, list price, discount, sales price for promotional items sold through the catalog
#channel where the promotion was not offered by mail or in an event for given gender, marital status and
#educational status.
import catalog_sales as cs;

where
    cs.bill_customer_demographic.gender = 'M'
    and cs.bill_customer_demographic.marital_status = 'S'
    and cs.bill_customer_demographic.education_status = 'College'
    and (cs.promotion.channel_email = 'N' or cs.promotion.channel_event = 'N')
    and cs.date.year = 2000
select
    cs.item.name,
    avg(cs.quantity) as agg1,
    avg(cs.list_price) as agg2,
    avg(cs.coupon_amt) as agg3,
    avg(cs.sales_price) as agg4,
order by
    cs.item.name asc
limit 100
;
```

## v4 generated SQL

```sql
WITH 
cooperative as (
SELECT
    "cs_promotion_promotion"."P_CHANNEL_EMAIL" as "cs_promotion_channel_email",
    "cs_promotion_promotion"."P_CHANNEL_EVENT" as "cs_promotion_channel_event",
    "cs_promotion_promotion"."P_PROMO_SK" as "cs_promotion_id"
FROM
    "memory"."promotion" as "cs_promotion_promotion"),
thoughtful as (
SELECT
    "cs_item_items"."I_ITEM_ID" as "cs_item_name",
    "cs_item_items"."I_ITEM_SK" as "cs_item_id"
FROM
    "memory"."item" as "cs_item_items"),
cheerful as (
SELECT
    "cs_date_date"."D_DATE_SK" as "cs_date_id",
    "cs_date_date"."D_YEAR" as "cs_date_year"
FROM
    "memory"."date_dim" as "cs_date_date"),
highfalutin as (
SELECT
    "cs_catalog_sales"."CS_BILL_CDEMO_SK" as "cs_bill_customer_demographic_id",
    "cs_catalog_sales"."CS_COUPON_AMT" as "cs_coupon_amt",
    "cs_catalog_sales"."CS_ITEM_SK" as "cs_item_id",
    "cs_catalog_sales"."CS_LIST_PRICE" as "cs_list_price",
    "cs_catalog_sales"."CS_PROMO_SK" as "cs_promotion_id",
    "cs_catalog_sales"."CS_QUANTITY" as "cs_quantity",
    "cs_catalog_sales"."CS_SALES_PRICE" as "cs_sales_price",
    "cs_catalog_sales"."CS_SOLD_DATE_SK" as "cs_date_id"
FROM
    "memory"."catalog_sales" as "cs_catalog_sales"),
wakeful as (
SELECT
    "highfalutin"."cs_bill_customer_demographic_id" as "cs_bill_customer_demographic_id",
    "highfalutin"."cs_coupon_amt" as "cs_coupon_amt",
    "highfalutin"."cs_date_id" as "cs_date_id",
    "highfalutin"."cs_item_id" as "cs_item_id",
    "highfalutin"."cs_list_price" as "cs_list_price",
    "highfalutin"."cs_promotion_id" as "cs_promotion_id",
    "highfalutin"."cs_quantity" as "cs_quantity",
    "highfalutin"."cs_sales_price" as "cs_sales_price"
FROM
    "highfalutin"
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8),
quizzical as (
SELECT
    "cs_bill_customer_demographic_customer_demographics"."CD_DEMO_SK" as "cs_bill_customer_demographic_id",
    "cs_bill_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS" as "cs_bill_customer_demographic_education_status",
    "cs_bill_customer_demographic_customer_demographics"."CD_GENDER" as "cs_bill_customer_demographic_gender",
    "cs_bill_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" as "cs_bill_customer_demographic_marital_status"
FROM
    "memory"."customer_demographics" as "cs_bill_customer_demographic_customer_demographics"),
questionable as (
SELECT
    "cheerful"."cs_date_year" as "cs_date_year",
    "cooperative"."cs_promotion_channel_email" as "cs_promotion_channel_email",
    "cooperative"."cs_promotion_channel_event" as "cs_promotion_channel_event",
    "quizzical"."cs_bill_customer_demographic_education_status" as "cs_bill_customer_demographic_education_status",
    "quizzical"."cs_bill_customer_demographic_gender" as "cs_bill_customer_demographic_gender",
    "quizzical"."cs_bill_customer_demographic_marital_status" as "cs_bill_customer_demographic_marital_status",
    "thoughtful"."cs_item_name" as "cs_item_name",
    "wakeful"."cs_coupon_amt" as "cs_coupon_amt",
    "wakeful"."cs_list_price" as "cs_list_price",
    "wakeful"."cs_quantity" as "cs_quantity",
    "wakeful"."cs_sales_price" as "cs_sales_price"
FROM
    "wakeful"
    LEFT OUTER JOIN "cheerful" on "wakeful"."cs_date_id" = "cheerful"."cs_date_id"
    INNER JOIN "thoughtful" on "wakeful"."cs_item_id" = "thoughtful"."cs_item_id"
    LEFT OUTER JOIN "cooperative" on "wakeful"."cs_promotion_id" = "cooperative"."cs_promotion_id"
    LEFT OUTER JOIN "quizzical" on "wakeful"."cs_bill_customer_demographic_id" = "quizzical"."cs_bill_customer_demographic_id"
WHERE
    "quizzical"."cs_bill_customer_demographic_gender" = 'M' and "quizzical"."cs_bill_customer_demographic_marital_status" = 'S' and "quizzical"."cs_bill_customer_demographic_education_status" = 'College' and ( "cooperative"."cs_promotion_channel_email" = 'N' or "cooperative"."cs_promotion_channel_event" = 'N' ) and "cheerful"."cs_date_year" = 2000

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
    11)
SELECT
    avg("questionable"."cs_quantity") as "agg1",
    avg("questionable"."cs_list_price") as "agg2",
    avg("questionable"."cs_coupon_amt") as "agg3",
    avg("questionable"."cs_sales_price") as "agg4",
    "questionable"."cs_item_name" as "cs_item_name"
FROM
    "questionable"
GROUP BY
    5
ORDER BY 
    "questionable"."cs_item_name" asc
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
SELECT
    "cs_item_items"."I_ITEM_ID" as "cs_item_name",
    avg("cs_catalog_sales"."CS_QUANTITY") as "agg1",
    avg("cs_catalog_sales"."CS_LIST_PRICE") as "agg2",
    avg("cs_catalog_sales"."CS_COUPON_AMT") as "agg3",
    avg("cs_catalog_sales"."CS_SALES_PRICE") as "agg4"
FROM
    "memory"."catalog_sales" as "cs_catalog_sales"
    INNER JOIN "memory"."date_dim" as "cs_date_date" on "cs_catalog_sales"."CS_SOLD_DATE_SK" = "cs_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "cs_item_items" on "cs_catalog_sales"."CS_ITEM_SK" = "cs_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."promotion" as "cs_promotion_promotion" on "cs_catalog_sales"."CS_PROMO_SK" = "cs_promotion_promotion"."P_PROMO_SK"
    INNER JOIN "memory"."customer_demographics" as "cs_bill_customer_demographic_customer_demographics" on "cs_catalog_sales"."CS_BILL_CDEMO_SK" = "cs_bill_customer_demographic_customer_demographics"."CD_DEMO_SK"
WHERE
    "cs_bill_customer_demographic_customer_demographics"."CD_GENDER" = 'M' and "cs_bill_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" = 'S' and "cs_bill_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS" = 'College' and ( "cs_promotion_promotion"."P_CHANNEL_EMAIL" = 'N' or "cs_promotion_promotion"."P_CHANNEL_EVENT" = 'N' ) and "cs_date_date"."D_YEAR" = 2000

GROUP BY
    1
ORDER BY 
    "cs_item_items"."I_ITEM_ID" asc
LIMIT (100)
```
