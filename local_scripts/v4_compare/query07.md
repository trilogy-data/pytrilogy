# Query 07

**Status:** `mismatch`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (5159 rows) |
| reference execution | OK (100 rows) |
| results identical | NO |

## Preql

```
# compute the average quantity, list price, discount, and sales price for promotional items sold in stores where the
# promotion is not offered by mail or a special event. Restrict the results to a specific gender, marital and
# educational status.
import store_sales as store_sales;

where
    store_sales.customer_demographic.gender = 'M'
    and store_sales.customer_demographic.marital_status = 'S'
    and store_sales.customer_demographic.education_status = 'College'
    and (store_sales.promotion.channel_email = 'N' or store_sales.promotion.channel_event = 'N')
    and store_sales.date.year = 2000
select
    store_sales.item.name,
    avg(store_sales.quantity) as avg_quantity,
    avg(store_sales.list_price) as avg_list_price,
    avg(store_sales.coupon_amt) as avg_coupon_amt,
    avg(store_sales.sales_price) as avg_sales_price,
order by
    store_sales.item.name asc
limit 100
;
```

## v4 generated SQL

```sql
WITH 
thoughtful as (
SELECT
    "store_sales_store_sales"."SS_CDEMO_SK" as "store_sales_customer_demographic_id",
    "store_sales_store_sales"."SS_COUPON_AMT" as "store_sales_coupon_amt",
    "store_sales_store_sales"."SS_ITEM_SK" as "store_sales_item_id",
    "store_sales_store_sales"."SS_LIST_PRICE" as "store_sales_list_price",
    "store_sales_store_sales"."SS_PROMO_SK" as "store_sales_promotion_id",
    "store_sales_store_sales"."SS_QUANTITY" as "store_sales_quantity",
    "store_sales_store_sales"."SS_SALES_PRICE" as "store_sales_sales_price",
    "store_sales_store_sales"."SS_SOLD_DATE_SK" as "store_sales_date_id"
FROM
    "memory"."store_sales" as "store_sales_store_sales"),
cooperative as (
SELECT
    "thoughtful"."store_sales_coupon_amt" as "store_sales_coupon_amt",
    "thoughtful"."store_sales_customer_demographic_id" as "store_sales_customer_demographic_id",
    "thoughtful"."store_sales_date_id" as "store_sales_date_id",
    "thoughtful"."store_sales_item_id" as "store_sales_item_id",
    "thoughtful"."store_sales_list_price" as "store_sales_list_price",
    "thoughtful"."store_sales_promotion_id" as "store_sales_promotion_id",
    "thoughtful"."store_sales_quantity" as "store_sales_quantity",
    "thoughtful"."store_sales_sales_price" as "store_sales_sales_price"
FROM
    "thoughtful"
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8),
cheerful as (
SELECT
    "store_sales_promotion_promotion"."P_CHANNEL_EMAIL" as "store_sales_promotion_channel_email",
    "store_sales_promotion_promotion"."P_CHANNEL_EVENT" as "store_sales_promotion_channel_event",
    "store_sales_promotion_promotion"."P_PROMO_SK" as "store_sales_promotion_id"
FROM
    "memory"."promotion" as "store_sales_promotion_promotion"),
wakeful as (
SELECT
    "store_sales_item_items"."I_ITEM_ID" as "store_sales_item_name",
    "store_sales_item_items"."I_ITEM_SK" as "store_sales_item_id"
FROM
    "memory"."item" as "store_sales_item_items"),
highfalutin as (
SELECT
    "store_sales_date_date"."D_DATE_SK" as "store_sales_date_id",
    "store_sales_date_date"."D_YEAR" as "store_sales_date_year"
FROM
    "memory"."date_dim" as "store_sales_date_date"),
quizzical as (
SELECT
    "store_sales_customer_demographic_customer_demographics"."CD_DEMO_SK" as "store_sales_customer_demographic_id",
    "store_sales_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS" as "store_sales_customer_demographic_education_status",
    "store_sales_customer_demographic_customer_demographics"."CD_GENDER" as "store_sales_customer_demographic_gender",
    "store_sales_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" as "store_sales_customer_demographic_marital_status"
FROM
    "memory"."customer_demographics" as "store_sales_customer_demographic_customer_demographics"),
questionable as (
SELECT
    "cheerful"."store_sales_promotion_channel_email" as "store_sales_promotion_channel_email",
    "cheerful"."store_sales_promotion_channel_event" as "store_sales_promotion_channel_event",
    "cooperative"."store_sales_coupon_amt" as "store_sales_coupon_amt",
    "cooperative"."store_sales_list_price" as "store_sales_list_price",
    "cooperative"."store_sales_quantity" as "store_sales_quantity",
    "cooperative"."store_sales_sales_price" as "store_sales_sales_price",
    "highfalutin"."store_sales_date_year" as "store_sales_date_year",
    "quizzical"."store_sales_customer_demographic_education_status" as "store_sales_customer_demographic_education_status",
    "quizzical"."store_sales_customer_demographic_gender" as "store_sales_customer_demographic_gender",
    "quizzical"."store_sales_customer_demographic_marital_status" as "store_sales_customer_demographic_marital_status",
    "wakeful"."store_sales_item_name" as "store_sales_item_name"
FROM
    "cooperative"
    LEFT OUTER JOIN "highfalutin" on "cooperative"."store_sales_date_id" = "highfalutin"."store_sales_date_id"
    INNER JOIN "wakeful" on "cooperative"."store_sales_item_id" = "wakeful"."store_sales_item_id"
    LEFT OUTER JOIN "cheerful" on "cooperative"."store_sales_promotion_id" = "cheerful"."store_sales_promotion_id"
    LEFT OUTER JOIN "quizzical" on "cooperative"."store_sales_customer_demographic_id" = "quizzical"."store_sales_customer_demographic_id"
WHERE
    "quizzical"."store_sales_customer_demographic_gender" = 'M' and "quizzical"."store_sales_customer_demographic_marital_status" = 'S' and "quizzical"."store_sales_customer_demographic_education_status" = 'College' and ( "cheerful"."store_sales_promotion_channel_email" = 'N' or "cheerful"."store_sales_promotion_channel_event" = 'N' ) and "highfalutin"."store_sales_date_year" = 2000

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
    avg("questionable"."store_sales_quantity") as "avg_quantity",
    avg("questionable"."store_sales_list_price") as "avg_list_price",
    avg("questionable"."store_sales_coupon_amt") as "avg_coupon_amt",
    avg("questionable"."store_sales_sales_price") as "avg_sales_price",
    "questionable"."store_sales_item_name" as "store_sales_item_name"
FROM
    "questionable"
WHERE
    "questionable"."store_sales_customer_demographic_gender" = 'M' and "questionable"."store_sales_customer_demographic_marital_status" = 'S' and "questionable"."store_sales_customer_demographic_education_status" = 'College' and ( "questionable"."store_sales_promotion_channel_email" = 'N' or "questionable"."store_sales_promotion_channel_event" = 'N' ) and "questionable"."store_sales_date_year" = 2000

GROUP BY
    5
```

## Reference SQL (zquery log)

```sql
SELECT
    "store_sales_item_items"."I_ITEM_ID" as "store_sales_item_name",
    avg("store_sales_store_sales"."SS_QUANTITY") as "avg_quantity",
    avg("store_sales_store_sales"."SS_LIST_PRICE") as "avg_list_price",
    avg("store_sales_store_sales"."SS_COUPON_AMT") as "avg_coupon_amt",
    avg("store_sales_store_sales"."SS_SALES_PRICE") as "avg_sales_price"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "store_sales_item_items" on "store_sales_store_sales"."SS_ITEM_SK" = "store_sales_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."promotion" as "store_sales_promotion_promotion" on "store_sales_store_sales"."SS_PROMO_SK" = "store_sales_promotion_promotion"."P_PROMO_SK"
    INNER JOIN "memory"."customer_demographics" as "store_sales_customer_demographic_customer_demographics" on "store_sales_store_sales"."SS_CDEMO_SK" = "store_sales_customer_demographic_customer_demographics"."CD_DEMO_SK"
WHERE
    "store_sales_customer_demographic_customer_demographics"."CD_GENDER" = 'M' and "store_sales_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" = 'S' and "store_sales_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS" = 'College' and ( "store_sales_promotion_promotion"."P_CHANNEL_EMAIL" = 'N' or "store_sales_promotion_promotion"."P_CHANNEL_EVENT" = 'N' ) and "store_sales_date_date"."D_YEAR" = 2000

GROUP BY
    1
ORDER BY 
    "store_sales_item_items"."I_ITEM_ID" asc
LIMIT (100)
```

## Result comparison

v4 rows: 5159 (5159 distinct)
ref rows: 100 (100 distinct)
only in v4 (showing up to 5 of 5159):
  1x  (13.0, 158.88, 0.0, 93.73, 'AAAAAAAANBCAAAAA')
  1x  (51.333333333333336, 65.18333333333334, 476.0366666666667, 45.78666666666667, 'AAAAAAAAHADAAAAA')
  1x  (39.25, 85.0225, 0.0, 52.44, 'AAAAAAAADKDAAAAA')
  1x  (35.0, 59.27, 0.0, 37.295, 'AAAAAAAAGCIAAAAA')
  1x  (42.0, 36.69, 0.0, 13.025, 'AAAAAAAAGEKAAAAA')
only in ref (showing up to 5 of 100):
  1x  ('AAAAAAAAAAABAAAA', 9.0, 86.25, 0.0, 75.03)
  1x  ('AAAAAAAAAAACAAAA', 25.0, 149.41, 0.0, 94.12)
  1x  ('AAAAAAAAAAAEAAAA', 16.0, 127.87, 0.0, 26.85)
  1x  ('AAAAAAAAAABBAAAA', 18.0, 23.43, 0.0, 3.98)
  1x  ('AAAAAAAAAABEAAAA', 20.5, 85.01, 0.0, 42.05)
