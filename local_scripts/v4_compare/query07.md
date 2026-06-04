# Query 07

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

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 1698 | 20 | 72.30 ms |
| reference | 1698 | 20 | 71.17 ms |
| v4 / ref | 1.00x | 1.00x | 1.02x |

## Preql

```
# compute the average quantity, list price, discount, and sales price for promotional items sold in stores where the
# promotion is not offered by mail or a special event. Restrict the results to a specific gender, marital and
# educational status.
import physical_sales as physical_sales;

where
    physical_sales.customer_demographic.gender = 'M'
    and physical_sales.customer_demographic.marital_status = 'S'
    and physical_sales.customer_demographic.education_status = 'College'
    and (physical_sales.promotion.channel_email = 'N' or physical_sales.promotion.channel_event = 'N')
    and physical_sales.date.year = 2000
select
    physical_sales.item.text_id,
    avg(physical_sales.quantity) as avg_quantity,
    avg(physical_sales.list_price) as avg_list_price,
    avg(physical_sales.coupon_amt) as avg_coupon_amt,
    avg(physical_sales.sales_price) as avg_sales_price,
order by
    physical_sales.item.text_id asc
limit 100
;
```

## v4 generated SQL

```sql
SELECT
    "physical_sales_item_items"."I_ITEM_ID" as "physical_sales_item_text_id",
    avg("physical_sales_store_sales"."SS_QUANTITY") as "avg_quantity",
    avg("physical_sales_store_sales"."SS_LIST_PRICE") as "avg_list_price",
    avg("physical_sales_store_sales"."SS_COUPON_AMT") as "avg_coupon_amt",
    avg("physical_sales_store_sales"."SS_SALES_PRICE") as "avg_sales_price"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "physical_sales_date_date" on "physical_sales_store_sales"."SS_SOLD_DATE_SK" = "physical_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "physical_sales_item_items" on "physical_sales_store_sales"."SS_ITEM_SK" = "physical_sales_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."promotion" as "physical_sales_promotion_promotion" on "physical_sales_store_sales"."SS_PROMO_SK" = "physical_sales_promotion_promotion"."P_PROMO_SK"
    INNER JOIN "memory"."customer_demographics" as "physical_sales_customer_demographic_customer_demographics" on "physical_sales_store_sales"."SS_CDEMO_SK" = "physical_sales_customer_demographic_customer_demographics"."CD_DEMO_SK"
WHERE
    "physical_sales_customer_demographic_customer_demographics"."CD_GENDER" = 'M' and "physical_sales_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" = 'S' and "physical_sales_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS" = 'College' and ( "physical_sales_promotion_promotion"."P_CHANNEL_EMAIL" = 'N' or "physical_sales_promotion_promotion"."P_CHANNEL_EVENT" = 'N' ) and "physical_sales_date_date"."D_YEAR" = 2000

GROUP BY
    1
ORDER BY 
    "physical_sales_item_items"."I_ITEM_ID" asc
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
SELECT
    "physical_sales_item_items"."I_ITEM_ID" as "physical_sales_item_text_id",
    avg("physical_sales_store_sales"."SS_QUANTITY") as "avg_quantity",
    avg("physical_sales_store_sales"."SS_LIST_PRICE") as "avg_list_price",
    avg("physical_sales_store_sales"."SS_COUPON_AMT") as "avg_coupon_amt",
    avg("physical_sales_store_sales"."SS_SALES_PRICE") as "avg_sales_price"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "physical_sales_date_date" on "physical_sales_store_sales"."SS_SOLD_DATE_SK" = "physical_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "physical_sales_item_items" on "physical_sales_store_sales"."SS_ITEM_SK" = "physical_sales_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."promotion" as "physical_sales_promotion_promotion" on "physical_sales_store_sales"."SS_PROMO_SK" = "physical_sales_promotion_promotion"."P_PROMO_SK"
    INNER JOIN "memory"."customer_demographics" as "physical_sales_customer_demographic_customer_demographics" on "physical_sales_store_sales"."SS_CDEMO_SK" = "physical_sales_customer_demographic_customer_demographics"."CD_DEMO_SK"
WHERE
    "physical_sales_customer_demographic_customer_demographics"."CD_GENDER" = 'M' and "physical_sales_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" = 'S' and "physical_sales_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS" = 'College' and ( "physical_sales_promotion_promotion"."P_CHANNEL_EMAIL" = 'N' or "physical_sales_promotion_promotion"."P_CHANNEL_EVENT" = 'N' ) and "physical_sales_date_date"."D_YEAR" = 2000

GROUP BY
    1
ORDER BY 
    "physical_sales_item_items"."I_ITEM_ID" asc
LIMIT (100)
```
