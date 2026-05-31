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
| v4 | 1617 | 20 | 63.60 ms |
| reference | 1617 | 20 | 59.12 ms |
| v4 / ref | 1.00x | 1.00x | 1.08x |

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
SELECT
    avg("store_sales_store_sales"."SS_COUPON_AMT") as "avg_coupon_amt",
    avg("store_sales_store_sales"."SS_LIST_PRICE") as "avg_list_price",
    avg("store_sales_store_sales"."SS_QUANTITY") as "avg_quantity",
    avg("store_sales_store_sales"."SS_SALES_PRICE") as "avg_sales_price",
    "store_sales_item_items"."I_ITEM_ID" as "store_sales_item_name"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "store_sales_item_items" on "store_sales_store_sales"."SS_ITEM_SK" = "store_sales_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."promotion" as "store_sales_promotion_promotion" on "store_sales_store_sales"."SS_PROMO_SK" = "store_sales_promotion_promotion"."P_PROMO_SK"
    INNER JOIN "memory"."customer_demographics" as "store_sales_customer_demographic_customer_demographics" on "store_sales_store_sales"."SS_CDEMO_SK" = "store_sales_customer_demographic_customer_demographics"."CD_DEMO_SK"
WHERE
    "store_sales_customer_demographic_customer_demographics"."CD_GENDER" = 'M' and "store_sales_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" = 'S' and "store_sales_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS" = 'College' and ( "store_sales_promotion_promotion"."P_CHANNEL_EMAIL" = 'N' or "store_sales_promotion_promotion"."P_CHANNEL_EVENT" = 'N' ) and "store_sales_date_date"."D_YEAR" = 2000

GROUP BY
    5
ORDER BY 
    "store_sales_item_items"."I_ITEM_ID" asc
LIMIT (100)
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
