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

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 1394 | 20 | 40.48 ms |
| reference | 1394 | 20 | 47.14 ms |
| v4 / ref | 1.00x | 1.00x | 0.86x |

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
