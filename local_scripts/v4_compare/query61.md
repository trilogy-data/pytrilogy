# Query 61

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (1 rows) |
| reference execution | OK (1 rows) |
| results identical | YES |

## Result comparison

v4 rows: 1 (1 distinct)
ref rows: 1 (1 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 1815 | 26 | 18.00 ms |
| reference | 3540 | 59 | 39.65 ms |
| v4 / ref | 0.51x | 0.44x | 0.45x |

## Preql

```
import physical_sales as ss;

auto promotional_sales <- sum(
    ss.ext_sales_price
        ? ss.promotion.channel_dmail = 'Y'
or ss.promotion.channel_email = 'Y'
or ss.promotion.channel_tv = 'Y'
);

where
    ss.date.year = 1998
    and ss.date.month_of_year = 11
    and ss.item.category = 'Jewelry'
    and ss.billing_customer.address.gmt_offset = -5
    and ss.store.gmt_offset = -5
select
    promotional_sales as promotions,
    sum(ss.ext_sales_price) as total,
    promotional_sales::numeric(15,4) / total::numeric(15,4) * 100 as ratio,
order by
    promotions asc nulls first,
    total asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
abundant as (
SELECT
    sum("ss_store_sales"."SS_EXT_SALES_PRICE") as "total",
    sum(CASE WHEN "ss_promotion_promotion"."P_CHANNEL_DMAIL" = 'Y' or "ss_promotion_promotion"."P_CHANNEL_EMAIL" = 'Y' or "ss_promotion_promotion"."P_CHANNEL_TV" = 'Y' THEN "ss_store_sales"."SS_EXT_SALES_PRICE" ELSE NULL END) as "promotional_sales"
FROM
    "memory"."store_sales" as "ss_store_sales"
    INNER JOIN "memory"."date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "ss_item_items" on "ss_store_sales"."SS_ITEM_SK" = "ss_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."store" as "ss_store_store" on "ss_store_sales"."SS_STORE_SK" = "ss_store_store"."S_STORE_SK"
    LEFT OUTER JOIN "memory"."promotion" as "ss_promotion_promotion" on "ss_store_sales"."SS_PROMO_SK" = "ss_promotion_promotion"."P_PROMO_SK"
    INNER JOIN "memory"."customer" as "ss_billing_customer_customers" on "ss_store_sales"."SS_CUSTOMER_SK" = "ss_billing_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."customer_address" as "ss_billing_customer_address_customer_address" on "ss_billing_customer_customers"."C_CURRENT_ADDR_SK" = "ss_billing_customer_address_customer_address"."CA_ADDRESS_SK"
WHERE
    "ss_date_date"."D_YEAR" = 1998 and "ss_date_date"."D_MOY" = 11 and "ss_item_items"."I_CATEGORY" = 'Jewelry' and "ss_billing_customer_address_customer_address"."CA_GMT_OFFSET" = -5 and "ss_store_store"."S_GMT_OFFSET" = -5
)
SELECT
    "abundant"."promotional_sales" as "promotions",
    ( cast("abundant"."promotional_sales" as numeric(15,4)) / cast("abundant"."total" as numeric(15,4)) ) * 100 as "ratio",
    "abundant"."total" as "total"
FROM
    "abundant"
ORDER BY 
    "promotions" asc nulls first,
    "abundant"."total" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
abundant as (
SELECT
    "ss_promotion_promotion"."P_CHANNEL_DMAIL" as "ss_promotion_channel_dmail",
    "ss_promotion_promotion"."P_CHANNEL_EMAIL" as "ss_promotion_channel_email",
    "ss_promotion_promotion"."P_CHANNEL_TV" as "ss_promotion_channel_tv",
    "ss_store_sales"."SS_EXT_SALES_PRICE" as "ss_ext_sales_price"
FROM
    "memory"."store_sales" as "ss_store_sales"
    INNER JOIN "memory"."date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "ss_item_items" on "ss_store_sales"."SS_ITEM_SK" = "ss_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."store" as "ss_store_store" on "ss_store_sales"."SS_STORE_SK" = "ss_store_store"."S_STORE_SK"
    LEFT OUTER JOIN "memory"."promotion" as "ss_promotion_promotion" on "ss_store_sales"."SS_PROMO_SK" = "ss_promotion_promotion"."P_PROMO_SK"
    INNER JOIN "memory"."customer" as "ss_billing_customer_customers" on "ss_store_sales"."SS_CUSTOMER_SK" = "ss_billing_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."customer_address" as "ss_billing_customer_address_customer_address" on "ss_billing_customer_customers"."C_CURRENT_ADDR_SK" = "ss_billing_customer_address_customer_address"."CA_ADDRESS_SK"
WHERE
    "ss_date_date"."D_YEAR" = 1998 and "ss_date_date"."D_MOY" = 11 and "ss_item_items"."I_CATEGORY" = 'Jewelry' and "ss_billing_customer_address_customer_address"."CA_GMT_OFFSET" = -5 and "ss_store_store"."S_GMT_OFFSET" = -5
),
vacuous as (
SELECT
    sum("ss_store_sales"."SS_EXT_SALES_PRICE") as "total"
FROM
    "memory"."store_sales" as "ss_store_sales"
    INNER JOIN "memory"."date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "ss_item_items" on "ss_store_sales"."SS_ITEM_SK" = "ss_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."store" as "ss_store_store" on "ss_store_sales"."SS_STORE_SK" = "ss_store_store"."S_STORE_SK"
    INNER JOIN "memory"."customer" as "ss_billing_customer_customers" on "ss_store_sales"."SS_CUSTOMER_SK" = "ss_billing_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."customer_address" as "ss_billing_customer_address_customer_address" on "ss_billing_customer_customers"."C_CURRENT_ADDR_SK" = "ss_billing_customer_address_customer_address"."CA_ADDRESS_SK"
WHERE
    "ss_date_date"."D_YEAR" = 1998 and "ss_date_date"."D_MOY" = 11 and "ss_item_items"."I_CATEGORY" = 'Jewelry' and "ss_billing_customer_address_customer_address"."CA_GMT_OFFSET" = -5 and "ss_store_store"."S_GMT_OFFSET" = -5
),
uneven as (
SELECT
    "abundant"."ss_ext_sales_price" as "ss_ext_sales_price",
    CASE WHEN "abundant"."ss_promotion_channel_dmail" = 'Y' or "abundant"."ss_promotion_channel_email" = 'Y' or "abundant"."ss_promotion_channel_tv" = 'Y' THEN "abundant"."ss_ext_sales_price" ELSE NULL END as "_virt_filter_ext_sales_price_1027446398664923"
FROM
    "abundant"),
juicy as (
SELECT
    ( cast(sum("uneven"."_virt_filter_ext_sales_price_1027446398664923") as numeric(15,4)) / cast(sum("uneven"."ss_ext_sales_price") as numeric(15,4)) ) * 100 as "ratio"
FROM
    "uneven"),
yummy as (
SELECT
    sum("uneven"."_virt_filter_ext_sales_price_1027446398664923") as "promotions"
FROM
    "uneven")
SELECT
    "yummy"."promotions" as "promotions",
    "vacuous"."total" as "total",
    "juicy"."ratio" as "ratio"
FROM
    "yummy"
    FULL JOIN "juicy" on 1=1
    FULL JOIN "vacuous" on 1=1
ORDER BY 
    "yummy"."promotions" asc nulls first,
    "vacuous"."total" asc nulls first
LIMIT (100)
```
