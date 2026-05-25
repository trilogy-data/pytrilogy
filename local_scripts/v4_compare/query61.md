# Query 61

**Status:** `exec_fail`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | FAILED |
| reference execution | OK (1 rows) |

## Result comparison

_at least one side did not produce rows._

## SQL size

| Source | Chars | Lines |
| --- | --- | --- |
| v4 | 4666 | 121 |
| reference | 3444 | 59 |
| v4 / ref | 1.35x | 2.05x |

## Preql

```
import store_sales as ss;

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
    and ss.customer.address.gmt_offset = -5
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
questionable as (
SELECT
    "ss_store_sales"."SS_CUSTOMER_SK" as "ss_customer_id",
    "ss_store_sales"."SS_EXT_SALES_PRICE" as "ss_ext_sales_price",
    "ss_store_sales"."SS_ITEM_SK" as "ss_item_id",
    "ss_store_sales"."SS_PROMO_SK" as "ss_promotion_id",
    "ss_store_sales"."SS_SOLD_DATE_SK" as "ss_date_id",
    "ss_store_sales"."SS_STORE_SK" as "ss_store_id"
FROM
    "memory"."store_sales" as "ss_store_sales"),
abundant as (
SELECT
    "questionable"."ss_customer_id" as "ss_customer_id",
    "questionable"."ss_date_id" as "ss_date_id",
    "questionable"."ss_ext_sales_price" as "ss_ext_sales_price",
    "questionable"."ss_item_id" as "ss_item_id",
    "questionable"."ss_promotion_id" as "ss_promotion_id",
    "questionable"."ss_store_id" as "ss_store_id"
FROM
    "questionable"
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6),
cooperative as (
SELECT
    "ss_store_store"."S_GMT_OFFSET" as "ss_store_gmt_offset",
    "ss_store_store"."S_STORE_SK" as "ss_store_id"
FROM
    "memory"."store" as "ss_store_store"),
thoughtful as (
SELECT
    "ss_promotion_promotion"."P_CHANNEL_DMAIL" as "ss_promotion_channel_dmail",
    "ss_promotion_promotion"."P_CHANNEL_EMAIL" as "ss_promotion_channel_email",
    "ss_promotion_promotion"."P_CHANNEL_TV" as "ss_promotion_channel_tv",
    "ss_promotion_promotion"."P_PROMO_SK" as "ss_promotion_id"
FROM
    "memory"."promotion" as "ss_promotion_promotion"),
cheerful as (
SELECT
    "ss_item_items"."I_CATEGORY" as "ss_item_category",
    "ss_item_items"."I_ITEM_SK" as "ss_item_id"
FROM
    "memory"."item" as "ss_item_items"),
wakeful as (
SELECT
    "ss_date_date"."D_DATE_SK" as "ss_date_id",
    "ss_date_date"."D_MOY" as "ss_date_month_of_year",
    "ss_date_date"."D_YEAR" as "ss_date_year"
FROM
    "memory"."date_dim" as "ss_date_date"),
highfalutin as (
SELECT
    "ss_customer_customers"."C_CURRENT_ADDR_SK" as "ss_customer_address_id",
    "ss_customer_customers"."C_CUSTOMER_SK" as "ss_customer_id"
FROM
    "memory"."customer" as "ss_customer_customers"),
quizzical as (
SELECT
    "ss_customer_address_customer_address"."CA_ADDRESS_SK" as "ss_customer_address_id",
    "ss_customer_address_customer_address"."CA_GMT_OFFSET" as "ss_customer_address_gmt_offset"
FROM
    "memory"."customer_address" as "ss_customer_address_customer_address"),
uneven as (
SELECT
    "abundant"."ss_ext_sales_price" as "ss_ext_sales_price",
    "cheerful"."ss_item_category" as "ss_item_category",
    "cooperative"."ss_store_gmt_offset" as "ss_store_gmt_offset",
    "quizzical"."ss_customer_address_gmt_offset" as "ss_customer_address_gmt_offset",
    "thoughtful"."ss_promotion_channel_dmail" as "ss_promotion_channel_dmail",
    "thoughtful"."ss_promotion_channel_email" as "ss_promotion_channel_email",
    "thoughtful"."ss_promotion_channel_tv" as "ss_promotion_channel_tv",
    "wakeful"."ss_date_month_of_year" as "ss_date_month_of_year",
    "wakeful"."ss_date_year" as "ss_date_year"
FROM
    "abundant"
    LEFT OUTER JOIN "wakeful" on "abundant"."ss_date_id" = "wakeful"."ss_date_id"
    INNER JOIN "cheerful" on "abundant"."ss_item_id" = "cheerful"."ss_item_id"
    LEFT OUTER JOIN "cooperative" on "abundant"."ss_store_id" = "cooperative"."ss_store_id"
    LEFT OUTER JOIN "highfalutin" on "abundant"."ss_customer_id" = "highfalutin"."ss_customer_id"
    LEFT OUTER JOIN "thoughtful" on "abundant"."ss_promotion_id" = "thoughtful"."ss_promotion_id"
    LEFT OUTER JOIN "quizzical" on "highfalutin"."ss_customer_address_id" = "quizzical"."ss_customer_address_id"
WHERE
    "wakeful"."ss_date_year" = 1998 and "wakeful"."ss_date_month_of_year" = 11 and "cheerful"."ss_item_category" = 'Jewelry' and "quizzical"."ss_customer_address_gmt_offset" = -5 and "cooperative"."ss_store_gmt_offset" = -5

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
yummy as (
SELECT
    CASE WHEN "uneven"."ss_promotion_channel_dmail" = 'Y' or "uneven"."ss_promotion_channel_email" = 'Y' or "uneven"."ss_promotion_channel_tv" = 'Y' THEN "uneven"."ss_ext_sales_price" ELSE NULL END as "_virt_filter_ext_sales_price_1027446398664923"
FROM
    "uneven"),
juicy as (
SELECT
    sum("uneven"."ss_ext_sales_price") as "total",
    sum("yummy"."_virt_filter_ext_sales_price_1027446398664923") as "promotional_sales"
FROM
    "yummy")
SELECT
    "juicy"."promotional_sales" as "promotions",
    ( cast("juicy"."promotional_sales" as numeric(15,4)) / cast("juicy"."total" as numeric(15,4)) ) * 100 as "ratio",
    "juicy"."promotional_sales" as "promotional_sales",
    "juicy"."total" as "total"
FROM
    "juicy"
ORDER BY 
    "promotions" asc nulls first,
    "juicy"."total" asc nulls first
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
    INNER JOIN "memory"."customer" as "ss_customer_customers" on "ss_store_sales"."SS_CUSTOMER_SK" = "ss_customer_customers"."C_CUSTOMER_SK"
    LEFT OUTER JOIN "memory"."promotion" as "ss_promotion_promotion" on "ss_store_sales"."SS_PROMO_SK" = "ss_promotion_promotion"."P_PROMO_SK"
    INNER JOIN "memory"."customer_address" as "ss_customer_address_customer_address" on "ss_customer_customers"."C_CURRENT_ADDR_SK" = "ss_customer_address_customer_address"."CA_ADDRESS_SK"
WHERE
    "ss_date_date"."D_YEAR" = 1998 and "ss_date_date"."D_MOY" = 11 and "ss_item_items"."I_CATEGORY" = 'Jewelry' and "ss_customer_address_customer_address"."CA_GMT_OFFSET" = -5 and "ss_store_store"."S_GMT_OFFSET" = -5
),
vacuous as (
SELECT
    sum("ss_store_sales"."SS_EXT_SALES_PRICE") as "total"
FROM
    "memory"."store_sales" as "ss_store_sales"
    INNER JOIN "memory"."date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "ss_item_items" on "ss_store_sales"."SS_ITEM_SK" = "ss_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."store" as "ss_store_store" on "ss_store_sales"."SS_STORE_SK" = "ss_store_store"."S_STORE_SK"
    INNER JOIN "memory"."customer" as "ss_customer_customers" on "ss_store_sales"."SS_CUSTOMER_SK" = "ss_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."customer_address" as "ss_customer_address_customer_address" on "ss_customer_customers"."C_CURRENT_ADDR_SK" = "ss_customer_address_customer_address"."CA_ADDRESS_SK"
WHERE
    "ss_date_date"."D_YEAR" = 1998 and "ss_date_date"."D_MOY" = 11 and "ss_item_items"."I_CATEGORY" = 'Jewelry' and "ss_customer_address_customer_address"."CA_GMT_OFFSET" = -5 and "ss_store_store"."S_GMT_OFFSET" = -5
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

## v4 execution error

```
Traceback (most recent call last):
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 161, in run_one
    result.v4_rows = execute(con, v4_sql)
                     ~~~~~~~^^^^^^^^^^^^^
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 102, in execute
    cursor = con.execute(sql)
_duckdb.BinderException: Binder Error: Referenced table "uneven" not found!
Candidate tables: "yummy"

LINE 107:     sum("uneven"."ss_ext_sales_price") as "total",
                  ^
```
