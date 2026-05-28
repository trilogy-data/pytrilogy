# Query 27

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
| v4 | 3065 | 63 | 202.87 ms |
| reference | 2277 | 34 | 48.68 ms |
| v4 / ref | 1.35x | 1.85x | 4.17x |

## Preql

```
import store_sales as store_sales;

where
    store_sales.customer_demographic.gender = 'M'
    and store_sales.customer_demographic.marital_status = 'S'
    and store_sales.customer_demographic.education_status = 'College'
    and store_sales.date.year = 2002
    and store_sales.store.state = 'TN'
select
    store_sales.item.name,
    store_sales.store.state,
    grouping(store_sales.store.state) by rollup store_sales.item.name, store_sales.store.state as g_state,
    avg(store_sales.quantity::numeric(12,2))
            by rollup store_sales.item.name, store_sales.store.state as agg1,
    avg(store_sales.list_price::numeric(12,2))
            by rollup store_sales.item.name, store_sales.store.state as agg2,
    avg(store_sales.coupon_amt::numeric(12,2))
            by rollup store_sales.item.name, store_sales.store.state as agg3,
    avg(store_sales.sales_price::numeric(12,2))
            by rollup store_sales.item.name, store_sales.store.state as agg4,
order by
    store_sales.item.name asc nulls first,
    store_sales.store.state asc nulls first
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
    "store_sales_store_sales"."SS_QUANTITY" as "store_sales_quantity",
    "store_sales_store_sales"."SS_SALES_PRICE" as "store_sales_sales_price",
    "store_sales_store_sales"."SS_SOLD_DATE_SK" as "store_sales_date_id",
    "store_sales_store_sales"."SS_STORE_SK" as "store_sales_store_id"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8),
questionable as (
SELECT
    "store_sales_item_items"."I_ITEM_ID" as "store_sales_item_name",
    "store_sales_store_store"."S_STATE" as "store_sales_store_state",
    "thoughtful"."store_sales_coupon_amt" as "store_sales_coupon_amt",
    "thoughtful"."store_sales_list_price" as "store_sales_list_price",
    "thoughtful"."store_sales_quantity" as "store_sales_quantity",
    "thoughtful"."store_sales_sales_price" as "store_sales_sales_price"
FROM
    "thoughtful"
    INNER JOIN "memory"."date_dim" as "store_sales_date_date" on "thoughtful"."store_sales_date_id" = "store_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "store_sales_item_items" on "thoughtful"."store_sales_item_id" = "store_sales_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."store" as "store_sales_store_store" on "thoughtful"."store_sales_store_id" = "store_sales_store_store"."S_STORE_SK"
    INNER JOIN "memory"."customer_demographics" as "store_sales_customer_demographic_customer_demographics" on "thoughtful"."store_sales_customer_demographic_id" = "store_sales_customer_demographic_customer_demographics"."CD_DEMO_SK"
WHERE
    "store_sales_customer_demographic_customer_demographics"."CD_GENDER" = 'M' and "store_sales_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" = 'S' and "store_sales_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS" = 'College' and "store_sales_date_date"."D_YEAR" = 2002 and "store_sales_store_store"."S_STATE" = 'TN'

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    "thoughtful"."store_sales_store_id")
SELECT
    avg(cast("questionable"."store_sales_quantity" as numeric(12,2))) as "agg1",
    avg(cast("questionable"."store_sales_list_price" as numeric(12,2))) as "agg2",
    avg(cast("questionable"."store_sales_coupon_amt" as numeric(12,2))) as "agg3",
    avg(cast("questionable"."store_sales_sales_price" as numeric(12,2))) as "agg4",
    grouping("questionable"."store_sales_store_state") as "g_state",
    "questionable"."store_sales_item_name" as "store_sales_item_name",
    "questionable"."store_sales_store_state" as "store_sales_store_state"
FROM
    "questionable"
GROUP BY
    ROLLUP (6, 7)
ORDER BY 
    "questionable"."store_sales_item_name" asc nulls first,
    "questionable"."store_sales_store_state" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
cooperative as (
SELECT
    "store_sales_item_items"."I_ITEM_ID" as "store_sales_item_name",
    "store_sales_store_sales"."SS_COUPON_AMT" as "store_sales_coupon_amt",
    "store_sales_store_sales"."SS_LIST_PRICE" as "store_sales_list_price",
    "store_sales_store_sales"."SS_QUANTITY" as "store_sales_quantity",
    "store_sales_store_sales"."SS_SALES_PRICE" as "store_sales_sales_price",
    "store_sales_store_store"."S_STATE" as "store_sales_store_state"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "store_sales_item_items" on "store_sales_store_sales"."SS_ITEM_SK" = "store_sales_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."store" as "store_sales_store_store" on "store_sales_store_sales"."SS_STORE_SK" = "store_sales_store_store"."S_STORE_SK"
    INNER JOIN "memory"."customer_demographics" as "store_sales_customer_demographic_customer_demographics" on "store_sales_store_sales"."SS_CDEMO_SK" = "store_sales_customer_demographic_customer_demographics"."CD_DEMO_SK"
WHERE
    "store_sales_customer_demographic_customer_demographics"."CD_GENDER" = 'M' and "store_sales_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" = 'S' and "store_sales_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS" = 'College' and "store_sales_date_date"."D_YEAR" = 2002 and "store_sales_store_store"."S_STATE" = 'TN'
)
SELECT
    "cooperative"."store_sales_item_name" as "store_sales_item_name",
    "cooperative"."store_sales_store_state" as "store_sales_store_state",
    grouping("cooperative"."store_sales_store_state") as "g_state",
    avg(cast("cooperative"."store_sales_quantity" as numeric(12,2))) as "agg1",
    avg(cast("cooperative"."store_sales_list_price" as numeric(12,2))) as "agg2",
    avg(cast("cooperative"."store_sales_coupon_amt" as numeric(12,2))) as "agg3",
    avg(cast("cooperative"."store_sales_sales_price" as numeric(12,2))) as "agg4"
FROM
    "cooperative"
GROUP BY
    ROLLUP (1, 2)
ORDER BY 
    "cooperative"."store_sales_item_name" asc nulls first,
    "cooperative"."store_sales_store_state" asc nulls first
LIMIT (100)
```
