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
| v4 | 2510 | 42 | 75.00 ms |
| reference | 2510 | 42 | 73.29 ms |
| v4 / ref | 1.00x | 1.00x | 1.02x |

## Preql

```
import physical_sales as physical_sales;

where
    physical_sales.customer_demographic.gender = 'M'
    and physical_sales.customer_demographic.marital_status = 'S'
    and physical_sales.customer_demographic.education_status = 'College'
    and physical_sales.date.year = 2002
    and physical_sales.store.state = 'TN'
select
    physical_sales.item.text_id,
    physical_sales.store.state,
    grouping(physical_sales.store.state) by rollup physical_sales.item.text_id, physical_sales.store.state as g_state,
    avg(physical_sales.quantity::numeric(12,2))
            by rollup physical_sales.item.text_id, physical_sales.store.state as agg1,
    avg(physical_sales.list_price::numeric(12,2))
            by rollup physical_sales.item.text_id, physical_sales.store.state as agg2,
    avg(physical_sales.coupon_amt::numeric(12,2))
            by rollup physical_sales.item.text_id, physical_sales.store.state as agg3,
    avg(physical_sales.sales_price::numeric(12,2))
            by rollup physical_sales.item.text_id, physical_sales.store.state as agg4,
order by
    physical_sales.item.text_id asc nulls first,
    physical_sales.store.state asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
cooperative as (
SELECT
    "physical_sales_item_items"."I_ITEM_ID" as "physical_sales_item_text_id",
    "physical_sales_store_sales"."SS_COUPON_AMT" as "physical_sales_coupon_amt",
    "physical_sales_store_sales"."SS_LIST_PRICE" as "physical_sales_list_price",
    "physical_sales_store_sales"."SS_QUANTITY" as "physical_sales_quantity",
    "physical_sales_store_sales"."SS_SALES_PRICE" as "physical_sales_sales_price",
    "physical_sales_store_store"."S_STATE" as "physical_sales_store_state"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "physical_sales_date_date" on "physical_sales_store_sales"."SS_SOLD_DATE_SK" = "physical_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "physical_sales_item_items" on "physical_sales_store_sales"."SS_ITEM_SK" = "physical_sales_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."store" as "physical_sales_store_store" on "physical_sales_store_sales"."SS_STORE_SK" = "physical_sales_store_store"."S_STORE_SK"
    INNER JOIN "memory"."customer_demographics" as "physical_sales_customer_demographic_customer_demographics" on "physical_sales_store_sales"."SS_CDEMO_SK" = "physical_sales_customer_demographic_customer_demographics"."CD_DEMO_SK"
WHERE
    "physical_sales_customer_demographic_customer_demographics"."CD_GENDER" = 'M' and "physical_sales_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" = 'S' and "physical_sales_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS" = 'College' and "physical_sales_date_date"."D_YEAR" = 2002 and "physical_sales_store_store"."S_STATE" = 'TN'

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    "physical_sales_store_sales"."SS_STORE_SK")
SELECT
    "cooperative"."physical_sales_item_text_id" as "physical_sales_item_text_id",
    "cooperative"."physical_sales_store_state" as "physical_sales_store_state",
    grouping("cooperative"."physical_sales_store_state") as "g_state",
    avg(cast("cooperative"."physical_sales_quantity" as numeric(12,2))) as "agg1",
    avg(cast("cooperative"."physical_sales_list_price" as numeric(12,2))) as "agg2",
    avg(cast("cooperative"."physical_sales_coupon_amt" as numeric(12,2))) as "agg3",
    avg(cast("cooperative"."physical_sales_sales_price" as numeric(12,2))) as "agg4"
FROM
    "cooperative"
GROUP BY
    ROLLUP (1, 2)
ORDER BY 
    "cooperative"."physical_sales_item_text_id" asc nulls first,
    "cooperative"."physical_sales_store_state" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
cooperative as (
SELECT
    "physical_sales_item_items"."I_ITEM_ID" as "physical_sales_item_text_id",
    "physical_sales_store_sales"."SS_COUPON_AMT" as "physical_sales_coupon_amt",
    "physical_sales_store_sales"."SS_LIST_PRICE" as "physical_sales_list_price",
    "physical_sales_store_sales"."SS_QUANTITY" as "physical_sales_quantity",
    "physical_sales_store_sales"."SS_SALES_PRICE" as "physical_sales_sales_price",
    "physical_sales_store_store"."S_STATE" as "physical_sales_store_state"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "physical_sales_date_date" on "physical_sales_store_sales"."SS_SOLD_DATE_SK" = "physical_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "physical_sales_item_items" on "physical_sales_store_sales"."SS_ITEM_SK" = "physical_sales_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."store" as "physical_sales_store_store" on "physical_sales_store_sales"."SS_STORE_SK" = "physical_sales_store_store"."S_STORE_SK"
    INNER JOIN "memory"."customer_demographics" as "physical_sales_customer_demographic_customer_demographics" on "physical_sales_store_sales"."SS_CDEMO_SK" = "physical_sales_customer_demographic_customer_demographics"."CD_DEMO_SK"
WHERE
    "physical_sales_customer_demographic_customer_demographics"."CD_GENDER" = 'M' and "physical_sales_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" = 'S' and "physical_sales_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS" = 'College' and "physical_sales_date_date"."D_YEAR" = 2002 and "physical_sales_store_store"."S_STATE" = 'TN'

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    "physical_sales_store_sales"."SS_STORE_SK")
SELECT
    "cooperative"."physical_sales_item_text_id" as "physical_sales_item_text_id",
    "cooperative"."physical_sales_store_state" as "physical_sales_store_state",
    grouping("cooperative"."physical_sales_store_state") as "g_state",
    avg(cast("cooperative"."physical_sales_quantity" as numeric(12,2))) as "agg1",
    avg(cast("cooperative"."physical_sales_list_price" as numeric(12,2))) as "agg2",
    avg(cast("cooperative"."physical_sales_coupon_amt" as numeric(12,2))) as "agg3",
    avg(cast("cooperative"."physical_sales_sales_price" as numeric(12,2))) as "agg4"
FROM
    "cooperative"
GROUP BY
    ROLLUP (1, 2)
ORDER BY 
    "cooperative"."physical_sales_item_text_id" asc nulls first,
    "cooperative"."physical_sales_store_state" asc nulls first
LIMIT (100)
```
