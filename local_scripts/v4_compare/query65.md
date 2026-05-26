# Query 65

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
| v4 | 4014 | 97 | 161.63 ms |
| reference | 2446 | 61 | 53.20 ms |
| v4 / ref | 1.64x | 1.59x | 3.04x |

## Preql

```
import store_sales as store_sales;

auto item_revenue <- sum(store_sales.sales_price) by store_sales.store.id, store_sales.item.id;
auto store_avg_revenue <- avg(item_revenue) by store_sales.store.id;

where
    store_sales.date.month_seq between 1176 and 1187 and store_sales.store.id is not null
select
    store_sales.store.name,
    store_sales.item.desc,
    item_revenue as revenue,
    store_sales.item.current_price,
    store_sales.item.wholesale_cost,
    store_sales.item.brand_name,
    --store_avg_revenue,
having
    revenue <= 0.1 * store_avg_revenue

order by
    store_sales.store.name asc nulls first,
    store_sales.item.desc asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
thoughtful as (
SELECT
    "store_sales_item_items"."I_BRAND" as "store_sales_item_brand_name",
    "store_sales_item_items"."I_CURRENT_PRICE" as "store_sales_item_current_price",
    "store_sales_item_items"."I_ITEM_DESC" as "store_sales_item_desc",
    "store_sales_item_items"."I_ITEM_SK" as "store_sales_item_id",
    "store_sales_item_items"."I_WHOLESALE_COST" as "store_sales_item_wholesale_cost",
    "store_sales_store_sales"."SS_SALES_PRICE" as "store_sales_sales_price",
    "store_sales_store_sales"."SS_STORE_SK" as "store_sales_store_id",
    "store_sales_store_store"."S_STORE_NAME" as "store_sales_store_name"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "store_sales_item_items" on "store_sales_store_sales"."SS_ITEM_SK" = "store_sales_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."store" as "store_sales_store_store" on "store_sales_store_sales"."SS_STORE_SK" = "store_sales_store_store"."S_STORE_SK"
WHERE
    "store_sales_date_date"."D_MONTH_SEQ" BETWEEN 1176 AND 1187 and "store_sales_store_sales"."SS_STORE_SK" is not null
),
yummy as (
SELECT
    "thoughtful"."store_sales_store_id" as "store_sales_store_id",
    "thoughtful"."store_sales_store_name" as "store_sales_store_name"
FROM
    "thoughtful"
GROUP BY
    1,
    2),
questionable as (
SELECT
    "thoughtful"."store_sales_item_id" as "store_sales_item_id",
    "thoughtful"."store_sales_store_id" as "store_sales_store_id",
    sum("thoughtful"."store_sales_sales_price") as "item_revenue"
FROM
    "thoughtful"
GROUP BY
    1,
    2),
cooperative as (
SELECT
    "thoughtful"."store_sales_item_brand_name" as "store_sales_item_brand_name",
    "thoughtful"."store_sales_item_current_price" as "store_sales_item_current_price",
    "thoughtful"."store_sales_item_desc" as "store_sales_item_desc",
    "thoughtful"."store_sales_item_id" as "store_sales_item_id",
    "thoughtful"."store_sales_item_wholesale_cost" as "store_sales_item_wholesale_cost"
FROM
    "thoughtful"
GROUP BY
    1,
    2,
    3,
    4,
    5),
uneven as (
SELECT
    "questionable"."store_sales_store_id" as "store_sales_store_id",
    avg("questionable"."item_revenue") as "store_avg_revenue"
FROM
    "questionable"
GROUP BY
    1),
abundant as (
SELECT
    "questionable"."item_revenue" as "revenue",
    "questionable"."store_sales_item_id" as "store_sales_item_id",
    "questionable"."store_sales_store_id" as "store_sales_store_id"
FROM
    "questionable"),
juicy as (
SELECT
    "abundant"."revenue" as "revenue",
    "cooperative"."store_sales_item_brand_name" as "store_sales_item_brand_name",
    "cooperative"."store_sales_item_current_price" as "store_sales_item_current_price",
    "cooperative"."store_sales_item_desc" as "store_sales_item_desc",
    "cooperative"."store_sales_item_wholesale_cost" as "store_sales_item_wholesale_cost",
    "yummy"."store_sales_store_name" as "store_sales_store_name"
FROM
    "cooperative"
    INNER JOIN "abundant" on "cooperative"."store_sales_item_id" = "abundant"."store_sales_item_id"
    INNER JOIN "yummy" on "abundant"."store_sales_store_id" = "yummy"."store_sales_store_id"
    INNER JOIN "uneven" on "yummy"."store_sales_store_id" = "uneven"."store_sales_store_id"
WHERE
    "abundant"."revenue" <= 0.1 * "uneven"."store_avg_revenue"
)
SELECT
    "juicy"."store_sales_store_name" as "store_sales_store_name",
    "juicy"."store_sales_item_desc" as "store_sales_item_desc",
    "juicy"."revenue" as "revenue",
    "juicy"."store_sales_item_current_price" as "store_sales_item_current_price",
    "juicy"."store_sales_item_wholesale_cost" as "store_sales_item_wholesale_cost",
    "juicy"."store_sales_item_brand_name" as "store_sales_item_brand_name"
FROM
    "juicy"
ORDER BY 
    "juicy"."store_sales_store_name" asc nulls first,
    "juicy"."store_sales_item_desc" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
wakeful as (
SELECT
    "store_sales_store_sales"."SS_ITEM_SK" as "store_sales_item_id",
    "store_sales_store_sales"."SS_STORE_SK" as "store_sales_store_id",
    sum("store_sales_store_sales"."SS_SALES_PRICE") as "item_revenue"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
WHERE
    "store_sales_date_date"."D_MONTH_SEQ" BETWEEN 1176 AND 1187 and "store_sales_store_sales"."SS_STORE_SK" is not null

GROUP BY
    1,
    2),
abundant as (
SELECT
    "store_sales_item_items"."I_BRAND" as "store_sales_item_brand_name",
    "store_sales_item_items"."I_CURRENT_PRICE" as "store_sales_item_current_price",
    "store_sales_item_items"."I_ITEM_DESC" as "store_sales_item_desc",
    "store_sales_item_items"."I_WHOLESALE_COST" as "store_sales_item_wholesale_cost",
    "store_sales_store_store"."S_STORE_NAME" as "store_sales_store_name",
    "wakeful"."item_revenue" as "item_revenue",
    "wakeful"."store_sales_store_id" as "store_sales_store_id"
FROM
    "wakeful"
    INNER JOIN "memory"."item" as "store_sales_item_items" on "wakeful"."store_sales_item_id" = "store_sales_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."store" as "store_sales_store_store" on "wakeful"."store_sales_store_id" = "store_sales_store_store"."S_STORE_SK"),
thoughtful as (
SELECT
    "wakeful"."store_sales_store_id" as "store_sales_store_id",
    avg("wakeful"."item_revenue") as "store_avg_revenue"
FROM
    "wakeful"
GROUP BY
    1)
SELECT
    "abundant"."store_sales_store_name" as "store_sales_store_name",
    "abundant"."store_sales_item_desc" as "store_sales_item_desc",
    "abundant"."item_revenue" as "revenue",
    "abundant"."store_sales_item_current_price" as "store_sales_item_current_price",
    "abundant"."store_sales_item_wholesale_cost" as "store_sales_item_wholesale_cost",
    "abundant"."store_sales_item_brand_name" as "store_sales_item_brand_name"
FROM
    "abundant"
    INNER JOIN "thoughtful" on "abundant"."store_sales_store_id" = "thoughtful"."store_sales_store_id"
WHERE
    "abundant"."item_revenue" <= 0.1 * "thoughtful"."store_avg_revenue"

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    "thoughtful"."store_avg_revenue"
ORDER BY 
    "abundant"."store_sales_store_name" asc nulls first,
    "abundant"."store_sales_item_desc" asc nulls first
LIMIT (100)
```
