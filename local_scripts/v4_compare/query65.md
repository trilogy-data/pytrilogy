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
| v4 | 3234 | 72 | 71.18 ms |
| reference | 2446 | 61 | 49.28 ms |
| v4 / ref | 1.32x | 1.18x | 1.44x |

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
    "store_sales_item_items"."I_WHOLESALE_COST" as "store_sales_item_wholesale_cost",
    "store_sales_store_sales"."SS_STORE_SK" as "store_sales_store_id",
    "store_sales_store_store"."S_STORE_NAME" as "store_sales_store_name",
    sum("store_sales_store_sales"."SS_SALES_PRICE") as "item_revenue"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "store_sales_item_items" on "store_sales_store_sales"."SS_ITEM_SK" = "store_sales_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."store" as "store_sales_store_store" on "store_sales_store_sales"."SS_STORE_SK" = "store_sales_store_store"."S_STORE_SK"
WHERE
    "store_sales_date_date"."D_MONTH_SEQ" BETWEEN 1176 AND 1187 and "store_sales_store_sales"."SS_STORE_SK" is not null

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    "store_sales_item_items"."I_ITEM_SK"),
abundant as (
SELECT
    "thoughtful"."store_sales_store_name" as "store_sales_store_name",
    avg("thoughtful"."item_revenue") as "store_avg_revenue"
FROM
    "thoughtful"
GROUP BY
    1,
    "thoughtful"."store_sales_store_id"),
questionable as (
SELECT
    "thoughtful"."item_revenue" as "revenue",
    "thoughtful"."store_sales_item_brand_name" as "store_sales_item_brand_name",
    "thoughtful"."store_sales_item_current_price" as "store_sales_item_current_price",
    "thoughtful"."store_sales_item_desc" as "store_sales_item_desc",
    "thoughtful"."store_sales_item_wholesale_cost" as "store_sales_item_wholesale_cost",
    "thoughtful"."store_sales_store_name" as "store_sales_store_name"
FROM
    "thoughtful"),
uneven as (
SELECT
    "abundant"."store_sales_store_name" as "store_sales_store_name",
    "questionable"."revenue" as "revenue",
    "questionable"."store_sales_item_brand_name" as "store_sales_item_brand_name",
    "questionable"."store_sales_item_current_price" as "store_sales_item_current_price",
    "questionable"."store_sales_item_desc" as "store_sales_item_desc",
    "questionable"."store_sales_item_wholesale_cost" as "store_sales_item_wholesale_cost"
FROM
    "abundant"
    INNER JOIN "questionable" on "abundant"."store_sales_store_name" = "questionable"."store_sales_store_name"
WHERE
    "questionable"."revenue" <= 0.1 * "abundant"."store_avg_revenue"
)
SELECT
    "uneven"."store_sales_store_name" as "store_sales_store_name",
    "uneven"."store_sales_item_desc" as "store_sales_item_desc",
    "uneven"."revenue" as "revenue",
    "uneven"."store_sales_item_current_price" as "store_sales_item_current_price",
    "uneven"."store_sales_item_wholesale_cost" as "store_sales_item_wholesale_cost",
    "uneven"."store_sales_item_brand_name" as "store_sales_item_brand_name"
FROM
    "uneven"
ORDER BY 
    "uneven"."store_sales_store_name" asc nulls first,
    "uneven"."store_sales_item_desc" asc nulls first
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
