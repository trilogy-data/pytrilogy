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
| v4 | 3095 | 72 | 48.06 ms |
| reference | 3095 | 72 | 48.48 ms |
| v4 / ref | 1.00x | 1.00x | 0.99x |

## Preql

```
import physical_sales as physical_sales;

auto item_revenue <- sum(physical_sales.sales_price) by physical_sales.store.id, physical_sales.item.id;
auto store_avg_revenue <- avg(item_revenue) by physical_sales.store.id;

where
    physical_sales.date.month_seq between 1176 and 1187 and physical_sales.store.id is not null
select
    physical_sales.store.name,
    physical_sales.item.desc,
    item_revenue as revenue,
    physical_sales.item.current_price,
    physical_sales.item.wholesale_cost,
    physical_sales.item.brand_name,
    --store_avg_revenue,
having
    revenue <= 0.1 * store_avg_revenue

order by
    physical_sales.store.name asc nulls first,
    physical_sales.item.desc asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
thoughtful as (
SELECT
    "physical_sales_item_items"."I_BRAND" as "physical_sales_item_brand_name",
    "physical_sales_item_items"."I_CURRENT_PRICE" as "physical_sales_item_current_price",
    "physical_sales_item_items"."I_ITEM_DESC" as "physical_sales_item_desc",
    "physical_sales_item_items"."I_WHOLESALE_COST" as "physical_sales_item_wholesale_cost",
    "physical_sales_store_sales"."SS_STORE_SK" as "physical_sales_store_id",
    "physical_sales_store_store"."S_STORE_NAME" as "physical_sales_store_name",
    sum("physical_sales_store_sales"."SS_SALES_PRICE") as "item_revenue"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "physical_sales_date_date" on "physical_sales_store_sales"."SS_SOLD_DATE_SK" = "physical_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "physical_sales_item_items" on "physical_sales_store_sales"."SS_ITEM_SK" = "physical_sales_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."store" as "physical_sales_store_store" on "physical_sales_store_sales"."SS_STORE_SK" = "physical_sales_store_store"."S_STORE_SK"
WHERE
    "physical_sales_date_date"."D_MONTH_SEQ" BETWEEN 1176 AND 1187 and "physical_sales_store_sales"."SS_STORE_SK" is not null

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    "physical_sales_item_items"."I_ITEM_SK"),
abundant as (
SELECT
    "thoughtful"."item_revenue" as "item_revenue",
    "thoughtful"."item_revenue" as "revenue",
    "thoughtful"."physical_sales_item_brand_name" as "physical_sales_item_brand_name",
    "thoughtful"."physical_sales_item_current_price" as "physical_sales_item_current_price",
    "thoughtful"."physical_sales_item_desc" as "physical_sales_item_desc",
    "thoughtful"."physical_sales_item_wholesale_cost" as "physical_sales_item_wholesale_cost",
    "thoughtful"."physical_sales_store_id" as "physical_sales_store_id",
    "thoughtful"."physical_sales_store_name" as "physical_sales_store_name"
FROM
    "thoughtful"),
uneven as (
SELECT
    "abundant"."physical_sales_store_id" as "physical_sales_store_id",
    avg("abundant"."item_revenue") as "store_avg_revenue"
FROM
    "abundant"
GROUP BY
    1,
    "abundant"."physical_sales_store_name")
SELECT
    "abundant"."physical_sales_store_name" as "physical_sales_store_name",
    "abundant"."physical_sales_item_desc" as "physical_sales_item_desc",
    "abundant"."revenue" as "revenue",
    "abundant"."physical_sales_item_current_price" as "physical_sales_item_current_price",
    "abundant"."physical_sales_item_wholesale_cost" as "physical_sales_item_wholesale_cost",
    "abundant"."physical_sales_item_brand_name" as "physical_sales_item_brand_name"
FROM
    "abundant"
    INNER JOIN "uneven" on "abundant"."physical_sales_store_id" = "uneven"."physical_sales_store_id"
WHERE
    "abundant"."revenue" <= 0.1 * "uneven"."store_avg_revenue"

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    "uneven"."store_avg_revenue"
ORDER BY 
    "abundant"."physical_sales_store_name" asc nulls first,
    "abundant"."physical_sales_item_desc" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
thoughtful as (
SELECT
    "physical_sales_item_items"."I_BRAND" as "physical_sales_item_brand_name",
    "physical_sales_item_items"."I_CURRENT_PRICE" as "physical_sales_item_current_price",
    "physical_sales_item_items"."I_ITEM_DESC" as "physical_sales_item_desc",
    "physical_sales_item_items"."I_WHOLESALE_COST" as "physical_sales_item_wholesale_cost",
    "physical_sales_store_sales"."SS_STORE_SK" as "physical_sales_store_id",
    "physical_sales_store_store"."S_STORE_NAME" as "physical_sales_store_name",
    sum("physical_sales_store_sales"."SS_SALES_PRICE") as "item_revenue"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "physical_sales_date_date" on "physical_sales_store_sales"."SS_SOLD_DATE_SK" = "physical_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "physical_sales_item_items" on "physical_sales_store_sales"."SS_ITEM_SK" = "physical_sales_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."store" as "physical_sales_store_store" on "physical_sales_store_sales"."SS_STORE_SK" = "physical_sales_store_store"."S_STORE_SK"
WHERE
    "physical_sales_date_date"."D_MONTH_SEQ" BETWEEN 1176 AND 1187 and "physical_sales_store_sales"."SS_STORE_SK" is not null

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    "physical_sales_item_items"."I_ITEM_SK"),
abundant as (
SELECT
    "thoughtful"."item_revenue" as "item_revenue",
    "thoughtful"."item_revenue" as "revenue",
    "thoughtful"."physical_sales_item_brand_name" as "physical_sales_item_brand_name",
    "thoughtful"."physical_sales_item_current_price" as "physical_sales_item_current_price",
    "thoughtful"."physical_sales_item_desc" as "physical_sales_item_desc",
    "thoughtful"."physical_sales_item_wholesale_cost" as "physical_sales_item_wholesale_cost",
    "thoughtful"."physical_sales_store_id" as "physical_sales_store_id",
    "thoughtful"."physical_sales_store_name" as "physical_sales_store_name"
FROM
    "thoughtful"),
uneven as (
SELECT
    "abundant"."physical_sales_store_id" as "physical_sales_store_id",
    avg("abundant"."item_revenue") as "store_avg_revenue"
FROM
    "abundant"
GROUP BY
    1,
    "abundant"."physical_sales_store_name")
SELECT
    "abundant"."physical_sales_store_name" as "physical_sales_store_name",
    "abundant"."physical_sales_item_desc" as "physical_sales_item_desc",
    "abundant"."revenue" as "revenue",
    "abundant"."physical_sales_item_current_price" as "physical_sales_item_current_price",
    "abundant"."physical_sales_item_wholesale_cost" as "physical_sales_item_wholesale_cost",
    "abundant"."physical_sales_item_brand_name" as "physical_sales_item_brand_name"
FROM
    "abundant"
    INNER JOIN "uneven" on "abundant"."physical_sales_store_id" = "uneven"."physical_sales_store_id"
WHERE
    "abundant"."revenue" <= 0.1 * "uneven"."store_avg_revenue"

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    "uneven"."store_avg_revenue"
ORDER BY 
    "abundant"."physical_sales_store_name" asc nulls first,
    "abundant"."physical_sales_item_desc" asc nulls first
LIMIT (100)
```
