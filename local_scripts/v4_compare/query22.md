# Query 22

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
| v4 | 2377 | 50 | 9.05 ms |
| reference | 1512 | 32 | 9.24 ms |
| v4 / ref | 1.57x | 1.56x | 0.98x |

## Preql

```
import inventory as inventory;

where
    inventory.date.month_seq between 1200 and 1211
select
    inventory.item.product_name,
    inventory.item.brand_name,
    inventory.item.class,
    inventory.item.category,
    avg(inventory.quantity_on_hand)
            by rollup inventory.item.product_name, inventory.item.brand_name, inventory.item.class, inventory.item.category as qoh,
order by
    qoh asc nulls first,
    inventory.item.product_name asc nulls first,
    inventory.item.brand_name asc nulls first,
    inventory.item.class asc nulls first,
    inventory.item.category asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
cheerful as (
SELECT
    "inventory_item_items"."I_BRAND" as "inventory_item_brand_name",
    "inventory_item_items"."I_CATEGORY" as "inventory_item_category",
    "inventory_item_items"."I_CLASS" as "inventory_item_class",
    "inventory_item_items"."I_PRODUCT_NAME" as "inventory_item_product_name",
    "inventory_warehouse_inventory"."inv_quantity_on_hand" as "inventory_quantity_on_hand"
FROM
    "memory"."inventory" as "inventory_warehouse_inventory"
    INNER JOIN "memory"."date_dim" as "inventory_date_date" on "inventory_warehouse_inventory"."inv_date_sk" = "inventory_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "inventory_item_items" on "inventory_warehouse_inventory"."inv_item_sk" = "inventory_item_items"."I_ITEM_SK"
WHERE
    "inventory_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1211
),
thoughtful as (
SELECT
    "cheerful"."inventory_item_brand_name" as "inventory_item_brand_name",
    "cheerful"."inventory_item_category" as "inventory_item_category",
    "cheerful"."inventory_item_class" as "inventory_item_class",
    "cheerful"."inventory_item_product_name" as "inventory_item_product_name",
    "cheerful"."inventory_quantity_on_hand" as "inventory_quantity_on_hand"
FROM
    "cheerful"),
cooperative as (
SELECT
    "thoughtful"."inventory_item_brand_name" as "inventory_item_brand_name",
    "thoughtful"."inventory_item_category" as "inventory_item_category",
    "thoughtful"."inventory_item_class" as "inventory_item_class",
    "thoughtful"."inventory_item_product_name" as "inventory_item_product_name",
    "thoughtful"."inventory_quantity_on_hand" as "inventory_quantity_on_hand"
FROM
    "thoughtful")
SELECT
    "cooperative"."inventory_item_product_name" as "inventory_item_product_name",
    "cooperative"."inventory_item_brand_name" as "inventory_item_brand_name",
    "cooperative"."inventory_item_class" as "inventory_item_class",
    "cooperative"."inventory_item_category" as "inventory_item_category",
    avg("cooperative"."inventory_quantity_on_hand") as "qoh"
FROM
    "cooperative"
GROUP BY
    ROLLUP (1, 2, 3, 4)
ORDER BY 
    "qoh" asc nulls first,
    "cooperative"."inventory_item_product_name" asc nulls first,
    "cooperative"."inventory_item_brand_name" asc nulls first,
    "cooperative"."inventory_item_class" asc nulls first,
    "cooperative"."inventory_item_category" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
cheerful as (
SELECT
    "inventory_item_items"."I_BRAND" as "inventory_item_brand_name",
    "inventory_item_items"."I_CATEGORY" as "inventory_item_category",
    "inventory_item_items"."I_CLASS" as "inventory_item_class",
    "inventory_item_items"."I_PRODUCT_NAME" as "inventory_item_product_name",
    "inventory_warehouse_inventory"."inv_quantity_on_hand" as "inventory_quantity_on_hand"
FROM
    "memory"."inventory" as "inventory_warehouse_inventory"
    INNER JOIN "memory"."date_dim" as "inventory_date_date" on "inventory_warehouse_inventory"."inv_date_sk" = "inventory_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "inventory_item_items" on "inventory_warehouse_inventory"."inv_item_sk" = "inventory_item_items"."I_ITEM_SK"
WHERE
    "inventory_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1211
)
SELECT
    "cheerful"."inventory_item_product_name" as "inventory_item_product_name",
    "cheerful"."inventory_item_brand_name" as "inventory_item_brand_name",
    "cheerful"."inventory_item_class" as "inventory_item_class",
    "cheerful"."inventory_item_category" as "inventory_item_category",
    avg("cheerful"."inventory_quantity_on_hand") as "qoh"
FROM
    "cheerful"
GROUP BY
    ROLLUP (1, 2, 3, 4)
ORDER BY 
    "qoh" asc nulls first,
    "cheerful"."inventory_item_product_name" asc nulls first,
    "cheerful"."inventory_item_brand_name" asc nulls first,
    "cheerful"."inventory_item_class" asc nulls first,
    "cheerful"."inventory_item_category" asc nulls first
LIMIT (100)
```
