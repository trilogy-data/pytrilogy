# Query 22

**Status:** `mismatch`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (100 rows) |
| reference execution | OK (100 rows) |
| results identical | NO |

## Result comparison

v4 rows: 100 (100 distinct)
ref rows: 100 (100 distinct)
only in v4 (showing up to 5 of 84):
  1x  (None, None, None, 'ationbarn station', 429.5413223140496)
  1x  ('amalgbrand #8', None, None, 'ationbarn station', 429.5413223140496)
  1x  ('amalgbrand #8', None, 'bathroom', 'ationbarn station', 429.5413223140496)
  1x  ('amalgbrand #8', 'Home', 'bathroom', 'ationbarn station', 429.5413223140496)
  1x  (None, None, None, 'oughtcallyn stantiought', 435.6144578313253)
only in ref (showing up to 5 of 84):
  1x  (None, None, None, 'ationbarn station', 430.3577235772358)
  1x  ('amalgbrand #8', None, None, 'ationbarn station', 430.3577235772358)
  1x  ('amalgbrand #8', None, 'bathroom', 'ationbarn station', 430.3577235772358)
  1x  ('amalgbrand #8', 'Home', 'bathroom', 'ationbarn station', 430.3577235772358)
  1x  (None, None, None, 'ationoughtn stn st', 435.26506024096386)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 1879 | 50 | 1.119 s |
| reference | 1512 | 32 | 291.02 ms |
| v4 / ref | 1.24x | 1.56x | 3.85x |

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
wakeful as (
SELECT
    "inventory_warehouse_inventory"."inv_date_sk" as "inventory_date_id",
    "inventory_warehouse_inventory"."inv_item_sk" as "inventory_item_id",
    "inventory_warehouse_inventory"."inv_quantity_on_hand" as "inventory_quantity_on_hand"
FROM
    "memory"."inventory" as "inventory_warehouse_inventory"
GROUP BY
    1,
    2,
    3),
thoughtful as (
SELECT
    "inventory_item_items"."I_BRAND" as "inventory_item_brand_name",
    "inventory_item_items"."I_CATEGORY" as "inventory_item_category",
    "inventory_item_items"."I_CLASS" as "inventory_item_class",
    "inventory_item_items"."I_PRODUCT_NAME" as "inventory_item_product_name",
    "wakeful"."inventory_quantity_on_hand" as "inventory_quantity_on_hand"
FROM
    "wakeful"
    INNER JOIN "memory"."date_dim" as "inventory_date_date" on "wakeful"."inventory_date_id" = "inventory_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "inventory_item_items" on "wakeful"."inventory_item_id" = "inventory_item_items"."I_ITEM_SK"
WHERE
    "inventory_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1211

GROUP BY
    1,
    2,
    3,
    4,
    5,
    "inventory_date_date"."D_MONTH_SEQ")
SELECT
    avg("thoughtful"."inventory_quantity_on_hand") as "qoh",
    "thoughtful"."inventory_item_brand_name" as "inventory_item_brand_name",
    "thoughtful"."inventory_item_product_name" as "inventory_item_product_name",
    "thoughtful"."inventory_item_class" as "inventory_item_class",
    "thoughtful"."inventory_item_category" as "inventory_item_category"
FROM
    "thoughtful"
GROUP BY
    ROLLUP (3, 2, 4, 5)
ORDER BY 
    "qoh" asc nulls first,
    "thoughtful"."inventory_item_product_name" asc nulls first,
    "thoughtful"."inventory_item_brand_name" asc nulls first,
    "thoughtful"."inventory_item_class" asc nulls first,
    "thoughtful"."inventory_item_category" asc nulls first
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
