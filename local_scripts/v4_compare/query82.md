# Query 82

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (2 rows) |
| reference execution | OK (2 rows) |
| results identical | YES |

## Result comparison

v4 rows: 2 (2 distinct)
ref rows: 2 (2 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 1583 | 34 | 177.62 ms |
| reference | 1583 | 34 | 182.03 ms |
| v4 / ref | 1.00x | 1.00x | 0.98x |

## Preql

```
import inventory as inventory;
import store_sales as store_sales;

merge inventory.item.id into ~store_sales.item.id;

where
    inventory.item.current_price between 62 and 92
    and inventory.date.date between '2000-05-25'::date and '2000-07-24'::date
    and inventory.item.manufacturer_id in (129, 270, 821, 423)
    and inventory.quantity_on_hand between 100 and 500
    and store_sales.item.id is not null
select
    inventory.item.name,
    inventory.item.desc,
    inventory.item.current_price,
order by
    inventory.item.name asc
limit 100
;
```

## v4 generated SQL

```sql
WITH 
wakeful as (
SELECT
    "inventory_warehouse_inventory"."inv_date_sk" as "inventory_date_id",
    "inventory_warehouse_inventory"."inv_item_sk" as "store_sales_item_id",
    "inventory_warehouse_inventory"."inv_quantity_on_hand" as "inventory_quantity_on_hand"
FROM
    "memory"."inventory" as "inventory_warehouse_inventory"
WHERE
    "inventory_warehouse_inventory"."inv_quantity_on_hand" BETWEEN 100 AND 500 and "inventory_warehouse_inventory"."inv_item_sk" is not null

GROUP BY
    1,
    2,
    3)
SELECT
    "inventory_item_items"."I_CURRENT_PRICE" as "inventory_item_current_price",
    "inventory_item_items"."I_ITEM_DESC" as "inventory_item_desc",
    "inventory_item_items"."I_ITEM_ID" as "inventory_item_name"
FROM
    "wakeful"
    INNER JOIN "memory"."date_dim" as "inventory_date_date" on "wakeful"."inventory_date_id" = "inventory_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "store_sales_item_items" on "wakeful"."store_sales_item_id" = "store_sales_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."item" as "inventory_item_items" on "store_sales_item_items"."I_ITEM_SK" = "inventory_item_items"."I_ITEM_SK"
WHERE
    "inventory_item_items"."I_CURRENT_PRICE" BETWEEN 62 AND 92 and cast("inventory_date_date"."D_DATE" as date) BETWEEN date '2000-05-25' AND date '2000-07-24' and "inventory_item_items"."I_MANUFACT_ID" in (129,270,821,423) and "wakeful"."inventory_quantity_on_hand" BETWEEN 100 AND 500 and "store_sales_item_items"."I_ITEM_SK" is not null

GROUP BY
    1,
    2,
    3
ORDER BY 
    "inventory_item_items"."I_ITEM_ID" asc
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
wakeful as (
SELECT
    "inventory_warehouse_inventory"."inv_date_sk" as "inventory_date_id",
    "inventory_warehouse_inventory"."inv_item_sk" as "store_sales_item_id",
    "inventory_warehouse_inventory"."inv_quantity_on_hand" as "inventory_quantity_on_hand"
FROM
    "memory"."inventory" as "inventory_warehouse_inventory"
WHERE
    "inventory_warehouse_inventory"."inv_quantity_on_hand" BETWEEN 100 AND 500 and "inventory_warehouse_inventory"."inv_item_sk" is not null

GROUP BY
    1,
    2,
    3)
SELECT
    "inventory_item_items"."I_ITEM_ID" as "inventory_item_name",
    "inventory_item_items"."I_ITEM_DESC" as "inventory_item_desc",
    "inventory_item_items"."I_CURRENT_PRICE" as "inventory_item_current_price"
FROM
    "wakeful"
    INNER JOIN "memory"."date_dim" as "inventory_date_date" on "wakeful"."inventory_date_id" = "inventory_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "store_sales_item_items" on "wakeful"."store_sales_item_id" = "store_sales_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."item" as "inventory_item_items" on "store_sales_item_items"."I_ITEM_SK" = "inventory_item_items"."I_ITEM_SK"
WHERE
    "inventory_item_items"."I_CURRENT_PRICE" BETWEEN 62 AND 92 and cast("inventory_date_date"."D_DATE" as date) BETWEEN date '2000-05-25' AND date '2000-07-24' and "inventory_item_items"."I_MANUFACT_ID" in (129,270,821,423) and "wakeful"."inventory_quantity_on_hand" BETWEEN 100 AND 500 and "store_sales_item_items"."I_ITEM_SK" is not null

GROUP BY
    1,
    2,
    3
ORDER BY 
    "inventory_item_items"."I_ITEM_ID" asc
LIMIT (100)
```
