# Query 37

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
| v4 | 1853 | 52 | 698.83 ms |
| reference | 1431 | 42 | 195.93 ms |
| v4 / ref | 1.29x | 1.24x | 3.57x |

## Preql

```
import item as items;
import inventory as inv;
import catalog_sales as cs;

auto inv_item_ids <- inv.item.id
    ? inv.date.date between '2000-02-01'::date and '2000-04-01'::date
and inv.quantity_on_hand between 100 and 500;

where
    items.current_price between 68 and 98
    and items.manufacturer_id in (677, 940, 694, 808)
    and items.id in inv_item_ids
select
    items.name,
    items.desc,
    items.current_price,
order by
    items.name asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
highfalutin as (
SELECT
    "inv_warehouse_inventory"."inv_date_sk" as "inv_date_id",
    "inv_warehouse_inventory"."inv_item_sk" as "inv_item_id",
    "inv_warehouse_inventory"."inv_quantity_on_hand" as "inv_quantity_on_hand"
FROM
    "memory"."inventory" as "inv_warehouse_inventory"
GROUP BY
    1,
    2,
    3),
cheerful as (
SELECT
    "highfalutin"."inv_item_id" as "inv_item_id",
    "highfalutin"."inv_quantity_on_hand" as "inv_quantity_on_hand",
    cast("inv_date_date"."D_DATE" as date) as "inv_date_date"
FROM
    "highfalutin"
    INNER JOIN "memory"."date_dim" as "inv_date_date" on "highfalutin"."inv_date_id" = "inv_date_date"."D_DATE_SK"
GROUP BY
    1,
    2,
    3),
thoughtful as (
SELECT
    CASE WHEN "cheerful"."inv_date_date" BETWEEN date '2000-02-01' AND date '2000-04-01' and "cheerful"."inv_quantity_on_hand" BETWEEN 100 AND 500 THEN "cheerful"."inv_item_id" ELSE NULL END as "inv_item_ids"
FROM
    "cheerful"),
cooperative as (
SELECT
    "items_items"."I_CURRENT_PRICE" as "items_current_price",
    "items_items"."I_ITEM_DESC" as "items_desc",
    "items_items"."I_ITEM_ID" as "items_name",
    "items_items"."I_ITEM_SK" as "items_id"
FROM
    "memory"."item" as "items_items"
WHERE
    "items_items"."I_CURRENT_PRICE" BETWEEN 68 AND 98 and "items_items"."I_MANUFACT_ID" in (677,940,694,808) and "items_items"."I_ITEM_SK" in (select thoughtful."inv_item_ids" from thoughtful where thoughtful."inv_item_ids" is not null)
)
SELECT
    "cooperative"."items_current_price" as "items_current_price",
    "cooperative"."items_desc" as "items_desc",
    "cooperative"."items_name" as "items_name"
FROM
    "cooperative"
WHERE
    "cooperative"."items_id" in (select thoughtful."inv_item_ids" from thoughtful where thoughtful."inv_item_ids" is not null)

ORDER BY 
    "cooperative"."items_name" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
wakeful as (
SELECT
    "inv_warehouse_inventory"."inv_date_sk" as "inv_date_id",
    "inv_warehouse_inventory"."inv_item_sk" as "inv_item_id",
    "inv_warehouse_inventory"."inv_quantity_on_hand" as "inv_quantity_on_hand"
FROM
    "memory"."inventory" as "inv_warehouse_inventory"
WHERE
    "inv_warehouse_inventory"."inv_quantity_on_hand" BETWEEN 100 AND 500

GROUP BY
    1,
    2,
    3),
thoughtful as (
SELECT
    CASE WHEN cast("inv_date_date"."D_DATE" as date) BETWEEN date '2000-02-01' AND date '2000-04-01' and "wakeful"."inv_quantity_on_hand" BETWEEN 100 AND 500 THEN "wakeful"."inv_item_id" ELSE NULL END as "inv_item_ids"
FROM
    "wakeful"
    INNER JOIN "memory"."date_dim" as "inv_date_date" on "wakeful"."inv_date_id" = "inv_date_date"."D_DATE_SK"
WHERE
    cast("inv_date_date"."D_DATE" as date) BETWEEN date '2000-02-01' AND date '2000-04-01'

GROUP BY
    1)
SELECT
    "items_items"."I_ITEM_ID" as "items_name",
    "items_items"."I_ITEM_DESC" as "items_desc",
    "items_items"."I_CURRENT_PRICE" as "items_current_price"
FROM
    "memory"."item" as "items_items"
WHERE
    "items_items"."I_CURRENT_PRICE" BETWEEN 68 AND 98 and "items_items"."I_MANUFACT_ID" in (677,940,694,808) and "items_items"."I_ITEM_SK" in (select thoughtful."inv_item_ids" from thoughtful where thoughtful."inv_item_ids" is not null)

GROUP BY
    1,
    2,
    3
ORDER BY 
    "items_items"."I_ITEM_ID" asc nulls first
LIMIT (100)
```
