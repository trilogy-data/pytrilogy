# Query 21

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

## SQL size

| Source | Chars | Lines |
| --- | --- | --- |
| v4 | 3168 | 83 |
| reference | 1510 | 32 |
| v4 / ref | 2.10x | 2.59x |

## Preql

```
import inventory as inventory;

where
    inventory.date.date between '2000-02-10'::date and '2000-04-10'::date
    and inventory.item.current_price between 0.99 and 1.49
select
    inventory.warehouse.name,
    inventory.item.name,
    sum(
            case
                when inventory.date.date < '2000-03-11'::date then inventory.quantity_on_hand
                else 0
            end
        ) as inv_before,
    sum(
            case
                when inventory.date.date >= '2000-03-11'::date then inventory.quantity_on_hand
                else 0
            end
        ) as inv_after,
having
    case
            when inv_before > 0 then (inv_after * 1.0) / inv_before
            else null
        end between 2.0 / 3.0 and 3.0 / 2.0

order by
    inventory.warehouse.name asc nulls first,
    inventory.item.name asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
cheerful as (
SELECT
    "inventory_warehouse_inventory"."inv_date_sk" as "inventory_date_id",
    "inventory_warehouse_inventory"."inv_item_sk" as "inventory_item_id",
    "inventory_warehouse_inventory"."inv_quantity_on_hand" as "inventory_quantity_on_hand",
    "inventory_warehouse_inventory"."inv_warehouse_sk" as "inventory_warehouse_id"
FROM
    "memory"."inventory" as "inventory_warehouse_inventory"),
wakeful as (
SELECT
    "inventory_warehouse_warehouse"."w_warehouse_name" as "inventory_warehouse_name",
    "inventory_warehouse_warehouse"."w_warehouse_sk" as "inventory_warehouse_id"
FROM
    "memory"."warehouse" as "inventory_warehouse_warehouse"),
highfalutin as (
SELECT
    "inventory_item_items"."I_CURRENT_PRICE" as "inventory_item_current_price",
    "inventory_item_items"."I_ITEM_ID" as "inventory_item_name",
    "inventory_item_items"."I_ITEM_SK" as "inventory_item_id"
FROM
    "memory"."item" as "inventory_item_items"),
quizzical as (
SELECT
    "inventory_date_date"."D_DATE_SK" as "inventory_date_id",
    cast("inventory_date_date"."D_DATE" as date) as "inventory_date_date"
FROM
    "memory"."date_dim" as "inventory_date_date"),
thoughtful as (
SELECT
    "cheerful"."inventory_quantity_on_hand" as "inventory_quantity_on_hand",
    "highfalutin"."inventory_item_current_price" as "inventory_item_current_price",
    "highfalutin"."inventory_item_name" as "inventory_item_name",
    "quizzical"."inventory_date_date" as "inventory_date_date",
    "wakeful"."inventory_warehouse_name" as "inventory_warehouse_name"
FROM
    "cheerful"
    INNER JOIN "quizzical" on "cheerful"."inventory_date_id" = "quizzical"."inventory_date_id"
    INNER JOIN "highfalutin" on "cheerful"."inventory_item_id" = "highfalutin"."inventory_item_id"
    LEFT OUTER JOIN "wakeful" on "cheerful"."inventory_warehouse_id" = "wakeful"."inventory_warehouse_id"
WHERE
    "quizzical"."inventory_date_date" BETWEEN date '2000-02-10' AND date '2000-04-10' and "highfalutin"."inventory_item_current_price" BETWEEN 0.99 AND 1.49

GROUP BY
    1,
    2,
    3,
    4,
    5),
cooperative as (
SELECT
    "thoughtful"."inventory_item_name" as "inventory_item_name",
    "thoughtful"."inventory_warehouse_name" as "inventory_warehouse_name",
    sum(CASE
	WHEN "thoughtful"."inventory_date_date" < date '2000-03-11' THEN "thoughtful"."inventory_quantity_on_hand"
	ELSE 0
	END) as "inv_before",
    sum(CASE
	WHEN "thoughtful"."inventory_date_date" >= date '2000-03-11' THEN "thoughtful"."inventory_quantity_on_hand"
	ELSE 0
	END) as "inv_after"
FROM
    "thoughtful"
GROUP BY
    1,
    2)
SELECT
    "cooperative"."inventory_warehouse_name" as "inventory_warehouse_name",
    "cooperative"."inventory_item_name" as "inventory_item_name",
    "cooperative"."inv_before" as "inv_before",
    "cooperative"."inv_after" as "inv_after"
FROM
    "cooperative"
WHERE
    CASE
	WHEN "cooperative"."inv_before" > 0 THEN ("cooperative"."inv_after" * 1.0) / "cooperative"."inv_before"
	ELSE null
	END BETWEEN 2.0 / 3.0 AND 3.0 / 2.0

ORDER BY 
    "cooperative"."inventory_warehouse_name" asc nulls first,
    "cooperative"."inventory_item_name" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
SELECT
    "inventory_warehouse_warehouse"."w_warehouse_name" as "inventory_warehouse_name",
    "inventory_item_items"."I_ITEM_ID" as "inventory_item_name",
    sum(CASE
	WHEN cast("inventory_date_date"."D_DATE" as date) < date '2000-03-11' THEN "inventory_warehouse_inventory"."inv_quantity_on_hand"
	ELSE 0
	END) as "inv_before",
    sum(CASE
	WHEN cast("inventory_date_date"."D_DATE" as date) >= date '2000-03-11' THEN "inventory_warehouse_inventory"."inv_quantity_on_hand"
	ELSE 0
	END) as "inv_after"
FROM
    "memory"."inventory" as "inventory_warehouse_inventory"
    INNER JOIN "memory"."date_dim" as "inventory_date_date" on "inventory_warehouse_inventory"."inv_date_sk" = "inventory_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "inventory_item_items" on "inventory_warehouse_inventory"."inv_item_sk" = "inventory_item_items"."I_ITEM_SK"
    LEFT OUTER JOIN "memory"."warehouse" as "inventory_warehouse_warehouse" on "inventory_warehouse_inventory"."inv_warehouse_sk" = "inventory_warehouse_warehouse"."w_warehouse_sk"
WHERE
    cast("inventory_date_date"."D_DATE" as date) BETWEEN date '2000-02-10' AND date '2000-04-10' and "inventory_item_items"."I_CURRENT_PRICE" BETWEEN 0.99 AND 1.49

GROUP BY
    1,
    2
HAVING
    CASE
	WHEN "inv_before" > 0 THEN ("inv_after" * 1.0) / "inv_before"
	ELSE null
	END BETWEEN 2.0 / 3.0 AND 3.0 / 2.0

ORDER BY 
    "inventory_warehouse_warehouse"."w_warehouse_name" asc nulls first,
    "inventory_item_items"."I_ITEM_ID" asc nulls first
LIMIT (100)
```
