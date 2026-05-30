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

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 1510 | 32 | 20.82 ms |
| reference | 1510 | 32 | 21.24 ms |
| v4 / ref | 1.00x | 1.00x | 0.98x |

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
