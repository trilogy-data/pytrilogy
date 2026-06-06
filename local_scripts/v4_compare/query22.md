# Query 22

**Status:** `gen_fail`

| Stage | Result |
| --- | --- |
| v4 SQL generation | FAILED |
| reference execution | OK (100 rows) |

## Result comparison

_at least one side did not produce rows._

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 0 | 0 | — |
| reference | 1512 | 32 | 8.53 ms |

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

_v4 did not produce SQL._

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

## v4 generation error

```
Traceback (most recent call last):
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 256, in generate_v4_sql
    statements = eng.generate_sql(preql_path.read_text())
  File "C:\Program Files\Python313\Lib\functools.py", line 983, in _method
    return dispatch(args[0].__class__).__get__(obj, cls)(*args, **kwargs)
           ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\executor.py", line 663, in _
    compiled_sql = self.generator.compile_statement(statement)
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\dialect\base.py", line 2315, in compile_statement
    raise ValueError(
    ...<2 lines>...
    )
ValueError: Invalid reference string found in query: SELECT
    INVALID_REFERENCE_BUG as "inventory_item_product_name",
    INVALID_REFERENCE_BUG as "inventory_item_brand_name",
    INVALID_REFERENCE_BUG as "inventory_item_class",
    INVALID_REFERENCE_BUG as "inventory_item_category",
    avg(INVALID_REFERENCE_BUG) as "qoh"

GROUP BY
    ROLLUP (1, 2, 3, 4)
ORDER BY 
    "qoh" asc nulls first,
    "inventory_item_product_name" asc nulls first,
    "inventory_item_brand_name" asc nulls first,
    "inventory_item_class" asc nulls first,
    "inventory_item_category" asc nulls first
LIMIT (100), this should never occur. Please create an issue to report this.
```
