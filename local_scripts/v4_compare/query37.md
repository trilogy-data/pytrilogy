# Query 37

**Status:** `gen_fail`

| Stage | Result |
| --- | --- |
| v4 SQL generation | FAILED |
| reference execution | OK (1 rows) |

## Result comparison

_at least one side did not produce rows._

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 0 | 0 | — |
| reference | 1434 | 42 | 103.68 ms |

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
    items.text_id,
    items.desc,
    items.current_price,
order by
    items.text_id asc nulls first
limit 100
;
```

## v4 generated SQL

_v4 did not produce SQL._

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
    "items_items"."I_ITEM_ID" as "items_text_id",
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
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\dialect\base.py", line 2306, in compile_statement
    raise ValueError(
    ...<2 lines>...
    )
ValueError: Invalid reference string found in query: 
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
    "items_items"."I_ITEM_ID" as "items_text_id"
FROM
    "memory"."item" as "items_items"
WHERE
    "items_items"."I_CURRENT_PRICE" BETWEEN 68 AND 98 and "items_items"."I_MANUFACT_ID" in (677,940,694,808) and "items_items"."I_ITEM_SK" in (select thoughtful."inv_item_ids" from thoughtful where thoughtful."inv_item_ids" is not null)
)
SELECT
    "cooperative"."items_text_id" as "items_text_id",
    "cooperative"."items_desc" as "items_desc",
    "cooperative"."items_current_price" as "items_current_price"
FROM
    "cooperative"
WHERE
    INVALID_REFERENCE_BUG in (677,940,694,808)

ORDER BY 
    "cooperative"."items_text_id" asc nulls first
LIMIT (100), this should never occur. Please create an issue to report this.
```
