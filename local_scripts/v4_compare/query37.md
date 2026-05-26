# Query 37

**Status:** `exec_fail`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | FAILED |
| reference execution | OK (1 rows) |

## Result comparison

_at least one side did not produce rows._

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 565 | 7 | — |
| reference | 1710 | 50 | 159.82 ms |
| v4 / ref | 0.33x | 0.14x | — |

## Preql

```
import item as items;
import inventory as inv;
import catalog_sales as cs;

auto cs_item_ids <- cs.item.id ? True = True;
auto inv_item_ids <- inv.item.id
    ? inv.date.date between '2000-02-01'::date and '2000-04-01'::date
and inv.quantity_on_hand between 100 and 500;

where
    items.current_price between 68 and 98
    and items.manufacturer_id in (677, 940, 694, 808)
    and items.id in cs_item_ids
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
SELECT
    CASE WHEN True is True THEN INVALID_REFERENCE_BUG_<Missing source reference to cs.item.id> ELSE NULL END as "cs_item_ids",
    CASE WHEN INVALID_REFERENCE_BUG_<Missing source reference to inv.date.date> BETWEEN date '2000-02-01' AND date '2000-04-01' and INVALID_REFERENCE_BUG_<Missing source reference to inv.quantity_on_hand> BETWEEN 100 AND 500 THEN INVALID_REFERENCE_BUG_<Missing source reference to inv.item.id> ELSE NULL END as "inv_item_ids"

ORDER BY 
    INVALID_REFERENCE_BUG_<Missing source reference to items.name> asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
thoughtful as (
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
wakeful as (
SELECT
    "cs_item_items"."I_ITEM_SK" as "cs_item_ids"
FROM
    "memory"."item" as "cs_item_items"
WHERE
    True is True
),
questionable as (
SELECT
    CASE WHEN cast("inv_date_date"."D_DATE" as date) BETWEEN date '2000-02-01' AND date '2000-04-01' and "thoughtful"."inv_quantity_on_hand" BETWEEN 100 AND 500 THEN "thoughtful"."inv_item_id" ELSE NULL END as "inv_item_ids"
FROM
    "thoughtful"
    INNER JOIN "memory"."date_dim" as "inv_date_date" on "thoughtful"."inv_date_id" = "inv_date_date"."D_DATE_SK"
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
    "items_items"."I_CURRENT_PRICE" BETWEEN 68 AND 98 and "items_items"."I_MANUFACT_ID" in (677,940,694,808) and "items_items"."I_ITEM_SK" in (select wakeful."cs_item_ids" from wakeful where wakeful."cs_item_ids" is not null) and "items_items"."I_ITEM_SK" in (select questionable."inv_item_ids" from questionable where questionable."inv_item_ids" is not null)

GROUP BY
    1,
    2,
    3
ORDER BY 
    "items_items"."I_ITEM_ID" asc nulls first
LIMIT (100)
```

## v4 execution error

```
Traceback (most recent call last):
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 179, in run_one
    result.v4_exec_seconds, result.v4_rows = _time(
                                             ~~~~~^
        lambda: execute(con, v4_sql)
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    )
    ^
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 45, in _time
    value = fn()
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 180, in <lambda>
    lambda: execute(con, v4_sql)
            ~~~~~~~^^^^^^^^^^^^^
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 120, in execute
    cursor = con.execute(sql)
_duckdb.ParserException: Parser Error: syntax error at or near "source"

LINE 2: ...    CASE WHEN True is True THEN INVALID_REFERENCE_BUG_<Missing source reference to cs.item.id> ELSE NULL END as "cs_item_ids...
                                                                          ^
```
