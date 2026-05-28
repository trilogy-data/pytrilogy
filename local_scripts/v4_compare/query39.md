# Query 39

**Status:** `exec_fail`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | FAILED |
| reference execution | OK (243 rows) |

## Result comparison

_at least one side did not produce rows._

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 2828 | 76 | — |
| reference | 2041 | 36 | 8.074 s |
| v4 / ref | 1.39x | 2.11x | — |

## Preql

```
import inventory as inventory;

def month_mean(month) -> avg(inventory.quantity_on_hand ? inventory.date.month_of_year = month)
    by inventory.warehouse.id, inventory.item.id;
def month_stdev(month) -> stddev(inventory.quantity_on_hand ? inventory.date.month_of_year = month)
    by inventory.warehouse.id, inventory.item.id;

auto mean1 <- @month_mean(1);
auto stdev1 <- @month_stdev(1);
auto mean2 <- @month_mean(2);
auto stdev2 <- @month_stdev(2);
auto cov1 <- case
    when mean1 = 0 then null
    else stdev1 / mean1
end;
auto cov2 <- case
    when mean2 = 0 then null
    else stdev2 / mean2
end;

where
    inventory.date.year = 2001
    and inventory.date.month_of_year in (1, 2)
    and inventory.warehouse.id is not null
select
    inventory.warehouse.id as wsk1,
    inventory.item.id as isk1,
    1 as dmoy1,
    mean1,
    cov1,
    inventory.warehouse.id as wsk2,
    inventory.item.id as isk2,
    2 as dmoy2,
    mean2,
    cov2,
having
    cov1 > 1 and cov2 > 1

order by
    wsk1 asc nulls first,
    isk1 asc nulls first,
    mean1 asc nulls first,
    cov1 asc nulls first,
    mean2 asc nulls first,
    cov2 asc nulls first
;
```

## v4 generated SQL

```sql
WITH 
wakeful as (
SELECT
    "inventory_date_date"."D_MOY" as "inventory_date_month_of_year",
    "inventory_warehouse_inventory"."inv_item_sk" as "inventory_item_id",
    "inventory_warehouse_inventory"."inv_quantity_on_hand" as "inventory_quantity_on_hand",
    "inventory_warehouse_inventory"."inv_warehouse_sk" as "inventory_warehouse_id"
FROM
    "memory"."inventory" as "inventory_warehouse_inventory"
    INNER JOIN "memory"."date_dim" as "inventory_date_date" on "inventory_warehouse_inventory"."inv_date_sk" = "inventory_date_date"."D_DATE_SK"
WHERE
    "inventory_date_date"."D_YEAR" = 2001 and "inventory_date_date"."D_MOY" in (1,2) and "inventory_warehouse_inventory"."inv_warehouse_sk" is not null
),
abundant as (
SELECT
    1 as "dmoy1",
    2 as "dmoy2"
),
questionable as (
SELECT
    "wakeful"."inventory_item_id" as "isk1",
    "wakeful"."inventory_item_id" as "isk2",
    "wakeful"."inventory_warehouse_id" as "wsk1",
    "wakeful"."inventory_warehouse_id" as "wsk2"
FROM
    "wakeful"),
cheerful as (
SELECT
    avg(CASE WHEN "wakeful"."inventory_date_month_of_year" = 1 THEN "wakeful"."inventory_quantity_on_hand" ELSE NULL END) as "mean1",
    avg(CASE WHEN "wakeful"."inventory_date_month_of_year" = 2 THEN "wakeful"."inventory_quantity_on_hand" ELSE NULL END) as "mean2",
    stddev_samp(CASE WHEN "wakeful"."inventory_date_month_of_year" = 1 THEN "wakeful"."inventory_quantity_on_hand" ELSE NULL END) as "stdev1",
    stddev_samp(CASE WHEN "wakeful"."inventory_date_month_of_year" = 2 THEN "wakeful"."inventory_quantity_on_hand" ELSE NULL END) as "stdev2"
FROM
    "wakeful"
GROUP BY
    "wakeful"."inventory_item_id",
    "wakeful"."inventory_warehouse_id"),
cooperative as (
SELECT
    "cheerful"."mean1" as "mean1",
    "cheerful"."mean2" as "mean2",
    CASE
	WHEN "cheerful"."mean1" = 0 THEN null
	ELSE "cheerful"."stdev1" / "cheerful"."mean1"
	END as "cov1",
    CASE
	WHEN "cheerful"."mean2" = 0 THEN null
	ELSE "cheerful"."stdev2" / "cheerful"."mean2"
	END as "cov2"
FROM
    "cheerful")
SELECT
    "questionable"."wsk1" as "wsk1",
    "questionable"."isk1" as "isk1",
    "abundant"."dmoy1" as "dmoy1",
    "cooperative"."mean1" as "mean1",
    "cooperative"."cov1" as "cov1",
    "questionable"."wsk2" as "wsk2",
    "questionable"."isk2" as "isk2",
    "abundant"."dmoy2" as "dmoy2",
    "cooperative"."mean2" as "mean2",
    "cooperative"."cov2" as "cov2"
FROM
    "questionable"
    RIGHT OUTER JOIN "cooperative" on 1=1
    LEFT OUTER JOIN "abundant" on 1=1
WHERE
    "cooperative"."cov1" > 1 and "cooperative"."cov2" > 1

ORDER BY 
    "questionable"."wsk1" asc nulls first,
    "questionable"."isk1" asc nulls first,
    "cooperative"."mean1" asc nulls first,
    "cooperative"."cov1" asc nulls first,
    "cooperative"."mean2" asc nulls first,
    "cooperative"."cov2" asc nulls first
```

## Reference SQL (zquery log)

```sql
SELECT
    "inventory_warehouse_inventory"."inv_warehouse_sk" as "wsk1",
    "inventory_warehouse_inventory"."inv_item_sk" as "isk1",
    1 as "dmoy1",
    avg(CASE WHEN "inventory_date_date"."D_MOY" = 1 THEN "inventory_warehouse_inventory"."inv_quantity_on_hand" ELSE NULL END) as "mean1",
    CASE
	WHEN avg(CASE WHEN "inventory_date_date"."D_MOY" = 1 THEN "inventory_warehouse_inventory"."inv_quantity_on_hand" ELSE NULL END) = 0 THEN null
	ELSE stddev_samp(CASE WHEN "inventory_date_date"."D_MOY" = 1 THEN "inventory_warehouse_inventory"."inv_quantity_on_hand" ELSE NULL END) / avg(CASE WHEN "inventory_date_date"."D_MOY" = 1 THEN "inventory_warehouse_inventory"."inv_quantity_on_hand" ELSE NULL END)
	END as "cov1",
    "inventory_warehouse_inventory"."inv_warehouse_sk" as "wsk2",
    "inventory_warehouse_inventory"."inv_item_sk" as "isk2",
    2 as "dmoy2",
    avg(CASE WHEN "inventory_date_date"."D_MOY" = 2 THEN "inventory_warehouse_inventory"."inv_quantity_on_hand" ELSE NULL END) as "mean2",
    CASE
	WHEN avg(CASE WHEN "inventory_date_date"."D_MOY" = 2 THEN "inventory_warehouse_inventory"."inv_quantity_on_hand" ELSE NULL END) = 0 THEN null
	ELSE stddev_samp(CASE WHEN "inventory_date_date"."D_MOY" = 2 THEN "inventory_warehouse_inventory"."inv_quantity_on_hand" ELSE NULL END) / avg(CASE WHEN "inventory_date_date"."D_MOY" = 2 THEN "inventory_warehouse_inventory"."inv_quantity_on_hand" ELSE NULL END)
	END as "cov2"
FROM
    "memory"."inventory" as "inventory_warehouse_inventory"
    INNER JOIN "memory"."date_dim" as "inventory_date_date" on "inventory_warehouse_inventory"."inv_date_sk" = "inventory_date_date"."D_DATE_SK"
WHERE
    "inventory_date_date"."D_YEAR" = 2001 and "inventory_date_date"."D_MOY" in (1,2) and "inventory_warehouse_inventory"."inv_warehouse_sk" is not null

GROUP BY
    1,
    2
HAVING
    "cov1" > 1 and "cov2" > 1

ORDER BY 
    "wsk1" asc nulls first,
    "isk1" asc nulls first,
    "mean1" asc nulls first,
    "cov1" asc nulls first,
    "mean2" asc nulls first,
    "cov2" asc nulls first
```

## v4 execution error

```
Traceback (most recent call last):
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 267, in run_one
    result.v4_exec_seconds, result.v4_rows = _time(lambda: _exec(v4_sql))
                                             ~~~~~^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 52, in _time
    value = fn()
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 267, in <lambda>
    result.v4_exec_seconds, result.v4_rows = _time(lambda: _exec(v4_sql))
                                                           ~~~~~^^^^^^^^
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 263, in _exec
    return execute(con, bound_sql, params or None)
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 185, in execute
    rows = cursor.fetchall()
MemoryError
```
