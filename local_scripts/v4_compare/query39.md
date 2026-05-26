# Query 39

**Status:** `mismatch`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (1944 rows) |
| reference execution | OK (243 rows) |
| results identical | NO |

## Result comparison

v4 rows: 1944 (243 distinct)
ref rows: 243 (243 distinct)
only in v4 (showing up to 5 of 243):
  7x  (1.2438391781531353, 1.0151581328149208, 1, 2, 265, 265, 324.75, 329.0, 1, 1)
  7x  (1.031941572270649, 1.1411766752007977, 1, 2, 363, 363, 499.5, 321.0, 1, 1)
  7x  (1.0955498064867504, 1.042970994259454, 1, 2, 679, 679, 373.75, 417.5, 1, 1)
  7x  (1.0835888283564505, 1.1356494125569416, 1, 2, 695, 695, 450.75, 368.75, 1, 1)
  7x  (1.03450938027956, 1.0284221852702604, 1, 2, 789, 789, 357.25, 410.0, 1, 1)
only in ref (showing up to 5 of 49):
  1x  (1.1702270938111008, 1.3057281471249385, 1, 2, 815, 815, 216.5, 150.5, 1, 1)
  1x  (1.1285483279713715, 1.2717809002195564, 1, 2, 1623, 1623, 338.25, 261.3333333333333, 1, 1)
  1x  (1.0604290412504491, 1.0362984739390064, 1, 2, 2581, 2581, 448.5, 476.25, 1, 1)
  1x  (1.0318296151625301, 1.1693842343776149, 1, 2, 4955, 4955, 495.25, 322.5, 1, 1)
  1x  (1.0874396852180854, 1.0470055593145149, 1, 2, 7569, 7569, 430.5, 360.25, 1, 1)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 3487 | 91 | 89.00 ms |
| reference | 2041 | 36 | 32.37 ms |
| v4 / ref | 1.71x | 2.53x | 2.75x |

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
cheerful as (
SELECT
    "wakeful"."inventory_item_id" as "inventory_item_id",
    "wakeful"."inventory_warehouse_id" as "inventory_warehouse_id",
    avg(CASE WHEN "wakeful"."inventory_date_month_of_year" = 1 THEN "wakeful"."inventory_quantity_on_hand" ELSE NULL END) as "mean1",
    avg(CASE WHEN "wakeful"."inventory_date_month_of_year" = 2 THEN "wakeful"."inventory_quantity_on_hand" ELSE NULL END) as "mean2",
    stddev_samp(CASE WHEN "wakeful"."inventory_date_month_of_year" = 1 THEN "wakeful"."inventory_quantity_on_hand" ELSE NULL END) as "stdev1",
    stddev_samp(CASE WHEN "wakeful"."inventory_date_month_of_year" = 2 THEN "wakeful"."inventory_quantity_on_hand" ELSE NULL END) as "stdev2"
FROM
    "wakeful"
GROUP BY
    1,
    2),
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
	END as "cov2",
    coalesce("cheerful"."inventory_item_id","wakeful"."inventory_item_id") as "isk1",
    coalesce("cheerful"."inventory_item_id","wakeful"."inventory_item_id") as "isk2",
    coalesce("cheerful"."inventory_warehouse_id","wakeful"."inventory_warehouse_id") as "wsk1",
    coalesce("cheerful"."inventory_warehouse_id","wakeful"."inventory_warehouse_id") as "wsk2"
FROM
    "cheerful"
    FULL JOIN "wakeful" on "cheerful"."inventory_item_id" = "wakeful"."inventory_item_id" AND "cheerful"."inventory_warehouse_id" is not distinct from "wakeful"."inventory_warehouse_id"),
uneven as (
SELECT
    "abundant"."dmoy1" as "dmoy1",
    "abundant"."dmoy2" as "dmoy2",
    "cooperative"."cov1" as "cov1",
    "cooperative"."cov2" as "cov2",
    "cooperative"."isk1" as "isk1",
    "cooperative"."isk2" as "isk2",
    "cooperative"."mean1" as "mean1",
    "cooperative"."mean2" as "mean2",
    "cooperative"."wsk1" as "wsk1",
    "cooperative"."wsk2" as "wsk2"
FROM
    "cooperative"
    LEFT OUTER JOIN "abundant" on 1=1
WHERE
    "cooperative"."cov1" > 1
)
SELECT
    "uneven"."wsk1" as "wsk1",
    "uneven"."isk1" as "isk1",
    "uneven"."dmoy1" as "dmoy1",
    "uneven"."mean1" as "mean1",
    "uneven"."cov1" as "cov1",
    "uneven"."wsk2" as "wsk2",
    "uneven"."isk2" as "isk2",
    "uneven"."dmoy2" as "dmoy2",
    "uneven"."mean2" as "mean2",
    "uneven"."cov2" as "cov2"
FROM
    "uneven"
WHERE
    "uneven"."cov2" > 1

ORDER BY 
    "uneven"."wsk1" asc nulls first,
    "uneven"."isk1" asc nulls first,
    "uneven"."mean1" asc nulls first,
    "uneven"."cov1" asc nulls first,
    "uneven"."mean2" asc nulls first,
    "uneven"."cov2" asc nulls first
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
