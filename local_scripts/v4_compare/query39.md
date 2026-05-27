# Query 39

**Status:** `mismatch`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (243 rows) |
| reference execution | OK (243 rows) |
| results identical | NO |

## Result comparison

v4 rows: 243 (243 distinct)
ref rows: 243 (243 distinct)
only in v4 (showing up to 5 of 7):
  1x  (1.2438391781531353, 1.0151581328149206, 1, 2, 265, 265, 324.75, 329.0, 1, 1)
  1x  (1.1702270938111008, 1.3057281471249382, 1, 2, 815, 815, 216.5, 150.5, 1, 1)
  1x  (1.1046890134130438, 1.1653198631238288, 1, 2, 827, 827, 271.75, 424.75, 1, 1)
  1x  (1.0318296151625301, 1.169384234377615, 1, 2, 4955, 4955, 495.25, 322.5, 1, 1)
  1x  (1.5657032366359889, 1.2084286841430676, 1, 2, 5627, 5627, 282.75, 297.5, 1, 1)
only in ref (showing up to 5 of 7):
  1x  (1.2438391781531353, 1.0151581328149208, 1, 2, 265, 265, 324.75, 329.0, 1, 1)
  1x  (1.1702270938111008, 1.3057281471249385, 1, 2, 815, 815, 216.5, 150.5, 1, 1)
  1x  (1.1046890134130438, 1.1653198631238286, 1, 2, 827, 827, 271.75, 424.75, 1, 1)
  1x  (1.0318296151625301, 1.1693842343776149, 1, 2, 4955, 4955, 495.25, 322.5, 1, 1)
  1x  (1.5657032366359889, 1.2084286841430678, 1, 2, 5627, 5627, 282.75, 297.5, 1, 1)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 2498 | 65 | 37.90 ms |
| reference | 2041 | 36 | 36.80 ms |
| v4 / ref | 1.22x | 1.81x | 1.03x |

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
    "inventory_warehouse_inventory"."inv_item_sk" as "inventory_item_id",
    "inventory_warehouse_inventory"."inv_warehouse_sk" as "inventory_warehouse_id",
    avg(CASE WHEN "inventory_date_date"."D_MOY" = 1 THEN "inventory_warehouse_inventory"."inv_quantity_on_hand" ELSE NULL END) as "mean1",
    avg(CASE WHEN "inventory_date_date"."D_MOY" = 2 THEN "inventory_warehouse_inventory"."inv_quantity_on_hand" ELSE NULL END) as "mean2",
    stddev_samp(CASE WHEN "inventory_date_date"."D_MOY" = 1 THEN "inventory_warehouse_inventory"."inv_quantity_on_hand" ELSE NULL END) as "stdev1",
    stddev_samp(CASE WHEN "inventory_date_date"."D_MOY" = 2 THEN "inventory_warehouse_inventory"."inv_quantity_on_hand" ELSE NULL END) as "stdev2"
FROM
    "memory"."inventory" as "inventory_warehouse_inventory"
    INNER JOIN "memory"."date_dim" as "inventory_date_date" on "inventory_warehouse_inventory"."inv_date_sk" = "inventory_date_date"."D_DATE_SK"
WHERE
    "inventory_date_date"."D_YEAR" = 2001 and "inventory_date_date"."D_MOY" in (1,2) and "inventory_warehouse_inventory"."inv_warehouse_sk" is not null

GROUP BY
    1,
    2),
questionable as (
SELECT
    1 as "dmoy1",
    2 as "dmoy2"
),
cooperative as (
SELECT
    "wakeful"."inventory_item_id" as "isk1",
    "wakeful"."inventory_item_id" as "isk2",
    "wakeful"."inventory_warehouse_id" as "wsk1",
    "wakeful"."inventory_warehouse_id" as "wsk2",
    "wakeful"."mean1" as "mean1",
    "wakeful"."mean2" as "mean2",
    CASE
	WHEN "wakeful"."mean1" = 0 THEN null
	ELSE "wakeful"."stdev1" / "wakeful"."mean1"
	END as "cov1",
    CASE
	WHEN "wakeful"."mean2" = 0 THEN null
	ELSE "wakeful"."stdev2" / "wakeful"."mean2"
	END as "cov2"
FROM
    "wakeful")
SELECT
    "cooperative"."wsk1" as "wsk1",
    "cooperative"."isk1" as "isk1",
    "questionable"."dmoy1" as "dmoy1",
    "cooperative"."mean1" as "mean1",
    "cooperative"."cov1" as "cov1",
    "cooperative"."wsk2" as "wsk2",
    "cooperative"."isk2" as "isk2",
    "questionable"."dmoy2" as "dmoy2",
    "cooperative"."mean2" as "mean2",
    "cooperative"."cov2" as "cov2"
FROM
    "cooperative"
    LEFT OUTER JOIN "questionable" on 1=1
WHERE
    "cooperative"."cov1" > 1 and "cooperative"."cov2" > 1

ORDER BY 
    "cooperative"."wsk1" asc nulls first,
    "cooperative"."isk1" asc nulls first,
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
