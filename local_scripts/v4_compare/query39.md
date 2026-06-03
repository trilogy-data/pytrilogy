# Query 39

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (243 rows) |
| reference execution | OK (243 rows) |
| results identical | YES |

## Result comparison

v4 rows: 243 (243 distinct)
ref rows: 243 (243 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 2864 | 82 | 39.88 ms |
| reference | 2041 | 36 | 39.69 ms |
| v4 / ref | 1.40x | 2.28x | 1.00x |

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
    "wakeful"),
abundant as (
SELECT
    "cooperative"."cov1" as "cov1",
    "cooperative"."cov2" as "cov2",
    "cooperative"."isk1" as "isk1",
    "cooperative"."isk2" as "isk2",
    "cooperative"."mean1" as "mean1",
    "cooperative"."mean2" as "mean2",
    "cooperative"."wsk1" as "wsk1",
    "cooperative"."wsk2" as "wsk2",
    "questionable"."dmoy1" as "dmoy1",
    "questionable"."dmoy2" as "dmoy2"
FROM
    "cooperative"
    LEFT OUTER JOIN "questionable" on 1=1
WHERE
    "cooperative"."cov1" > 1
)
SELECT
    "abundant"."wsk1" as "wsk1",
    "abundant"."isk1" as "isk1",
    "abundant"."dmoy1" as "dmoy1",
    "abundant"."mean1" as "mean1",
    "abundant"."cov1" as "cov1",
    "abundant"."wsk2" as "wsk2",
    "abundant"."isk2" as "isk2",
    "abundant"."dmoy2" as "dmoy2",
    "abundant"."mean2" as "mean2",
    "abundant"."cov2" as "cov2"
FROM
    "abundant"
WHERE
    "abundant"."cov2" > 1

ORDER BY 
    "abundant"."wsk1" asc nulls first,
    "abundant"."isk1" asc nulls first,
    "abundant"."mean1" asc nulls first,
    "abundant"."cov1" asc nulls first,
    "abundant"."mean2" asc nulls first,
    "abundant"."cov2" asc nulls first
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
