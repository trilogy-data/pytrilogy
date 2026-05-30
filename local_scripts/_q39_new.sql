
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