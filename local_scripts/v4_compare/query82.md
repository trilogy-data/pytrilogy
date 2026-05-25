# Query 82

**Status:** `mismatch`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (34 rows) |
| reference execution | OK (2 rows) |
| results identical | NO |

## Result comparison

v4 rows: 34 (34 distinct)
ref rows: 2 (2 distinct)
only in v4 (showing up to 5 of 34):
  1x  (datetime.date(2000, 7, 13), Decimal('67.28'), 'Arab, financial pol', 270, 'AAAAAAAAECMCAAAA', 317, 11300)
  1x  (datetime.date(2000, 6, 29), Decimal('67.28'), 'Arab, financial pol', 270, 'AAAAAAAAECMCAAAA', 110, 11300)
  1x  (datetime.date(2000, 6, 8), Decimal('67.28'), 'Arab, financial pol', 270, 'AAAAAAAAECMCAAAA', 282, 11300)
  1x  (datetime.date(2000, 7, 20), Decimal('67.28'), 'Arab, financial pol', 270, 'AAAAAAAAECMCAAAA', 482, 11300)
  1x  (datetime.date(2000, 6, 1), Decimal('67.28'), 'Arab, financial pol', 270, 'AAAAAAAAECMCAAAA', 199, 11300)
only in ref (showing up to 5 of 2):
  1x  (Decimal('67.28'), 'Arab, financial pol', 'AAAAAAAAECMCAAAA')
  1x  (Decimal('86.90'), 'Clinical, labour aspects might sit enough like a problems. Remarkably mysterious experts shall learn to th', 'AAAAAAAALIHCAAAA')

## SQL size

| Source | Chars | Lines |
| --- | --- | --- |
| v4 | 2707 | 66 |
| reference | 1583 | 34 |
| v4 / ref | 1.71x | 1.94x |

## Preql

```
import inventory as inventory;
import store_sales as store_sales;

merge inventory.item.id into ~store_sales.item.id;

where
    inventory.item.current_price between 62 and 92
    and inventory.date.date between '2000-05-25'::date and '2000-07-24'::date
    and inventory.item.manufacturer_id in (129, 270, 821, 423)
    and inventory.quantity_on_hand between 100 and 500
    and store_sales.item.id is not null
select
    inventory.item.name,
    inventory.item.desc,
    inventory.item.current_price,
order by
    inventory.item.name asc
limit 100
;
```

## v4 generated SQL

```sql
WITH 
thoughtful as (
SELECT
    "store_sales_item_items"."I_ITEM_SK" as "store_sales_item_id"
FROM
    "memory"."item" as "store_sales_item_items"),
wakeful as (
SELECT
    "inventory_warehouse_inventory"."inv_date_sk" as "inventory_date_id",
    "inventory_warehouse_inventory"."inv_item_sk" as "store_sales_item_id",
    "inventory_warehouse_inventory"."inv_quantity_on_hand" as "inventory_quantity_on_hand"
FROM
    "memory"."inventory" as "inventory_warehouse_inventory"),
cheerful as (
SELECT
    "wakeful"."inventory_date_id" as "inventory_date_id",
    "wakeful"."inventory_quantity_on_hand" as "inventory_quantity_on_hand",
    "wakeful"."store_sales_item_id" as "store_sales_item_id"
FROM
    "wakeful"
GROUP BY
    1,
    2,
    3),
highfalutin as (
SELECT
    "inventory_item_items"."I_CURRENT_PRICE" as "inventory_item_current_price",
    "inventory_item_items"."I_ITEM_DESC" as "inventory_item_desc",
    "inventory_item_items"."I_ITEM_ID" as "inventory_item_name",
    "inventory_item_items"."I_ITEM_SK" as "store_sales_item_id",
    "inventory_item_items"."I_MANUFACT_ID" as "inventory_item_manufacturer_id"
FROM
    "memory"."item" as "inventory_item_items"),
quizzical as (
SELECT
    "inventory_date_date"."D_DATE_SK" as "inventory_date_id",
    cast("inventory_date_date"."D_DATE" as date) as "inventory_date_date"
FROM
    "memory"."date_dim" as "inventory_date_date")
SELECT
    "quizzical"."inventory_date_date" as "inventory_date_date",
    "highfalutin"."inventory_item_current_price" as "inventory_item_current_price",
    "highfalutin"."inventory_item_desc" as "inventory_item_desc",
    "highfalutin"."inventory_item_manufacturer_id" as "inventory_item_manufacturer_id",
    "highfalutin"."inventory_item_name" as "inventory_item_name",
    "cheerful"."inventory_quantity_on_hand" as "inventory_quantity_on_hand",
    "thoughtful"."store_sales_item_id" as "store_sales_item_id"
FROM
    "cheerful"
    INNER JOIN "quizzical" on "cheerful"."inventory_date_id" = "quizzical"."inventory_date_id"
    RIGHT OUTER JOIN "thoughtful" on "cheerful"."store_sales_item_id" = "thoughtful"."store_sales_item_id"
    LEFT OUTER JOIN "highfalutin" on "thoughtful"."store_sales_item_id" = "highfalutin"."store_sales_item_id"
WHERE
    "highfalutin"."inventory_item_current_price" BETWEEN 62 AND 92 and "quizzical"."inventory_date_date" BETWEEN date '2000-05-25' AND date '2000-07-24' and "highfalutin"."inventory_item_manufacturer_id" in (129,270,821,423) and "cheerful"."inventory_quantity_on_hand" BETWEEN 100 AND 500 and "thoughtful"."store_sales_item_id" is not null

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7
ORDER BY 
    "highfalutin"."inventory_item_name" asc
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
wakeful as (
SELECT
    "inventory_warehouse_inventory"."inv_date_sk" as "inventory_date_id",
    "inventory_warehouse_inventory"."inv_item_sk" as "store_sales_item_id",
    "inventory_warehouse_inventory"."inv_quantity_on_hand" as "inventory_quantity_on_hand"
FROM
    "memory"."inventory" as "inventory_warehouse_inventory"
WHERE
    "inventory_warehouse_inventory"."inv_quantity_on_hand" BETWEEN 100 AND 500 and "inventory_warehouse_inventory"."inv_item_sk" is not null

GROUP BY
    1,
    2,
    3)
SELECT
    "inventory_item_items"."I_ITEM_ID" as "inventory_item_name",
    "inventory_item_items"."I_ITEM_DESC" as "inventory_item_desc",
    "inventory_item_items"."I_CURRENT_PRICE" as "inventory_item_current_price"
FROM
    "wakeful"
    INNER JOIN "memory"."date_dim" as "inventory_date_date" on "wakeful"."inventory_date_id" = "inventory_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "store_sales_item_items" on "wakeful"."store_sales_item_id" = "store_sales_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."item" as "inventory_item_items" on "store_sales_item_items"."I_ITEM_SK" = "inventory_item_items"."I_ITEM_SK"
WHERE
    "inventory_item_items"."I_CURRENT_PRICE" BETWEEN 62 AND 92 and cast("inventory_date_date"."D_DATE" as date) BETWEEN date '2000-05-25' AND date '2000-07-24' and "inventory_item_items"."I_MANUFACT_ID" in (129,270,821,423) and "wakeful"."inventory_quantity_on_hand" BETWEEN 100 AND 500 and "store_sales_item_items"."I_ITEM_SK" is not null

GROUP BY
    1,
    2,
    3
ORDER BY 
    "inventory_item_items"."I_ITEM_ID" asc
LIMIT (100)
```
