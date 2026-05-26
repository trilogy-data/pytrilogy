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
  1x  (datetime.date(2000, 6, 22), Decimal('67.28'), 'Arab, financial pol', 270, 'AAAAAAAAECMCAAAA', 394, 11300)
  1x  (datetime.date(2000, 7, 20), Decimal('67.28'), 'Arab, financial pol', 270, 'AAAAAAAAECMCAAAA', 482, 11300)
  1x  (datetime.date(2000, 6, 1), Decimal('67.28'), 'Arab, financial pol', 270, 'AAAAAAAAECMCAAAA', 298, 11300)
  1x  (datetime.date(2000, 6, 1), Decimal('67.28'), 'Arab, financial pol', 270, 'AAAAAAAAECMCAAAA', 199, 11300)
  1x  (datetime.date(2000, 6, 8), Decimal('67.28'), 'Arab, financial pol', 270, 'AAAAAAAAECMCAAAA', 282, 11300)
only in ref (showing up to 5 of 2):
  1x  (Decimal('67.28'), 'Arab, financial pol', 'AAAAAAAAECMCAAAA')
  1x  (Decimal('86.90'), 'Clinical, labour aspects might sit enough like a problems. Remarkably mysterious experts shall learn to th', 'AAAAAAAALIHCAAAA')

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 1909 | 42 | 188.73 ms |
| reference | 1583 | 34 | 188.23 ms |
| v4 / ref | 1.21x | 1.24x | 1.00x |

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
    cast("inventory_date_date"."D_DATE" as date) as "inventory_date_date",
    "inventory_item_items"."I_CURRENT_PRICE" as "inventory_item_current_price",
    "inventory_item_items"."I_ITEM_DESC" as "inventory_item_desc",
    "inventory_item_items"."I_MANUFACT_ID" as "inventory_item_manufacturer_id",
    "inventory_item_items"."I_ITEM_ID" as "inventory_item_name",
    "wakeful"."inventory_quantity_on_hand" as "inventory_quantity_on_hand",
    "store_sales_item_items"."I_ITEM_SK" as "store_sales_item_id"
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
    3,
    4,
    5,
    6,
    7
ORDER BY 
    "inventory_item_items"."I_ITEM_ID" asc
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
