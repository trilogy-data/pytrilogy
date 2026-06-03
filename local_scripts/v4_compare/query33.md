# Query 33

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (12 rows) |
| reference execution | OK (12 rows) |
| results identical | YES |

## Result comparison

v4 rows: 12 (12 distinct)
ref rows: 12 (12 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 3995 | 65 | 10.69 ms |
| reference | 4269 | 79 | 11.35 ms |
| v4 / ref | 0.94x | 0.82x | 0.94x |

## Preql

```
import all_sales as sales;
import item as items;

auto electronics_manuf_ids <- items.manufacturer_id ? items.category = 'Electronics';

where
    sales.item.manufacturer_id in electronics_manuf_ids
    and sales.date.year = 1998
    and sales.date.month_of_year = 5
    and sales.bill_address.gmt_offset = -5
select
    sales.item.manufacturer_id,
    sum(sales.ext_sales_price) as total_sales,
order by
    total_sales asc nulls first,
    sales.item.manufacturer_id asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
highfalutin as (
SELECT
    CASE WHEN "items_items"."I_CATEGORY" = 'Electronics' THEN "items_items"."I_MANUFACT_ID" ELSE NULL END as "electronics_manuf_ids"
FROM
    "memory"."item" as "items_items"),
questionable as (
SELECT
    "sales_catalog_sales_unified"."CS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_item_items"."I_MANUFACT_ID" as "sales_item_manufacturer_id"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
    INNER JOIN "memory"."customer_address" as "sales_bill_address_customer_address" on "sales_catalog_sales_unified"."CS_BILL_ADDR_SK" = "sales_bill_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "sales_item_items" on "sales_catalog_sales_unified"."CS_ITEM_SK" = "sales_item_items"."I_ITEM_SK"
WHERE
    "sales_bill_address_customer_address"."CA_GMT_OFFSET" = -5 and "sales_date_date"."D_YEAR" = 1998 and "sales_date_date"."D_MOY" = 5 and "sales_item_items"."I_MANUFACT_ID" in (select highfalutin."electronics_manuf_ids" from highfalutin where highfalutin."electronics_manuf_ids" is not null)

UNION ALL
SELECT
    "sales_store_sales_unified"."SS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_item_items"."I_MANUFACT_ID" as "sales_item_manufacturer_id"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "memory"."customer_address" as "sales_bill_address_customer_address" on "sales_store_sales_unified"."SS_ADDR_SK" = "sales_bill_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "sales_item_items" on "sales_store_sales_unified"."SS_ITEM_SK" = "sales_item_items"."I_ITEM_SK"
WHERE
    "sales_bill_address_customer_address"."CA_GMT_OFFSET" = -5 and "sales_date_date"."D_YEAR" = 1998 and "sales_date_date"."D_MOY" = 5 and "sales_item_items"."I_MANUFACT_ID" in (select highfalutin."electronics_manuf_ids" from highfalutin where highfalutin."electronics_manuf_ids" is not null)

UNION ALL
SELECT
    "sales_web_sales_unified"."WS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_item_items"."I_MANUFACT_ID" as "sales_item_manufacturer_id"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "memory"."customer_address" as "sales_bill_address_customer_address" on "sales_web_sales_unified"."WS_BILL_ADDR_SK" = "sales_bill_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "sales_item_items" on "sales_web_sales_unified"."WS_ITEM_SK" = "sales_item_items"."I_ITEM_SK"
WHERE
    "sales_bill_address_customer_address"."CA_GMT_OFFSET" = -5 and "sales_date_date"."D_YEAR" = 1998 and "sales_date_date"."D_MOY" = 5 and "sales_item_items"."I_MANUFACT_ID" in (select highfalutin."electronics_manuf_ids" from highfalutin where highfalutin."electronics_manuf_ids" is not null)
),
yummy as (
SELECT
    "questionable"."sales_ext_sales_price" as "sales_ext_sales_price",
    "questionable"."sales_item_manufacturer_id" as "sales_item_manufacturer_id"
FROM
    "questionable"
WHERE
    "questionable"."sales_item_manufacturer_id" in (select highfalutin."electronics_manuf_ids" from highfalutin where highfalutin."electronics_manuf_ids" is not null)
)
SELECT
    sum("yummy"."sales_ext_sales_price") as "total_sales",
    "yummy"."sales_item_manufacturer_id" as "sales_item_manufacturer_id"
FROM
    "yummy"
WHERE
    "yummy"."sales_item_manufacturer_id" in (select highfalutin."electronics_manuf_ids" from highfalutin where highfalutin."electronics_manuf_ids" is not null)

GROUP BY
    2
ORDER BY 
    "total_sales" asc nulls first,
    "yummy"."sales_item_manufacturer_id" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
quizzical as (
SELECT
    "items_items"."I_MANUFACT_ID" as "electronics_manuf_ids"
FROM
    "memory"."item" as "items_items"
WHERE
    "items_items"."I_CATEGORY" = 'Electronics'

GROUP BY
    1),
abundant as (
SELECT
    "sales_catalog_sales_unified"."CS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_catalog_sales_unified"."CS_ITEM_SK" as "sales_item_id",
    "sales_catalog_sales_unified"."CS_ORDER_NUMBER" as "sales_order_id",
     'CATALOG'  as "sales_sales_channel",
    "sales_item_items"."I_MANUFACT_ID" as "sales_item_manufacturer_id"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
    INNER JOIN "memory"."customer_address" as "sales_bill_address_customer_address" on "sales_catalog_sales_unified"."CS_BILL_ADDR_SK" = "sales_bill_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "sales_item_items" on "sales_catalog_sales_unified"."CS_ITEM_SK" = "sales_item_items"."I_ITEM_SK"
WHERE
    "sales_bill_address_customer_address"."CA_GMT_OFFSET" = -5 and "sales_date_date"."D_YEAR" = 1998 and "sales_date_date"."D_MOY" = 5 and "sales_item_items"."I_MANUFACT_ID" in (select quizzical."electronics_manuf_ids" from quizzical where quizzical."electronics_manuf_ids" is not null)

UNION ALL
SELECT
    "sales_store_sales_unified"."SS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_id",
    "sales_store_sales_unified"."SS_TICKET_NUMBER" as "sales_order_id",
     'STORE'  as "sales_sales_channel",
    "sales_item_items"."I_MANUFACT_ID" as "sales_item_manufacturer_id"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "memory"."customer_address" as "sales_bill_address_customer_address" on "sales_store_sales_unified"."SS_ADDR_SK" = "sales_bill_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "sales_item_items" on "sales_store_sales_unified"."SS_ITEM_SK" = "sales_item_items"."I_ITEM_SK"
WHERE
    "sales_bill_address_customer_address"."CA_GMT_OFFSET" = -5 and "sales_date_date"."D_YEAR" = 1998 and "sales_date_date"."D_MOY" = 5 and "sales_item_items"."I_MANUFACT_ID" in (select quizzical."electronics_manuf_ids" from quizzical where quizzical."electronics_manuf_ids" is not null)

UNION ALL
SELECT
    "sales_web_sales_unified"."WS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_id",
    "sales_web_sales_unified"."WS_ORDER_NUMBER" as "sales_order_id",
     'WEB'  as "sales_sales_channel",
    "sales_item_items"."I_MANUFACT_ID" as "sales_item_manufacturer_id"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "memory"."customer_address" as "sales_bill_address_customer_address" on "sales_web_sales_unified"."WS_BILL_ADDR_SK" = "sales_bill_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "sales_item_items" on "sales_web_sales_unified"."WS_ITEM_SK" = "sales_item_items"."I_ITEM_SK"
WHERE
    "sales_bill_address_customer_address"."CA_GMT_OFFSET" = -5 and "sales_date_date"."D_YEAR" = 1998 and "sales_date_date"."D_MOY" = 5 and "sales_item_items"."I_MANUFACT_ID" in (select quizzical."electronics_manuf_ids" from quizzical where quizzical."electronics_manuf_ids" is not null)
),
juicy as (
SELECT
    "abundant"."sales_ext_sales_price" as "sales_ext_sales_price",
    "abundant"."sales_item_manufacturer_id" as "sales_item_manufacturer_id"
FROM
    "abundant"
GROUP BY
    1,
    2,
    "abundant"."sales_item_id",
    "abundant"."sales_order_id",
    "abundant"."sales_sales_channel")
SELECT
    "juicy"."sales_item_manufacturer_id" as "sales_item_manufacturer_id",
    sum("juicy"."sales_ext_sales_price") as "total_sales"
FROM
    "juicy"
GROUP BY
    1
ORDER BY 
    "total_sales" asc nulls first,
    "juicy"."sales_item_manufacturer_id" asc nulls first
LIMIT (100)
```
