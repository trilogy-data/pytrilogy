# Query 60

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (100 rows) |
| reference execution | OK (100 rows) |
| results identical | YES |

## Result comparison

v4 rows: 100 (100 distinct)
ref rows: 100 (100 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 3705 | 68 | 33.51 ms |
| reference | 3304 | 58 | 37.09 ms |
| v4 / ref | 1.12x | 1.17x | 0.90x |

## Preql

```
import all_sales as sales;

auto music_item_ids <- sales.item.text_id ? sales.item.category = 'Music';

where
    sales.item.text_id in music_item_ids
    and sales.date.year = 1998
    and sales.date.month_of_year = 9
    and sales.bill_address.gmt_offset = -5
select
    sales.item.text_id,
    sum(sales.ext_sales_price) as total_sales,
order by
    sales.item.text_id asc,
    total_sales asc
limit 100
;
```

## v4 generated SQL

```sql
WITH 
uneven as (
SELECT
    "sales_item_items"."I_ITEM_ID" as "music_item_ids"
FROM
    "memory"."item" as "sales_item_items"
WHERE
    "sales_item_items"."I_CATEGORY" = 'Music'
),
thoughtful as (
SELECT
    "sales_catalog_sales_unified"."CS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_item_items"."I_ITEM_ID" as "sales_item_text_id"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
    INNER JOIN "memory"."customer_address" as "sales_bill_address_customer_address" on "sales_catalog_sales_unified"."CS_BILL_ADDR_SK" = "sales_bill_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "sales_item_items" on "sales_catalog_sales_unified"."CS_ITEM_SK" = "sales_item_items"."I_ITEM_SK"
WHERE
    "sales_bill_address_customer_address"."CA_GMT_OFFSET" = -5 and "sales_date_date"."D_YEAR" = 1998 and "sales_date_date"."D_MOY" = 9 and "sales_item_items"."I_ITEM_ID" in (select uneven."music_item_ids" from uneven where uneven."music_item_ids" is not null)

UNION ALL
SELECT
    "sales_store_sales_unified"."SS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_item_items"."I_ITEM_ID" as "sales_item_text_id"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "memory"."customer_address" as "sales_bill_address_customer_address" on "sales_store_sales_unified"."SS_ADDR_SK" = "sales_bill_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "sales_item_items" on "sales_store_sales_unified"."SS_ITEM_SK" = "sales_item_items"."I_ITEM_SK"
WHERE
    "sales_bill_address_customer_address"."CA_GMT_OFFSET" = -5 and "sales_date_date"."D_YEAR" = 1998 and "sales_date_date"."D_MOY" = 9 and "sales_item_items"."I_ITEM_ID" in (select uneven."music_item_ids" from uneven where uneven."music_item_ids" is not null)

UNION ALL
SELECT
    "sales_web_sales_unified"."WS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_item_items"."I_ITEM_ID" as "sales_item_text_id"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "memory"."customer_address" as "sales_bill_address_customer_address" on "sales_web_sales_unified"."WS_BILL_ADDR_SK" = "sales_bill_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "sales_item_items" on "sales_web_sales_unified"."WS_ITEM_SK" = "sales_item_items"."I_ITEM_SK"
WHERE
    "sales_bill_address_customer_address"."CA_GMT_OFFSET" = -5 and "sales_date_date"."D_YEAR" = 1998 and "sales_date_date"."D_MOY" = 9 and "sales_item_items"."I_ITEM_ID" in (select uneven."music_item_ids" from uneven where uneven."music_item_ids" is not null)
),
abundant as (
SELECT
    "thoughtful"."sales_ext_sales_price" as "sales_ext_sales_price",
    "thoughtful"."sales_item_text_id" as "sales_item_text_id"
FROM
    "thoughtful"
WHERE
    "thoughtful"."sales_item_text_id" in (select uneven."music_item_ids" from uneven where uneven."music_item_ids" is not null)
)
SELECT
    "abundant"."sales_item_text_id" as "sales_item_text_id",
    sum("abundant"."sales_ext_sales_price") as "total_sales"
FROM
    "abundant"
WHERE
    "abundant"."sales_item_text_id" in (select uneven."music_item_ids" from uneven where uneven."music_item_ids" is not null)

GROUP BY
    1
ORDER BY 
    "abundant"."sales_item_text_id" asc,
    "total_sales" asc
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
abundant as (
SELECT
    "sales_item_items"."I_ITEM_ID" as "music_item_ids"
FROM
    "memory"."item" as "sales_item_items"
WHERE
    "sales_item_items"."I_CATEGORY" = 'Music'

GROUP BY
    1),
thoughtful as (
SELECT
    "sales_catalog_sales_unified"."CS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_item_items"."I_ITEM_ID" as "sales_item_text_id"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
    INNER JOIN "memory"."customer_address" as "sales_bill_address_customer_address" on "sales_catalog_sales_unified"."CS_BILL_ADDR_SK" = "sales_bill_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "sales_item_items" on "sales_catalog_sales_unified"."CS_ITEM_SK" = "sales_item_items"."I_ITEM_SK"
WHERE
    "sales_bill_address_customer_address"."CA_GMT_OFFSET" = -5 and "sales_date_date"."D_YEAR" = 1998 and "sales_date_date"."D_MOY" = 9 and "sales_item_items"."I_ITEM_ID" in (select abundant."music_item_ids" from abundant where abundant."music_item_ids" is not null)

UNION ALL
SELECT
    "sales_store_sales_unified"."SS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_item_items"."I_ITEM_ID" as "sales_item_text_id"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "memory"."customer_address" as "sales_bill_address_customer_address" on "sales_store_sales_unified"."SS_ADDR_SK" = "sales_bill_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "sales_item_items" on "sales_store_sales_unified"."SS_ITEM_SK" = "sales_item_items"."I_ITEM_SK"
WHERE
    "sales_bill_address_customer_address"."CA_GMT_OFFSET" = -5 and "sales_date_date"."D_YEAR" = 1998 and "sales_date_date"."D_MOY" = 9 and "sales_item_items"."I_ITEM_ID" in (select abundant."music_item_ids" from abundant where abundant."music_item_ids" is not null)

UNION ALL
SELECT
    "sales_web_sales_unified"."WS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_item_items"."I_ITEM_ID" as "sales_item_text_id"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "memory"."customer_address" as "sales_bill_address_customer_address" on "sales_web_sales_unified"."WS_BILL_ADDR_SK" = "sales_bill_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "sales_item_items" on "sales_web_sales_unified"."WS_ITEM_SK" = "sales_item_items"."I_ITEM_SK"
WHERE
    "sales_bill_address_customer_address"."CA_GMT_OFFSET" = -5 and "sales_date_date"."D_YEAR" = 1998 and "sales_date_date"."D_MOY" = 9 and "sales_item_items"."I_ITEM_ID" in (select abundant."music_item_ids" from abundant where abundant."music_item_ids" is not null)
)
SELECT
    "thoughtful"."sales_item_text_id" as "sales_item_text_id",
    sum("thoughtful"."sales_ext_sales_price") as "total_sales"
FROM
    "thoughtful"
GROUP BY
    1
ORDER BY 
    "thoughtful"."sales_item_text_id" asc,
    "total_sales" asc
LIMIT (100)
```
