# Query 56

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
| v4 | 3907 | 68 | 44.25 ms |
| reference | 3316 | 58 | 51.80 ms |
| v4 / ref | 1.18x | 1.17x | 0.85x |

## Preql

```
import all_sales as sales;

auto color_ids <- sales.item.text_id ? sales.item.color in ('slate', 'blanched', 'burnished');

where
    sales.item.text_id in color_ids
    and sales.date.year = 2001
    and sales.date.month_of_year = 2
    and sales.bill_address.gmt_offset = -5
select
    sales.item.text_id,
    sum(sales.ext_sales_price) as total_sales,
order by
    total_sales asc nulls first,
    sales.item.text_id asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
uneven as (
SELECT
    "sales_item_items"."I_ITEM_ID" as "sales_item_text_id",
    "sales_item_items"."I_ITEM_SK" as "sales_item_id",
    CASE WHEN "sales_item_items"."I_COLOR" in ('slate','blanched','burnished') THEN "sales_item_items"."I_ITEM_ID" ELSE NULL END as "color_ids"
FROM
    "memory"."item" as "sales_item_items"),
thoughtful as (
SELECT
    "sales_catalog_sales_unified"."CS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_catalog_sales_unified"."CS_ITEM_SK" as "sales_item_id",
    "uneven"."sales_item_text_id" as "sales_item_text_id"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
    INNER JOIN "memory"."customer_address" as "sales_bill_address_customer_address" on "sales_catalog_sales_unified"."CS_BILL_ADDR_SK" = "sales_bill_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "uneven" on "sales_catalog_sales_unified"."CS_ITEM_SK" = "uneven"."sales_item_id"
WHERE
    "sales_bill_address_customer_address"."CA_GMT_OFFSET" = -5 and "sales_date_date"."D_YEAR" = 2001 and "sales_date_date"."D_MOY" = 2 and "uneven"."sales_item_text_id" in (select uneven."color_ids" from uneven where uneven."color_ids" is not null)

UNION ALL
SELECT
    "sales_store_sales_unified"."SS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_id",
    "uneven"."sales_item_text_id" as "sales_item_text_id"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "memory"."customer_address" as "sales_bill_address_customer_address" on "sales_store_sales_unified"."SS_ADDR_SK" = "sales_bill_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "uneven" on "sales_store_sales_unified"."SS_ITEM_SK" = "uneven"."sales_item_id"
WHERE
    "sales_bill_address_customer_address"."CA_GMT_OFFSET" = -5 and "sales_date_date"."D_YEAR" = 2001 and "sales_date_date"."D_MOY" = 2 and "uneven"."sales_item_text_id" in (select uneven."color_ids" from uneven where uneven."color_ids" is not null)

UNION ALL
SELECT
    "sales_web_sales_unified"."WS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_id",
    "uneven"."sales_item_text_id" as "sales_item_text_id"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "memory"."customer_address" as "sales_bill_address_customer_address" on "sales_web_sales_unified"."WS_BILL_ADDR_SK" = "sales_bill_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "uneven" on "sales_web_sales_unified"."WS_ITEM_SK" = "uneven"."sales_item_id"
WHERE
    "sales_bill_address_customer_address"."CA_GMT_OFFSET" = -5 and "sales_date_date"."D_YEAR" = 2001 and "sales_date_date"."D_MOY" = 2 and "uneven"."sales_item_text_id" in (select uneven."color_ids" from uneven where uneven."color_ids" is not null)
),
abundant as (
SELECT
    "thoughtful"."sales_ext_sales_price" as "sales_ext_sales_price",
    "thoughtful"."sales_item_text_id" as "sales_item_text_id"
FROM
    "thoughtful"
    INNER JOIN "memory"."item" as "sales_item_items" on "thoughtful"."sales_item_id" = "sales_item_items"."I_ITEM_SK")
SELECT
    sum("abundant"."sales_ext_sales_price") as "total_sales",
    "abundant"."sales_item_text_id" as "sales_item_text_id"
FROM
    "abundant"
WHERE
    "abundant"."sales_item_text_id" in (select uneven."color_ids" from uneven where uneven."color_ids" is not null)

GROUP BY
    2
ORDER BY 
    "total_sales" asc nulls first,
    "abundant"."sales_item_text_id" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
abundant as (
SELECT
    "sales_item_items"."I_ITEM_ID" as "color_ids"
FROM
    "memory"."item" as "sales_item_items"
WHERE
    "sales_item_items"."I_COLOR" in ('slate','blanched','burnished')

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
    "sales_bill_address_customer_address"."CA_GMT_OFFSET" = -5 and "sales_date_date"."D_YEAR" = 2001 and "sales_date_date"."D_MOY" = 2 and "sales_item_items"."I_ITEM_ID" in (select abundant."color_ids" from abundant where abundant."color_ids" is not null)

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
    "sales_bill_address_customer_address"."CA_GMT_OFFSET" = -5 and "sales_date_date"."D_YEAR" = 2001 and "sales_date_date"."D_MOY" = 2 and "sales_item_items"."I_ITEM_ID" in (select abundant."color_ids" from abundant where abundant."color_ids" is not null)

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
    "sales_bill_address_customer_address"."CA_GMT_OFFSET" = -5 and "sales_date_date"."D_YEAR" = 2001 and "sales_date_date"."D_MOY" = 2 and "sales_item_items"."I_ITEM_ID" in (select abundant."color_ids" from abundant where abundant."color_ids" is not null)
)
SELECT
    "thoughtful"."sales_item_text_id" as "sales_item_text_id",
    sum("thoughtful"."sales_ext_sales_price") as "total_sales"
FROM
    "thoughtful"
GROUP BY
    1
ORDER BY 
    "total_sales" asc nulls first,
    "thoughtful"."sales_item_text_id" asc nulls first
LIMIT (100)
```
