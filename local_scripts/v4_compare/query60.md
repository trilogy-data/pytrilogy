# Query 60

**Status:** `mismatch`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (100 rows) |
| reference execution | OK (100 rows) |
| results identical | NO |

## Result comparison

v4 rows: 100 (100 distinct)
ref rows: 100 (100 distinct)
only in v4 (showing up to 5 of 80):
  1x  ('AAAAAAAAAAABAAAA', Decimal('4074.68'))
  1x  ('AAAAAAAAAAACAAAA', Decimal('5797.72'))
  1x  ('AAAAAAAAAAAEAAAA', Decimal('16943.52'))
  1x  ('AAAAAAAAAABAAAAA', Decimal('11968.56'))
  1x  ('AAAAAAAAAABDAAAA', Decimal('16554.68'))
only in ref (showing up to 5 of 80):
  1x  ('AAAAAAAAACCDAAAA', Decimal('12089.65'))
  1x  ('AAAAAAAAACDCAAAA', Decimal('5974.75'))
  1x  ('AAAAAAAAACLBAAAA', Decimal('9762.97'))
  1x  ('AAAAAAAAACNCAAAA', Decimal('3951.33'))
  1x  ('AAAAAAAAACODAAAA', Decimal('6673.40'))

## SQL size

| Source | Chars | Lines |
| --- | --- | --- |
| v4 | 3408 | 92 |
| reference | 3286 | 58 |
| v4 / ref | 1.04x | 1.59x |

## Preql

```
import unified_sales as sales;

auto music_item_ids <- sales.item.name ? sales.item.category = 'Music';

where
    sales.item.name in music_item_ids
    and sales.date.year = 1998
    and sales.date.month_of_year = 9
    and sales.bill_address.gmt_offset = -5
select
    sales.item.name,
    sum(sales.ext_sales_price) as total_sales,
order by
    sales.item.name asc,
    total_sales asc
limit 100
;
```

## v4 generated SQL

```sql
WITH 
abundant as (
SELECT
    "sales_item_items"."I_CATEGORY" as "sales_item_category",
    "sales_item_items"."I_ITEM_ID" as "sales_item_name",
    "sales_item_items"."I_ITEM_SK" as "sales_item_id"
FROM
    "memory"."item" as "sales_item_items"),
questionable as (
SELECT
    "sales_date_date"."D_DATE_SK" as "sales_date_id",
    "sales_date_date"."D_MOY" as "sales_date_month_of_year",
    "sales_date_date"."D_YEAR" as "sales_date_year"
FROM
    "memory"."date_dim" as "sales_date_date"),
thoughtful as (
SELECT
    "sales_catalog_sales_unified"."CS_BILL_ADDR_SK" as "sales_bill_address_id",
    "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "sales_date_id",
    "sales_catalog_sales_unified"."CS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_catalog_sales_unified"."CS_ITEM_SK" as "sales_item_id"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
UNION ALL
SELECT
    "sales_store_sales_unified"."SS_ADDR_SK" as "sales_bill_address_id",
    "sales_store_sales_unified"."SS_SOLD_DATE_SK" as "sales_date_id",
    "sales_store_sales_unified"."SS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_id"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
UNION ALL
SELECT
    "sales_web_sales_unified"."WS_BILL_ADDR_SK" as "sales_bill_address_id",
    "sales_web_sales_unified"."WS_SOLD_DATE_SK" as "sales_date_id",
    "sales_web_sales_unified"."WS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_id"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"),
cooperative as (
SELECT
    "thoughtful"."sales_bill_address_id" as "sales_bill_address_id",
    "thoughtful"."sales_date_id" as "sales_date_id",
    "thoughtful"."sales_ext_sales_price" as "sales_ext_sales_price",
    "thoughtful"."sales_item_id" as "sales_item_id"
FROM
    "thoughtful"
GROUP BY
    1,
    2,
    3,
    4),
quizzical as (
SELECT
    "sales_bill_address_customer_address"."CA_ADDRESS_SK" as "sales_bill_address_id",
    "sales_bill_address_customer_address"."CA_GMT_OFFSET" as "sales_bill_address_gmt_offset"
FROM
    "memory"."customer_address" as "sales_bill_address_customer_address"),
uneven as (
SELECT
    "abundant"."sales_item_category" as "sales_item_category",
    "abundant"."sales_item_name" as "sales_item_name",
    "cooperative"."sales_ext_sales_price" as "sales_ext_sales_price",
    "questionable"."sales_date_month_of_year" as "sales_date_month_of_year",
    "questionable"."sales_date_year" as "sales_date_year",
    "quizzical"."sales_bill_address_gmt_offset" as "sales_bill_address_gmt_offset"
FROM
    "cooperative"
    LEFT OUTER JOIN "questionable" on "cooperative"."sales_date_id" = "questionable"."sales_date_id"
    INNER JOIN "abundant" on "cooperative"."sales_item_id" = "abundant"."sales_item_id"
    LEFT OUTER JOIN "quizzical" on "cooperative"."sales_bill_address_id" = "quizzical"."sales_bill_address_id"
WHERE
    "questionable"."sales_date_year" = 1998 and "questionable"."sales_date_month_of_year" = 9 and "quizzical"."sales_bill_address_gmt_offset" = -5

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6)
SELECT
    sum("uneven"."sales_ext_sales_price") as "total_sales",
    "uneven"."sales_item_name" as "sales_item_name"
FROM
    "uneven"
GROUP BY
    2
ORDER BY 
    "uneven"."sales_item_name" asc,
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
    "sales_item_items"."I_ITEM_ID" as "sales_item_name"
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
    "sales_item_items"."I_ITEM_ID" as "sales_item_name"
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
    "sales_item_items"."I_ITEM_ID" as "sales_item_name"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "memory"."customer_address" as "sales_bill_address_customer_address" on "sales_web_sales_unified"."WS_BILL_ADDR_SK" = "sales_bill_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "sales_item_items" on "sales_web_sales_unified"."WS_ITEM_SK" = "sales_item_items"."I_ITEM_SK"
WHERE
    "sales_bill_address_customer_address"."CA_GMT_OFFSET" = -5 and "sales_date_date"."D_YEAR" = 1998 and "sales_date_date"."D_MOY" = 9 and "sales_item_items"."I_ITEM_ID" in (select abundant."music_item_ids" from abundant where abundant."music_item_ids" is not null)
)
SELECT
    "thoughtful"."sales_item_name" as "sales_item_name",
    sum("thoughtful"."sales_ext_sales_price") as "total_sales"
FROM
    "thoughtful"
GROUP BY
    1
ORDER BY 
    "thoughtful"."sales_item_name" asc,
    "total_sales" asc
LIMIT (100)
```
