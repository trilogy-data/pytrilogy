# Query 32

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (1 rows) |
| reference execution | OK (1 rows) |
| results identical | YES |

## Result comparison

v4 rows: 1 (1 distinct)
ref rows: 1 (1 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 1397 | 36 | 30.44 ms |
| reference | 1242 | 33 | 11.47 ms |
| v4 / ref | 1.12x | 1.09x | 2.65x |

## Preql

```
import catalog_sales;

const start_date <- '2000-01-27'::date;
const end_date <- '2000-04-26'::date;

# Transform this tpc-ds sql query to trilogy following trilogy syntax
auto avg_item_disc <- 1.3 * avg(discount_amount ? sold_date.date between start_date and end_date) by item.id;

where
    item.manufacturer_id = 977
    and sold_date.date between start_date and end_date
    and discount_amount > avg_item_disc
select
    sum(discount_amount) as total_discount,
limit 100
;
```

## v4 generated SQL

```sql
WITH 
thoughtful as (
SELECT
    "catalog_sales"."CS_ITEM_SK" as "item_id",
    avg(CASE WHEN cast("sold_date_date"."D_DATE" as date) BETWEEN :start_date AND :end_date THEN "catalog_sales"."CS_EXT_DISCOUNT_AMT" ELSE NULL END) as "_virt_agg_avg_5510773609506287"
FROM
    "memory"."catalog_sales" as "catalog_sales"
    LEFT OUTER JOIN "memory"."date_dim" as "sold_date_date" on "catalog_sales"."CS_SOLD_DATE_SK" = "sold_date_date"."D_DATE_SK"
GROUP BY
    1),
cheerful as (
SELECT
    "catalog_sales"."CS_EXT_DISCOUNT_AMT" as "discount_amount",
    "catalog_sales"."CS_ITEM_SK" as "item_id"
FROM
    "memory"."catalog_sales" as "catalog_sales"
    INNER JOIN "memory"."item" as "item_items" on "catalog_sales"."CS_ITEM_SK" = "item_items"."I_ITEM_SK"
    INNER JOIN "memory"."date_dim" as "sold_date_date" on "catalog_sales"."CS_SOLD_DATE_SK" = "sold_date_date"."D_DATE_SK"
WHERE
    "item_items"."I_MANUFACT_ID" = 977 and cast("sold_date_date"."D_DATE" as date) BETWEEN :start_date AND :end_date
),
abundant as (
SELECT
    "thoughtful"."item_id" as "item_id",
    1.3 * "thoughtful"."_virt_agg_avg_5510773609506287" as "avg_item_disc"
FROM
    "thoughtful")
SELECT
    sum("cheerful"."discount_amount") as "total_discount"
FROM
    "cheerful"
    INNER JOIN "abundant" on "cheerful"."item_id" = "abundant"."item_id"
WHERE
    "cheerful"."discount_amount" > "abundant"."avg_item_disc"

LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
cheerful as (
SELECT
    "catalog_sales"."CS_EXT_DISCOUNT_AMT" as "discount_amount",
    "catalog_sales"."CS_ITEM_SK" as "item_id"
FROM
    "memory"."catalog_sales" as "catalog_sales"
    INNER JOIN "memory"."item" as "item_items" on "catalog_sales"."CS_ITEM_SK" = "item_items"."I_ITEM_SK"
    INNER JOIN "memory"."date_dim" as "sold_date_date" on "catalog_sales"."CS_SOLD_DATE_SK" = "sold_date_date"."D_DATE_SK"
WHERE
    "item_items"."I_MANUFACT_ID" = 977 and cast("sold_date_date"."D_DATE" as date) BETWEEN :start_date AND :end_date
),
thoughtful as (
SELECT
    "catalog_sales"."CS_ITEM_SK" as "item_id",
    avg("catalog_sales"."CS_EXT_DISCOUNT_AMT") as "_virt_agg_avg_5510773609506287"
FROM
    "memory"."catalog_sales" as "catalog_sales"
    INNER JOIN "memory"."date_dim" as "sold_date_date" on "catalog_sales"."CS_SOLD_DATE_SK" = "sold_date_date"."D_DATE_SK"
WHERE
    cast("sold_date_date"."D_DATE" as date) BETWEEN :start_date AND :end_date

GROUP BY
    1)
SELECT
    sum("cheerful"."discount_amount") as "total_discount"
FROM
    "cheerful"
    INNER JOIN "thoughtful" on "cheerful"."item_id" = "thoughtful"."item_id"
WHERE
    "cheerful"."discount_amount" > 1.3 * "thoughtful"."_virt_agg_avg_5510773609506287"

LIMIT (100)
```
