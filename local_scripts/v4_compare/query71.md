# Query 71

**Status:** `mismatch`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (64847 rows) |
| reference execution | OK (1031 rows) |
| results identical | NO |

## Result comparison

v4 rows: 64847 (1031 distinct)
ref rows: 1031 (1031 distinct)
only in v4 (showing up to 5 of 1031):
  179x  ('exportischolar #1', 5003001, None, 9, 3)
  23x  ('edu packnameless #9', 8004009, None, 8, 59)
  35x  ('amalgmaxi #9', 8011009, None, 19, 15)
  14x  ('corpunivamalg #3', 9016003, None, 8, 42)
  5x  ('namelesscorp #5', 6008005, Decimal('20597.76'), 17, 47)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 4315 | 83 | 279.22 ms |
| reference | 3459 | 60 | 30.12 ms |
| v4 / ref | 1.25x | 1.38x | 9.27x |

## Preql

```
import unified_sales as sales;

where
    sales.date.year = 1999
    and sales.date.month_of_year = 11
    and sales.item.manager_id = 1
    and (sales.time.meal_time = 'breakfast' or sales.time.meal_time = 'dinner')
select
    sales.item.brand_id as brand_id,
    sales.item.brand_name as brand,
    sales.time.hour as t_hour,
    sales.time.minute as t_minute,
    sum(sales.ext_sales_price) as ext_price,
order by
    ext_price desc nulls first,
    brand_id asc nulls first,
    t_hour asc nulls first,
    t_minute asc nulls first
;
```

## v4 generated SQL

```sql
WITH 
cheerful as (
SELECT
    "sales_catalog_sales_unified"."CS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_catalog_sales_unified"."CS_ITEM_SK" as "sales_item_id",
    "sales_catalog_sales_unified"."CS_SOLD_TIME_SK" as "sales_time_id"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "sales_item_items" on "sales_catalog_sales_unified"."CS_ITEM_SK" = "sales_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."time_dim" as "sales_time_time" on "sales_catalog_sales_unified"."CS_SOLD_TIME_SK" = "sales_time_time"."T_TIME_SK"
WHERE
    "sales_date_date"."D_YEAR" = 1999 and "sales_date_date"."D_MOY" = 11 and "sales_item_items"."I_MANAGER_ID" = 1 and ( "sales_time_time"."T_MEAL_TIME" = 'breakfast' or "sales_time_time"."T_MEAL_TIME" = 'dinner' )

UNION ALL
SELECT
    "sales_store_sales_unified"."SS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_id",
    "sales_store_sales_unified"."SS_SOLD_TIME_SK" as "sales_time_id"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "sales_item_items" on "sales_store_sales_unified"."SS_ITEM_SK" = "sales_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."time_dim" as "sales_time_time" on "sales_store_sales_unified"."SS_SOLD_TIME_SK" = "sales_time_time"."T_TIME_SK"
WHERE
    "sales_date_date"."D_YEAR" = 1999 and "sales_date_date"."D_MOY" = 11 and "sales_item_items"."I_MANAGER_ID" = 1 and ( "sales_time_time"."T_MEAL_TIME" = 'breakfast' or "sales_time_time"."T_MEAL_TIME" = 'dinner' )

UNION ALL
SELECT
    "sales_web_sales_unified"."WS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_id",
    "sales_web_sales_unified"."WS_SOLD_TIME_SK" as "sales_time_id"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "sales_item_items" on "sales_web_sales_unified"."WS_ITEM_SK" = "sales_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."time_dim" as "sales_time_time" on "sales_web_sales_unified"."WS_SOLD_TIME_SK" = "sales_time_time"."T_TIME_SK"
WHERE
    "sales_date_date"."D_YEAR" = 1999 and "sales_date_date"."D_MOY" = 11 and "sales_item_items"."I_MANAGER_ID" = 1 and ( "sales_time_time"."T_MEAL_TIME" = 'breakfast' or "sales_time_time"."T_MEAL_TIME" = 'dinner' )
),
abundant as (
SELECT
    "sales_item_items"."I_BRAND" as "sales_item_brand_name",
    "sales_item_items"."I_BRAND_ID" as "sales_item_brand_id",
    "sales_time_time"."T_HOUR" as "sales_time_hour",
    "sales_time_time"."T_MINUTE" as "sales_time_minute",
    sum("cheerful"."sales_ext_sales_price") as "ext_price"
FROM
    "cheerful"
    INNER JOIN "memory"."item" as "sales_item_items" on "cheerful"."sales_item_id" = "sales_item_items"."I_ITEM_SK"
    LEFT OUTER JOIN "memory"."time_dim" as "sales_time_time" on "cheerful"."sales_time_id" = "sales_time_time"."T_TIME_SK"
GROUP BY
    1,
    2,
    3,
    4),
juicy as (
SELECT
    "abundant"."sales_time_hour" as "t_hour",
    "abundant"."sales_time_minute" as "t_minute"
FROM
    "abundant"),
yummy as (
SELECT
    "abundant"."sales_item_brand_id" as "brand_id",
    "abundant"."sales_item_brand_name" as "brand"
FROM
    "abundant")
SELECT
    "yummy"."brand_id" as "brand_id",
    "yummy"."brand" as "brand",
    "juicy"."t_hour" as "t_hour",
    "juicy"."t_minute" as "t_minute",
    "abundant"."ext_price" as "ext_price"
FROM
    "abundant"
    INNER JOIN "yummy" on "abundant"."sales_item_brand_id" = "yummy"."brand_id" AND "abundant"."sales_item_brand_name" = "yummy"."brand"
    LEFT OUTER JOIN "juicy" on "abundant"."sales_time_hour" = "juicy"."t_hour" AND "abundant"."sales_time_minute" = "juicy"."t_minute"
ORDER BY 
    "abundant"."ext_price" desc nulls first,
    "yummy"."brand_id" asc nulls first,
    "juicy"."t_hour" asc nulls first,
    "juicy"."t_minute" asc nulls first
```

## Reference SQL (zquery log)

```sql
WITH 
cheerful as (
SELECT
    "sales_catalog_sales_unified"."CS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_catalog_sales_unified"."CS_ITEM_SK" as "sales_item_id",
    "sales_catalog_sales_unified"."CS_SOLD_TIME_SK" as "sales_time_id"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "sales_item_items" on "sales_catalog_sales_unified"."CS_ITEM_SK" = "sales_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."time_dim" as "sales_time_time" on "sales_catalog_sales_unified"."CS_SOLD_TIME_SK" = "sales_time_time"."T_TIME_SK"
WHERE
    "sales_date_date"."D_YEAR" = 1999 and "sales_date_date"."D_MOY" = 11 and "sales_item_items"."I_MANAGER_ID" = 1 and ( "sales_time_time"."T_MEAL_TIME" = 'breakfast' or "sales_time_time"."T_MEAL_TIME" = 'dinner' )

UNION ALL
SELECT
    "sales_store_sales_unified"."SS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_id",
    "sales_store_sales_unified"."SS_SOLD_TIME_SK" as "sales_time_id"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "sales_item_items" on "sales_store_sales_unified"."SS_ITEM_SK" = "sales_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."time_dim" as "sales_time_time" on "sales_store_sales_unified"."SS_SOLD_TIME_SK" = "sales_time_time"."T_TIME_SK"
WHERE
    "sales_date_date"."D_YEAR" = 1999 and "sales_date_date"."D_MOY" = 11 and "sales_item_items"."I_MANAGER_ID" = 1 and ( "sales_time_time"."T_MEAL_TIME" = 'breakfast' or "sales_time_time"."T_MEAL_TIME" = 'dinner' )

UNION ALL
SELECT
    "sales_web_sales_unified"."WS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_id",
    "sales_web_sales_unified"."WS_SOLD_TIME_SK" as "sales_time_id"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "sales_item_items" on "sales_web_sales_unified"."WS_ITEM_SK" = "sales_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."time_dim" as "sales_time_time" on "sales_web_sales_unified"."WS_SOLD_TIME_SK" = "sales_time_time"."T_TIME_SK"
WHERE
    "sales_date_date"."D_YEAR" = 1999 and "sales_date_date"."D_MOY" = 11 and "sales_item_items"."I_MANAGER_ID" = 1 and ( "sales_time_time"."T_MEAL_TIME" = 'breakfast' or "sales_time_time"."T_MEAL_TIME" = 'dinner' )
)
SELECT
    "sales_item_items"."I_BRAND_ID" as "brand_id",
    "sales_item_items"."I_BRAND" as "brand",
    "sales_time_time"."T_HOUR" as "t_hour",
    "sales_time_time"."T_MINUTE" as "t_minute",
    sum("cheerful"."sales_ext_sales_price") as "ext_price"
FROM
    "cheerful"
    INNER JOIN "memory"."item" as "sales_item_items" on "cheerful"."sales_item_id" = "sales_item_items"."I_ITEM_SK"
    LEFT OUTER JOIN "memory"."time_dim" as "sales_time_time" on "cheerful"."sales_time_id" = "sales_time_time"."T_TIME_SK"
GROUP BY
    1,
    2,
    3,
    4
ORDER BY 
    "ext_price" desc nulls first,
    "brand_id" asc nulls first,
    "t_hour" asc nulls first,
    "t_minute" asc nulls first
```
