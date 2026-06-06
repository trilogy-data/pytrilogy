# Query 12

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
| v4 | 4135 | 85 | 105.18 ms |
| reference | 2029 | 48 | 31.42 ms |
| v4 / ref | 2.04x | 1.77x | 3.35x |

## Preql

```
import web_sales as web_sales;

where
    web_sales.date.date between '1999-02-22'::date and '1999-03-24'::date
    and web_sales.item.category in ('Sports', 'Books', 'Home')
select
    web_sales.item.text_id,
    web_sales.item.desc,
    web_sales.item.category,
    web_sales.item.class,
    web_sales.item.current_price,
    sum(web_sales.ext_sales_price) as itemrevenue,
    --sum(itemrevenue) by web_sales.item.class as itemclassrevenue,
    (itemrevenue * 100.0) / itemclassrevenue as revenueratio,
order by
    web_sales.item.category asc,
    web_sales.item.class asc,
    web_sales.item.text_id asc,
    web_sales.item.desc asc,
    revenueratio asc
limit 100
;
```

## v4 generated SQL

```sql
WITH 
questionable as (
SELECT
    "web_sales_web_sales"."WS_ITEM_SK" as "web_sales_item_id",
    "web_sales_web_sales"."WS_SOLD_DATE_SK" as "web_sales_date_id"
FROM
    "memory"."web_sales" as "web_sales_web_sales"
GROUP BY
    1,
    2),
cheerful as (
SELECT
    "web_sales_item_items"."I_CATEGORY" as "web_sales_item_category",
    "web_sales_item_items"."I_CLASS" as "web_sales_item_class",
    "web_sales_item_items"."I_CURRENT_PRICE" as "web_sales_item_current_price",
    "web_sales_item_items"."I_ITEM_DESC" as "web_sales_item_desc",
    "web_sales_item_items"."I_ITEM_ID" as "web_sales_item_text_id",
    sum("web_sales_web_sales"."WS_EXT_SALES_PRICE") as "itemrevenue"
FROM
    "memory"."web_sales" as "web_sales_web_sales"
    INNER JOIN "memory"."date_dim" as "web_sales_date_date" on "web_sales_web_sales"."WS_SOLD_DATE_SK" = "web_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "web_sales_item_items" on "web_sales_web_sales"."WS_ITEM_SK" = "web_sales_item_items"."I_ITEM_SK"
WHERE
    cast("web_sales_date_date"."D_DATE" as date) BETWEEN date '1999-02-22' AND date '1999-03-24' and "web_sales_item_items"."I_CATEGORY" in ('Sports','Books','Home')

GROUP BY
    1,
    2,
    3,
    4,
    5),
abundant as (
SELECT
    "web_sales_item_items"."I_CATEGORY" as "web_sales_item_category",
    "web_sales_item_items"."I_CLASS" as "web_sales_item_class",
    "web_sales_item_items"."I_CURRENT_PRICE" as "web_sales_item_current_price",
    "web_sales_item_items"."I_ITEM_DESC" as "web_sales_item_desc",
    "web_sales_item_items"."I_ITEM_ID" as "web_sales_item_text_id"
FROM
    "questionable"
    INNER JOIN "memory"."date_dim" as "web_sales_date_date" on "questionable"."web_sales_date_id" = "web_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "web_sales_item_items" on "questionable"."web_sales_item_id" = "web_sales_item_items"."I_ITEM_SK"
WHERE
    cast("web_sales_date_date"."D_DATE" as date) BETWEEN date '1999-02-22' AND date '1999-03-24' and "web_sales_item_items"."I_CATEGORY" in ('Sports','Books','Home')
),
uneven as (
SELECT
    "cheerful"."itemrevenue" as "itemrevenue",
    coalesce("abundant"."web_sales_item_class","cheerful"."web_sales_item_class") as "web_sales_item_class"
FROM
    "cheerful"
    FULL JOIN "abundant" on "cheerful"."web_sales_item_category" is not distinct from "abundant"."web_sales_item_category" AND "cheerful"."web_sales_item_class" is not distinct from "abundant"."web_sales_item_class" AND "cheerful"."web_sales_item_current_price" is not distinct from "abundant"."web_sales_item_current_price" AND "cheerful"."web_sales_item_desc" is not distinct from "abundant"."web_sales_item_desc" AND "cheerful"."web_sales_item_text_id" = "abundant"."web_sales_item_text_id"
GROUP BY
    1,
    2,
    coalesce("abundant"."web_sales_item_category","cheerful"."web_sales_item_category"),
    coalesce("abundant"."web_sales_item_current_price","cheerful"."web_sales_item_current_price"),
    coalesce("abundant"."web_sales_item_desc","cheerful"."web_sales_item_desc"),
    coalesce("abundant"."web_sales_item_text_id","cheerful"."web_sales_item_text_id")),
juicy as (
SELECT
    "uneven"."web_sales_item_class" as "web_sales_item_class",
    sum("uneven"."itemrevenue") as "itemclassrevenue"
FROM
    "uneven"
GROUP BY
    1)
SELECT
    "cheerful"."web_sales_item_text_id" as "web_sales_item_text_id",
    "cheerful"."web_sales_item_desc" as "web_sales_item_desc",
    "cheerful"."web_sales_item_category" as "web_sales_item_category",
    "cheerful"."web_sales_item_class" as "web_sales_item_class",
    "cheerful"."web_sales_item_current_price" as "web_sales_item_current_price",
    "cheerful"."itemrevenue" as "itemrevenue",
    ("cheerful"."itemrevenue" * 100.0) / "juicy"."itemclassrevenue" as "revenueratio"
FROM
    "cheerful"
    INNER JOIN "juicy" on "cheerful"."web_sales_item_class" is not distinct from "juicy"."web_sales_item_class"
ORDER BY 
    "cheerful"."web_sales_item_category" asc,
    "cheerful"."web_sales_item_class" asc,
    "cheerful"."web_sales_item_text_id" asc,
    "cheerful"."web_sales_item_desc" asc,
    "revenueratio" asc
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
cheerful as (
SELECT
    "web_sales_item_items"."I_CATEGORY" as "web_sales_item_category",
    "web_sales_item_items"."I_CLASS" as "web_sales_item_class",
    "web_sales_item_items"."I_CURRENT_PRICE" as "web_sales_item_current_price",
    "web_sales_item_items"."I_ITEM_DESC" as "web_sales_item_desc",
    "web_sales_item_items"."I_ITEM_ID" as "web_sales_item_text_id",
    sum("web_sales_web_sales"."WS_EXT_SALES_PRICE") as "itemrevenue"
FROM
    "memory"."web_sales" as "web_sales_web_sales"
    INNER JOIN "memory"."date_dim" as "web_sales_date_date" on "web_sales_web_sales"."WS_SOLD_DATE_SK" = "web_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "web_sales_item_items" on "web_sales_web_sales"."WS_ITEM_SK" = "web_sales_item_items"."I_ITEM_SK"
WHERE
    cast("web_sales_date_date"."D_DATE" as date) BETWEEN date '1999-02-22' AND date '1999-03-24' and "web_sales_item_items"."I_CATEGORY" in ('Sports','Books','Home')

GROUP BY
    1,
    2,
    3,
    4,
    5),
cooperative as (
SELECT
    "cheerful"."web_sales_item_class" as "web_sales_item_class",
    sum("cheerful"."itemrevenue") as "itemclassrevenue"
FROM
    "cheerful"
GROUP BY
    1)
SELECT
    "cheerful"."web_sales_item_text_id" as "web_sales_item_text_id",
    "cheerful"."web_sales_item_desc" as "web_sales_item_desc",
    "cheerful"."web_sales_item_category" as "web_sales_item_category",
    "cheerful"."web_sales_item_class" as "web_sales_item_class",
    "cheerful"."web_sales_item_current_price" as "web_sales_item_current_price",
    "cheerful"."itemrevenue" as "itemrevenue",
    ("cheerful"."itemrevenue" * 100.0) / "cooperative"."itemclassrevenue" as "revenueratio"
FROM
    "cheerful"
    INNER JOIN "cooperative" on "cheerful"."web_sales_item_class" is not distinct from "cooperative"."web_sales_item_class"
ORDER BY 
    "cheerful"."web_sales_item_category" asc,
    "cheerful"."web_sales_item_class" asc,
    "cheerful"."web_sales_item_text_id" asc,
    "cheerful"."web_sales_item_desc" asc,
    "revenueratio" asc
LIMIT (100)
```
