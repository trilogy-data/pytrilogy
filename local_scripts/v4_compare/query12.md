# Query 12

**Status:** `exec_fail`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | FAILED |
| reference execution | OK (100 rows) |

## Result comparison

_at least one side did not produce rows._

## SQL size

| Source | Chars | Lines |
| --- | --- | --- |
| v4 | 3539 | 82 |
| reference | 2111 | 48 |
| v4 / ref | 1.68x | 1.71x |

## Preql

```
import web_sales as web_sales;

where
    web_sales.date.date between '1999-02-22'::date and '1999-03-24'::date
    and web_sales.item.category in ('Sports', 'Books', 'Home')
select
    web_sales.item.name,
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
    web_sales.item.name asc,
    web_sales.item.desc asc,
    revenueratio asc
limit 100
;
```

## v4 generated SQL

```sql
WITH 
wakeful as (
SELECT
    "web_sales_web_sales"."WS_EXT_SALES_PRICE" as "web_sales_ext_sales_price",
    "web_sales_web_sales"."WS_ITEM_SK" as "web_sales_item_id",
    "web_sales_web_sales"."WS_SOLD_DATE_SK" as "web_sales_date_id"
FROM
    "memory"."web_sales" as "web_sales_web_sales"),
highfalutin as (
SELECT
    "web_sales_item_items"."I_CATEGORY" as "web_sales_item_category",
    "web_sales_item_items"."I_CLASS" as "web_sales_item_class",
    "web_sales_item_items"."I_CURRENT_PRICE" as "web_sales_item_current_price",
    "web_sales_item_items"."I_ITEM_DESC" as "web_sales_item_desc",
    "web_sales_item_items"."I_ITEM_ID" as "web_sales_item_name",
    "web_sales_item_items"."I_ITEM_SK" as "web_sales_item_id"
FROM
    "memory"."item" as "web_sales_item_items"),
quizzical as (
SELECT
    "web_sales_date_date"."D_DATE_SK" as "web_sales_date_id",
    cast("web_sales_date_date"."D_DATE" as date) as "web_sales_date_date"
FROM
    "memory"."date_dim" as "web_sales_date_date"),
cheerful as (
SELECT
    "highfalutin"."web_sales_item_category" as "web_sales_item_category",
    "highfalutin"."web_sales_item_class" as "web_sales_item_class",
    "highfalutin"."web_sales_item_current_price" as "web_sales_item_current_price",
    "highfalutin"."web_sales_item_desc" as "web_sales_item_desc",
    "highfalutin"."web_sales_item_name" as "web_sales_item_name",
    "quizzical"."web_sales_date_date" as "web_sales_date_date",
    "wakeful"."web_sales_ext_sales_price" as "web_sales_ext_sales_price"
FROM
    "wakeful"
    LEFT OUTER JOIN "quizzical" on "wakeful"."web_sales_date_id" = "quizzical"."web_sales_date_id"
    INNER JOIN "highfalutin" on "wakeful"."web_sales_item_id" = "highfalutin"."web_sales_item_id"
WHERE
    "quizzical"."web_sales_date_date" BETWEEN date '1999-02-22' AND date '1999-03-24' and "highfalutin"."web_sales_item_category" in ('Sports','Books','Home')
),
thoughtful as (
SELECT
    "cheerful"."web_sales_item_category" as "web_sales_item_category",
    "cheerful"."web_sales_item_class" as "web_sales_item_class",
    "cheerful"."web_sales_item_current_price" as "web_sales_item_current_price",
    "cheerful"."web_sales_item_desc" as "web_sales_item_desc",
    "cheerful"."web_sales_item_name" as "web_sales_item_name",
    sum("cheerful"."web_sales_ext_sales_price") as "itemrevenue"
FROM
    "cheerful"
GROUP BY
    1,
    2,
    3,
    4,
    5),
cooperative as (
SELECT
    "cheerful"."web_sales_item_class" as "web_sales_item_class",
    sum("thoughtful"."itemrevenue") as "itemclassrevenue"
FROM
    "cheerful"
GROUP BY
    1)
SELECT
    ("thoughtful"."itemrevenue" * 100.0) / "cooperative"."itemclassrevenue" as "revenueratio",
    "thoughtful"."web_sales_item_current_price" as "web_sales_item_current_price",
    "thoughtful"."web_sales_item_desc" as "web_sales_item_desc",
    "thoughtful"."web_sales_item_category" as "web_sales_item_category",
    "thoughtful"."web_sales_item_name" as "web_sales_item_name",
    coalesce("cooperative"."web_sales_item_class","thoughtful"."web_sales_item_class") as "web_sales_item_class",
    "thoughtful"."itemrevenue" as "itemrevenue"
FROM
    "cooperative"
    FULL JOIN "thoughtful" on "cooperative"."web_sales_item_class" is not distinct from "thoughtful"."web_sales_item_class"
ORDER BY 
    "thoughtful"."web_sales_item_category" asc,
    coalesce("cooperative"."web_sales_item_class","thoughtful"."web_sales_item_class") asc,
    "thoughtful"."web_sales_item_name" asc,
    "thoughtful"."web_sales_item_desc" asc,
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
    "web_sales_item_items"."I_ITEM_ID" as "web_sales_item_name",
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
    "cheerful"."web_sales_item_name" as "web_sales_item_name",
    "cheerful"."web_sales_item_desc" as "web_sales_item_desc",
    "cheerful"."web_sales_item_category" as "web_sales_item_category",
    coalesce("cheerful"."web_sales_item_class","cooperative"."web_sales_item_class") as "web_sales_item_class",
    "cheerful"."web_sales_item_current_price" as "web_sales_item_current_price",
    "cheerful"."itemrevenue" as "itemrevenue",
    ("cheerful"."itemrevenue" * 100.0) / "cooperative"."itemclassrevenue" as "revenueratio"
FROM
    "cooperative"
    INNER JOIN "cheerful" on "cooperative"."web_sales_item_class" is not distinct from "cheerful"."web_sales_item_class"
ORDER BY 
    "cheerful"."web_sales_item_category" asc,
    coalesce("cheerful"."web_sales_item_class","cooperative"."web_sales_item_class") asc,
    "cheerful"."web_sales_item_name" asc,
    "cheerful"."web_sales_item_desc" asc,
    "revenueratio" asc
LIMIT (100)
```

## v4 execution error

```
Traceback (most recent call last):
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 161, in run_one
    result.v4_rows = execute(con, v4_sql)
                     ~~~~~~~^^^^^^^^^^^^^
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 102, in execute
    cursor = con.execute(sql)
_duckdb.BinderException: Binder Error: Referenced table "thoughtful" not found!
Candidate tables: "cheerful"

LINE 60:     sum("thoughtful"."itemrevenue") as "itemclassrevenue"
                 ^
```
