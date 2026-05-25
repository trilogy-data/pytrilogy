# Query 43

**Status:** `exec_fail`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | FAILED |
| reference execution | OK (6 rows) |

## Result comparison

_at least one side did not produce rows._

## SQL size

| Source | Chars | Lines |
| --- | --- | --- |
| v4 | 4032 | 75 |
| reference | 1919 | 31 |
| v4 / ref | 2.10x | 2.42x |

## Preql

```
import store_sales as store_sales;

where
    store_sales.store.gmt_offset = -5 and store_sales.date.year = 2000
select
    store_sales.store.name,
    store_sales.store.text_id,
    sum(store_sales.sales_price ? store_sales.date.day_name = 'Sunday') as sun_sales,
    sum(store_sales.sales_price ? store_sales.date.day_name = 'Monday') as mon_sales,
    sum(store_sales.sales_price ? store_sales.date.day_name = 'Tuesday') as tue_sales,
    sum(store_sales.sales_price ? store_sales.date.day_name = 'Wednesday') as wed_sales,
    sum(store_sales.sales_price ? store_sales.date.day_name = 'Thursday') as thu_sales,
    sum(store_sales.sales_price ? store_sales.date.day_name = 'Friday') as fri_sales,
    sum(store_sales.sales_price ? store_sales.date.day_name = 'Saturday') as sat_sales,
order by
    store_sales.store.name asc,
    store_sales.store.text_id asc,
    sun_sales asc,
    mon_sales asc,
    tue_sales asc,
    wed_sales asc,
    thu_sales asc,
    fri_sales asc,
    sat_sales asc
limit 100
;
```

## v4 generated SQL

```sql
WITH 
wakeful as (
SELECT
    "store_sales_store_sales"."SS_SALES_PRICE" as "store_sales_sales_price",
    "store_sales_store_sales"."SS_SOLD_DATE_SK" as "store_sales_date_id",
    "store_sales_store_sales"."SS_STORE_SK" as "store_sales_store_id"
FROM
    "memory"."store_sales" as "store_sales_store_sales"),
highfalutin as (
SELECT
    "store_sales_store_store"."S_GMT_OFFSET" as "store_sales_store_gmt_offset",
    "store_sales_store_store"."S_STORE_ID" as "store_sales_store_text_id",
    "store_sales_store_store"."S_STORE_NAME" as "store_sales_store_name",
    "store_sales_store_store"."S_STORE_SK" as "store_sales_store_id"
FROM
    "memory"."store" as "store_sales_store_store"),
quizzical as (
SELECT
    "store_sales_date_date"."D_DATE_SK" as "store_sales_date_id",
    "store_sales_date_date"."D_DAY_NAME" as "store_sales_date_day_name",
    "store_sales_date_date"."D_YEAR" as "store_sales_date_year"
FROM
    "memory"."date_dim" as "store_sales_date_date"),
cheerful as (
SELECT
    "highfalutin"."store_sales_store_gmt_offset" as "store_sales_store_gmt_offset",
    "highfalutin"."store_sales_store_name" as "store_sales_store_name",
    "highfalutin"."store_sales_store_text_id" as "store_sales_store_text_id",
    "quizzical"."store_sales_date_day_name" as "store_sales_date_day_name",
    "quizzical"."store_sales_date_year" as "store_sales_date_year",
    "wakeful"."store_sales_sales_price" as "store_sales_sales_price"
FROM
    "wakeful"
    LEFT OUTER JOIN "quizzical" on "wakeful"."store_sales_date_id" = "quizzical"."store_sales_date_id"
    LEFT OUTER JOIN "highfalutin" on "wakeful"."store_sales_store_id" = "highfalutin"."store_sales_store_id"
WHERE
    "highfalutin"."store_sales_store_gmt_offset" = -5 and "quizzical"."store_sales_date_year" = 2000
),
thoughtful as (
SELECT
    CASE WHEN "cheerful"."store_sales_date_day_name" = 'Friday' THEN "cheerful"."store_sales_sales_price" ELSE NULL END as "_virt_filter_sales_price_3265047064841977",
    CASE WHEN "cheerful"."store_sales_date_day_name" = 'Monday' THEN "cheerful"."store_sales_sales_price" ELSE NULL END as "_virt_filter_sales_price_870692220845785",
    CASE WHEN "cheerful"."store_sales_date_day_name" = 'Saturday' THEN "cheerful"."store_sales_sales_price" ELSE NULL END as "_virt_filter_sales_price_4311435794136489",
    CASE WHEN "cheerful"."store_sales_date_day_name" = 'Sunday' THEN "cheerful"."store_sales_sales_price" ELSE NULL END as "_virt_filter_sales_price_615389228694613",
    CASE WHEN "cheerful"."store_sales_date_day_name" = 'Thursday' THEN "cheerful"."store_sales_sales_price" ELSE NULL END as "_virt_filter_sales_price_3382904634884294",
    CASE WHEN "cheerful"."store_sales_date_day_name" = 'Tuesday' THEN "cheerful"."store_sales_sales_price" ELSE NULL END as "_virt_filter_sales_price_1558841270808694",
    CASE WHEN "cheerful"."store_sales_date_day_name" = 'Wednesday' THEN "cheerful"."store_sales_sales_price" ELSE NULL END as "_virt_filter_sales_price_9958261983372219"
FROM
    "cheerful")
SELECT
    sum("thoughtful"."_virt_filter_sales_price_615389228694613") as "sun_sales",
    sum("thoughtful"."_virt_filter_sales_price_870692220845785") as "mon_sales",
    sum("thoughtful"."_virt_filter_sales_price_1558841270808694") as "tue_sales",
    sum("thoughtful"."_virt_filter_sales_price_9958261983372219") as "wed_sales",
    sum("thoughtful"."_virt_filter_sales_price_3382904634884294") as "thu_sales",
    sum("thoughtful"."_virt_filter_sales_price_3265047064841977") as "fri_sales",
    sum("thoughtful"."_virt_filter_sales_price_4311435794136489") as "sat_sales",
    "cheerful"."store_sales_store_name" as "store_sales_store_name",
    "cheerful"."store_sales_store_text_id" as "store_sales_store_text_id"
FROM
    "thoughtful"
GROUP BY
    8,
    9
ORDER BY 
    "cheerful"."store_sales_store_name" asc,
    "cheerful"."store_sales_store_text_id" asc,
    "sun_sales" asc,
    "mon_sales" asc,
    "tue_sales" asc,
    "wed_sales" asc,
    "thu_sales" asc,
    "fri_sales" asc,
    "sat_sales" asc
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
SELECT
    "store_sales_store_store"."S_STORE_NAME" as "store_sales_store_name",
    "store_sales_store_store"."S_STORE_ID" as "store_sales_store_text_id",
    sum(CASE WHEN "store_sales_date_date"."D_DAY_NAME" = 'Sunday' THEN "store_sales_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "sun_sales",
    sum(CASE WHEN "store_sales_date_date"."D_DAY_NAME" = 'Monday' THEN "store_sales_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "mon_sales",
    sum(CASE WHEN "store_sales_date_date"."D_DAY_NAME" = 'Tuesday' THEN "store_sales_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "tue_sales",
    sum(CASE WHEN "store_sales_date_date"."D_DAY_NAME" = 'Wednesday' THEN "store_sales_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "wed_sales",
    sum(CASE WHEN "store_sales_date_date"."D_DAY_NAME" = 'Thursday' THEN "store_sales_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "thu_sales",
    sum(CASE WHEN "store_sales_date_date"."D_DAY_NAME" = 'Friday' THEN "store_sales_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "fri_sales",
    sum(CASE WHEN "store_sales_date_date"."D_DAY_NAME" = 'Saturday' THEN "store_sales_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "sat_sales"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."store" as "store_sales_store_store" on "store_sales_store_sales"."SS_STORE_SK" = "store_sales_store_store"."S_STORE_SK"
WHERE
    "store_sales_store_store"."S_GMT_OFFSET" = -5 and "store_sales_date_date"."D_YEAR" = 2000

GROUP BY
    1,
    2
ORDER BY 
    "store_sales_store_store"."S_STORE_NAME" asc,
    "store_sales_store_store"."S_STORE_ID" asc,
    "sun_sales" asc,
    "mon_sales" asc,
    "tue_sales" asc,
    "wed_sales" asc,
    "thu_sales" asc,
    "fri_sales" asc,
    "sat_sales" asc
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
_duckdb.BinderException: Binder Error: Referenced table "cheerful" not found!
Candidate tables: "thoughtful"

LINE 58:     "cheerful"."store_sales_store_name" as "store_sales_store_name...
             ^
```
