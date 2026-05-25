# Query 02

**Status:** `exec_fail`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | FAILED |
| reference execution | OK (53 rows) |

## Result comparison

_at least one side did not produce rows._

## SQL size

| Source | Chars | Lines |
| --- | --- | --- |
| v4 | 8105 | 141 |
| reference | 4649 | 80 |
| v4 / ref | 1.74x | 1.76x |

## Preql

```
import unified_sales as sales;

auto relevent_week_seq <- sales.date.week_seq ? sales.date.year in (2001, 2002);

def weekday_sum(weekday) -> sum(sales.ext_sales_price ? sales.date.day_of_week = weekday) by sales.date.week_seq;
def round_lag(amt) -> round(amt / (lead(amt, 53) over (order by sales.date.week_seq asc)), 2);

where
    sales.sales_channel in ('WEB', 'CATALOG') and sales.date.week_seq in relevent_week_seq
select
    sales.date.week_seq,
    @round_lag(@weekday_sum(0)) as sunday_increase,
    @round_lag(@weekday_sum(1)) as monday_increase,
    @round_lag(@weekday_sum(2)) as tuesday_increase,
    @round_lag(@weekday_sum(3)) as wednesday_increase,
    @round_lag(@weekday_sum(4)) as thursday_increase,
    @round_lag(@weekday_sum(5)) as friday_increase,
    @round_lag(@weekday_sum(6)) as saturday_increase,
having
    sunday_increase is not null

order by
    sales.date.week_seq asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
cooperative as (
SELECT
    "sales_date_date"."D_DATE_SK" as "sales_date_id",
    "sales_date_date"."D_DOW" as "sales_date_day_of_week",
    "sales_date_date"."D_WEEK_SEQ" as "sales_date_week_seq",
    "sales_date_date"."D_YEAR" as "sales_date_year"
FROM
    "memory"."date_dim" as "sales_date_date"),
cheerful as (
SELECT
    "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "sales_date_id",
    "sales_catalog_sales_unified"."CS_EXT_SALES_PRICE" as "sales_ext_sales_price",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
UNION ALL
SELECT
    "sales_store_sales_unified"."SS_SOLD_DATE_SK" as "sales_date_id",
    "sales_store_sales_unified"."SS_EXT_SALES_PRICE" as "sales_ext_sales_price",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
UNION ALL
SELECT
    "sales_web_sales_unified"."WS_SOLD_DATE_SK" as "sales_date_id",
    "sales_web_sales_unified"."WS_EXT_SALES_PRICE" as "sales_ext_sales_price",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"),
thoughtful as (
SELECT
    "cheerful"."sales_date_id" as "sales_date_id",
    "cheerful"."sales_ext_sales_price" as "sales_ext_sales_price",
    "cheerful"."sales_sales_channel" as "sales_sales_channel"
FROM
    "cheerful"
GROUP BY
    1,
    2,
    3),
questionable as (
SELECT
    "cooperative"."sales_date_day_of_week" as "sales_date_day_of_week",
    "cooperative"."sales_date_week_seq" as "sales_date_week_seq",
    "cooperative"."sales_date_year" as "sales_date_year",
    "thoughtful"."sales_ext_sales_price" as "sales_ext_sales_price",
    "thoughtful"."sales_sales_channel" as "sales_sales_channel"
FROM
    "thoughtful"
    LEFT OUTER JOIN "cooperative" on "thoughtful"."sales_date_id" = "cooperative"."sales_date_id"
WHERE
    "thoughtful"."sales_sales_channel" in ('WEB','CATALOG')

GROUP BY
    1,
    2,
    3,
    4,
    5),
abundant as (
SELECT
    CASE WHEN "questionable"."sales_date_day_of_week" = 0 THEN "questionable"."sales_ext_sales_price" ELSE NULL END as "_virt_filter_ext_sales_price_6193604629288196",
    CASE WHEN "questionable"."sales_date_day_of_week" = 1 THEN "questionable"."sales_ext_sales_price" ELSE NULL END as "_virt_filter_ext_sales_price_2309616592158297",
    CASE WHEN "questionable"."sales_date_day_of_week" = 2 THEN "questionable"."sales_ext_sales_price" ELSE NULL END as "_virt_filter_ext_sales_price_8408694516239689",
    CASE WHEN "questionable"."sales_date_day_of_week" = 3 THEN "questionable"."sales_ext_sales_price" ELSE NULL END as "_virt_filter_ext_sales_price_9902093486763843",
    CASE WHEN "questionable"."sales_date_day_of_week" = 4 THEN "questionable"."sales_ext_sales_price" ELSE NULL END as "_virt_filter_ext_sales_price_7643134029038705",
    CASE WHEN "questionable"."sales_date_day_of_week" = 5 THEN "questionable"."sales_ext_sales_price" ELSE NULL END as "_virt_filter_ext_sales_price_7358070075058354",
    CASE WHEN "questionable"."sales_date_day_of_week" = 6 THEN "questionable"."sales_ext_sales_price" ELSE NULL END as "_virt_filter_ext_sales_price_462418217325741"
FROM
    "questionable"),
uneven as (
SELECT
    "questionable"."sales_date_week_seq" as "sales_date_week_seq",
    sum("abundant"."_virt_filter_ext_sales_price_2309616592158297") as "_virt_agg_sum_1215995592885356",
    sum("abundant"."_virt_filter_ext_sales_price_462418217325741") as "_virt_agg_sum_3160525683686265",
    sum("abundant"."_virt_filter_ext_sales_price_6193604629288196") as "_virt_agg_sum_5898269946212687",
    sum("abundant"."_virt_filter_ext_sales_price_7358070075058354") as "_virt_agg_sum_1755492547499297",
    sum("abundant"."_virt_filter_ext_sales_price_7643134029038705") as "_virt_agg_sum_3226984322777641",
    sum("abundant"."_virt_filter_ext_sales_price_8408694516239689") as "_virt_agg_sum_5503961012463124",
    sum("abundant"."_virt_filter_ext_sales_price_9902093486763843") as "_virt_agg_sum_6232287870778562"
FROM
    "abundant"
GROUP BY
    1),
yummy as (
SELECT
    "questionable"."sales_date_week_seq" as "sales_date_week_seq",
    lead("uneven"."_virt_agg_sum_1215995592885356", 53) over (order by "questionable"."sales_date_week_seq" asc ) as "_virt_window_lead_8434916643189094",
    lead("uneven"."_virt_agg_sum_1755492547499297", 53) over (order by "questionable"."sales_date_week_seq" asc ) as "_virt_window_lead_1513977696668684",
    lead("uneven"."_virt_agg_sum_3160525683686265", 53) over (order by "questionable"."sales_date_week_seq" asc ) as "_virt_window_lead_6726398054446491",
    lead("uneven"."_virt_agg_sum_3226984322777641", 53) over (order by "questionable"."sales_date_week_seq" asc ) as "_virt_window_lead_7589933802981203",
    lead("uneven"."_virt_agg_sum_5503961012463124", 53) over (order by "questionable"."sales_date_week_seq" asc ) as "_virt_window_lead_5402686874923245",
    lead("uneven"."_virt_agg_sum_5898269946212687", 53) over (order by "questionable"."sales_date_week_seq" asc ) as "_virt_window_lead_3355739386573542",
    lead("uneven"."_virt_agg_sum_6232287870778562", 53) over (order by "questionable"."sales_date_week_seq" asc ) as "_virt_window_lead_8846802885933861"
FROM
    "uneven"),
juicy as (
SELECT
    "uneven"."_virt_agg_sum_1215995592885356" as "_virt_agg_sum_1215995592885356",
    "uneven"."_virt_agg_sum_1755492547499297" as "_virt_agg_sum_1755492547499297",
    "uneven"."_virt_agg_sum_3160525683686265" as "_virt_agg_sum_3160525683686265",
    "uneven"."_virt_agg_sum_3226984322777641" as "_virt_agg_sum_3226984322777641",
    "uneven"."_virt_agg_sum_5503961012463124" as "_virt_agg_sum_5503961012463124",
    "uneven"."_virt_agg_sum_5898269946212687" as "_virt_agg_sum_5898269946212687",
    "uneven"."_virt_agg_sum_6232287870778562" as "_virt_agg_sum_6232287870778562",
    "uneven"."sales_date_week_seq" as "sales_date_week_seq",
    "yummy"."_virt_window_lead_1513977696668684" as "_virt_window_lead_1513977696668684",
    "yummy"."_virt_window_lead_3355739386573542" as "_virt_window_lead_3355739386573542",
    "yummy"."_virt_window_lead_5402686874923245" as "_virt_window_lead_5402686874923245",
    "yummy"."_virt_window_lead_6726398054446491" as "_virt_window_lead_6726398054446491",
    "yummy"."_virt_window_lead_7589933802981203" as "_virt_window_lead_7589933802981203",
    "yummy"."_virt_window_lead_8434916643189094" as "_virt_window_lead_8434916643189094",
    "yummy"."_virt_window_lead_8846802885933861" as "_virt_window_lead_8846802885933861",
    round("uneven"."_virt_agg_sum_1215995592885356" / ("yummy"."_virt_window_lead_8434916643189094"),2) as "monday_increase",
    round("uneven"."_virt_agg_sum_1755492547499297" / ("yummy"."_virt_window_lead_1513977696668684"),2) as "friday_increase",
    round("uneven"."_virt_agg_sum_3160525683686265" / ("yummy"."_virt_window_lead_6726398054446491"),2) as "saturday_increase",
    round("uneven"."_virt_agg_sum_3226984322777641" / ("yummy"."_virt_window_lead_7589933802981203"),2) as "thursday_increase",
    round("uneven"."_virt_agg_sum_5503961012463124" / ("yummy"."_virt_window_lead_5402686874923245"),2) as "tuesday_increase",
    round("uneven"."_virt_agg_sum_5898269946212687" / ("yummy"."_virt_window_lead_3355739386573542"),2) as "sunday_increase",
    round("uneven"."_virt_agg_sum_6232287870778562" / ("yummy"."_virt_window_lead_8846802885933861"),2) as "wednesday_increase"
FROM
    "uneven"
    LEFT OUTER JOIN "yummy" on "uneven"."sales_date_week_seq" = "yummy"."sales_date_week_seq")
SELECT
    "juicy"."sales_date_week_seq" as "sales_date_week_seq",
    "juicy"."sunday_increase" as "sunday_increase",
    "juicy"."monday_increase" as "monday_increase",
    "juicy"."tuesday_increase" as "tuesday_increase",
    "juicy"."wednesday_increase" as "wednesday_increase",
    "juicy"."thursday_increase" as "thursday_increase",
    "juicy"."friday_increase" as "friday_increase",
    "juicy"."saturday_increase" as "saturday_increase"
FROM
    "juicy"
WHERE
    "juicy"."sunday_increase" is not null

ORDER BY 
    "juicy"."sales_date_week_seq" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
cooperative as (
SELECT
    "sales_date_date"."D_WEEK_SEQ" as "relevent_week_seq"
FROM
    "memory"."date_dim" as "sales_date_date"
WHERE
    "sales_date_date"."D_YEAR" in (2001,2002)

GROUP BY
    1),
cheerful as (
SELECT
    "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "sales_date_id",
    "sales_catalog_sales_unified"."CS_EXT_SALES_PRICE" as "sales_ext_sales_price",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_date_date"."D_WEEK_SEQ" in (select cooperative."relevent_week_seq" from cooperative where cooperative."relevent_week_seq" is not null)

UNION ALL
SELECT
    "sales_web_sales_unified"."WS_SOLD_DATE_SK" as "sales_date_id",
    "sales_web_sales_unified"."WS_EXT_SALES_PRICE" as "sales_ext_sales_price",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_date_date"."D_WEEK_SEQ" in (select cooperative."relevent_week_seq" from cooperative where cooperative."relevent_week_seq" is not null)
),
abundant as (
SELECT
    "sales_date_date"."D_WEEK_SEQ" as "sales_date_week_seq",
    sum(CASE WHEN "sales_date_date"."D_DOW" = 0 THEN "cheerful"."sales_ext_sales_price" ELSE NULL END) as "_virt_agg_sum_5898269946212687",
    sum(CASE WHEN "sales_date_date"."D_DOW" = 1 THEN "cheerful"."sales_ext_sales_price" ELSE NULL END) as "_virt_agg_sum_1215995592885356",
    sum(CASE WHEN "sales_date_date"."D_DOW" = 2 THEN "cheerful"."sales_ext_sales_price" ELSE NULL END) as "_virt_agg_sum_5503961012463124",
    sum(CASE WHEN "sales_date_date"."D_DOW" = 3 THEN "cheerful"."sales_ext_sales_price" ELSE NULL END) as "_virt_agg_sum_6232287870778562",
    sum(CASE WHEN "sales_date_date"."D_DOW" = 4 THEN "cheerful"."sales_ext_sales_price" ELSE NULL END) as "_virt_agg_sum_3226984322777641",
    sum(CASE WHEN "sales_date_date"."D_DOW" = 5 THEN "cheerful"."sales_ext_sales_price" ELSE NULL END) as "_virt_agg_sum_1755492547499297",
    sum(CASE WHEN "sales_date_date"."D_DOW" = 6 THEN "cheerful"."sales_ext_sales_price" ELSE NULL END) as "_virt_agg_sum_3160525683686265"
FROM
    "cheerful"
    LEFT OUTER JOIN "memory"."date_dim" as "sales_date_date" on "cheerful"."sales_date_id" = "sales_date_date"."D_DATE_SK"
WHERE
    "cheerful"."sales_sales_channel" in ('WEB','CATALOG')

GROUP BY
    1),
juicy as (
SELECT
    "abundant"."sales_date_week_seq" as "sales_date_week_seq",
    round("abundant"."_virt_agg_sum_1215995592885356" / (lead("abundant"."_virt_agg_sum_1215995592885356", 53) over (order by "abundant"."sales_date_week_seq" asc )),2) as "monday_increase",
    round("abundant"."_virt_agg_sum_1755492547499297" / (lead("abundant"."_virt_agg_sum_1755492547499297", 53) over (order by "abundant"."sales_date_week_seq" asc )),2) as "friday_increase",
    round("abundant"."_virt_agg_sum_3160525683686265" / (lead("abundant"."_virt_agg_sum_3160525683686265", 53) over (order by "abundant"."sales_date_week_seq" asc )),2) as "saturday_increase",
    round("abundant"."_virt_agg_sum_3226984322777641" / (lead("abundant"."_virt_agg_sum_3226984322777641", 53) over (order by "abundant"."sales_date_week_seq" asc )),2) as "thursday_increase",
    round("abundant"."_virt_agg_sum_5503961012463124" / (lead("abundant"."_virt_agg_sum_5503961012463124", 53) over (order by "abundant"."sales_date_week_seq" asc )),2) as "tuesday_increase",
    round("abundant"."_virt_agg_sum_5898269946212687" / (lead("abundant"."_virt_agg_sum_5898269946212687", 53) over (order by "abundant"."sales_date_week_seq" asc )),2) as "sunday_increase",
    round("abundant"."_virt_agg_sum_6232287870778562" / (lead("abundant"."_virt_agg_sum_6232287870778562", 53) over (order by "abundant"."sales_date_week_seq" asc )),2) as "wednesday_increase"
FROM
    "abundant")
SELECT
    "juicy"."sales_date_week_seq" as "sales_date_week_seq",
    "juicy"."sunday_increase" as "sunday_increase",
    "juicy"."monday_increase" as "monday_increase",
    "juicy"."tuesday_increase" as "tuesday_increase",
    "juicy"."wednesday_increase" as "wednesday_increase",
    "juicy"."thursday_increase" as "thursday_increase",
    "juicy"."friday_increase" as "friday_increase",
    "juicy"."saturday_increase" as "saturday_increase"
FROM
    "juicy"
WHERE
    "juicy"."sunday_increase" is not null

ORDER BY 
    "juicy"."sales_date_week_seq" asc nulls first
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
_duckdb.BinderException: Binder Error: Referenced table "questionable" not found!
Candidate tables: "abundant"

LINE 74:     "questionable"."sales_date_week_seq" as "sales_date_week_seq...
             ^
```
