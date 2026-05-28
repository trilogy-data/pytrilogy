# Query 58

**Status:** `mismatch`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (0 rows) |
| reference execution | OK (5 rows) |
| results identical | NO |

## Result comparison

v4 rows: 0 (0 distinct)
ref rows: 5 (5 distinct)
only in ref (showing up to 5 of 5):
  1x  (3965.2999999999997, 100.0882657049908, Decimal('3968.80'), 'AAAAAAAAEHEBAAAA', 103.71926462058356, Decimal('4112.78'), 96.19246967442565, Decimal('3814.32'))
  1x  (4220.856666666667, 98.1085198344412, Decimal('4141.02'), 'AAAAAAAAFDKBAAAA', 100.98281786398813, Decimal('4262.34'), 100.9086623015707, Decimal('4259.21'))
  1x  (1909.17, 96.39529219503763, Decimal('1840.35'), 'AAAAAAAAGOPDAAAA', 105.04879083580822, Decimal('2005.56'), 98.55591696915413, Decimal('1881.60'))
  1x  (2884.3633333333332, 103.63846903244969, Decimal('2989.31'), 'AAAAAAAAOMOAAAAA', 95.33299665206303, Decimal('2749.75'), 101.0285343154873, Decimal('2914.03'))
  1x  (1395.6833333333334, 100.9204571237506, Decimal('1408.53'), 'AAAAAAAAPGOCAAAA', 96.28975054035658, Decimal('1343.90'), 102.7897923358928, Decimal('1434.62'))

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 4248 | 99 | 278.47 ms |
| reference | 5265 | 78 | 100.72 ms |
| v4 / ref | 0.81x | 1.27x | 2.76x |

## Preql

```
import unified_sales as sales;
import date as date;

auto target_week_seq <- date.week_seq ? date._date_string = '2000-01-03';

def channel_item_rev(channel) -> sum(sales.ext_sales_price ? sales.sales_channel = channel) by sales.item.name;

auto ss_item_rev <- sum(sales.ext_sales_price ? sales.sales_channel = 'STORE') by sales.item.name;
auto cs_item_rev <- sum(sales.ext_sales_price ? sales.sales_channel = 'CATALOG') by sales.item.name;
auto ws_item_rev <- sum(sales.ext_sales_price ? sales.sales_channel = 'WEB') by sales.item.name;
auto avg_rev <- (ss_item_rev + cs_item_rev + ws_item_rev) / 3;
auto ss_dev <- ss_item_rev / avg_rev * 100;
auto cs_dev <- cs_item_rev / avg_rev * 100;
auto ws_dev <- ws_item_rev / avg_rev * 100;

where
    sales.date.week_seq in target_week_seq
select
    sales.item.name as item_id,
    ss_item_rev,
    ss_dev,
    cs_item_rev,
    cs_dev,
    ws_item_rev,
    ws_dev,
    avg_rev,
having
    ss_item_rev between 0.9 * cs_item_rev and 1.1 * cs_item_rev
    and ss_item_rev between 0.9 * ws_item_rev and 1.1 * ws_item_rev
    and cs_item_rev between 0.9 * ss_item_rev and 1.1 * ss_item_rev
    and cs_item_rev between 0.9 * ws_item_rev and 1.1 * ws_item_rev
    and ws_item_rev between 0.9 * ss_item_rev and 1.1 * ss_item_rev
    and ws_item_rev between 0.9 * cs_item_rev and 1.1 * cs_item_rev

order by
    item_id asc nulls first,
    ss_item_rev asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
cooperative as (
SELECT
    "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "sales_date_id",
    "sales_catalog_sales_unified"."CS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_catalog_sales_unified"."CS_ITEM_SK" as "sales_item_id",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
UNION ALL
SELECT
    "sales_store_sales_unified"."SS_SOLD_DATE_SK" as "sales_date_id",
    "sales_store_sales_unified"."SS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_id",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
UNION ALL
SELECT
    "sales_web_sales_unified"."WS_SOLD_DATE_SK" as "sales_date_id",
    "sales_web_sales_unified"."WS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_id",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"),
highfalutin as (
SELECT
    CASE WHEN "date_date"."D_DATE" = '2000-01-03' THEN "date_date"."D_WEEK_SEQ" ELSE NULL END as "target_week_seq"
FROM
    "memory"."date_dim" as "date_date"),
uneven as (
SELECT
    "cooperative"."sales_ext_sales_price" as "sales_ext_sales_price",
    "cooperative"."sales_sales_channel" as "sales_sales_channel",
    "sales_date_date"."D_WEEK_SEQ" as "sales_date_week_seq",
    "sales_item_items"."I_ITEM_ID" as "sales_item_name"
FROM
    "cooperative"
    LEFT OUTER JOIN "memory"."date_dim" as "sales_date_date" on "cooperative"."sales_date_id" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "sales_item_items" on "cooperative"."sales_item_id" = "sales_item_items"."I_ITEM_SK"),
juicy as (
SELECT
    sum(CASE WHEN "uneven"."sales_sales_channel" = 'CATALOG' THEN "uneven"."sales_ext_sales_price" ELSE NULL END) as "cs_item_rev",
    sum(CASE WHEN "uneven"."sales_sales_channel" = 'STORE' THEN "uneven"."sales_ext_sales_price" ELSE NULL END) as "ss_item_rev",
    sum(CASE WHEN "uneven"."sales_sales_channel" = 'WEB' THEN "uneven"."sales_ext_sales_price" ELSE NULL END) as "ws_item_rev"
FROM
    "uneven"
GROUP BY
    "uneven"."sales_item_name"),
yummy as (
SELECT
    "uneven"."sales_item_name" as "item_id"
FROM
    "uneven"
WHERE
    "uneven"."sales_date_week_seq" in (select highfalutin."target_week_seq" from highfalutin where highfalutin."target_week_seq" is not null)
),
concerned as (
SELECT
    "juicy"."cs_item_rev" as "cs_item_rev",
    "juicy"."ss_item_rev" as "ss_item_rev",
    "juicy"."ws_item_rev" as "ws_item_rev",
    (( "juicy"."ss_item_rev" + "juicy"."cs_item_rev" ) + "juicy"."ws_item_rev") / 3 as "avg_rev"
FROM
    "juicy"
WHERE
    "juicy"."ss_item_rev" BETWEEN 0.9 * "juicy"."cs_item_rev" AND 1.1 * "juicy"."cs_item_rev"
),
young as (
SELECT
    "concerned"."avg_rev" as "avg_rev",
    "concerned"."cs_item_rev" as "cs_item_rev",
    "concerned"."ss_item_rev" as "ss_item_rev",
    "concerned"."ws_item_rev" as "ws_item_rev",
    "yummy"."item_id" as "item_id"
FROM
    "concerned"
    LEFT OUTER JOIN "yummy" on 1=1
WHERE
    "concerned"."ss_item_rev" BETWEEN 0.9 * "concerned"."cs_item_rev" AND 1.1 * "concerned"."cs_item_rev"
)
SELECT
    "young"."item_id" as "item_id",
    "young"."ss_item_rev" as "ss_item_rev",
    ( "young"."ss_item_rev" / ( "young"."avg_rev" ) ) * 100 as "ss_dev",
    "young"."cs_item_rev" as "cs_item_rev",
    ( "young"."cs_item_rev" / ( "young"."avg_rev" ) ) * 100 as "cs_dev",
    "young"."ws_item_rev" as "ws_item_rev",
    ( "young"."ws_item_rev" / ( "young"."avg_rev" ) ) * 100 as "ws_dev",
    "young"."avg_rev" as "avg_rev"
FROM
    "young"
WHERE
    "young"."ss_item_rev" BETWEEN 0.9 * "young"."ws_item_rev" AND 1.1 * "young"."ws_item_rev" and "young"."cs_item_rev" BETWEEN 0.9 * "young"."ss_item_rev" AND 1.1 * "young"."ss_item_rev" and "young"."cs_item_rev" BETWEEN 0.9 * "young"."ws_item_rev" AND 1.1 * "young"."ws_item_rev" and "young"."ws_item_rev" BETWEEN 0.9 * "young"."ss_item_rev" AND 1.1 * "young"."ss_item_rev" and "young"."ws_item_rev" BETWEEN 0.9 * "young"."cs_item_rev" AND 1.1 * "young"."cs_item_rev"

ORDER BY 
    "young"."item_id" asc nulls first,
    "young"."ss_item_rev" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
quizzical as (
SELECT
    CASE WHEN "date_date"."D_DATE" = '2000-01-03' THEN "date_date"."D_WEEK_SEQ" ELSE NULL END as "target_week_seq"
FROM
    "memory"."date_dim" as "date_date"
GROUP BY
    1),
questionable as (
SELECT
    "sales_catalog_sales_unified"."CS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_catalog_sales_unified"."CS_ITEM_SK" as "sales_item_id",
    "sales_catalog_sales_unified"."CS_ORDER_NUMBER" as "sales_order_id",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_date_date"."D_WEEK_SEQ" in (select quizzical."target_week_seq" from quizzical where quizzical."target_week_seq" is not null)

UNION ALL
SELECT
    "sales_store_sales_unified"."SS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_id",
    "sales_store_sales_unified"."SS_TICKET_NUMBER" as "sales_order_id",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_date_date"."D_WEEK_SEQ" in (select quizzical."target_week_seq" from quizzical where quizzical."target_week_seq" is not null)

UNION ALL
SELECT
    "sales_web_sales_unified"."WS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_id",
    "sales_web_sales_unified"."WS_ORDER_NUMBER" as "sales_order_id",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_date_date"."D_WEEK_SEQ" in (select quizzical."target_week_seq" from quizzical where quizzical."target_week_seq" is not null)
),
yummy as (
SELECT
    "questionable"."sales_ext_sales_price" as "sales_ext_sales_price",
    "questionable"."sales_sales_channel" as "sales_sales_channel",
    "sales_item_items"."I_ITEM_ID" as "sales_item_name"
FROM
    "questionable"
    INNER JOIN "memory"."item" as "sales_item_items" on "questionable"."sales_item_id" = "sales_item_items"."I_ITEM_SK"
GROUP BY
    1,
    2,
    3,
    "questionable"."sales_item_id",
    "questionable"."sales_order_id")
SELECT
    "yummy"."sales_item_name" as "item_id",
    sum(CASE WHEN "yummy"."sales_sales_channel" = 'STORE' THEN "yummy"."sales_ext_sales_price" ELSE NULL END) as "ss_item_rev",
    ( sum(CASE WHEN "yummy"."sales_sales_channel" = 'STORE' THEN "yummy"."sales_ext_sales_price" ELSE NULL END) / ( (( sum(CASE WHEN "yummy"."sales_sales_channel" = 'STORE' THEN "yummy"."sales_ext_sales_price" ELSE NULL END) + sum(CASE WHEN "yummy"."sales_sales_channel" = 'CATALOG' THEN "yummy"."sales_ext_sales_price" ELSE NULL END) ) + sum(CASE WHEN "yummy"."sales_sales_channel" = 'WEB' THEN "yummy"."sales_ext_sales_price" ELSE NULL END)) / 3 ) ) * 100 as "ss_dev",
    sum(CASE WHEN "yummy"."sales_sales_channel" = 'CATALOG' THEN "yummy"."sales_ext_sales_price" ELSE NULL END) as "cs_item_rev",
    ( sum(CASE WHEN "yummy"."sales_sales_channel" = 'CATALOG' THEN "yummy"."sales_ext_sales_price" ELSE NULL END) / ( (( sum(CASE WHEN "yummy"."sales_sales_channel" = 'STORE' THEN "yummy"."sales_ext_sales_price" ELSE NULL END) + sum(CASE WHEN "yummy"."sales_sales_channel" = 'CATALOG' THEN "yummy"."sales_ext_sales_price" ELSE NULL END) ) + sum(CASE WHEN "yummy"."sales_sales_channel" = 'WEB' THEN "yummy"."sales_ext_sales_price" ELSE NULL END)) / 3 ) ) * 100 as "cs_dev",
    sum(CASE WHEN "yummy"."sales_sales_channel" = 'WEB' THEN "yummy"."sales_ext_sales_price" ELSE NULL END) as "ws_item_rev",
    ( sum(CASE WHEN "yummy"."sales_sales_channel" = 'WEB' THEN "yummy"."sales_ext_sales_price" ELSE NULL END) / ( (( sum(CASE WHEN "yummy"."sales_sales_channel" = 'STORE' THEN "yummy"."sales_ext_sales_price" ELSE NULL END) + sum(CASE WHEN "yummy"."sales_sales_channel" = 'CATALOG' THEN "yummy"."sales_ext_sales_price" ELSE NULL END) ) + sum(CASE WHEN "yummy"."sales_sales_channel" = 'WEB' THEN "yummy"."sales_ext_sales_price" ELSE NULL END)) / 3 ) ) * 100 as "ws_dev",
    (( sum(CASE WHEN "yummy"."sales_sales_channel" = 'STORE' THEN "yummy"."sales_ext_sales_price" ELSE NULL END) + sum(CASE WHEN "yummy"."sales_sales_channel" = 'CATALOG' THEN "yummy"."sales_ext_sales_price" ELSE NULL END) ) + sum(CASE WHEN "yummy"."sales_sales_channel" = 'WEB' THEN "yummy"."sales_ext_sales_price" ELSE NULL END)) / 3 as "avg_rev"
FROM
    "yummy"
GROUP BY
    1
HAVING
    "ss_item_rev" BETWEEN 0.9 * "cs_item_rev" AND 1.1 * "cs_item_rev" and "ss_item_rev" BETWEEN 0.9 * "ws_item_rev" AND 1.1 * "ws_item_rev" and "cs_item_rev" BETWEEN 0.9 * "ss_item_rev" AND 1.1 * "ss_item_rev" and "cs_item_rev" BETWEEN 0.9 * "ws_item_rev" AND 1.1 * "ws_item_rev" and "ws_item_rev" BETWEEN 0.9 * "ss_item_rev" AND 1.1 * "ss_item_rev" and "ws_item_rev" BETWEEN 0.9 * "cs_item_rev" AND 1.1 * "cs_item_rev"

ORDER BY 
    "item_id" asc nulls first,
    "ss_item_rev" asc nulls first
LIMIT (100)
```
