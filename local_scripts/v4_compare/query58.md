# Query 58

**Status:** `exec_fail`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | FAILED |
| reference execution | OK (5 rows) |

## Result comparison

_at least one side did not produce rows._

## SQL size

| Source | Chars | Lines |
| --- | --- | --- |
| v4 | 5394 | 36 |
| reference | 5265 | 78 |
| v4 / ref | 1.02x | 0.46x |

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
quizzical as (
SELECT
    ( CASE WHEN INVALID_REFERENCE_BUG_<Missing source reference to sales.sales_channel> = 'CATALOG' THEN INVALID_REFERENCE_BUG_<Missing source reference to sales.ext_sales_price> ELSE NULL END / ( (( CASE WHEN INVALID_REFERENCE_BUG_<Missing source reference to sales.sales_channel> = 'STORE' THEN INVALID_REFERENCE_BUG_<Missing source reference to sales.ext_sales_price> ELSE NULL END + CASE WHEN INVALID_REFERENCE_BUG_<Missing source reference to sales.sales_channel> = 'CATALOG' THEN INVALID_REFERENCE_BUG_<Missing source reference to sales.ext_sales_price> ELSE NULL END ) + CASE WHEN INVALID_REFERENCE_BUG_<Missing source reference to sales.sales_channel> = 'WEB' THEN INVALID_REFERENCE_BUG_<Missing source reference to sales.ext_sales_price> ELSE NULL END) / 3 ) ) * 100 as "cs_dev",
    ( CASE WHEN INVALID_REFERENCE_BUG_<Missing source reference to sales.sales_channel> = 'STORE' THEN INVALID_REFERENCE_BUG_<Missing source reference to sales.ext_sales_price> ELSE NULL END / ( (( CASE WHEN INVALID_REFERENCE_BUG_<Missing source reference to sales.sales_channel> = 'STORE' THEN INVALID_REFERENCE_BUG_<Missing source reference to sales.ext_sales_price> ELSE NULL END + CASE WHEN INVALID_REFERENCE_BUG_<Missing source reference to sales.sales_channel> = 'CATALOG' THEN INVALID_REFERENCE_BUG_<Missing source reference to sales.ext_sales_price> ELSE NULL END ) + CASE WHEN INVALID_REFERENCE_BUG_<Missing source reference to sales.sales_channel> = 'WEB' THEN INVALID_REFERENCE_BUG_<Missing source reference to sales.ext_sales_price> ELSE NULL END) / 3 ) ) * 100 as "ss_dev",
    ( CASE WHEN INVALID_REFERENCE_BUG_<Missing source reference to sales.sales_channel> = 'WEB' THEN INVALID_REFERENCE_BUG_<Missing source reference to sales.ext_sales_price> ELSE NULL END / ( (( CASE WHEN INVALID_REFERENCE_BUG_<Missing source reference to sales.sales_channel> = 'STORE' THEN INVALID_REFERENCE_BUG_<Missing source reference to sales.ext_sales_price> ELSE NULL END + CASE WHEN INVALID_REFERENCE_BUG_<Missing source reference to sales.sales_channel> = 'CATALOG' THEN INVALID_REFERENCE_BUG_<Missing source reference to sales.ext_sales_price> ELSE NULL END ) + CASE WHEN INVALID_REFERENCE_BUG_<Missing source reference to sales.sales_channel> = 'WEB' THEN INVALID_REFERENCE_BUG_<Missing source reference to sales.ext_sales_price> ELSE NULL END) / 3 ) ) * 100 as "ws_dev",
    (( CASE WHEN INVALID_REFERENCE_BUG_<Missing source reference to sales.sales_channel> = 'STORE' THEN INVALID_REFERENCE_BUG_<Missing source reference to sales.ext_sales_price> ELSE NULL END + CASE WHEN INVALID_REFERENCE_BUG_<Missing source reference to sales.sales_channel> = 'CATALOG' THEN INVALID_REFERENCE_BUG_<Missing source reference to sales.ext_sales_price> ELSE NULL END ) + CASE WHEN INVALID_REFERENCE_BUG_<Missing source reference to sales.sales_channel> = 'WEB' THEN INVALID_REFERENCE_BUG_<Missing source reference to sales.ext_sales_price> ELSE NULL END) / 3 as "avg_rev",
    CASE WHEN INVALID_REFERENCE_BUG_<Missing source reference to sales.sales_channel> = 'CATALOG' THEN INVALID_REFERENCE_BUG_<Missing source reference to sales.ext_sales_price> ELSE NULL END as "cs_item_rev",
    CASE WHEN INVALID_REFERENCE_BUG_<Missing source reference to sales.sales_channel> = 'STORE' THEN INVALID_REFERENCE_BUG_<Missing source reference to sales.ext_sales_price> ELSE NULL END as "ss_item_rev",
    CASE WHEN INVALID_REFERENCE_BUG_<Missing source reference to sales.sales_channel> = 'WEB' THEN INVALID_REFERENCE_BUG_<Missing source reference to sales.ext_sales_price> ELSE NULL END as "ws_item_rev",
    INVALID_REFERENCE_BUG_<Missing source reference to date._date_string> as "date__date_string",
    INVALID_REFERENCE_BUG_<Missing source reference to date.week_seq> as "date_week_seq",
    INVALID_REFERENCE_BUG_<Missing source reference to sales.date.week_seq> as "sales_date_week_seq",
    INVALID_REFERENCE_BUG_<Missing source reference to sales.ext_sales_price> as "sales_ext_sales_price",
    INVALID_REFERENCE_BUG_<Missing source reference to sales.item.name> as "item_id",
    INVALID_REFERENCE_BUG_<Missing source reference to sales.item.name> as "sales_item_name",
    INVALID_REFERENCE_BUG_<Missing source reference to sales.sales_channel> as "sales_sales_channel"
)
SELECT
    "quizzical"."item_id" as "item_id",
    "quizzical"."ss_item_rev" as "ss_item_rev",
    "quizzical"."ss_dev" as "ss_dev",
    "quizzical"."cs_item_rev" as "cs_item_rev",
    "quizzical"."cs_dev" as "cs_dev",
    "quizzical"."ws_item_rev" as "ws_item_rev",
    "quizzical"."ws_dev" as "ws_dev",
    "quizzical"."avg_rev" as "avg_rev"
FROM
    "quizzical"
WHERE
    "quizzical"."ss_item_rev" BETWEEN 0.9 * "quizzical"."cs_item_rev" AND 1.1 * "quizzical"."cs_item_rev" and "quizzical"."ss_item_rev" BETWEEN 0.9 * "quizzical"."ws_item_rev" AND 1.1 * "quizzical"."ws_item_rev" and "quizzical"."cs_item_rev" BETWEEN 0.9 * "quizzical"."ss_item_rev" AND 1.1 * "quizzical"."ss_item_rev" and "quizzical"."cs_item_rev" BETWEEN 0.9 * "quizzical"."ws_item_rev" AND 1.1 * "quizzical"."ws_item_rev" and "quizzical"."ws_item_rev" BETWEEN 0.9 * "quizzical"."ss_item_rev" AND 1.1 * "quizzical"."ss_item_rev" and "quizzical"."ws_item_rev" BETWEEN 0.9 * "quizzical"."cs_item_rev" AND 1.1 * "quizzical"."cs_item_rev"

ORDER BY 
    "quizzical"."item_id" asc nulls first,
    "quizzical"."ss_item_rev" asc nulls first
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

## v4 execution error

```
Traceback (most recent call last):
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 161, in run_one
    result.v4_rows = execute(con, v4_sql)
                     ~~~~~~~^^^^^^^^^^^^^
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 102, in execute
    cursor = con.execute(sql)
_duckdb.ParserException: Parser Error: syntax error at or near "source"

LINE 4:     ( CASE WHEN INVALID_REFERENCE_BUG_<Missing source reference to sales.sales_channel> = 'CATALOG' THEN...
                                                       ^
```
