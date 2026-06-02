# Query 58

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (0 rows) |
| reference execution | OK (0 rows) |
| results identical | YES |

## Result comparison

v4 rows: 0 (0 distinct)
ref rows: 0 (0 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 4633 | 85 | 30.95 ms |
| reference | 5253 | 81 | 39.34 ms |
| v4 / ref | 0.88x | 1.05x | 0.79x |

## Preql

```
import all_sales as sales;
import date as date;

auto target_week_seq <- date.week_seq ? date._date_string = '2000-01-03';

def channel_item_rev(channel) -> sum(sales.ext_sales_price ? sales.sales_channel = channel) by sales.item.text_id;

auto ss_item_rev <- sum(sales.ext_sales_price ? sales.sales_channel = 'STORE') by sales.item.text_id;
auto cs_item_rev <- sum(sales.ext_sales_price ? sales.sales_channel = 'CATALOG') by sales.item.text_id;
auto ws_item_rev <- sum(sales.ext_sales_price ? sales.sales_channel = 'WEB') by sales.item.text_id;
auto avg_rev <- (ss_item_rev + cs_item_rev + ws_item_rev) / 3;
auto ss_dev <- ss_item_rev / avg_rev * 100;
auto cs_dev <- cs_item_rev / avg_rev * 100;
auto ws_dev <- ws_item_rev / avg_rev * 100;

where
    sales.date.week_seq in target_week_seq
select
    sales.item.text_id as item_id,
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
highfalutin as (
SELECT
    CASE WHEN "date_date"."D_DATE" = '2000-01-03' THEN "date_date"."D_WEEK_SEQ" ELSE NULL END as "target_week_seq"
FROM
    "memory"."date_dim" as "date_date"),
cooperative as (
SELECT
    "sales_catalog_sales_unified"."CS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_catalog_sales_unified"."CS_ITEM_SK" as "sales_item_id",
     'CATALOG'  as "sales_sales_channel",
    "sales_date_date"."D_WEEK_SEQ" as "sales_date_week_seq"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_date_date"."D_WEEK_SEQ" in (select highfalutin."target_week_seq" from highfalutin where highfalutin."target_week_seq" is not null)

UNION ALL
SELECT
    "sales_store_sales_unified"."SS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_id",
     'STORE'  as "sales_sales_channel",
    "sales_date_date"."D_WEEK_SEQ" as "sales_date_week_seq"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_date_date"."D_WEEK_SEQ" in (select highfalutin."target_week_seq" from highfalutin where highfalutin."target_week_seq" is not null)

UNION ALL
SELECT
    "sales_web_sales_unified"."WS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_id",
     'WEB'  as "sales_sales_channel",
    "sales_date_date"."D_WEEK_SEQ" as "sales_date_week_seq"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_date_date"."D_WEEK_SEQ" in (select highfalutin."target_week_seq" from highfalutin where highfalutin."target_week_seq" is not null)
),
uneven as (
SELECT
    "cooperative"."sales_date_week_seq" as "sales_date_week_seq",
    "cooperative"."sales_ext_sales_price" as "sales_ext_sales_price",
    "cooperative"."sales_sales_channel" as "sales_sales_channel",
    "sales_item_items"."I_ITEM_ID" as "sales_item_text_id"
FROM
    "cooperative"
    INNER JOIN "memory"."item" as "sales_item_items" on "cooperative"."sales_item_id" = "sales_item_items"."I_ITEM_SK"
WHERE
    "cooperative"."sales_date_week_seq" in (select highfalutin."target_week_seq" from highfalutin where highfalutin."target_week_seq" is not null)
),
yummy as (
SELECT
    "uneven"."sales_item_text_id" as "sales_item_text_id",
    sum(CASE WHEN "uneven"."sales_sales_channel" = 'CATALOG' THEN "uneven"."sales_ext_sales_price" ELSE NULL END) as "cs_item_rev",
    sum(CASE WHEN "uneven"."sales_sales_channel" = 'STORE' THEN "uneven"."sales_ext_sales_price" ELSE NULL END) as "ss_item_rev",
    sum(CASE WHEN "uneven"."sales_sales_channel" = 'WEB' THEN "uneven"."sales_ext_sales_price" ELSE NULL END) as "ws_item_rev"
FROM
    "uneven"
WHERE
    "uneven"."sales_date_week_seq" in (select highfalutin."target_week_seq" from highfalutin where highfalutin."target_week_seq" is not null)

GROUP BY
    1)
SELECT
    "yummy"."sales_item_text_id" as "item_id",
    "yummy"."ss_item_rev" as "ss_item_rev",
    ( "yummy"."ss_item_rev" / ( (( "yummy"."ss_item_rev" + "yummy"."cs_item_rev" ) + "yummy"."ws_item_rev") / 3 ) ) * 100 as "ss_dev",
    "yummy"."cs_item_rev" as "cs_item_rev",
    ( "yummy"."cs_item_rev" / ( (( "yummy"."ss_item_rev" + "yummy"."cs_item_rev" ) + "yummy"."ws_item_rev") / 3 ) ) * 100 as "cs_dev",
    "yummy"."ws_item_rev" as "ws_item_rev",
    ( "yummy"."ws_item_rev" / ( (( "yummy"."ss_item_rev" + "yummy"."cs_item_rev" ) + "yummy"."ws_item_rev") / 3 ) ) * 100 as "ws_dev",
    (( "yummy"."ss_item_rev" + "yummy"."cs_item_rev" ) + "yummy"."ws_item_rev") / 3 as "avg_rev"
FROM
    "yummy"
WHERE
    "yummy"."ss_item_rev" BETWEEN 0.9 * "yummy"."cs_item_rev" AND 1.1 * "yummy"."cs_item_rev" and "yummy"."ss_item_rev" BETWEEN 0.9 * "yummy"."ws_item_rev" AND 1.1 * "yummy"."ws_item_rev" and "yummy"."cs_item_rev" BETWEEN 0.9 * "yummy"."ss_item_rev" AND 1.1 * "yummy"."ss_item_rev" and "yummy"."cs_item_rev" BETWEEN 0.9 * "yummy"."ws_item_rev" AND 1.1 * "yummy"."ws_item_rev" and "yummy"."ws_item_rev" BETWEEN 0.9 * "yummy"."ss_item_rev" AND 1.1 * "yummy"."ss_item_rev" and "yummy"."ws_item_rev" BETWEEN 0.9 * "yummy"."cs_item_rev" AND 1.1 * "yummy"."cs_item_rev"

ORDER BY 
    "item_id" asc nulls first,
    "yummy"."ss_item_rev" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
quizzical as (
SELECT
    "date_date"."D_WEEK_SEQ" as "target_week_seq"
FROM
    "memory"."date_dim" as "date_date"
WHERE
    "date_date"."D_DATE" = '2000-01-03'

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
    "sales_item_items"."I_ITEM_ID" as "sales_item_text_id"
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
    "yummy"."sales_item_text_id" as "item_id",
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
