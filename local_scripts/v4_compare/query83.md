# Query 83

**Status:** `exec_fail`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | FAILED |
| reference execution | OK (24 rows) |

## Result comparison

_at least one side did not produce rows._

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 4850 | 32 | — |
| reference | 5946 | 132 | 124.67 ms |
| v4 / ref | 0.82x | 0.24x | — |

## Preql

```
import unified_sales as sales;
import date as date;

auto target_week_seqs <- date.week_seq ? date.date in ('2000-06-30'::date, '2000-09-27'::date, '2000-11-17'::date);

# Per-channel return quantities by item.name; the week_seq filter applies to
# the return date.
def channel_qty(channel) -> sum(sales.return_quantity ? sales.sales_channel = channel) by sales.item.name;
def channel_present(channel) -> count(sales.order_id ? sales.sales_channel = channel) by sales.item.name;

auto sr_item_qty <- sum(sales.return_quantity ? sales.sales_channel = 'STORE') by sales.item.name;
auto cr_item_qty <- sum(sales.return_quantity ? sales.sales_channel = 'CATALOG') by sales.item.name;
auto wr_item_qty <- sum(sales.return_quantity ? sales.sales_channel = 'WEB') by sales.item.name;
auto sr_item_present <- count(sales.order_id ? sales.sales_channel = 'STORE') by sales.item.name;
auto cr_item_present <- count(sales.order_id ? sales.sales_channel = 'CATALOG') by sales.item.name;
auto wr_item_present <- count(sales.order_id ? sales.sales_channel = 'WEB') by sales.item.name;
auto total_qty <- sr_item_qty + cr_item_qty + wr_item_qty;
auto avg_qty <- total_qty / 3.0;
auto sr_dev <- (sr_item_qty * 1.0) / total_qty / 3.0 * 100;
auto cr_dev <- (cr_item_qty * 1.0) / total_qty / 3.0 * 100;
auto wr_dev <- (wr_item_qty * 1.0) / total_qty / 3.0 * 100;

where
    sales.return_date.week_seq in target_week_seqs
select
    sales.item.name as item_id,
    sr_item_qty,
    sr_dev,
    cr_item_qty,
    cr_dev,
    wr_item_qty,
    wr_dev,
    avg_qty as average,
    --sr_item_present,
    --cr_item_present,
    --wr_item_present,
having
    sr_item_present > 0 and cr_item_present > 0 and wr_item_present > 0

order by
    item_id asc nulls first,
    sr_item_qty asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
quizzical as (
SELECT
    ( ( (CASE WHEN INVALID_REFERENCE_BUG_<Missing source reference to sales.sales_channel> = 'CATALOG' THEN INVALID_REFERENCE_BUG_<Missing source reference to sales.return_quantity> ELSE NULL END * 1.0) / ( ( CASE WHEN INVALID_REFERENCE_BUG_<Missing source reference to sales.sales_channel> = 'STORE' THEN INVALID_REFERENCE_BUG_<Missing source reference to sales.return_quantity> ELSE NULL END + CASE WHEN INVALID_REFERENCE_BUG_<Missing source reference to sales.sales_channel> = 'CATALOG' THEN INVALID_REFERENCE_BUG_<Missing source reference to sales.return_quantity> ELSE NULL END ) + CASE WHEN INVALID_REFERENCE_BUG_<Missing source reference to sales.sales_channel> = 'WEB' THEN INVALID_REFERENCE_BUG_<Missing source reference to sales.return_quantity> ELSE NULL END ) ) / 3.0 ) * 100 as "cr_dev",
    ( ( (CASE WHEN INVALID_REFERENCE_BUG_<Missing source reference to sales.sales_channel> = 'STORE' THEN INVALID_REFERENCE_BUG_<Missing source reference to sales.return_quantity> ELSE NULL END * 1.0) / ( ( CASE WHEN INVALID_REFERENCE_BUG_<Missing source reference to sales.sales_channel> = 'STORE' THEN INVALID_REFERENCE_BUG_<Missing source reference to sales.return_quantity> ELSE NULL END + CASE WHEN INVALID_REFERENCE_BUG_<Missing source reference to sales.sales_channel> = 'CATALOG' THEN INVALID_REFERENCE_BUG_<Missing source reference to sales.return_quantity> ELSE NULL END ) + CASE WHEN INVALID_REFERENCE_BUG_<Missing source reference to sales.sales_channel> = 'WEB' THEN INVALID_REFERENCE_BUG_<Missing source reference to sales.return_quantity> ELSE NULL END ) ) / 3.0 ) * 100 as "sr_dev",
    ( ( (CASE WHEN INVALID_REFERENCE_BUG_<Missing source reference to sales.sales_channel> = 'WEB' THEN INVALID_REFERENCE_BUG_<Missing source reference to sales.return_quantity> ELSE NULL END * 1.0) / ( ( CASE WHEN INVALID_REFERENCE_BUG_<Missing source reference to sales.sales_channel> = 'STORE' THEN INVALID_REFERENCE_BUG_<Missing source reference to sales.return_quantity> ELSE NULL END + CASE WHEN INVALID_REFERENCE_BUG_<Missing source reference to sales.sales_channel> = 'CATALOG' THEN INVALID_REFERENCE_BUG_<Missing source reference to sales.return_quantity> ELSE NULL END ) + CASE WHEN INVALID_REFERENCE_BUG_<Missing source reference to sales.sales_channel> = 'WEB' THEN INVALID_REFERENCE_BUG_<Missing source reference to sales.return_quantity> ELSE NULL END ) ) / 3.0 ) * 100 as "wr_dev",
    ( ( CASE WHEN INVALID_REFERENCE_BUG_<Missing source reference to sales.sales_channel> = 'STORE' THEN INVALID_REFERENCE_BUG_<Missing source reference to sales.return_quantity> ELSE NULL END + CASE WHEN INVALID_REFERENCE_BUG_<Missing source reference to sales.sales_channel> = 'CATALOG' THEN INVALID_REFERENCE_BUG_<Missing source reference to sales.return_quantity> ELSE NULL END ) + CASE WHEN INVALID_REFERENCE_BUG_<Missing source reference to sales.sales_channel> = 'WEB' THEN INVALID_REFERENCE_BUG_<Missing source reference to sales.return_quantity> ELSE NULL END ) / 3.0 as "average",
    CASE WHEN INVALID_REFERENCE_BUG_<Missing source reference to sales.sales_channel> = 'CATALOG' THEN INVALID_REFERENCE_BUG_<Missing source reference to sales.return_quantity> ELSE NULL END as "cr_item_qty",
    CASE WHEN INVALID_REFERENCE_BUG_<Missing source reference to sales.sales_channel> = 'STORE' THEN INVALID_REFERENCE_BUG_<Missing source reference to sales.return_quantity> ELSE NULL END as "sr_item_qty",
    CASE WHEN INVALID_REFERENCE_BUG_<Missing source reference to sales.sales_channel> = 'WEB' THEN INVALID_REFERENCE_BUG_<Missing source reference to sales.return_quantity> ELSE NULL END as "wr_item_qty",
    INVALID_REFERENCE_BUG_<Missing source reference to sales.item.name> as "item_id",
    INVALID_REFERENCE_BUG_<Missing source reference to sales.order_id> as "sales_order_id",
    INVALID_REFERENCE_BUG_<Missing source reference to sales.sales_channel> as "sales_sales_channel"
)
SELECT
    "quizzical"."item_id" as "item_id",
    "quizzical"."sr_item_qty" as "sr_item_qty",
    "quizzical"."sr_dev" as "sr_dev",
    "quizzical"."cr_item_qty" as "cr_item_qty",
    "quizzical"."cr_dev" as "cr_dev",
    "quizzical"."wr_item_qty" as "wr_item_qty",
    "quizzical"."wr_dev" as "wr_dev",
    "quizzical"."average" as "average"
FROM
    "quizzical"
WHERE
    CASE WHEN CASE WHEN "quizzical"."sales_sales_channel" = 'STORE' THEN "quizzical"."sales_order_id" ELSE NULL END IS NOT NULL THEN 1 ELSE 0 END > 0 and CASE WHEN CASE WHEN "quizzical"."sales_sales_channel" = 'CATALOG' THEN "quizzical"."sales_order_id" ELSE NULL END IS NOT NULL THEN 1 ELSE 0 END > 0 and CASE WHEN CASE WHEN "quizzical"."sales_sales_channel" = 'WEB' THEN "quizzical"."sales_order_id" ELSE NULL END IS NOT NULL THEN 1 ELSE 0 END > 0

ORDER BY 
    "quizzical"."item_id" asc nulls first,
    "quizzical"."sr_item_qty" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
quizzical as (
SELECT
    CASE WHEN cast("date_date"."D_DATE" as date) in (date '2000-06-30',date '2000-09-27',date '2000-11-17') THEN "date_date"."D_WEEK_SEQ" ELSE NULL END as "target_week_seqs"
FROM
    "memory"."date_dim" as "date_date"
GROUP BY
    1),
questionable as (
SELECT
    "sales_catalog_returns_unified"."CR_ITEM_SK" as "sales_item_id",
    "sales_catalog_returns_unified"."CR_ORDER_NUMBER" as "sales_order_id",
     'CATALOG'  as "sales_sales_channel",
    "sales_catalog_returns_unified"."CR_RETURN_QUANTITY" as "sales_return_quantity"
FROM
    "memory"."catalog_returns" as "sales_catalog_returns_unified"
    INNER JOIN "memory"."date_dim" as "sales_return_date_date" on "sales_catalog_returns_unified"."CR_RETURNED_DATE_SK" = "sales_return_date_date"."D_DATE_SK"
WHERE
    "sales_return_date_date"."D_WEEK_SEQ" in (select quizzical."target_week_seqs" from quizzical where quizzical."target_week_seqs" is not null)

UNION ALL
SELECT
    "sales_store_returns_unified"."SR_ITEM_SK" as "sales_item_id",
    "sales_store_returns_unified"."SR_TICKET_NUMBER" as "sales_order_id",
     'STORE'  as "sales_sales_channel",
    "sales_store_returns_unified"."SR_RETURN_QUANTITY" as "sales_return_quantity"
FROM
    "memory"."store_returns" as "sales_store_returns_unified"
    INNER JOIN "memory"."date_dim" as "sales_return_date_date" on "sales_store_returns_unified"."SR_RETURNED_DATE_SK" = "sales_return_date_date"."D_DATE_SK"
WHERE
    "sales_return_date_date"."D_WEEK_SEQ" in (select quizzical."target_week_seqs" from quizzical where quizzical."target_week_seqs" is not null)

UNION ALL
SELECT
    "sales_web_returns_unified"."WR_ITEM_SK" as "sales_item_id",
    "sales_web_returns_unified"."WR_ORDER_NUMBER" as "sales_order_id",
     'WEB'  as "sales_sales_channel",
    "sales_web_returns_unified"."WR_RETURN_QUANTITY" as "sales_return_quantity"
FROM
    "memory"."web_returns" as "sales_web_returns_unified"
    INNER JOIN "memory"."date_dim" as "sales_return_date_date" on "sales_web_returns_unified"."WR_RETURNED_DATE_SK" = "sales_return_date_date"."D_DATE_SK"
WHERE
    "sales_return_date_date"."D_WEEK_SEQ" in (select quizzical."target_week_seqs" from quizzical where quizzical."target_week_seqs" is not null)
),
concerned as (
SELECT
    "sales_item_items"."I_ITEM_ID" as "sales_item_name",
    CASE WHEN "questionable"."sales_sales_channel" = 'CATALOG' THEN "questionable"."sales_order_id" ELSE NULL END as "_virt_filter_order_id_7518965045904948",
    CASE WHEN "questionable"."sales_sales_channel" = 'STORE' THEN "questionable"."sales_order_id" ELSE NULL END as "_virt_filter_order_id_5282889778133979",
    CASE WHEN "questionable"."sales_sales_channel" = 'WEB' THEN "questionable"."sales_order_id" ELSE NULL END as "_virt_filter_order_id_4128599423878258"
FROM
    "memory"."item" as "sales_item_items"
    LEFT OUTER JOIN "questionable" on "sales_item_items"."I_ITEM_SK" = "questionable"."sales_item_id"
GROUP BY
    1,
    2,
    3,
    4,
    "questionable"."sales_order_id"),
yummy as (
SELECT
    "questionable"."sales_return_quantity" as "sales_return_quantity",
    "questionable"."sales_sales_channel" as "sales_sales_channel",
    "sales_item_items"."I_ITEM_ID" as "sales_item_name"
FROM
    "memory"."item" as "sales_item_items"
    LEFT OUTER JOIN "questionable" on "sales_item_items"."I_ITEM_SK" = "questionable"."sales_item_id"
GROUP BY
    1,
    2,
    3,
    "questionable"."sales_order_id",
    "sales_item_items"."I_ITEM_SK"),
abhorrent as (
SELECT
    "concerned"."sales_item_name" as "sales_item_name",
    count("concerned"."_virt_filter_order_id_4128599423878258") as "wr_item_present",
    count("concerned"."_virt_filter_order_id_5282889778133979") as "sr_item_present",
    count("concerned"."_virt_filter_order_id_7518965045904948") as "cr_item_present"
FROM
    "concerned"
GROUP BY
    1
HAVING
    "sr_item_present" > 0
),
juicy as (
SELECT
    "yummy"."sales_item_name" as "sales_item_name",
    sum(CASE WHEN "yummy"."sales_sales_channel" = 'CATALOG' THEN "yummy"."sales_return_quantity" ELSE NULL END) as "cr_item_qty",
    sum(CASE WHEN "yummy"."sales_sales_channel" = 'STORE' THEN "yummy"."sales_return_quantity" ELSE NULL END) as "sr_item_qty",
    sum(CASE WHEN "yummy"."sales_sales_channel" = 'WEB' THEN "yummy"."sales_return_quantity" ELSE NULL END) as "wr_item_qty"
FROM
    "yummy"
GROUP BY
    1),
sweltering as (
SELECT
    "abhorrent"."cr_item_present" as "cr_item_present",
    "abhorrent"."wr_item_present" as "wr_item_present",
    "juicy"."cr_item_qty" as "cr_item_qty",
    "juicy"."sales_item_name" as "item_id",
    "juicy"."sr_item_qty" as "sr_item_qty",
    "juicy"."wr_item_qty" as "wr_item_qty",
    ( ( "juicy"."sr_item_qty" + "juicy"."cr_item_qty" ) + "juicy"."wr_item_qty" ) / 3.0 as "average",
    ( ( ("juicy"."cr_item_qty" * 1.0) / ( ( "juicy"."sr_item_qty" + "juicy"."cr_item_qty" ) + "juicy"."wr_item_qty" ) ) / 3.0 ) * 100 as "cr_dev",
    ( ( ("juicy"."sr_item_qty" * 1.0) / ( ( "juicy"."sr_item_qty" + "juicy"."cr_item_qty" ) + "juicy"."wr_item_qty" ) ) / 3.0 ) * 100 as "sr_dev",
    ( ( ("juicy"."wr_item_qty" * 1.0) / ( ( "juicy"."sr_item_qty" + "juicy"."cr_item_qty" ) + "juicy"."wr_item_qty" ) ) / 3.0 ) * 100 as "wr_dev"
FROM
    "abhorrent"
    INNER JOIN "juicy" on "abhorrent"."sales_item_name" = "juicy"."sales_item_name"
WHERE
    "abhorrent"."sr_item_present" > 0
)
SELECT
    "sweltering"."item_id" as "item_id",
    "sweltering"."sr_item_qty" as "sr_item_qty",
    "sweltering"."sr_dev" as "sr_dev",
    "sweltering"."cr_item_qty" as "cr_item_qty",
    "sweltering"."cr_dev" as "cr_dev",
    "sweltering"."wr_item_qty" as "wr_item_qty",
    "sweltering"."wr_dev" as "wr_dev",
    "sweltering"."average" as "average"
FROM
    "sweltering"
WHERE
    "sweltering"."cr_item_present" > 0 and "sweltering"."wr_item_present" > 0

ORDER BY 
    "sweltering"."item_id" asc nulls first,
    "sweltering"."sr_item_qty" asc nulls first
LIMIT (100)
```

## v4 execution error

```
Traceback (most recent call last):
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 179, in run_one
    result.v4_exec_seconds, result.v4_rows = _time(
                                             ~~~~~^
        lambda: execute(con, v4_sql)
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    )
    ^
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 45, in _time
    value = fn()
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 180, in <lambda>
    lambda: execute(con, v4_sql)
            ~~~~~~~^^^^^^^^^^^^^
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 120, in execute
    cursor = con.execute(sql)
_duckdb.ParserException: Parser Error: syntax error at or near "source"

LINE 4:     ( ( (CASE WHEN INVALID_REFERENCE_BUG_<Missing source reference to sales.sales_channel> = 'CATALOG' THEN...
                                                          ^
```
