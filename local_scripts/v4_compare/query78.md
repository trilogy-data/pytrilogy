# Query 78

**Status:** `exec_fail`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | FAILED |
| reference execution | OK (100 rows) |

## Result comparison

_at least one side did not produce rows._

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 7760 | 163 | — |
| reference | 6271 | 109 | 297.47 ms |
| v4 / ref | 1.24x | 1.50x | — |

## Preql

```
import unified_sales as sales;

def channel_sum(metric, channel) -> sum(metric ? sales.sales_channel = channel) by sales.date.year, sales.item.id, sales.customer.id;

auto ss_qty <- sum(sales.quantity ? sales.sales_channel = 'STORE')
    by sales.date.year, sales.item.id, sales.customer.id;
auto ss_wc <- sum(sales.wholesale_cost ? sales.sales_channel = 'STORE')
    by sales.date.year, sales.item.id, sales.customer.id;
auto ss_sp <- sum(sales.sales_price ? sales.sales_channel = 'STORE')
    by sales.date.year, sales.item.id, sales.customer.id;
auto ws_qty <- sum(sales.quantity ? sales.sales_channel = 'WEB')
    by sales.date.year, sales.item.id, sales.customer.id;
auto ws_wc <- sum(sales.wholesale_cost ? sales.sales_channel = 'WEB')
    by sales.date.year, sales.item.id, sales.customer.id;
auto ws_sp <- sum(sales.sales_price ? sales.sales_channel = 'WEB')
    by sales.date.year, sales.item.id, sales.customer.id;
auto cs_qty <- sum(sales.quantity ? sales.sales_channel = 'CATALOG')
    by sales.date.year, sales.item.id, sales.customer.id;
auto cs_wc <- sum(sales.wholesale_cost ? sales.sales_channel = 'CATALOG')
    by sales.date.year, sales.item.id, sales.customer.id;
auto cs_sp <- sum(sales.sales_price ? sales.sales_channel = 'CATALOG')
    by sales.date.year, sales.item.id, sales.customer.id;
auto other_chan_qty <- coalesce(ws_qty, 0) + coalesce(cs_qty, 0);
auto other_chan_wc <- coalesce(ws_wc, 0) + coalesce(cs_wc, 0);
auto other_chan_sp <- coalesce(ws_sp, 0) + coalesce(cs_sp, 0);

where
    sales.is_returned is null and sales.date.year = 2000 and sales.customer.id is not null
select
    sales.date.year as ss_sold_year,
    sales.item.id as ss_item_sk,
    sales.customer.id as ss_customer_sk,
    round(ss_qty::numeric(7,2) / other_chan_qty::numeric(7,2), 2) as ratio,
    ss_qty as store_qty,
    ss_wc as store_wholesale_cost,
    ss_sp as store_sales_price,
    other_chan_qty,
    other_chan_wc as other_chan_wholesale_cost,
    other_chan_sp as other_chan_sales_price,
    --ws_qty,
    --cs_qty,
having
    store_qty > 0 and (coalesce(ws_qty, 0) > 0 or coalesce(cs_qty, 0) > 0)

order by
    ss_sold_year asc nulls first,
    ss_item_sk asc nulls first,
    ss_customer_sk asc nulls first,
    store_qty desc nulls last,
    store_wholesale_cost desc nulls last,
    store_sales_price desc nulls last,
    other_chan_qty asc nulls first,
    other_chan_wholesale_cost asc nulls first,
    other_chan_sales_price asc nulls first,
    ratio asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
cheerful as (
SELECT
     true  as "sales_is_returned",
    "sales_catalog_returns_unified"."CR_ITEM_SK" as "sales_item_id",
    "sales_catalog_returns_unified"."CR_ORDER_NUMBER" as "sales_order_id",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_returns" as "sales_catalog_returns_unified"
UNION ALL
SELECT
     true  as "sales_is_returned",
    "sales_store_returns_unified"."SR_ITEM_SK" as "sales_item_id",
    "sales_store_returns_unified"."SR_TICKET_NUMBER" as "sales_order_id",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_returns" as "sales_store_returns_unified"
UNION ALL
SELECT
     true  as "sales_is_returned",
    "sales_web_returns_unified"."WR_ITEM_SK" as "sales_item_id",
    "sales_web_returns_unified"."WR_ORDER_NUMBER" as "sales_order_id",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_returns" as "sales_web_returns_unified"),
abundant as (
SELECT
    "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" as "sales_customer_id",
    "sales_catalog_sales_unified"."CS_ITEM_SK" as "sales_item_id",
    "sales_catalog_sales_unified"."CS_ORDER_NUMBER" as "sales_order_id",
    "sales_catalog_sales_unified"."CS_QUANTITY" as "sales_quantity",
     'CATALOG'  as "sales_sales_channel",
    "sales_catalog_sales_unified"."CS_SALES_PRICE" as "sales_sales_price",
    "sales_catalog_sales_unified"."CS_WHOLESALE_COST" as "sales_wholesale_cost",
    "sales_date_date"."D_YEAR" as "sales_date_year"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" is not null and "sales_date_date"."D_YEAR" = 2000

UNION ALL
SELECT
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "sales_customer_id",
    "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_id",
    "sales_store_sales_unified"."SS_TICKET_NUMBER" as "sales_order_id",
    "sales_store_sales_unified"."SS_QUANTITY" as "sales_quantity",
     'STORE'  as "sales_sales_channel",
    "sales_store_sales_unified"."SS_SALES_PRICE" as "sales_sales_price",
    "sales_store_sales_unified"."SS_WHOLESALE_COST" as "sales_wholesale_cost",
    "sales_date_date"."D_YEAR" as "sales_date_year"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_store_sales_unified"."SS_CUSTOMER_SK" is not null and "sales_date_date"."D_YEAR" = 2000

UNION ALL
SELECT
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" as "sales_customer_id",
    "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_id",
    "sales_web_sales_unified"."WS_ORDER_NUMBER" as "sales_order_id",
    "sales_web_sales_unified"."WS_QUANTITY" as "sales_quantity",
     'WEB'  as "sales_sales_channel",
    "sales_web_sales_unified"."WS_SALES_PRICE" as "sales_sales_price",
    "sales_web_sales_unified"."WS_WHOLESALE_COST" as "sales_wholesale_cost",
    "sales_date_date"."D_YEAR" as "sales_date_year"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" is not null and "sales_date_date"."D_YEAR" = 2000
),
yummy as (
SELECT
    "abundant"."sales_customer_id" as "sales_customer_id",
    "abundant"."sales_date_year" as "sales_date_year",
    "abundant"."sales_item_id" as "sales_item_id",
    "abundant"."sales_quantity" as "sales_quantity",
    "abundant"."sales_sales_channel" as "sales_sales_channel",
    "abundant"."sales_sales_price" as "sales_sales_price",
    "abundant"."sales_wholesale_cost" as "sales_wholesale_cost"
FROM
    "abundant"
    LEFT OUTER JOIN "cheerful" on "abundant"."sales_item_id" = "cheerful"."sales_item_id" AND "abundant"."sales_order_id" = "cheerful"."sales_order_id" AND "abundant"."sales_sales_channel" = "cheerful"."sales_sales_channel"
WHERE
    "cheerful"."sales_is_returned" is null
),
vacuous as (
SELECT
    sum(CASE WHEN "yummy"."sales_sales_channel" = 'CATALOG' THEN "yummy"."sales_quantity" ELSE NULL END) as "cs_qty",
    sum(CASE WHEN "yummy"."sales_sales_channel" = 'STORE' THEN "yummy"."sales_quantity" ELSE NULL END) as "ss_qty",
    sum(CASE WHEN "yummy"."sales_sales_channel" = 'STORE' THEN "yummy"."sales_sales_price" ELSE NULL END) as "ss_sp",
    sum(CASE WHEN "yummy"."sales_sales_channel" = 'STORE' THEN "yummy"."sales_wholesale_cost" ELSE NULL END) as "ss_wc",
    sum(CASE WHEN "yummy"."sales_sales_channel" = 'WEB' THEN "yummy"."sales_quantity" ELSE NULL END) as "ws_qty"
FROM
    "yummy"
GROUP BY
    "yummy"."sales_customer_id",
    "yummy"."sales_date_year",
    "yummy"."sales_item_id"),
juicy as (
SELECT
    "yummy"."sales_customer_id" as "ss_customer_sk",
    "yummy"."sales_date_year" as "ss_sold_year",
    "yummy"."sales_item_id" as "ss_item_sk"
FROM
    "yummy"),
young as (
SELECT
    "vacuous"."cs_qty" as "cs_qty",
    "vacuous"."ss_qty" as "store_qty",
    "vacuous"."ss_sp" as "store_sales_price",
    "vacuous"."ss_wc" as "store_wholesale_cost",
    "vacuous"."ws_qty" as "ws_qty",
    coalesce("vacuous"."ws_qty",0) + coalesce("vacuous"."cs_qty",0) as "other_chan_qty"
FROM
    "vacuous"),
sparkling as (
SELECT
    "juicy"."ss_customer_sk" as "ss_customer_sk",
    "juicy"."ss_item_sk" as "ss_item_sk",
    "juicy"."ss_sold_year" as "ss_sold_year",
    "young"."cs_qty" as "cs_qty",
    "young"."other_chan_qty" as "other_chan_qty",
    "young"."store_qty" as "store_qty",
    "young"."store_sales_price" as "store_sales_price",
    "young"."store_wholesale_cost" as "store_wholesale_cost",
    "young"."ws_qty" as "ws_qty"
FROM
    "young"
    LEFT OUTER JOIN "juicy" on 1=1
WHERE
    "young"."store_qty" > 0
)
SELECT
    "sparkling"."ss_sold_year" as "ss_sold_year",
    "sparkling"."ss_item_sk" as "ss_item_sk",
    "sparkling"."ss_customer_sk" as "ss_customer_sk",
    round(cast(CASE WHEN  'CATALOG'  = 'STORE' THEN INVALID_REFERENCE_BUG_<Missing source reference to sales.quantity> ELSE NULL END as numeric(7,2)) / cast("sparkling"."other_chan_qty" as numeric(7,2)),2) as "ratio",
    "sparkling"."store_qty" as "store_qty",
    "sparkling"."store_wholesale_cost" as "store_wholesale_cost",
    "sparkling"."store_sales_price" as "store_sales_price",
    "sparkling"."other_chan_qty" as "other_chan_qty",
    coalesce(CASE WHEN  'CATALOG'  = 'WEB' THEN INVALID_REFERENCE_BUG_<Missing source reference to sales.wholesale_cost> ELSE NULL END,0) + coalesce(CASE WHEN  'CATALOG'  = 'CATALOG' THEN INVALID_REFERENCE_BUG_<Missing source reference to sales.wholesale_cost> ELSE NULL END,0) as "other_chan_wholesale_cost",
    coalesce(CASE WHEN  'CATALOG'  = 'WEB' THEN INVALID_REFERENCE_BUG_<Missing source reference to sales.sales_price> ELSE NULL END,0) + coalesce(CASE WHEN  'CATALOG'  = 'CATALOG' THEN INVALID_REFERENCE_BUG_<Missing source reference to sales.sales_price> ELSE NULL END,0) as "other_chan_sales_price"
FROM
    "sparkling"
WHERE
    ( coalesce("sparkling"."ws_qty",0) > 0 or coalesce("sparkling"."cs_qty",0) > 0 )

ORDER BY 
    "sparkling"."ss_sold_year" asc nulls first,
    "sparkling"."ss_item_sk" asc nulls first,
    "sparkling"."ss_customer_sk" asc nulls first,
    "sparkling"."store_qty" desc nulls last,
    "sparkling"."store_wholesale_cost" desc nulls last,
    "sparkling"."store_sales_price" desc nulls last,
    "sparkling"."other_chan_qty" asc nulls first,
    "other_chan_wholesale_cost" asc nulls first,
    "other_chan_sales_price" asc nulls first,
    "ratio" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
cheerful as (
SELECT
     true  as "sales_is_returned",
    "sales_catalog_returns_unified"."CR_ITEM_SK" as "sales_item_id",
    "sales_catalog_returns_unified"."CR_ORDER_NUMBER" as "sales_order_id",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_returns" as "sales_catalog_returns_unified"
UNION ALL
SELECT
     true  as "sales_is_returned",
    "sales_store_returns_unified"."SR_ITEM_SK" as "sales_item_id",
    "sales_store_returns_unified"."SR_TICKET_NUMBER" as "sales_order_id",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_returns" as "sales_store_returns_unified"
UNION ALL
SELECT
     true  as "sales_is_returned",
    "sales_web_returns_unified"."WR_ITEM_SK" as "sales_item_id",
    "sales_web_returns_unified"."WR_ORDER_NUMBER" as "sales_order_id",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_returns" as "sales_web_returns_unified"),
abundant as (
SELECT
    "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" as "sales_customer_id",
    "sales_catalog_sales_unified"."CS_ITEM_SK" as "sales_item_id",
    "sales_catalog_sales_unified"."CS_ORDER_NUMBER" as "sales_order_id",
    "sales_catalog_sales_unified"."CS_QUANTITY" as "sales_quantity",
     'CATALOG'  as "sales_sales_channel",
    "sales_catalog_sales_unified"."CS_SALES_PRICE" as "sales_sales_price",
    "sales_catalog_sales_unified"."CS_WHOLESALE_COST" as "sales_wholesale_cost",
    "sales_date_date"."D_YEAR" as "sales_date_year"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" is not null and "sales_date_date"."D_YEAR" = 2000

UNION ALL
SELECT
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "sales_customer_id",
    "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_id",
    "sales_store_sales_unified"."SS_TICKET_NUMBER" as "sales_order_id",
    "sales_store_sales_unified"."SS_QUANTITY" as "sales_quantity",
     'STORE'  as "sales_sales_channel",
    "sales_store_sales_unified"."SS_SALES_PRICE" as "sales_sales_price",
    "sales_store_sales_unified"."SS_WHOLESALE_COST" as "sales_wholesale_cost",
    "sales_date_date"."D_YEAR" as "sales_date_year"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_store_sales_unified"."SS_CUSTOMER_SK" is not null and "sales_date_date"."D_YEAR" = 2000

UNION ALL
SELECT
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" as "sales_customer_id",
    "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_id",
    "sales_web_sales_unified"."WS_ORDER_NUMBER" as "sales_order_id",
    "sales_web_sales_unified"."WS_QUANTITY" as "sales_quantity",
     'WEB'  as "sales_sales_channel",
    "sales_web_sales_unified"."WS_SALES_PRICE" as "sales_sales_price",
    "sales_web_sales_unified"."WS_WHOLESALE_COST" as "sales_wholesale_cost",
    "sales_date_date"."D_YEAR" as "sales_date_year"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" is not null and "sales_date_date"."D_YEAR" = 2000
)
SELECT
    "abundant"."sales_date_year" as "ss_sold_year",
    "abundant"."sales_item_id" as "ss_item_sk",
    "abundant"."sales_customer_id" as "ss_customer_sk",
    round(cast(sum(CASE WHEN "abundant"."sales_sales_channel" = 'STORE' THEN "abundant"."sales_quantity" ELSE NULL END) as numeric(7,2)) / cast(coalesce(sum(CASE WHEN "abundant"."sales_sales_channel" = 'WEB' THEN "abundant"."sales_quantity" ELSE NULL END),0) + coalesce(sum(CASE WHEN "abundant"."sales_sales_channel" = 'CATALOG' THEN "abundant"."sales_quantity" ELSE NULL END),0) as numeric(7,2)),2) as "ratio",
    sum(CASE WHEN "abundant"."sales_sales_channel" = 'STORE' THEN "abundant"."sales_quantity" ELSE NULL END) as "store_qty",
    sum(CASE WHEN "abundant"."sales_sales_channel" = 'STORE' THEN "abundant"."sales_wholesale_cost" ELSE NULL END) as "store_wholesale_cost",
    sum(CASE WHEN "abundant"."sales_sales_channel" = 'STORE' THEN "abundant"."sales_sales_price" ELSE NULL END) as "store_sales_price",
    coalesce(sum(CASE WHEN "abundant"."sales_sales_channel" = 'WEB' THEN "abundant"."sales_quantity" ELSE NULL END),0) + coalesce(sum(CASE WHEN "abundant"."sales_sales_channel" = 'CATALOG' THEN "abundant"."sales_quantity" ELSE NULL END),0) as "other_chan_qty",
    coalesce(sum(CASE WHEN "abundant"."sales_sales_channel" = 'WEB' THEN "abundant"."sales_wholesale_cost" ELSE NULL END),0) + coalesce(sum(CASE WHEN "abundant"."sales_sales_channel" = 'CATALOG' THEN "abundant"."sales_wholesale_cost" ELSE NULL END),0) as "other_chan_wholesale_cost",
    coalesce(sum(CASE WHEN "abundant"."sales_sales_channel" = 'WEB' THEN "abundant"."sales_sales_price" ELSE NULL END),0) + coalesce(sum(CASE WHEN "abundant"."sales_sales_channel" = 'CATALOG' THEN "abundant"."sales_sales_price" ELSE NULL END),0) as "other_chan_sales_price"
FROM
    "abundant"
    LEFT OUTER JOIN "cheerful" on "abundant"."sales_item_id" = "cheerful"."sales_item_id" AND "abundant"."sales_order_id" = "cheerful"."sales_order_id" AND "abundant"."sales_sales_channel" = "cheerful"."sales_sales_channel"
WHERE
    "cheerful"."sales_is_returned" is null

GROUP BY
    1,
    2,
    3
HAVING
    "store_qty" > 0 and ( coalesce(sum(CASE WHEN "abundant"."sales_sales_channel" = 'WEB' THEN "abundant"."sales_quantity" ELSE NULL END),0) > 0 or coalesce(sum(CASE WHEN "abundant"."sales_sales_channel" = 'CATALOG' THEN "abundant"."sales_quantity" ELSE NULL END),0) > 0 )

ORDER BY 
    "ss_sold_year" asc nulls first,
    "ss_item_sk" asc nulls first,
    "ss_customer_sk" asc nulls first,
    "store_qty" desc nulls last,
    "store_wholesale_cost" desc nulls last,
    "store_sales_price" desc nulls last,
    "other_chan_qty" asc nulls first,
    "other_chan_wholesale_cost" asc nulls first,
    "other_chan_sales_price" asc nulls first,
    "ratio" asc nulls first
LIMIT (100)
```

## v4 execution error

```
Traceback (most recent call last):
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 267, in run_one
    result.v4_exec_seconds, result.v4_rows = _time(lambda: _exec(v4_sql))
                                             ~~~~~^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 52, in _time
    value = fn()
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 267, in <lambda>
    result.v4_exec_seconds, result.v4_rows = _time(lambda: _exec(v4_sql))
                                                           ~~~~~^^^^^^^^
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 263, in _exec
    return execute(con, bound_sql, params or None)
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 183, in execute
    cursor = con.execute(sql, params) if params else con.execute(sql)
                                                     ~~~~~~~~~~~^^^^^
_duckdb.ParserException: Parser Error: syntax error at or near "source"

LINE 140: ...  'CATALOG'  = 'STORE' THEN INVALID_REFERENCE_BUG_<Missing source reference to sales.quantity> ELSE NULL END as numeric...
                                                                        ^
```
