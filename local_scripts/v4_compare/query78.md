# Query 78

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
| v4 | 6361 | 128 | 327.82 ms |
| reference | 6303 | 109 | 307.19 ms |
| v4 / ref | 1.01x | 1.17x | 1.07x |

## Preql

```
import all_sales as sales;

def channel_sum(metric, channel) -> sum(metric ? sales.sales_channel = channel) by sales.date.year, sales.item.id, sales.billing_customer.id;

auto ss_qty <- sum(sales.quantity ? sales.sales_channel = 'STORE')
    by sales.date.year, sales.item.id, sales.billing_customer.id;
auto ss_wc <- sum(sales.wholesale_cost ? sales.sales_channel = 'STORE')
    by sales.date.year, sales.item.id, sales.billing_customer.id;
auto ss_sp <- sum(sales.sales_price ? sales.sales_channel = 'STORE')
    by sales.date.year, sales.item.id, sales.billing_customer.id;
auto ws_qty <- sum(sales.quantity ? sales.sales_channel = 'WEB')
    by sales.date.year, sales.item.id, sales.billing_customer.id;
auto ws_wc <- sum(sales.wholesale_cost ? sales.sales_channel = 'WEB')
    by sales.date.year, sales.item.id, sales.billing_customer.id;
auto ws_sp <- sum(sales.sales_price ? sales.sales_channel = 'WEB')
    by sales.date.year, sales.item.id, sales.billing_customer.id;
auto cs_qty <- sum(sales.quantity ? sales.sales_channel = 'CATALOG')
    by sales.date.year, sales.item.id, sales.billing_customer.id;
auto cs_wc <- sum(sales.wholesale_cost ? sales.sales_channel = 'CATALOG')
    by sales.date.year, sales.item.id, sales.billing_customer.id;
auto cs_sp <- sum(sales.sales_price ? sales.sales_channel = 'CATALOG')
    by sales.date.year, sales.item.id, sales.billing_customer.id;
auto other_chan_qty <- coalesce(ws_qty, 0) + coalesce(cs_qty, 0);
auto other_chan_wc <- coalesce(ws_wc, 0) + coalesce(cs_wc, 0);
auto other_chan_sp <- coalesce(ws_sp, 0) + coalesce(cs_sp, 0);

where
    sales.is_returned is null and sales.date.year = 2000 and sales.billing_customer.id is not null
select
    sales.date.year as ss_sold_year,
    sales.item.id as ss_item_sk,
    sales.billing_customer.id as ss_customer_sk,
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
    "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" as "sales_billing_customer_id",
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
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "sales_billing_customer_id",
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
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" as "sales_billing_customer_id",
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
    "abundant"."sales_billing_customer_id" as "sales_billing_customer_id",
    "abundant"."sales_date_year" as "sales_date_year",
    "abundant"."sales_item_id" as "sales_item_id",
    sum(CASE WHEN "abundant"."sales_sales_channel" = 'CATALOG' THEN "abundant"."sales_quantity" ELSE NULL END) as "cs_qty",
    sum(CASE WHEN "abundant"."sales_sales_channel" = 'CATALOG' THEN "abundant"."sales_sales_price" ELSE NULL END) as "cs_sp",
    sum(CASE WHEN "abundant"."sales_sales_channel" = 'CATALOG' THEN "abundant"."sales_wholesale_cost" ELSE NULL END) as "cs_wc",
    sum(CASE WHEN "abundant"."sales_sales_channel" = 'STORE' THEN "abundant"."sales_quantity" ELSE NULL END) as "ss_qty",
    sum(CASE WHEN "abundant"."sales_sales_channel" = 'STORE' THEN "abundant"."sales_sales_price" ELSE NULL END) as "ss_sp",
    sum(CASE WHEN "abundant"."sales_sales_channel" = 'STORE' THEN "abundant"."sales_wholesale_cost" ELSE NULL END) as "ss_wc",
    sum(CASE WHEN "abundant"."sales_sales_channel" = 'WEB' THEN "abundant"."sales_quantity" ELSE NULL END) as "ws_qty",
    sum(CASE WHEN "abundant"."sales_sales_channel" = 'WEB' THEN "abundant"."sales_sales_price" ELSE NULL END) as "ws_sp",
    sum(CASE WHEN "abundant"."sales_sales_channel" = 'WEB' THEN "abundant"."sales_wholesale_cost" ELSE NULL END) as "ws_wc"
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
    ( coalesce("ws_qty",0) > 0 or coalesce("cs_qty",0) > 0 )
)
SELECT
    "yummy"."sales_date_year" as "ss_sold_year",
    "yummy"."sales_item_id" as "ss_item_sk",
    "yummy"."sales_billing_customer_id" as "ss_customer_sk",
    round(cast("yummy"."ss_qty" as numeric(7,2)) / cast(coalesce("yummy"."ws_qty",0) + coalesce("yummy"."cs_qty",0) as numeric(7,2)),2) as "ratio",
    "yummy"."ss_qty" as "store_qty",
    "yummy"."ss_wc" as "store_wholesale_cost",
    "yummy"."ss_sp" as "store_sales_price",
    coalesce("yummy"."ws_qty",0) + coalesce("yummy"."cs_qty",0) as "other_chan_qty",
    coalesce("yummy"."ws_wc",0) + coalesce("yummy"."cs_wc",0) as "other_chan_wholesale_cost",
    coalesce("yummy"."ws_sp",0) + coalesce("yummy"."cs_sp",0) as "other_chan_sales_price"
FROM
    "yummy"
WHERE
    "yummy"."ss_qty" > 0

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
    "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" as "sales_billing_customer_id",
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
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "sales_billing_customer_id",
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
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" as "sales_billing_customer_id",
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
    "abundant"."sales_billing_customer_id" as "ss_customer_sk",
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
