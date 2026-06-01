# Query 49

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (34 rows) |
| reference execution | OK (34 rows) |
| results identical | YES |

## Result comparison

v4 rows: 34 (34 distinct)
ref rows: 34 (34 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 5732 | 124 | 63.92 ms |
| reference | 5628 | 128 | 64.14 ms |
| v4 / ref | 1.02x | 0.97x | 1.00x |

## Preql

```
import all_sales as sales;

# Per-(channel, item) ratios. Filter requires a returns row with return_amount > 10000
# (acts as INNER on LEFT JOIN to returns), and Dec 2001.
def in_window(metric) -> sum(metric) by sales.sales_channel, sales.item.id;

auto return_qty <- sum(sales.return_quantity) by sales.sales_channel, sales.item.id;
auto sale_qty <- sum(sales.quantity) by sales.sales_channel, sales.item.id;
auto return_amt <- sum(sales.return_amount) by sales.sales_channel, sales.item.id;
auto sale_paid <- sum(sales.net_paid) by sales.sales_channel, sales.item.id;
auto return_rank <- rank(sales.item.id) over (partition by sales.sales_channel order by return_ratio asc);
auto currency_rank <- rank(sales.item.id) over (partition by sales.sales_channel order by currency_ratio asc);
auto channel_label <- case
    when sales.sales_channel = 'WEB' then 'web'
    when sales.sales_channel = 'CATALOG' then 'catalog'
    when sales.sales_channel = 'STORE' then 'store'
    else null
end;
auto return_ratio <- return_qty::numeric(15,4) / sale_qty::numeric(15,4);
auto currency_ratio <- return_amt::numeric(15,4) / sale_paid::numeric(15,4);

where
    sales.return_amount > 10000
    and sales.net_profit > 1
    and sales.net_paid > 0
    and sales.quantity > 0
    and sales.date.year = 2001
    and sales.date.month_of_year = 12
select
    channel_label as channel,
    sales.item.id as item,
    return_ratio,
    return_rank,
    currency_rank,
having
    return_rank <= 10 or currency_rank <= 10

order by
    channel asc nulls first,
    return_rank asc nulls first,
    currency_rank asc nulls first,
    item asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
cheerful as (
SELECT
    "sales_catalog_returns_unified"."CR_ITEM_SK" as "sales_item_id",
    "sales_catalog_returns_unified"."CR_ORDER_NUMBER" as "sales_order_id",
    "sales_catalog_returns_unified"."CR_RETURN_AMOUNT" as "sales_return_amount",
    "sales_catalog_returns_unified"."CR_RETURN_QUANTITY" as "sales_return_quantity",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_returns" as "sales_catalog_returns_unified"
WHERE
    "sales_catalog_returns_unified"."CR_RETURN_AMOUNT" > 10000

UNION ALL
SELECT
    "sales_store_returns_unified"."SR_ITEM_SK" as "sales_item_id",
    "sales_store_returns_unified"."SR_TICKET_NUMBER" as "sales_order_id",
    "sales_store_returns_unified"."SR_RETURN_AMT" as "sales_return_amount",
    "sales_store_returns_unified"."SR_RETURN_QUANTITY" as "sales_return_quantity",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_returns" as "sales_store_returns_unified"
WHERE
    "sales_store_returns_unified"."SR_RETURN_AMT" > 10000

UNION ALL
SELECT
    "sales_web_returns_unified"."WR_ITEM_SK" as "sales_item_id",
    "sales_web_returns_unified"."WR_ORDER_NUMBER" as "sales_order_id",
    "sales_web_returns_unified"."WR_RETURN_AMT" as "sales_return_amount",
    "sales_web_returns_unified"."WR_RETURN_QUANTITY" as "sales_return_quantity",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_returns" as "sales_web_returns_unified"
WHERE
    "sales_web_returns_unified"."WR_RETURN_AMT" > 10000
),
abundant as (
SELECT
    "sales_catalog_sales_unified"."CS_ITEM_SK" as "sales_item_id",
    "sales_catalog_sales_unified"."CS_NET_PAID" as "sales_net_paid",
    "sales_catalog_sales_unified"."CS_ORDER_NUMBER" as "sales_order_id",
    "sales_catalog_sales_unified"."CS_QUANTITY" as "sales_quantity",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_catalog_sales_unified"."CS_NET_PROFIT" > 1 and "sales_catalog_sales_unified"."CS_NET_PAID" > 0 and "sales_catalog_sales_unified"."CS_QUANTITY" > 0 and "sales_date_date"."D_YEAR" = 2001 and "sales_date_date"."D_MOY" = 12

UNION ALL
SELECT
    "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_id",
    "sales_store_sales_unified"."SS_NET_PAID" as "sales_net_paid",
    "sales_store_sales_unified"."SS_TICKET_NUMBER" as "sales_order_id",
    "sales_store_sales_unified"."SS_QUANTITY" as "sales_quantity",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_store_sales_unified"."SS_NET_PROFIT" > 1 and "sales_store_sales_unified"."SS_NET_PAID" > 0 and "sales_store_sales_unified"."SS_QUANTITY" > 0 and "sales_date_date"."D_YEAR" = 2001 and "sales_date_date"."D_MOY" = 12

UNION ALL
SELECT
    "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_id",
    "sales_web_sales_unified"."WS_NET_PAID" as "sales_net_paid",
    "sales_web_sales_unified"."WS_ORDER_NUMBER" as "sales_order_id",
    "sales_web_sales_unified"."WS_QUANTITY" as "sales_quantity",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_web_sales_unified"."WS_NET_PROFIT" > 1 and "sales_web_sales_unified"."WS_NET_PAID" > 0 and "sales_web_sales_unified"."WS_QUANTITY" > 0 and "sales_date_date"."D_YEAR" = 2001 and "sales_date_date"."D_MOY" = 12
),
yummy as (
SELECT
    "abundant"."sales_item_id" as "sales_item_id",
    "abundant"."sales_sales_channel" as "sales_sales_channel",
    sum("abundant"."sales_net_paid") as "sale_paid",
    sum("abundant"."sales_quantity") as "sale_qty",
    sum("cheerful"."sales_return_amount") as "return_amt",
    sum("cheerful"."sales_return_quantity") as "return_qty"
FROM
    "abundant"
    INNER JOIN "cheerful" on "abundant"."sales_item_id" = "cheerful"."sales_item_id" AND "abundant"."sales_order_id" = "cheerful"."sales_order_id" AND "abundant"."sales_sales_channel" = "cheerful"."sales_sales_channel"
WHERE
    "cheerful"."sales_return_amount" > 10000

GROUP BY
    1,
    2),
vacuous as (
SELECT
    "yummy"."sales_item_id" as "sales_item_id",
    "yummy"."sales_sales_channel" as "sales_sales_channel",
    cast("yummy"."return_qty" as numeric(15,4)) / cast("yummy"."sale_qty" as numeric(15,4)) as "return_ratio",
    rank() over (partition by "yummy"."sales_sales_channel" order by cast("yummy"."return_amt" as numeric(15,4)) / cast("yummy"."sale_paid" as numeric(15,4)) asc ) as "currency_rank",
    rank() over (partition by "yummy"."sales_sales_channel" order by cast("yummy"."return_qty" as numeric(15,4)) / cast("yummy"."sale_qty" as numeric(15,4)) asc ) as "return_rank"
FROM
    "yummy")
SELECT
    CASE
	WHEN "vacuous"."sales_sales_channel" = 'WEB' THEN 'web'
	WHEN "vacuous"."sales_sales_channel" = 'CATALOG' THEN 'catalog'
	WHEN "vacuous"."sales_sales_channel" = 'STORE' THEN 'store'
	ELSE null
	END as "channel",
    "vacuous"."sales_item_id" as "item",
    "vacuous"."return_ratio" as "return_ratio",
    "vacuous"."return_rank" as "return_rank",
    "vacuous"."currency_rank" as "currency_rank"
FROM
    "vacuous"
WHERE
    "vacuous"."return_rank" <= 10 or "vacuous"."currency_rank" <= 10

ORDER BY 
    "channel" asc nulls first,
    "vacuous"."return_rank" asc nulls first,
    "vacuous"."currency_rank" asc nulls first,
    "item" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
cheerful as (
SELECT
    "sales_catalog_returns_unified"."CR_ITEM_SK" as "sales_item_id",
    "sales_catalog_returns_unified"."CR_ORDER_NUMBER" as "sales_order_id",
    "sales_catalog_returns_unified"."CR_RETURN_AMOUNT" as "sales_return_amount",
    "sales_catalog_returns_unified"."CR_RETURN_QUANTITY" as "sales_return_quantity",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_returns" as "sales_catalog_returns_unified"
WHERE
    "sales_catalog_returns_unified"."CR_RETURN_AMOUNT" > 10000

UNION ALL
SELECT
    "sales_store_returns_unified"."SR_ITEM_SK" as "sales_item_id",
    "sales_store_returns_unified"."SR_TICKET_NUMBER" as "sales_order_id",
    "sales_store_returns_unified"."SR_RETURN_AMT" as "sales_return_amount",
    "sales_store_returns_unified"."SR_RETURN_QUANTITY" as "sales_return_quantity",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_returns" as "sales_store_returns_unified"
WHERE
    "sales_store_returns_unified"."SR_RETURN_AMT" > 10000

UNION ALL
SELECT
    "sales_web_returns_unified"."WR_ITEM_SK" as "sales_item_id",
    "sales_web_returns_unified"."WR_ORDER_NUMBER" as "sales_order_id",
    "sales_web_returns_unified"."WR_RETURN_AMT" as "sales_return_amount",
    "sales_web_returns_unified"."WR_RETURN_QUANTITY" as "sales_return_quantity",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_returns" as "sales_web_returns_unified"
WHERE
    "sales_web_returns_unified"."WR_RETURN_AMT" > 10000
),
abundant as (
SELECT
    "sales_catalog_sales_unified"."CS_ITEM_SK" as "sales_item_id",
    "sales_catalog_sales_unified"."CS_NET_PAID" as "sales_net_paid",
    "sales_catalog_sales_unified"."CS_ORDER_NUMBER" as "sales_order_id",
    "sales_catalog_sales_unified"."CS_QUANTITY" as "sales_quantity",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_catalog_sales_unified"."CS_NET_PROFIT" > 1 and "sales_catalog_sales_unified"."CS_NET_PAID" > 0 and "sales_catalog_sales_unified"."CS_QUANTITY" > 0 and "sales_date_date"."D_YEAR" = 2001 and "sales_date_date"."D_MOY" = 12

UNION ALL
SELECT
    "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_id",
    "sales_store_sales_unified"."SS_NET_PAID" as "sales_net_paid",
    "sales_store_sales_unified"."SS_TICKET_NUMBER" as "sales_order_id",
    "sales_store_sales_unified"."SS_QUANTITY" as "sales_quantity",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_store_sales_unified"."SS_NET_PROFIT" > 1 and "sales_store_sales_unified"."SS_NET_PAID" > 0 and "sales_store_sales_unified"."SS_QUANTITY" > 0 and "sales_date_date"."D_YEAR" = 2001 and "sales_date_date"."D_MOY" = 12

UNION ALL
SELECT
    "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_id",
    "sales_web_sales_unified"."WS_NET_PAID" as "sales_net_paid",
    "sales_web_sales_unified"."WS_ORDER_NUMBER" as "sales_order_id",
    "sales_web_sales_unified"."WS_QUANTITY" as "sales_quantity",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_web_sales_unified"."WS_NET_PROFIT" > 1 and "sales_web_sales_unified"."WS_NET_PAID" > 0 and "sales_web_sales_unified"."WS_QUANTITY" > 0 and "sales_date_date"."D_YEAR" = 2001 and "sales_date_date"."D_MOY" = 12
),
yummy as (
SELECT
    "abundant"."sales_item_id" as "sales_item_id",
    "abundant"."sales_sales_channel" as "sales_sales_channel",
    cast(sum("cheerful"."sales_return_amount") as numeric(15,4)) / cast(sum("abundant"."sales_net_paid") as numeric(15,4)) as "currency_ratio",
    cast(sum("cheerful"."sales_return_quantity") as numeric(15,4)) / cast(sum("abundant"."sales_quantity") as numeric(15,4)) as "return_ratio"
FROM
    "abundant"
    INNER JOIN "cheerful" on "abundant"."sales_item_id" = "cheerful"."sales_item_id" AND "abundant"."sales_order_id" = "cheerful"."sales_order_id" AND "abundant"."sales_sales_channel" = "cheerful"."sales_sales_channel"
WHERE
    "cheerful"."sales_return_amount" > 10000

GROUP BY
    1,
    2),
vacuous as (
SELECT
    "yummy"."return_ratio" as "return_ratio",
    "yummy"."sales_item_id" as "sales_item_id",
    CASE
	WHEN "yummy"."sales_sales_channel" = 'WEB' THEN 'web'
	WHEN "yummy"."sales_sales_channel" = 'CATALOG' THEN 'catalog'
	WHEN "yummy"."sales_sales_channel" = 'STORE' THEN 'store'
	ELSE null
	END as "channel_label",
    rank() over (partition by "yummy"."sales_sales_channel" order by "yummy"."currency_ratio" asc ) as "currency_rank",
    rank() over (partition by "yummy"."sales_sales_channel" order by "yummy"."return_ratio" asc ) as "return_rank"
FROM
    "yummy")
SELECT
    "vacuous"."channel_label" as "channel",
    "vacuous"."sales_item_id" as "item",
    "vacuous"."return_ratio" as "return_ratio",
    "vacuous"."return_rank" as "return_rank",
    "vacuous"."currency_rank" as "currency_rank"
FROM
    "vacuous"
WHERE
    "vacuous"."return_rank" <= 10 or "vacuous"."currency_rank" <= 10

GROUP BY
    1,
    2,
    3,
    4,
    5
ORDER BY 
    "channel" asc nulls first,
    "vacuous"."return_rank" asc nulls first,
    "vacuous"."currency_rank" asc nulls first,
    "item" asc nulls first
LIMIT (100)
```
