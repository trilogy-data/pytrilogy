# Query 05

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
| v4 | 13665 | 309 | — |
| reference | 10731 | 230 | 108.07 ms |
| v4 / ref | 1.27x | 1.34x | — |

## Preql

```
import all_sales as sales;

auto channel_label <- case
    when sales.sales_channel = 'STORE' then 'store channel'
    when sales.sales_channel = 'CATALOG' then 'catalog channel'
    when sales.sales_channel = 'WEB' then 'web channel'
    else null
end;
auto sales_id_label <- case
    when sales.sales_channel = 'STORE' then concat('store', sales.channel_dim_text_id)
    when sales.sales_channel = 'CATALOG' then concat('catalog_page', sales.channel_dim_text_id)
    when sales.sales_channel = 'WEB' then concat('web_site', sales.channel_dim_text_id)
    else null
end;
auto return_id_label <- case
    when sales.sales_channel = 'STORE' then concat('store', sales.return_channel_dim_text_id)
    when sales.sales_channel = 'CATALOG' then concat('catalog_page', sales.return_channel_dim_text_id)
    when sales.sales_channel = 'WEB' then concat('web_site', sales.return_channel_dim_text_id)
    else null
end;

where
    sales.channel_dim_text_id is not null
    and sales.date.date between '2000-08-23'::date and '2000-09-06'::date
select
    --channel_label as s_channel,
    --sales_id_label as s_id,
    --sum(sales.ext_sales_price) by rollup channel_label, sales_id_label as sales_total_a,
    --sum(sales.net_profit) by rollup channel_label, sales_id_label as profit_only_a,
merge
where
    sales.return_channel_dim_text_id is not null
    and sales.return_date.date between '2000-08-23'::date and '2000-09-06'::date
select
    --channel_label as r_channel,
    --return_id_label as r_id,
    --sum(coalesce(sales.return_amount, 0)) by rollup channel_label, return_id_label as returns_total_b,
    --sum(coalesce(sales.return_net_loss, 0)) by rollup channel_label, return_id_label as loss_only_b,
align
    channel: s_channel, r_channel
    and id: s_id, r_id
derive
    coalesce(sales_total_a, 0.0) -> sales_metric,
    coalesce(returns_total_b, 0.0) -> returns_metric,
    coalesce(profit_only_a, 0.0) - coalesce(loss_only_b, 0.0) -> profit_metric
order by
    channel asc nulls first,
    id asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
scrawny as (
SELECT
    "sales_catalog_dim_unified"."CP_CATALOG_PAGE_SK" as "sales_channel_dim_id",
    "sales_catalog_dim_unified"."CP_CATALOG_PAGE_ID" as "sales_channel_dim_text_id",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_page" as "sales_catalog_dim_unified"
WHERE
    "sales_catalog_dim_unified"."CP_CATALOG_PAGE_ID" is not null

UNION ALL
SELECT
    "sales_store_dim_unified"."S_STORE_SK" as "sales_channel_dim_id",
    "sales_store_dim_unified"."S_STORE_ID" as "sales_channel_dim_text_id",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store" as "sales_store_dim_unified"
WHERE
    "sales_store_dim_unified"."S_STORE_ID" is not null

UNION ALL
SELECT
    "sales_web_dim_unified"."web_site_sk" as "sales_channel_dim_id",
    "sales_web_dim_unified"."web_site_id" as "sales_channel_dim_text_id",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_site" as "sales_web_dim_unified"
WHERE
    "sales_web_dim_unified"."web_site_id" is not null
),
divergent as (
SELECT
    "sales_catalog_sales_unified"."CS_CATALOG_PAGE_SK" as "sales_channel_dim_id",
    "sales_catalog_sales_unified"."CS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_catalog_sales_unified"."CS_NET_PROFIT" as "sales_net_profit",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    cast("sales_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-06'

UNION ALL
SELECT
    "sales_store_sales_unified"."SS_STORE_SK" as "sales_channel_dim_id",
    "sales_store_sales_unified"."SS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_store_sales_unified"."SS_NET_PROFIT" as "sales_net_profit",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    cast("sales_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-06'

UNION ALL
SELECT
    "sales_web_sales_unified"."WS_WEB_SITE_SK" as "sales_channel_dim_id",
    "sales_web_sales_unified"."WS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_web_sales_unified"."WS_NET_PROFIT" as "sales_net_profit",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    cast("sales_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-06'
),
cheerful as (
SELECT
    "sales_catalog_dim_return_unified"."CP_CATALOG_PAGE_SK" as "sales_return_channel_dim_id",
    "sales_catalog_dim_return_unified"."CP_CATALOG_PAGE_ID" as "sales_return_channel_dim_text_id",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_page" as "sales_catalog_dim_return_unified"
WHERE
    "sales_catalog_dim_return_unified"."CP_CATALOG_PAGE_ID" is not null

UNION ALL
SELECT
    "sales_store_dim_return_unified"."S_STORE_SK" as "sales_return_channel_dim_id",
    "sales_store_dim_return_unified"."S_STORE_ID" as "sales_return_channel_dim_text_id",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store" as "sales_store_dim_return_unified"
WHERE
    "sales_store_dim_return_unified"."S_STORE_ID" is not null

UNION ALL
SELECT
    "sales_web_dim_return_unified"."web_site_sk" as "sales_return_channel_dim_id",
    "sales_web_dim_return_unified"."web_site_id" as "sales_return_channel_dim_text_id",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_site" as "sales_web_dim_return_unified"
WHERE
    "sales_web_dim_return_unified"."web_site_id" is not null
),
abundant as (
SELECT
    "sales_catalog_returns_unified"."CR_ITEM_SK" as "sales_item_id",
    "sales_catalog_returns_unified"."CR_ORDER_NUMBER" as "sales_order_id",
    "sales_catalog_returns_unified"."CR_RETURN_AMOUNT" as "sales_return_amount",
    "sales_catalog_returns_unified"."CR_RETURNED_DATE_SK" as "sales_return_date_id",
    "sales_catalog_returns_unified"."CR_NET_LOSS" as "sales_return_net_loss",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_returns" as "sales_catalog_returns_unified"
UNION ALL
SELECT
    "sales_store_returns_unified"."SR_ITEM_SK" as "sales_item_id",
    "sales_store_returns_unified"."SR_TICKET_NUMBER" as "sales_order_id",
    "sales_store_returns_unified"."SR_RETURN_AMT" as "sales_return_amount",
    "sales_store_returns_unified"."SR_RETURNED_DATE_SK" as "sales_return_date_id",
    "sales_store_returns_unified"."SR_NET_LOSS" as "sales_return_net_loss",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_returns" as "sales_store_returns_unified"
UNION ALL
SELECT
    "sales_web_returns_unified"."WR_ITEM_SK" as "sales_item_id",
    "sales_web_returns_unified"."WR_ORDER_NUMBER" as "sales_order_id",
    "sales_web_returns_unified"."WR_RETURN_AMT" as "sales_return_amount",
    "sales_web_returns_unified"."WR_RETURNED_DATE_SK" as "sales_return_date_id",
    "sales_web_returns_unified"."WR_NET_LOSS" as "sales_return_net_loss",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_returns" as "sales_web_returns_unified"),
yummy as (
SELECT
    "sales_catalog_returns_unified"."CR_ITEM_SK" as "sales_item_id",
    "sales_catalog_returns_unified"."CR_ORDER_NUMBER" as "sales_order_id",
    "sales_catalog_returns_unified"."CR_CATALOG_PAGE_SK" as "sales_return_channel_dim_id",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_returns" as "sales_catalog_returns_unified"
UNION ALL
SELECT
    "sales_store_returns_unified"."SR_ITEM_SK" as "sales_item_id",
    "sales_store_returns_unified"."SR_TICKET_NUMBER" as "sales_order_id",
    "sales_store_returns_unified"."SR_STORE_SK" as "sales_return_channel_dim_id",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_returns" as "sales_store_returns_unified"
UNION ALL
SELECT
    "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_id",
    "sales_web_sales_unified"."WS_ORDER_NUMBER" as "sales_order_id",
    "sales_web_sales_unified"."WS_WEB_SITE_SK" as "sales_return_channel_dim_id",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"),
charming as (
SELECT
    "divergent"."sales_ext_sales_price" as "sales_ext_sales_price",
    "divergent"."sales_net_profit" as "sales_net_profit",
    "divergent"."sales_sales_channel" as "sales_sales_channel",
    "scrawny"."sales_channel_dim_text_id" as "sales_channel_dim_text_id"
FROM
    "divergent"
    INNER JOIN "scrawny" on "divergent"."sales_channel_dim_id" = "scrawny"."sales_channel_dim_id" AND "divergent"."sales_sales_channel" = "scrawny"."sales_sales_channel"
WHERE
    "scrawny"."sales_channel_dim_text_id" is not null
),
vacuous as (
SELECT
    "abundant"."sales_return_amount" as "sales_return_amount",
    "abundant"."sales_return_net_loss" as "sales_return_net_loss",
    "cheerful"."sales_return_channel_dim_text_id" as "sales_return_channel_dim_text_id",
    coalesce("abundant"."sales_sales_channel","cheerful"."sales_sales_channel","yummy"."sales_sales_channel") as "sales_sales_channel"
FROM
    "yummy"
    INNER JOIN "abundant" on "yummy"."sales_item_id" = "abundant"."sales_item_id" AND "yummy"."sales_order_id" = "abundant"."sales_order_id" AND "yummy"."sales_sales_channel" = "abundant"."sales_sales_channel"
    INNER JOIN "memory"."date_dim" as "sales_return_date_date" on "abundant"."sales_return_date_id" = "sales_return_date_date"."D_DATE_SK"
    INNER JOIN "cheerful" on "yummy"."sales_return_channel_dim_id" = "cheerful"."sales_return_channel_dim_id" AND coalesce("yummy"."sales_sales_channel", "abundant"."sales_sales_channel") = "cheerful"."sales_sales_channel"
WHERE
    "cheerful"."sales_return_channel_dim_text_id" is not null and cast("sales_return_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-06'
),
premium as (
SELECT
    "charming"."sales_ext_sales_price" as "sales_ext_sales_price",
    "charming"."sales_net_profit" as "sales_net_profit",
    CASE
	WHEN "charming"."sales_sales_channel" = 'STORE' THEN 'store channel'
	WHEN "charming"."sales_sales_channel" = 'CATALOG' THEN 'catalog channel'
	WHEN "charming"."sales_sales_channel" = 'WEB' THEN 'web channel'
	ELSE null
	END as "channel_label",
    CASE
	WHEN "charming"."sales_sales_channel" = 'STORE' THEN 'store channel'
	WHEN "charming"."sales_sales_channel" = 'CATALOG' THEN 'catalog channel'
	WHEN "charming"."sales_sales_channel" = 'WEB' THEN 'web channel'
	ELSE null
	END as "s_channel",
    CASE
	WHEN "charming"."sales_sales_channel" = 'STORE' THEN ('store' || "charming"."sales_channel_dim_text_id")
	WHEN "charming"."sales_sales_channel" = 'CATALOG' THEN ('catalog_page' || "charming"."sales_channel_dim_text_id")
	WHEN "charming"."sales_sales_channel" = 'WEB' THEN ('web_site' || "charming"."sales_channel_dim_text_id")
	ELSE null
	END as "sales_id_label"
FROM
    "charming"),
protective as (
SELECT
    CASE
	WHEN "charming"."sales_sales_channel" = 'STORE' THEN 'store channel'
	WHEN "charming"."sales_sales_channel" = 'CATALOG' THEN 'catalog channel'
	WHEN "charming"."sales_sales_channel" = 'WEB' THEN 'web channel'
	ELSE null
	END as "channel_label",
    CASE
	WHEN "charming"."sales_sales_channel" = 'STORE' THEN ('store' || "charming"."sales_channel_dim_text_id")
	WHEN "charming"."sales_sales_channel" = 'CATALOG' THEN ('catalog_page' || "charming"."sales_channel_dim_text_id")
	WHEN "charming"."sales_sales_channel" = 'WEB' THEN ('web_site' || "charming"."sales_channel_dim_text_id")
	ELSE null
	END as "s_id"
FROM
    "charming"),
sparkling as (
SELECT
    CASE
	WHEN "vacuous"."sales_sales_channel" = 'STORE' THEN 'store channel'
	WHEN "vacuous"."sales_sales_channel" = 'CATALOG' THEN 'catalog channel'
	WHEN "vacuous"."sales_sales_channel" = 'WEB' THEN 'web channel'
	ELSE null
	END as "channel_label",
    CASE
	WHEN "vacuous"."sales_sales_channel" = 'STORE' THEN 'store channel'
	WHEN "vacuous"."sales_sales_channel" = 'CATALOG' THEN 'catalog channel'
	WHEN "vacuous"."sales_sales_channel" = 'WEB' THEN 'web channel'
	ELSE null
	END as "r_channel",
    CASE
	WHEN "vacuous"."sales_sales_channel" = 'STORE' THEN ('store' || "vacuous"."sales_return_channel_dim_text_id")
	WHEN "vacuous"."sales_sales_channel" = 'CATALOG' THEN ('catalog_page' || "vacuous"."sales_return_channel_dim_text_id")
	WHEN "vacuous"."sales_sales_channel" = 'WEB' THEN ('web_site' || "vacuous"."sales_return_channel_dim_text_id")
	ELSE null
	END as "r_id"
FROM
    "vacuous"),
concerned as (
SELECT
    "vacuous"."sales_return_amount" as "sales_return_amount",
    "vacuous"."sales_return_net_loss" as "sales_return_net_loss",
    CASE
	WHEN "vacuous"."sales_sales_channel" = 'STORE' THEN 'store channel'
	WHEN "vacuous"."sales_sales_channel" = 'CATALOG' THEN 'catalog channel'
	WHEN "vacuous"."sales_sales_channel" = 'WEB' THEN 'web channel'
	ELSE null
	END as "channel_label",
    CASE
	WHEN "vacuous"."sales_sales_channel" = 'STORE' THEN ('store' || "vacuous"."sales_return_channel_dim_text_id")
	WHEN "vacuous"."sales_sales_channel" = 'CATALOG' THEN ('catalog_page' || "vacuous"."sales_return_channel_dim_text_id")
	WHEN "vacuous"."sales_sales_channel" = 'WEB' THEN ('web_site' || "vacuous"."sales_return_channel_dim_text_id")
	ELSE null
	END as "return_id_label"
FROM
    "vacuous"),
puzzled as (
SELECT
    "premium"."channel_label" as "channel_label",
    "premium"."s_channel" as "s_channel",
    "premium"."sales_id_label" as "sales_id_label",
    sum("premium"."sales_ext_sales_price") as "sales_total_a",
    sum("premium"."sales_net_profit") as "profit_only_a"
FROM
    "premium"
GROUP BY
    ROLLUP (1, 3)),
young as (
SELECT
    "concerned"."channel_label" as "channel_label",
    "concerned"."return_id_label" as "return_id_label",
    sum(coalesce("concerned"."sales_return_amount",0)) as "returns_total_b",
    sum(coalesce("concerned"."sales_return_net_loss",0)) as "loss_only_b"
FROM
    "concerned"
GROUP BY
    ROLLUP (1, 2)),
waggish as (
SELECT
    "protective"."s_id" as "id",
    "puzzled"."profit_only_a" as "profit_only_a",
    "puzzled"."s_channel" as "channel",
    "puzzled"."sales_total_a" as "sales_total_a"
FROM
    "puzzled"
    FULL JOIN "protective" on "puzzled"."channel_label" is not distinct from "protective"."channel_label" AND "puzzled"."sales_id_label" is not distinct from "protective"."s_id"),
abhorrent as (
SELECT
    "sparkling"."r_channel" as "channel",
    "sparkling"."r_id" as "id",
    "young"."loss_only_b" as "loss_only_b",
    "young"."returns_total_b" as "returns_total_b"
FROM
    "sparkling"
    FULL JOIN "young" on "sparkling"."channel_label" is not distinct from "young"."channel_label" AND "sparkling"."r_id" is not distinct from "young"."return_id_label")
SELECT
    coalesce("abhorrent"."channel","waggish"."channel") as "channel",
    coalesce("abhorrent"."returns_total_b",0.0) as "returns_metric",
    coalesce("waggish"."sales_total_a",0.0) as "sales_metric",
    coalesce("waggish"."profit_only_a",0.0) - coalesce("abhorrent"."loss_only_b",0.0) as "profit_metric",
    coalesce("abhorrent"."id","waggish"."id") as "id"
FROM
    "waggish"
    FULL JOIN "abhorrent" on "waggish"."channel" is not distinct from "abhorrent"."channel" AND "waggish"."id" is not distinct from "abhorrent"."id"
ORDER BY 
    coalesce("abhorrent"."channel","waggish"."channel") asc nulls first,
    coalesce("abhorrent"."id","waggish"."id") asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
sweltering as (
SELECT
    "sales_catalog_dim_unified"."CP_CATALOG_PAGE_SK" as "sales_channel_dim_id",
    "sales_catalog_dim_unified"."CP_CATALOG_PAGE_ID" as "sales_channel_dim_text_id",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_page" as "sales_catalog_dim_unified"
WHERE
    "sales_catalog_dim_unified"."CP_CATALOG_PAGE_ID" is not null

UNION ALL
SELECT
    "sales_store_dim_unified"."S_STORE_SK" as "sales_channel_dim_id",
    "sales_store_dim_unified"."S_STORE_ID" as "sales_channel_dim_text_id",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store" as "sales_store_dim_unified"
WHERE
    "sales_store_dim_unified"."S_STORE_ID" is not null

UNION ALL
SELECT
    "sales_web_dim_unified"."web_site_sk" as "sales_channel_dim_id",
    "sales_web_dim_unified"."web_site_id" as "sales_channel_dim_text_id",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_site" as "sales_web_dim_unified"
WHERE
    "sales_web_dim_unified"."web_site_id" is not null
),
scrawny as (
SELECT
    "sales_catalog_sales_unified"."CS_CATALOG_PAGE_SK" as "sales_channel_dim_id",
    "sales_catalog_sales_unified"."CS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_catalog_sales_unified"."CS_NET_PROFIT" as "sales_net_profit",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    cast("sales_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-06'

UNION ALL
SELECT
    "sales_store_sales_unified"."SS_STORE_SK" as "sales_channel_dim_id",
    "sales_store_sales_unified"."SS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_store_sales_unified"."SS_NET_PROFIT" as "sales_net_profit",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    cast("sales_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-06'

UNION ALL
SELECT
    "sales_web_sales_unified"."WS_WEB_SITE_SK" as "sales_channel_dim_id",
    "sales_web_sales_unified"."WS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_web_sales_unified"."WS_NET_PROFIT" as "sales_net_profit",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    cast("sales_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-06'
),
cheerful as (
SELECT
    "sales_catalog_dim_return_unified"."CP_CATALOG_PAGE_SK" as "sales_return_channel_dim_id",
    "sales_catalog_dim_return_unified"."CP_CATALOG_PAGE_ID" as "sales_return_channel_dim_text_id",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_page" as "sales_catalog_dim_return_unified"
WHERE
    "sales_catalog_dim_return_unified"."CP_CATALOG_PAGE_ID" is not null

UNION ALL
SELECT
    "sales_store_dim_return_unified"."S_STORE_SK" as "sales_return_channel_dim_id",
    "sales_store_dim_return_unified"."S_STORE_ID" as "sales_return_channel_dim_text_id",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store" as "sales_store_dim_return_unified"
WHERE
    "sales_store_dim_return_unified"."S_STORE_ID" is not null

UNION ALL
SELECT
    "sales_web_dim_return_unified"."web_site_sk" as "sales_return_channel_dim_id",
    "sales_web_dim_return_unified"."web_site_id" as "sales_return_channel_dim_text_id",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_site" as "sales_web_dim_return_unified"
WHERE
    "sales_web_dim_return_unified"."web_site_id" is not null
),
abundant as (
SELECT
    "sales_catalog_returns_unified"."CR_ITEM_SK" as "sales_item_id",
    "sales_catalog_returns_unified"."CR_ORDER_NUMBER" as "sales_order_id",
    "sales_catalog_returns_unified"."CR_RETURN_AMOUNT" as "sales_return_amount",
    "sales_catalog_returns_unified"."CR_RETURNED_DATE_SK" as "sales_return_date_id",
    "sales_catalog_returns_unified"."CR_NET_LOSS" as "sales_return_net_loss",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_returns" as "sales_catalog_returns_unified"
UNION ALL
SELECT
    "sales_store_returns_unified"."SR_ITEM_SK" as "sales_item_id",
    "sales_store_returns_unified"."SR_TICKET_NUMBER" as "sales_order_id",
    "sales_store_returns_unified"."SR_RETURN_AMT" as "sales_return_amount",
    "sales_store_returns_unified"."SR_RETURNED_DATE_SK" as "sales_return_date_id",
    "sales_store_returns_unified"."SR_NET_LOSS" as "sales_return_net_loss",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_returns" as "sales_store_returns_unified"
UNION ALL
SELECT
    "sales_web_returns_unified"."WR_ITEM_SK" as "sales_item_id",
    "sales_web_returns_unified"."WR_ORDER_NUMBER" as "sales_order_id",
    "sales_web_returns_unified"."WR_RETURN_AMT" as "sales_return_amount",
    "sales_web_returns_unified"."WR_RETURNED_DATE_SK" as "sales_return_date_id",
    "sales_web_returns_unified"."WR_NET_LOSS" as "sales_return_net_loss",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_returns" as "sales_web_returns_unified"),
yummy as (
SELECT
    "sales_catalog_returns_unified"."CR_ITEM_SK" as "sales_item_id",
    "sales_catalog_returns_unified"."CR_ORDER_NUMBER" as "sales_order_id",
    "sales_catalog_returns_unified"."CR_CATALOG_PAGE_SK" as "sales_return_channel_dim_id",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_returns" as "sales_catalog_returns_unified"
UNION ALL
SELECT
    "sales_store_returns_unified"."SR_ITEM_SK" as "sales_item_id",
    "sales_store_returns_unified"."SR_TICKET_NUMBER" as "sales_order_id",
    "sales_store_returns_unified"."SR_STORE_SK" as "sales_return_channel_dim_id",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_returns" as "sales_store_returns_unified"
UNION ALL
SELECT
    "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_id",
    "sales_web_sales_unified"."WS_ORDER_NUMBER" as "sales_order_id",
    "sales_web_sales_unified"."WS_WEB_SITE_SK" as "sales_return_channel_dim_id",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"),
kaput as (
SELECT
    "scrawny"."sales_ext_sales_price" as "sales_ext_sales_price",
    "scrawny"."sales_net_profit" as "sales_net_profit",
    CASE
	WHEN "scrawny"."sales_sales_channel" = 'STORE' THEN 'store channel'
	WHEN "scrawny"."sales_sales_channel" = 'CATALOG' THEN 'catalog channel'
	WHEN "scrawny"."sales_sales_channel" = 'WEB' THEN 'web channel'
	ELSE null
	END as "channel_label",
    CASE
	WHEN "scrawny"."sales_sales_channel" = 'STORE' THEN ('store' || "sweltering"."sales_channel_dim_text_id")
	WHEN "scrawny"."sales_sales_channel" = 'CATALOG' THEN ('catalog_page' || "sweltering"."sales_channel_dim_text_id")
	WHEN "scrawny"."sales_sales_channel" = 'WEB' THEN ('web_site' || "sweltering"."sales_channel_dim_text_id")
	ELSE null
	END as "sales_id_label"
FROM
    "scrawny"
    INNER JOIN "sweltering" on "scrawny"."sales_channel_dim_id" = "sweltering"."sales_channel_dim_id" AND "scrawny"."sales_sales_channel" = "sweltering"."sales_sales_channel"
WHERE
    "sweltering"."sales_channel_dim_text_id" is not null
),
vacuous as (
SELECT
    "abundant"."sales_return_amount" as "sales_return_amount",
    "abundant"."sales_return_net_loss" as "sales_return_net_loss",
    CASE
	WHEN coalesce("abundant"."sales_sales_channel","cheerful"."sales_sales_channel","yummy"."sales_sales_channel") = 'STORE' THEN 'store channel'
	WHEN coalesce("abundant"."sales_sales_channel","cheerful"."sales_sales_channel","yummy"."sales_sales_channel") = 'CATALOG' THEN 'catalog channel'
	WHEN coalesce("abundant"."sales_sales_channel","cheerful"."sales_sales_channel","yummy"."sales_sales_channel") = 'WEB' THEN 'web channel'
	ELSE null
	END as "channel_label",
    CASE
	WHEN coalesce("abundant"."sales_sales_channel","cheerful"."sales_sales_channel","yummy"."sales_sales_channel") = 'STORE' THEN ('store' || "cheerful"."sales_return_channel_dim_text_id")
	WHEN coalesce("abundant"."sales_sales_channel","cheerful"."sales_sales_channel","yummy"."sales_sales_channel") = 'CATALOG' THEN ('catalog_page' || "cheerful"."sales_return_channel_dim_text_id")
	WHEN coalesce("abundant"."sales_sales_channel","cheerful"."sales_sales_channel","yummy"."sales_sales_channel") = 'WEB' THEN ('web_site' || "cheerful"."sales_return_channel_dim_text_id")
	ELSE null
	END as "return_id_label"
FROM
    "yummy"
    INNER JOIN "abundant" on "yummy"."sales_item_id" = "abundant"."sales_item_id" AND "yummy"."sales_order_id" = "abundant"."sales_order_id" AND "yummy"."sales_sales_channel" = "abundant"."sales_sales_channel"
    INNER JOIN "memory"."date_dim" as "sales_return_date_date" on "abundant"."sales_return_date_id" = "sales_return_date_date"."D_DATE_SK"
    INNER JOIN "cheerful" on "yummy"."sales_return_channel_dim_id" = "cheerful"."sales_return_channel_dim_id" AND coalesce("yummy"."sales_sales_channel", "abundant"."sales_sales_channel") = "cheerful"."sales_sales_channel"
WHERE
    "cheerful"."sales_return_channel_dim_text_id" is not null and cast("sales_return_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-06'
),
divergent as (
SELECT
    "kaput"."channel_label" as "channel",
    "kaput"."sales_id_label" as "id",
    sum("kaput"."sales_ext_sales_price") as "sales_total_a",
    sum("kaput"."sales_net_profit") as "profit_only_a"
FROM
    "kaput"
GROUP BY
    ROLLUP (1, 2)),
concerned as (
SELECT
    "vacuous"."channel_label" as "channel",
    "vacuous"."return_id_label" as "id",
    sum(coalesce("vacuous"."sales_return_amount",0)) as "returns_total_b",
    sum(coalesce("vacuous"."sales_return_net_loss",0)) as "loss_only_b"
FROM
    "vacuous"
GROUP BY
    ROLLUP (1, 2))
SELECT
    coalesce("concerned"."channel","divergent"."channel") as "channel",
    coalesce("concerned"."id","divergent"."id") as "id",
    coalesce("divergent"."sales_total_a",0.0) as "sales_metric",
    coalesce("concerned"."returns_total_b",0.0) as "returns_metric",
    coalesce("divergent"."profit_only_a",0.0) - coalesce("concerned"."loss_only_b",0.0) as "profit_metric"
FROM
    "divergent"
    FULL JOIN "concerned" on "divergent"."channel" is not distinct from "concerned"."channel" AND "divergent"."id" is not distinct from "concerned"."id"
ORDER BY 
    coalesce("concerned"."channel","divergent"."channel") asc nulls first,
    coalesce("concerned"."id","divergent"."id") asc nulls first
LIMIT (100)
```

## v4 execution error

```
Traceback (most recent call last):
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 281, in run_one
    result.v4_exec_seconds, result.v4_rows = _time(lambda: _exec(v4_sql))
                                             ~~~~~^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 52, in _time
    value = fn()
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 281, in <lambda>
    result.v4_exec_seconds, result.v4_rows = _time(lambda: _exec(v4_sql))
                                                           ~~~~~^^^^^^^^
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 277, in _exec
    return execute(con, bound_sql, params or None)
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 197, in execute
    cursor = con.execute(sql, params) if params else con.execute(sql)
                                                     ~~~~~~~~~~~^^^^^
_duckdb.BinderException: Binder Error: column "s_channel" must appear in the GROUP BY clause or must be part of an aggregate function.
Either add it to the GROUP BY list, or use "ANY_VALUE(s_channel)" if the exact value of "s_channel" is not important.

LINE 261:     "premium"."s_channel" as "s_channel",
              ^
```
