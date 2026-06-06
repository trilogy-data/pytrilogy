# Query -3

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (30 rows) |
| reference execution | OK (30 rows) |
| results identical | YES |

## Result comparison

v4 rows: 30 (30 distinct)
ref rows: 30 (30 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 2818 | 79 | 4.52 ms |
| reference | 1572 | 38 | 3.85 ms |
| v4 / ref | 1.79x | 2.08x | 1.17x |

## Preql

```
# q77 WEB branch, extracted as a standalone select (negative test id -3).
# Same merge-join shape as the store branch but keyed on web_page; included so
# the per-branch v3/v4 parity audit covers all three multiselect arms.
import web_sales as ws;
import web_returns as wr;

const period_start <- '2000-08-23'::date;
const period_end <- '2000-09-22'::date;

rowset ws_grouped <- where
    ws.date.date between period_start and period_end
select
    ws.web_page.id as ws_wp_id,
    sum(ws.ext_sales_price) as ws_sales,
    sum(ws.net_profit) as ws_profit,
;

rowset wr_grouped <- where
    wr.return_date.date between period_start and period_end
select
    wr.web_page.id as wr_wp_id,
    sum(wr.return_amount) as wr_returns,
    sum(wr.net_loss) as wr_loss,
;

merge wr_grouped.wr_wp_id into ws_grouped.ws_wp_id;

select
    'web channel' as u_channel_w,
    ws_grouped.ws_wp_id as u_id_w,
    ws_grouped.ws_sales::numeric(15,2) as u_sales_w,
    coalesce(wr_grouped.wr_returns, 0)::numeric(15,2) as u_returns_w,
    ws_grouped.ws_profit - coalesce(wr_grouped.wr_loss, 0)::numeric(15,2) as u_profit_w,
order by u_id_w asc nulls first
;
```

## v4 generated SQL

```sql
WITH 
juicy as (
SELECT
    "ws_web_sales"."WS_WEB_PAGE_SK" as "ws_web_page_id",
    sum("ws_web_sales"."WS_EXT_SALES_PRICE") as "_ws_grouped_ws_sales",
    sum("ws_web_sales"."WS_NET_PROFIT") as "_ws_grouped_ws_profit"
FROM
    "memory"."web_sales" as "ws_web_sales"
    INNER JOIN "memory"."date_dim" as "ws_date_date" on "ws_web_sales"."WS_SOLD_DATE_SK" = "ws_date_date"."D_DATE_SK"
WHERE
    cast("ws_date_date"."D_DATE" as date) BETWEEN :period_start AND :period_end

GROUP BY
    1),
cheerful as (
SELECT
    "wr_web_returns"."WR_WEB_PAGE_SK" as "wr_web_page_id",
    sum("wr_web_returns"."WR_NET_LOSS") as "_wr_grouped_wr_loss",
    sum("wr_web_returns"."WR_RETURN_AMT") as "_wr_grouped_wr_returns"
FROM
    "memory"."web_returns" as "wr_web_returns"
    INNER JOIN "memory"."date_dim" as "wr_return_date_date" on "wr_web_returns"."WR_RETURNED_DATE_SK" = "wr_return_date_date"."D_DATE_SK"
WHERE
    cast("wr_return_date_date"."D_DATE" as date) BETWEEN :period_start AND :period_end

GROUP BY
    1),
quizzical as (
SELECT
    :u_channel_w as "u_channel_w"
),
young as (
SELECT
    "juicy"."_ws_grouped_ws_profit" as "_ws_grouped_ws_profit",
    "juicy"."_ws_grouped_ws_sales" as "_ws_grouped_ws_sales",
    "juicy"."ws_web_page_id" as "_ws_grouped_ws_wp_id"
FROM
    "juicy"),
questionable as (
SELECT
    "cheerful"."_wr_grouped_wr_loss" as "_wr_grouped_wr_loss",
    "cheerful"."_wr_grouped_wr_returns" as "_wr_grouped_wr_returns",
    "cheerful"."wr_web_page_id" as "_wr_grouped_wr_wp_id"
FROM
    "cheerful"),
sparkling as (
SELECT
    "young"."_ws_grouped_ws_profit" as "ws_grouped_ws_profit",
    "young"."_ws_grouped_ws_sales" as "ws_grouped_ws_sales",
    "young"."_ws_grouped_ws_wp_id" as "ws_grouped_ws_wp_id"
FROM
    "young"),
abundant as (
SELECT
    "questionable"."_wr_grouped_wr_loss" as "wr_grouped_wr_loss",
    "questionable"."_wr_grouped_wr_returns" as "wr_grouped_wr_returns",
    "questionable"."_wr_grouped_wr_wp_id" as "wr_grouped_wr_wp_id"
FROM
    "questionable"),
abhorrent as (
SELECT
    "sparkling"."ws_grouped_ws_profit" - cast(coalesce("abundant"."wr_grouped_wr_loss",0) as numeric(15,2)) as "u_profit_w",
    "sparkling"."ws_grouped_ws_wp_id" as "u_id_w",
    cast("sparkling"."ws_grouped_ws_sales" as numeric(15,2)) as "u_sales_w",
    cast(coalesce("abundant"."wr_grouped_wr_returns",0) as numeric(15,2)) as "u_returns_w"
FROM
    "sparkling"
    INNER JOIN "abundant" on "sparkling"."ws_grouped_ws_wp_id" = "abundant"."wr_grouped_wr_wp_id")
SELECT
    "quizzical"."u_channel_w" as "u_channel_w",
    "abhorrent"."u_id_w" as "u_id_w",
    "abhorrent"."u_sales_w" as "u_sales_w",
    "abhorrent"."u_returns_w" as "u_returns_w",
    "abhorrent"."u_profit_w" as "u_profit_w"
FROM
    "quizzical"
    FULL JOIN "abhorrent" on 1=1
ORDER BY 
    "abhorrent"."u_id_w" asc nulls first
```

## Reference SQL (zquery log)

```sql
WITH 
wakeful as (
SELECT
    "wr_web_returns"."WR_WEB_PAGE_SK" as "wr_grouped_wr_wp_id",
    sum("wr_web_returns"."WR_NET_LOSS") as "wr_grouped_wr_loss",
    sum("wr_web_returns"."WR_RETURN_AMT") as "wr_grouped_wr_returns"
FROM
    "memory"."web_returns" as "wr_web_returns"
    INNER JOIN "memory"."date_dim" as "wr_return_date_date" on "wr_web_returns"."WR_RETURNED_DATE_SK" = "wr_return_date_date"."D_DATE_SK"
WHERE
    cast("wr_return_date_date"."D_DATE" as date) BETWEEN :period_start AND :period_end

GROUP BY
    1),
questionable as (
SELECT
    "ws_web_sales"."WS_WEB_PAGE_SK" as "ws_grouped_ws_wp_id",
    sum("ws_web_sales"."WS_EXT_SALES_PRICE") as "ws_grouped_ws_sales",
    sum("ws_web_sales"."WS_NET_PROFIT") as "ws_grouped_ws_profit"
FROM
    "memory"."web_sales" as "ws_web_sales"
    INNER JOIN "memory"."date_dim" as "ws_date_date" on "ws_web_sales"."WS_SOLD_DATE_SK" = "ws_date_date"."D_DATE_SK"
WHERE
    cast("ws_date_date"."D_DATE" as date) BETWEEN :period_start AND :period_end

GROUP BY
    1)
SELECT
    :u_channel_w as "u_channel_w",
    "questionable"."ws_grouped_ws_wp_id" as "u_id_w",
    cast("questionable"."ws_grouped_ws_sales" as numeric(15,2)) as "u_sales_w",
    cast(coalesce("wakeful"."wr_grouped_wr_returns",0) as numeric(15,2)) as "u_returns_w",
    "questionable"."ws_grouped_ws_profit" - cast(coalesce("wakeful"."wr_grouped_wr_loss",0) as numeric(15,2)) as "u_profit_w"
FROM
    "questionable"
    INNER JOIN "wakeful" on "questionable"."ws_grouped_ws_wp_id" = "wakeful"."wr_grouped_wr_wp_id"
ORDER BY 
    "u_id_w" asc nulls first
```
