# Query -2

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (1 rows) |
| reference execution | OK (1 rows) |
| results identical | YES |

## Result comparison

v4 rows: 1 (1 distinct)
ref rows: 1 (1 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 2730 | 67 | 6.02 ms |
| reference | 1805 | 39 | 4.95 ms |
| v4 / ref | 1.51x | 1.72x | 1.22x |

## Preql

```
# q77 STORE branch, extracted as a standalone select (negative test id -2).
# Two per-store aggregates joined by a rowset `merge` (sales keyed by store,
# returns keyed by return_store). Checks v4 parity on the merge-join shape.
import physical_sales as ss;

const period_start <- '2000-08-23'::date;
const period_end <- '2000-09-22'::date;

rowset ss_grouped <- where
    ss.date.date between period_start and period_end
select
    ss.store.id as ss_store_id,
    sum(ss.ext_sales_price) as ss_sales,
    sum(ss.net_profit) as ss_profit,
;

rowset sr_grouped <- where
    ss.return_date.date between period_start and period_end
select
    ss.return_store.id as sr_store_id,
    sum(ss.return_amount) as sr_returns,
    sum(ss.return_net_loss) as sr_loss,
;

merge sr_grouped.sr_store_id into ss_grouped.ss_store_id;

select
    'store channel' as u_channel_s,
    ss_grouped.ss_store_id as u_id_s,
    ss_grouped.ss_sales::numeric(15,2) as u_sales_s,
    coalesce(sr_grouped.sr_returns, 0)::numeric(15,2) as u_returns_s,
    ss_grouped.ss_profit - coalesce(sr_grouped.sr_loss, 0)::numeric(15,2) as u_profit_s,
order by u_id_s asc nulls first
;
```

## v4 generated SQL

```sql
WITH 
uneven as (
SELECT
    "ss_store_returns"."SR_STORE_SK" as "ss_return_store_id",
    sum("ss_store_returns"."SR_NET_LOSS") as "_sr_grouped_sr_loss",
    sum("ss_store_returns"."SR_RETURN_AMT") as "_sr_grouped_sr_returns"
FROM
    "memory"."store_sales" as "ss_store_sales"
    INNER JOIN "memory"."store_returns" as "ss_store_returns" on "ss_store_sales"."SS_ITEM_SK" = "ss_store_returns"."SR_ITEM_SK" AND "ss_store_sales"."SS_TICKET_NUMBER" = "ss_store_returns"."SR_TICKET_NUMBER"
    INNER JOIN "memory"."date_dim" as "ss_return_date_date" on "ss_store_returns"."SR_RETURNED_DATE_SK" = "ss_return_date_date"."D_DATE_SK"
WHERE
    cast("ss_return_date_date"."D_DATE" as date) BETWEEN :period_start AND :period_end

GROUP BY
    1),
wakeful as (
SELECT
    "ss_store_sales"."SS_STORE_SK" as "ss_store_id",
    sum("ss_store_sales"."SS_EXT_SALES_PRICE") as "_ss_grouped_ss_sales",
    sum("ss_store_sales"."SS_NET_PROFIT") as "_ss_grouped_ss_profit"
FROM
    "memory"."store_sales" as "ss_store_sales"
    INNER JOIN "memory"."date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
WHERE
    cast("ss_date_date"."D_DATE" as date) BETWEEN :period_start AND :period_end

GROUP BY
    1),
juicy as (
SELECT
    "uneven"."_sr_grouped_sr_loss" as "_sr_grouped_sr_loss",
    "uneven"."_sr_grouped_sr_returns" as "_sr_grouped_sr_returns",
    "uneven"."ss_return_store_id" as "_sr_grouped_sr_store_id"
FROM
    "uneven"),
thoughtful as (
SELECT
    "wakeful"."_ss_grouped_ss_profit" as "_ss_grouped_ss_profit",
    "wakeful"."_ss_grouped_ss_sales" as "_ss_grouped_ss_sales",
    "wakeful"."ss_store_id" as "_ss_grouped_ss_store_id"
FROM
    "wakeful"),
vacuous as (
SELECT
    "juicy"."_sr_grouped_sr_loss" as "sr_grouped_sr_loss",
    "juicy"."_sr_grouped_sr_returns" as "sr_grouped_sr_returns",
    "juicy"."_sr_grouped_sr_store_id" as "sr_grouped_sr_store_id"
FROM
    "juicy"),
cooperative as (
SELECT
    "thoughtful"."_ss_grouped_ss_profit" as "ss_grouped_ss_profit",
    "thoughtful"."_ss_grouped_ss_sales" as "ss_grouped_ss_sales",
    "thoughtful"."_ss_grouped_ss_store_id" as "ss_grouped_ss_store_id"
FROM
    "thoughtful")
SELECT
    :u_channel_s as "u_channel_s",
    "cooperative"."ss_grouped_ss_store_id" as "u_id_s",
    cast("cooperative"."ss_grouped_ss_sales" as numeric(15,2)) as "u_sales_s",
    cast(coalesce("vacuous"."sr_grouped_sr_returns",0) as numeric(15,2)) as "u_returns_s",
    "cooperative"."ss_grouped_ss_profit" - cast(coalesce("vacuous"."sr_grouped_sr_loss",0) as numeric(15,2)) as "u_profit_s"
FROM
    "vacuous"
    INNER JOIN "cooperative" on "vacuous"."sr_grouped_sr_store_id" = "cooperative"."ss_grouped_ss_store_id"
ORDER BY 
    "u_id_s" asc nulls first
```

## Reference SQL (zquery log)

```sql
WITH 
wakeful as (
SELECT
    "ss_store_sales"."SS_STORE_SK" as "ss_grouped_ss_store_id",
    sum("ss_store_sales"."SS_EXT_SALES_PRICE") as "ss_grouped_ss_sales",
    sum("ss_store_sales"."SS_NET_PROFIT") as "ss_grouped_ss_profit"
FROM
    "memory"."store_sales" as "ss_store_sales"
    INNER JOIN "memory"."date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
WHERE
    cast("ss_date_date"."D_DATE" as date) BETWEEN :period_start AND :period_end

GROUP BY
    1),
questionable as (
SELECT
    "ss_store_returns"."SR_STORE_SK" as "sr_grouped_sr_store_id",
    sum("ss_store_returns"."SR_NET_LOSS") as "sr_grouped_sr_loss",
    sum("ss_store_returns"."SR_RETURN_AMT") as "sr_grouped_sr_returns"
FROM
    "memory"."store_sales" as "ss_store_sales"
    INNER JOIN "memory"."store_returns" as "ss_store_returns" on "ss_store_sales"."SS_ITEM_SK" = "ss_store_returns"."SR_ITEM_SK" AND "ss_store_sales"."SS_TICKET_NUMBER" = "ss_store_returns"."SR_TICKET_NUMBER"
    INNER JOIN "memory"."date_dim" as "ss_return_date_date" on "ss_store_returns"."SR_RETURNED_DATE_SK" = "ss_return_date_date"."D_DATE_SK"
WHERE
    cast("ss_return_date_date"."D_DATE" as date) BETWEEN :period_start AND :period_end

GROUP BY
    1)
SELECT
    :u_channel_s as "u_channel_s",
    "wakeful"."ss_grouped_ss_store_id" as "u_id_s",
    cast("wakeful"."ss_grouped_ss_sales" as numeric(15,2)) as "u_sales_s",
    cast(coalesce("questionable"."sr_grouped_sr_returns",0) as numeric(15,2)) as "u_returns_s",
    "wakeful"."ss_grouped_ss_profit" - cast(coalesce("questionable"."sr_grouped_sr_loss",0) as numeric(15,2)) as "u_profit_s"
FROM
    "questionable"
    INNER JOIN "wakeful" on "questionable"."sr_grouped_sr_store_id" = "wakeful"."ss_grouped_ss_store_id"
ORDER BY 
    "u_id_s" asc nulls first
```
