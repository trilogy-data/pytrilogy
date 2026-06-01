# Query 77

**Status:** `mismatch`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (44 rows) |
| reference execution | OK (44 rows) |
| results identical | NO |

## Result comparison

v4 rows: 44 (44 distinct)
ref rows: 44 (44 distinct)
only in v4 (showing up to 5 of 43):
  1x  (None, None, Decimal('-748988848.34000000'), Decimal('90608859.22000000'), Decimal('4105104402.17000000'))
  1x  ('catalog channel', None, Decimal('-177444090.20000000'), Decimal('32133262.24000000'), Decimal('1599077337.60000000'))
  1x  ('catalog channel', 1, Decimal('-44361022.55000000'), Decimal('8033315.56000000'), Decimal('399769334.40000000'))
  1x  ('catalog channel', 2, Decimal('-44361022.55000000'), Decimal('8033315.56000000'), Decimal('399769334.40000000'))
  1x  ('catalog channel', 5, Decimal('-44361022.55000000'), Decimal('8033315.56000000'), Decimal('399769334.40000000'))
only in ref (showing up to 5 of 43):
  1x  (None, None, Decimal('-101829695.15000000'), Decimal('12403336.49000000'), Decimal('557191426.37000000'))
  1x  ('catalog channel', None, Decimal('-1262553.25000000'), Decimal('2008328.89000000'), Decimal('379488.65000000'))
  1x  ('catalog channel', 1, Decimal('-15825538.05000000'), Decimal('2008328.89000000'), Decimal('131267647.10000000'))
  1x  ('catalog channel', 2, Decimal('-14812307.25000000'), Decimal('2008328.89000000'), Decimal('139389660.25000000'))
  1x  ('catalog channel', 5, Decimal('-12460624.00000000'), Decimal('2008328.89000000'), Decimal('128732538.40000000'))

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 11103 | 280 | 28.28 ms |
| reference | 7495 | 166 | 22.95 ms |
| v4 / ref | 1.48x | 1.69x | 1.23x |

## Preql

```
import catalog_sales as cs;
import catalog_returns as cr;
import physical_sales as ss;
import web_sales as ws;
import web_returns as wr;

const period_start <- '2000-08-23'::date;
const period_end <- '2000-09-22'::date;

# Catalog returns aggregated per call_center (NULL group included via
# coalesce sentinel), then folded to a 1-row scalar so the catalog branch
# can broadcast: per-id sales/profit get multiplied by N_cr_groups, per-id
# returns/loss broadcast as the total. This replicates the reference's
# `FROM cs , cr` (CROSS JOIN with no key) catalog branch.
rowset cr_grouped <- where
    cr.date.date between period_start and period_end
select
    coalesce(cr.call_center.id, -1) as cr_cc_key,
    sum(cr.return_amount) as cr_returns_per_cc,
    sum(cr.net_loss) as cr_loss_per_cc,
;

rowset cr_totals <- select
    count(cr_grouped.cr_cc_key) as cr_n_groups,
    sum(cr_grouped.cr_returns_per_cc) as cr_total_returns,
    sum(cr_grouped.cr_loss_per_cc) as cr_total_loss,
;

# Store-side per-store_sk aggregations.
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

# Web-side per-web_page_sk aggregations.
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

# UNION the 3 branches via multi-select with align. The catalog branch is
# inlined (rather than wrapped in its own rowset) to keep the force_group
# flag uniform across all multi-select branches â€” wrapping the cross-join
# in a rowset hits "can only merge two datasources if the force_group
# flag is the same" during planning.
rowset l0_union <- where
    cs.date.date between period_start and period_end
select
    'catalog channel' as u_channel_c,
    cs.call_center.id as u_id_c,
    sum(cs.ext_sales_price) * cr_totals.cr_n_groups::numeric(15,2) as u_sales_c,
    cr_totals.cr_total_returns::numeric(15,2) as u_returns_c,
    sum(cs.net_profit) * cr_totals.cr_n_groups - cr_totals.cr_total_loss::numeric(15,2) as u_profit_c,
merge
select
    'store channel' as u_channel_s,
    ss_grouped.ss_store_id as u_id_s,
    ss_grouped.ss_sales::numeric(15,2) as u_sales_s,
    coalesce(sr_grouped.sr_returns, 0)::numeric(15,2) as u_returns_s,
    ss_grouped.ss_profit - coalesce(sr_grouped.sr_loss, 0)::numeric(15,2) as u_profit_s,
merge
select
    'web channel' as u_channel_w,
    ws_grouped.ws_wp_id as u_id_w,
    ws_grouped.ws_sales::numeric(15,2) as u_sales_w,
    coalesce(wr_grouped.wr_returns, 0)::numeric(15,2) as u_returns_w,
    ws_grouped.ws_profit - coalesce(wr_grouped.wr_loss, 0)::numeric(15,2) as u_profit_w,
align
    u_channel: u_channel_c, u_channel_s, u_channel_w
    and u_id: u_id_c, u_id_s, u_id_w
    and u_sales: u_sales_c, u_sales_s, u_sales_w
    and u_returns: u_returns_c, u_returns_s, u_returns_w
    and u_profit: u_profit_c, u_profit_s, u_profit_w
;

# Final ROLLUP(channel, id) over the unioned L0.
select
    l0_union.u_channel as channel,
    l0_union.u_id as id,
    sum(l0_union.u_sales) by rollup l0_union.u_channel, l0_union.u_id as sales,
    sum(l0_union.u_returns) by rollup l0_union.u_channel, l0_union.u_id as returns_,
    sum(l0_union.u_profit) by rollup l0_union.u_channel, l0_union.u_id as profit,
order by
    channel asc nulls first,
    id asc nulls first,
    returns_ desc
limit 100
;
```

## v4 generated SQL

```sql
WITH 
ceaseless as (
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
vast as (
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
puzzled as (
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
friendly as (
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
vacuous as (
SELECT
    "cs_catalog_sales"."CS_CALL_CENTER_SK" as "cs_call_center_id",
    sum("cs_catalog_sales"."CS_EXT_SALES_PRICE") as "_virt_agg_sum_6520591768854391",
    sum("cs_catalog_sales"."CS_NET_PROFIT") as "_virt_agg_sum_6226990944561419"
FROM
    "memory"."catalog_sales" as "cs_catalog_sales"
    INNER JOIN "memory"."date_dim" as "cs_date_date" on "cs_catalog_sales"."CS_SOLD_DATE_SK" = "cs_date_date"."D_DATE_SK"
WHERE
    cast("cs_date_date"."D_DATE" as date) BETWEEN :period_start AND :period_end

GROUP BY
    1),
wakeful as (
SELECT
    coalesce("cr_catalog_returns"."CR_CALL_CENTER_SK",-1) as "_cr_grouped_cr_cc_key",
    sum("cr_catalog_returns"."CR_NET_LOSS") as "_cr_grouped_cr_loss_per_cc",
    sum("cr_catalog_returns"."CR_RETURN_AMOUNT") as "_cr_grouped_cr_returns_per_cc"
FROM
    "memory"."catalog_returns" as "cr_catalog_returns"
    INNER JOIN "memory"."date_dim" as "cr_date_date" on "cr_catalog_returns"."CR_RETURNED_DATE_SK" = "cr_date_date"."D_DATE_SK"
WHERE
    cast("cr_date_date"."D_DATE" as date) BETWEEN :period_start AND :period_end

GROUP BY
    1),
sweltering as (
SELECT
    :_l0_union_u_channel_c as "_l0_union_u_channel_c",
    :_l0_union_u_channel_s as "_l0_union_u_channel_s",
    :_l0_union_u_channel_w as "_l0_union_u_channel_w"
),
cloudy as (
SELECT
    "ceaseless"."_ws_grouped_ws_profit" as "_ws_grouped_ws_profit",
    "ceaseless"."_ws_grouped_ws_sales" as "_ws_grouped_ws_sales",
    "ceaseless"."ws_web_page_id" as "_ws_grouped_ws_wp_id"
FROM
    "ceaseless"),
elated as (
SELECT
    "vast"."_wr_grouped_wr_loss" as "_wr_grouped_wr_loss",
    "vast"."_wr_grouped_wr_returns" as "_wr_grouped_wr_returns",
    "vast"."wr_web_page_id" as "_wr_grouped_wr_wp_id"
FROM
    "vast"),
rambunctious as (
SELECT
    "puzzled"."_sr_grouped_sr_loss" as "_sr_grouped_sr_loss",
    "puzzled"."_sr_grouped_sr_returns" as "_sr_grouped_sr_returns",
    "puzzled"."ss_return_store_id" as "_sr_grouped_sr_store_id"
FROM
    "puzzled"),
divergent as (
SELECT
    "friendly"."_ss_grouped_ss_profit" as "_ss_grouped_ss_profit",
    "friendly"."_ss_grouped_ss_sales" as "_ss_grouped_ss_sales",
    "friendly"."ss_store_id" as "_ss_grouped_ss_store_id"
FROM
    "friendly"),
abhorrent as (
SELECT
    "vacuous"."cs_call_center_id" as "_l0_union_u_id_c"
FROM
    "vacuous"),
cooperative as (
SELECT
    count("wakeful"."_cr_grouped_cr_cc_key") as "_cr_totals_cr_n_groups",
    sum("wakeful"."_cr_grouped_cr_loss_per_cc") as "_cr_totals_cr_total_loss",
    sum("wakeful"."_cr_grouped_cr_returns_per_cc") as "_cr_totals_cr_total_returns"
FROM
    "wakeful"),
gullible as (
SELECT
    "cloudy"."_ws_grouped_ws_profit" as "ws_grouped_ws_profit",
    "cloudy"."_ws_grouped_ws_sales" as "ws_grouped_ws_sales",
    "cloudy"."_ws_grouped_ws_wp_id" as "ws_grouped_ws_wp_id"
FROM
    "cloudy"),
wary as (
SELECT
    "elated"."_wr_grouped_wr_loss" as "wr_grouped_wr_loss",
    "elated"."_wr_grouped_wr_returns" as "wr_grouped_wr_returns",
    "elated"."_wr_grouped_wr_wp_id" as "wr_grouped_wr_wp_id"
FROM
    "elated"),
puffy as (
SELECT
    "rambunctious"."_sr_grouped_sr_loss" as "sr_grouped_sr_loss",
    "rambunctious"."_sr_grouped_sr_returns" as "sr_grouped_sr_returns",
    "rambunctious"."_sr_grouped_sr_store_id" as "sr_grouped_sr_store_id"
FROM
    "rambunctious"),
busy as (
SELECT
    "divergent"."_ss_grouped_ss_profit" as "ss_grouped_ss_profit",
    "divergent"."_ss_grouped_ss_sales" as "ss_grouped_ss_sales",
    "divergent"."_ss_grouped_ss_store_id" as "ss_grouped_ss_store_id"
FROM
    "divergent"),
abundant as (
SELECT
    "cooperative"."_cr_totals_cr_n_groups" as "cr_totals_cr_n_groups",
    "cooperative"."_cr_totals_cr_total_loss" as "cr_totals_cr_total_loss",
    "cooperative"."_cr_totals_cr_total_returns" as "cr_totals_cr_total_returns"
FROM
    "cooperative"),
quick as (
SELECT
    "gullible"."ws_grouped_ws_wp_id" as "_l0_union_u_id_w",
    cast("gullible"."ws_grouped_ws_sales" as numeric(15,2)) as "_l0_union_u_sales_w"
FROM
    "gullible"),
flashy as (
SELECT
    "gullible"."ws_grouped_ws_profit" - cast(coalesce("wary"."wr_grouped_wr_loss",0) as numeric(15,2)) as "_l0_union_u_profit_w",
    "gullible"."ws_grouped_ws_wp_id" as "ws_grouped_ws_wp_id"
FROM
    "gullible"
    INNER JOIN "wary" on "gullible"."ws_grouped_ws_wp_id" = "wary"."wr_grouped_wr_wp_id"),
bewildered as (
SELECT
    cast(coalesce("wary"."wr_grouped_wr_returns",0) as numeric(15,2)) as "_l0_union_u_returns_w"
FROM
    "wary"),
yellow as (
SELECT
    cast(coalesce("puffy"."sr_grouped_sr_returns",0) as numeric(15,2)) as "_l0_union_u_returns_s"
FROM
    "puffy"),
hard as (
SELECT
    "busy"."ss_grouped_ss_profit" - cast(coalesce("puffy"."sr_grouped_sr_loss",0) as numeric(15,2)) as "_l0_union_u_profit_s",
    "busy"."ss_grouped_ss_store_id" as "ss_grouped_ss_store_id"
FROM
    "puffy"
    INNER JOIN "busy" on "puffy"."sr_grouped_sr_store_id" = "busy"."ss_grouped_ss_store_id"),
charming as (
SELECT
    "busy"."ss_grouped_ss_store_id" as "_l0_union_u_id_s",
    cast("busy"."ss_grouped_ss_sales" as numeric(15,2)) as "_l0_union_u_sales_s"
FROM
    "busy"),
young as (
SELECT
    "vacuous"."_virt_agg_sum_6520591768854391" * cast("abundant"."cr_totals_cr_n_groups" as numeric(15,2)) as "_l0_union_u_sales_c",
    ( "vacuous"."_virt_agg_sum_6226990944561419" * "abundant"."cr_totals_cr_n_groups" ) - cast("abundant"."cr_totals_cr_total_loss" as numeric(15,2)) as "_l0_union_u_profit_c"
FROM
    "abundant"
    FULL JOIN "vacuous" on 1=1),
uneven as (
SELECT
    cast("abundant"."cr_totals_cr_total_returns" as numeric(15,2)) as "_l0_union_u_returns_c"
FROM
    "abundant"),
nondescript as (
SELECT
    "bewildered"."_l0_union_u_returns_w" as "u_returns",
    "flashy"."_l0_union_u_profit_w" as "u_profit",
    "quick"."_l0_union_u_id_w" as "u_id",
    "quick"."_l0_union_u_sales_w" as "u_sales",
    "sweltering"."_l0_union_u_channel_w" as "u_channel"
FROM
    "quick"
    INNER JOIN "flashy" on "quick"."_l0_union_u_id_w" = "flashy"."ws_grouped_ws_wp_id"
    FULL JOIN "sweltering" on 1=1
    FULL JOIN "bewildered" on 1=1),
resonant as (
SELECT
    "charming"."_l0_union_u_id_s" as "u_id",
    "charming"."_l0_union_u_sales_s" as "u_sales",
    "hard"."_l0_union_u_profit_s" as "u_profit",
    "sweltering"."_l0_union_u_channel_s" as "u_channel",
    "yellow"."_l0_union_u_returns_s" as "u_returns"
FROM
    "charming"
    INNER JOIN "hard" on "charming"."_l0_union_u_id_s" = "hard"."ss_grouped_ss_store_id"
    FULL JOIN "sweltering" on 1=1
    FULL JOIN "yellow" on 1=1),
late as (
SELECT
    "abhorrent"."_l0_union_u_id_c" as "u_id",
    "sweltering"."_l0_union_u_channel_c" as "u_channel",
    "uneven"."_l0_union_u_returns_c" as "u_returns",
    "young"."_l0_union_u_profit_c" as "u_profit",
    "young"."_l0_union_u_sales_c" as "u_sales"
FROM
    "abhorrent"
    FULL JOIN "sweltering" on 1=1
    FULL JOIN "young" on 1=1
    FULL JOIN "uneven" on 1=1),
round as (
SELECT
    coalesce("late"."u_channel","nondescript"."u_channel","resonant"."u_channel") as "l0_union_u_channel",
    coalesce("late"."u_id","nondescript"."u_id","resonant"."u_id") as "l0_union_u_id",
    coalesce("late"."u_profit","nondescript"."u_profit","resonant"."u_profit") as "l0_union_u_profit",
    coalesce("late"."u_returns","nondescript"."u_returns","resonant"."u_returns") as "l0_union_u_returns",
    coalesce("late"."u_sales","nondescript"."u_sales","resonant"."u_sales") as "l0_union_u_sales"
FROM
    "late"
    FULL JOIN "resonant" on "late"."u_channel" is not distinct from "resonant"."u_channel" AND "late"."u_id" is not distinct from "resonant"."u_id" AND "late"."u_profit" is not distinct from "resonant"."u_profit" AND "late"."u_returns" is not distinct from "resonant"."u_returns" AND "late"."u_sales" is not distinct from "resonant"."u_sales"
    FULL JOIN "nondescript" on coalesce("late"."u_channel", "resonant"."u_channel") = "nondescript"."u_channel" AND coalesce("late"."u_id", "resonant"."u_id") = "nondescript"."u_id" AND coalesce("late"."u_profit", "resonant"."u_profit") = "nondescript"."u_profit" AND coalesce("late"."u_returns", "resonant"."u_returns") = "nondescript"."u_returns" AND coalesce("late"."u_sales", "resonant"."u_sales") = "nondescript"."u_sales"),
spiritual as (
SELECT
    "round"."l0_union_u_channel" as "l0_union_u_channel",
    "round"."l0_union_u_id" as "l0_union_u_id",
    sum("round"."l0_union_u_profit") as "profit",
    sum("round"."l0_union_u_returns") as "returns_",
    sum("round"."l0_union_u_sales") as "sales"
FROM
    "round"
GROUP BY
    ROLLUP (1, 2))
SELECT
    "spiritual"."l0_union_u_channel" as "channel",
    "spiritual"."l0_union_u_id" as "id",
    "spiritual"."profit" as "profit",
    "spiritual"."returns_" as "returns_",
    "spiritual"."sales" as "sales"
FROM
    "spiritual"
ORDER BY 
    "channel" asc nulls first,
    "id" asc nulls first,
    "spiritual"."returns_" desc
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
wakeful as (
SELECT
    coalesce("cr_catalog_returns"."CR_CALL_CENTER_SK",-1) as "cr_grouped_cr_cc_key",
    sum("cr_catalog_returns"."CR_NET_LOSS") as "cr_grouped_cr_loss_per_cc",
    sum("cr_catalog_returns"."CR_RETURN_AMOUNT") as "cr_grouped_cr_returns_per_cc"
FROM
    "memory"."catalog_returns" as "cr_catalog_returns"
    INNER JOIN "memory"."date_dim" as "cr_date_date" on "cr_catalog_returns"."CR_RETURNED_DATE_SK" = "cr_date_date"."D_DATE_SK"
WHERE
    cast("cr_date_date"."D_DATE" as date) BETWEEN :period_start AND :period_end

GROUP BY
    1),
busy as (
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
puzzled as (
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
    1),
sparkling as (
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
macho as (
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
    1),
abundant as (
SELECT
    "cs_catalog_sales"."CS_CALL_CENTER_SK" as "cs_call_center_id",
    sum("cs_catalog_sales"."CS_EXT_SALES_PRICE") as "_virt_agg_sum_6520591768854391",
    sum("cs_catalog_sales"."CS_NET_PROFIT") as "_virt_agg_sum_6226990944561419"
FROM
    "memory"."catalog_sales" as "cs_catalog_sales"
    INNER JOIN "memory"."date_dim" as "cs_date_date" on "cs_catalog_sales"."CS_SOLD_DATE_SK" = "cs_date_date"."D_DATE_SK"
WHERE
    cast("cs_date_date"."D_DATE" as date) BETWEEN :period_start AND :period_end

GROUP BY
    1),
juicy as (
SELECT
    cast(sum("wakeful"."cr_grouped_cr_returns_per_cc") as numeric(15,2)) as "_l0_union_u_returns_c"
FROM
    "wakeful"),
thoughtful as (
SELECT
    count("wakeful"."cr_grouped_cr_cc_key") as "cr_totals_cr_n_groups",
    sum("wakeful"."cr_grouped_cr_loss_per_cc") as "cr_totals_cr_total_loss"
FROM
    "wakeful"),
rambunctious as (
SELECT
    "puzzled"."ws_grouped_ws_profit" - cast(coalesce("busy"."wr_grouped_wr_loss",0) as numeric(15,2)) as "u_profit",
    "puzzled"."ws_grouped_ws_wp_id" as "u_id",
    :_l0_union_u_channel_w as "u_channel",
    cast("puzzled"."ws_grouped_ws_sales" as numeric(15,2)) as "u_sales",
    cast(coalesce("busy"."wr_grouped_wr_returns",0) as numeric(15,2)) as "u_returns"
FROM
    "puzzled"
    INNER JOIN "busy" on "puzzled"."ws_grouped_ws_wp_id" = "busy"."wr_grouped_wr_wp_id"),
friendly as (
SELECT
    "sparkling"."ss_grouped_ss_profit" - cast(coalesce("macho"."sr_grouped_sr_loss",0) as numeric(15,2)) as "u_profit",
    "sparkling"."ss_grouped_ss_store_id" as "u_id",
    :_l0_union_u_channel_s as "u_channel",
    cast("sparkling"."ss_grouped_ss_sales" as numeric(15,2)) as "u_sales",
    cast(coalesce("macho"."sr_grouped_sr_returns",0) as numeric(15,2)) as "u_returns"
FROM
    "macho"
    INNER JOIN "sparkling" on "macho"."sr_grouped_sr_store_id" = "sparkling"."ss_grouped_ss_store_id"),
yummy as (
SELECT
    "abundant"."_virt_agg_sum_6520591768854391" * cast("thoughtful"."cr_totals_cr_n_groups" as numeric(15,2)) as "_l0_union_u_sales_c",
    "abundant"."cs_call_center_id" as "_l0_union_u_id_c",
    ( "abundant"."_virt_agg_sum_6226990944561419" * "thoughtful"."cr_totals_cr_n_groups" ) - cast("thoughtful"."cr_totals_cr_total_loss" as numeric(15,2)) as "_l0_union_u_profit_c",
    :_l0_union_u_channel_c as "_l0_union_u_channel_c"
FROM
    "thoughtful"
    FULL JOIN "abundant" on 1=1),
vacuous as (
SELECT
    "juicy"."_l0_union_u_returns_c" as "u_returns",
    "yummy"."_l0_union_u_channel_c" as "u_channel",
    "yummy"."_l0_union_u_id_c" as "u_id",
    "yummy"."_l0_union_u_profit_c" as "u_profit",
    "yummy"."_l0_union_u_sales_c" as "u_sales"
FROM
    "yummy"
    FULL JOIN "juicy" on 1=1),
puffy as (
SELECT
    coalesce("friendly"."u_channel","rambunctious"."u_channel","vacuous"."u_channel") as "u_channel",
    coalesce("friendly"."u_id","rambunctious"."u_id","vacuous"."u_id") as "u_id",
    coalesce("friendly"."u_profit","rambunctious"."u_profit","vacuous"."u_profit") as "u_profit",
    coalesce("friendly"."u_returns","rambunctious"."u_returns","vacuous"."u_returns") as "u_returns",
    coalesce("friendly"."u_sales","rambunctious"."u_sales","vacuous"."u_sales") as "u_sales"
FROM
    "vacuous"
    FULL JOIN "friendly" on "vacuous"."u_channel" is not distinct from "friendly"."u_channel" AND "vacuous"."u_id" is not distinct from "friendly"."u_id" AND "vacuous"."u_profit" is not distinct from "friendly"."u_profit" AND "vacuous"."u_returns" is not distinct from "friendly"."u_returns" AND "vacuous"."u_sales" is not distinct from "friendly"."u_sales"
    FULL JOIN "rambunctious" on coalesce("vacuous"."u_channel", "friendly"."u_channel") = "rambunctious"."u_channel" AND coalesce("vacuous"."u_id", "friendly"."u_id") = "rambunctious"."u_id" AND coalesce("vacuous"."u_profit", "friendly"."u_profit") = "rambunctious"."u_profit" AND coalesce("vacuous"."u_returns", "friendly"."u_returns") = "rambunctious"."u_returns" AND coalesce("vacuous"."u_sales", "friendly"."u_sales") = "rambunctious"."u_sales"),
hard as (
SELECT
    "puffy"."u_channel" as "l0_union_u_channel",
    "puffy"."u_id" as "l0_union_u_id",
    "puffy"."u_profit" as "l0_union_u_profit",
    "puffy"."u_returns" as "l0_union_u_returns",
    "puffy"."u_sales" as "l0_union_u_sales"
FROM
    "puffy"
    FULL JOIN "juicy" on 1=1)
SELECT
    "hard"."l0_union_u_channel" as "channel",
    "hard"."l0_union_u_id" as "id",
    sum("hard"."l0_union_u_sales") as "sales",
    sum("hard"."l0_union_u_returns") as "returns_",
    sum("hard"."l0_union_u_profit") as "profit"
FROM
    "hard"
GROUP BY
    ROLLUP (1, 2)
ORDER BY 
    "channel" asc nulls first,
    "id" asc nulls first,
    "returns_" desc
LIMIT (100)
```
