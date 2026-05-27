# Query 77

**Status:** `gen_fail`

| Stage | Result |
| --- | --- |
| v4 SQL generation | FAILED |
| reference execution | FAILED |

## Result comparison

_at least one side did not produce rows._

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 0 | 0 | — |
| reference | 7829 | 181 | — |

## Preql

```
import catalog_sales as cs;
import catalog_returns as cr;
import store_sales as ss;
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

_v4 did not produce SQL._

## Reference SQL (zquery log)

```sql
WITH 
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
uneven as (
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
rambunctious as (
SELECT
    "puzzled"."ws_grouped_ws_profit" - cast(coalesce("busy"."wr_grouped_wr_loss",0) as numeric(15,2)) as "u_profit",
    "puzzled"."ws_grouped_ws_wp_id" as "u_id",
    :_l0_union_u_channel_w as "u_channel",
    cast("puzzled"."ws_grouped_ws_sales" as numeric(15,2)) as "u_sales",
    cast(coalesce("busy"."wr_grouped_wr_returns",0) as numeric(15,2)) as "u_returns"
FROM
    "busy"
    INNER JOIN "puzzled" on "busy"."wr_grouped_wr_wp_id" = "puzzled"."ws_grouped_ws_wp_id"
GROUP BY
    1,
    2,
    3,
    4,
    5),
friendly as (
SELECT
    "sparkling"."ss_grouped_ss_profit" - cast(coalesce("macho"."sr_grouped_sr_loss",0) as numeric(15,2)) as "u_profit",
    "sparkling"."ss_grouped_ss_store_id" as "u_id",
    :_l0_union_u_channel_s as "u_channel",
    cast("sparkling"."ss_grouped_ss_sales" as numeric(15,2)) as "u_sales",
    cast(coalesce("macho"."sr_grouped_sr_returns",0) as numeric(15,2)) as "u_returns"
FROM
    "macho"
    INNER JOIN "sparkling" on "macho"."sr_grouped_sr_store_id" = "sparkling"."ss_grouped_ss_store_id"
GROUP BY
    1,
    2,
    3,
    4,
    5),
thoughtful as (
SELECT
    count("wakeful"."cr_grouped_cr_cc_key") as "_cr_totals_cr_n_groups",
    sum("wakeful"."cr_grouped_cr_loss_per_cc") as "_cr_totals_cr_total_loss",
    sum("wakeful"."cr_grouped_cr_returns_per_cc") as "_cr_totals_cr_total_returns"
FROM
    "wakeful"),
cooperative as (
SELECT
    "thoughtful"."_cr_totals_cr_n_groups" as "cr_totals_cr_n_groups",
    "thoughtful"."_cr_totals_cr_total_loss" as "cr_totals_cr_total_loss",
    cast("thoughtful"."_cr_totals_cr_total_returns" as numeric(15,2)) as "_l0_union_u_returns_c"
FROM
    "thoughtful"),
juicy as (
SELECT
    "uneven"."_virt_agg_sum_6520591768854391" * cast("cooperative"."cr_totals_cr_n_groups" as numeric(15,2)) as "_l0_union_u_sales_c",
    "uneven"."cs_call_center_id" as "_l0_union_u_id_c",
    ( "uneven"."_virt_agg_sum_6226990944561419" * "cooperative"."cr_totals_cr_n_groups" ) - cast("cooperative"."cr_totals_cr_total_loss" as numeric(15,2)) as "_l0_union_u_profit_c",
    :_l0_union_u_channel_c as "_l0_union_u_channel_c"
FROM
    "cooperative"
    FULL JOIN "uneven" on 1=1),
vacuous as (
SELECT
    "cooperative"."_l0_union_u_returns_c" as "u_returns",
    "juicy"."_l0_union_u_channel_c" as "u_channel",
    "juicy"."_l0_union_u_id_c" as "u_id",
    "juicy"."_l0_union_u_profit_c" as "u_profit",
    "juicy"."_l0_union_u_sales_c" as "u_sales"
FROM
    "juicy"
    FULL JOIN "cooperative" on 1=1),
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
    FULL JOIN "cooperative" on 1=1)
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

## v4 generation error

```
Traceback (most recent call last):
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 132, in generate_v4_sql
    info, build_env, _, build_stmt = run_tpcds_query(query_id)
                                     ~~~~~~~~~~~~~~~^^^^^^^^^^
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4.py", line 469, in run_tpcds_query
    info = search_concepts(
        mandatory_list=list(build_stmt.output_components),
    ...<4 lines>...
        conditions=[conditions] if conditions else [],
    )
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\concept_strategies_v4.py", line 92, in search_concepts
    result = _search_concepts(
        mandatory_list,
    ...<5 lines>...
        conditions=conditions,
    )
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\concept_strategies_v4.py", line 58, in _search_concepts
    strategy_node = build_strategy_node(
        group_graph, mandatory_list, environment, g, history
    )
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\v4_helper\strategy_builder.py", line 412, in build_strategy_node
    # pass in `_compute_concept_sets`. The SELECT needs to project the
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\v4_helper\strategy_builder.py", line 223, in _topological_order
    return list(nx.topological_sort(lineage_only))
  File "C:\Users\ethan\coding_projects\pytrilogy\.venv\Lib\site-packages\networkx\algorithms\dag.py", line 308, in topological_sort
    for generation in nx.topological_generations(G):
                      ~~~~~~~~~~~~~~~~~~~~~~~~~~^^^
  File "C:\Users\ethan\coding_projects\pytrilogy\.venv\Lib\site-packages\networkx\algorithms\dag.py", line 238, in topological_generations
    raise nx.NetworkXUnfeasible(
        "Graph contains a cycle or graph changed during iteration"
    )
networkx.exception.NetworkXUnfeasible: Graph contains a cycle or graph changed during iteration
```

## reference execution error

```
Traceback (most recent call last):
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 187, in run_one
    result.ref_exec_seconds, result.ref_rows = _time(
                                               ~~~~~^
        lambda: execute(con, ref_sql)
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    )
    ^
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 45, in _time
    value = fn()
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 188, in <lambda>
    lambda: execute(con, ref_sql)
            ~~~~~~~^^^^^^^^^^^^^^
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 120, in execute
    cursor = con.execute(sql)
_duckdb.ParserException: Parser Error: syntax error at or near ":"

LINE 11:     cast("wr_return_date_date"."D_DATE" as date) BETWEEN :period_start AND :period_end
                                                                  ^
```
