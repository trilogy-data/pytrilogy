# Query 70

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (3 rows) |
| reference execution | OK (3 rows) |
| results identical | YES |

## Result comparison

v4 rows: 3 (3 distinct)
ref rows: 3 (3 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 3079 | 96 | 37.75 ms |
| reference | 2979 | 86 | 76.80 ms |
| v4 / ref | 1.03x | 1.12x | 0.49x |

## Preql

```
import physical_sales as ss;

# Top-5 states by sum(profit) â€” same as reference's IN-subquery.
auto state_total <- sum(ss.net_profit ? ss.date.month_seq between 1200 and 1211) by ss.store.state;
auto state_rnk <- rank(ss.store.state) over (order by state_total desc);

rowset top_states <- where
    ss.date.month_seq between 1200 and 1211
select
    ss.store.state as ts_state,
    --state_rnk,
having
    state_rnk <= 5
;

# Single ROLLUP(state, county). The sum and the grouping() columns now resolve
# into one rollup CTE, and the window runs directly over that CTE's rows, so
# the NULL-keyed subtotal/total rows stay aligned. Mirrors the reference's
# GROUP BY ROLLUP(state, county) with rank() OVER (PARTITION BY ...).
auto total_sum <- sum(ss.net_profit) by rollup ss.store.state, ss.store.county;
auto g_state <- grouping(ss.store.state) by rollup ss.store.state, ss.store.county;
auto g_county <- grouping(ss.store.county) by rollup ss.store.state, ss.store.county;
auto lochierarchy <- g_state + g_county;
# Partition by state only for the rows still resolved to a county (g_county=0);
# subtotal/total rows partition on NULL. Matches the reference's
# `case when grouping(s_county) = 0 then s_state end`.
auto partition_state <- case when g_county = 0 then ss.store.state else null end;
auto rank_within_parent <- rank(ss.store.state, ss.store.county)
    over (partition by lochierarchy, partition_state order by total_sum desc);

where
    ss.date.month_seq between 1200 and 1211
    and ss.store.state in top_states.ts_state
select
    total_sum,
    ss.store.state as s_state,
    ss.store.county as s_county,
    lochierarchy,
    rank_within_parent,
order by
    lochierarchy desc nulls first,
    case
            when lochierarchy = 0 then ss.store.state
            else null
        end asc nulls first,
    rank_within_parent asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
cheerful as (
SELECT
    "ss_date_date"."D_MONTH_SEQ" as "ss_date_month_seq",
    "ss_store_sales"."SS_NET_PROFIT" as "ss_net_profit",
    "ss_store_store"."S_COUNTY" as "ss_store_county",
    "ss_store_store"."S_STATE" as "ss_store_state"
FROM
    "memory"."store_sales" as "ss_store_sales"
    INNER JOIN "memory"."date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
    LEFT OUTER JOIN "memory"."store" as "ss_store_store" on "ss_store_sales"."SS_STORE_SK" = "ss_store_store"."S_STORE_SK"
WHERE
    "ss_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1211
),
thoughtful as (
SELECT
    "cheerful"."ss_store_state" as "ss_store_state",
    sum(CASE WHEN "cheerful"."ss_date_month_seq" BETWEEN 1200 AND 1211 THEN "cheerful"."ss_net_profit" ELSE NULL END) as "state_total"
FROM
    "cheerful"
GROUP BY
    1),
questionable as (
SELECT
    "thoughtful"."ss_store_state" as "ss_store_state",
    rank() over (order by "thoughtful"."state_total" desc ) as "state_rnk"
FROM
    "thoughtful"),
abundant as (
SELECT
    "questionable"."ss_store_state" as "_top_states_ts_state",
    "questionable"."state_rnk" as "state_rnk"
FROM
    "questionable"),
uneven as (
SELECT
    "abundant"."_top_states_ts_state" as "_top_states_ts_state"
FROM
    "abundant"
WHERE
    "abundant"."state_rnk" <= 5
),
yummy as (
SELECT
    "uneven"."_top_states_ts_state" as "top_states_ts_state"
FROM
    "uneven"),
juicy as (
SELECT
    "cheerful"."ss_net_profit" as "ss_net_profit",
    "cheerful"."ss_store_county" as "ss_store_county",
    "cheerful"."ss_store_state" as "ss_store_state"
FROM
    "cheerful"
WHERE
    "cheerful"."ss_store_state" in (select yummy."top_states_ts_state" from yummy where yummy."top_states_ts_state" is not null)
),
vacuous as (
SELECT
    "juicy"."ss_store_county" as "ss_store_county",
    "juicy"."ss_store_state" as "ss_store_state",
    grouping("juicy"."ss_store_county") as "g_county",
    grouping("juicy"."ss_store_state") as "g_state",
    sum("juicy"."ss_net_profit") as "total_sum"
FROM
    "juicy"
GROUP BY
    ROLLUP (2, 1)),
concerned as (
SELECT
    "vacuous"."g_state" + "vacuous"."g_county" as "lochierarchy",
    "vacuous"."ss_store_county" as "ss_store_county",
    "vacuous"."ss_store_state" as "ss_store_state",
    "vacuous"."total_sum" as "total_sum",
    rank() over (partition by "vacuous"."g_state" + "vacuous"."g_county",CASE
	WHEN "vacuous"."g_county" = 0 THEN "vacuous"."ss_store_state"
	ELSE null
	END order by "vacuous"."total_sum" desc ) as "rank_within_parent"
FROM
    "vacuous")
SELECT
    "concerned"."lochierarchy" as "lochierarchy",
    "concerned"."rank_within_parent" as "rank_within_parent",
    "concerned"."ss_store_county" as "s_county",
    "concerned"."ss_store_state" as "s_state",
    "concerned"."total_sum" as "total_sum"
FROM
    "concerned"
ORDER BY 
    "concerned"."lochierarchy" desc nulls first,
    CASE
	WHEN "concerned"."lochierarchy" = 0 THEN "concerned"."ss_store_state"
	ELSE null
	END asc nulls first,
    "concerned"."rank_within_parent" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
cheerful as (
SELECT
    "ss_store_store"."S_STATE" as "ss_store_state",
    sum("ss_store_sales"."SS_NET_PROFIT") as "state_total"
FROM
    "memory"."store_sales" as "ss_store_sales"
    INNER JOIN "memory"."date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
    LEFT OUTER JOIN "memory"."store" as "ss_store_store" on "ss_store_sales"."SS_STORE_SK" = "ss_store_store"."S_STORE_SK"
WHERE
    "ss_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1211

GROUP BY
    1),
cooperative as (
SELECT
    "cheerful"."ss_store_state" as "_top_states_ts_state",
    rank() over (order by "cheerful"."state_total" desc ) as "state_rnk"
FROM
    "cheerful"),
questionable as (
SELECT
    "cooperative"."_top_states_ts_state" as "top_states_ts_state"
FROM
    "cooperative"
WHERE
    "cooperative"."state_rnk" <= 5
),
abundant as (
SELECT
    "ss_store_sales"."SS_NET_PROFIT" as "ss_net_profit",
    "ss_store_store"."S_COUNTY" as "ss_store_county",
    "ss_store_store"."S_STATE" as "ss_store_state"
FROM
    "memory"."store_sales" as "ss_store_sales"
    INNER JOIN "memory"."date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
    INNER JOIN "memory"."store" as "ss_store_store" on "ss_store_sales"."SS_STORE_SK" = "ss_store_store"."S_STORE_SK"
WHERE
    "ss_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1211 and "ss_store_store"."S_STATE" in (select questionable."top_states_ts_state" from questionable where questionable."top_states_ts_state" is not null)

GROUP BY
    1,
    2,
    3,
    "ss_store_sales"."SS_ITEM_SK",
    "ss_store_sales"."SS_STORE_SK",
    "ss_store_sales"."SS_TICKET_NUMBER"),
uneven as (
SELECT
    "abundant"."ss_store_county" as "ss_store_county",
    "abundant"."ss_store_state" as "ss_store_state",
    CASE
	WHEN grouping("abundant"."ss_store_county") = 0 THEN "abundant"."ss_store_state"
	ELSE null
	END as "partition_state",
    grouping("abundant"."ss_store_state") + grouping("abundant"."ss_store_county") as "lochierarchy",
    sum("abundant"."ss_net_profit") as "total_sum"
FROM
    "abundant"
GROUP BY
    ROLLUP (2, 1)),
yummy as (
SELECT
    "uneven"."lochierarchy" as "lochierarchy",
    "uneven"."ss_store_county" as "ss_store_county",
    "uneven"."ss_store_state" as "ss_store_state",
    "uneven"."total_sum" as "total_sum",
    rank() over (partition by "uneven"."lochierarchy","uneven"."partition_state" order by "uneven"."total_sum" desc ) as "rank_within_parent"
FROM
    "uneven")
SELECT
    "yummy"."total_sum" as "total_sum",
    "yummy"."ss_store_state" as "s_state",
    "yummy"."ss_store_county" as "s_county",
    "yummy"."lochierarchy" as "lochierarchy",
    "yummy"."rank_within_parent" as "rank_within_parent"
FROM
    "yummy"
ORDER BY 
    "yummy"."lochierarchy" desc nulls first,
    CASE
	WHEN "yummy"."lochierarchy" = 0 THEN "yummy"."ss_store_state"
	ELSE null
	END asc nulls first,
    "yummy"."rank_within_parent" asc nulls first
LIMIT (100)
```
