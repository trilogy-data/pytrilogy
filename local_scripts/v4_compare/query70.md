# Query 70

**Status:** `mismatch`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (1 rows) |
| reference execution | OK (3 rows) |
| results identical | NO |

## Result comparison

v4 rows: 1 (1 distinct)
ref rows: 3 (3 distinct)
only in v4 (showing up to 5 of 1):
  1x  (0, 'Williamson County', 'TN', Decimal('-444194870.30'), 1, 'Williamson County', 'TN')
only in ref (showing up to 5 of 3):
  1x  (2, Decimal('-444194870.30'), 1, None, None)
  1x  (1, Decimal('-444194870.30'), 1, None, 'TN')
  1x  (0, Decimal('-444194870.30'), 1, 'Williamson County', 'TN')

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 2992 | 81 | 33.17 ms |
| reference | 2675 | 75 | 32.46 ms |
| v4 / ref | 1.12x | 1.08x | 1.02x |

## Preql

```
import store_sales as ss;

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

# Single ROLLUP(state, county) materialized in q70_rolled. Mirrors reference:
# GROUP BY ROLLUP(state, county) with rank() OVER (PARTITION BY ...).
#
# We avoid `grouping()` because trilogy's planner splits the rollup into
# separate CTEs when mixing `grouping()` with `sum()` (different dependency
# bases on the source rows). Instead we derive lochierarchy/partition_state
# from the rollup-output NULL pattern: store rows always have non-null
# state/county in source, so a NULL in the rollup output unambiguously means
# "rolled up at that level".
rowset q70_rolled <- where
    ss.date.month_seq between 1200 and 1211 and ss.store.state in top_states.ts_state
select
    sum(ss.net_profit) by rollup ss.store.state, ss.store.county as total_sum,
    ss.store.state as r_state,
    ss.store.county as r_county,
;

def loc_level(state, county) -> case
    when county is not null then 0
    when state is not null then 1
    else 2
end;
def partition_state(state, county) -> case
    when county is not null then state
    else null
end;

select
    q70_rolled.total_sum,
    q70_rolled.r_state as s_state,
    q70_rolled.r_county as s_county,
    @loc_level(q70_rolled.r_state, q70_rolled.r_county) as lochierarchy,
    rank(q70_rolled.r_county, q70_rolled.r_state)
            over (partition by @loc_level(q70_rolled.r_state, q70_rolled.r_county),
                    @partition_state(q70_rolled.r_state, q70_rolled.r_county)
                order by q70_rolled.total_sum desc) as rank_within_parent,
order by
    lochierarchy desc nulls first,
    case
            when lochierarchy = 0 then s_state
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
    "ss_store_store"."S_COUNTY" as "q70_rolled_r_county",
    "ss_store_store"."S_STATE" as "q70_rolled_r_state",
    sum("ss_store_sales"."SS_NET_PROFIT") as "q70_rolled_total_sum"
FROM
    "memory"."store_sales" as "ss_store_sales"
    INNER JOIN "memory"."date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
    INNER JOIN "memory"."store" as "ss_store_store" on "ss_store_sales"."SS_STORE_SK" = "ss_store_store"."S_STORE_SK"
WHERE
    "ss_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1211 and "ss_store_store"."S_STATE" in (select questionable."top_states_ts_state" from questionable where questionable."top_states_ts_state" is not null)

GROUP BY
    ROLLUP (2, 1)),
yummy as (
SELECT
    "abundant"."q70_rolled_r_county" as "s_county",
    "abundant"."q70_rolled_r_state" as "s_state",
    CASE
	WHEN "abundant"."q70_rolled_r_county" is not null THEN "abundant"."q70_rolled_r_state"
	ELSE null
	END as "partition_state",
    CASE
	WHEN "abundant"."q70_rolled_r_county" is not null THEN 0
	WHEN "abundant"."q70_rolled_r_state" is not null THEN 1
	ELSE 2
	END as "loc_level",
    CASE
	WHEN "abundant"."q70_rolled_r_county" is not null THEN 0
	WHEN "abundant"."q70_rolled_r_state" is not null THEN 1
	ELSE 2
	END as "lochierarchy"
FROM
    "abundant")
SELECT
    "yummy"."lochierarchy" as "lochierarchy",
    rank() over (partition by "yummy"."loc_level","yummy"."partition_state" order by "abundant"."q70_rolled_total_sum" desc ) as "rank_within_parent",
    "yummy"."s_county" as "s_county",
    "yummy"."s_state" as "s_state",
    "abundant"."q70_rolled_r_county" as "q70_rolled_r_county",
    "abundant"."q70_rolled_r_state" as "q70_rolled_r_state",
    "abundant"."q70_rolled_total_sum" as "q70_rolled_total_sum"
FROM
    "yummy"
    INNER JOIN "abundant" on "yummy"."s_county" = "abundant"."q70_rolled_r_county" AND "yummy"."s_state" = "abundant"."q70_rolled_r_state"
ORDER BY 
    "yummy"."lochierarchy" desc nulls first,
    CASE
	WHEN "yummy"."lochierarchy" = 0 THEN "yummy"."s_state"
	ELSE null
	END asc nulls first,
    "rank_within_parent" asc nulls first
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
    "ss_store_store"."S_COUNTY" as "q70_rolled_r_county",
    "ss_store_store"."S_STATE" as "q70_rolled_r_state",
    CASE
	WHEN "ss_store_store"."S_COUNTY" is not null THEN "ss_store_store"."S_STATE"
	ELSE null
	END as "partition_state",
    CASE
	WHEN "ss_store_store"."S_COUNTY" is not null THEN 0
	WHEN "ss_store_store"."S_STATE" is not null THEN 1
	ELSE 2
	END as "loc_level",
    sum("ss_store_sales"."SS_NET_PROFIT") as "q70_rolled_total_sum"
FROM
    "memory"."store_sales" as "ss_store_sales"
    INNER JOIN "memory"."date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
    INNER JOIN "memory"."store" as "ss_store_store" on "ss_store_sales"."SS_STORE_SK" = "ss_store_store"."S_STORE_SK"
WHERE
    "ss_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1211 and "ss_store_store"."S_STATE" in (select questionable."top_states_ts_state" from questionable where questionable."top_states_ts_state" is not null)

GROUP BY
    ROLLUP (2, 1))
SELECT
    "abundant"."q70_rolled_total_sum" as "q70_rolled_total_sum",
    "abundant"."q70_rolled_r_state" as "s_state",
    "abundant"."q70_rolled_r_county" as "s_county",
    CASE
	WHEN "abundant"."q70_rolled_r_county" is not null THEN 0
	WHEN "abundant"."q70_rolled_r_state" is not null THEN 1
	ELSE 2
	END as "lochierarchy",
    rank() over (partition by "abundant"."loc_level","abundant"."partition_state" order by "abundant"."q70_rolled_total_sum" desc ) as "rank_within_parent"
FROM
    "abundant"
ORDER BY 
    "lochierarchy" desc nulls first,
    CASE
	WHEN CASE
	WHEN "abundant"."q70_rolled_r_county" is not null THEN 0
	WHEN "abundant"."q70_rolled_r_state" is not null THEN 1
	ELSE 2
	END = 0 THEN "abundant"."q70_rolled_r_state"
	ELSE null
	END asc nulls first,
    "rank_within_parent" asc nulls first
LIMIT (100)
```
