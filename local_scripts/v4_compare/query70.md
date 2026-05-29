# Query 70

**Status:** `gen_fail`

| Stage | Result |
| --- | --- |
| v4 SQL generation | FAILED |
| reference execution | OK (3 rows) |

## Result comparison

_at least one side did not produce rows._

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 0 | 0 | — |
| reference | 2675 | 75 | 38.13 ms |

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

_v4 did not produce SQL._

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
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\concept_strategies_v4.py", line 57, in _search_concepts
    group_graph = build_group_graph(concept_graph, conditions, mandatory_list)
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\v4_helper\group_graph.py", line 631, in build_group_graph
    _compute_concept_sets(group_graph, concept_graph, mandatory_list)
    ~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\v4_helper\group_graph.py", line 526, in _compute_concept_sets
    topo = list(nx.topological_sort(lineage_only))
  File "C:\Users\ethan\coding_projects\pytrilogy\.venv\Lib\site-packages\networkx\algorithms\dag.py", line 308, in topological_sort
    for generation in nx.topological_generations(G):
                      ~~~~~~~~~~~~~~~~~~~~~~~~~~^^^
  File "C:\Users\ethan\coding_projects\pytrilogy\.venv\Lib\site-packages\networkx\algorithms\dag.py", line 238, in topological_generations
    raise nx.NetworkXUnfeasible(
        "Graph contains a cycle or graph changed during iteration"
    )
networkx.exception.NetworkXUnfeasible: Graph contains a cycle or graph changed during iteration
```
