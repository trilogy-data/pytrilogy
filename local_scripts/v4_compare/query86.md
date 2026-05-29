# Query 86

**Status:** `gen_fail`

| Stage | Result |
| --- | --- |
| v4 SQL generation | FAILED |
| reference execution | OK (100 rows) |

## Result comparison

_at least one side did not produce rows._

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 0 | 0 | — |
| reference | 2339 | 58 | 107.47 ms |

## Preql

```
import web_sales as web_sales;

# Single ROLLUP(category, class) with rank() over (partition by lochierarchy,
# partition_cat order by total_sum desc). Mirrors the reference's
# GROUP BY ROLLUP + rank() OVER (PARTITION BY ...).
#
# Same pattern as query36.preql: all aggregates + grouping bits + CASE WHEN
# derivations are computed inside the rowset and the underlying `sum`/
# `grouping` rows are hidden so trilogy's planner doesn't split them across
# CTEs. Real `grouping()` (not the rollup-NULL-pattern trick from q70)
# because items can have NULL category/class.
rowset q86_rolled <- where
    web_sales.date.month_seq between 1200 and 1211
select
    --sum(web_sales.net_paid) by rollup web_sales.item.category, web_sales.item.class as total_sum_raw,
    --grouping(web_sales.item.category) by rollup web_sales.item.category, web_sales.item.class as g_cat,
    --grouping(web_sales.item.class) by rollup web_sales.item.category, web_sales.item.class as g_class,
    total_sum_raw as total_sum,
    g_cat + g_class as lochierarchy,
    case
            when g_class = 0 then web_sales.item.category
            else null
        end as partition_cat,
    web_sales.item.category as r_category,
    web_sales.item.class as r_class,
;

select
    q86_rolled.total_sum,
    q86_rolled.r_category as i_category,
    q86_rolled.r_class as i_class,
    q86_rolled.lochierarchy,
    rank(q86_rolled.r_class, q86_rolled.r_category)
            over (partition by q86_rolled.lochierarchy, q86_rolled.partition_cat
                order by q86_rolled.total_sum desc) as rank_within_parent,
order by
    q86_rolled.lochierarchy desc nulls first,
    case
            when q86_rolled.lochierarchy = 0 then i_category
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
    "web_sales_item_items"."I_CATEGORY" as "web_sales_item_category",
    "web_sales_item_items"."I_CLASS" as "web_sales_item_class",
    "web_sales_web_sales"."WS_NET_PAID" as "web_sales_net_paid"
FROM
    "memory"."web_sales" as "web_sales_web_sales"
    INNER JOIN "memory"."date_dim" as "web_sales_date_date" on "web_sales_web_sales"."WS_SOLD_DATE_SK" = "web_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "web_sales_item_items" on "web_sales_web_sales"."WS_ITEM_SK" = "web_sales_item_items"."I_ITEM_SK"
WHERE
    "web_sales_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1211
),
thoughtful as (
SELECT
    "cheerful"."web_sales_item_category" as "q86_rolled_r_category",
    "cheerful"."web_sales_item_class" as "q86_rolled_r_class",
    CASE
	WHEN grouping("cheerful"."web_sales_item_class") = 0 THEN "cheerful"."web_sales_item_category"
	ELSE null
	END as "q86_rolled_partition_cat",
    grouping("cheerful"."web_sales_item_category") + grouping("cheerful"."web_sales_item_class") as "q86_rolled_lochierarchy",
    sum("cheerful"."web_sales_net_paid") as "q86_rolled_total_sum"
FROM
    "cheerful"
GROUP BY
    ROLLUP (1, 2)),
cooperative as (
SELECT
    "thoughtful"."q86_rolled_lochierarchy" as "q86_rolled_lochierarchy",
    "thoughtful"."q86_rolled_r_category" as "q86_rolled_r_category",
    "thoughtful"."q86_rolled_r_class" as "q86_rolled_r_class",
    "thoughtful"."q86_rolled_total_sum" as "q86_rolled_total_sum",
    rank() over (partition by "thoughtful"."q86_rolled_lochierarchy","thoughtful"."q86_rolled_partition_cat" order by "thoughtful"."q86_rolled_total_sum" desc ) as "rank_within_parent"
FROM
    "thoughtful")
SELECT
    "cooperative"."q86_rolled_total_sum" as "q86_rolled_total_sum",
    "cooperative"."q86_rolled_r_category" as "i_category",
    "cooperative"."q86_rolled_r_class" as "i_class",
    "cooperative"."q86_rolled_lochierarchy" as "q86_rolled_lochierarchy",
    "cooperative"."rank_within_parent" as "rank_within_parent"
FROM
    "cooperative"
GROUP BY
    1,
    2,
    3,
    4,
    5
ORDER BY 
    "cooperative"."q86_rolled_lochierarchy" desc nulls first,
    CASE
	WHEN "cooperative"."q86_rolled_lochierarchy" = 0 THEN "cooperative"."q86_rolled_r_category"
	ELSE null
	END asc nulls first,
    "cooperative"."rank_within_parent" asc nulls first
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
