# Query 44

**Status:** `gen_fail`

| Stage | Result |
| --- | --- |
| v4 SQL generation | FAILED |
| reference execution | OK (10 rows) |

## Result comparison

_at least one side did not produce rows._

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 0 | 0 | — |
| reference | 2861 | 101 | 75.25 ms |

## Preql

```
import store_sales as ss;

# Threshold: avg net_profit for store 4 where address is null.
# Pre-compute in a filtered `with` block so trilogy emits
# `WHERE SS_ADDR_SK IS NULL` instead of `avg(CASE WHEN SS_ADDR_SK IS NULL THEN ...)`.
rowset addr_null_threshold <- where
    ss.store.id = 4 and ss.sale_address.id is null
select
    avg(ss.net_profit) as threshold,
;

# Per-item avg profit at store 4
auto item_avg_profit <- avg(ss.net_profit) by ss.item.id;

rowset ascending <- where
    ss.store.id = 4 and item_avg_profit > 0.9 * addr_null_threshold.threshold
select
    rank(ss.item.id) over (order by item_avg_profit asc) as rnk_a,
    ss.item.product_name as best_performing,
;

rowset descending <- where
    ss.store.id = 4 and item_avg_profit > 0.9 * addr_null_threshold.threshold
select
    rank(ss.item.id) over (order by item_avg_profit desc) as rnk_d,
    ss.item.product_name as worst_performing,
;

merge ascending.rnk_a into descending.rnk_d;

where
    ss.store.id = 4
select
    ascending.rnk_a as rnk,
    ascending.best_performing,
    descending.worst_performing,
having
    rnk < 11

order by
    rnk asc nulls first,
    ascending.best_performing desc nulls first,
    descending.worst_performing desc nulls first
limit 100
;
```

## v4 generated SQL

_v4 did not produce SQL._

## Reference SQL (zquery log)

```sql
WITH 
thoughtful as (
SELECT
    "ss_store_sales"."SS_ITEM_SK" as "ss_item_id",
    avg("ss_store_sales"."SS_NET_PROFIT") as "item_avg_profit"
FROM
    "memory"."store_sales" as "ss_store_sales"
WHERE
    "ss_store_sales"."SS_STORE_SK" = 4

GROUP BY
    1),
cooperative as (
SELECT
    "ss_store_sales"."SS_ITEM_SK" as "ss_item_id"
FROM
    "memory"."store_sales" as "ss_store_sales"
WHERE
    "ss_store_sales"."SS_STORE_SK" = 4

GROUP BY
    1,
    "ss_store_sales"."SS_STORE_SK"),
highfalutin as (
SELECT
    avg("ss_store_sales"."SS_NET_PROFIT") as "addr_null_threshold_threshold"
FROM
    "memory"."store_sales" as "ss_store_sales"
WHERE
    "ss_store_sales"."SS_STORE_SK" = 4 and "ss_store_sales"."SS_ADDR_SK" is null
),
questionable as (
SELECT
    "thoughtful"."item_avg_profit" as "item_avg_profit",
    "thoughtful"."ss_item_id" as "ss_item_id"
FROM
    "cooperative"
    INNER JOIN "thoughtful" on "cooperative"."ss_item_id" = "thoughtful"."ss_item_id"),
abundant as (
SELECT
    "questionable"."item_avg_profit" as "item_avg_profit",
    "questionable"."ss_item_id" as "ss_item_id"
FROM
    "highfalutin"
    INNER JOIN "questionable" on 1=1
WHERE
    "questionable"."item_avg_profit" > 0.9 * "highfalutin"."addr_null_threshold_threshold"

GROUP BY
    1,
    2),
uneven as (
SELECT
    "abundant"."ss_item_id" as "ss_item_id",
    rank() over (order by "abundant"."item_avg_profit" asc ) as "_ascending_rnk_a",
    rank() over (order by "abundant"."item_avg_profit" desc ) as "_descending_rnk_d"
FROM
    "abundant"),
juicy as (
SELECT
    "ss_item_items"."I_PRODUCT_NAME" as "descending_worst_performing",
    "uneven"."_descending_rnk_d" as "descending_rnk_d",
    "uneven"."_descending_rnk_d" as "rnk"
FROM
    "uneven"
    INNER JOIN "memory"."item" as "ss_item_items" on "uneven"."ss_item_id" = "ss_item_items"."I_ITEM_SK"
GROUP BY
    1,
    2),
yummy as (
SELECT
    "ss_item_items"."I_PRODUCT_NAME" as "ascending_best_performing",
    "uneven"."_ascending_rnk_a" as "ascending_rnk_a"
FROM
    "uneven"
    INNER JOIN "memory"."item" as "ss_item_items" on "uneven"."ss_item_id" = "ss_item_items"."I_ITEM_SK"
GROUP BY
    1,
    2),
vacuous as (
SELECT
    "juicy"."descending_worst_performing" as "descending_worst_performing",
    "juicy"."rnk" as "rnk",
    "yummy"."ascending_best_performing" as "ascending_best_performing"
FROM
    "juicy"
    INNER JOIN "yummy" on "juicy"."descending_rnk_d" = "yummy"."ascending_rnk_a"
WHERE
    "juicy"."rnk" < 11
)
SELECT
    "vacuous"."rnk" as "rnk",
    "vacuous"."ascending_best_performing" as "ascending_best_performing",
    "vacuous"."descending_worst_performing" as "descending_worst_performing"
FROM
    "vacuous"
ORDER BY 
    "vacuous"."rnk" asc nulls first,
    "vacuous"."ascending_best_performing" desc nulls first,
    "vacuous"."descending_worst_performing" desc nulls first
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
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\v4_helper\strategy_builder.py", line 288, in build_strategy_node
    for gid in _topological_order(group_graph):
               ~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\v4_helper\strategy_builder.py", line 125, in _topological_order
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
