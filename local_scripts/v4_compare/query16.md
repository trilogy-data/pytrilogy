# Query 16

**Status:** `gen_fail`

| Stage | Result |
| --- | --- |
| v4 SQL generation | FAILED |
| reference execution | OK (1 rows) |

## Result comparison

_at least one side did not produce rows._

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 0 | 0 | — |
| reference | 3713 | 83 | 68.10 ms |

## Preql

```
import catalog_sales as cs;
import catalog_returns as cr;

auto multi_warehouse_sales <- cs.order_number ? count(cs.warehouse.id) by cs.order_number > 1;

where
    cs.ship_date.date between '2002-02-01'::date and '2002-04-02'::date
    and cs.customer_address.state = 'GA'
    and cs.call_center.county = 'Williamson County'
    and cs.order_number not in cr.order_number
    and cs.order_number in multi_warehouse_sales
select
    count_distinct(cs.order_number) as order_count,
    sum(cs.ext_ship_cost) as total_shipping_cost,
    sum(cs.net_profit) as total_net_profit,
order by
    order_count desc
limit 100
;
```

## v4 generated SQL

_v4 did not produce SQL._

## Reference SQL (zquery log)

```sql
WITH 
cooperative as (
SELECT
    "cs_catalog_sales"."CS_ORDER_NUMBER" as "cs_order_number",
    "cs_catalog_sales"."CS_WAREHOUSE_SK" as "cs_warehouse_id"
FROM
    "memory"."catalog_sales" as "cs_catalog_sales"
GROUP BY
    1,
    2),
quizzical as (
SELECT
    "cr_catalog_returns"."CR_ORDER_NUMBER" as "cr_order_number"
FROM
    "memory"."catalog_returns" as "cr_catalog_returns"
GROUP BY
    1),
questionable as (
SELECT
    "cooperative"."cs_order_number" as "multi_warehouse_sales"
FROM
    "cooperative"
GROUP BY
    1
HAVING
    count("cooperative"."cs_warehouse_id") > 1
),
abundant as (
SELECT
    "questionable"."multi_warehouse_sales" as "multi_warehouse_sales"
FROM
    "questionable"),
thoughtful as (
SELECT
    "cs_catalog_sales"."CS_ORDER_NUMBER" as "cs_order_number"
FROM
    "memory"."catalog_sales" as "cs_catalog_sales"
    INNER JOIN "memory"."date_dim" as "cs_ship_date_date" on "cs_catalog_sales"."CS_SHIP_DATE_SK" = "cs_ship_date_date"."D_DATE_SK"
    INNER JOIN "memory"."call_center" as "cs_call_center_call_center" on "cs_catalog_sales"."CS_CALL_CENTER_SK" = "cs_call_center_call_center"."CC_CALL_CENTER_SK"
    INNER JOIN "memory"."customer_address" as "cs_customer_address_customer_address" on "cs_catalog_sales"."CS_SHIP_ADDR_SK" = "cs_customer_address_customer_address"."CA_ADDRESS_SK"
WHERE
    cast("cs_ship_date_date"."D_DATE" as date) BETWEEN date '2002-02-01' AND date '2002-04-02' and "cs_call_center_call_center"."CC_COUNTY" = 'Williamson County' and "cs_customer_address_customer_address"."CA_STATE" = 'GA' and "cs_catalog_sales"."CS_ORDER_NUMBER" not in (select quizzical."cr_order_number" from quizzical where quizzical."cr_order_number" is not null) and "cs_catalog_sales"."CS_ORDER_NUMBER" in (select abundant."multi_warehouse_sales" from abundant where abundant."multi_warehouse_sales" is not null)

GROUP BY
    1),
concerned as (
SELECT
    "cs_catalog_sales"."CS_EXT_SHIP_COST" as "cs_ext_ship_cost",
    "cs_catalog_sales"."CS_NET_PROFIT" as "cs_net_profit"
FROM
    "memory"."catalog_sales" as "cs_catalog_sales"
    INNER JOIN "memory"."date_dim" as "cs_ship_date_date" on "cs_catalog_sales"."CS_SHIP_DATE_SK" = "cs_ship_date_date"."D_DATE_SK"
    INNER JOIN "memory"."call_center" as "cs_call_center_call_center" on "cs_catalog_sales"."CS_CALL_CENTER_SK" = "cs_call_center_call_center"."CC_CALL_CENTER_SK"
    INNER JOIN "memory"."customer_address" as "cs_customer_address_customer_address" on "cs_catalog_sales"."CS_SHIP_ADDR_SK" = "cs_customer_address_customer_address"."CA_ADDRESS_SK"
WHERE
    cast("cs_ship_date_date"."D_DATE" as date) BETWEEN date '2002-02-01' AND date '2002-04-02' and "cs_customer_address_customer_address"."CA_STATE" = 'GA' and "cs_call_center_call_center"."CC_COUNTY" = 'Williamson County' and "cs_catalog_sales"."CS_ORDER_NUMBER" not in (select quizzical."cr_order_number" from quizzical where quizzical."cr_order_number" is not null) and "cs_catalog_sales"."CS_ORDER_NUMBER" in (select abundant."multi_warehouse_sales" from abundant where abundant."multi_warehouse_sales" is not null)

GROUP BY
    1,
    2,
    "cs_catalog_sales"."CS_ITEM_SK",
    "cs_catalog_sales"."CS_ORDER_NUMBER"),
vacuous as (
SELECT
    count(distinct "thoughtful"."cs_order_number") as "order_count"
FROM
    "thoughtful"),
young as (
SELECT
    sum("concerned"."cs_ext_ship_cost") as "total_shipping_cost",
    sum("concerned"."cs_net_profit") as "total_net_profit"
FROM
    "concerned")
SELECT
    "vacuous"."order_count" as "order_count",
    "young"."total_shipping_cost" as "total_shipping_cost",
    "young"."total_net_profit" as "total_net_profit"
FROM
    "vacuous"
    FULL JOIN "young" on 1=1
ORDER BY 
    "vacuous"."order_count" desc
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
