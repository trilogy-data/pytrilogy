# Query 95

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
| reference | 4030 | 76 | 106.85 ms |

## Preql

```
import web_sales as web_sales;

auto multi_warehouse_order <- web_sales.order_number ? count(web_sales.warehouse.id) by web_sales.order_number > 1;
auto returned_orders <- web_sales.order_number ? web_sales.is_returned is True;

where
    web_sales.ship_date.date between '1999-02-01'::date and '1999-04-02'::date
    and web_sales.ship_address.state = 'IL'
    and web_sales.web_site.company_name = 'pri'
    and web_sales.order_number in multi_warehouse_order
    and web_sales.order_number in returned_orders
select
    count(web_sales.order_number) as order_count,
    sum(web_sales.ext_ship_cost) as total_shipping_cost,
    sum(web_sales.net_profit) as total_net_profit,
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
questionable as (
SELECT
    "web_sales_web_sales"."WS_ORDER_NUMBER" as "web_sales_order_number",
    "web_sales_web_sales"."WS_WAREHOUSE_SK" as "web_sales_warehouse_id"
FROM
    "memory"."web_sales" as "web_sales_web_sales"
GROUP BY
    1,
    2),
abundant as (
SELECT
    "questionable"."web_sales_order_number" as "multi_warehouse_order"
FROM
    "questionable"
GROUP BY
    1
HAVING
    count("questionable"."web_sales_warehouse_id") > 1
),
uneven as (
SELECT
    "abundant"."multi_warehouse_order" as "multi_warehouse_order"
FROM
    "abundant"),
thoughtful as (
SELECT
    "web_sales_web_sales"."WS_ORDER_NUMBER" as "returned_orders"
FROM
    "memory"."web_sales" as "web_sales_web_sales"
    INNER JOIN "memory"."web_returns" as "web_sales_web_returns" on "web_sales_web_sales"."WS_ITEM_SK" = "web_sales_web_returns"."WR_ITEM_SK" AND "web_sales_web_sales"."WS_ORDER_NUMBER" = "web_sales_web_returns"."WR_ORDER_NUMBER"
WHERE
    CASE WHEN WR_ORDER_NUMBER IS NOT NULL THEN 1 else 0 END is True and "web_sales_web_sales"."WS_ORDER_NUMBER" in (select uneven."multi_warehouse_order" from uneven where uneven."multi_warehouse_order" is not null)

GROUP BY
    1),
cooperative as (
SELECT
    "web_sales_web_sales"."WS_ORDER_NUMBER" as "web_sales_order_number"
FROM
    "memory"."web_sales" as "web_sales_web_sales"
    INNER JOIN "memory"."web_site" as "web_sales_web_site_web_site" on "web_sales_web_sales"."WS_WEB_SITE_SK" = "web_sales_web_site_web_site"."web_site_sk"
    INNER JOIN "memory"."date_dim" as "web_sales_ship_date_date" on "web_sales_web_sales"."WS_SHIP_DATE_SK" = "web_sales_ship_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer_address" as "web_sales_ship_address_customer_address" on "web_sales_web_sales"."WS_SHIP_ADDR_SK" = "web_sales_ship_address_customer_address"."CA_ADDRESS_SK"
WHERE
    "web_sales_web_site_web_site"."web_company_name" = 'pri' and cast("web_sales_ship_date_date"."D_DATE" as date) BETWEEN date '1999-02-01' AND date '1999-04-02' and "web_sales_ship_address_customer_address"."CA_STATE" = 'IL' and "web_sales_web_sales"."WS_ORDER_NUMBER" in (select uneven."multi_warehouse_order" from uneven where uneven."multi_warehouse_order" is not null) and "web_sales_web_sales"."WS_ORDER_NUMBER" in (select thoughtful."returned_orders" from thoughtful where thoughtful."returned_orders" is not null)

GROUP BY
    1),
concerned as (
SELECT
    sum("web_sales_web_sales"."WS_EXT_SHIP_COST") as "total_shipping_cost",
    sum("web_sales_web_sales"."WS_NET_PROFIT") as "total_net_profit"
FROM
    "memory"."web_sales" as "web_sales_web_sales"
    INNER JOIN "memory"."web_site" as "web_sales_web_site_web_site" on "web_sales_web_sales"."WS_WEB_SITE_SK" = "web_sales_web_site_web_site"."web_site_sk"
    INNER JOIN "memory"."date_dim" as "web_sales_ship_date_date" on "web_sales_web_sales"."WS_SHIP_DATE_SK" = "web_sales_ship_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer_address" as "web_sales_ship_address_customer_address" on "web_sales_web_sales"."WS_SHIP_ADDR_SK" = "web_sales_ship_address_customer_address"."CA_ADDRESS_SK"
WHERE
    cast("web_sales_ship_date_date"."D_DATE" as date) BETWEEN date '1999-02-01' AND date '1999-04-02' and "web_sales_ship_address_customer_address"."CA_STATE" = 'IL' and "web_sales_web_site_web_site"."web_company_name" = 'pri' and "web_sales_web_sales"."WS_ORDER_NUMBER" in (select uneven."multi_warehouse_order" from uneven where uneven."multi_warehouse_order" is not null) and "web_sales_web_sales"."WS_ORDER_NUMBER" in (select thoughtful."returned_orders" from thoughtful where thoughtful."returned_orders" is not null)
),
vacuous as (
SELECT
    count("cooperative"."web_sales_order_number") as "order_count"
FROM
    "cooperative")
SELECT
    coalesce("vacuous"."order_count",0) as "order_count",
    "concerned"."total_shipping_cost" as "total_shipping_cost",
    "concerned"."total_net_profit" as "total_net_profit"
FROM
    "vacuous"
    FULL JOIN "concerned" on 1=1
ORDER BY 
    coalesce("vacuous"."order_count",0) desc
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
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\v4_helper\strategy_builder.py", line 386, in build_strategy_node
    for gid in _topological_order(group_graph):
               ~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^
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
