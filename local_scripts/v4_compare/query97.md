# Query 97

**Status:** `gen_fail`

| Stage | Result |
| --- | --- |
| v4 SQL generation | FAILED |
| reference execution | OK (1 rows) |

## Result comparison

_at least one side did not produce rows._

## SQL size

| Source | Chars | Lines |
| --- | --- | --- |
| v4 | 0 | 0 |
| reference | 2285 | 55 |

## Preql

```
# Generate counts of (customer, item) pairs that appear only in store sales,
# only in catalog sales, or in both, within a 12-month window.
import unified_sales as sales;

rowset pair_presence <- where
    sales.sales_channel in ('STORE', 'CATALOG')
    and sales.date.month_seq between 1200 and 1200 + 11
    and sales.customer.id is not null
    and sales.item.id is not null
select
    sales.customer.id,
    sales.item.id,
    max(
            case
                when sales.sales_channel = 'STORE' then sales.order_id
                else 0
            end
        ) as store_present,
    max(
            case
                when sales.sales_channel = 'CATALOG' then sales.order_id
                else 0
            end
        ) as catalog_present,
;

select
    sum(
            case
                when pair_presence.store_present >= 1 and pair_presence.catalog_present = 0 then 1
                else 0
            end
        ) as store_sale_count,
    sum(
            case
                when pair_presence.store_present = 0 and pair_presence.catalog_present >= 1 then 1
                else 0
            end
        ) as catalog_sale_count,
    sum(
            case
                when pair_presence.store_present >= 1 and pair_presence.catalog_present >= 1 then 1
                else 0
            end
        ) as both_sale_count,
;
```

## v4 generated SQL

_v4 did not produce SQL._

## Reference SQL (zquery log)

```sql
WITH 
cheerful as (
SELECT
    "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" as "sales_customer_id",
    "sales_catalog_sales_unified"."CS_ITEM_SK" as "sales_item_id",
    "sales_catalog_sales_unified"."CS_ORDER_NUMBER" as "sales_order_id",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" is not null and "sales_catalog_sales_unified"."CS_ITEM_SK" is not null and "sales_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1200 + 11

UNION ALL
SELECT
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "sales_customer_id",
    "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_id",
    "sales_store_sales_unified"."SS_TICKET_NUMBER" as "sales_order_id",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_store_sales_unified"."SS_CUSTOMER_SK" is not null and "sales_store_sales_unified"."SS_ITEM_SK" is not null and "sales_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1200 + 11
),
cooperative as (
SELECT
    max(CASE
	WHEN "cheerful"."sales_sales_channel" = 'CATALOG' THEN "cheerful"."sales_order_id"
	ELSE 0
	END) as "pair_presence_catalog_present",
    max(CASE
	WHEN "cheerful"."sales_sales_channel" = 'STORE' THEN "cheerful"."sales_order_id"
	ELSE 0
	END) as "pair_presence_store_present"
FROM
    "cheerful"
GROUP BY
    "cheerful"."sales_customer_id",
    "cheerful"."sales_item_id")
SELECT
    sum(CASE
	WHEN "cooperative"."pair_presence_store_present" >= 1 and "cooperative"."pair_presence_catalog_present" = 0 THEN 1
	ELSE 0
	END) as "store_sale_count",
    sum(CASE
	WHEN "cooperative"."pair_presence_store_present" = 0 and "cooperative"."pair_presence_catalog_present" >= 1 THEN 1
	ELSE 0
	END) as "catalog_sale_count",
    sum(CASE
	WHEN "cooperative"."pair_presence_store_present" >= 1 and "cooperative"."pair_presence_catalog_present" >= 1 THEN 1
	ELSE 0
	END) as "both_sale_count"
FROM
    "cooperative"
```

## v4 generation error

```
Traceback (most recent call last):
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 114, in generate_v4_sql
    info, build_env, _, build_stmt = run_tpcds_query(query_id)
                                     ~~~~~~~~~~~~~~~^^^^^^^^^^
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4.py", line 470, in run_tpcds_query
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
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\v4_helper\strategy_builder.py", line 275, in build_strategy_node
    for gid in _topological_order(group_graph):
               ~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\v4_helper\strategy_builder.py", line 126, in _topological_order
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
