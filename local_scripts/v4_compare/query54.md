# Query 54

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
| reference | 4183 | 91 | 31.48 ms |

## Preql

```
import unified_sales as sales;
import store_sales as ss;
import store as store;

# my_customers: customers who bought i_category='Women' & i_class='maternity'
# from catalog or web in Dec 1998
rowset my_customers <- where
    sales.sales_channel in ('CATALOG', 'WEB')
    and sales.item.category = 'Women'
    and sales.item.class = 'maternity'
    and sales.date.year = 1998
    and sales.date.month_of_year = 12
    and sales.customer.id is not null
select
    sales.customer.id as my_cust_id,
;

# Reference q54 cross-joins each ss row with every store matching the
# customer-address county/state. Compute as (ss revenue per customer) *
# (count of stores in customer's county/state), via 2 rowsets keyed on
# (county, state) and merged.
rowset cust_ss <- where
    ss.customer.id in my_customers.my_cust_id
    and ss.date.month_seq >= 1188
    and ss.date.month_seq <= 1190
    and ss.customer.id is not null
select
    ss.customer.id as ss_cust_id,
    ss.customer.address.county as ss_cust_county,
    ss.customer.address.state as ss_cust_state,
    sum(ss.ext_sales_price) as ss_revenue,
;

rowset stores_cs <- select
    store.county as scs_county,
    store.state as scs_state,
    count(store.id) as scs_count,
;

merge cust_ss.ss_cust_county into stores_cs.scs_county;
merge cust_ss.ss_cust_state into stores_cs.scs_state;

# Materialize per-customer revenue at the (cust, county, state, count) grain
# so the downstream count is over the post-join row set.
rowset my_revenue <- select
    cust_ss.ss_cust_id as rev_cust_id,
    cust_ss.ss_revenue * stores_cs.scs_count as revenue,
;

auto segment <- round(my_revenue.revenue / 50, 0)::int;

select
    segment,
    count(my_revenue.rev_cust_id) as num_customers,
    segment * 50 as segment_base,
order by
    segment asc nulls first,
    num_customers asc nulls first,
    segment_base asc nulls first
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
    "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" as "sales_customer_id"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "sales_item_items" on "sales_catalog_sales_unified"."CS_ITEM_SK" = "sales_item_items"."I_ITEM_SK"
WHERE
    "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" is not null and "sales_date_date"."D_YEAR" = 1998 and "sales_date_date"."D_MOY" = 12 and "sales_item_items"."I_CATEGORY" = 'Women' and "sales_item_items"."I_CLASS" = 'maternity'

UNION ALL
SELECT
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" as "sales_customer_id"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "sales_item_items" on "sales_web_sales_unified"."WS_ITEM_SK" = "sales_item_items"."I_ITEM_SK"
WHERE
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" is not null and "sales_date_date"."D_YEAR" = 1998 and "sales_date_date"."D_MOY" = 12 and "sales_item_items"."I_CATEGORY" = 'Women' and "sales_item_items"."I_CLASS" = 'maternity'
),
sparkling as (
SELECT
    "store_store"."S_COUNTY" as "stores_cs_scs_county",
    "store_store"."S_STATE" as "stores_cs_scs_state",
    count("store_store"."S_STORE_SK") as "stores_cs_scs_count"
FROM
    "memory"."store" as "store_store"
GROUP BY
    1,
    2),
thoughtful as (
SELECT
    "cheerful"."sales_customer_id" as "my_customers_my_cust_id"
FROM
    "cheerful"
GROUP BY
    1),
concerned as (
SELECT
    "ss_customer_address_customer_address"."CA_COUNTY" as "cust_ss_ss_cust_county",
    "ss_customer_address_customer_address"."CA_STATE" as "cust_ss_ss_cust_state",
    "ss_store_sales"."SS_CUSTOMER_SK" as "cust_ss_ss_cust_id",
    sum("ss_store_sales"."SS_EXT_SALES_PRICE") as "cust_ss_ss_revenue"
FROM
    "memory"."store_sales" as "ss_store_sales"
    INNER JOIN "memory"."date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer" as "ss_customer_customers" on "ss_store_sales"."SS_CUSTOMER_SK" = "ss_customer_customers"."C_CUSTOMER_SK"
    LEFT OUTER JOIN "memory"."customer_address" as "ss_customer_address_customer_address" on "ss_customer_customers"."C_CURRENT_ADDR_SK" = "ss_customer_address_customer_address"."CA_ADDRESS_SK"
WHERE
    "ss_store_sales"."SS_CUSTOMER_SK" in (select thoughtful."my_customers_my_cust_id" from thoughtful where thoughtful."my_customers_my_cust_id" is not null) and "ss_date_date"."D_MONTH_SEQ" >= 1188 and "ss_date_date"."D_MONTH_SEQ" <= 1190 and "ss_store_sales"."SS_CUSTOMER_SK" is not null

GROUP BY
    1,
    2,
    3),
late as (
SELECT
    "concerned"."cust_ss_ss_cust_id" as "my_revenue_rev_cust_id"
FROM
    "concerned"
    INNER JOIN "sparkling" on "concerned"."cust_ss_ss_cust_county" = "sparkling"."stores_cs_scs_county" AND "concerned"."cust_ss_ss_cust_state" = "sparkling"."stores_cs_scs_state"
GROUP BY
    1),
sweltering as (
SELECT
    cast(round(( "concerned"."cust_ss_ss_revenue" * "sparkling"."stores_cs_scs_count" ) / 50,0) as int) * 50 as "segment_base",
    cast(round(( "concerned"."cust_ss_ss_revenue" * "sparkling"."stores_cs_scs_count" ) / 50,0) as int) as "segment"
FROM
    "concerned"
    INNER JOIN "sparkling" on "concerned"."cust_ss_ss_cust_county" = "sparkling"."stores_cs_scs_county" AND "concerned"."cust_ss_ss_cust_state" = "sparkling"."stores_cs_scs_state"
GROUP BY
    1,
    2),
macho as (
SELECT
    CASE WHEN "late"."my_revenue_rev_cust_id" IS NOT NULL THEN 1 ELSE 0 END as "num_customers"
FROM
    "late")
SELECT
    "sweltering"."segment" as "segment",
    coalesce("macho"."num_customers",0) as "num_customers",
    "sweltering"."segment_base" as "segment_base"
FROM
    "macho"
    FULL JOIN "sweltering" on 1=1
ORDER BY 
    "sweltering"."segment" asc nulls first,
    coalesce("macho"."num_customers",0) asc nulls first,
    "sweltering"."segment_base" asc nulls first
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
