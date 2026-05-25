# Query 92

**Status:** `gen_fail`

| Stage | Result |
| --- | --- |
| v4 SQL generation | FAILED |
| reference execution | FAILED |

## Result comparison

_at least one side did not produce rows._

## SQL size

| Source | Chars | Lines |
| --- | --- | --- |
| v4 | 0 | 0 |
| reference | 1318 | 35 |

## Preql

```
import web_sales as ws;

const start_date <- '2000-01-27'::date;
const end_date <- '2000-04-26'::date;
auto avg_item_disc <- 1.3 * avg(ws.ext_discount_amount ? ws.date.date between start_date and end_date) by ws.item.id;

where
    ws.item.manufacturer_id = 350
    and ws.date.date between start_date and end_date
    and ws.ext_discount_amount > avg_item_disc
select
    sum(ws.ext_discount_amount) as excess_discount_amount,
order by
    excess_discount_amount asc nulls first
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
    "ws_item_items"."I_ITEM_SK" as "ws_item_id",
    "ws_web_sales"."WS_EXT_DISCOUNT_AMT" as "ws_ext_discount_amount"
FROM
    "memory"."web_sales" as "ws_web_sales"
    INNER JOIN "memory"."date_dim" as "ws_date_date" on "ws_web_sales"."WS_SOLD_DATE_SK" = "ws_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "ws_item_items" on "ws_web_sales"."WS_ITEM_SK" = "ws_item_items"."I_ITEM_SK"
WHERE
    "ws_item_items"."I_MANUFACT_ID" = 350 and cast("ws_date_date"."D_DATE" as date) BETWEEN :start_date AND :end_date
),
thoughtful as (
SELECT
    "ws_web_sales"."WS_ITEM_SK" as "ws_item_id",
    avg("ws_web_sales"."WS_EXT_DISCOUNT_AMT") as "_virt_agg_avg_5364249642270353"
FROM
    "memory"."web_sales" as "ws_web_sales"
    INNER JOIN "memory"."date_dim" as "ws_date_date" on "ws_web_sales"."WS_SOLD_DATE_SK" = "ws_date_date"."D_DATE_SK"
WHERE
    cast("ws_date_date"."D_DATE" as date) BETWEEN :start_date AND :end_date

GROUP BY
    1)
SELECT
    sum("cheerful"."ws_ext_discount_amount") as "excess_discount_amount"
FROM
    "thoughtful"
    INNER JOIN "cheerful" on "thoughtful"."ws_item_id" = "cheerful"."ws_item_id"
WHERE
    "cheerful"."ws_ext_discount_amount" > 1.3 * "thoughtful"."_virt_agg_avg_5364249642270353"

ORDER BY 
    "excess_discount_amount" asc nulls first
LIMIT (100)
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
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\concept_strategies_v4.py", line 57, in _search_concepts
    group_graph = build_group_graph(concept_graph, conditions)
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\v4_helper\group_graph.py", line 252, in build_group_graph
    condition_group_ids = _inject_conditions(group_graph, buckets, conditions)
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\v4_helper\group_graph.py", line 184, in _inject_conditions
    raise ValueError(
    ...<3 lines>...
    )
ValueError: Atom ws.ext_discount_amount > local.avg_item_disc would be injected at grp:basic:d1:ws.item.id|ws.order_number, which is downstream of d0 barrier(s) ['grp:aggregate:d0:ws.item.id']; conditions cannot be pushed past row-shape changes.
```

## reference execution error

```
Traceback (most recent call last):
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 167, in run_one
    result.ref_rows = execute(con, ref_sql)
                      ~~~~~~~^^^^^^^^^^^^^^
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 102, in execute
    cursor = con.execute(sql)
_duckdb.ParserException: Parser Error: syntax error at or near ":"

LINE 11: ..." = 350 and cast("ws_date_date"."D_DATE" as date) BETWEEN :start_date AND :end_date
                                                                      ^
```
