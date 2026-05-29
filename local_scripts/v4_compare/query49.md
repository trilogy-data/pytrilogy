# Query 49

**Status:** `gen_fail`

| Stage | Result |
| --- | --- |
| v4 SQL generation | FAILED |
| reference execution | OK (34 rows) |

## Result comparison

_at least one side did not produce rows._

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 0 | 0 | — |
| reference | 5430 | 119 | 55.80 ms |

## Preql

```
import unified_sales as sales;

# Per-(channel, item) ratios. Filter requires a returns row with return_amount > 10000
# (acts as INNER on LEFT JOIN to returns), and Dec 2001.
def in_window(metric) -> sum(metric) by sales.sales_channel, sales.item.id;

auto return_qty <- sum(sales.return_quantity) by sales.sales_channel, sales.item.id;
auto sale_qty <- sum(sales.quantity) by sales.sales_channel, sales.item.id;
auto return_amt <- sum(sales.return_amount) by sales.sales_channel, sales.item.id;
auto sale_paid <- sum(sales.net_paid) by sales.sales_channel, sales.item.id;
auto return_rank <- rank(sales.item.id) over (partition by sales.sales_channel order by return_ratio asc);
auto currency_rank <- rank(sales.item.id) over (partition by sales.sales_channel order by currency_ratio asc);
auto channel_label <- case
    when sales.sales_channel = 'WEB' then 'web'
    when sales.sales_channel = 'CATALOG' then 'catalog'
    when sales.sales_channel = 'STORE' then 'store'
    else null
end;
auto return_ratio <- return_qty::numeric(15,4) / sale_qty::numeric(15,4);
auto currency_ratio <- return_amt::numeric(15,4) / sale_paid::numeric(15,4);

where
    sales.return_amount > 10000
    and sales.net_profit > 1
    and sales.net_paid > 0
    and sales.quantity > 0
    and sales.date.year = 2001
    and sales.date.month_of_year = 12
select
    channel_label as channel,
    sales.item.id as item,
    return_ratio,
    return_rank,
    currency_rank,
having
    return_rank <= 10 or currency_rank <= 10

order by
    channel asc nulls first,
    return_rank asc nulls first,
    currency_rank asc nulls first,
    item asc nulls first
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
    "sales_catalog_returns_unified"."CR_ITEM_SK" as "sales_item_id",
    "sales_catalog_returns_unified"."CR_ORDER_NUMBER" as "sales_order_id",
    "sales_catalog_returns_unified"."CR_RETURN_AMOUNT" as "sales_return_amount",
    "sales_catalog_returns_unified"."CR_RETURN_QUANTITY" as "sales_return_quantity",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_returns" as "sales_catalog_returns_unified"
UNION ALL
SELECT
    "sales_store_returns_unified"."SR_ITEM_SK" as "sales_item_id",
    "sales_store_returns_unified"."SR_TICKET_NUMBER" as "sales_order_id",
    "sales_store_returns_unified"."SR_RETURN_AMT" as "sales_return_amount",
    "sales_store_returns_unified"."SR_RETURN_QUANTITY" as "sales_return_quantity",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_returns" as "sales_store_returns_unified"
UNION ALL
SELECT
    "sales_web_returns_unified"."WR_ITEM_SK" as "sales_item_id",
    "sales_web_returns_unified"."WR_ORDER_NUMBER" as "sales_order_id",
    "sales_web_returns_unified"."WR_RETURN_AMT" as "sales_return_amount",
    "sales_web_returns_unified"."WR_RETURN_QUANTITY" as "sales_return_quantity",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_returns" as "sales_web_returns_unified"),
abundant as (
SELECT
    "sales_catalog_sales_unified"."CS_ITEM_SK" as "sales_item_id",
    "sales_catalog_sales_unified"."CS_NET_PAID" as "sales_net_paid",
    "sales_catalog_sales_unified"."CS_ORDER_NUMBER" as "sales_order_id",
    "sales_catalog_sales_unified"."CS_QUANTITY" as "sales_quantity",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_catalog_sales_unified"."CS_NET_PROFIT" > 1 and "sales_catalog_sales_unified"."CS_NET_PAID" > 0 and "sales_catalog_sales_unified"."CS_QUANTITY" > 0 and "sales_date_date"."D_YEAR" = 2001 and "sales_date_date"."D_MOY" = 12

UNION ALL
SELECT
    "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_id",
    "sales_store_sales_unified"."SS_NET_PAID" as "sales_net_paid",
    "sales_store_sales_unified"."SS_TICKET_NUMBER" as "sales_order_id",
    "sales_store_sales_unified"."SS_QUANTITY" as "sales_quantity",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_store_sales_unified"."SS_NET_PROFIT" > 1 and "sales_store_sales_unified"."SS_NET_PAID" > 0 and "sales_store_sales_unified"."SS_QUANTITY" > 0 and "sales_date_date"."D_YEAR" = 2001 and "sales_date_date"."D_MOY" = 12

UNION ALL
SELECT
    "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_id",
    "sales_web_sales_unified"."WS_NET_PAID" as "sales_net_paid",
    "sales_web_sales_unified"."WS_ORDER_NUMBER" as "sales_order_id",
    "sales_web_sales_unified"."WS_QUANTITY" as "sales_quantity",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_web_sales_unified"."WS_NET_PROFIT" > 1 and "sales_web_sales_unified"."WS_NET_PAID" > 0 and "sales_web_sales_unified"."WS_QUANTITY" > 0 and "sales_date_date"."D_YEAR" = 2001 and "sales_date_date"."D_MOY" = 12
),
yummy as (
SELECT
    "abundant"."sales_item_id" as "sales_item_id",
    "abundant"."sales_sales_channel" as "sales_sales_channel",
    cast(sum("cheerful"."sales_return_amount") as numeric(15,4)) / cast(sum("abundant"."sales_net_paid") as numeric(15,4)) as "currency_ratio",
    cast(sum("cheerful"."sales_return_quantity") as numeric(15,4)) / cast(sum("abundant"."sales_quantity") as numeric(15,4)) as "return_ratio"
FROM
    "abundant"
    INNER JOIN "cheerful" on "abundant"."sales_item_id" = "cheerful"."sales_item_id" AND "abundant"."sales_order_id" = "cheerful"."sales_order_id" AND "abundant"."sales_sales_channel" = "cheerful"."sales_sales_channel"
WHERE
    "cheerful"."sales_return_amount" > 10000

GROUP BY
    1,
    2),
vacuous as (
SELECT
    "yummy"."return_ratio" as "return_ratio",
    "yummy"."sales_item_id" as "sales_item_id",
    CASE
	WHEN "yummy"."sales_sales_channel" = 'WEB' THEN 'web'
	WHEN "yummy"."sales_sales_channel" = 'CATALOG' THEN 'catalog'
	WHEN "yummy"."sales_sales_channel" = 'STORE' THEN 'store'
	ELSE null
	END as "channel_label",
    rank() over (partition by "yummy"."sales_sales_channel" order by "yummy"."currency_ratio" asc ) as "currency_rank",
    rank() over (partition by "yummy"."sales_sales_channel" order by "yummy"."return_ratio" asc ) as "return_rank"
FROM
    "yummy")
SELECT
    "vacuous"."channel_label" as "channel",
    "vacuous"."sales_item_id" as "item",
    "vacuous"."return_ratio" as "return_ratio",
    "vacuous"."return_rank" as "return_rank",
    "vacuous"."currency_rank" as "currency_rank"
FROM
    "vacuous"
WHERE
    "vacuous"."return_rank" <= 10 or "vacuous"."currency_rank" <= 10

GROUP BY
    1,
    2,
    3,
    4,
    5
ORDER BY 
    "channel" asc nulls first,
    "vacuous"."return_rank" asc nulls first,
    "vacuous"."currency_rank" asc nulls first,
    "item" asc nulls first
LIMIT (100)
```

## v4 generation error

```
Traceback (most recent call last):
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 210, in generate_v4_sql
    sql = compile_sql(info, build_env, build_stmt)
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4.py", line 541, in compile_sql
    node.rebuild_cache()
    ~~~~~~~~~~~~~~~~~~^^
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\nodes\base_node.py", line 440, in rebuild_cache
    return self.resolve()
           ~~~~~~~~~~~~^^
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\nodes\base_node.py", line 447, in resolve
    qds = self._resolve()
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\nodes\select_node_v2.py", line 188, in _resolve
    return super()._resolve()
           ~~~~~~~~~~~~~~~~^^
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\nodes\base_node.py", line 406, in _resolve
    p.resolve() for p in self.parents
    ~~~~~~~~~^^
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\nodes\base_node.py", line 447, in resolve
    qds = self._resolve()
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\nodes\merge_node.py", line 359, in _resolve
    joins: List[BaseJoin | UnnestJoin] = self.generate_joins(
                                         ~~~~~~~~~~~~~~~~~~~^
        join_candidates, final_joins, raw_pregrain, grain, self.environment
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    )
    ^
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\nodes\merge_node.py", line 243, in generate_joins
    joins = get_node_joins(dataset_list, environment=environment)
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\join_resolution.py", line 696, in get_node_joins
    right=resolve_instantiated_concept(
          ~~~~~~~~~~~~~~~~~~~~~~~~~~~~^
        concept_map[concept], ds_node_map[j.right]
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ),
    ^
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\join_resolution.py", line 508, in resolve_instantiated_concept
    raise SyntaxError(
    ...<3 lines>...
    )
SyntaxError: Could not find sales.item.id in sales.catalog_returns_unified_at_sales_item_id_sales_order_id_sales_sales_channel_union_sales.store_returns_unified_at_sales_item_id_sales_order_id_sales_sales_channel_union_sales.web_returns_unified_at_sales_item_id_sales_order_id_sales_sales_channel_unioned_join_sales.catalog_sales_unified_at_sales_item_id_sales_order_id_sales_sales_channel_union_sales.store_sales_unified_at_sales_item_id_sales_order_id_sales_sales_channel_union_sales.web_sales_unified_at_sales_item_id_sales_order_id_sales_sales_channel_unioned_join_sales.date.date_at_sales_date_id_at_sales_item_id_sales_order_id_sales_sales_channel_filtered_by_2844165004558022_at_local_item output ['local.item'], acceptable synonyms set()
```
