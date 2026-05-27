# Query 29

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
| reference | 3694 | 60 | 48.79 ms |

## Preql

```
import store_sales as store_sales;
import catalog_sales as catalog_sales;

merge catalog_sales.bill_customer.id into store_sales.return_customer.id;
merge catalog_sales.item.id into store_sales.item.id;

# RC-A correlated-grain shape (2026-05-20). Reference q29 has ONE source
# grain â€” the joined rowset of store_sales â‹ˆ store_returns â‹ˆ
# catalog_sales on (customer, item) â€” at effective grain
# (ss.ticket_number, item.id, catalog_sales.order_number). The
# generator correctly refuses to co-group additive aggregates whose
# declared per-fact grains differ; we give it a single source grain
# explicitly by projecting all three correlation keys + dim cols + the
# raw measures in one rowset, then SUM out at the requested output
# grain. Selecting `catalog_sales.order_number` alongside
# `store_sales.ticket_number` forces the cross-fact join and pins the
# grain.
rowset correlated <- where
    store_sales.date.month_of_year = 9
    and store_sales.date.year = 1999
    and store_sales.return_date.month_of_year between 9 and 12
    and store_sales.return_date.year = 1999
    and catalog_sales.date.year in (1999, 2000, 2001)
    and catalog_sales.quantity > 0
    and store_sales.is_returned
    and store_sales.customer.id = store_sales.return_customer.id
select
    store_sales.ticket_number,
    store_sales.item.id,
    catalog_sales.order_number,
    store_sales.item.name,
    store_sales.item.desc,
    store_sales.store.text_id,
    store_sales.store.name,
    store_sales.quantity,
    store_sales.return_quantity,
    catalog_sales.quantity,
;

select
    correlated.store_sales.item.name as store_sales_item_name,
    correlated.store_sales.item.desc as store_sales_item_desc,
    correlated.store_sales.store.text_id as store_sales_store_text_id,
    correlated.store_sales.store.name as store_name,
    sum(correlated.store_sales.quantity) as store_sales_quantity,
    sum(correlated.store_sales.return_quantity) as store_returns_quantity,
    sum(correlated.catalog_sales.quantity) as catalog_sales_quantity,
having
    catalog_sales_quantity > 0

order by
    store_sales_item_name asc,
    store_sales_item_desc asc,
    store_sales_store_text_id asc,
    store_name asc
limit 100
;
```

## v4 generated SQL

_v4 did not produce SQL._

## Reference SQL (zquery log)

```sql
WITH 
uneven as (
SELECT
    "store_sales_item_items"."I_ITEM_DESC" as "correlated_store_sales_item_desc",
    "store_sales_item_items"."I_ITEM_ID" as "correlated_store_sales_item_name",
    "store_sales_store_store"."S_STORE_ID" as "correlated_store_sales_store_text_id",
    "store_sales_store_store"."S_STORE_NAME" as "correlated_store_sales_store_name",
    sum("catalog_sales_catalog_sales"."CS_QUANTITY") as "catalog_sales_quantity",
    sum("store_sales_store_returns"."SR_RETURN_QUANTITY") as "store_returns_quantity",
    sum("store_sales_store_sales"."SS_QUANTITY") as "store_sales_quantity"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
    LEFT OUTER JOIN "memory"."store" as "store_sales_store_store" on "store_sales_store_sales"."SS_STORE_SK" = "store_sales_store_store"."S_STORE_SK"
    INNER JOIN "memory"."store_returns" as "store_sales_store_returns" on "store_sales_store_sales"."SS_ITEM_SK" = "store_sales_store_returns"."SR_ITEM_SK" AND "store_sales_store_sales"."SS_TICKET_NUMBER" = "store_sales_store_returns"."SR_TICKET_NUMBER"
    INNER JOIN "memory"."date_dim" as "store_sales_return_date_date" on "store_sales_store_returns"."SR_RETURNED_DATE_SK" = "store_sales_return_date_date"."D_DATE_SK"
    INNER JOIN "memory"."catalog_sales" as "catalog_sales_catalog_sales" on "store_sales_store_returns"."SR_CUSTOMER_SK" = "catalog_sales_catalog_sales"."CS_BILL_CUSTOMER_SK" AND "store_sales_store_sales"."SS_ITEM_SK" = "catalog_sales_catalog_sales"."CS_ITEM_SK"
    INNER JOIN "memory"."date_dim" as "catalog_sales_date_date" on "catalog_sales_catalog_sales"."CS_SOLD_DATE_SK" = "catalog_sales_date_date"."D_DATE_SK"
    LEFT OUTER JOIN "memory"."item" as "store_sales_item_items" on "catalog_sales_catalog_sales"."CS_ITEM_SK" = "store_sales_item_items"."I_ITEM_SK"
WHERE
    "store_sales_date_date"."D_MOY" = 9 and "store_sales_date_date"."D_YEAR" = 1999 and "store_sales_return_date_date"."D_MOY" BETWEEN 9 AND 12 and "store_sales_return_date_date"."D_YEAR" = 1999 and "catalog_sales_date_date"."D_YEAR" in (1999,2000,2001) and "catalog_sales_catalog_sales"."CS_QUANTITY" > 0 and SR_RETURN_TIME_SK IS NOT NULL and "store_sales_store_sales"."SS_CUSTOMER_SK" = "catalog_sales_catalog_sales"."CS_BILL_CUSTOMER_SK"

GROUP BY
    1,
    2,
    3,
    4
HAVING
    "catalog_sales_quantity" > 0
),
juicy as (
SELECT
    "uneven"."catalog_sales_quantity" as "catalog_sales_quantity",
    "uneven"."correlated_store_sales_item_desc" as "store_sales_item_desc",
    "uneven"."correlated_store_sales_item_name" as "store_sales_item_name",
    "uneven"."correlated_store_sales_store_name" as "store_name",
    "uneven"."correlated_store_sales_store_text_id" as "store_sales_store_text_id",
    "uneven"."store_returns_quantity" as "store_returns_quantity",
    "uneven"."store_sales_quantity" as "store_sales_quantity"
FROM
    "uneven")
SELECT
    "juicy"."store_sales_item_name" as "store_sales_item_name",
    "juicy"."store_sales_item_desc" as "store_sales_item_desc",
    "juicy"."store_sales_store_text_id" as "store_sales_store_text_id",
    "juicy"."store_name" as "store_name",
    "juicy"."store_sales_quantity" as "store_sales_quantity",
    "juicy"."store_returns_quantity" as "store_returns_quantity",
    "juicy"."catalog_sales_quantity" as "catalog_sales_quantity"
FROM
    "juicy"
WHERE
    "juicy"."catalog_sales_quantity" > 0

ORDER BY 
    "juicy"."store_sales_item_name" asc,
    "juicy"."store_sales_item_desc" asc,
    "juicy"."store_sales_store_text_id" asc,
    "juicy"."store_name" asc
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
SyntaxError: Could not find correlated.store_sales.item.name in catalog_sales.catalog_sales_at_catalog_sales_order_number_store_sales_item_id_join_catalog_sales.date.date_at_catalog_sales_date_id_join_store_sales.date.date_at_store_sales_date_id_join_store_sales.item.items_at_store_sales_item_id_join_store_sales.return_date.date_at_store_sales_return_date_id_join_store_sales.store.store_at_store_sales_store_id_join_store_sales.store_returns_at_store_sales_item_id_store_sales_ticket_number_join_store_sales.store_sales_at_store_sales_item_id_store_sales_ticket_number_at_correlated_catalog_sales_order_number_correlated_store_sales_item_id_correlated_store_sales_store_name_correlated_store_sales_store_text_id_correlated_store_sales_ticket_number_filtered_by_8306884892883996_at_local_store_name_local_store_sales_item_desc_local_store_sales_item_name_local_store_sales_store_text_id output ['local.store_name', 'local.store_sales_item_desc', 'local.store_sales_item_name', 'local.store_sales_store_text_id'], acceptable synonyms set()
```
