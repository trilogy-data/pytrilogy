# Query 23

**Status:** `gen_fail`

| Stage | Result |
| --- | --- |
| v4 SQL generation | FAILED |
| reference execution | OK (4 rows) |

## Result comparison

_at least one side did not produce rows._

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 0 | 0 | — |
| reference | 7652 | 178 | 565.50 ms |

## Preql

```
import unified_sales as sales;

# frequent_ss_items: items with at least one (truncated_desc, item.id, sold_date) combo having > 4 store_sales rows in 2000-2003
auto sales.item.desc_truncated <- substring(sales.item.desc, 1, 30);
auto customer_total_in_window <- sum(sales.quantity * sales.sales_price) by sales.customer.id;

rowset frequent_items <- where
    sales.sales_channel = 'STORE' and sales.date.year in (2000, 2001, 2002, 2003)
select
    sales.item.id as frequent_item_id,
    --ss_combo_count,
having
    ss_combo_count > 4

;

# max_store_sales: scalar max over customers of total ss_qty*ss_price for years 2000-2003
auto customer_total_overall <- sum(sales.quantity * sales.sales_price) by sales.customer.id;

rowset max_total <- where
    sales.customer.id is not null
    and sales.sales_channel = 'STORE'
    and sales.date.year in (2000, 2001, 2002, 2003)
select
    max(customer_total_in_window) as cmax,
;

# best_ss_customer: customers whose lifetime ss_qty*ss_price > 50% of cmax (no date filter)
auto ss_combo_count <- count(sales.order_id) by sales.item.desc_truncated, sales.item.id, sales.date.date;

rowset best_customers <- where
    sales.customer.id is not null and sales.sales_channel = 'STORE'
select
    sales.customer.id as best_customer_id,
    --customer_total_overall,
    --max_total.cmax,
having
    customer_total_overall > 0.5 * max_total.cmax

;

# Final: sum (qty * list_price) by (last, first, channel) for Feb 2000, items in frequent, customers in best
where
    sales.date.year = 2000
    and sales.date.month_of_year = 2
    and sales.item.id in frequent_items.frequent_item_id
    and sales.customer.id in best_customers.best_customer_id
select
    sales.customer.last_name as c_last_name,
    sales.customer.first_name as c_first_name,
    sum((sales.quantity * sales.list_price) ? sales.sales_channel in ('WEB', 'CATALOG')) as sales_total,
having
    sales_total > 0

order by
    c_last_name asc nulls first,
    c_first_name asc nulls first,
    sales_total asc nulls first
limit 100
;
```

## v4 generated SQL

_v4 did not produce SQL._

## Reference SQL (zquery log)

```sql
WITH 
concerned as (
SELECT
     'STORE'  as "sales_sales_channel",
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "sales_customer_id",
    "sales_store_sales_unified"."SS_QUANTITY" as "sales_quantity",
    "sales_store_sales_unified"."SS_SALES_PRICE" as "sales_sales_price",
    "sales_store_sales_unified"."SS_SOLD_DATE_SK" as "sales_date_id"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
WHERE
    "sales_store_sales_unified"."SS_CUSTOMER_SK" is not null
),
questionable as (
SELECT
    "sales_date_date"."D_DATE_SK" as "sales_date_id",
    cast("sales_date_date"."D_DATE" as date) as "sales_date_date"
FROM
    "memory"."date_dim" as "sales_date_date"
WHERE
    "sales_date_date"."D_YEAR" in (2000,2001,2002,2003)
),
sweltering as (
SELECT
    "concerned"."sales_customer_id" as "_best_customers_best_customer_id",
    sum("concerned"."sales_quantity" * "concerned"."sales_sales_price") as "customer_total_overall"
FROM
    "concerned"
GROUP BY
    1),
young as (
SELECT
    sum("concerned"."sales_quantity" * "concerned"."sales_sales_price") as "customer_total_in_window"
FROM
    "concerned"
    INNER JOIN "questionable" on "concerned"."sales_date_id" = "questionable"."sales_date_id"
WHERE
    "concerned"."sales_sales_channel" = 'STORE'

GROUP BY
    "concerned"."sales_customer_id"),
uneven as (
SELECT
    "questionable"."sales_date_date" as "sales_date_date",
    "sales_item_items"."I_ITEM_SK" as "sales_item_id",
    "sales_store_sales_unified"."SS_TICKET_NUMBER" as "sales_order_id",
    SUBSTRING("sales_item_items"."I_ITEM_DESC",1,30) as "sales_item_desc_truncated"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "questionable" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "questionable"."sales_date_id"
    INNER JOIN "memory"."item" as "sales_item_items" on "sales_store_sales_unified"."SS_ITEM_SK" = "sales_item_items"."I_ITEM_SK"
WHERE
     'STORE'  = 'STORE'

GROUP BY
    1,
    2,
    3,
    4),
abhorrent as (
SELECT
    max("young"."customer_total_in_window") as "max_total_cmax"
FROM
    "young"),
yummy as (
SELECT
    "uneven"."sales_item_id" as "_frequent_items_frequent_item_id"
FROM
    "uneven"
GROUP BY
    1,
    "uneven"."sales_date_date",
    "uneven"."sales_item_desc_truncated"
HAVING
    count("uneven"."sales_order_id") > 4
),
late as (
SELECT
    "sweltering"."_best_customers_best_customer_id" as "_best_customers_best_customer_id"
FROM
    "sweltering"
    INNER JOIN "abhorrent" on 1=1
WHERE
    "sweltering"."customer_total_overall" > 0.5 * "abhorrent"."max_total_cmax"
),
juicy as (
SELECT
    "yummy"."_frequent_items_frequent_item_id" as "frequent_items_frequent_item_id"
FROM
    "yummy"
GROUP BY
    1),
macho as (
SELECT
    "late"."_best_customers_best_customer_id" as "best_customers_best_customer_id"
FROM
    "late"),
cheerful as (
SELECT
    "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" as "sales_customer_id",
    "sales_catalog_sales_unified"."CS_ITEM_SK" as "sales_item_id",
    "sales_catalog_sales_unified"."CS_LIST_PRICE" as "sales_list_price",
    "sales_catalog_sales_unified"."CS_ORDER_NUMBER" as "sales_order_id",
    "sales_catalog_sales_unified"."CS_QUANTITY" as "sales_quantity",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
    INNER JOIN "memory"."customer" as "sales_customer_customers" on "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" = "sales_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_catalog_sales_unified"."CS_ITEM_SK" in (select juicy."frequent_items_frequent_item_id" from juicy where juicy."frequent_items_frequent_item_id" is not null) and "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" in (select macho."best_customers_best_customer_id" from macho where macho."best_customers_best_customer_id" is not null) and "sales_date_date"."D_YEAR" = 2000 and "sales_date_date"."D_MOY" = 2

UNION ALL
SELECT
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "sales_customer_id",
    "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_id",
    "sales_store_sales_unified"."SS_LIST_PRICE" as "sales_list_price",
    "sales_store_sales_unified"."SS_TICKET_NUMBER" as "sales_order_id",
    "sales_store_sales_unified"."SS_QUANTITY" as "sales_quantity",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "memory"."customer" as "sales_customer_customers" on "sales_store_sales_unified"."SS_CUSTOMER_SK" = "sales_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_store_sales_unified"."SS_ITEM_SK" in (select juicy."frequent_items_frequent_item_id" from juicy where juicy."frequent_items_frequent_item_id" is not null) and "sales_store_sales_unified"."SS_CUSTOMER_SK" in (select macho."best_customers_best_customer_id" from macho where macho."best_customers_best_customer_id" is not null) and "sales_date_date"."D_YEAR" = 2000 and "sales_date_date"."D_MOY" = 2

UNION ALL
SELECT
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" as "sales_customer_id",
    "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_id",
    "sales_web_sales_unified"."WS_LIST_PRICE" as "sales_list_price",
    "sales_web_sales_unified"."WS_ORDER_NUMBER" as "sales_order_id",
    "sales_web_sales_unified"."WS_QUANTITY" as "sales_quantity",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "memory"."customer" as "sales_customer_customers" on "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" = "sales_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_web_sales_unified"."WS_ITEM_SK" in (select juicy."frequent_items_frequent_item_id" from juicy where juicy."frequent_items_frequent_item_id" is not null) and "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" in (select macho."best_customers_best_customer_id" from macho where macho."best_customers_best_customer_id" is not null) and "sales_date_date"."D_YEAR" = 2000 and "sales_date_date"."D_MOY" = 2
),
scrawny as (
SELECT
    "cheerful"."sales_quantity" * "cheerful"."sales_list_price" as "_virt_func_multiply_8507033399516423",
    "cheerful"."sales_sales_channel" as "sales_sales_channel",
    "sales_customer_customers"."C_FIRST_NAME" as "sales_customer_first_name",
    "sales_customer_customers"."C_LAST_NAME" as "sales_customer_last_name"
FROM
    "cheerful"
    LEFT OUTER JOIN "memory"."customer" as "sales_customer_customers" on "cheerful"."sales_customer_id" = "sales_customer_customers"."C_CUSTOMER_SK"
WHERE
    "cheerful"."sales_item_id" in (select juicy."frequent_items_frequent_item_id" from juicy where juicy."frequent_items_frequent_item_id" is not null)

GROUP BY
    1,
    2,
    3,
    4,
    "cheerful"."sales_item_id",
    "cheerful"."sales_order_id")
SELECT
    "scrawny"."sales_customer_last_name" as "c_last_name",
    "scrawny"."sales_customer_first_name" as "c_first_name",
    sum(CASE WHEN "scrawny"."sales_sales_channel" in ('WEB','CATALOG') THEN "scrawny"."_virt_func_multiply_8507033399516423" ELSE NULL END) as "sales_total"
FROM
    "scrawny"
GROUP BY
    1,
    2
HAVING
    "sales_total" > 0

ORDER BY 
    "c_last_name" asc nulls first,
    "c_first_name" asc nulls first,
    "sales_total" asc nulls first
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
SyntaxError: Could not find sales.customer.last_name in sales.catalog_sales_unified_at_sales_item_id_sales_order_id_sales_sales_channel_union_sales.store_sales_unified_at_sales_item_id_sales_order_id_sales_sales_channel_union_sales.web_sales_unified_at_sales_item_id_sales_order_id_sales_sales_channel_unioned_join_sales.customer.customers_at_sales_customer_id_join_sales.date.date_at_sales_date_id_join_sales.date.date_at_sales_date_id_filtered_by_1381268244931442_join_sales.item.items_at_sales_item_id_join_sales.store_sales_unified_at_sales_item_id_sales_order_id_sales_sales_channel_grouped_by_sales.date.date_sales.item.desc_truncated_sales.item.id_sales.order_id_at_sales_date_date_sales_item_id_sales_order_id_filtered_by_9080651742928098_grouped_by_local._frequent_items_frequent_item_id_sales.date.date_sales.item.desc_truncated_sales.item.id_at_sales_date_date_sales_item_id_at_frequent_items_frequent_item_id_filtered_by_578530982766841_grouped_by_frequent_items.frequent_item_id_at_frequent_items_frequent_item_id_join_sales.date.date_at_sales_date_id_filtered_by_1381268244931442_join_sales.store_sales_unified_at_sales_item_id_sales_order_id_sales_sales_channel_filtered_by_3403810150931339_at_sales_item_id_sales_order_id_sales_sales_channel_filtered_by_794436733259538_grouped_by_sales.customer.id_at_sales_customer_id_grouped_by__at_abstract_join_sales.store_sales_unified_at_sales_item_id_sales_order_id_sales_sales_channel_filtered_by_3403810150931339_grouped_by_local._best_customers_best_customer_id_sales.customer.id_at_sales_customer_id_at_sales_customer_id_at_best_customers_best_customer_id_filtered_by_2484749007924451_grouped_by_sales.customer.first_name_sales.customer.last_name_sales.item.id_sales.list_price_sales.order_id_sales.quantity_sales.sales_channel_at_sales_customer_first_name_sales_customer_last_name_sales_item_id_sales_order_id_sales_sales_channel_filtered_by_1902506739459039_at_local_c_first_name_local_c_last_name output ['local.c_first_name', 'local.c_last_name'], acceptable synonyms set()
```
