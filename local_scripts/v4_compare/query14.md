# Query 14

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
| reference | 6613 | 165 | 389.38 ms |

## Preql

```
import unified_sales as sales;

# Composite key for (brand_id, class_id, category_id) tuples â€” encoded so a
# single-column `in` filter can carry membership downstream.
auto tuple_key <- concat(
    sales.item.brand_id::string,
    '|',
    sales.item.class_id::string,
    '|',
    sales.item.category_id::string
);
auto fact_row_one <- count(sales.order_id) by sales.sales_channel, sales.order_id, sales.item.id;

# cross_tuples: tuples sold in all 3 channels during 1999-2001. Equivalent to
# the reference's 3-way INTERSECT on (i_brand_id, i_class_id, i_category_id)
# across store/catalog/web sales.
rowset cross_tuples <- where
    sales.date.year between 1999 and 2001
select
    tuple_key as ci_tuple_key,
    --cross_channel_count,
having
    cross_channel_count = 3

;

# avg_sales: scalar avg(quantity * list_price) over all 3 channels for 1999-2001.
rowset avg_sales <- where
    sales.date.year between 1999 and 2001
select
    avg(sales.quantity * sales.list_price) as average_sales,
;

# One per fact row (unified_sales grain is (sales_channel, order_id, item.id)).
auto channel_label <- case
    when sales.sales_channel = 'STORE' then 'store'
    when sales.sales_channel = 'CATALOG' then 'catalog'
    when sales.sales_channel = 'WEB' then 'web'
    else null
end;

# Map unified_sales' uppercase channel codes to the reference's lowercase labels.
auto cross_channel_count <- count_distinct(sales.sales_channel) by tuple_key;

# Per (channel, brand_id, class_id, category_id) aggregates for Nov 2001, items in
# cross_tuples, HAVING sum(quantity*list_price) > avg_sales.
rowset l0_filtered <- where
    sales.date.year = 2001
    and sales.date.month_of_year = 11
    and tuple_key in cross_tuples.ci_tuple_key
select
    channel_label as channel_l0,
    sales.item.brand_id as brand_l0,
    sales.item.class_id as class_l0,
    sales.item.category_id as category_l0,
    sum(sales.quantity * sales.list_price) as bucket_sum_l0,
    sum(fact_row_one) as bucket_count_l0,
    --avg_sales.average_sales,
having
    bucket_sum_l0 > avg_sales.average_sales

;

# Final ROLLUP(channel, brand_id, class_id, category_id) over the per-channel L0.
select
    l0_filtered.channel_l0 as channel,
    l0_filtered.brand_l0 as i_brand_id,
    l0_filtered.class_l0 as i_class_id,
    l0_filtered.category_l0 as i_category_id,
    sum(l0_filtered.bucket_sum_l0)
            by rollup l0_filtered.channel_l0, l0_filtered.brand_l0, l0_filtered.class_l0, l0_filtered.category_l0 as sum_sales,
    sum(l0_filtered.bucket_count_l0)
            by rollup l0_filtered.channel_l0, l0_filtered.brand_l0, l0_filtered.class_l0, l0_filtered.category_l0 as sum_number_sales,
order by
    channel asc nulls first,
    i_brand_id asc nulls first,
    i_class_id asc nulls first,
    i_category_id asc nulls first
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
    "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "sales_date_id",
    "sales_catalog_sales_unified"."CS_ITEM_SK" as "sales_item_id",
    "sales_catalog_sales_unified"."CS_LIST_PRICE" as "sales_list_price",
    "sales_catalog_sales_unified"."CS_ORDER_NUMBER" as "sales_order_id",
    "sales_catalog_sales_unified"."CS_QUANTITY" as "sales_quantity",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
UNION ALL
SELECT
    "sales_store_sales_unified"."SS_SOLD_DATE_SK" as "sales_date_id",
    "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_id",
    "sales_store_sales_unified"."SS_LIST_PRICE" as "sales_list_price",
    "sales_store_sales_unified"."SS_TICKET_NUMBER" as "sales_order_id",
    "sales_store_sales_unified"."SS_QUANTITY" as "sales_quantity",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
UNION ALL
SELECT
    "sales_web_sales_unified"."WS_SOLD_DATE_SK" as "sales_date_id",
    "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_id",
    "sales_web_sales_unified"."WS_LIST_PRICE" as "sales_list_price",
    "sales_web_sales_unified"."WS_ORDER_NUMBER" as "sales_order_id",
    "sales_web_sales_unified"."WS_QUANTITY" as "sales_quantity",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"),
young as (
SELECT
    avg("cheerful"."sales_quantity" * "cheerful"."sales_list_price") as "avg_sales_average_sales"
FROM
    "cheerful"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "cheerful"."sales_date_id" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_date_date"."D_YEAR" BETWEEN 1999 AND 2001
),
questionable as (
SELECT
    "cheerful"."sales_sales_channel" as "sales_sales_channel",
    (cast("sales_item_items"."I_BRAND_ID" as string) || '|' || cast("sales_item_items"."I_CLASS_ID" as string) || '|' || cast("sales_item_items"."I_CATEGORY_ID" as string)) as "tuple_key"
FROM
    "cheerful"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "cheerful"."sales_date_id" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "sales_item_items" on "cheerful"."sales_item_id" = "sales_item_items"."I_ITEM_SK"
WHERE
    "sales_date_date"."D_YEAR" BETWEEN 1999 AND 2001

GROUP BY
    1,
    2),
abundant as (
SELECT
    "questionable"."tuple_key" as "_cross_tuples_ci_tuple_key"
FROM
    "questionable"
GROUP BY
    1
HAVING
    count(distinct "questionable"."sales_sales_channel") = 3
),
uneven as (
SELECT
    "abundant"."_cross_tuples_ci_tuple_key" as "cross_tuples_ci_tuple_key"
FROM
    "abundant"),
yummy as (
SELECT
    "cheerful"."sales_item_id" as "sales_item_id",
    "cheerful"."sales_list_price" as "sales_list_price",
    "cheerful"."sales_order_id" as "sales_order_id",
    "cheerful"."sales_quantity" as "sales_quantity",
    "cheerful"."sales_sales_channel" as "sales_sales_channel",
    "sales_item_items"."I_BRAND_ID" as "sales_item_brand_id",
    "sales_item_items"."I_CATEGORY_ID" as "sales_item_category_id",
    "sales_item_items"."I_CLASS_ID" as "sales_item_class_id"
FROM
    "cheerful"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "cheerful"."sales_date_id" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "sales_item_items" on "cheerful"."sales_item_id" = "sales_item_items"."I_ITEM_SK"
WHERE
    "sales_date_date"."D_YEAR" = 2001 and "sales_date_date"."D_MOY" = 11 and (cast("sales_item_items"."I_BRAND_ID" as string) || '|' || cast("sales_item_items"."I_CLASS_ID" as string) || '|' || cast("sales_item_items"."I_CATEGORY_ID" as string)) in (select uneven."cross_tuples_ci_tuple_key" from uneven where uneven."cross_tuples_ci_tuple_key" is not null)

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8),
juicy as (
SELECT
    "yummy"."sales_item_id" as "sales_item_id",
    "yummy"."sales_order_id" as "sales_order_id",
    "yummy"."sales_sales_channel" as "sales_sales_channel",
    CASE WHEN "yummy"."sales_order_id" IS NOT NULL THEN 1 ELSE 0 END as "fact_row_one"
FROM
    "yummy"),
vacuous as (
SELECT
    "yummy"."sales_item_brand_id" as "_l0_filtered_brand_l0",
    "yummy"."sales_item_category_id" as "_l0_filtered_category_l0",
    "yummy"."sales_item_class_id" as "_l0_filtered_class_l0",
    CASE
	WHEN "yummy"."sales_sales_channel" = 'STORE' THEN 'store'
	WHEN "yummy"."sales_sales_channel" = 'CATALOG' THEN 'catalog'
	WHEN "yummy"."sales_sales_channel" = 'WEB' THEN 'web'
	ELSE null
	END as "_l0_filtered_channel_l0",
    sum("juicy"."fact_row_one") as "_l0_filtered_bucket_count_l0",
    sum("yummy"."sales_quantity" * "yummy"."sales_list_price") as "_l0_filtered_bucket_sum_l0"
FROM
    "juicy"
    INNER JOIN "yummy" on "juicy"."sales_item_id" = "yummy"."sales_item_id" AND "juicy"."sales_order_id" = "yummy"."sales_order_id" AND "juicy"."sales_sales_channel" = "yummy"."sales_sales_channel"
GROUP BY
    1,
    2,
    3,
    4),
abhorrent as (
SELECT
    "vacuous"."_l0_filtered_brand_l0" as "_l0_filtered_brand_l0",
    "vacuous"."_l0_filtered_bucket_count_l0" as "_l0_filtered_bucket_count_l0",
    "vacuous"."_l0_filtered_bucket_sum_l0" as "_l0_filtered_bucket_sum_l0",
    "vacuous"."_l0_filtered_category_l0" as "_l0_filtered_category_l0",
    "vacuous"."_l0_filtered_channel_l0" as "_l0_filtered_channel_l0",
    "vacuous"."_l0_filtered_class_l0" as "_l0_filtered_class_l0"
FROM
    "young"
    INNER JOIN "vacuous" on 1=1
WHERE
    "vacuous"."_l0_filtered_bucket_sum_l0" > "young"."avg_sales_average_sales"
),
sweltering as (
SELECT
    "abhorrent"."_l0_filtered_brand_l0" as "l0_filtered_brand_l0",
    "abhorrent"."_l0_filtered_bucket_count_l0" as "l0_filtered_bucket_count_l0",
    "abhorrent"."_l0_filtered_bucket_sum_l0" as "l0_filtered_bucket_sum_l0",
    "abhorrent"."_l0_filtered_category_l0" as "l0_filtered_category_l0",
    "abhorrent"."_l0_filtered_channel_l0" as "l0_filtered_channel_l0",
    "abhorrent"."_l0_filtered_class_l0" as "l0_filtered_class_l0"
FROM
    "abhorrent")
SELECT
    "sweltering"."l0_filtered_channel_l0" as "channel",
    "sweltering"."l0_filtered_brand_l0" as "i_brand_id",
    "sweltering"."l0_filtered_class_l0" as "i_class_id",
    "sweltering"."l0_filtered_category_l0" as "i_category_id",
    sum("sweltering"."l0_filtered_bucket_sum_l0") as "sum_sales",
    sum("sweltering"."l0_filtered_bucket_count_l0") as "sum_number_sales"
FROM
    "sweltering"
GROUP BY
    ROLLUP (1, 2, 3, 4)
ORDER BY 
    "channel" asc nulls first,
    "i_brand_id" asc nulls first,
    "i_class_id" asc nulls first,
    "i_category_id" asc nulls first
LIMIT (100)
```

## v4 generation error

```
Traceback (most recent call last):
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 225, in generate_v4_sql
    sql = compile_sql(info, build_env, build_stmt)
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4.py", line 801, in compile_sql
    node.rebuild_cache()
    ~~~~~~~~~~~~~~~~~~^^
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\nodes\base_node.py", line 440, in rebuild_cache
    return self.resolve()
           ~~~~~~~~~~~~^^
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
SyntaxError: Could not find l0_filtered.channel_l0 in sales.catalog_sales_unified_at_sales_item_id_sales_order_id_sales_sales_channel_union_sales.store_sales_unified_at_sales_item_id_sales_order_id_sales_sales_channel_union_sales.web_sales_unified_at_sales_item_id_sales_order_id_sales_sales_channel_unioned_join_sales.catalog_sales_unified_at_sales_item_id_sales_order_id_sales_sales_channel_union_sales.store_sales_unified_at_sales_item_id_sales_order_id_sales_sales_channel_union_sales.web_sales_unified_at_sales_item_id_sales_order_id_sales_sales_channel_unioned_join_sales.date.date_at_sales_date_id_join_sales.item.items_at_sales_item_id_grouped_by_local.tuple_key_sales.sales_channel_at_local_tuple_key_sales_sales_channel_filtered_by_462697851882500_grouped_by_local._cross_tuples_ci_tuple_key_local.tuple_key_at_local_tuple_key_at_cross_tuples_ci_tuple_key_filtered_by_3358484936324238_join_sales.date.date_at_sales_date_id_join_sales.item.items_at_sales_item_id_grouped_by_sales.item.brand_id_sales.item.category_id_sales.item.class_id_sales.item.id_sales.list_price_sales.order_id_sales.quantity_sales.sales_channel_at_sales_item_id_sales_order_id_sales_sales_channel_filtered_by_7565679496992495_join_sales.catalog_sales_unified_at_sales_item_id_sales_order_id_sales_sales_channel_union_sales.store_sales_unified_at_sales_item_id_sales_order_id_sales_sales_channel_union_sales.web_sales_unified_at_sales_item_id_sales_order_id_sales_sales_channel_unioned_join_sales.catalog_sales_unified_at_sales_item_id_sales_order_id_sales_sales_channel_union_sales.store_sales_unified_at_sales_item_id_sales_order_id_sales_sales_channel_union_sales.web_sales_unified_at_sales_item_id_sales_order_id_sales_sales_channel_unioned_join_sales.date.date_at_sales_date_id_join_sales.item.items_at_sales_item_id_grouped_by_local.tuple_key_sales.sales_channel_at_local_tuple_key_sales_sales_channel_filtered_by_462697851882500_grouped_by_local._cross_tuples_ci_tuple_key_local.tuple_key_at_local_tuple_key_at_cross_tuples_ci_tuple_key_filtered_by_3358484936324238_join_sales.date.date_at_sales_date_id_join_sales.item.items_at_sales_item_id_grouped_by_sales.item.brand_id_sales.item.category_id_sales.item.class_id_sales.item.id_sales.list_price_sales.order_id_sales.quantity_sales.sales_channel_at_sales_item_id_sales_order_id_sales_sales_channel_filtered_by_7565679496992495_at_sales_item_id_sales_order_id_sales_sales_channel_at_sales_item_id_sales_order_id_sales_sales_channel_grouped_by_local._l0_filtered_brand_l0_local._l0_filtered_category_l0_local._l0_filtered_channel_l0_local._l0_filtered_class_l0_local.channel_label_sales.item.brand_id_sales.item.category_id_sales.item.class_id_at_local_channel_label_sales_item_brand_id_sales_item_category_id_sales_item_class_id_join_sales.catalog_sales_unified_at_sales_item_id_sales_order_id_sales_sales_channel_union_sales.store_sales_unified_at_sales_item_id_sales_order_id_sales_sales_channel_union_sales.web_sales_unified_at_sales_item_id_sales_order_id_sales_sales_channel_unioned_join_sales.date.date_at_sales_date_id_at_sales_item_id_sales_order_id_sales_sales_channel_filtered_by_462697851882500_grouped_by__at_abstract_at_local_channel_label_sales_item_brand_id_sales_item_category_id_sales_item_class_id_at_l0_filtered_brand_l0_l0_filtered_category_l0_l0_filtered_channel_l0_l0_filtered_class_l0_filtered_by_671402334736040_at_local_channel_local_i_brand_id_local_i_category_id_local_i_class_id output ['local.channel', 'local.i_brand_id', 'local.i_category_id', 'local.i_class_id'], acceptable synonyms set()
```
