# Query 58

**Status:** `gen_fail`

| Stage | Result |
| --- | --- |
| v4 SQL generation | FAILED |
| reference execution | OK (5 rows) |

## Result comparison

_at least one side did not produce rows._

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 0 | 0 | — |
| reference | 5265 | 78 | 100.51 ms |

## Preql

```
import unified_sales as sales;
import date as date;

auto target_week_seq <- date.week_seq ? date._date_string = '2000-01-03';

def channel_item_rev(channel) -> sum(sales.ext_sales_price ? sales.sales_channel = channel) by sales.item.name;

auto ss_item_rev <- sum(sales.ext_sales_price ? sales.sales_channel = 'STORE') by sales.item.name;
auto cs_item_rev <- sum(sales.ext_sales_price ? sales.sales_channel = 'CATALOG') by sales.item.name;
auto ws_item_rev <- sum(sales.ext_sales_price ? sales.sales_channel = 'WEB') by sales.item.name;
auto avg_rev <- (ss_item_rev + cs_item_rev + ws_item_rev) / 3;
auto ss_dev <- ss_item_rev / avg_rev * 100;
auto cs_dev <- cs_item_rev / avg_rev * 100;
auto ws_dev <- ws_item_rev / avg_rev * 100;

where
    sales.date.week_seq in target_week_seq
select
    sales.item.name as item_id,
    ss_item_rev,
    ss_dev,
    cs_item_rev,
    cs_dev,
    ws_item_rev,
    ws_dev,
    avg_rev,
having
    ss_item_rev between 0.9 * cs_item_rev and 1.1 * cs_item_rev
    and ss_item_rev between 0.9 * ws_item_rev and 1.1 * ws_item_rev
    and cs_item_rev between 0.9 * ss_item_rev and 1.1 * ss_item_rev
    and cs_item_rev between 0.9 * ws_item_rev and 1.1 * ws_item_rev
    and ws_item_rev between 0.9 * ss_item_rev and 1.1 * ss_item_rev
    and ws_item_rev between 0.9 * cs_item_rev and 1.1 * cs_item_rev

order by
    item_id asc nulls first,
    ss_item_rev asc nulls first
limit 100
;
```

## v4 generated SQL

_v4 did not produce SQL._

## Reference SQL (zquery log)

```sql
WITH 
quizzical as (
SELECT
    CASE WHEN "date_date"."D_DATE" = '2000-01-03' THEN "date_date"."D_WEEK_SEQ" ELSE NULL END as "target_week_seq"
FROM
    "memory"."date_dim" as "date_date"
GROUP BY
    1),
questionable as (
SELECT
    "sales_catalog_sales_unified"."CS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_catalog_sales_unified"."CS_ITEM_SK" as "sales_item_id",
    "sales_catalog_sales_unified"."CS_ORDER_NUMBER" as "sales_order_id",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_date_date"."D_WEEK_SEQ" in (select quizzical."target_week_seq" from quizzical where quizzical."target_week_seq" is not null)

UNION ALL
SELECT
    "sales_store_sales_unified"."SS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_id",
    "sales_store_sales_unified"."SS_TICKET_NUMBER" as "sales_order_id",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_date_date"."D_WEEK_SEQ" in (select quizzical."target_week_seq" from quizzical where quizzical."target_week_seq" is not null)

UNION ALL
SELECT
    "sales_web_sales_unified"."WS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_id",
    "sales_web_sales_unified"."WS_ORDER_NUMBER" as "sales_order_id",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_date_date"."D_WEEK_SEQ" in (select quizzical."target_week_seq" from quizzical where quizzical."target_week_seq" is not null)
),
yummy as (
SELECT
    "questionable"."sales_ext_sales_price" as "sales_ext_sales_price",
    "questionable"."sales_sales_channel" as "sales_sales_channel",
    "sales_item_items"."I_ITEM_ID" as "sales_item_name"
FROM
    "questionable"
    INNER JOIN "memory"."item" as "sales_item_items" on "questionable"."sales_item_id" = "sales_item_items"."I_ITEM_SK"
GROUP BY
    1,
    2,
    3,
    "questionable"."sales_item_id",
    "questionable"."sales_order_id")
SELECT
    "yummy"."sales_item_name" as "item_id",
    sum(CASE WHEN "yummy"."sales_sales_channel" = 'STORE' THEN "yummy"."sales_ext_sales_price" ELSE NULL END) as "ss_item_rev",
    ( sum(CASE WHEN "yummy"."sales_sales_channel" = 'STORE' THEN "yummy"."sales_ext_sales_price" ELSE NULL END) / ( (( sum(CASE WHEN "yummy"."sales_sales_channel" = 'STORE' THEN "yummy"."sales_ext_sales_price" ELSE NULL END) + sum(CASE WHEN "yummy"."sales_sales_channel" = 'CATALOG' THEN "yummy"."sales_ext_sales_price" ELSE NULL END) ) + sum(CASE WHEN "yummy"."sales_sales_channel" = 'WEB' THEN "yummy"."sales_ext_sales_price" ELSE NULL END)) / 3 ) ) * 100 as "ss_dev",
    sum(CASE WHEN "yummy"."sales_sales_channel" = 'CATALOG' THEN "yummy"."sales_ext_sales_price" ELSE NULL END) as "cs_item_rev",
    ( sum(CASE WHEN "yummy"."sales_sales_channel" = 'CATALOG' THEN "yummy"."sales_ext_sales_price" ELSE NULL END) / ( (( sum(CASE WHEN "yummy"."sales_sales_channel" = 'STORE' THEN "yummy"."sales_ext_sales_price" ELSE NULL END) + sum(CASE WHEN "yummy"."sales_sales_channel" = 'CATALOG' THEN "yummy"."sales_ext_sales_price" ELSE NULL END) ) + sum(CASE WHEN "yummy"."sales_sales_channel" = 'WEB' THEN "yummy"."sales_ext_sales_price" ELSE NULL END)) / 3 ) ) * 100 as "cs_dev",
    sum(CASE WHEN "yummy"."sales_sales_channel" = 'WEB' THEN "yummy"."sales_ext_sales_price" ELSE NULL END) as "ws_item_rev",
    ( sum(CASE WHEN "yummy"."sales_sales_channel" = 'WEB' THEN "yummy"."sales_ext_sales_price" ELSE NULL END) / ( (( sum(CASE WHEN "yummy"."sales_sales_channel" = 'STORE' THEN "yummy"."sales_ext_sales_price" ELSE NULL END) + sum(CASE WHEN "yummy"."sales_sales_channel" = 'CATALOG' THEN "yummy"."sales_ext_sales_price" ELSE NULL END) ) + sum(CASE WHEN "yummy"."sales_sales_channel" = 'WEB' THEN "yummy"."sales_ext_sales_price" ELSE NULL END)) / 3 ) ) * 100 as "ws_dev",
    (( sum(CASE WHEN "yummy"."sales_sales_channel" = 'STORE' THEN "yummy"."sales_ext_sales_price" ELSE NULL END) + sum(CASE WHEN "yummy"."sales_sales_channel" = 'CATALOG' THEN "yummy"."sales_ext_sales_price" ELSE NULL END) ) + sum(CASE WHEN "yummy"."sales_sales_channel" = 'WEB' THEN "yummy"."sales_ext_sales_price" ELSE NULL END)) / 3 as "avg_rev"
FROM
    "yummy"
GROUP BY
    1
HAVING
    "ss_item_rev" BETWEEN 0.9 * "cs_item_rev" AND 1.1 * "cs_item_rev" and "ss_item_rev" BETWEEN 0.9 * "ws_item_rev" AND 1.1 * "ws_item_rev" and "cs_item_rev" BETWEEN 0.9 * "ss_item_rev" AND 1.1 * "ss_item_rev" and "cs_item_rev" BETWEEN 0.9 * "ws_item_rev" AND 1.1 * "ws_item_rev" and "ws_item_rev" BETWEEN 0.9 * "ss_item_rev" AND 1.1 * "ss_item_rev" and "ws_item_rev" BETWEEN 0.9 * "cs_item_rev" AND 1.1 * "cs_item_rev"

ORDER BY 
    "item_id" asc nulls first,
    "ss_item_rev" asc nulls first
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
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\nodes\merge_node.py", line 262, in _resolve
    p.resolve() for p in self.parents
    ~~~~~~~~~^^
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
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\nodes\group_node.py", line 90, in _resolve
    grains = self.check_if_required(
        self.output_concepts, parent_sources, self.environment, self.depth
    )
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\nodes\group_node.py", line 83, in check_if_required
    return check_if_group_required(downstream_concepts, parents, environment, depth)
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\discovery_utility.py", line 124, in check_if_group_required
    comp_grain += calculate_effective_parent_grain(source)
                  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\discovery_utility.py", line 65, in calculate_effective_parent_grain
    return qds.datasources[0].grain
           ~~~~~~~~~~~~~~~^^^
IndexError: list index out of range
```
