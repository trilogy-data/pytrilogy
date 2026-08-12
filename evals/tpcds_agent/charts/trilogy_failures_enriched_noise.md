# Trilogy failure analysis — 20260811-133917

- Run `20260811-133909_enriched_noise` | `deepseek/deepseek-v4-flash` | sf=1
- `trilogy` calls: 407 | failed: 17 (4%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `disabled-tool` | 8 | 47% |
| `other` | 4 | 24% |
| `syntax-parse` | 3 | 18% |
| `join-resolution` | 2 | 12% |

## Detail

### `disabled-tool`

- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/item.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under root/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```

### `other`

- `trilogy run scratch3.preql`

  ```text
  Syntax error in scratch3.preql: ORDER BY references 'a.item.sk', which is not in the SELECT projection (line 3). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --a.item.sk order by a.item.sk asc`.
  ```
- `trilogy run probe_narrow.preql`

  ```text
  Unexpected error in probe_narrow.preql: name 'is_grouping_identity' is not defined
  ```
- `trilogy run answer_3697706765.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Unexpected error in answer_3697706765.preql: name '_aggregate_axis_members' is not defined
  ```
- `trilogy run answer_219203908.preql`

  ```text
  Unexpected error in answer_219203908.preql: Could not render the query: global (by *) aggregate local.overall_avg_sales rendered in CTE protective at keyed grain Grain<leaf.brand_id,leaf.category_id,leaf.channel,leaf.class_id. A planned reference has no backing source CTE -- typically an unsupported cross-rowset or membership shape the planner could not wire. Review the rowset/join structure (or file an issue if the query looks valid).

  Full SQL with sentinel(s):

  WITH
  cheerful as (
  SELECT
       'CATALOG'  as "s_channel",
      "s_catalog_sales_unified"."CS_ITEM_SK" as "s_item_sk",
      "s_catalog_sales_unified"."CS_ORDER_NUMBER" as "s_order_id",
      "s_catalog_sales_unified"."CS_SOLD_DATE_SK" as "s_sale_date_sk",
      "s_catalog_sales_unified"."CS_LIST_PRICE" as "s_list_price",
      "s_catalog_sales_unified"."CS_QUANTITY" as "s_quantity"
  FROM
      "fact_catalog_sales" as "s_catalog_sales_unified"
  UNION ALL
  SELECT
       'STORE'  as "s_channel",
      "s_store_sales_unified"."SS_ITEM_SK" as "s_item_sk",
      "s_store_sales_unified"."SS_TICKET_NUMBER" as "s_order_id",
      "s_store_sales_unified"."SS_SOLD_DATE_SK" as "s_sale_date_sk",
      "s_store_sales_unified"."SS_LIST_PRICE" as "s_list_price",
      "s_store_sales_unified"."SS_QUANTITY" as "s_quantity"
  FROM
      "fact_store_sales" as "s_store_sales_unified"
  UNION ALL
  SELECT
       'WEB'  as "s_channel",
      "s_web_sales_unified"."WS_ITEM_SK" as "s_item_sk",
      "s_web_sales_unified"."WS_ORDER_NUMBER" as "s_order_id",
      "s_web_sales_unified"."WS_SOLD_DATE_SK" as "s_sale_date_sk",
      "s_web_sales_unified"."WS_LIST_PRICE" as "s_list_price",
      "s_web_sales_unified"."WS_QUANTITY" as "s_quantity"
  FROM
      "fact_web_sales" as "s_web_sales_unified"),
  abundant as (
  SELECT
      "cheerful"."s_channel" as "s_channel",
      "cheerful"."s_item_sk" as "s_item_sk"
  FROM
      "cheerful"
      INNER JOIN "dim_date_dim" as "s_sale_date_date" on "cheerful"."s_sale_date_sk" = "s_sale_date_date"."D_DATE_SK"
  WHERE
      "s_sale_date_date"."D_YEAR" BETWEEN 1999 AND 2001

  GROUP BY
      1,
      2),
  yummy as (
  SELECT
      "abundant"."s_item_sk" as "s_item_sk"
  FROM
      "abundant"
  GROUP BY
      1
  HAVING
      count(distinct "abundant"."s_channel") >= 3
  ),
  juicy as (
  SELECT
      "yummy"."s_item_sk" as "presence_item_sk"
  FROM
      "yummy"
  GROUP BY
      1),
  abhorrent as (
  SELECT
      "s_item_items"."I_BRAND_ID" as "tups_brand_id",
      "s_item_items"."I_CATEGORY_ID" as "tups_category_id",
      "s_item_items"."I_CLASS_ID" as "tups_class_id"
  FROM
      "dim_item" as "s_item_items"
  WHERE
      exists (select 1 from juicy where juicy."presence_item_sk" is not distinct from "s_item_items"."I_ITEM_SK") and "s_item_items"."I_BRAND_ID" is not null and "s_item_items"."I_CLASS_ID" is not null and "s_item_items"."I_CATEGORY_ID" is not null
  ),
  questionable as (
  SELECT
      "cheerful"."s_channel" as "s_channel",
      "cheerful"."s_item_sk" as "s_item_sk",
      "cheerful"."s_list_price" as "s_list_price",
      "cheerful"."s_order_id" as "s_order_id",
      "cheerful"."s_quantity" as "s_quantity",
      "s_item_items"."I_BRAND_ID" as "s_item_brand_id",
      "s_item_items"."I_CATEGORY_ID" as "s_item_category_id",
      "s_item_items"."I_CLASS_ID" as "s_item_class_id"
  FROM
      "cheerful"
      INNER JOIN "dim_item" as "s_item_items" on "cheerful"."s_item_sk" = "s_item_items"."I_ITEM_SK"
      INNER JOIN "dim_date_dim" as "s_sale_date_date" on "cheerful"."s_sale_date_sk" = "s_sale_date_date"."D_DATE_SK"
  WHERE
      "s_sale_date_date"."D_YEAR" = 2001 and "s_sale_date_date"."D_MOY" = 11 and exists (select 1 from abhorrent where abhorrent."tups_brand_id" is not distinct from "s_item_items"."I_BRAND_ID" and abhorrent."tups_class_id" is not distinct from "s_item_items"."I_CLASS_ID" and abhorrent."tups_category_id" is not distinct from "s_item_items"."I_CATEGORY_ID")
  ),
  macho as (
  SELECT
      "questionable"."s_item_brand_id" as "_leaf_brand_id",
      "questionable"."s_item_brand_id" as "s_item_brand_id",
      "questionable"."s_item_category_id" as "_leaf_category_id",
      "questionable"."s_item_category_id" as "s_item_category_id",
      "questionable"."s_item_class_id" as "_leaf_class_id",
      "questionable"."s_item_class_id" as "s_item_class_id",
      "questionable"."s_item_sk" as "s_item_sk",
      "questionable"."s_list_price" as "s_list_price",
      "questionable"."s_order_id" as "s_order_id",
      "questionable"."s_quantity" as "s_quantity",
      LOWER("questionable"."s_channel")  as "_leaf_channel"
  FROM
      "questionable"
  WHERE
      exists (select 1 from abhorrent where abhorrent."tups_brand_id" is not distinct from "questionable"."s_item_brand_id" and abhorrent."tups_class_id" is not distinct from "questionable"."s_item_class_id" and abhorrent."tups_category_id" is not distinct from "questionable"."s_item_category_id")
  ),
  kaput as (
  SELECT
      "macho"."_leaf_channel" as "_leaf_channel",
      "macho"."s_item_brand_id" as "s_item_brand_id",
      "macho"."s_item_category_id" as "s_item_category_id",
      "macho"."s_item_class_id" as "s_item_class_id",
      "macho"."s_item_sk" as "s_item_sk",
      "macho"."s_order_id" as "s_order_id"
  FROM
      "macho"
  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6),
  divergent as (
  SELECT
      "kaput"."_leaf_channel" as "_leaf_channel",
      "kaput"."s_item_brand_id" as "s_item_brand_id",
      "kaput"."s_item_category_id" as "s_item_category_id",
      "kaput"."s_item_class_id" as "s_item_class_id",
      count(md5(CONCAT_WS('', coalesce(cast("kaput"."s_order_id" as string),'
  '), coalesce(cast("kaput"."s_item_sk" as string),'
  ')))) as "_leaf_line_count"
  FROM
      "kaput"
  GROUP BY
      1,
      2,
      3,
      4),
  scrawny as (
  SELECT
      "macho"."_leaf_brand_id" as "_leaf_brand_id",
      "macho"."_leaf_category_id" as "_leaf_category_id",
      "macho"."_leaf_channel" as "_leaf_channel",
      "macho"."_leaf_class_id" as "_leaf_class_id",
      "macho"."s_item_brand_id" as "s_item_brand_id",
      "macho"."s_item_category_id" as "s_item_category_id",
      "macho"."s_item_class_id" as "s_item_class_id",
      sum("macho"."s_quantity" * "macho"."s_list_price") as "_leaf_sales"
  FROM
      "macho"
  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7),
  busy as (
  SELECT
      "divergent"."_leaf_line_count" as "leaf_line_count",
      "scrawny"."_leaf_brand_id" as "leaf_brand_id",
      "scrawny"."_leaf_category_id" as "leaf_category_id",
      "scrawny"."_leaf_channel" as "leaf_channel",
      "scrawny"."_leaf_class_id" as "leaf_class_id",
      "scrawny"."_leaf_sales" as "leaf_sales"
  FROM
      "divergent"
      INNER JOIN "scrawny" on "divergent"."_leaf_channel" = "scrawny"."_leaf_channel" AND "divergent"."s_item_brand_id" is not distinct from "scrawny"."s_item_brand_id" AND "divergent"."s_item_category_id" is not distinct from "scrawny"."s_item_category_id" AND "divergent"."s_item_class_id" is not distinct from "scrawny"."s_item_class_id"),
  protective as (
  SELECT
      "busy"."leaf_brand_id" as "kept_brand_id",
      "busy"."leaf_category_id" as "kept_category_id",
      "busy"."leaf_channel" as "kept_channel",
      "busy"."leaf_class_id" as "kept_class_id",
      "busy"."leaf_line_count" as "kept_line_count",
      "busy"."leaf_sales" as "kept_sales"
  FROM
      "busy"
  WHERE
      "busy"."leaf_sales" > INVALID_REFERENCE_BUG_AGG_GRAIN_MISMATCH<global (by *) aggregate local.overall_avg_sales rendered in CTE protective at keyed grain Grain<leaf.brand_id,leaf.category_id,leaf.channel,leaf.class_id>>
  )
  SELECT
      "protective"."kept_channel" as "channel",
      "protective"."kept_brand_id" as "brand_id",
      "protective"."kept_class_id" as "class_id",
      "protective"."kept_category_id" as "category_id",
      sum("protective"."kept_sales") as "total_sales",
      sum("protective"."kept_line_count") as "total_line_count"
  FROM
      "protective"
  GROUP BY
      ROLLUP (1, 2, 3, 4)
  ORDER BY
      "channel" asc nulls first,
      "brand_id" asc nulls first,
      "class_id" asc nulls first,
      "category_id" asc nulls first
  LIMIT (100)
  ```

### `syntax-parse`

- `trilogy file write scratch2.preql`

  ```text
  refused to write 'scratch2.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:121
    |
  7 |   count(a.item.sk ? a.is_returned is not null and a.return_channel_dim_id is not null and a.return_channel_dim_text_id <> a.channel_dim_text_id) as ret_diff_entity,
    |                                                                                                                         ^---
    |
    = expected sum_operator
  Location:
  ...a.return_channel_dim_text_id < ??? > a.channel_dim_text_id) as re...
  ```
- `trilogy run --stdin`

  ```text
  Syntax error in stdin:  --> 1:3
    |
  1 | --stdin;
    |   ^---
    |
    = expected PURPOSE, PROPERTY, UNIQUE, or AUTO
  Location:
  -- ??? stdin;
  ```
- `trilogy file write probe_a.preql`

  ```text
  refused to write 'probe_a.preql': not syntactically valid Trilogy.

  Parse error:
    --> 13:1
     |
  13 | by s.channel;
     | ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ..._of_year = 11) as rows_nov01
   ??? by s.channel;
  ```

### `join-resolution`

- `trilogy run probe_item2.preql`

  ```text
  Resolution error in probe_item2.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 6). The requested concepts split into 2 disconnected subgraphs: {avg_price}; {category, n_lines, ss.customer.current_address.sk, ss.item.category, ss.sale_date.month_of_year, ss.sale_date.year}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run scratch_probe10.preql`

  ```text
  Resolution error in scratch_probe10.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {cs.billing_customer.sk, cs.item.id, cs.order_number, cs.quantity, cs.sale_date.year}; {ss.customer.sk, ss.item.id, ss.quantity, ss.return_customer.sk, ss.return_date.year, ss.return_quantity, ss.sale_date.year, ss.store.state, ss.ticket_number}. Are you missing a join or merge statement to relate them?
  ```
