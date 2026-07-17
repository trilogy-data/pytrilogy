# Trilogy failure analysis — 20260717-023249

- Run `20260717-023235_enriched` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 1278 | failed: 128 (10%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 75 | 59% |
| `syntax-parse` | 43 | 34% |
| `cli-misuse` | 4 | 3% |
| `syntax-missing-alias` | 3 | 2% |
| `type-error` | 2 | 2% |
| `planner-recursion` | 1 | 1% |

## Detail

### `other`

- `trilogy run answer_1858999935.preql`

  ```text
  Syntax error in answer_1858999935.preql: Nothing was executed: parsed 3 definition statement(s) (2 imports, 1 rowset) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy run answer_1858999935.preql`

  ```text
  Syntax error in answer_1858999935.preql: Undefined concept: s.day_of_week. Suggestions: ['n.day_of_week', 'sales.date.day_of_week', 'weekly_sales.day_of_week', 'sales.return_date.day_of_week', 'sales.ship_customer.first_sales_date.day_of_week', 'sales.ship_customer.first_shipto_date.day_of_week']
  ```
- `trilogy run answer_1858999935.preql`

  ```text
  Syntax error in answer_1858999935.preql: Undefined concept: s.day_of_week. Suggestions: ['n.day_of_week', 'sales.date.day_of_week', 'weekly_sales.day_of_week', 'sales.return_date.day_of_week', 'sales.ship_customer.first_sales_date.day_of_week', 'sales.ship_customer.first_shipto_date.day_of_week']
  ```
- `trilogy run answer_1858999935.preql`

  ```text
  Syntax error in answer_1858999935.preql: Undefined concept: next.week_seq. Suggestions: ['sales.date.week_seq', 'weekly_sales.week_seq', 'sales.return_date.week_seq', 'sales.billing_customer.first_sales_date.week_seq', 'sales.billing_customer.first_shipto_date.week_seq', 'sales.ship_customer.first_sales_date.week_seq']
  ```
- `trilogy run answer_3805575480.preql`

  ```text
  Resolution error in answer_3805575480.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 5). The requested concepts split into 3 disconnected subgraphs: {date.month_of_year, date.year}; {item.brand_id, item.brand_name, item.manufacturer_id}; {total_ext_sales_price}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_3863442186.preql`

  ```text
  {
    "event": "statement_result",
    "index": 0,
    "total": 1,
    "duration_ms": 96.275,
    "success": false,
    "error": "Could not render the query: Missing source reference to store.customer.id; Missing source reference to store.customer.first_name; Missing source reference to store.customer.last_name; Missing source reference to store.customer.preferred_cust_flag. A planned reference has no backing source CTE -- typically an unsupported cross-rowset or membership shape the planner could not wire. Review the rowset/join structure (or file an issue if the query looks valid).\n\nFull SQL with sen
  …
  e\".\"web_val_2002_val\" / \"sedate\".\"web_val_2001_val\"\n\nGROUP BY\n    1,\n    2,\n    3,\n    4\nORDER BY \n    \"customer_id\" asc nulls first,\n    \"first_name\" asc nulls first,\n    \"last_name\" asc nulls first,\n    \"preferred_cust_flag\" asc nulls first\nLIMIT (100)",
    "error_type": "ValueError"
  }
  {
    "event": "summary",
    "statements": 1,
    "duration_ms": 96.783,
    "ok": false,
    "rows": 0
  }
  {
    "event": "output_truncated",
    "dropped_events": 1,
    "note": "Output exceeded the tool cap; trailing events dropped. Narrow the call (--regex, --show, fewer rows) to see the rest."
  }
  ```
- `trilogy run --import raw.store_sales:store --import raw.catalog_sales:catalog --import raw.web_sales:web select
    store.customer.id as cid,
    sum(store.e…b.ext_wholesale_cost - web.ext_discount_amount + web.ext_sales_price ? web.date.year = 2002) as web_2002
where store.customer.id = 'AAAAAAAADIIOAAAA'`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {store.customer.id, store.ext_discount_amount, store.ext_list_price, store.ext_wholesale_cost, store.item.sk, store.ticket_number}; {web_2001, web_2002}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_883027685.preql`

  ```text
  Syntax error in answer_883027685.preql: Undefined concept: item.current_price. Suggestions: ['ss.item.current_price']
  ```
- `trilogy run answer_3697706765.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Syntax error in answer_3697706765.preql: Undefined concept: store.zip. Suggestions: ['store_sales.store.zip', 'store_sales.pos_address.zip', 'store_sales.return_store.zip', 'store_sales.return_address.zip', 'store_sales.customer.current_address.zip', 'store_sales.return_customer.current_address.zip']
  ```
- `trilogy run answer_4199102535.preql`

  ```text
  Syntax error in answer_4199102535.preql: Nothing was executed: parsed 8 definition statement(s) (4 imports, 4 rowsets) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy file read answer_1297355556.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_219203908.preql`

  ```text
  Syntax error in answer_219203908.preql: a `(select ...)` subquery used as a scalar value or membership set must select exactly one column; project only the key/value consumed by the outer expression (line 43, column 72)
  ```
- `trilogy run answer_219203908.preql`

  ```text
  Resolution error in answer_219203908.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 83). The requested concepts split into 2 disconnected subgraphs: {leaf_groups.brand_id, leaf_groups.category_id, leaf_groups.channel, leaf_groups.class_id, leaf_groups.line_item_cnt, leaf_groups.total_sales}; {_virt_agg_count_5699980413592369_wscope, _virt_agg_count_7611119458605887_wscope, _virt_agg_count_7800722893541382_wscope, _virt_agg_sum_205631958646933_wscope, _virt_agg_sum_4839729510492476_wscope, _virt_agg_sum_506561807742109_wscope}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_219203908.preql`

  ```text
  Resolution error in answer_219203908.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 50). The requested concepts split into 2 disconnected subgraphs: {leaf_groups.brand_id, leaf_groups.category_id, leaf_groups.channel, leaf_groups.class_id, leaf_groups.line_item_cnt, leaf_groups.total_sales}; {_virt_agg_sum_5299979389418488_wscope}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_219203908.preql`

  ```text
  Unexpected error in answer_219203908.preql: (_duckdb.ConversionException) Conversion Error: Could not convert string 'store' to DOUBLE when casting from source column leaf_groups_lchan

  LINE 183: ..."friendly" on "cooperative"."overall_avg" is not distinct from "friendly"."leaf_groups_lchan"
                                                                              ^
  [SQL:
  WITH
  cheerful as (
  SELECT
       'CATALOG'  as "sales_channel",
      "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "sales_date_sk",
      "sales_catalog_sales_unified"."CS_ITEM_SK" as "sales_item_sk",
      "sales_catalog_sales_unified"."CS_LIST_PRICE" as "sales_list_price",
      "sales_catalog_sales_unified"."CS_ORDER_NUMBER" as "sales_order_id",
      "sales_catalog_sales_unified"."CS_QUANTITY" as "sales_quantity",
       1  as "sales_sale_line_item_counter"
  FROM
      "catalog_sales" as "sales_catalog_sales_unified"
  UNION ALL
  SELECT
       'STORE'  as "sales_channel",
      "sales_store_sales_unified"."SS_SOLD_DATE_SK" as "sales_date_sk",
      "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_sk",
      "sales_store_sales_unified"."SS_LIST_PRICE" as "sales_list_price",
      "sales_store_sales_unified"."SS_TICKET_NUMBER" as "sales_order_id",
      "sales_store_sales_unified"."SS_QUANTITY" as "sales_quantity",
       1  as "sales_sale_line_item_counter"
  FROM
      "store_sales" as "sales_store_sales_unified"
  UNION ALL
  SELECT
       'WEB'  as "sales_channel",
      "sales_web_sales_unified"."WS_SOLD_DATE_SK" as "sales_date_sk",
      "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_sk",
      "sales_web_sales_unified"."WS_LIST_PRICE" as "sales_list_price",
      "sales_web_sales_unified"."WS_ORDER_NUMBER" as "sales_order_id",
      "sales_web_sales_unified"."WS_QUANTITY" as "sales_quantity",
       1  as "sales_sale_line_item_counter"
  FROM
      "web_sales" as "sales_web_sales_unified"),
  yummy as (
  SELECT
      "sales_item_items"."I_BRAND_ID" as "sales_item_brand_id",
      "sales_item_items"."I_CATEGORY_ID" as "sales_item_category_id",
      "sales_item_items"."I_CLASS_ID" as "sales_item_class_id"
  FROM
      "store_sales" as "sales_store_sales_unified"
      INNER JOIN "date_dim" as "sales_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
      INNER JOIN "item" as "sales_item_items" on "sales_store_sales_unified"."SS_ITEM_SK" = "sales_item_items"."I_ITEM_SK"
  WHERE
      "sales_date_date"."D_YEAR" BETWEEN 1999 AND 2001 and "sales_item_items"."I_BRAND_ID" is not null and "sales_item_items"."I_CLASS_ID" is not null and "sales_item_items"."I_CATEGORY_ID" is not null

  GROUP BY
      1,
      2,
      3),
  vacuous as (
  SELECT
      "sales_item_items"."I_BRAND_ID" as "sales_item_brand_id",
      "sales_item_items"."I_CATEGORY_ID" as "sales_item_category_id",
      "sales_item_items"."I_CLASS_ID" as "sales_item_class_id"
  FROM
      "catalog_sales" as "sales_catalog_sales_unified"
      INNER JOIN "date_dim" as "sales_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
      INNER JOIN "item" as "sales_item_items" on "sales_catalog_sales_unified"."CS_ITEM_SK" = "sales_item_items"."I_ITEM_SK"
  WHERE
      "sales_date_date"."D_YEAR" BETWEEN 1999 AND 2001 and "sales_item_items"."I_BRAND_ID" is not null and "sales_item_items"."I_CLASS_ID" is not null and "sales_item_items"."I_CATEGORY_ID" is not null

  GROUP BY
      1,
      2,
      3),
  young as (
  SELECT
      "sales_item_items"."I_BRAND_ID" as "sales_item_brand_id",
      "sales_item_items"."I_CATEGORY_ID" as "sales_item_category_id",
      "sales_item_items"."I_CLASS_ID" as "sales_item_class_id"
  FROM
      "web_sales" as "sales_web_sales_unified"
      INNER JOIN "date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
      INNER JOIN "item" as "sales_item_items" on "sales_web_sales_unified"."WS_ITEM_SK" = "sales_item_items"."I_ITEM_SK"
  WHERE
      "sales_date_date"."D_YEAR" BETWEEN 1999 AND 2001 and "sales_item_items"."I_BRAND_ID" is not null and "sales_item_items"."I_CLASS_ID" is not null and "sales_item_items"."I_CATEGORY_ID" is not null

  GROUP BY
      1,
      2,
      3),
  cooperative as (
  SELECT
      ( sum("cheerful"."sales_quantity" * CASE WHEN "sales_date_date"."D_YEAR" BETWEEN 1999 AND 2001 THEN "cheerful"."sales_list_price" ELSE NULL END) * 1.0 ) / count(CASE WHEN "sales_date_date"."D_YEAR" BETWEEN 1999 AND 2001 THEN "cheerful"."sales_sale_line_item_counter" ELSE NULL END) as "overall_avg"
  FROM
      "cheerful"
      LEFT OUTER JOIN "date_dim" as "sales_date_date" on "cheerful"."sales_date_sk" = "sales_date_date"."D_DATE_SK"),
  abhorrent as (
  SELECT
      "yummy"."sales_item_brand_id" as "_common_tuples_brand_id",
      "yummy"."sales_item_class_id" as "_common_tuples_class_id",
      "yummy"."sales_item_category_id" as "_common_tuples_category_id"
  FROM
      "yummy"
  INTERSECT
  SELECT
      "vacuous"."sales_item_brand_id" as "_common_tuples_brand_id",
      "vacuous"."sales_item_class_id" as "_common_tuples_class_id",
      "vacuous"."sales_item_category_id" as "_common_tuples_category_id"
  FROM
      "vacuous"
  INTERSECT
  SELECT
      "young"."sales_item_brand_id" as "_common_tuples_brand_id",
      "young"."sales_item_class_id" as "_common_tuples_class_id",
      "young"."sales_item_category_id" as "_common_tuples_category_id"
  FROM
      "young"),
  sweltering as (
  SELECT
      "abhorrent"."_common_tuples_brand_id" as "_common_tuples_brand_id",
      "abhorrent"."_common_tuples_category_id" as "_common_tuples_category_id",
      "abhorrent"."_common_tuples_class_id" as "_common_tuples_class_id"
  FROM
      "abhorrent"),
  late as (
  SELECT
      "sweltering"."_common_tuples_brand_id" as "common_tuples_brand_id",
      "sweltering"."_common_tuples_category_id" as "common_tuples_category_id",
      "sweltering"."_common_tuples_class_id" as "common_tuples_class_id"
  FROM
      "sweltering"),
  macho as (
  SELECT
      "cheerful"."sales_list_price" as "sales_list_price",
      "cheerful"."sales_quantity" as "sales_quantity",
      "cheerful"."sales_sale_line_item_counter" as "sales_sale_line_item_counter",
      "sales_item_items"."I_BRAND_ID" as "sales_item_brand_id",
      "sales_item_items"."I_CATEGORY_ID" as "sales_item_category_id",
      "sales_item_items"."I_CLASS_ID" as "sales_item_class_id",
      LOWER("cheerful"."sales_channel")  as "_leaf_groups_lchan"
  FROM
      "cheerful"
      INNER JOIN "date_dim" as "sales_date_date" on "cheerful"."sales_date_sk" = "sales_date_date"."D_DATE_SK"
      INNER JOIN "item" as "sales_item_items" on "cheerful"."sales_item_sk" = "sales_item_items"."I_ITEM_SK"
  WHERE
      "sales_date_date"."D_YEAR" = 2001 and "sales_date_date"."D_MOY" = 11 and "sales_item_items"."I_BRAND_ID" is not null and "sales_item_items"."I_CLASS_ID" is not null and "sales_item_items"."I_CATEGORY_ID" is not null and exists (select 1 from late where late."common_tuples_brand_id" is not distinct from "sales_item_items"."I_BRAND_ID" and late."common_tuples_class_id" is not distinct from "sales_item_items"."I_CLASS_ID" and late."common_tuples_category_id" is not distinct from "sales_item_items"."I_CATEGORY_ID")

  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      "cheerful"."sales_channel",
      "cheerful"."sales_item_sk",
      "cheerful"."sales_order_id"),
  scrawny as (
  SELECT
      "macho"."_leaf_groups_lchan" as "_leaf_groups_lchan",
      "macho"."sales_item_brand_id" as "_leaf_groups_brand_id",
      "macho"."sales_item_category_id" as "_leaf_groups_category_id",
      "macho"."sales_item_class_id" as "_leaf_groups_class_id",
      count("macho"."sales_sale_line_item_counter") as "_leaf_groups_line_item_cnt",
      sum("macho"."sales_quantity" * "macho"."sales_list_price") as "_leaf_groups_total_sales"
  FROM
      "macho"
  GROUP BY
      1,
      2,
      3,
      4),
  friendly as (
  SELECT
      "scrawny"."_leaf_groups_brand_id" as "leaf_groups_brand_id",
      "scrawny"."_leaf_groups_category_id" as "leaf_groups_category_id",
      "scrawny"."_leaf_groups_class_id" as "leaf_groups_class_id",
      "scrawny"."_leaf_groups_lchan" as "leaf_groups_lchan",
      "scrawny"."_leaf_groups_line_item_cnt" as "leaf_groups_line_item_cnt",
      "scrawny"."_leaf_groups_total_sales" as "leaf_groups_total_sales"
  FROM
      "scrawny"),
  busy as (
  SELECT
      "cooperative"."overall_avg" as "channel",
      "cooperative"."overall_avg" as "overall_avg"
  FROM
      "cooperative"
      INNER JOIN "friendly" on "cooperative"."overall_avg" is not distinct from "friendly"."leaf_groups_lchan"
  WHERE
      "friendly"."leaf_groups_total_sales" > "cooperative"."overall_avg"

  GROUP BY
      2),
  kaput as (
  SELECT
      "cooperative"."overall_avg" as "overall_avg",
      "friendly"."leaf_groups_brand_id" as "leaf_groups_brand_id",
      "friendly"."leaf_groups_category_id" as "leaf_groups_category_id",
      "friendly"."leaf_groups_class_id" as "leaf_groups_class_id",
      "friendly"."leaf_groups_line_item_cnt" as "leaf_groups_line_item_cnt",
      "friendly"."leaf_groups_total_sales" as "leaf_groups_total_sales"
  FROM
      "cooperative"
      INNER JOIN "friendly" on "cooperative"."overall_avg" is not distinct from "friendly"."leaf_groups_lchan"
  WHERE
      "friendly"."leaf_groups_total_sales" > "cooperative"."overall_avg"
  ),
  divergent as (
  SELECT
      "kaput"."leaf_groups_brand_id" as "leaf_groups_brand_id",
      "kaput"."leaf_groups_category_id" as "leaf_groups_category_id",
      "kaput"."leaf_groups_class_id" as "leaf_groups_class_id",
      "kaput"."overall_avg" as "overall_avg",
      sum("kaput"."leaf_groups_line_item_cnt") as "total_line_item_cnt",
      sum("kaput"."leaf_groups_total_sales") as "total_sales"
  FROM
      "kaput"
  GROUP BY
      ROLLUP (4, 1, 3, 2))
  SELECT
      "busy"."channel" as "channel",
      "divergent"."leaf_groups_brand_id" as "leaf_groups_brand_id",
      "divergent"."leaf_groups_class_id" as "leaf_groups_class_id",
      "divergent"."leaf_groups_category_id" as "leaf_groups_category_id",
      "divergent"."total_sales" as "total_sales",
      "divergent"."total_line_item_cnt" as "total_line_item_cnt"
  FROM
      "divergent"
      FULL JOIN "busy" on "divergent"."overall_avg" is not distinct from "busy"."overall_avg"
  ORDER BY
      "busy"."channel" asc nulls first,
      "divergent"."leaf_groups_brand_id" asc nulls first,
      "divergent"."leaf_groups_class_id" asc nulls first,
      "divergent"."leaf_groups_category_id" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/9h9h)
  ```
- `trilogy run answer_219203908.preql`

  ```text
  Resolution error in answer_219203908.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 47). The requested concepts split into 2 disconnected subgraphs: {leaf_groups.brand_id, leaf_groups.category_id, leaf_groups.channel, leaf_groups.class_id, leaf_groups.line_item_cnt, leaf_groups.total_sales}; {_virt_agg_sum_5299979389418488_wscope}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_219203908.preql`

  ```text
  Unexpected error in answer_219203908.preql: (_duckdb.ConversionException) Conversion Error: Could not convert string 'catalog' to DOUBLE when casting from source column leaf_groups_channel

  LINE 193:     INNER JOIN "uneven" on "kaput"."leaf_groups_channel" is not distinct from "uneven...
                                       ^
  [SQL:
  WITH
  cheerful as (
  SELECT
       'CATALOG'  as "sales_channel",
      "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "sales_date_sk",
      "sales_catalog_sales_unified"."CS_ITEM_SK" as "sales_item_sk",
      "sales_catalog_sales_unified"."CS_LIST_PRICE" as "sales_list_price",
      "sales_catalog_sales_unified"."CS_ORDER_NUMBER" as "sales_order_id",
      "sales_catalog_sales_unified"."CS_QUANTITY" as "sales_quantity",
       1  as "sales_sale_line_item_counter"
  FROM
      "catalog_sales" as "sales_catalog_sales_unified"
  UNION ALL
  SELECT
       'STORE'  as "sales_channel",
      "sales_store_sales_unified"."SS_SOLD_DATE_SK" as "sales_date_sk",
      "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_sk",
      "sales_store_sales_unified"."SS_LIST_PRICE" as "sales_list_price",
      "sales_store_sales_unified"."SS_TICKET_NUMBER" as "sales_order_id",
      "sales_store_sales_unified"."SS_QUANTITY" as "sales_quantity",
       1  as "sales_sale_line_item_counter"
  FROM
      "store_sales" as "sales_store_sales_unified"
  UNION ALL
  SELECT
       'WEB'  as "sales_channel",
      "sales_web_sales_unified"."WS_SOLD_DATE_SK" as "sales_date_sk",
      "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_sk",
      "sales_web_sales_unified"."WS_LIST_PRICE" as "sales_list_price",
      "sales_web_sales_unified"."WS_ORDER_NUMBER" as "sales_order_id",
      "sales_web_sales_unified"."WS_QUANTITY" as "sales_quantity",
       1  as "sales_sale_line_item_counter"
  FROM
      "web_sales" as "sales_web_sales_unified"),
  juicy as (
  SELECT
      "sales_item_items"."I_BRAND_ID" as "sales_item_brand_id",
      "sales_item_items"."I_CATEGORY_ID" as "sales_item_category_id",
      "sales_item_items"."I_CLASS_ID" as "sales_item_class_id"
  FROM
      "store_sales" as "sales_store_sales_unified"
      INNER JOIN "date_dim" as "sales_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
      INNER JOIN "item" as "sales_item_items" on "sales_store_sales_unified"."SS_ITEM_SK" = "sales_item_items"."I_ITEM_SK"
  WHERE
      "sales_date_date"."D_YEAR" BETWEEN 1999 AND 2001 and "sales_item_items"."I_BRAND_ID" is not null and "sales_item_items"."I_CLASS_ID" is not null and "sales_item_items"."I_CATEGORY_ID" is not null

  GROUP BY
      1,
      2,
      3),
  concerned as (
  SELECT
      "sales_item_items"."I_BRAND_ID" as "sales_item_brand_id",
      "sales_item_items"."I_CATEGORY_ID" as "sales_item_category_id",
      "sales_item_items"."I_CLASS_ID" as "sales_item_class_id"
  FROM
      "catalog_sales" as "sales_catalog_sales_unified"
      INNER JOIN "date_dim" as "sales_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
      INNER JOIN "item" as "sales_item_items" on "sales_catalog_sales_unified"."CS_ITEM_SK" = "sales_item_items"."I_ITEM_SK"
  WHERE
      "sales_date_date"."D_YEAR" BETWEEN 1999 AND 2001 and "sales_item_items"."I_BRAND_ID" is not null and "sales_item_items"."I_CLASS_ID" is not null and "sales_item_items"."I_CATEGORY_ID" is not null

  GROUP BY
      1,
      2,
      3),
  sparkling as (
  SELECT
      "sales_item_items"."I_BRAND_ID" as "sales_item_brand_id",
      "sales_item_items"."I_CATEGORY_ID" as "sales_item_category_id",
      "sales_item_items"."I_CLASS_ID" as "sales_item_class_id"
  FROM
      "web_sales" as "sales_web_sales_unified"
      INNER JOIN "date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
      INNER JOIN "item" as "sales_item_items" on "sales_web_sales_unified"."WS_ITEM_SK" = "sales_item_items"."I_ITEM_SK"
  WHERE
      "sales_date_date"."D_YEAR" BETWEEN 1999 AND 2001 and "sales_item_items"."I_BRAND_ID" is not null and "sales_item_items"."I_CLASS_ID" is not null and "sales_item_items"."I_CATEGORY_ID" is not null

  GROUP BY
      1,
      2,
      3),
  cooperative as (
  SELECT
      ( sum("cheerful"."sales_quantity" * CASE WHEN "sales_date_date"."D_YEAR" BETWEEN 1999 AND 2001 THEN "cheerful"."sales_list_price" ELSE NULL END) * 1.0 ) / count(CASE WHEN "sales_date_date"."D_YEAR" BETWEEN 1999 AND 2001 THEN "cheerful"."sales_sale_line_item_counter" ELSE NULL END) as "_overall_stats_overall_avg"
  FROM
      "cheerful"
      LEFT OUTER JOIN "date_dim" as "sales_date_date" on "cheerful"."sales_date_sk" = "sales_date_date"."D_DATE_SK"),
  sweltering as (
  SELECT
      "juicy"."sales_item_brand_id" as "_common_tuples_brand_id",
      "juicy"."sales_item_class_id" as "_common_tuples_class_id",
      "juicy"."sales_item_category_id" as "_common_tuples_category_id"
  FROM
      "juicy"
  INTERSECT
  SELECT
      "concerned"."sales_item_brand_id" as "_common_tuples_brand_id",
      "concerned"."sales_item_class_id" as "_common_tuples_class_id",
      "concerned"."sales_item_category_id" as "_common_tuples_category_id"
  FROM
      "concerned"
  INTERSECT
  SELECT
      "sparkling"."sales_item_brand_id" as "_common_tuples_brand_id",
      "sparkling"."sales_item_class_id" as "_common_tuples_class_id",
      "sparkling"."sales_item_category_id" as "_common_tuples_category_id"
  FROM
      "sparkling"),
  uneven as (
  SELECT
      "cooperative"."_overall_stats_overall_avg" as "overall_stats_overall_avg"
  FROM
      "cooperative"),
  late as (
  SELECT
      "sweltering"."_common_tuples_brand_id" as "_common_tuples_brand_id",
      "sweltering"."_common_tuples_category_id" as "_common_tuples_category_id",
      "sweltering"."_common_tuples_class_id" as "_common_tuples_class_id"
  FROM
      "sweltering"),
  macho as (
  SELECT
      "late"."_common_tuples_brand_id" as "common_tuples_brand_id",
      "late"."_common_tuples_category_id" as "common_tuples_category_id",
      "late"."_common_tuples_class_id" as "common_tuples_class_id"
  FROM
      "late"),
  scrawny as (
  SELECT
      "cheerful"."sales_list_price" as "sales_list_price",
      "cheerful"."sales_quantity" as "sales_quantity",
      "cheerful"."sales_sale_line_item_counter" as "sales_sale_line_item_counter",
      "sales_item_items"."I_BRAND_ID" as "sales_item_brand_id",
      "sales_item_items"."I_CATEGORY_ID" as "sales_item_category_id",
      "sales_item_items"."I_CLASS_ID" as "sales_item_class_id",
      LOWER("cheerful"."sales_channel")  as "_leaf_groups_channel"
  FROM
      "cheerful"
      INNER JOIN "date_dim" as "sales_date_date" on "cheerful"."sales_date_sk" = "sales_date_date"."D_DATE_SK"
      INNER JOIN "item" as "sales_item_items" on "cheerful"."sales_item_sk" = "sales_item_items"."I_ITEM_SK"
  WHERE
      "sales_date_date"."D_YEAR" = 2001 and "sales_date_date"."D_MOY" = 11 and "sales_item_items"."I_BRAND_ID" is not null and "sales_item_items"."I_CLASS_ID" is not null and "sales_item_items"."I_CATEGORY_ID" is not null and exists (select 1 from macho where macho."common_tuples_brand_id" is not distinct from "sales_item_items"."I_BRAND_ID" and macho."common_tuples_class_id" is not distinct from "sales_item_items"."I_CLASS_ID" and macho."common_tuples_category_id" is not distinct from "sales_item_items"."I_CATEGORY_ID")

  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      "cheerful"."sales_channel",
      "cheerful"."sales_item_sk",
      "cheerful"."sales_order_id"),
  friendly as (
  SELECT
      "scrawny"."_leaf_groups_channel" as "_leaf_groups_channel",
      "scrawny"."sales_item_brand_id" as "_leaf_groups_brand_id",
      "scrawny"."sales_item_category_id" as "_leaf_groups_category_id",
      "scrawny"."sales_item_class_id" as "_leaf_groups_class_id",
      count("scrawny"."sales_sale_line_item_counter") as "_leaf_groups_line_item_cnt",
      sum("scrawny"."sales_quantity" * "scrawny"."sales_list_price") as "_leaf_groups_total_sales"
  FROM
      "scrawny"
  GROUP BY
      1,
      2,
      3,
      4),
  kaput as (
  SELECT
      "friendly"."_leaf_groups_brand_id" as "leaf_groups_brand_id",
      "friendly"."_leaf_groups_category_id" as "leaf_groups_category_id",
      "friendly"."_leaf_groups_channel" as "leaf_groups_channel",
      "friendly"."_leaf_groups_class_id" as "leaf_groups_class_id",
      "friendly"."_leaf_groups_line_item_cnt" as "leaf_groups_line_item_cnt",
      "friendly"."_leaf_groups_total_sales" as "leaf_groups_total_sales"
  FROM
      "friendly"),
  divergent as (
  SELECT
      "kaput"."leaf_groups_brand_id" as "leaf_groups_brand_id",
      "kaput"."leaf_groups_category_id" as "leaf_groups_category_id",
      "kaput"."leaf_groups_channel" as "leaf_groups_channel",
      "kaput"."leaf_groups_class_id" as "leaf_groups_class_id",
      "kaput"."leaf_groups_line_item_cnt" as "leaf_groups_line_item_cnt",
      "kaput"."leaf_groups_total_sales" as "leaf_groups_total_sales",
      "uneven"."overall_stats_overall_avg" as "overall_stats_overall_avg"
  FROM
      "kaput"
      INNER JOIN "uneven" on "kaput"."leaf_groups_channel" is not distinct from "uneven"."overall_stats_overall_avg"
  WHERE
      "kaput"."leaf_groups_total_sales" > "uneven"."overall_stats_overall_avg"
  )
  SELECT
      "divergent"."leaf_groups_channel" as "leaf_groups_channel",
      "divergent"."leaf_groups_brand_id" as "leaf_groups_brand_id",
      "divergent"."leaf_groups_class_id" as "leaf_groups_class_id",
      "divergent"."leaf_groups_category_id" as "leaf_groups_category_id",
      sum("divergent"."leaf_groups_total_sales") as "total_sales",
      sum("divergent"."leaf_groups_line_item_cnt") as "total_line_item_cnt"
  FROM
      "divergent"
  GROUP BY
      ROLLUP (1, 2, 3, 4)
  ORDER BY
      "divergent"."leaf_groups_channel" asc nulls first,
      "divergent"."leaf_groups_brand_id" asc nulls first,
      "divergent"."leaf_groups_class_id" asc nulls first,
      "divergent"."leaf_groups_category_id" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/9h9h)
  ```
- `trilogy run answer_219203908.preql`

  ```text
  Syntax error in answer_219203908.preql: Nothing was executed: parsed 5 definition statement(s) (3 rowsets, 2 imports) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_2524943990.preql`

  ```text
  Resolution error in answer_2524943990.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 27). The requested concepts split into 2 disconnected subgraphs: {cs.order_number, total_ext_ship_cost, total_net_profit}; {order_stats.warehouse_count}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_765177085.preql`

  ```text
  Resolution error in answer_765177085.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 66). The requested concepts split into 2 disconnected subgraphs: {cat_intersect.cat_qty}; {store_intersect.item_desc, store_intersect.item_id, store_intersect.store_state}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read answer_1835050598.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 23 column 22 (char 994). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file read trilogy.toml`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_1798498862.preql`

  ```text
  Syntax error in answer_1798498862.preql: Comparison `ss.return_date.month_of_year <= 12` matches every value of enum field 'ss.return_date.month_of_year', which contains only these values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12. It is always true and should be removed.
  ```
- `trilogy run answer_1798498862.preql`

  ```text
  Syntax error in answer_1798498862.preql: 2 undefined concept references; fix all before re-running:
    - combined.ss_returned.cust_sk (line 47, col 7, in WHERE); did you mean: combined.ss_returned.qty, combined.ss_returned.item_desc, combined.ss_returned.ret_qty, ss_returned.cust_sk?
    - combined.cs_by_cust_item.bc_sk (line 47, col 40, in WHERE); did you mean: combined.cs_by_cust_item.cs_qty, combined.ss_returned.cust_sk, combined.ss_returned.item_desc, cs_by_cust_item.bc_sk, cs_by_cust_item.cs_item_sk?
  ```
- `trilogy run answer_1798498862.preql`

  ```text
  Resolution error in answer_1798498862.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 15). The requested concepts split into 2 disconnected subgraphs: {cs_data.bc_sk, cs_data.catalog_qty, cs_data.cs_item_sk}; {store_return_qty, store_sale_qty, ss.item.desc, ss.item.id, ss.store.id, ss.store.name}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_1798498862.preql`

  ```text
  Syntax error in answer_1798498862.preql: Undefined concept: joined.cs_filtered.billing_customer.sk (line 29, col 7, in WHERE). Suggestions: ['cs_filtered.cs.billing_customer.sk', 'cs.billing_customer.sk', 'cs_filtered.cs.item.sk', 'cs.billing_customer.current_address.sk', 'cs.return_customer.sk', 'cs.billing_customer.first_sales_date.sk']
  ```
- `trilogy run answer_3347758002.preql`

  ```text
  Syntax error in answer_3347758002.preql: Undefined concept: item.category. Suggestions: ['all_sales.item.category', 'all_sales.item.category_id']
  ```
- `trilogy run answer_3770074305.preql`

  ```text
  Resolution error in answer_3770074305.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 5). The requested concepts split into 2 disconnected subgraphs: {inventory.date.date, inventory.quantity_on_hand}; {item.current_price, item.manufacturer_id, item.sk, current_price, description, item_code}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 59 column 3 (char 2105). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run answer_142117636.preql`

  ```text
  Resolution error in answer_142117636.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 8). The requested concepts split into 3 disconnected subgraphs: {inv.date.year, _monthly_agg_mean_qoh, _monthly_agg_month_num, _monthly_agg_stddev_qoh}; {_monthly_agg_item_sk}; {_monthly_agg_warehouse_sk}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_630074061.preql`

  ```text
  Syntax error in answer_630074061.preql: Undefined concept: item.current_price. Suggestions: ['cs.item.current_price']
  ```
- `trilogy run answer_1663418777.preql`

  ```text
  Syntax error in answer_1663418777.preql: Undefined concept: local.manufact (line 33, col 34, in SELECT). Suggestions: ['item.manufact', 'manufact_counts.manufact', 'profile_manufacts.manufact']
  ```
- `trilogy run answer_1663418777.preql`

  ```text
  Syntax error in answer_1663418777.preql: Undefined concept: local.manufact (line 33, col 34, in SELECT). Suggestions: ['item.manufact', 'profile_manufacts.manufact', 'manufact_item_counts.manufact']
  ```
- `trilogy run answer_968172492.preql`

  ```text
  Syntax error in answer_968172492.preql: Undefined concept: item.sk. Suggestions: ['ws.item.sk', 'ws.date.sk', 'ws.time.sk', 'ws.web_site.sk', 'ws.ship_date.sk', 'ws.ship_customer.sk']
  ```
- `trilogy run answer_2118989494.preql`

  ```text
  Syntax error in answer_2118989494.preql: Undefined concept: store_sales.date.year. Suggestions: ['customer.first_sales_date.year', 'return_customer.first_sales_date.year', 'return_date.year', 'date.year', 'customer.first_shipto_date.year', 'return_customer.first_shipto_date.year']
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3553309440.preql`

  ```text
  Resolution error in answer_3553309440.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 33). The requested concepts split into 2 disconnected subgraphs: {date.month_seq}; {cust_sk, total_price, ss.customer.current_address.county, ss.customer.current_address.state, ss.customer.sk, ss.store.county, ss.store.state}.
    - `date.month_seq` is disconnected — did you mean `ss.date.month_seq`? (connected to the other concepts)
  These look like separately-imported copies of models already reachable through a connected import; chain through that path (e.g. `ss.date.month_seq`) instead of importing a second, disconnected copy.
  ```
- `trilogy run answer_3553309440.preql`

  ```text
  Resolution error in answer_3553309440.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 15). The requested concepts split into 2 disconnected subgraphs: {cs.sold_date.month_of_year, cs.sold_date.year, ___tvf_arm_0_cust_sk}; {item.category, item.class}.
    - `item.category` is disconnected — did you mean `cs.item.category`? (connected to the other concepts)
    - `item.class` is disconnected — did you mean `cs.item.class`? (connected to the other concepts)
  These look like separately-imported copies of models already reachable through a connected import; chain through that path (e.g. `cs.item.category`) instead of importing a second, disconnected copy.
  ```
- `trilogy file read answer_2986518257.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 86 column 33 (char 3705). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run answer_1623435181.preql`

  ```text
  Resolution error in answer_1623435181.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {yr2001.fri, yr2001.mon, yr2001.sat, yr2001.sun, yr2001.thu, yr2001.tue, yr2001.wed, yr2001.week_data.store_code, yr2001.week_data.store_name, yr2001.wk_seq, yr2002.fri, yr2002.mon, yr2002.sat, yr2002.sun, yr2002.thu, yr2002.tue, yr2002.wed, yr2002.week_data.store_code, yr2002.week_data.store_name, yr2002.wk_seq}
  ```
- `trilogy run answer_1623435181.preql`

  ```text
  Unexpected error in answer_1623435181.preql: Invalid input concepts to node! ['local._virt_presence_3715449506300623'] are missing non-hidden parent nodes; have {'yr2002.mon', 'yr2001.week_data.store_code', 'yr2002.week_data.store_code', 'local._virt_func_subtract_4357750871020059', 'yr2002.sun', 'yr2002.sat', 'yr2002.fri', 'yr2002.thu', 'yr2001.wk_seq', 'yr2002.wed', 'yr2002.tue'} and hidden {'local._virt_presence_3715449506300623', 'yr2002.wk_seq'} from root {'yr2002.mon', 'yr2002.week_data.store_code', 'local._virt_func_subtract_4357750871020059', 'yr2002.sun', 'yr2002.sat', 'yr2002.fri', 'yr2002.thu', 'yr2002.wed', 'yr2002.tue'}
  ```
- `trilogy file read answer_1623435181.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3544057080.preql`

  ```text
  Syntax error in answer_3544057080.preql: Undefined concept: local.item. Suggestions: ['item.sk', 'item.id', 'item.size']
  ```
- `trilogy run answer_3544057080.preql`

  ```text
  Resolution error in answer_3544057080.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 27). The requested concepts split into 2 disconnected subgraphs: {_agg_coupon_sum, _agg_cur_city, _agg_cur_street_name, _agg_cur_street_num, _agg_cur_zip, _agg_first_sales_year, _agg_first_ship_year, _agg_line_count, _agg_list_price_sum, _agg_pos_city, _agg_pos_street_name, _agg_pos_street_num, _agg_pos_zip, _agg_sale_year, _agg_wholesale_sum, diff_marital, store_sale_with_return, ss.item.id, ss.item.product_name, ss.store.name, ss.store.zip}; {color_ok, price_ok, qualifying_item}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_3544057080.preql`

  ```text
  Unexpected error in answer_3544057080.preql: (_duckdb.BinderException) Binder Error: Referenced table "busy" not found!
  Candidate tables: "kaput"

  LINE 244: ..._zip" is not distinct from "kaput"."yr1999_store_zip") and "busy"."yr2000_cnt00" <= "kaput"."yr1999_cnt99"
                                                                          ^
  [SQL:
  WITH
  wakeful as (
  SELECT
      coalesce("cs_catalog_returns"."CR_ITEM_SK","cs_catalog_sales"."CS_ITEM_SK") as "_cat_agg_item_sk",
      sum("cs_catalog_sales"."CS_EXT_LIST_PRICE") as "_cat_agg_cat_ext_list_price",
      sum(( coalesce("cs_catalog_returns"."CR_REFUNDED_CASH",0) + coalesce("cs_catalog_returns"."CR_REVERSED_CHARGE",0) ) + coalesce("cs_catalog_returns"."CR_STORE_CREDIT",0)) as "_cat_agg_cat_refund"
  FROM
      "catalog_sales" as "cs_catalog_sales"
      LEFT OUTER JOIN "catalog_returns" as "cs_catalog_returns" on "cs_catalog_sales"."CS_ITEM_SK" = "cs_catalog_returns"."CR_ITEM_SK" AND "cs_catalog_sales"."CS_ORDER_NUMBER" = "cs_catalog_returns"."CR_ORDER_NUMBER"
  GROUP BY
      1),
  thoughtful as (
  SELECT
      "wakeful"."_cat_agg_cat_ext_list_price" as "cat_agg_cat_ext_list_price",
      "wakeful"."_cat_agg_cat_refund" as "cat_agg_cat_refund",
      "wakeful"."_cat_agg_item_sk" as "cat_agg_item_sk"
  FROM
      "wakeful"),
  cooperative as (
  SELECT
      "thoughtful"."cat_agg_item_sk" as "qualifying_item_sk"
  FROM
      "thoughtful"
  WHERE
      "thoughtful"."cat_agg_cat_ext_list_price" > 2 * "thoughtful"."cat_agg_cat_refund"

  GROUP BY
      1),
  macho as (
  SELECT
      "ss_customer_current_address_customer_address"."CA_CITY" as "ss_customer_current_address_city",
      "ss_customer_current_address_customer_address"."CA_STREET_NAME" as "ss_customer_current_address_street_name",
      "ss_customer_current_address_customer_address"."CA_STREET_NUMBER" as "ss_customer_current_address_street_number",
      "ss_customer_current_address_customer_address"."CA_ZIP" as "ss_customer_current_address_zip",
      "ss_customer_first_sales_date_date"."D_YEAR" as "ss_customer_first_sales_date_year",
      "ss_customer_first_shipto_date_date"."D_YEAR" as "ss_customer_first_shipto_date_year",
      "ss_date_date"."D_YEAR" as "ss_date_year",
      "ss_item_items"."I_ITEM_ID" as "ss_item_id",
      "ss_item_items"."I_PRODUCT_NAME" as "ss_item_product_name",
      "ss_pos_address_customer_address"."CA_CITY" as "ss_pos_address_city",
      "ss_pos_address_customer_address"."CA_STREET_NAME" as "ss_pos_address_street_name",
      "ss_pos_address_customer_address"."CA_STREET_NUMBER" as "ss_pos_address_street_number",
      "ss_pos_address_customer_address"."CA_ZIP" as "ss_pos_address_zip",
      "ss_store_sales"."SS_COUPON_AMT" as "ss_coupon_amt",
      "ss_store_sales"."SS_LIST_PRICE" as "ss_list_price",
      "ss_store_sales"."SS_WHOLESALE_COST" as "ss_wholesale_cost",
      "ss_store_store"."S_STORE_NAME" as "ss_store_name",
      "ss_store_store"."S_ZIP" as "ss_store_zip",
      coalesce("ss_item_items"."I_ITEM_SK","ss_store_returns"."SR_ITEM_SK","ss_store_sales"."SS_ITEM_SK") as "ss_item_sk",
      coalesce("ss_store_returns"."SR_TICKET_NUMBER","ss_store_sales"."SS_TICKET_NUMBER") as "ss_ticket_number"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "item" as "ss_item_items" on "ss_store_sales"."SS_ITEM_SK" = "ss_item_items"."I_ITEM_SK"
      LEFT OUTER JOIN "date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "store" as "ss_store_store" on "ss_store_sales"."SS_STORE_SK" = "ss_store_store"."S_STORE_SK"
      LEFT OUTER JOIN "customer" as "ss_customer_customers" on "ss_store_sales"."SS_CUSTOMER_SK" = "ss_customer_customers"."C_CUSTOMER_SK"
      LEFT OUTER JOIN "store_returns" as "ss_store_returns" on "ss_store_sales"."SS_ITEM_SK" = "ss_store_returns"."SR_ITEM_SK" AND "ss_store_sales"."SS_TICKET_NUMBER" = "ss_store_returns"."SR_TICKET_NUMBER"
      LEFT OUTER JOIN "customer_address" as "ss_pos_address_customer_address" on "ss_store_sales"."SS_ADDR_SK" = "ss_pos_address_customer_address"."CA_ADDRESS_SK"
      LEFT OUTER JOIN "customer_address" as "ss_customer_current_address_customer_address" on "ss_customer_customers"."C_CURRENT_ADDR_SK" = "ss_customer_current_address_customer_address"."CA_ADDRESS_SK"
      LEFT OUTER JOIN "customer_demographics" as "ss_pos_customer_demographic_customer_demographics" on "ss_store_sales"."SS_CDEMO_SK" = "ss_pos_customer_demographic_customer_demographics"."CD_DEMO_SK"
      LEFT OUTER JOIN "date_dim" as "ss_customer_first_sales_date_date" on "ss_customer_customers"."C_FIRST_SALES_DATE_SK" = "ss_customer_first_sales_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "date_dim" as "ss_customer_first_shipto_date_date" on "ss_customer_customers"."C_FIRST_SHIPTO_DATE_SK" = "ss_customer_first_shipto_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "customer_demographics" as "ss_customer_current_demographics_customer_demographics" on "ss_customer_customers"."C_CURRENT_CDEMO_SK" = "ss_customer_current_demographics_customer_demographics"."CD_DEMO_SK"
  WHERE
      exists (select 1 from cooperative where cooperative."qualifying_item_sk" is not distinct from coalesce("ss_item_items"."I_ITEM_SK","ss_store_returns"."SR_ITEM_SK","ss_store_sales"."SS_ITEM_SK")) and ("ss_item_items"."I_COLOR" is not null and "ss_item_items"."I_COLOR" in ('purple','burlywood','indian','spring','floral','medium')) and ( "ss_item_items"."I_CURRENT_PRICE" BETWEEN 65 AND 74 ) and ("ss_store_returns"."SR_TICKET_NUMBER" is not null) and ("ss_pos_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" != "ss_customer_current_demographics_customer_demographics"."CD_MARITAL_STATUS")

  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      10,
      11,
      12,
      13,
      14,
      15,
      16,
      17,
      18,
      19,
      20),
  scrawny as (
  SELECT
      "macho"."ss_customer_current_address_city" as "_agg_cur_city",
      "macho"."ss_customer_current_address_street_name" as "_agg_cur_street_name",
      "macho"."ss_customer_current_address_street_number" as "_agg_cur_street_num",
      "macho"."ss_customer_current_address_zip" as "_agg_cur_zip",
      "macho"."ss_date_year" as "_agg_sale_year",
      "macho"."ss_item_id" as "ss_item_id",
      "macho"."ss_item_product_name" as "ss_item_product_name",
      "macho"."ss_pos_address_city" as "_agg_pos_city",
      "macho"."ss_pos_address_street_name" as "_agg_pos_street_name",
      "macho"."ss_pos_address_street_number" as "_agg_pos_street_num",
      "macho"."ss_pos_address_zip" as "_agg_pos_zip",
      "macho"."ss_store_name" as "ss_store_name",
      "macho"."ss_store_zip" as "ss_store_zip",
      count(md5(CONCAT_WS('', coalesce(cast("macho"."ss_ticket_number" as string),'
  '), coalesce(cast("macho"."ss_item_sk" as string),'
  ')))) as "_agg_line_count",
      sum("macho"."ss_coupon_amt") as "_agg_coupon_sum",
      sum("macho"."ss_list_price") as "_agg_list_price_sum",
      sum("macho"."ss_wholesale_cost") as "_agg_wholesale_sum"
  FROM
      "macho"
  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      10,
      11,
      12,
      13,
      "macho"."ss_customer_first_sales_date_year",
      "macho"."ss_customer_first_shipto_date_year"),
  divergent as (
  SELECT
      "scrawny"."_agg_coupon_sum" as "_yr2000_cp00",
      "scrawny"."_agg_line_count" as "_yr2000_cnt00",
      "scrawny"."_agg_list_price_sum" as "_yr2000_lp00",
      "scrawny"."_agg_sale_year" as "agg_sale_year",
      "scrawny"."_agg_wholesale_sum" as "_yr2000_ws00",
      "scrawny"."ss_item_id" as "_yr2000_item_id",
      "scrawny"."ss_store_name" as "_yr2000_store_name",
      "scrawny"."ss_store_zip" as "_yr2000_store_zip"
  FROM
      "scrawny"
  WHERE
      "scrawny"."_agg_sale_year" = 2000
  ),
  friendly as (
  SELECT
      "scrawny"."_agg_coupon_sum" as "_yr1999_cp99",
      "scrawny"."_agg_cur_city" as "agg_cur_city",
      "scrawny"."_agg_cur_street_name" as "agg_cur_street_name",
      "scrawny"."_agg_cur_street_num" as "agg_cur_street_num",
      "scrawny"."_agg_cur_zip" as "agg_cur_zip",
      "scrawny"."_agg_line_count" as "_yr1999_cnt99",
      "scrawny"."_agg_list_price_sum" as "_yr1999_lp99",
      "scrawny"."_agg_pos_city" as "agg_pos_city",
      "scrawny"."_agg_pos_street_name" as "agg_pos_street_name",
      "scrawny"."_agg_pos_street_num" as "agg_pos_street_num",
      "scrawny"."_agg_pos_zip" as "agg_pos_zip",
      "scrawny"."_agg_sale_year" as "agg_sale_year",
      "scrawny"."_agg_wholesale_sum" as "_yr1999_ws99",
      "scrawny"."ss_item_id" as "_yr1999_item_id",
      "scrawny"."ss_item_product_name" as "_yr1999_product_name",
      "scrawny"."ss_store_name" as "_yr1999_store_name",
      "scrawny"."ss_store_zip" as "_yr1999_store_zip"
  FROM
      "scrawny"
  WHERE
      "scrawny"."_agg_sale_year" = 1999
  ),
  busy as (
  SELECT
      "divergent"."_yr2000_cnt00" as "yr2000_cnt00",
      "divergent"."_yr2000_cp00" as "yr2000_cp00",
      "divergent"."_yr2000_item_id" as "yr2000_item_id",
      "divergent"."_yr2000_lp00" as "yr2000_lp00",
      "divergent"."_yr2000_store_name" as "yr2000_store_name",
      "divergent"."_yr2000_store_zip" as "yr2000_store_zip",
      "divergent"."_yr2000_ws00" as "yr2000_ws00",
      "divergent"."agg_sale_year" as "yr2000_agg_sale_year"
  FROM
      "divergent"),
  kaput as (
  SELECT
      "friendly"."_yr1999_cnt99" as "yr1999_cnt99",
      "friendly"."_yr1999_cp99" as "yr1999_cp99",
      "friendly"."_yr1999_item_id" as "yr1999_item_id",
      "friendly"."_yr1999_lp99" as "yr1999_lp99",
      "friendly"."_yr1999_product_name" as "yr1999_product_name",
      "friendly"."_yr1999_store_name" as "yr1999_store_name",
      "friendly"."_yr1999_store_zip" as "yr1999_store_zip",
      "friendly"."_yr1999_ws99" as "yr1999_ws99",
      "friendly"."agg_cur_city" as "yr1999_agg_cur_city",
      "friendly"."agg_cur_street_name" as "yr1999_agg_cur_street_name",
      "friendly"."agg_cur_street_num" as "yr1999_agg_cur_street_num",
      "friendly"."agg_cur_zip" as "yr1999_agg_cur_zip",
      "friendly"."agg_pos_city" as "yr1999_agg_pos_city",
      "friendly"."agg_pos_street_name" as "yr1999_agg_pos_street_name",
      "friendly"."agg_pos_street_num" as "yr1999_agg_pos_street_num",
      "friendly"."agg_pos_zip" as "yr1999_agg_pos_zip",
      "friendly"."agg_sale_year" as "yr1999_agg_sale_year"
  FROM
      "friendly"),
  premium as (
  SELECT
      "busy"."yr2000_store_zip" as "yr2000_store_zip"
  FROM
      "busy"
  GROUP BY
      1),
  protective as (
  SELECT
      "busy"."yr2000_store_name" as "yr2000_store_name"
  FROM
      "busy"
  GROUP BY
      1),
  charming as (
  SELECT
      "busy"."yr2000_item_id" as "yr2000_item_id"
  FROM
      "busy"
  GROUP BY
      1)
  SELECT
      "kaput"."yr1999_product_name" as "yr1999_product_name",
      "kaput"."yr1999_store_name" as "yr1999_store_name",
      "kaput"."yr1999_store_zip" as "yr1999_store_zip",
      "kaput"."yr1999_agg_pos_street_num" as "yr1999_agg_pos_street_num",
      "kaput"."yr1999_agg_pos_street_name" as "yr1999_agg_pos_street_name",
      "kaput"."yr1999_agg_pos_city" as "yr1999_agg_pos_city",
      "kaput"."yr1999_agg_pos_zip" as "yr1999_agg_pos_zip",
      "kaput"."yr1999_agg_cur_street_num" as "yr1999_agg_cur_street_num",
      "kaput"."yr1999_agg_cur_street_name" as "yr1999_agg_cur_street_name",
      "kaput"."yr1999_agg_cur_city" as "yr1999_agg_cur_city",
      "kaput"."yr1999_agg_cur_zip" as "yr1999_agg_cur_zip",
      "kaput"."yr1999_agg_sale_year" as "year1999",
      "kaput"."yr1999_cnt99" as "count1999",
      "kaput"."yr1999_ws99" as "wholesale_sum_1999",
      "kaput"."yr1999_lp99" as "list_price_sum_1999",
      "kaput"."yr1999_cp99" as "coupon_sum_1999",
      "busy"."yr2000_ws00" as "wholesale_sum_2000",
      "busy"."yr2000_lp00" as "list_price_sum_2000",
      "busy"."yr2000_cp00" as "coupon_sum_2000",
      "busy"."yr2000_agg_sale_year" as "year2000",
      "busy"."yr2000_cnt00" as "count2000"
  FROM
      "kaput"
  WHERE
      exists (select 1 from busy where busy."yr2000_item_id" is not distinct from "kaput"."yr1999_item_id") and exists (select 1 from busy where busy."yr2000_store_name" is not distinct from "kaput"."yr1999_store_name") and exists (select 1 from busy where busy."yr2000_store_zip" is not distinct from "kaput"."yr1999_store_zip") and "busy"."yr2000_cnt00" <= "kaput"."yr1999_cnt99"

  ORDER BY
      "kaput"."yr1999_product_name" asc,
      "kaput"."yr1999_store_name" asc,
      "count2000" asc,
      "wholesale_sum_1999" asc,
      "wholesale_sum_2000" asc]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run answer_3544057080.preql`

  ```text
  Unexpected error in answer_3544057080.preql: (_duckdb.BinderException) Binder Error: Referenced table "busy" not found!
  Candidate tables: "kaput"

  LINE 244: ..._zip" is not distinct from "kaput"."yr1999_store_zip") and "busy"."yr2000_cnt" <= "kaput"."yr1999_cnt"
                                                                          ^
  [SQL:
  WITH
  wakeful as (
  SELECT
      coalesce("cs_catalog_returns"."CR_ITEM_SK","cs_catalog_sales"."CS_ITEM_SK") as "_cat_agg_item_sk",
      sum("cs_catalog_sales"."CS_EXT_LIST_PRICE") as "_cat_agg_cat_ext_list_price",
      sum(( coalesce("cs_catalog_returns"."CR_REFUNDED_CASH",0) + coalesce("cs_catalog_returns"."CR_REVERSED_CHARGE",0) ) + coalesce("cs_catalog_returns"."CR_STORE_CREDIT",0)) as "_cat_agg_cat_refund"
  FROM
      "catalog_sales" as "cs_catalog_sales"
      LEFT OUTER JOIN "catalog_returns" as "cs_catalog_returns" on "cs_catalog_sales"."CS_ITEM_SK" = "cs_catalog_returns"."CR_ITEM_SK" AND "cs_catalog_sales"."CS_ORDER_NUMBER" = "cs_catalog_returns"."CR_ORDER_NUMBER"
  GROUP BY
      1),
  thoughtful as (
  SELECT
      "wakeful"."_cat_agg_cat_ext_list_price" as "cat_agg_cat_ext_list_price",
      "wakeful"."_cat_agg_cat_refund" as "cat_agg_cat_refund",
      "wakeful"."_cat_agg_item_sk" as "cat_agg_item_sk"
  FROM
      "wakeful"),
  cooperative as (
  SELECT
      "thoughtful"."cat_agg_item_sk" as "qualifying_item_sk"
  FROM
      "thoughtful"
  WHERE
      "thoughtful"."cat_agg_cat_ext_list_price" > 2 * "thoughtful"."cat_agg_cat_refund"

  GROUP BY
      1),
  macho as (
  SELECT
      "ss_customer_current_address_customer_address"."CA_CITY" as "ss_customer_current_address_city",
      "ss_customer_current_address_customer_address"."CA_STREET_NAME" as "ss_customer_current_address_street_name",
      "ss_customer_current_address_customer_address"."CA_STREET_NUMBER" as "ss_customer_current_address_street_number",
      "ss_customer_current_address_customer_address"."CA_ZIP" as "ss_customer_current_address_zip",
      "ss_customer_first_sales_date_date"."D_YEAR" as "ss_customer_first_sales_date_year",
      "ss_customer_first_shipto_date_date"."D_YEAR" as "ss_customer_first_shipto_date_year",
      "ss_date_date"."D_YEAR" as "ss_date_year",
      "ss_item_items"."I_ITEM_ID" as "ss_item_id",
      "ss_item_items"."I_PRODUCT_NAME" as "ss_item_product_name",
      "ss_pos_address_customer_address"."CA_CITY" as "ss_pos_address_city",
      "ss_pos_address_customer_address"."CA_STREET_NAME" as "ss_pos_address_street_name",
      "ss_pos_address_customer_address"."CA_STREET_NUMBER" as "ss_pos_address_street_number",
      "ss_pos_address_customer_address"."CA_ZIP" as "ss_pos_address_zip",
      "ss_store_sales"."SS_COUPON_AMT" as "ss_coupon_amt",
      "ss_store_sales"."SS_LIST_PRICE" as "ss_list_price",
      "ss_store_sales"."SS_WHOLESALE_COST" as "ss_wholesale_cost",
      "ss_store_store"."S_STORE_NAME" as "ss_store_name",
      "ss_store_store"."S_ZIP" as "ss_store_zip",
      coalesce("ss_item_items"."I_ITEM_SK","ss_store_returns"."SR_ITEM_SK","ss_store_sales"."SS_ITEM_SK") as "ss_item_sk",
      coalesce("ss_store_returns"."SR_TICKET_NUMBER","ss_store_sales"."SS_TICKET_NUMBER") as "ss_ticket_number"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "customer" as "ss_customer_customers" on "ss_store_sales"."SS_CUSTOMER_SK" = "ss_customer_customers"."C_CUSTOMER_SK"
      INNER JOIN "store_returns" as "ss_store_returns" on "ss_store_sales"."SS_ITEM_SK" = "ss_store_returns"."SR_ITEM_SK" AND "ss_store_sales"."SS_TICKET_NUMBER" = "ss_store_returns"."SR_TICKET_NUMBER"
      INNER JOIN "customer_demographics" as "ss_pos_customer_demographic_customer_demographics" on "ss_store_sales"."SS_CDEMO_SK" = "ss_pos_customer_demographic_customer_demographics"."CD_DEMO_SK"
      INNER JOIN "customer_demographics" as "ss_customer_current_demographics_customer_demographics" on "ss_customer_customers"."C_CURRENT_CDEMO_SK" = "ss_customer_current_demographics_customer_demographics"."CD_DEMO_SK"
      INNER JOIN "item" as "ss_item_items" on "ss_store_sales"."SS_ITEM_SK" = "ss_item_items"."I_ITEM_SK"
      LEFT OUTER JOIN "date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "store" as "ss_store_store" on "ss_store_sales"."SS_STORE_SK" = "ss_store_store"."S_STORE_SK"
      LEFT OUTER JOIN "customer_address" as "ss_pos_address_customer_address" on "ss_store_sales"."SS_ADDR_SK" = "ss_pos_address_customer_address"."CA_ADDRESS_SK"
      LEFT OUTER JOIN "customer_address" as "ss_customer_current_address_customer_address" on "ss_customer_customers"."C_CURRENT_ADDR_SK" = "ss_customer_current_address_customer_address"."CA_ADDRESS_SK"
      LEFT OUTER JOIN "date_dim" as "ss_customer_first_sales_date_date" on "ss_customer_customers"."C_FIRST_SALES_DATE_SK" = "ss_customer_first_sales_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "date_dim" as "ss_customer_first_shipto_date_date" on "ss_customer_customers"."C_FIRST_SHIPTO_DATE_SK" = "ss_customer_first_shipto_date_date"."D_DATE_SK"
  WHERE
      exists (select 1 from cooperative where cooperative."qualifying_item_sk" is not distinct from coalesce("ss_item_items"."I_ITEM_SK","ss_store_returns"."SR_ITEM_SK","ss_store_sales"."SS_ITEM_SK")) and ("ss_item_items"."I_COLOR" is not null and "ss_item_items"."I_COLOR" in ('purple','burlywood','indian','spring','floral','medium')) and ( "ss_item_items"."I_CURRENT_PRICE" BETWEEN 65 AND 74 ) and "ss_store_returns"."SR_TICKET_NUMBER" is not null and "ss_pos_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" != "ss_customer_current_demographics_customer_demographics"."CD_MARITAL_STATUS"

  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      10,
      11,
      12,
      13,
      14,
      15,
      16,
      17,
      18,
      19,
      20),
  scrawny as (
  SELECT
      "macho"."ss_customer_current_address_city" as "_agg_cur_city",
      "macho"."ss_customer_current_address_street_name" as "_agg_cur_street_name",
      "macho"."ss_customer_current_address_street_number" as "_agg_cur_street_num",
      "macho"."ss_customer_current_address_zip" as "_agg_cur_zip",
      "macho"."ss_date_year" as "_agg_sale_year",
      "macho"."ss_item_id" as "ss_item_id",
      "macho"."ss_item_product_name" as "ss_item_product_name",
      "macho"."ss_pos_address_city" as "_agg_pos_city",
      "macho"."ss_pos_address_street_name" as "_agg_pos_street_name",
      "macho"."ss_pos_address_street_number" as "_agg_pos_street_num",
      "macho"."ss_pos_address_zip" as "_agg_pos_zip",
      "macho"."ss_store_name" as "ss_store_name",
      "macho"."ss_store_zip" as "ss_store_zip",
      count(md5(CONCAT_WS('', coalesce(cast("macho"."ss_ticket_number" as string),'
  '), coalesce(cast("macho"."ss_item_sk" as string),'
  ')))) as "_agg_line_count",
      sum("macho"."ss_coupon_amt") as "_agg_coupon_sum",
      sum("macho"."ss_list_price") as "_agg_list_price_sum",
      sum("macho"."ss_wholesale_cost") as "_agg_wholesale_sum"
  FROM
      "macho"
  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      10,
      11,
      12,
      13,
      "macho"."ss_customer_first_sales_date_year",
      "macho"."ss_customer_first_shipto_date_year"),
  divergent as (
  SELECT
      "scrawny"."_agg_coupon_sum" as "_yr2000_cp",
      "scrawny"."_agg_line_count" as "_yr2000_cnt",
      "scrawny"."_agg_list_price_sum" as "_yr2000_lp",
      "scrawny"."_agg_sale_year" as "_yr2000_yr",
      "scrawny"."_agg_wholesale_sum" as "_yr2000_ws",
      "scrawny"."ss_item_id" as "_yr2000_item_id",
      "scrawny"."ss_store_name" as "_yr2000_store_name",
      "scrawny"."ss_store_zip" as "_yr2000_store_zip"
  FROM
      "scrawny"
  WHERE
      "scrawny"."_agg_sale_year" = 2000
  ),
  friendly as (
  SELECT
      "scrawny"."_agg_coupon_sum" as "_yr1999_cp",
      "scrawny"."_agg_cur_city" as "agg_cur_city",
      "scrawny"."_agg_cur_street_name" as "agg_cur_street_name",
      "scrawny"."_agg_cur_street_num" as "agg_cur_street_num",
      "scrawny"."_agg_cur_zip" as "agg_cur_zip",
      "scrawny"."_agg_line_count" as "_yr1999_cnt",
      "scrawny"."_agg_list_price_sum" as "_yr1999_lp",
      "scrawny"."_agg_pos_city" as "agg_pos_city",
      "scrawny"."_agg_pos_street_name" as "agg_pos_street_name",
      "scrawny"."_agg_pos_street_num" as "agg_pos_street_num",
      "scrawny"."_agg_pos_zip" as "agg_pos_zip",
      "scrawny"."_agg_sale_year" as "_yr1999_yr",
      "scrawny"."_agg_wholesale_sum" as "_yr1999_ws",
      "scrawny"."ss_item_id" as "_yr1999_item_id",
      "scrawny"."ss_item_product_name" as "_yr1999_product_name",
      "scrawny"."ss_store_name" as "_yr1999_store_name",
      "scrawny"."ss_store_zip" as "_yr1999_store_zip"
  FROM
      "scrawny"
  WHERE
      "scrawny"."_agg_sale_year" = 1999
  ),
  busy as (
  SELECT
      "divergent"."_yr2000_cnt" as "yr2000_cnt",
      "divergent"."_yr2000_cp" as "yr2000_cp",
      "divergent"."_yr2000_item_id" as "yr2000_item_id",
      "divergent"."_yr2000_lp" as "yr2000_lp",
      "divergent"."_yr2000_store_name" as "yr2000_store_name",
      "divergent"."_yr2000_store_zip" as "yr2000_store_zip",
      "divergent"."_yr2000_ws" as "yr2000_ws",
      "divergent"."_yr2000_yr" as "yr2000_yr"
  FROM
      "divergent"),
  kaput as (
  SELECT
      "friendly"."_yr1999_cnt" as "yr1999_cnt",
      "friendly"."_yr1999_cp" as "yr1999_cp",
      "friendly"."_yr1999_item_id" as "yr1999_item_id",
      "friendly"."_yr1999_lp" as "yr1999_lp",
      "friendly"."_yr1999_product_name" as "yr1999_product_name",
      "friendly"."_yr1999_store_name" as "yr1999_store_name",
      "friendly"."_yr1999_store_zip" as "yr1999_store_zip",
      "friendly"."_yr1999_ws" as "yr1999_ws",
      "friendly"."_yr1999_yr" as "yr1999_yr",
      "friendly"."agg_cur_city" as "yr1999_agg_cur_city",
      "friendly"."agg_cur_street_name" as "yr1999_agg_cur_street_name",
      "friendly"."agg_cur_street_num" as "yr1999_agg_cur_street_num",
      "friendly"."agg_cur_zip" as "yr1999_agg_cur_zip",
      "friendly"."agg_pos_city" as "yr1999_agg_pos_city",
      "friendly"."agg_pos_street_name" as "yr1999_agg_pos_street_name",
      "friendly"."agg_pos_street_num" as "yr1999_agg_pos_street_num",
      "friendly"."agg_pos_zip" as "yr1999_agg_pos_zip"
  FROM
      "friendly"),
  premium as (
  SELECT
      "busy"."yr2000_store_zip" as "yr2000_store_zip"
  FROM
      "busy"
  GROUP BY
      1),
  protective as (
  SELECT
      "busy"."yr2000_store_name" as "yr2000_store_name"
  FROM
      "busy"
  GROUP BY
      1),
  charming as (
  SELECT
      "busy"."yr2000_item_id" as "yr2000_item_id"
  FROM
      "busy"
  GROUP BY
      1)
  SELECT
      "kaput"."yr1999_product_name" as "yr1999_product_name",
      "kaput"."yr1999_store_name" as "yr1999_store_name",
      "kaput"."yr1999_store_zip" as "yr1999_store_zip",
      "kaput"."yr1999_agg_pos_street_num" as "yr1999_agg_pos_street_num",
      "kaput"."yr1999_agg_pos_street_name" as "yr1999_agg_pos_street_name",
      "kaput"."yr1999_agg_pos_city" as "yr1999_agg_pos_city",
      "kaput"."yr1999_agg_pos_zip" as "yr1999_agg_pos_zip",
      "kaput"."yr1999_agg_cur_street_num" as "yr1999_agg_cur_street_num",
      "kaput"."yr1999_agg_cur_street_name" as "yr1999_agg_cur_street_name",
      "kaput"."yr1999_agg_cur_city" as "yr1999_agg_cur_city",
      "kaput"."yr1999_agg_cur_zip" as "yr1999_agg_cur_zip",
      "kaput"."yr1999_yr" as "yr1999",
      "kaput"."yr1999_cnt" as "cnt1999",
      "kaput"."yr1999_ws" as "ws1999",
      "kaput"."yr1999_lp" as "lp1999",
      "kaput"."yr1999_cp" as "cp1999",
      "busy"."yr2000_ws" as "ws2000",
      "busy"."yr2000_lp" as "lp2000",
      "busy"."yr2000_cp" as "cp2000",
      "busy"."yr2000_yr" as "yr2000",
      "busy"."yr2000_cnt" as "cnt2000"
  FROM
      "kaput"
  WHERE
      exists (select 1 from busy where busy."yr2000_item_id" is not distinct from "kaput"."yr1999_item_id") and exists (select 1 from busy where busy."yr2000_store_name" is not distinct from "kaput"."yr1999_store_name") and exists (select 1 from busy where busy."yr2000_store_zip" is not distinct from "kaput"."yr1999_store_zip") and "busy"."yr2000_cnt" <= "kaput"."yr1999_cnt"

  ORDER BY
      "kaput"."yr1999_product_name" asc,
      "kaput"."yr1999_store_name" asc,
      "cnt2000" asc,
      "ws1999" asc,
      "ws2000" asc]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run answer_3544057080.preql`

  ```text
  Unexpected error in answer_3544057080.preql: (_duckdb.BinderException) Binder Error: Referenced table "protective" not found!
  Candidate tables: "divergent"

  LINE 324: ...szip" is not distinct from "divergent"."y99_agg_szip") and "protective"."y00_agg_lc" <= "divergent"."y99_agg_lc"
                                                                          ^
  [SQL:
  WITH
  wakeful as (
  SELECT
      coalesce("cs_catalog_returns"."CR_ITEM_SK","cs_catalog_sales"."CS_ITEM_SK") as "_cat_agg_item_sk",
      sum("cs_catalog_sales"."CS_EXT_LIST_PRICE") as "_cat_agg_cat_list",
      sum(( coalesce("cs_catalog_returns"."CR_REFUNDED_CASH",0) + coalesce("cs_catalog_returns"."CR_REVERSED_CHARGE",0) ) + coalesce("cs_catalog_returns"."CR_STORE_CREDIT",0)) as "_cat_agg_cat_refund"
  FROM
      "catalog_sales" as "cs_catalog_sales"
      LEFT OUTER JOIN "catalog_returns" as "cs_catalog_returns" on "cs_catalog_sales"."CS_ITEM_SK" = "cs_catalog_returns"."CR_ITEM_SK" AND "cs_catalog_sales"."CS_ORDER_NUMBER" = "cs_catalog_returns"."CR_ORDER_NUMBER"
  GROUP BY
      1),
  thoughtful as (
  SELECT
      "wakeful"."_cat_agg_cat_list" as "cat_agg_cat_list",
      "wakeful"."_cat_agg_cat_refund" as "cat_agg_cat_refund",
      "wakeful"."_cat_agg_item_sk" as "cat_agg_item_sk"
  FROM
      "wakeful"),
  cooperative as (
  SELECT
      "thoughtful"."cat_agg_item_sk" as "qual_item_sk"
  FROM
      "thoughtful"
  WHERE
      "thoughtful"."cat_agg_cat_list" > 2 * "thoughtful"."cat_agg_cat_refund"

  GROUP BY
      1),
  macho as (
  SELECT
      "ss_customer_current_address_customer_address"."CA_CITY" as "ss_customer_current_address_city",
      "ss_customer_current_address_customer_address"."CA_STREET_NAME" as "ss_customer_current_address_street_name",
      "ss_customer_current_address_customer_address"."CA_STREET_NUMBER" as "ss_customer_current_address_street_number",
      "ss_customer_current_address_customer_address"."CA_ZIP" as "ss_customer_current_address_zip",
      "ss_customer_first_sales_date_date"."D_YEAR" as "ss_customer_first_sales_date_year",
      "ss_customer_first_shipto_date_date"."D_YEAR" as "ss_customer_first_shipto_date_year",
      "ss_date_date"."D_YEAR" as "ss_date_year",
      "ss_item_items"."I_ITEM_ID" as "ss_item_id",
      "ss_item_items"."I_PRODUCT_NAME" as "ss_item_product_name",
      "ss_pos_address_customer_address"."CA_CITY" as "ss_pos_address_city",
      "ss_pos_address_customer_address"."CA_STREET_NAME" as "ss_pos_address_street_name",
      "ss_pos_address_customer_address"."CA_STREET_NUMBER" as "ss_pos_address_street_number",
      "ss_pos_address_customer_address"."CA_ZIP" as "ss_pos_address_zip",
      "ss_store_sales"."SS_COUPON_AMT" as "ss_coupon_amt",
      "ss_store_sales"."SS_LIST_PRICE" as "ss_list_price",
      "ss_store_sales"."SS_WHOLESALE_COST" as "ss_wholesale_cost",
      "ss_store_store"."S_STORE_NAME" as "ss_store_name",
      "ss_store_store"."S_ZIP" as "ss_store_zip",
      coalesce("ss_item_items"."I_ITEM_SK","ss_store_returns"."SR_ITEM_SK","ss_store_sales"."SS_ITEM_SK") as "ss_item_sk",
      coalesce("ss_store_returns"."SR_TICKET_NUMBER","ss_store_sales"."SS_TICKET_NUMBER") as "ss_ticket_number"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "customer" as "ss_customer_customers" on "ss_store_sales"."SS_CUSTOMER_SK" = "ss_customer_customers"."C_CUSTOMER_SK"
      INNER JOIN "store_returns" as "ss_store_returns" on "ss_store_sales"."SS_ITEM_SK" = "ss_store_returns"."SR_ITEM_SK" AND "ss_store_sales"."SS_TICKET_NUMBER" = "ss_store_returns"."SR_TICKET_NUMBER"
      INNER JOIN "customer_demographics" as "ss_pos_customer_demographic_customer_demographics" on "ss_store_sales"."SS_CDEMO_SK" = "ss_pos_customer_demographic_customer_demographics"."CD_DEMO_SK"
      INNER JOIN "customer_demographics" as "ss_customer_current_demographics_customer_demographics" on "ss_customer_customers"."C_CURRENT_CDEMO_SK" = "ss_customer_current_demographics_customer_demographics"."CD_DEMO_SK"
      INNER JOIN "item" as "ss_item_items" on "ss_store_sales"."SS_ITEM_SK" = "ss_item_items"."I_ITEM_SK"
      LEFT OUTER JOIN "date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "store" as "ss_store_store" on "ss_store_sales"."SS_STORE_SK" = "ss_store_store"."S_STORE_SK"
      LEFT OUTER JOIN "customer_address" as "ss_pos_address_customer_address" on "ss_store_sales"."SS_ADDR_SK" = "ss_pos_address_customer_address"."CA_ADDRESS_SK"
      LEFT OUTER JOIN "customer_address" as "ss_customer_current_address_customer_address" on "ss_customer_customers"."C_CURRENT_ADDR_SK" = "ss_customer_current_address_customer_address"."CA_ADDRESS_SK"
      LEFT OUTER JOIN "date_dim" as "ss_customer_first_sales_date_date" on "ss_customer_customers"."C_FIRST_SALES_DATE_SK" = "ss_customer_first_sales_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "date_dim" as "ss_customer_first_shipto_date_date" on "ss_customer_customers"."C_FIRST_SHIPTO_DATE_SK" = "ss_customer_first_shipto_date_date"."D_DATE_SK"
  WHERE
      exists (select 1 from cooperative where cooperative."qual_item_sk" is not distinct from coalesce("ss_item_items"."I_ITEM_SK","ss_store_returns"."SR_ITEM_SK","ss_store_sales"."SS_ITEM_SK")) and ("ss_item_items"."I_COLOR" is not null and "ss_item_items"."I_COLOR" in ('purple','burlywood','indian','spring','floral','medium')) and "ss_item_items"."I_CURRENT_PRICE" BETWEEN 65 AND 74 and "ss_store_returns"."SR_TICKET_NUMBER" is not null and "ss_pos_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" != "ss_customer_current_demographics_customer_demographics"."CD_MARITAL_STATUS"

  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      10,
      11,
      12,
      13,
      14,
      15,
      16,
      17,
      18,
      19,
      20),
  scrawny as (
  SELECT
      "macho"."ss_customer_current_address_city" as "_agg_ccity",
      "macho"."ss_customer_current_address_street_name" as "_agg_cst_name",
      "macho"."ss_customer_current_address_street_number" as "_agg_cst_num",
      "macho"."ss_customer_current_address_zip" as "_agg_czip",
      "macho"."ss_customer_first_sales_date_year" as "_agg_fs_yr",
      "macho"."ss_customer_first_shipto_date_year" as "_agg_fsh_yr",
      "macho"."ss_date_year" as "_agg_yr",
      "macho"."ss_item_id" as "_agg_iid",
      "macho"."ss_item_product_name" as "_agg_pname",
      "macho"."ss_pos_address_city" as "_agg_pcity",
      "macho"."ss_pos_address_street_name" as "_agg_pst_name",
      "macho"."ss_pos_address_street_number" as "_agg_pst_num",
      "macho"."ss_pos_address_zip" as "_agg_pzip",
      "macho"."ss_store_name" as "_agg_sname",
      "macho"."ss_store_zip" as "_agg_szip",
      count(md5(CONCAT_WS('', coalesce(cast("macho"."ss_ticket_number" as string),'
  '), coalesce(cast("macho"."ss_item_sk" as string),'
  ')))) as "_agg_lc",
      sum("macho"."ss_coupon_amt") as "_agg_cp",
      sum("macho"."ss_list_price") as "_agg_lp",
      sum("macho"."ss_wholesale_cost") as "_agg_ws"
  FROM
      "macho"
  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      10,
      11,
      12,
      13,
      14,
      15),
  busy as (
  SELECT
      "scrawny"."_agg_ccity" as "agg_ccity",
      "scrawny"."_agg_cp" as "agg_cp",
      "scrawny"."_agg_cst_name" as "agg_cst_name",
      "scrawny"."_agg_cst_num" as "agg_cst_num",
      "scrawny"."_agg_czip" as "agg_czip",
      "scrawny"."_agg_fs_yr" as "agg_fs_yr",
      "scrawny"."_agg_fsh_yr" as "agg_fsh_yr",
      "scrawny"."_agg_iid" as "agg_iid",
      "scrawny"."_agg_lc" as "agg_lc",
      "scrawny"."_agg_lp" as "agg_lp",
      "scrawny"."_agg_pcity" as "agg_pcity",
      "scrawny"."_agg_pname" as "agg_pname",
      "scrawny"."_agg_pst_name" as "agg_pst_name",
      "scrawny"."_agg_pst_num" as "agg_pst_num",
      "scrawny"."_agg_pzip" as "agg_pzip",
      "scrawny"."_agg_sname" as "agg_sname",
      "scrawny"."_agg_szip" as "agg_szip",
      "scrawny"."_agg_ws" as "agg_ws"
  FROM
      "scrawny"
  WHERE
      "scrawny"."_agg_yr" = 2000
  ),
  friendly as (
  SELECT
      "scrawny"."_agg_ccity" as "agg_ccity",
      "scrawny"."_agg_cp" as "agg_cp",
      "scrawny"."_agg_cst_name" as "agg_cst_name",
      "scrawny"."_agg_cst_num" as "agg_cst_num",
      "scrawny"."_agg_czip" as "agg_czip",
      "scrawny"."_agg_fs_yr" as "agg_fs_yr",
      "scrawny"."_agg_fsh_yr" as "agg_fsh_yr",
      "scrawny"."_agg_iid" as "agg_iid",
      "scrawny"."_agg_lc" as "agg_lc",
      "scrawny"."_agg_lp" as "agg_lp",
      "scrawny"."_agg_pcity" as "agg_pcity",
      "scrawny"."_agg_pname" as "agg_pname",
      "scrawny"."_agg_pst_name" as "agg_pst_name",
      "scrawny"."_agg_pst_num" as "agg_pst_num",
      "scrawny"."_agg_pzip" as "agg_pzip",
      "scrawny"."_agg_sname" as "agg_sname",
      "scrawny"."_agg_szip" as "agg_szip",
      "scrawny"."_agg_ws" as "agg_ws"
  FROM
      "scrawny"
  WHERE
      "scrawny"."_agg_yr" = 1999
  ),
  charming as (
  SELECT
      "busy"."agg_cp" as "agg_cp",
      "busy"."agg_iid" as "agg_iid",
      "busy"."agg_lc" as "agg_lc",
      "busy"."agg_lp" as "agg_lp",
      "busy"."agg_sname" as "agg_sname",
      "busy"."agg_szip" as "agg_szip",
      "busy"."agg_ws" as "agg_ws"
  FROM
      "busy"
  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      "busy"."agg_ccity",
      "busy"."agg_cst_name",
      "busy"."agg_cst_num",
      "busy"."agg_czip",
      "busy"."agg_fs_yr",
      "busy"."agg_fsh_yr",
      "busy"."agg_pcity",
      "busy"."agg_pname",
      "busy"."agg_pst_name",
      "busy"."agg_pst_num",
      "busy"."agg_pzip"),
  kaput as (
  SELECT
      "friendly"."agg_ccity" as "agg_ccity",
      "friendly"."agg_cp" as "agg_cp",
      "friendly"."agg_cst_name" as "agg_cst_name",
      "friendly"."agg_cst_num" as "agg_cst_num",
      "friendly"."agg_czip" as "agg_czip",
      "friendly"."agg_iid" as "agg_iid",
      "friendly"."agg_lc" as "agg_lc",
      "friendly"."agg_lp" as "agg_lp",
      "friendly"."agg_pcity" as "agg_pcity",
      "friendly"."agg_pname" as "agg_pname",
      "friendly"."agg_pst_name" as "agg_pst_name",
      "friendly"."agg_pst_num" as "agg_pst_num",
      "friendly"."agg_pzip" as "agg_pzip",
      "friendly"."agg_sname" as "agg_sname",
      "friendly"."agg_szip" as "agg_szip",
      "friendly"."agg_ws" as "agg_ws"
  FROM
      "friendly"
  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      10,
      11,
      12,
      13,
      14,
      15,
      16,
      "friendly"."agg_fs_yr",
      "friendly"."agg_fsh_yr"),
  protective as (
  SELECT
      "charming"."agg_cp" as "y00_agg_cp",
      "charming"."agg_iid" as "y00_agg_iid",
      "charming"."agg_lc" as "y00_agg_lc",
      "charming"."agg_lp" as "y00_agg_lp",
      "charming"."agg_sname" as "y00_agg_sname",
      "charming"."agg_szip" as "y00_agg_szip",
      "charming"."agg_ws" as "y00_agg_ws"
  FROM
      "charming"),
  divergent as (
  SELECT
      "kaput"."agg_ccity" as "y99_agg_ccity",
      "kaput"."agg_cp" as "y99_agg_cp",
      "kaput"."agg_cst_name" as "y99_agg_cst_name",
      "kaput"."agg_cst_num" as "y99_agg_cst_num",
      "kaput"."agg_czip" as "y99_agg_czip",
      "kaput"."agg_iid" as "y99_agg_iid",
      "kaput"."agg_lc" as "y99_agg_lc",
      "kaput"."agg_lp" as "y99_agg_lp",
      "kaput"."agg_pcity" as "y99_agg_pcity",
      "kaput"."agg_pname" as "y99_agg_pname",
      "kaput"."agg_pst_name" as "y99_agg_pst_name",
      "kaput"."agg_pst_num" as "y99_agg_pst_num",
      "kaput"."agg_pzip" as "y99_agg_pzip",
      "kaput"."agg_sname" as "y99_agg_sname",
      "kaput"."agg_szip" as "y99_agg_szip",
      "kaput"."agg_ws" as "y99_agg_ws"
  FROM
      "kaput"),
  waggish as (
  SELECT
      "protective"."y00_agg_szip" as "y00_agg_szip"
  FROM
      "protective"
  GROUP BY
      1),
  puzzled as (
  SELECT
      "protective"."y00_agg_sname" as "y00_agg_sname"
  FROM
      "protective"
  GROUP BY
      1),
  premium as (
  SELECT
      "protective"."y00_agg_iid" as "y00_agg_iid"
  FROM
      "protective"
  GROUP BY
      1)
  SELECT
      "divergent"."y99_agg_pname" as "y99_agg_pname",
      "divergent"."y99_agg_sname" as "y99_agg_sname",
      "divergent"."y99_agg_szip" as "y99_agg_szip",
      "divergent"."y99_agg_pst_num" as "y99_agg_pst_num",
      "divergent"."y99_agg_pst_name" as "y99_agg_pst_name",
      "divergent"."y99_agg_pcity" as "y99_agg_pcity",
      "divergent"."y99_agg_pzip" as "y99_agg_pzip",
      "divergent"."y99_agg_cst_num" as "y99_agg_cst_num",
      "divergent"."y99_agg_cst_name" as "y99_agg_cst_name",
      "divergent"."y99_agg_ccity" as "y99_agg_ccity",
      "divergent"."y99_agg_czip" as "y99_agg_czip",
      1999 as "yr99",
      "divergent"."y99_agg_lc" as "cnt99",
      "divergent"."y99_agg_ws" as "ws99",
      "divergent"."y99_agg_lp" as "lp99",
      "divergent"."y99_agg_cp" as "cp99",
      "protective"."y00_agg_ws" as "ws00",
      "protective"."y00_agg_lp" as "lp00",
      "protective"."y00_agg_cp" as "cp00",
      2000 as "yr00",
      "protective"."y00_agg_lc" as "cnt00"
  FROM
      "divergent"
  WHERE
      exists (select 1 from protective where protective."y00_agg_iid" is not distinct from "divergent"."y99_agg_iid") and exists (select 1 from protective where protective."y00_agg_sname" is not distinct from "divergent"."y99_agg_sname") and exists (select 1 from protective where protective."y00_agg_szip" is not distinct from "divergent"."y99_agg_szip") and "protective"."y00_agg_lc" <= "divergent"."y99_agg_lc"

  ORDER BY
      "divergent"."y99_agg_pname" asc,
      "divergent"."y99_agg_sname" asc,
      "cnt00" asc,
      "ws99" asc,
      "ws00" asc]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run --import raw.store_sales:ss select ss.store.county, ss.state, count(grain(ss.ticket_number, ss.item.sk)) as sales_lines where ss.date.year = 2000 and ss.store.sk is not null limit 20;`

  ```text
  Syntax error in stdin: Undefined concept: ss.state (line 2, col 25, in SELECT). Suggestions: ['ss.store.state', 'ss.return_store.state', 'ss.pos_address.state', 'ss.return_address.state', 'ss.customer.current_address.state', 'ss.return_customer.current_address.state']
  ```
- `trilogy file read answer_42596196.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe catalog_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_2091535883.preql`

  ```text
  Syntax error in answer_2091535883.preql: Undefined concept: cs.desc. Suggestions: ['cs.item.desc', 'cs.return_reason.desc', 'cs.date.sk', 'inv.item.desc']
  ```
- `trilogy run answer_1772060640.preql`

  ```text
  Syntax error in answer_1772060640.preql: ORDER BY references 'ss.customer.sk', which is not in the SELECT projection (line 3). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --ss.customer.sk order by ss.customer.sk asc`.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_2869182220.preql`

  ```text
  Syntax error in answer_2869182220.preql: 6 undefined concept references; fix all before re-running:
    - customer.last_name (line 19, in SELECT); did you mean: ss.customer.last_name, customer.first_name, customer.id, ss.return_customer.last_name, ss.customer.first_name?
    - customer.first_name (line 19, in SELECT); did you mean: ss.customer.first_name, customer.last_name, customer.id, ss.return_customer.first_name, ss.customer.last_name?
    - customer.current_demographics.sk (line 13, col 33, in WHERE); did you mean: ss.customer.current_demographics.sk, customer.current_address.city, customer.current_household_demographics.income_band.upper_bound, customer.current_household_demographics.income_band.lower_bound, ss.return_customer.current_demographics.sk, ss.customer.current_household_demographics.sk?
    - customer.current_address.city (line 14, col 9, in WHERE); did you mean: ss.customer.current_address.city, customer.current_demographics.sk, ss.return_customer.current_address.city, ss.return_address.city, ss.pos_address.city, ss.store.city?
    - customer.current_household_demographics.income_band.lower_bound (line 15, col 9, in WHERE); did you mean: ss.customer.current_household_demographics.income_band.lower_bound, customer.current_household_demographics.income_band.upper_bound, customer.current_demographics.sk, ss.return_customer.current_household_demographics.income_band.lower_bound, ss.return_household_demographic.income_band.lower_bound, ss.pos_household_demographic.income_band.lower_bound?
    - customer.current_household_demographics.income_band.upper_bound (line 16, col 9, in WHERE); did you mean: ss.customer.current_household_demographics.income_band.upper_bound, customer.current_household_demographics.income_band.lower_bound, customer.current_demographics.sk, ss.return_customer.current_household_demographics.income_band.upper_bound, ss.return_household_demographic.income_band.upper_bound, ss.pos_household_demographic.income_band.upper_bound?
  ```
- `trilogy run answer_2869182220.preql`

  ```text
  Resolution error in answer_2869182220.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 13). The requested concepts split into 2 disconnected subgraphs: {c.current_address.city, c.current_demographics.sk, c.current_household_demographics.income_band.lower_bound, c.current_household_demographics.income_band.upper_bound, customer_code, full_name}; {_item_sk, _ticket, store_returns.ret_demo_sk}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run answer_569612608.preql`

  ```text
  Resolution error in answer_569612608.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 12). The requested concepts split into 2 disconnected subgraphs: {eligible_orders.has_return, eligible_orders.wh_count}; {ws.order_number, ws.pos_ship_address.state, ws.ship_date.date, ws.web_site.company_name}. Are you missing a join or merge statement to relate them?
  Note: the membership predicate(s) `(ws.order_number) in (eligible_orders.order_id)` span these subgraphs, but membership only filters rows on its left side — it does not join the two sides, so it cannot relate them for outputs or grouping. To combine values from both sides, author a query-scoped join or a merge on shared keys.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy run --import raw/all_sales:all_sales select all_sales.channel, all_sales.channel_dim_text_id, sum(all_sales.ext_sales_price) as total_sales, sum(coal…where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date and all_sales.channel_dim_text_id is not null group by all_sales.channel;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...annel_dim_text_id is not null ??? group by all_sales.channel;
  ```
- `trilogy file write answer_219203908.preql --content import raw.item as item;
import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sal… ws.item.class_id, ws.item.category_id)
) -> (brand_id, class_id, category_id);

# Debug: count common tuples
select count(*) as common_tuple_count;
`

  ```text
  refused to write 'answer_219203908.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a NON-NULL GRAIN KEY: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). It MUST be a key, and one that is not nullable: `count(x)` skips rows where `x` is NULL, so counting a nullable property (a name, a date, any optional field) silently undercounts. When the grain takes SEVERAL keys, name them with `grain(...)`: `count(grain(order_id, item.id))` counts order+item combinations, and `count_distinct(grain(first_name, last_name, sale_date))` counts distinct combinations - `grain()` is never NULL, so combinations with a missing member still count. For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...g: count common tuples select ??? count(*) as common_tuple_count...

  Write stats: received 1104 chars / 1104 bytes; tail: …' count common tuples\\nselect count(*) as common_tuple_count;\\n'.
  ```
- `trilogy file write answer_219203908.preql --content import raw.all_sales as sales;
import raw.item as item;

# Step 1: Find (brand_id, class_id, category_id)… overall_avg
order by channel nulls first,
         brand_id nulls first,
         class_id nulls first,
         category_id nulls first
limit 100;
`

  ```text
  refused to write 'answer_219203908.preql': not syntactically valid Trilogy.

  Parse error:
    --> 38:11
     |
  38 | ) by lower(sales.channel), sales.item.brand_id, sales.item.class_id, sales.item.category_id;
     |           ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, window_sql_over, or OVER_COMPONENT_REF
  Location:
  ...tuples.category_id) ) by lower ??? (sales.channel), sales.item.br...

  Write stats: received 3095 chars / 3095 bytes; tail: …'id nulls first,\\n         category_id nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_219203908.preql --content import raw.all_sales as sales;
import raw.item as item;

# Step 1: Find (brand_id, class_id, category_id)… overall_avg
order by channel nulls first,
         brand_id nulls first,
         class_id nulls first,
         category_id nulls first
limit 100;
`

  ```text
  refused to write 'answer_219203908.preql': not syntactically valid Trilogy.

  Parse error:
    --> 35:9
     |
  35 | by lower(sales.channel), sales.item.brand_id, sales.item.class_id, sales.item.category_id;
     |         ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, window_sql_over, or OVER_COMPONENT_REF
  Location:
  ...uples.category_id)))  by lower ??? (sales.channel), sales.item.br...

  Write stats: received 2974 chars / 2974 bytes; tail: …'id nulls first,\\n         category_id nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_219203908.preql --content import raw.all_sales as sales;
import raw.item as item;

# Step 1: Find (brand_id, class_id, category_id)… overall_avg
order by channel nulls first,
         brand_id nulls first,
         class_id nulls first,
         category_id nulls first
limit 100;
`

  ```text
  refused to write 'answer_219203908.preql': not syntactically valid Trilogy.

  Parse error:
    --> 35:26
     |
  35 | by (lower(sales.channel)), sales.item.brand_id, sales.item.class_id, sales.item.category_id;
     |                          ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or window_sql_over
  Location:
  ...)))  by (lower(sales.channel)) ??? , sales.item.brand_id, sales.i...

  Write stats: received 2962 chars / 2962 bytes; tail: …'id nulls first,\\n         category_id nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_219203908.preql --content import raw.all_sales as sales;
import raw.item as item;

# Step 1: Find (brand_id, class_id, category_id)… overall_avg
order by channel nulls first,
         brand_id nulls first,
         class_id nulls first,
         category_id nulls first
limit 100;
`

  ```text
  refused to write 'answer_219203908.preql': not syntactically valid Trilogy.

  Parse error:
    --> 35:26
     |
  35 | by (lower(sales.channel)), (sales.item.brand_id), (sales.item.class_id), (sales.item.category_id);
     |                          ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or window_sql_over
  Location:
  ...)))  by (lower(sales.channel)) ??? , (sales.item.brand_id), (sale...

  Write stats: received 2666 chars / 2666 bytes; tail: …'id nulls first,\\n         category_id nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_219203908.preql --content import raw.all_sales as sales;
import raw.item as item;

# Step 1: Find (brand_id, class_id, category_id)…category_id)
order by channel nulls first,
         brand_id nulls first,
         class_id nulls first,
         category_id nulls first
limit 100;
`

  ```text
  refused to write 'answer_219203908.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...tem_counter) as line_item_cnt ??? subset join lower(sales.channe...

  Hint: the `by rollup/cube/grouping sets` clause must come *before* HAVING in Trilogy (same order as SQL's `GROUP BY ... HAVING`). Reorder to:
      select <cols> by rollup (<keys>) having <cond> order by <cols> limit <n>;

  Write stats: received 2460 chars / 2460 bytes; tail: …'id nulls first,\\n         category_id nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_765177085.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;

# Store sales in 2001 that were returned i…qty_cv
order by matched.item_id asc nulls first,
         matched.item_desc asc nulls first,
         matched.store_state asc nulls first
limit 100;
`

  ```text
  refused to write 'answer_765177085.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...sk = cs_qualifying.cs_item_sk ??? union join ss_qualifying.cust_...

  Write stats: received 2077 chars / 2077 bytes; tail: …'st,\\n         matched.store_state asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_2928586490.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Part 1: Fre…000_2003_totals.period_total) from customer_2000_2003_totals or period_stats)
;

# Hmm, let me rethink. I'll compute the max separately then filter.
`

  ```text
  refused to write 'answer_2928586490.preql': not syntactically valid Trilogy.

  Parse error:
    --> 13:1
     |
  13 | by
     | ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...ticket_number) as order_count ??? by     ss.item.sk,     substri...

  Write stats: received 1537 chars / 1537 bytes; tail: …"et me rethink. I'll compute the max separately then filter.\\n".
  ```
- `trilogy file write answer_2928586490.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Part 1: Fre…tal_sales > 0
order by
    feb_sales.last_name asc nulls first,
    feb_sales.first_name asc nulls first,
    total_sales asc nulls first
limit 100;
`

  ```text
  refused to write 'answer_2928586490.preql': not syntactically valid Trilogy.

  Parse error:
    --> 14:1
     |
  14 | by
     | ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...ticket_number) as order_count ??? by     ss.item.sk,     substri...

  Write stats: received 3023 chars / 3023 bytes; tail: …'asc nulls first,\\n    total_sales asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_2928586490.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Part 1: Fre…tal_sales > 0
order by
    feb_sales.last_name asc nulls first,
    feb_sales.first_name asc nulls first,
    total_sales asc nulls first
limit 100;
`

  ```text
  refused to write 'answer_2928586490.preql': not syntactically valid Trilogy.

  Parse error:
    --> 14:1
     |
  14 | by
     | ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...ticket_number) as order_count ??? by     ss.item.sk,     (substr...

  Write stats: received 3010 chars / 3010 bytes; tail: …'asc nulls first,\\n    total_sales asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_2928586490.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Part 1: Fre…tal_sales > 0
order by
    feb_sales.last_name asc nulls first,
    feb_sales.first_name asc nulls first,
    total_sales asc nulls first
limit 100;
`

  ```text
  refused to write 'answer_2928586490.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...50 * (select max_period_total ??? from period_max) ;  # Part 3:

  Write stats: received 2856 chars / 2856 bytes; tail: …'asc nulls first,\\n    total_sales asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_2928586490.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Part 1: Fre…tal_sales > 0
order by
    feb_sales.last_name asc nulls first,
    feb_sales.first_name asc nulls first,
    total_sales asc nulls first
limit 100;
`

  ```text
  refused to write 'answer_2928586490.preql': not syntactically valid Trilogy.

  Parse error:
    --> 87:1
     |
  87 | by
     | ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...es.line_total) as total_sales ??? by     feb_sales.last_name,

  Write stats: received 2916 chars / 2916 bytes; tail: …'asc nulls first,\\n    total_sales asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_525180526.preql --content import raw.catalog_sales as cs;

# Per-item extended discount amount total (over the date window, manufac…m.manufacturer_id = 48
    and cs.discount_amount > 1.3 * item_avg_discount
select
    sum(cs.discount_amount) as total_extended_discount
limit 100;
`

  ```text
  refused to write 'answer_525180526.preql': not syntactically valid Trilogy.

  Parse error:
   --> 9:1
    |
  9 | Hmm, actually I need a different approach. Let me think about this more carefully.
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...s.item.manufacturer_id = 48;  ??? Hmm, actually I need a differe...

  Write stats: received 1608 chars / 1608 bytes; tail: …'m(cs.discount_amount) as total_extended_discount\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_751385098.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Distinct (cu…ombos.sale_date),
    (select ws_combos.lname, ws_combos.fname, ws_combos.sale_date)
) -> (lname, fname, sale_date)
select count(*) as combo_count
;
`

  ```text
  refused to write 'answer_751385098.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...) -> (lname, fname, sale_date) ??? select count(*) as combo_coun...

  Write stats: received 1385 chars / 1385 bytes; tail: …' (lname, fname, sale_date)\\nselect count(*) as combo_count\\n;\\n'.
  ```
- `trilogy file write answer_751385098.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Distinct (cu…combos.lname, ws_combos.fname, ws_combos.sale_date)
) -> (lname, fname, sale_date);

select count(*) as combo_count
from intersection_set
limit 100;
`

  ```text
  refused to write 'answer_751385098.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a NON-NULL GRAIN KEY: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). It MUST be a key, and one that is not nullable: `count(x)` skips rows where `x` is NULL, so counting a nullable property (a name, a date, any optional field) silently undercounts. When the grain takes SEVERAL keys, name them with `grain(...)`: `count(grain(order_id, item.id))` counts order+item combinations, and `count_distinct(grain(first_name, last_name, sale_date))` counts distinct combinations - `grain()` is never NULL, so combinations with a missing member still count. For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...e, fname, sale_date);  select ??? count(*) as combo_count from i...

  Write stats: received 1418 chars / 1418 bytes; tail: …'ct count(*) as combo_count\\nfrom intersection_set\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_1663418777.preql --content import raw.item as item;

# Items matching one of the eight profiles
auto profile_items <- item.id ? (
 …etween 1 and 500
    and item.manufact in shared_manufacts
select
    distinct item.product_name as product_name
order by
    product_name
limit 100;`

  ```text
  refused to write 'answer_1663418777.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...n shared_manufacts select     ??? distinct item.product_name as

  Write stats: received 1351 chars / 1351 bytes; tail: …'ct_name as product_name\\norder by\\n    product_name\\nlimit 100;'.
  ```
- `trilogy file write answer_3079393690.preql --content import raw.store_sales as store_sales;

# Define a macro for each day-of-week filtered sum of unit price…ay,
    @thursday_sales as thursday,
    @friday_sales as friday,
    @saturday_sales as saturday
order by
    store_name,
    store_code
limit 100;
`

  ```text
  refused to write 'answer_3079393690.preql': not syntactically valid Trilogy.

  Parse error:
    --> 18:5
     |
  18 |     @sunday_sales as sunday,
     |     ^---
     |
     = expected select_item, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...s.store.id as store_code,     ??? @sunday_sales as sunday,     @...

  Write stats: received 1184 chars / 1184 bytes; tail: …'saturday\\norder by\\n    store_name,\\n    store_code\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_374591292.preql --content import raw.store_sales as ss;

# Restrict to store.sk = 1
where ss.store.sk = 1

# Compute the 90% thresh…
union join best_10.pair_rank = worst_10.pair_rank
order by best_10.pair_rank asc, best_10.best_product desc, worst_10.worst_product desc
limit 100;
`

  ```text
  refused to write 'answer_374591292.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block - they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
   where pos_address.sk is null ??? auto threshold <- 0.9 * avg(ss...

  Write stats: received 1305 chars / 1305 bytes; tail: …'0.best_product desc, worst_10.worst_product desc\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_3560698360.preql --content import raw.store_sales as st;
import raw.web_sales as ws;

# Web sales: per-item per-day, year 2000, ide… = store_rt.sale_date
having web_running_max > store_running_max
order by web_rt.item_sk asc nulls first, web_rt.sale_date asc nulls first
limit 100;`

  ```text
  refused to write 'answer_3560698360.preql': not syntactically valid Trilogy.

  Parse error:
    --> 30:174
     |
  30 |     max(sum(web_daily.daily_price_sum) over (partition by web_daily.item_sk order by web_daily.sale_date)) over (partition by web_daily.item_sk order by web_daily.sale_date rows between unbounded preceding and current row) as web_rmax
     |                                                                                                                                                                              ^---
     |
     = expected ORDER_DIRECTION_LA, ORDERING_DIRECTION, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
   order by web_daily.sale_date ??? rows between unbounded precedi...

  Write stats: received 1970 chars / 1970 bytes; tail: …'asc nulls first, web_rt.sale_date asc nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw/web_sales:ws select ws.item.sk, ws.date.date, sum(ws.sales_price) as dp sum(sum(dp)) over (partition by ws.item.sk order by ws.date.date) as rt where ws.date.year = 2000 and ws.item.sk is not null limit 10;`

  ```text
  Syntax error in stdin:  --> 2:60
    |
  2 | select ws.item.sk, ws.date.date, sum(ws.sales_price) as dp sum(sum(dp)) over (partition by ws.item.sk order by ws.date.date) as rt where ws.date.year = 2000 and ws.item.sk is not null limit 10;
    |                                                            ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...te, sum(ws.sales_price) as dp ??? sum(sum(dp)) over (partition b...
  ```
- `trilogy run --import raw/web_sales:ws with web_daily as where ws.date.year = 2000 and ws.item.sk is not null select ws.item.sk as item_sk, ws.date.date as sa…sk, web_daily.sale_date, sum(web_daily.daily_price) over (partition by web_daily.item_sk order by web_daily.sale_date) as rt from web_daily limit 10;`

  ```text
  Syntax error in stdin: Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...by web_daily.sale_date) as rt ??? from web_daily limit 10;
  ```
- `trilogy run --import raw/web_sales:ws with web_daily as where ws.date.year = 2000 and ws.item.sk is not null select ws.item.sk as item_sk, ws.date.date as sa…ly.daily_price) over (partition by web_daily.item_sk order by web_daily.sale_date) as rt select web_rt.item_sk, web_rt.sale_date, web_rt.rt limit 10;`

  ```text
  Syntax error in stdin:  --> 2:327
    |
  2 | with web_daily as where ws.date.year = 2000 and ws.item.sk is not null select ws.item.sk as item_sk, ws.date.date as sale_date, sum(ws.sales_price) as daily_price; with web_rt as select web_daily.item_sk, web_daily.sale_date, sum(web_daily.daily_price) over (partition by web_daily.item_sk order by web_daily.sale_date) as rt select web_rt.item_sk, web_rt.sale_date, web_rt.rt limit 10;
    |                                                                                                                                                                                                                                                                                                                                       ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...by web_daily.sale_date) as rt ??? select web_rt.item_sk, web_rt....
  ```
- `trilogy file write answer_3553309440.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;
import raw.ite…re.state = ss.customer.current_address.state
select
    ss.customer.sk as cust_sk,
    sum(ss.ext_sales_price) as total_price
order by
    cust_sk
;
`

  ```text
  refused to write 'answer_3553309440.preql': not syntactically valid Trilogy.

  Parse error:
    --> 22:1
     |
  22 | w_target_custs as
     | ^---
     |
     = expected EOI, block, or show_statement
  Location:
  ...ing_customer.sk as cust_sk ;  ??? w_target_custs as where     it...

  Write stats: received 1346 chars / 1346 bytes; tail: …'m(ss.ext_sales_price) as total_price\\norder by\\n    cust_sk\\n;\\n'.
  ```
- `trilogy run --import raw.store_sales:ss select ss.store.name, ss.store.id, count(ss.ticket_number) where ss.store.name is null and ss.store.id is not null group by ss.store.name, ss.store.id limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...l and ss.store.id is not null ??? group by ss.store.name, ss.sto...
  ```
- `trilogy run --import raw.store_sales:store_sales select distinct store_sales.item.brand_name limit 30;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ..._sales as store_sales; select ??? distinct store_sales.item.bran...
  ```
- `trilogy file write answer_3544057080.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;

# Cumulative extended list price per item…year2000,
    yr2000.cnt00 as count2000
order by
    yr1999.product_name,
    yr1999.store_name,
    yr2000.cnt00,
    yr1999.ws99,
    yr2000.ws00
;`

  ```text
  refused to write 'answer_3544057080.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [226]: Misplaced `subset|union join`. The key looks fine - the join is in the wrong PLACE. A query-scoped join is part of a `select` statement, not a standalone statement and not a pre-`where` clause. Put it right after the select list (preferred, SQL-like): `where <filters> select <cols> subset join a.key = b.key`. The clause order is `where` -> `select` <cols> -> join(s) -> `having` -> `order by` -> `limit`; a join may also sit between `where` and `select`, but never before `where` and never on its own. Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...tore_name = yr2000.store_name ??? subset join yr1999.store_zip =...

  Write stats: received 4504 chars / 4504 bytes; tail: …'e_name,\\n    yr2000.cnt00,\\n    yr1999.ws99,\\n    yr2000.ws00\\n;'.
  ```
- `trilogy run --import raw/store_sales:ss select ss.store.id, count(grain(ss.store.sk, ss.item.sk, ss.ticket_number)) as cnt where ss.date.year = 2000 and ss.store.sk is not null group by ss.store.id order by cnt desc limit 20;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...0 and ss.store.sk is not null ??? group by ss.store.id order by
  ```
- `trilogy run --import raw.store_sales:ss select ss.store.county, count(grain(ss.ticket_number, ss.item.sk)) as sales_lines where ss.date.year = 2000 and ss.store.sk is not null group by ss.store.county limit 20;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...0 and ss.store.sk is not null ??? group by ss.store.county limit...
  ```
- `trilogy run --import raw.all_sales:all_sales where all_sales.item.category = 'Books' select all_sales.date.year, count(grain(all_sales.item.sk, all_sales.channel, all_sales.order_id)) as cnt by all_sales.date.year;`

  ```text
  Syntax error in stdin:  --> 2:147
    |
  2 | where all_sales.item.category = 'Books' select all_sales.date.year, count(grain(all_sales.item.sk, all_sales.channel, all_sales.order_id)) as cnt by all_sales.date.year;
    |                                                                                                                                                   ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ..., all_sales.order_id)) as cnt ??? by all_sales.date.year;
  ```
- `trilogy run --import raw.all_sales:all_sales where all_sales.item.category = 'Books' and all_sales.date.year in (2001,2002) select all_sales.is_returned, count(*) as cnt, count(all_sales.quantity) as qty_cnt, count(all_sales.return_quantity) as ret_qty_cnt limit 10;`

  ```text
  Syntax error in stdin: Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a NON-NULL GRAIN KEY: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). It MUST be a key, and one that is not nullable: `count(x)` skips rows where `x` is NULL, so counting a nullable property (a name, a date, any optional field) silently undercounts. When the grain takes SEVERAL keys, name them with `grain(...)`: `count(grain(order_id, item.id))` counts order+item combinations, and `count_distinct(grain(first_name, last_name, sale_date))` counts distinct combinations - `grain()` is never NULL, so combinations with a missing member still count. For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...select all_sales.is_returned, ??? count(*) as cnt, count(all_sal...
  ```
- `trilogy run --import raw.all_sales:all_sales where all_sales.item.category = 'Books' and all_sales.is_returned is not null and all_sales.date.year is not null select count(*) as cnt limit 5;`

  ```text
  Syntax error in stdin: Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a NON-NULL GRAIN KEY: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). It MUST be a key, and one that is not nullable: `count(x)` skips rows where `x` is NULL, so counting a nullable property (a name, a date, any optional field) silently undercounts. When the grain takes SEVERAL keys, name them with `grain(...)`: `count(grain(order_id, item.id))` counts order+item combinations, and `count_distinct(grain(first_name, last_name, sale_date))` counts distinct combinations - `grain()` is never NULL, so combinations with a missing member still count. For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ....date.year is not null select ??? count(*) as cnt limit 5;
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.item.sk, all_sales.order_id, all_sales.channel, count(grain(all_sales.item.sk, all_sales.order_id, all_sales.channel)) as cnt group by all_sales.item.sk, all_sales.order_id, all_sales.channel order by cnt desc limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...d, all_sales.channel)) as cnt ??? group by all_sales.item.sk, al...
  ```
- `trilogy file write answer_3046445280.preql --content import raw.all_sales as all_sales;

# Compute net sales quantity and amount per sale line
# For rows wit… y2002.qty is not null
  and cast(y2002.qty as numeric(18,4)) / cast(y2001.qty as numeric(18,4)) < 0.9
order by qty_diff asc, amt_diff asc
limit 100;`

  ```text
  refused to write 'answer_3046445280.preql': not syntactically valid Trilogy.

  Parse error:
    --> 34:1
     |
  34 | rowset y2001 as
     | ^---
     |
     = expected EOI, block, or show_statement
  Location:
  ...te into 2001 and 2002 rowsets ??? rowset y2001 as where yr_sales...

  Write stats: received 2460 chars / 2460 bytes; tail: …'(18,4)) < 0.9\\norder by qty_diff asc, amt_diff asc\\nlimit 100;'.
  ```
- `trilogy run --import raw/store_sales:ss select ss.date.date, ss.store.sk, sum(ss.ext_sales_price) as sales where ss.date.date between '2000-08-23'::date and '2000-09-22'::date and ss.store.sk is not null group by ss.date.date, ss.store.sk limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...e and ss.store.sk is not null ??? group by ss.date.date, ss.stor...
  ```
- `trilogy run --import raw/store_sales:ss --import raw/catalog_sales:cs --import raw/web_sales:ws with store_sales_agg as select ss.store.sk as outlet_sk, sum(…_amt, 0) as ret_amt, coalesce(store_returns_agg.ret_loss, 0) as ret_loss union join store_sales_agg.outlet_sk = store_returns_agg.outlet_sk limit 10;`

  ```text
  Syntax error in stdin:  --> 4:217
    |
  4 | with store_sales_agg as select ss.store.sk as outlet_sk, sum(ss.ext_sales_price) as sales, sum(ss.net_profit) as profit where ss.date.date between '2000-08-23'::date and '2000-09-22'::date and ss.store.sk is not null, store_returns_agg as select ss.return_store.sk as outlet_sk, sum(ss.return_amount) as ret_amt, sum(ss.return_net_loss) as ret_loss where ss.return_date.date between '2000-08-23'::date and '2000-09-22'::date and ss.return_store.sk is not null select store_sales_agg.outlet_sk, store_sales_agg.sales, store_sales_agg.profit, coalesce(store_returns_agg.ret_amt, 0) as ret_amt, coalesce(store_returns_agg.ret_loss, 0) as ret_loss union join store_sales_agg.outlet_sk = store_returns_agg.outlet_sk limit 10;
    |                                                                                                                                                                                                                         ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...te and ss.store.sk is not null ??? , store_returns_agg as select
  ```
- `trilogy file write test_check4.preql --content import raw.all_sales as s;
select distinct s.date.year, s.date.date
where s.channel = 'STORE'
limit 5;
`

  ```text
  refused to write 'test_check4.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...rt raw.all_sales as s; select ??? distinct s.date.year, s.date.d...

  Write stats: received 103 chars / 103 bytes; tail: …"s.date.year, s.date.date\\nwhere s.channel = 'STORE'\\nlimit 5;\\n".
  ```
- `trilogy run --import raw/store_sales:ss select ss.item.id, sum(ss.return_quantity) as store_ret_qty where ss.return_date.week_seq in (5244, 5257, 5264) group by ss.item.id limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...eek_seq in (5244, 5257, 5264) ??? group by ss.item.id limit 5;
  ```
- `trilogy file write answer_4111870542.preql --content import raw.store_sales as store;
import raw.catalog_sales as catalog;
import raw.web_sales as web;

# Pe…talog_qty as catalog_return_qty,
    web_returns.web_qty as web_return_qty
order by item_code asc, store_returns.store_qty asc nulls first
limit 100;`

  ```text
  refused to write 'answer_4111870542.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...e = catalog_returns.item_code ??? union join store_returns.item_...

  Write stats: received 1612 chars / 1612 bytes; tail: …'code asc, store_returns.store_qty asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write answer_2852230229.preql --content import raw.web_sales as ws;

# Filter to year 2000
where ws.date.year = 2000

# Define auto concepts for…y hlevel desc nulls first,
         case when hlevel = 0 then ws.item.category else null end asc nulls first,
         rnk asc nulls first
limit 100;`

  ```text
  refused to write 'answer_2852230229.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block - they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...efine auto concepts for reuse ??? auto total_net_paid <- sum(ws....

  Write stats: received 887 chars / 887 bytes; tail: …'end asc nulls first,\\n         rnk asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write answer_210925266.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Store sales …r.last_name, ws.billing_customer.first_name, ws.date.date)
) -> (last_name, first_name, sale_date);

select count(*) as unique_count from only_store;`

  ```text
  refused to write 'answer_210925266.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a NON-NULL GRAIN KEY: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). It MUST be a key, and one that is not nullable: `count(x)` skips rows where `x` is NULL, so counting a nullable property (a name, a date, any optional field) silently undercounts. When the grain takes SEVERAL keys, name them with `grain(...)`: `count(grain(order_id, item.id))` counts order+item combinations, and `count_distinct(grain(first_name, last_name, sale_date))` counts distinct combinations - `grain()` is never NULL, so combinations with a missing member still count. For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...rst_name, sale_date);  select ??? count(*) as unique_count from

  Write stats: received 887 chars / 887 bytes; tail: …'ale_date);\\n\\nselect count(*) as unique_count from only_store;'.
  ```
- `trilogy file write answer_210925266.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Store sales …ame, first_name, sale_date);

select count(grain(only_store.last_name, only_store.first_name, only_store.sale_date)) as unique_count from only_store;`

  ```text
  refused to write 'answer_210925266.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...e.sale_date)) as unique_count ??? from only_store;

  Write stats: received 958 chars / 958 bytes; tail: …'ame, only_store.sale_date)) as unique_count from only_store;'.
  ```
- `trilogy file write answer_569612608.preql --content import raw.web_sales as ws;

# Step 1: Eligible orders = more than one warehouse AND at least one web ret…ount,
    sum(ws.ext_ship_cost) as total_ext_ship_cost,
    sum(ws.net_profit) as total_net_profit
order by
    eligible_order_count desc
limit 100;
`

  ```text
  refused to write 'answer_569612608.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [226]: Misplaced `subset|union join`. The key looks fine - the join is in the wrong PLACE. A query-scoped join is part of a `select` statement, not a standalone statement and not a pre-`where` clause. Put it right after the select list (preferred, SQL-like): `where <filters> select <cols> subset join a.key = b.key`. The clause order is `where` -> `select` <cols> -> join(s) -> `having` -> `order by` -> `limit`; a join may also sit between `where` and `select`, but never before `where` and never on its own. Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...rder attributes with ws lines ??? subset join eligible_orders.or...

  Write stats: received 895 chars / 895 bytes; tail: …'et_profit\\norder by\\n    eligible_order_count desc\\nlimit 100;\\n'.
  ```

### `cli-misuse`

- `trilogy explore raw/`

  ```text
  Invalid value for 'PATH': File 'raw/' is a directory.
  ```
- `trilogy explore raw/`

  ```text
  Invalid value for 'PATH': File 'raw/' is a directory.
  ```
- `trilogy explore raw/catalog_returns.preql`

  ```text
  Invalid value for 'PATH': File 'raw/catalog_returns.preql' does not exist.
  ```
- `trilogy explore raw/store_returns.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_returns.preql' does not exist.
  ```

### `syntax-missing-alias`

- `trilogy file write answer_3979964698.preql --content import raw.all_sales as sales;

auto line_sales <- case
    when sales.channel = 'WEB' then sales.quanti…net,
    sum(line_net ? sales.date.month_of_year = 12) as dec_net,
order by sales.warehouse.name nulls first, sales.date.year nulls first
limit 100;
`

  ```text
  refused to write 'answer_3979964698.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `warehouse.sk hidden
      sales.warehouse.name as warehouse_sk_hidden_sales_warehouse_name`
  Location:
  ...') select     -- warehouse.sk ??? hidden     sales.warehouse.nam...

  Write stats: received 3938 chars / 3938 bytes; tail: …'se.name nulls first, sales.date.year nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_2869182220.preql --content import raw.store_sales as ss;

# CTE: store returns with their point-in-time customer demographic key
wi…ustomer_code,
    store_returns.ticket_number as _ticket,
    store_returns.item_sk as _item_sk
order by
    customer_code asc nulls first
limit 100;`

  ```text
  refused to write 'answer_2869182220.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `hidden grain fields to keep one row per (customer, ticket_number, item.sk)
      --customer.id as hidden_grain_fields_to_keep_one_row_per_`
  Location:
  ...) as full_name,     -- hidden ??? grain fields to keep one row p...

  Write stats: received 1165 chars / 1165 bytes; tail: …'tem_sk\\norder by\\n    customer_code asc nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw/store_sales:store_sales select store_sales.customer.sk, store_sales.return_reason.desc, store_sales.quantity, store_sales.sales_price, store_sales.return_quantity limit 10 where store_sales.return_reason.desc = 'reason 28';`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `store_sales.return_quantity as store_sales_return_quantity`
  Location:
  ..., store_sales.return_quantity ??? limit 10 where store_sales.ret...
  ```

### `type-error`

- `trilogy run --import raw/all_sales:all_sales select all_sales.return_date.date, all_sales.return_amount, all_sales.return_net_loss, all_sales.channel, all_sales.channel_dim_text_id, all_sales.return_channel_dim_text_id where all_sales.return_date.date between '2000-08-23' and '2000-09-06' limit 5;`

  ```text
  Syntax error in stdin: Cannot use BETWEEN with incompatible types DATE and STRING (low)
  ```
- `trilogy run answer_1197120511.preql`

  ```text
  Syntax error in answer_1197120511.preql: Cannot use BETWEEN with incompatible types DATE and STRING (low)
  ```

### `planner-recursion`

- `trilogy run answer_219203908.preql`

  ```text
  Resolution error in answer_219203908.preql: query could not be planned; this is a bug.
  ```
