# Trilogy failure analysis — 20260708-130405

- Run `20260708-130405` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 1194 | failed: 108 (9%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 59 | 55% |
| `syntax-parse` | 36 | 33% |
| `cli-misuse` | 6 | 6% |
| `syntax-missing-alias` | 4 | 4% |
| `type-error` | 3 | 3% |

## Detail

### `other`

- `trilogy run query04.preql`

  ```text
  Unexpected error in query04.preql: Conflicting join types (full, left outer) on keys joined into one group: a FULL/UNION join cannot be mixed with another type on the same key (it is ambiguous whether the key is required or one-sided). Make the whole group one type (e.g. `UNION JOIN a = b = c`), or use a distinct key.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query06.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query11.preql`

  ```text
  Resolution error in query11.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 29). The requested concepts split into 2 disconnected subgraphs: {all.billing_customer.id, store_2001_rev, store_2002_rev, web_2001_rev, web_2002_rev}; {cust_info.first_name, cust_info.last_name}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 5 column 26 (char 285). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query13.preql`

  ```text
  Syntax error in query13.preql: Comparison `ss.customer.demographics.marital_status = 'Married'` can never match enum field 'ss.customer.demographics.marital_status', which contains only these values: 'M', 'S', 'D', 'W', 'U'. It is always false and should be removed.
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: ORDER BY contains aggregate `grouping(s.channel)` (line 20), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(s.channel) as g order by g desc`.
  ```
- `trilogy run query14.preql`

  ```text
  Unexpected error in query14.preql: (_duckdb.BinderException) Binder Error: GROUPING function is not supported here
  [SQL:
  WITH
  uneven as (
  SELECT
      "s_item_items"."I_BRAND_ID" as "s_item_brand_id",
      "s_item_items"."I_CATEGORY_ID" as "s_item_category_id",
      "s_item_items"."I_CLASS_ID" as "s_item_class_id"
  FROM
      "store_sales" as "s_store_sales_unified"
      INNER JOIN "date_dim" as "s_date_date" on "s_store_sales_unified"."SS_SOLD_DATE_SK" = "s_date_date"."D_DATE_SK"
      INNER JOIN "item" as "s_item_items" on "s_store_sales_unified"."SS_ITEM_SK" = "s_item_items"."I_ITEM_SK"
  WHERE
      "s_date_date"."D_YEAR" BETWEEN 1999 AND 2001

  GROUP BY
      1,
      2,
      3),
  juicy as (
  SELECT
      "s_item_items"."I_BRAND_ID" as "s_item_brand_id",
      "s_item_items"."I_CATEGORY_ID" as "s_item_category_id",
      "s_item_items"."I_CLASS_ID" as "s_item_class_id"
  FROM
      "catalog_sales" as "s_catalog_sales_unified"
      INNER JOIN "date_dim" as "s_date_date" on "s_catalog_sales_unified"."CS_SOLD_DATE_SK" = "s_date_date"."D_DATE_SK"
      INNER JOIN "item" as "s_item_items" on "s_catalog_sales_unified"."CS_ITEM_SK" = "s_item_items"."I_ITEM_SK"
  WHERE
      "s_date_date"."D_YEAR" BETWEEN 1999 AND 2001

  GROUP BY
      1,
      2,
      3),
  concerned as (
  SELECT
      "s_item_items"."I_BRAND_ID" as "s_item_brand_id",
      "s_item_items"."I_CATEGORY_ID" as "s_item_category_id",
      "s_item_items"."I_CLASS_ID" as "s_item_class_id"
  FROM
      "web_sales" as "s_web_sales_unified"
      INNER JOIN "date_dim" as "s_date_date" on "s_web_sales_unified"."WS_SOLD_DATE_SK" = "s_date_date"."D_DATE_SK"
      INNER JOIN "item" as "s_item_items" on "s_web_sales_unified"."WS_ITEM_SK" = "s_item_items"."I_ITEM_SK"
  WHERE
      "s_date_date"."D_YEAR" BETWEEN 1999 AND 2001

  GROUP BY
      1,
      2,
      3),
  sparkling as (
  SELECT
      "uneven"."s_item_brand_id" as "_multi_channel_items_brand_id",
      "uneven"."s_item_class_id" as "_multi_channel_items_class_id",
      "uneven"."s_item_category_id" as "_multi_channel_items_category_id"
  FROM
      "uneven"
  INTERSECT
  SELECT
      "juicy"."s_item_brand_id" as "_multi_channel_items_brand_id",
      "juicy"."s_item_class_id" as "_multi_channel_items_class_id",
      "juicy"."s_item_category_id" as "_multi_channel_items_category_id"
  FROM
      "juicy"
  INTERSECT
  SELECT
      "concerned"."s_item_brand_id" as "_multi_channel_items_brand_id",
      "concerned"."s_item_class_id" as "_multi_channel_items_class_id",
      "concerned"."s_item_category_id" as "_multi_channel_items_category_id"
  FROM
      "concerned"),
  abhorrent as (
  SELECT
      "sparkling"."_multi_channel_items_brand_id" as "_multi_channel_items_brand_id",
      "sparkling"."_multi_channel_items_category_id" as "_multi_channel_items_category_id",
      "sparkling"."_multi_channel_items_class_id" as "_multi_channel_items_class_id"
  FROM
      "sparkling"),
  sweltering as (
  SELECT
      "abhorrent"."_multi_channel_items_brand_id" as "multi_channel_items_brand_id",
      "abhorrent"."_multi_channel_items_category_id" as "multi_channel_items_category_id",
      "abhorrent"."_multi_channel_items_class_id" as "multi_channel_items_class_id"
  FROM
      "abhorrent"),
  cheerful as (
  SELECT
       'CATALOG'  as "s_channel",
      "s_catalog_sales_unified"."CS_SOLD_DATE_SK" as "s_date_sk",
      "s_catalog_sales_unified"."CS_ITEM_SK" as "s_item_sk",
      "s_catalog_sales_unified"."CS_LIST_PRICE" as "s_list_price",
      "s_catalog_sales_unified"."CS_ORDER_NUMBER" as "s_order_id",
      "s_catalog_sales_unified"."CS_QUANTITY" as "s_quantity",
       1  as "s_sale_line_item_counter",
      "s_item_items"."I_BRAND_ID" as "s_item_brand_id",
      "s_item_items"."I_CATEGORY_ID" as "s_item_category_id",
      "s_item_items"."I_CLASS_ID" as "s_item_class_id"
  FROM
      "catalog_sales" as "s_catalog_sales_unified"
      INNER JOIN "item" as "s_item_items" on "s_catalog_sales_unified"."CS_ITEM_SK" = "s_item_items"."I_ITEM_SK"
  WHERE
      ("s_item_items"."I_BRAND_ID", "s_item_items"."I_CLASS_ID", "s_item_items"."I_CATEGORY_ID") in (select sweltering."multi_channel_items_brand_id", sweltering."multi_channel_items_class_id", sweltering."multi_channel_items_category_id" from sweltering where sweltering."multi_channel_items_brand_id" is not null and sweltering."multi_channel_items_class_id" is not null and sweltering."multi_channel_items_category_id" is not null)

  UNION ALL
  SELECT
       'STORE'  as "s_channel",
      "s_store_sales_unified"."SS_SOLD_DATE_SK" as "s_date_sk",
      "s_store_sales_unified"."SS_ITEM_SK" as "s_item_sk",
      "s_store_sales_unified"."SS_LIST_PRICE" as "s_list_price",
      "s_store_sales_unified"."SS_TICKET_NUMBER" as "s_order_id",
      "s_store_sales_unified"."SS_QUANTITY" as "s_quantity",
       1  as "s_sale_line_item_counter",
      "s_item_items"."I_BRAND_ID" as "s_item_brand_id",
      "s_item_items"."I_CATEGORY_ID" as "s_item_category_id",
      "s_item_items"."I_CLASS_ID" as "s_item_class_id"
  FROM
      "store_sales" as "s_store_sales_unified"
      INNER JOIN "item" as "s_item_items" on "s_store_sales_unified"."SS_ITEM_SK" = "s_item_items"."I_ITEM_SK"
  WHERE
      ("s_item_items"."I_BRAND_ID", "s_item_items"."I_CLASS_ID", "s_item_items"."I_CATEGORY_ID") in (select sweltering."multi_channel_items_brand_id", sweltering."multi_channel_items_class_id", sweltering."multi_channel_items_category_id" from sweltering where sweltering."multi_channel_items_brand_id" is not null and sweltering."multi_channel_items_class_id" is not null and sweltering."multi_channel_items_category_id" is not null)

  UNION ALL
  SELECT
       'WEB'  as "s_channel",
      "s_web_sales_unified"."WS_SOLD_DATE_SK" as "s_date_sk",
      "s_web_sales_unified"."WS_ITEM_SK" as "s_item_sk",
      "s_web_sales_unified"."WS_LIST_PRICE" as "s_list_price",
      "s_web_sales_unified"."WS_ORDER_NUMBER" as "s_order_id",
      "s_web_sales_unified"."WS_QUANTITY" as "s_quantity",
       1  as "s_sale_line_item_counter",
      "s_item_items"."I_BRAND_ID" as "s_item_brand_id",
      "s_item_items"."I_CATEGORY_ID" as "s_item_category_id",
      "s_item_items"."I_CLASS_ID" as "s_item_class_id"
  FROM
      "web_sales" as "s_web_sales_unified"
      INNER JOIN "item" as "s_item_items" on "s_web_sales_unified"."WS_ITEM_SK" = "s_item_items"."I_ITEM_SK"
  WHERE
      ("s_item_items"."I_BRAND_ID", "s_item_items"."I_CLASS_ID", "s_item_items"."I_CATEGORY_ID") in (select sweltering."multi_channel_items_brand_id", sweltering."multi_channel_items_class_id", sweltering."multi_channel_items_category_id" from sweltering where sweltering."multi_channel_items_brand_id" is not null and sweltering."multi_channel_items_class_id" is not null and sweltering."multi_channel_items_category_id" is not null)
  ),
  thoughtful as (
  SELECT
      "cheerful"."s_date_sk" as "s_date_sk"
  FROM
      "cheerful"
  GROUP BY
      1,
      "cheerful"."s_item_brand_id",
      "cheerful"."s_item_category_id",
      "cheerful"."s_item_class_id",
      "cheerful"."s_item_sk"),
  friendly as (
  SELECT
      "cheerful"."s_channel" as "s_channel",
      "cheerful"."s_item_brand_id" as "s_item_brand_id",
      "cheerful"."s_item_category_id" as "s_item_category_id",
      "cheerful"."s_item_class_id" as "s_item_class_id",
      "cheerful"."s_list_price" as "s_list_price",
      "cheerful"."s_quantity" as "s_quantity",
      "cheerful"."s_sale_line_item_counter" as "s_sale_line_item_counter"
  FROM
      "cheerful"
      INNER JOIN "date_dim" as "s_date_date" on "cheerful"."s_date_sk" = "s_date_date"."D_DATE_SK"
  WHERE
      "s_date_date"."D_YEAR" = 2001 and "s_date_date"."D_MOY" = 11

  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      "cheerful"."s_item_sk",
      "cheerful"."s_order_id"),
  late as (
  SELECT
      "cheerful"."s_list_price" as "s_list_price",
      "cheerful"."s_quantity" as "s_quantity"
  FROM
      "cheerful"
      INNER JOIN "date_dim" as "s_date_date" on "cheerful"."s_date_sk" = "s_date_date"."D_DATE_SK"
  WHERE
      "s_date_date"."D_YEAR" = 2001 and "s_date_date"."D_MOY" = 11

  GROUP BY
      1,
      2,
      "cheerful"."s_channel",
      "cheerful"."s_item_sk",
      "cheerful"."s_order_id"),
  abundant as (
  SELECT
      "s_date_date"."D_YEAR" as "s_date_year",
      1 as "__preql_internal_all_rows"
  FROM
      "thoughtful"
      INNER JOIN "date_dim" as "s_date_date" on "thoughtful"."s_date_sk" = "s_date_date"."D_DATE_SK"
  WHERE
      "s_date_date"."D_YEAR" = 2001 and "s_date_date"."D_MOY" = 11 and "s_date_date"."D_YEAR" >= 1999 and "s_date_date"."D_YEAR" <= 2001
  ),
  kaput as (
  SELECT
      "friendly"."s_channel" as "s_channel",
      "friendly"."s_item_brand_id" as "s_item_brand_id",
      "friendly"."s_item_category_id" as "s_item_category_id",
      "friendly"."s_item_class_id" as "s_item_class_id",
      sum("friendly"."s_quantity" * "friendly"."s_list_price") as "_leaf_groups_total_sales",
      sum("friendly"."s_sale_line_item_counter") as "_leaf_groups_num_sales"
  FROM
      "friendly"
  GROUP BY
      1,
      2,
      3,
      4),
  macho as (
  SELECT
      1 as "__preql_internal_all_rows",
      avg("late"."s_quantity" * "late"."s_list_price") as "_virt_agg_avg_777932281024922"
  FROM
      "late"),
  scrawny as (
  SELECT
      CASE WHEN "abundant"."s_date_year" >= 1999 and "abundant"."s_date_year" <= 2001 THEN "macho"."_virt_agg_avg_777932281024922" ELSE NULL END as "overall_avg_sale"
  FROM
      "abundant"
      LEFT OUTER JOIN "macho" on "abundant"."__preql_internal_all_rows" = "macho"."__preql_internal_all_rows"
  GROUP BY
      1),
  divergent as (
  SELECT
      "kaput"."_leaf_groups_num_sales" as "_leaf_groups_num_sales",
      "kaput"."_leaf_groups_total_sales" as "_leaf_groups_total_sales",
      "kaput"."s_channel" as "s_channel",
      "kaput"."s_item_brand_id" as "s_item_brand_id",
      "kaput"."s_item_category_id" as "s_item_category_id",
      "kaput"."s_item_class_id" as "s_item_class_id"
  FROM
      "kaput"
      INNER JOIN "scrawny" on 1=1
  WHERE
      "kaput"."_leaf_groups_total_sales" > "scrawny"."overall_avg_sale"
  ),
  busy as (
  SELECT
      "divergent"."_leaf_groups_num_sales" as "leaf_groups_num_sales",
      "divergent"."_leaf_groups_total_sales" as "leaf_groups_total_sales",
      "divergent"."s_channel" as "leaf_groups_s_channel",
      "divergent"."s_item_brand_id" as "leaf_groups_s_item_brand_id",
      "divergent"."s_item_category_id" as "leaf_groups_s_item_category_id",
      "divergent"."s_item_class_id" as "leaf_groups_s_item_class_id"
  FROM
      "divergent")
  SELECT
      "busy"."leaf_groups_s_channel" as "leaf_groups_s_channel",
      "busy"."leaf_groups_s_item_brand_id" as "leaf_groups_s_item_brand_id",
      "busy"."leaf_groups_s_item_class_id" as "leaf_groups_s_item_class_id",
      "busy"."leaf_groups_s_item_category_id" as "leaf_groups_s_item_category_id",
      sum("busy"."leaf_groups_total_sales") as "total_sales",
      sum("busy"."leaf_groups_num_sales") as "num_sales"
  FROM
      "busy"
  GROUP BY
      ROLLUP (1, 2, 3, 4)
  ORDER BY
      MIN(grouping_id("busy"."leaf_groups_s_channel","busy"."leaf_groups_s_item_brand_id","busy"."leaf_groups_s_item_class_id","busy"."leaf_groups_s_item_category_id")) asc,
      "busy"."leaf_groups_s_channel" asc nulls first,
      "busy"."leaf_groups_s_item_brand_id" asc nulls first,
      "busy"."leaf_groups_s_item_class_id" asc nulls first,
      "busy"."leaf_groups_s_item_category_id" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query17.preql`

  ```text
  Unexpected error in query17.preql: Could not render the query: Missing source reference to ss.quantity; Missing source reference to sr.return_quantity; Missing source reference to cs.quantity. A planned reference has no backing source CTE -- typically an unsupported cross-rowset or membership shape the planner could not wire. Review the rowset/join structure (or file an issue if the query looks valid).

  Full SQL with sentinel(s):

  WITH
  highfalutin as (
  SELECT
      "cs_catalog_sales"."CS_ITEM_SK" as "cs_item_sk",
      "cs_catalog_sales"."CS_SOLD_DATE_SK" as "cs_sold_date_sk"
  FROM
      "catalog_sales" as "cs_catalog_sales"
  GROUP BY
      1,
      2),
  vacuous as (
  SELECT
      "cs_catalog_sales"."CS_QUANTITY" as "cs_quantity",
      "ss_item_items"."I_ITEM_DESC" as "ss_item_desc",
      "ss_item_items"."I_ITEM_ID" as "ss_item_id",
      "ss_store_store"."S_STATE" as "ss_store_state"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
      INNER JOIN "store_returns" as "sr_store_returns" on "ss_store_sales"."SS_ITEM_SK" = "sr_store_returns"."SR_ITEM_SK" AND "ss_store_sales"."SS_TICKET_NUMBER" = "sr_store_returns"."SR_TICKET_NUMBER"
      INNER JOIN "date_dim" as "sr_return_date_date" on "sr_store_returns"."SR_RETURNED_DATE_SK" = "sr_return_date_date"."D_DATE_SK"
      INNER JOIN "catalog_sales" as "cs_catalog_sales" on coalesce("ss_store_sales"."SS_ITEM_SK", "sr_store_returns"."SR_ITEM_SK") = "cs_catalog_sales"."CS_ITEM_SK"
      INNER JOIN "date_dim" as "cs_sold_date_date" on "cs_catalog_sales"."CS_SOLD_DATE_SK" = "cs_sold_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "store" as "ss_store_store" on "ss_store_sales"."SS_STORE_SK" = "ss_store_store"."S_STORE_SK"
      LEFT OUTER JOIN "item" as "ss_item_items" on coalesce("ss_store_sales"."SS_ITEM_SK", "sr_store_returns"."SR_ITEM_SK", "cs_catalog_sales"."CS_ITEM_SK") = "ss_item_items"."I_ITEM_SK"
  WHERE
      "ss_date_date"."D_YEAR" = 2001 and "sr_return_date_date"."D_YEAR" in (2001,2002) and "cs_sold_date_date"."D_YEAR" in (2001,2002) and "cs_catalog_sales"."CS_QUANTITY" is not null

  GROUP BY
      1,
      2,
      3,
      4,
      "cs_catalog_sales"."CS_ORDER_NUMBER",
      coalesce("cs_catalog_sales"."CS_ITEM_SK","sr_store_returns"."SR_ITEM_SK","ss_item_items"."I_ITEM_SK","ss_store_sales"."SS_ITEM_SK")),
  yummy as (
  SELECT
      "sr_store_returns"."SR_RETURN_QUANTITY" as "sr_return_quantity",
      "ss_item_items"."I_ITEM_DESC" as "ss_item_desc",
      "ss_item_items"."I_ITEM_ID" as "ss_item_id",
      "ss_store_sales"."SS_QUANTITY" as "ss_quantity",
      "ss_store_store"."S_STATE" as "ss_store_state"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
      INNER JOIN "store_returns" as "sr_store_returns" on "ss_store_sales"."SS_ITEM_SK" = "sr_store_returns"."SR_ITEM_SK" AND "ss_store_sales"."SS_TICKET_NUMBER" = "sr_store_returns"."SR_TICKET_NUMBER"
      INNER JOIN "date_dim" as "sr_return_date_date" on "sr_store_returns"."SR_RETURNED_DATE_SK" = "sr_return_date_date"."D_DATE_SK"
      INNER JOIN "highfalutin" on coalesce("ss_store_sales"."SS_ITEM_SK", "sr_store_returns"."SR_ITEM_SK") = "highfalutin"."cs_item_sk"
      INNER JOIN "date_dim" as "cs_sold_date_date" on "highfalutin"."cs_sold_date_sk" = "cs_sold_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "store" as "ss_store_store" on "ss_store_sales"."SS_STORE_SK" = "ss_store_store"."S_STORE_SK"
      LEFT OUTER JOIN "item" as "ss_item_items" on coalesce("ss_store_sales"."SS_ITEM_SK", "sr_store_returns"."SR_ITEM_SK", "highfalutin"."cs_item_sk") = "ss_item_items"."I_ITEM_SK"
  WHERE
      "ss_date_date"."D_YEAR" = 2001 and "sr_return_date_date"."D_YEAR" in (2001,2002) and "cs_sold_date_date"."D_YEAR" in (2001,2002) and "ss_store_sales"."SS_QUANTITY" is not null and "sr_store_returns"."SR_RETURN_QUANTITY" is not null

  GROUP BY
      1,
      2,
      3,
      4,
      5,
      coalesce("highfalutin"."cs_item_sk","sr_store_returns"."SR_ITEM_SK","ss_item_items"."I_ITEM_SK","ss_store_sales"."SS_ITEM_SK"),
      coalesce("sr_store_returns"."SR_TICKET_NUMBER","ss_store_sales"."SS_TICKET_NUMBER")),
  concerned as (
  SELECT
      "vacuous"."ss_item_desc" as "ss_item_desc",
      "vacuous"."ss_item_id" as "ss_item_id",
      "vacuous"."ss_store_state" as "ss_store_state",
      avg(cast("vacuous"."cs_quantity" as float)) as "_virt_agg_avg_6548680003665131",
      avg(cast("vacuous"."cs_quantity" as float)) as "cs_qty_avg",
      count("vacuous"."cs_quantity") as "cs_qty_count",
      stddev_samp(cast("vacuous"."cs_quantity" as float)) as "_virt_agg_stddev_6025591228639107",
      stddev_samp(cast("vacuous"."cs_quantity" as float)) as "cs_qty_stddev"
  FROM
      "vacuous"
  WHERE
      "vacuous"."cs_quantity" is not null

  GROUP BY
      1,
      2,
      3),
  juicy as (
  SELECT
      "yummy"."ss_item_desc" as "ss_item_desc",
      "yummy"."ss_item_id" as "ss_item_id",
      "yummy"."ss_store_state" as "ss_store_state",
      avg(cast("yummy"."sr_return_quantity" as float)) as "_virt_agg_avg_2900539903680214",
      avg(cast("yummy"."sr_return_quantity" as float)) as "sr_qty_avg",
      avg(cast("yummy"."ss_quantity" as float)) as "_virt_agg_avg_261899251274428",
      avg(cast("yummy"."ss_quantity" as float)) as "ss_qty_avg",
      count("yummy"."sr_return_quantity") as "sr_qty_count",
      count("yummy"."ss_quantity") as "ss_qty_count",
      stddev_samp(cast("yummy"."sr_return_quantity" as float)) as "_virt_agg_stddev_9681359528168367",
      stddev_samp(cast("yummy"."sr_return_quantity" as float)) as "sr_qty_stddev",
      stddev_samp(cast("yummy"."ss_quantity" as float)) as "_virt_agg_stddev_3577223334008968",
      stddev_samp(cast("yummy"."ss_quantity" as float)) as "ss_qty_stddev"
  FROM
      "yummy"
  WHERE
      "yummy"."ss_quantity" is not null and "yummy"."sr_return_quantity" is not null

  GROUP BY
      1,
      2,
      3)
  SELECT
      "juicy"."ss_item_id" as "item_id",
      "juicy"."ss_item_desc" as "item_desc",
      "juicy"."ss_store_state" as "store_state",
      coalesce("juicy"."ss_qty_count",0) as "ss_qty_count",
      "juicy"."ss_qty_avg" as "ss_qty_avg",
      "juicy"."ss_qty_stddev" as "ss_qty_stddev",
      "juicy"."_virt_agg_stddev_3577223334008968" / nullif("juicy"."_virt_agg_avg_261899251274428",0) as "ss_qty_cv",
      coalesce("juicy"."sr_qty_count",0) as "sr_qty_count",
      "juicy"."sr_qty_avg" as "sr_qty_avg",
      "juicy"."sr_qty_stddev" as "sr_qty_stddev",
      "juicy"."_virt_agg_stddev_9681359528168367" / nullif("juicy"."_virt_agg_avg_2900539903680214",0) as "sr_qty_cv",
      "concerned"."cs_qty_count" as "cs_qty_count",
      "concerned"."cs_qty_avg" as "cs_qty_avg",
      "concerned"."cs_qty_stddev" as "cs_qty_stddev",
      "concerned"."_virt_agg_stddev_6025591228639107" / nullif("concerned"."_virt_agg_avg_6548680003665131",0) as "cs_qty_cv"
  FROM
      "concerned"
      INNER JOIN "juicy" on "concerned"."ss_item_desc" is not distinct from "juicy"."ss_item_desc" AND "concerned"."ss_item_id" is not distinct from "juicy"."ss_item_id" AND "concerned"."ss_store_state" is not distinct from "juicy"."ss_store_state"
  WHERE
      INVALID_REFERENCE_BUG<Missing source reference to ss.quantity> is not null and INVALID_REFERENCE_BUG<Missing source reference to sr.return_quantity> is not null and INVALID_REFERENCE_BUG<Missing source reference to cs.quantity> is not null

  ORDER BY
      "item_id" asc nulls first,
      "item_desc" asc nulls first,
      "store_state" asc nulls first
  LIMIT (100)
  ```
- `trilogy run query17.preql`

  ```text
  Unexpected error in query17.preql: Could not render the query: Missing source reference to sr.return_quantity; Missing source reference to cs.quantity. A planned reference has no backing source CTE -- typically an unsupported cross-rowset or membership shape the planner could not wire. Review the rowset/join structure (or file an issue if the query looks valid).

  Full SQL with sentinel(s):

  WITH
  highfalutin as (
  SELECT
      "cs_catalog_sales"."CS_BILL_CUSTOMER_SK" as "cs_billing_customer_sk",
      "cs_catalog_sales"."CS_ITEM_SK" as "cs_item_sk",
      "cs_catalog_sales"."CS_QUANTITY" as "cs_quantity",
      "cs_catalog_sales"."CS_SOLD_DATE_SK" as "cs_sold_date_sk"
  FROM
      "catalog_sales" as "cs_catalog_sales"
  GROUP BY
      1,
      2,
      3,
      4),
  sweltering as (
  SELECT
      "ss_customer_customers"."C_CUSTOMER_ID" as "_ss_filtered_customer_id",
      "ss_item_items"."I_ITEM_DESC" as "_ss_filtered_item_desc",
      "ss_item_items"."I_ITEM_ID" as "_ss_filtered_item_id",
      "ss_item_items"."I_ITEM_SK" as "_ss_filtered_item_sk",
      "ss_store_sales"."SS_QUANTITY" as "_ss_filtered_ss_qty",
      "ss_store_sales"."SS_TICKET_NUMBER" as "_ss_filtered_ticket_number",
      "ss_store_store"."S_STATE" as "_ss_filtered_store_state"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
      INNER JOIN "item" as "ss_item_items" on "ss_store_sales"."SS_ITEM_SK" = "ss_item_items"."I_ITEM_SK"
      LEFT OUTER JOIN "store" as "ss_store_store" on "ss_store_sales"."SS_STORE_SK" = "ss_store_store"."S_STORE_SK"
      LEFT OUTER JOIN "customer" as "ss_customer_customers" on "ss_store_sales"."SS_CUSTOMER_SK" = "ss_customer_customers"."C_CUSTOMER_SK"
  WHERE
      "ss_date_date"."D_YEAR" = 2001
  ),
  yummy as (
  SELECT
      "sr_billing_customer_customers"."C_CUSTOMER_ID" as "_sr_filtered_customer_id",
      "sr_store_returns"."SR_ITEM_SK" as "_sr_filtered_item_sk",
      "sr_store_returns"."SR_RETURN_QUANTITY" as "_sr_filtered_sr_qty",
      "sr_store_returns"."SR_TICKET_NUMBER" as "_sr_filtered_ticket_number"
  FROM
      "store_returns" as "sr_store_returns"
      INNER JOIN "date_dim" as "sr_return_date_date" on "sr_store_returns"."SR_RETURNED_DATE_SK" = "sr_return_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "customer" as "sr_billing_customer_customers" on "sr_store_returns"."SR_CUSTOMER_SK" = "sr_billing_customer_customers"."C_CUSTOMER_SK"
  WHERE
      "sr_return_date_date"."D_YEAR" in (2001,2002)
  ),
  thoughtful as (
  SELECT
      "cs_billing_customer_customers"."C_CUSTOMER_ID" as "_cs_filtered_customer_id",
      "highfalutin"."cs_item_sk" as "_cs_filtered_item_sk",
      "highfalutin"."cs_quantity" as "_cs_filtered_cs_qty"
  FROM
      "highfalutin"
      INNER JOIN "date_dim" as "cs_sold_date_date" on "highfalutin"."cs_sold_date_sk" = "cs_sold_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "customer" as "cs_billing_customer_customers" on "highfalutin"."cs_billing_customer_sk" = "cs_billing_customer_customers"."C_CUSTOMER_SK"
  WHERE
      "cs_sold_date_date"."D_YEAR" in (2001,2002)

  GROUP BY
      1,
      2,
      3),
  late as (
  SELECT
      "sweltering"."_ss_filtered_customer_id" as "ss_filtered_customer_id",
      "sweltering"."_ss_filtered_item_desc" as "ss_filtered_item_desc",
      "sweltering"."_ss_filtered_item_id" as "ss_filtered_item_id",
      "sweltering"."_ss_filtered_item_sk" as "ss_filtered_item_sk",
      "sweltering"."_ss_filtered_ss_qty" as "ss_filtered_ss_qty",
      "sweltering"."_ss_filtered_store_state" as "ss_filtered_store_state",
      "sweltering"."_ss_filtered_ticket_number" as "ss_filtered_ticket_number"
  FROM
      "sweltering"),
  juicy as (
  SELECT
      "yummy"."_sr_filtered_customer_id" as "sr_filtered_customer_id",
      "yummy"."_sr_filtered_item_sk" as "sr_filtered_item_sk",
      "yummy"."_sr_filtered_sr_qty" as "sr_filtered_sr_qty",
      "yummy"."_sr_filtered_ticket_number" as "sr_filtered_ticket_number"
  FROM
      "yummy"),
  cooperative as (
  SELECT
      "thoughtful"."_cs_filtered_cs_qty" as "cs_filtered_cs_qty",
      "thoughtful"."_cs_filtered_customer_id" as "cs_filtered_customer_id",
      "thoughtful"."_cs_filtered_item_sk" as "cs_filtered_item_sk"
  FROM
      "thoughtful"),
  friendly as (
  SELECT
      "cooperative"."cs_filtered_cs_qty" as "cs_filtered_cs_qty",
      "late"."ss_filtered_item_desc" as "ss_filtered_item_desc",
      "late"."ss_filtered_item_id" as "ss_filtered_item_id",
      "late"."ss_filtered_store_state" as "ss_filtered_store_state",
      coalesce("cooperative"."cs_filtered_customer_id","late"."ss_filtered_customer_id") as "cs_filtered_customer_id",
      coalesce("cooperative"."cs_filtered_item_sk","late"."ss_filtered_item_sk") as "cs_filtered_item_sk"
  FROM
      "late"
      INNER JOIN "cooperative" on "late"."ss_filtered_customer_id" = "cooperative"."cs_filtered_customer_id" AND "late"."ss_filtered_item_sk" = "cooperative"."cs_filtered_item_sk"
  WHERE
      "late"."ss_filtered_item_id" is not null and "cooperative"."cs_filtered_cs_qty" is not null
  ),
  macho as (
  SELECT
      "juicy"."sr_filtered_sr_qty" as "sr_filtered_sr_qty",
      "late"."ss_filtered_item_desc" as "ss_filtered_item_desc",
      "late"."ss_filtered_item_id" as "ss_filtered_item_id",
      "late"."ss_filtered_ss_qty" as "ss_filtered_ss_qty",
      "late"."ss_filtered_store_state" as "ss_filtered_store_state",
      coalesce("cooperative"."cs_filtered_item_sk","juicy"."sr_filtered_item_sk","late"."ss_filtered_item_sk") as "cs_filtered_item_sk",
      coalesce("juicy"."sr_filtered_ticket_number","late"."ss_filtered_ticket_number") as "sr_filtered_ticket_number"
  FROM
      "late"
      INNER JOIN "juicy" on "late"."ss_filtered_customer_id" = "juicy"."sr_filtered_customer_id" AND "late"."ss_filtered_item_sk" = "juicy"."sr_filtered_item_sk" AND "late"."ss_filtered_ticket_number" = "juicy"."sr_filtered_ticket_number"
      LEFT OUTER JOIN "cooperative" on coalesce("late"."ss_filtered_customer_id", "juicy"."sr_filtered_customer_id") = "cooperative"."cs_filtered_customer_id" AND coalesce("late"."ss_filtered_item_sk", "juicy"."sr_filtered_item_sk") = "cooperative"."cs_filtered_item_sk"
  WHERE
      "late"."ss_filtered_item_id" is not null and "juicy"."sr_filtered_sr_qty" is not null
  ),
  kaput as (
  SELECT
      "friendly"."cs_filtered_item_sk" as "cs_filtered_item_sk",
      "friendly"."ss_filtered_item_desc" as "ss_filtered_item_desc",
      "friendly"."ss_filtered_item_id" as "ss_filtered_item_id",
      "friendly"."ss_filtered_store_state" as "ss_filtered_store_state",
      avg(cast("friendly"."cs_filtered_cs_qty" as float)) as "_virt_agg_avg_3740738998512210",
      avg(cast("friendly"."cs_filtered_cs_qty" as float)) as "cs_qty_avg",
      count("friendly"."cs_filtered_cs_qty") as "cs_qty_count",
      stddev_samp(cast("friendly"."cs_filtered_cs_qty" as float)) as "_virt_agg_stddev_4285935967896813",
      stddev_samp(cast("friendly"."cs_filtered_cs_qty" as float)) as "cs_qty_stddev"
  FROM
      "friendly"
  WHERE
      "friendly"."ss_filtered_item_id" is not null and "friendly"."cs_filtered_cs_qty" is not null

  GROUP BY
      1,
      2,
      3,
      4,
      "friendly"."cs_filtered_customer_id"),
  scrawny as (
  SELECT
      "macho"."cs_filtered_item_sk" as "cs_filtered_item_sk",
      "macho"."ss_filtered_item_desc" as "ss_filtered_item_desc",
      "macho"."ss_filtered_item_id" as "ss_filtered_item_id",
      "macho"."ss_filtered_store_state" as "ss_filtered_store_state",
      avg(cast("macho"."sr_filtered_sr_qty" as float)) as "_virt_agg_avg_1640011108931452",
      avg(cast("macho"."sr_filtered_sr_qty" as float)) as "sr_qty_avg",
      avg(cast("macho"."ss_filtered_ss_qty" as float)) as "_virt_agg_avg_9821488600046701",
      avg(cast("macho"."ss_filtered_ss_qty" as float)) as "ss_qty_avg",
      count("macho"."sr_filtered_sr_qty") as "sr_qty_count",
      count("macho"."ss_filtered_ss_qty") as "ss_qty_count",
      stddev_samp(cast("macho"."sr_filtered_sr_qty" as float)) as "_virt_agg_stddev_1792668191249213",
      stddev_samp(cast("macho"."sr_filtered_sr_qty" as float)) as "sr_qty_stddev",
      stddev_samp(cast("macho"."ss_filtered_ss_qty" as float)) as "_virt_agg_stddev_9158805045043173",
      stddev_samp(cast("macho"."ss_filtered_ss_qty" as float)) as "ss_qty_stddev"
  FROM
      "macho"
  WHERE
      "macho"."ss_filtered_item_id" is not null and "macho"."sr_filtered_sr_qty" is not null

  GROUP BY
      1,
      2,
      3,
      4,
      "macho"."sr_filtered_ticket_number")
  SELECT
      coalesce("kaput"."ss_filtered_item_id","scrawny"."ss_filtered_item_id") as "ss_filtered_item_id",
      coalesce("kaput"."ss_filtered_item_desc","scrawny"."ss_filtered_item_desc") as "ss_filtered_item_desc",
      coalesce("kaput"."ss_filtered_store_state","scrawny"."ss_filtered_store_state") as "ss_filtered_store_state",
      coalesce("scrawny"."ss_qty_count",0) as "ss_qty_count",
      "scrawny"."ss_qty_avg" as "ss_qty_avg",
      "scrawny"."ss_qty_stddev" as "ss_qty_stddev",
      "scrawny"."_virt_agg_stddev_9158805045043173" / nullif("scrawny"."_virt_agg_avg_9821488600046701",0) as "ss_qty_cv",
      coalesce("scrawny"."sr_qty_count",0) as "sr_qty_count",
      "scrawny"."sr_qty_avg" as "sr_qty_avg",
      "scrawny"."sr_qty_stddev" as "sr_qty_stddev",
      "scrawny"."_virt_agg_stddev_1792668191249213" / nullif("scrawny"."_virt_agg_avg_1640011108931452",0) as "sr_qty_cv",
      coalesce("kaput"."cs_qty_count",0) as "cs_qty_count",
      "kaput"."cs_qty_avg" as "cs_qty_avg",
      "kaput"."cs_qty_stddev" as "cs_qty_stddev",
      "kaput"."_virt_agg_stddev_4285935967896813" / nullif("kaput"."_virt_agg_avg_3740738998512210",0) as "cs_qty_cv"
  FROM
      "kaput"
      FULL JOIN "scrawny" on "kaput"."cs_filtered_item_sk" is not distinct from "scrawny"."cs_filtered_item_sk" AND "kaput"."ss_filtered_item_desc" is not distinct from "scrawny"."ss_filtered_item_desc" AND "kaput"."ss_filtered_item_id" is not distinct from "scrawny"."ss_filtered_item_id" AND "kaput"."ss_filtered_store_state" is not distinct from "scrawny"."ss_filtered_store_state"
  WHERE
      coalesce("kaput"."ss_filtered_item_id","scrawny"."ss_filtered_item_id") is not null and INVALID_REFERENCE_BUG<Missing source reference to sr.return_quantity> is not null and INVALID_REFERENCE_BUG<Missing source reference to cs.quantity> is not null

  ORDER BY
      coalesce("kaput"."ss_filtered_item_id","scrawny"."ss_filtered_item_id") asc nulls first,
      coalesce("kaput"."ss_filtered_item_desc","scrawny"."ss_filtered_item_desc") asc nulls first,
      coalesce("kaput"."ss_filtered_store_state","scrawny"."ss_filtered_store_state") asc nulls first
  LIMIT (100)
  ```
- `trilogy file read query17.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query21.preql`

  ```text
  Syntax error in query21.preql: Cannot compare DATE (ref:inv.date.date) and STRING (2000-02-10) of different types with operator >= in ref:inv.date.date >= 2000-02-10
  ```
- `trilogy run query23.preql`

  ```text
  Resolution error in query23.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 36). The requested concepts split into 2 disconnected subgraphs: {customer_limited.total_2000_2003}; {customer_wide.customer_id, customer_wide.total_all_time}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query25.preql`

  ```text
  Resolution error in query25.preql: (AmbiguousRelationshipResolutionException(...), "Multiple possible concept additions (intermediate join keys) found to resolve ['cs.billing_customer.id', 'cs.item.id', 'cs.sold_date.month_of_year', 'cs.sold_date.year', 'ss._returned_ticket', 'ss.date.month_of_year', 'ss.date.year', 'ss.item.desc', 'ss.item.sk', 'ss.net_profit', 'ss.return_customer.id', 'ss.return_date.month_of_year', 'ss.return_date.year', 'ss.return_net_loss', 'ss.store.id', 'ss.store.name', 'ss.ticket_number', 'cs.billing_customer.sk', 'cs.sold_date.sk', 'ss.date.sk', 'ss.return_customer.sk', 'ss.return_date.sk', 'ss.store.sk'], have {'ss.customer.sk'} or {'cs.item.sk'}. Different paths are is: [{'ss.customer.sk'}, {'cs.item.sk'}]")
  ```
- `trilogy run query29.preql`

  ```text
  Syntax error in query29.preql: Comparison `ss.return_date.month_of_year <= 12` matches every value of enum field 'ss.return_date.month_of_year', which contains only these values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12. It is always true and should be removed.
  ```
- `trilogy file read query29.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query31.preql`

  ```text
  Syntax error in query31.preql: Undefined concept: web_total. Suggestions: ['web_totals.county', 'web_totals.quarter', '_web_totals_county', 'web_totals.web_total']
  ```
- `trilogy file read query34.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 35 column 115 (char 1219). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 81 column 12 (char 3449). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query35.preql`

  ```text
  Syntax error in query35.preql: Undefined concept: cust.demographics (line 37, col 9, in WHERE). Suggestions: ['cust.demographics.sk', 'cust.demographics.gender', 'cust.demographics.marital_status', 'cust.demographics.education_status', 'cust.demographics.purchase_estimate', 'cust.demographics.credit_rating']
  ```
- `trilogy file read query35.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query37.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query39.preql`

  ```text
  Resolution error in query39.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 8). The requested concepts split into 4 disconnected subgraphs: {date.year, _monthly_stats_month_of_year}; {_monthly_stats_item_sk}; {_monthly_stats_mn, _monthly_stats_sd}; {_monthly_stats_warehouse_sk}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query39.preql`

  ```text
  Resolution error in query39.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 8). The requested concepts split into 4 disconnected subgraphs: {date.year, _monthly_stats_month_of_year}; {_monthly_stats_item_sk}; {_monthly_stats_mn, _monthly_stats_sd}; {_monthly_stats_warehouse_sk}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query39.preql`

  ```text
  Resolution error in query39.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 27). The requested concepts split into 4 disconnected subgraphs: {date.month_of_year, date.year}; {_feb_it}; {_feb_wh}; {cv, mn}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query39.preql`

  ```text
  Resolution error in query39.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 8). The requested concepts split into 4 disconnected subgraphs: {date.year, _monthly_stats_mo}; {_monthly_stats_it}; {_monthly_stats_mn, _monthly_stats_sd}; {_monthly_stats_wh}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/inventory.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query43.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query45.preql`

  ```text
  Syntax error in query45.preql: Undefined concept: item.sk. Suggestions: ['ws.item.sk', 'ws.item.size', 'ws.item.desc', 'ws.date.sk', 'ws.ship_date.sk', 'ws.return_date.sk']
  ```
- `trilogy file read query48.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file write --content import raw.store_sales as ss;
import raw.web_sales as ws;

# Per-item, per-day sum of unit price (sales_price) for store sales i…ng.sale_date = web_running.sale_date
order by store_running.item_id asc nulls first, store_running.sale_date asc nulls first
limit 100; query51.preql`

  ```text
  trilogy file write: `--content` takes a SINGLE string argument. Your args list put 2 separate tokens after --content (treating it like a shell command). In a tool call, pass the entire file body as one string element after --content, with newlines embedded literally — e.g.
    {"args": ["file", "write", "query70.preql", "--content", "import raw.store_sales as store_sales;\n\nselect ..."]}
  Alternatively use `--escapes` with a single-line `\n`-escaped string.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Unterminated string starting at: line 1 column 54 (char 53). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query54.preql`

  ```text
  Syntax error in query54.preql: Nothing was executed: parsed 10 definition statement(s) (7 imports, 3 rowsets) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy file read query58.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run raw/store_sales.preql duckdb --param limit=10 --format json`

  ```text
  Syntax error in raw\store_sales.preql: Nothing was executed: parsed 23 definition statement(s) (15 imports, 5 concepts, 2 datasources, 1 property) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy file read query63.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: 23 undefined concept references; fix all before re-running:
    - yr1999.sale_street_number (line 58, col 5, in SELECT); did you mean: yr1999.sale_street_name, yr1999.cust_street_number, yr1999.cust_street_name, base_agg.sale_street_number?
    - yr1999.sale_street_name (line 59, col 5, in SELECT); did you mean: yr1999.sale_street_number, yr1999.cust_street_name, yr1999.cust_street_number, base_agg.sale_street_name?
    - yr1999.sale_city (line 60, col 5, in SELECT); did you mean: yr1999.sale_zip, yr1999.sale_year, yr1999.cust_city, base_agg.sale_city?
    - yr1999.sale_zip (line 61, col 5, in SELECT); did you mean: yr1999.sale_city, yr1999.cust_zip, yr1999.sale_year, base_agg.sale_zip?
    - yr1999.cust_street_number (line 62, col 5, in SELECT); did you mean: yr1999.cust_street_name, yr1999.sale_street_number, yr1999.sale_street_name, base_agg.cust_street_number?
    - yr1999.cust_street_name (line 63, col 5, in SELECT); did you mean: yr1999.cust_street_number, yr1999.sale_street_name, yr1999.sale_street_number, base_agg.cust_street_name?
    - yr1999.cust_city (line 64, col 5, in SELECT); did you mean: yr1999.cust_zip, yr1999.sale_city, yr1999.cnt, base_agg.cust_city?
    - yr1999.cust_zip (line 65, col 5, in SELECT); did you mean: yr1999.cust_city, yr1999.sale_zip, yr1999.cnt, base_agg.cust_zip?
    - yr1999.sale_year (line 66, col 5, in SELECT); did you mean: yr1999.sale_city, yr1999.sale_zip, yr1999.sale_street_name, yr2000.sale_year, base_agg.sale_year?
    - yr1999.cnt (line 67, col 5, in SELECT); did you mean: yr1999, yr1999.cust_zip, yr1999.sale_city, base_agg.cnt, yr2000.cnt?
    - yr1999.wholesale_cost_sum (line 68, col 5, in SELECT); did you mean: yr1999.sale_city, yr1999.sale_street_number, yr1999.sale_street_name, yr2000.wholesale_cost_sum, base_agg.wholesale_cost_sum, ss.wholesale_cost?
    - yr1999.list_price_sum (line 69, col 5, in SELECT); did you mean: yr1999.cust_zip, yr1999.wholesale_cost_sum, yr2000.list_price_sum, base_agg.list_price_sum, ss.list_price?
    - yr1999.coupon_amt_sum (line 70, col 5, in SELECT); did you mean: yr1999.cnt, yr1999.cust_zip, yr1999.wholesale_cost_sum, yr2000.coupon_amt_sum, base_agg.coupon_amt_sum, ss.coupon_amt?
    - yr2000.wholesale_cost_sum (line 71, col 5, in SELECT); did you mean: yr2000.list_price_sum, yr2000.coupon_amt_sum, yr1999.wholesale_cost_sum, base_agg.wholesale_cost_sum, ss.wholesale_cost?
    - yr2000.list_price_sum (line 72, col 5, in SELECT); did you mean: yr2000.wholesale_cost_sum, yr1999.list_price_sum, base_agg.list_price_sum, ss.list_price?
    - yr2000.coupon_amt_sum (line 73, col 5, in SELECT); did you mean: yr2000.cnt, yr2000.wholesale_cost_sum, yr1999.coupon_amt_sum, base_agg.coupon_amt_sum, ss.coupon_amt?
    - yr2000.sale_year (line 74, col 5, in SELECT); did you mean: yr1999.sale_year, base_agg.sale_year?
    - yr2000.cnt (line 75, col 5, in SELECT); did you mean: yr2000, yr2000.coupon_amt_sum, yr1999.cnt, base_agg.cnt?
    - yr2000.cnt (line 53, col 9, in WHERE); did you mean: yr2000, yr2000.coupon_amt_sum, yr1999.cnt, base_agg.cnt?
    - yr1999.cnt (line 53, col 23, in WHERE); did you mean: yr1999, yr1999.cust_zip, yr1999.sale_city, base_agg.cnt, yr2000.cnt?
    - yr2000.cnt (line 79, col 5, in ORDER BY); did you mean: yr2000, yr2000.coupon_amt_sum, yr1999.cnt, base_agg.cnt?
    - yr1999.wholesale_cost_sum (line 80, col 5, in ORDER BY); did you mean: yr1999.sale_city, yr1999.sale_street_number, yr1999.sale_street_name, yr2000.wholesale_cost_sum, base_agg.wholesale_cost_sum, ss.wholesale_cost?
    - yr2000.wholesale_cost_sum (line 81, col 5, in ORDER BY); did you mean: yr2000.list_price_sum, yr2000.coupon_amt_sum, yr1999.wholesale_cost_sum, base_agg.wholesale_cost_sum, ss.wholesale_cost?
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 11). The requested concepts split into 3 disconnected subgraphs: {_base_agg_cnt, _base_agg_coupon_amt_sum, _base_agg_cust_city, _base_agg_cust_street_name, _base_agg_cust_street_number, _base_agg_cust_zip, _base_agg_first_sales_year, _base_agg_first_ship_year, _base_agg_item_sk, _base_agg_list_price_sum, _base_agg_product_name, _base_agg_sale_city, _base_agg_sale_street_name, _base_agg_sale_street_number, _base_agg_sale_year, _base_agg_sale_zip, _base_agg_store_name, _base_agg_store_zip, _base_agg_wholesale_cost_sum, ss.customer.demographics.marital_status, ss.customer_demographic.marital_status, ss.item.color, ss.item.current_price, ss.item.sk, ss.ticket_number}; {cum_cr_refund_per_item}; {cum_cs_list_per_item}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 43). The requested concepts split into 2 disconnected subgraphs: {base_agg.cnt, base_agg.coupon_amt_sum, base_agg.cust_city, base_agg.cust_street_name, base_agg.cust_street_number, base_agg.cust_zip, base_agg.first_sales_year, base_agg.first_ship_year, base_agg.item_sk, base_agg.list_price_sum, base_agg.product_name, base_agg.sale_city, base_agg.sale_street_name, base_agg.sale_street_number, base_agg.sale_year, base_agg.sale_zip, base_agg.store_name, base_agg.store_zip, base_agg.wholesale_cost_sum}; {catalog_per_item.cum_list, catalog_per_item.cum_refund}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query65.preql`

  ```text
  Syntax error in query65.preql: ORDER BY references 'ss.store.id', which is not in the SELECT projection (line 10). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --ss.store.id order by ss.store.id asc`.
  ```
- `trilogy database describe catalog_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy database describe web_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query67.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query70.preql`

  ```text
  Syntax error in query70.preql: ORDER BY references 'state_profits.state_net_profit', which is not in the SELECT projection (line 13). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --state_profits.state_net_profit order by state_profits.state_net_profit asc`.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe catalog_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query82.preql`

  ```text
  Resolution error in query82.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 5). The requested concepts split into 2 disconnected subgraphs: {inv.date.date, inv.quantity_on_hand}; {item.current_price, item.manufacturer_id, item.sk, description, item_code}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 2 column 1 (char 42). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file read query84.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query91.preql`

  ```text
  Syntax error in query91.preql: Comparison `cr.billing_customer.demographics.marital_status = 'Married'` can never match enum field 'cr.billing_customer.demographics.marital_status', which contains only these values: 'M', 'S', 'D', 'W', 'U'. It is always false and should be removed.
  ```
- `trilogy file read query93.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query94.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query96.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy file write query02.preql --content import raw.all_sales as s;

# Sum sales (ext_sales_price) per week and day-of-week, for WEB and CATALOG only
# Use…  sum(day_ratio ? daily.day_of_week = 5) as fri_ratio,
    sum(day_ratio ? daily.day_of_week = 6) as sat_ratio
order by daily.week_seq asc
limit 100;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [213]: A `by <grain>` clause must follow an aggregate, but the expression before it has none. If the `by` sits inside an aggregate's parentheses (`max(x by *)`), move it outside the call: `max(x) by *`. To take each distinct value once per grain, wrap it in `group(...)` - e.g. `group(item.current_price) by item.id, item.category`. For a reduction, use an aggregate: `sum(x) by ...`, `avg(x) by ...`, `max(x) by ...`.
  Location:
  ...ver (order by daily.week_seq) ??? by daily.day_of_week;  # Compu...

  Write stats: received 1492 chars / 1492 bytes; tail: …'eek = 6) as sat_ratio\\norder by daily.week_seq asc\\nlimit 100;'.
  ```
- `trilogy file write query02.preql --content import raw.all_sales as s;

# Sum sales (ext_sales_price) per week and day-of-week, for WEB and CATALOG only
with …  sum(day_ratio ? daily.day_of_week = 5) as fri_ratio,
    sum(day_ratio ? daily.day_of_week = 6) as sat_ratio
order by daily.week_seq asc
limit 100;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block - they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...nel in ('WEB', 'CATALOG')     ??? import raw.all_sales as s2 );

  Write stats: received 1301 chars / 1301 bytes; tail: …'eek = 6) as sat_ratio\\norder by daily.week_seq asc\\nlimit 100;'.
  ```
- `trilogy run --import raw/all_sales:all select count(*) as cnt, all.channel as ch, all.is_returned as ret by rollup(all.channel, all.is_returned);`

  ```text
  Syntax error in stdin: Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
   raw.all_sales as all; select ??? count(*) as cnt, all.channel a...
  ```
- `trilogy run --import raw/all_sales:all select count(all.channel_dim_id) as cnt, all.channel as ch, all.is_returned as ret group by all.channel, all.is_returned;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...as ch, all.is_returned as ret ??? group by all.channel, all.is_r...
  ```
- `trilogy run --import raw/all_sales:all where all.date.date between '2000-08-23'::date and '2000-09-06'::date and all.channel_dim_text_id is not null select count(all.channel_dim_id) as cnt union join ... limit 5;`

  ```text
  Syntax error in stdin:  --> 2:162
    |
  2 | where all.date.date between '2000-08-23'::date and '2000-09-06'::date and all.channel_dim_text_id is not null select count(all.channel_dim_id) as cnt union join ... limit 5;
    |                                                                                                                                                                  ^---
    |
    = expected sum_operator
  Location:
  ...nel_dim_id) as cnt union join ??? ... limit 5;
  ```
- `trilogy run --import raw/all_sales:all with combined as union((where all.date.date between '2000-08-23'::date and '2000-09-06'::date and all.channel_dim_text…les_amt) as sales_amt, sum(combined.returns_amt) as returns_amt, sum(combined.profit_amt) as profit_amt, sum(combined.loss_amt) as loss_amt limit 10;`

  ```text
  Syntax error in stdin: Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...rns_amt, profit_amt, loss_amt) ??? select combined.ch as ch, com...
  ```
- `trilogy file write query09.preql --content import raw.store_sales as ss;

auto cnt_1_20 <- count(ss.line_item ? ss.quantity between 1 and 20) by *;
auto cnt_…isc_61_80 else avg_net_61_80 end as bucket_61_80,
  case when cnt_81_100 > 165306 then avg_disc_81_100 else avg_net_81_100 end as bucket_81_100
by *;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [213]: A `by <grain>` clause must follow an aggregate, but the expression before it has none. If the `by` sits inside an aggregate's parentheses (`max(x by *)`), move it outside the call: `max(x) by *`. To take each distinct value once per grain, wrap it in `group(...)` - e.g. `group(item.current_price) by item.id, item.category`. For a reduction, use an aggregate: `sum(x) by ...`, `avg(x) by ...`, `max(x) by ...`.
  Location:
  ...t_81_100 end as bucket_81_100 ??? by *;

  Write stats: received 1707 chars / 1707 bytes; tail: …'g_disc_81_100 else avg_net_81_100 end as bucket_81_100\\nby *;'.
  ```
- `trilogy file write query11.preql --content import raw.all_sales as all;

# Revenue = ext_list_price - ext_discount_amount per line item
# Store revenue by ye…ion join store_rev.cust_id = store_rev.cust_id
union join store_rev.cust_id = web_rev.cust_id
union join store_rev.cust_id = store_2002_pref.cust_id
`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [202]: Missing closing semicolon? Statements must be terminated with a semicolon `;`.
  Location:
  ..._id = store_2002_pref.cust_id ???

  Write stats: received 1759 chars / 1759 bytes; tail: …'t_id\\nunion join store_rev.cust_id = store_2002_pref.cust_id\\n'.
  ```
- `trilogy run --import raw.item:item select item.id, count(item.sk) as versions, count_distinct(item.current_price) as distinct_prices, count(item.current_price) as non_null_prices group by item.id having count(item.sk) > 1 limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...ent_price) as non_null_prices ??? group by item.id having count(...
  ```
- `trilogy file write query13.preql --content import raw.store_sales as ss;

# Filter to year 2001
where ss.date.year = 2001

# Both clauses must be true:
# Dem…les_price) as avg_ext_sales_price,
  avg(ss.ext_wholesale_cost) as avg_ext_wholesale_cost,
  sum(ss.ext_wholesale_cost) as total_ext_wholesale_cost
;`

  ```text
  refused to write 'query13.preql': not syntactically valid Trilogy.

  Parse error:
    --> 11:1
     |
  11 | where
     | ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
    # Build the combined filter ??? where   (     (ss.customer.dem...

  Write stats: received 1497 chars / 1497 bytes; tail: …',\\n  sum(ss.ext_wholesale_cost) as total_ext_wholesale_cost\\n;'.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as s;

# Overall average per-line sale value (qty * list_price) across all channels in 1999-2…category_id) desc,
    s.channel nulls first, s.item.brand_id nulls first,
    s.item.class_id nulls first, s.item.category_id nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
    --> 10:6
     |
  10 |     (import raw.all_sales as a; where a.date.year between 1999 and 2001 and a.channel = 'STORE' select a.item.brand_id, a.item.class_id, a.item.category_id),
     |      ^---
     |
     = expected select_statement
  Location:
  ...nnel_items as intersect(     ( ??? import raw.all_sales as a; whe...

  Hint: the `by rollup/cube/grouping sets` clause must come *before* HAVING in Trilogy (same order as SQL's `GROUP BY ... HAVING`). Reorder to:
      select <cols> by rollup (<keys>) having <cond> order by <cols> limit <n>;

  Write stats: received 1810 chars / 1810 bytes; tail: …'ss_id nulls first, s.item.category_id nulls first\\nlimit 100;'.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as s;

# Overall average per-line sale value (qty * list_price) across all channels in 1999-2…y
    g_level asc,
    s.channel nulls first, s.item.brand_id nulls first,
    s.item.class_id nulls first, s.item.category_id nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
    --> 32:25
     |
  32 |     --grouping(s.channel, s.item.brand_id, s.item.class_id, s.item.category_id) as g_level
     |                         ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...ales,     --grouping(s.channel ??? , s.item.brand_id, s.item.clas...

  Write stats: received 1721 chars / 1721 bytes; tail: …'ss_id nulls first, s.item.category_id nulls first\\nlimit 100;'.
  ```
- `trilogy file write query16.preql --content import raw/catalog_sales as cs;
import raw/catalog_returns as cr;

# Count distinct warehouses per order
auto ware…s order_count,
    sum(cs.ext_ship_cost) as total_extended_ship_cost,
    sum(cs.net_profit) as total_net_profit
order by order_count desc
limit 100;`

  ```text
  refused to write 'query16.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/catalog_sales as cs;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/catalog_sales as cs; impor...

  Write stats: received 671 chars / 671 bytes; tail: …'it) as total_net_profit\\norder by order_count desc\\nlimit 100;'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store;
import raw.catalog_sales as catalog;
import raw.web_sales as web;

# Step 1: Iden…total_sales > 0
order by
    customer_last_name asc nulls first,
    customer_first_name asc nulls first,
    total_sales asc nulls first
limit 100;
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
    --> 16:1
     |
  16 | select
     | ^---
     |
     = expected limit, order_by, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...t having     triple_count > 4 ??? select     frequent_items.item...

  Write stats: received 3600 chars / 3600 bytes; tail: …'asc nulls first,\\n    total_sales asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store;
import raw.catalog_sales as catalog;
import raw.web_sales as web;

# Step 1: Freq…total_sales > 0
order by
    customer_last_name asc nulls first,
    customer_first_name asc nulls first,
    total_sales asc nulls first
limit 100;
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
    --> 58:6
     |
  58 |     (import raw.catalog_sales as cat;
     |      ^---
     |
     = expected select_statement
  Location:
   channel_union as union(     ( ??? import raw.catalog_sales as ca...

  Write stats: received 3297 chars / 3297 bytes; tail: …'asc nulls first,\\n    total_sales asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store;
import raw.catalog_sales as catalog;
import raw.web_sales as web;

# Step 1: Freq…
having
    total_sales > 0
order by
    customer_last_name asc nulls first,
    customer_first_name asc nulls first,
    total_sales asc
limit 100;
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...item_desc_prefix, sale_amount) ???  where channel_union.item_id

  Write stats: received 3124 chars / 3124 bytes; tail: …'_first_name asc nulls first,\\n    total_sales asc\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw/store_sales:store_sales select distinct store_sales.customer_demographic.marital_status, store_sales.customer_demographic.gender, store_sales.customer_demographic.education_status limit 50;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ..._sales as store_sales; select ??? distinct store_sales.customer_...
  ```
- `trilogy file write query36.preql --content import raw.store_sales as ss;

auto gm_ratio <- sum(ss.net_profit) / sum(ss.ext_sales_price);
auto level <- groupi…where ss.date.year = 2001 and ss.store.state = 'TN'
order by level desc nulls first, ss.item.category asc nulls first, rnk asc nulls first
limit 100;`

  ```text
  refused to write 'query36.preql': not syntactically valid Trilogy.

  Parse error:
    --> 16:1
     |
  16 | where ss.date.year = 2001 and ss.store.state = 'TN'
     | ^---
     |
     = expected limit, order_by, or having
  Location:
  ...item.category, ss.item.class) ??? where ss.date.year = 2001 and

  Write stats: received 638 chars / 638 bytes; tail: …'tem.category asc nulls first, rnk asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query41.preql --content import raw.item as item;

# Manufacturers that have at least one item matching any of the 8 profiles
with profile_…e_manus.manufact
  and count(item.id) by item.manufact > 1
select
    distinct item.product_name as product_name
order by
    product_name
limit 100;`

  ```text
  refused to write 'query41.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
   item.manufact > 1 select     ??? distinct item.product_name as

  Write stats: received 1396 chars / 1396 bytes; tail: …'ct_name as product_name\\norder by\\n    product_name\\nlimit 100;'.
  ```
- `trilogy file write query44.preql --content import raw.store_sales as ss;

# Filter to store with surrogate id = 1
auto store1_sales <- ss.net_profit ? ss.sto…former
subset join best_items.best_rank = worst_items.worst_rank
order by
    rank asc,
    best_performer desc,
    worst_performer desc
limit 100;
`

  ```text
  refused to write 'query44.preql': not syntactically valid Trilogy.

  Parse error:
    --> 28:1
     |
  28 | select
     | ^---
     |
     = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...est_rank where rank_asc <= 10 ??? select     ss.item.product_nam...

  Write stats: received 1662 chars / 1662 bytes; tail: …'   best_performer desc,\\n    worst_performer desc\\nlimit 100;\\n'.
  ```
- `trilogy file write query51.preql --escapes --content import raw.store_sales as ss;
import raw.web_sales as ws;

# Per-item, per-day sum of unit price (sales_…g.sale_date = web_running.sale_date
having web_running_max > store_running_max
order by item_id asc nulls first, sale_date asc nulls first
limit 100;`

  ```text
  refused to write 'query51.preql': not syntactically valid Trilogy.

  Parse error:
   --> 5:1
    |
  5 | rowset store_daily as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
   for store sales in year 2000 ??? rowset store_daily as where ss...

  Write stats: received 2014 chars / 2014 bytes; tail: …'tem_id asc nulls first, sale_date asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query54.preql --content import raw.catalog_sales as cs;
import raw.web_sales as ws;
import raw.store_sales as ss;
import raw.item as i;
im… qualifying_cust as
select
    cat_cust.cid
;

with qualifying_cust2 as
select
    cat_cust.cid as cid
;

select
    cat_cust.cid
from
    cat_cust
;`

  ```text
  refused to write 'query54.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...id ;  select     cat_cust.cid ??? from     cat_cust ;

  Write stats: received 899 chars / 899 bytes; tail: …'st.cid as cid\\n;\\n\\nselect\\n    cat_cust.cid\\nfrom\\n    cat_cust\\n;'.
  ```
- `trilogy run --import raw/catalog_sales:cs select cs.billing_customer.address.county, cs.billing_customer.address.state, count_distinct(cs.billing_customer.id…s.sold_date.year = 1998 and cs.sold_date.month_of_year = 12 and cs.billing_customer.address.county is not null group by 1,2 order by 3 desc limit 20;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...er.address.county is not null ??? group by 1,2 order by 3 desc l...
  ```
- `trilogy file write query59.preql --content import raw.store_sales as ss;

def sun_sum -> sum(ss.sales_price ? ss.date.day_of_week = 0);
def mon_sum -> sum(ss…seq = next_year.week_seq - 52
order by this_year.store_name nulls first, this_year.store_code nulls first, this_year.week_seq nulls first
limit 100;
`

  ```text
  refused to write 'query59.preql': not syntactically valid Trilogy.

  Parse error:
    --> 18:5
     |
  18 |     @sun_sum as sun,
     |     ^---
     |
     = expected select_item, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
      ss.date.year as year,     ??? @sun_sum as sun,     @mon_sum

  Write stats: received 2033 chars / 2033 bytes; tail: …'code nulls first, this_year.week_seq nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query64.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.store_r…as cnt,
    sum(ss.ext_wholesale_cost) as wholesale_cost_sum,
    sum(ss.ext_list_price) as list_price_sum,
    sum(ss.coupon_amt) as coupon_amt_sum
`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [202]: Missing closing semicolon? Statements must be terminated with a semicolon `;`.
  Location:
  ...coupon_amt) as coupon_amt_sum ???

  Write stats: received 1778 chars / 1778 bytes; tail: …'as list_price_sum,\\n    sum(ss.coupon_amt) as coupon_amt_sum\\n'.
  ```
- `trilogy file write query66.preql --content import raw.web_sales as ws;
import raw.catalog_sales as cs;

auto ship_carriers <- 'DHL,BARIAN';

with combined as…es_psf,
  coalesce(sum(combined.net_amt ? combined.mo = 12), 0) as m12_net
order by combined.wh_name nulls first, combined.yr nulls first
limit 100;
`

  ```text
  refused to write 'query66.preql': not syntactically valid Trilogy.

  Parse error:
    --> 21:39
     |
  21 |       ws.quantity * ws.ext_sales_price,
     |                                       ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ....quantity * ws.ext_sales_price ??? ,       ws.quantity * ws.net_p...

  Write stats: received 4314 chars / 4314 bytes; tail: …'ned.wh_name nulls first, combined.yr nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query69.preql --content import raw/store_sales as store;
import raw/web_sales as web;
import raw/catalog_sales as cat;

# Customers whose …ng.gender,
    qualifying.marital_status,
    qualifying.education_status,
    qualifying.purchase_estimate,
    qualifying.credit_rating
limit 100;
`

  ```text
  refused to write 'query69.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/store_sales as store;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/store_sales as store; impo...

  Write stats: received 3229 chars / 3229 bytes; tail: …'.purchase_estimate,\\n    qualifying.credit_rating\\nlimit 100;\\n'.
  ```
- `trilogy file write query70.preql --content import raw.store_sales as ss;

# Step 1: For store sales in year 2000, compute net profit by state
# to find top 5…by rollup (ss.store.state, ss.store.county)
having total_net_profit is not null
order by hierarchy_level desc, ss.store.state asc, rnk asc
limit 100;`

  ```text
  refused to write 'query70.preql': not syntactically valid Trilogy.

  Parse error:
    --> 30:28
     |
  30 |     grouping(ss.store.state, ss.store.county) as hierarchy_level,
     |                            ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...y,     grouping(ss.store.state ??? , ss.store.county) as hierarch...

  Write stats: received 1392 chars / 1392 bytes; tail: …'hierarchy_level desc, ss.store.state asc, rnk asc\\nlimit 100;'.
  ```
- `trilogy file write query70.preql --content import raw.store_sales as ss;

# Step 1: Top 5 states by net profit in 2000
with state_profits as
where ss.date.ye…s sp
    sum(ss.net_profit) as state_net_profit
;

# Wait, no - that's the same statement. state_profits is the rowset.
# Let me redo this properly.
`

  ```text
  refused to write 'query70.preql': not syntactically valid Trilogy.

  Parse error:
    --> 10:5
     |
  10 |     sum(ss.net_profit) as state_net_profit
     |     ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...ts.state_net_profit as sp     ??? sum(ss.net_profit) as state_ne...

  Write stats: received 368 chars / 368 bytes; tail: …'. state_profits is the rowset.\\n# Let me redo this properly.\\n'.
  ```
- `trilogy file write query72.preql --content import raw.catalog_sales as cs;
import raw.inventory as inv;

# Catalog sales in 1999 where:
# - billing-time hous…rders
order by
    total_orders desc nulls first,
    item_desc nulls first,
    warehouse_name nulls first,
    week_sequence nulls first
limit 100;`

  ```text
  refused to write 'query72.preql': not syntactically valid Trilogy.

  Parse error:
    --> 20:1
     |
  20 | where
     | ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
  ...s than catalog sales quantity ??? where      inv.quantity_on_han...

  Write stats: received 1349 chars / 1349 bytes; tail: …'e_name nulls first,\\n    week_sequence nulls first\\nlimit 100;'.
  ```
- `trilogy file write query75.preql -e -c import raw.all_sales as s;

# Step 1: Deduplicate per-line records - compute net qty and net amt per line
# All_sales …arly.yr_amt) over (partition by yearly.brand_id, yearly.class_id, yearly.category_id, yearly.manufacturer_id order by yearly.year asc) as prev_amt
;
`

  ```text
  refused to write 'query75.preql': not syntactically valid Trilogy.

  Parse error:
    --> 19:39
     |
  19 | auto year_agg <- sum(deduped.net_qty) as yr_qty;
     |                                       ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, aggregate_over, or window_sql_over
  Location:
  ...r_agg <- sum(deduped.net_qty) ??? as yr_qty; auto year_amt <- su...

  Write stats: received 1496 chars / 1496 bytes; tail: …'rly.manufacturer_id order by yearly.year asc) as prev_amt\\n;\\n'.
  ```
- `trilogy file write query76.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sales as cs;

with combined as union(…ce
order by
    combined.channel,
    combined.missing_ref,
    combined.year,
    combined.quarter,
    combined.category
    nulls first
limit 100;`

  ```text
  refused to write 'query76.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
   int, ext_sales_price numeric) ???  select     combined.channel,...

  Write stats: received 1772 chars / 1772 bytes; tail: …'ed.quarter,\\n    combined.category\\n    nulls first\\nlimit 100;'.
  ```
- `trilogy file write query77.preql --content import raw.all_sales as s;

select
    case
        when s.channel = 'STORE' then 'store channel'
        when s.c…_id is not null
by rollup (channel_label, s.outlet_id)
order by channel_label asc nulls first, s.outlet_id asc nulls first
desc nulls last
limit 100;`

  ```text
  refused to write 'query77.preql': not syntactically valid Trilogy.

  Parse error:
    --> 16:1
     |
  16 | desc nulls last
     | ^---
     |
     = expected limit
  Location:
  ..., s.outlet_id asc nulls first ??? desc nulls last limit 100;

  Write stats: received 839 chars / 839 bytes; tail: …'irst, s.outlet_id asc nulls first\\ndesc nulls last\\nlimit 100;'.
  ```
- `trilogy file write query78.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Store sales that were… desc, store_agg.store_wholesale desc, store_agg.store_sales_price desc, other_qty asc, other_wholesale asc, other_sales_price asc, ratio
limit 100;
`

  ```text
  refused to write 'query78.preql': not syntactically valid Trilogy.

  Parse error:
   --> 6:1
    |
  6 | rowset store_agg as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...ted by (year, item, customer) ??? rowset store_agg as where ss.d...

  Write stats: received 2656 chars / 2656 bytes; tail: …'ther_wholesale asc, other_sales_price asc, ratio\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw/all_sales:s select s.item.id, s.channel, sum(s.return_quantity) as total_rq, count(s.order_id) as row_count where s.return_date.week_seq in (5244, 5257, 5264) and s.is_returned = true and s.item.id = 'AAAAAAAAAFAAAAAA' group by s.item.id, s.channel;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ....item.id = 'AAAAAAAAAFAAAAAA' ??? group by s.item.id, s.channel;...
  ```
- `trilogy file write query95.preql -e -c import raw.web_sales as ws;

# Stage 1: Identify orders with >= 2 distinct warehouses AND at least one return
rowset q…s order_count,
    sum(ws.ext_ship_cost) as total_extended_shipping_cost,
    sum(ws.net_profit) as total_net_profit
order by order_count
limit 100;
`

  ```text
  refused to write 'query95.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:1
    |
  4 | rowset qualified_orders as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...ouses AND at least one return ??? rowset qualified_orders as sel...

  Write stats: received 808 chars / 808 bytes; tail: …'profit) as total_net_profit\\norder by order_count\\nlimit 100;\\n'.
  ```

### `cli-misuse`

- `trilogy explore raw/customer_demographics.preql`

  ```text
  Invalid value for 'PATH': File 'raw/customer_demographics.preql' does not exist.
  ```
- `trilogy explore raw/date_dim.preql`

  ```text
  Invalid value for 'PATH': File 'raw/date_dim.preql' does not exist.
  ```
- `trilogy explore raw/ship_date.preql`

  ```text
  Invalid value for 'PATH': File 'raw/ship_date.preql' does not exist.
  ```
- `trilogy explore raw/sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/sales.preql' does not exist.
  ```
- `trilogy explore raw/store_sales.facts.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.facts.preql' does not exist.
  ```
- `trilogy explore raw/web_sales.facts.preql`

  ```text
  Invalid value for 'PATH': File 'raw/web_sales.facts.preql' does not exist.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw/all_sales:all select all.channel all.channel_dim_text_id all.return_channel_dim_text_id limit 5;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `all.channel all.channel_dim_text_id all.return_channel_dim_text_id as all_channel_all_channel_dim_text_id_all_`
  Location:
  ...es as all; select all.channel ??? all.channel_dim_text_id all.re...
  ```
- `trilogy run --import raw/catalog_sales:cs select count_distinct(cs.billing_customer.id) where cs.item.category = 'Women' and cs.item.class = 'maternity' and cs.sold_date.year = 1998 and cs.sold_date.month_of_year = 12;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count_distinct(cs.billing_customer.id) as id_count_distinct`
  Location:
  ...tinct(cs.billing_customer.id) ??? where cs.item.category = 'Wome...
  ```
- `trilogy run --import raw/store:st select st.county, st.state, count(st.sk) limit 15;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(st.sk) as sk_count`
  Location:
  ...ounty, st.state, count(st.sk) ??? limit 15;
  ```
- `trilogy run --import raw/all_sales:s select s.channel, count(s.order_id) where s.return_date.week_seq in (5244, 5257, 5264) and s.is_returned = true group by s.channel;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(s.order_id) as order_id_count`
  Location:
   s.channel, count(s.order_id) ??? where s.return_date.week_seq i...
  ```

### `type-error`

- `trilogy run query32.preql`

  ```text
  Syntax error in query32.preql: Cannot use BETWEEN with incompatible types DATE and STRING (low)
  ```
- `trilogy run query76.preql`

  ```text
  Syntax error in query76.preql: Output column '_combined_quarter' declared as STRING but arms produce incompatible types {EnumType(type=<DataType.INTEGER: 'int'>, values=[1, 2, 3, 4])}
  ```
- `trilogy run query77.preql`

  ```text
  Syntax error in query77.preql: Cannot use BETWEEN with incompatible types DATE and STRING (low)
  ```
