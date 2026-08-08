# Trilogy failure analysis — 20260726-191805

- Run `20260726-191755_enriched` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 1212 | failed: 120 (10%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 66 | 55% |
| `syntax-parse` | 41 | 34% |
| `cli-misuse` | 9 | 8% |
| `syntax-missing-alias` | 2 | 2% |
| `join-resolution` | 1 | 1% |
| `planner-recursion` | 1 | 1% |

## Detail

### `other`

- `trilogy file read answer_1858999935.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3705756794.preql`

  ```text
  Resolution error in answer_3705756794.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {all_sales.channel_dim_id, all_sales.channel_dim_text_id, all_sales.sale_date.date, _sales_side_ch, _sales_side_ent, _sales_side_ext_sales, _sales_side_net_prof}; {_sales_side_ret_amt}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_3705756794.preql`

  ```text
  Syntax error in answer_3705756794.preql: Undefined concept: all_sales.row_counter. Suggestions: ['all_sales.coupon_amt', 'all_sales.return_amount', 'all_sales.warehouse.country']
  ```
- `trilogy run -e 100 answer_3705756794.preql`

  ```text
  Environment variable must be in KEY=VALUE format or be a path to an existing env file: 100
  ```
- `trilogy run answer_883027685.preql`

  ```text
  Resolution error in answer_883027685.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {cat_avg_price}; {qualifying_line_items, state, ss.customer.current_address.sk, ss.item.category, ss.item.current_price, ss.sale_date.month_of_year, ss.sale_date.year}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read answer_883027685.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3697440276.preql`

  ```text
  Resolution error in answer_3697440276.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 66). The requested concepts split into 2 disconnected subgraphs: {combined.s01_rev, combined.s01_rev, combined.s01_rev, combined.s01_rev, combined.s02_rev, combined.s02_rev, billing_customer_code, first_name, last_name, preferred_cust_flag, store.sale_date.year}; {combined.w01_rev, combined.w01_rev, combined.w01_rev, combined.w01_rev, combined.w02_rev, combined.w02_rev}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_3697440276.preql`

  ```text
  Resolution error in answer_3697440276.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 66). The requested concepts split into 2 disconnected subgraphs: {combined.s01_rev, combined.s01_rev, combined.s01_rev, combined.s01_rev, combined.s02_rev, combined.s02_rev, billing_customer_code, first_name, last_name, preferred_cust_flag}; {combined.w01_rev, combined.w01_rev, combined.w01_rev, combined.w01_rev, combined.w02_rev, combined.w02_rev}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_3697440276.preql`

  ```text
  trilogy error: subprocess timed out after 600s.
  ```
- `trilogy run answer_219203908.preql`

  ```text
  Unexpected error in answer_219203908.preql: (_duckdb.BinderException) Binder Error: GROUPING child "lower(kaput.sales_channel)" must be a grouping column

  LINE 206:     grouping(LOWER("kaput"."sales_channel") ) as "_virt_agg_gro...
                ^
  [SQL:
  WITH
  cheerful as (
  SELECT
       'CATALOG'  as "sales_channel",
      "sales_catalog_sales_unified"."CS_ITEM_SK" as "sales_item_sk",
      "sales_catalog_sales_unified"."CS_LIST_PRICE" as "sales_list_price",
      "sales_catalog_sales_unified"."CS_ORDER_NUMBER" as "sales_order_id",
      "sales_catalog_sales_unified"."CS_QUANTITY" as "sales_quantity",
      "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "sales_sale_date_sk"
  FROM
      "catalog_sales" as "sales_catalog_sales_unified"
  UNION ALL
  SELECT
       'STORE'  as "sales_channel",
      "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_sk",
      "sales_store_sales_unified"."SS_LIST_PRICE" as "sales_list_price",
      "sales_store_sales_unified"."SS_TICKET_NUMBER" as "sales_order_id",
      "sales_store_sales_unified"."SS_QUANTITY" as "sales_quantity",
      "sales_store_sales_unified"."SS_SOLD_DATE_SK" as "sales_sale_date_sk"
  FROM
      "store_sales" as "sales_store_sales_unified"
  UNION ALL
  SELECT
       'WEB'  as "sales_channel",
      "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_sk",
      "sales_web_sales_unified"."WS_LIST_PRICE" as "sales_list_price",
      "sales_web_sales_unified"."WS_ORDER_NUMBER" as "sales_order_id",
      "sales_web_sales_unified"."WS_QUANTITY" as "sales_quantity",
      "sales_web_sales_unified"."WS_SOLD_DATE_SK" as "sales_sale_date_sk"
  FROM
      "web_sales" as "sales_web_sales_unified"),
  premium as (
  SELECT
      avg("cheerful"."sales_quantity" * "cheerful"."sales_list_price") as "_subquery_43_52_overall_avg_overall_average"
  FROM
      "cheerful"
      INNER JOIN "date_dim" as "sales_sale_date_date" on "cheerful"."sales_sale_date_sk" = "sales_sale_date_date"."D_DATE_SK"
  WHERE
      "sales_sale_date_date"."D_YEAR" BETWEEN 1999 AND 2001
  ),
  abhorrent as (
  SELECT
      "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_sk",
      "sales_web_sales_unified"."WS_SOLD_DATE_SK" as "sales_sale_date_sk"
  FROM
      "web_sales" as "sales_web_sales_unified"
  WHERE
       'WEB'  = 'WEB'

  GROUP BY
      1,
      2,
       'WEB' ),
  sweltering as (
  SELECT
      "sales_item_items"."I_BRAND_ID" as "___tvf_arm_2_bid",
      "sales_item_items"."I_CATEGORY_ID" as "___tvf_arm_2_catid",
      "sales_item_items"."I_CLASS_ID" as "___tvf_arm_2_cid"
  FROM
      "item" as "sales_item_items"
      INNER JOIN "abhorrent" on "sales_item_items"."I_ITEM_SK" = "abhorrent"."sales_item_sk"
      INNER JOIN "date_dim" as "sales_sale_date_date" on "abhorrent"."sales_sale_date_sk" = "sales_sale_date_date"."D_DATE_SK"
  WHERE
      "sales_sale_date_date"."D_YEAR" BETWEEN 1999 AND 2001 and "sales_item_items"."I_BRAND_ID" is not null and "sales_item_items"."I_CLASS_ID" is not null and "sales_item_items"."I_CATEGORY_ID" is not null
  ),
  vacuous as (
  SELECT
      "sales_catalog_sales_unified"."CS_ITEM_SK" as "sales_item_sk",
      "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "sales_sale_date_sk"
  FROM
      "catalog_sales" as "sales_catalog_sales_unified"
  WHERE
       'CATALOG'  = 'CATALOG'

  GROUP BY
      1,
      2,
       'CATALOG' ),
  concerned as (
  SELECT
      "sales_item_items"."I_BRAND_ID" as "___tvf_arm_1_bid",
      "sales_item_items"."I_CATEGORY_ID" as "___tvf_arm_1_catid",
      "sales_item_items"."I_CLASS_ID" as "___tvf_arm_1_cid"
  FROM
      "item" as "sales_item_items"
      INNER JOIN "vacuous" on "sales_item_items"."I_ITEM_SK" = "vacuous"."sales_item_sk"
      INNER JOIN "date_dim" as "sales_sale_date_date" on "vacuous"."sales_sale_date_sk" = "sales_sale_date_date"."D_DATE_SK"
  WHERE
      "sales_sale_date_date"."D_YEAR" BETWEEN 1999 AND 2001 and "sales_item_items"."I_BRAND_ID" is not null and "sales_item_items"."I_CLASS_ID" is not null and "sales_item_items"."I_CATEGORY_ID" is not null
  ),
  abundant as (
  SELECT
      "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_sk",
      "sales_store_sales_unified"."SS_SOLD_DATE_SK" as "sales_sale_date_sk"
  FROM
      "store_sales" as "sales_store_sales_unified"
  WHERE
       'STORE'  = 'STORE'

  GROUP BY
      1,
      2,
       'STORE' ),
  uneven as (
  SELECT
      "sales_item_items"."I_BRAND_ID" as "___tvf_arm_0_bid",
      "sales_item_items"."I_CATEGORY_ID" as "___tvf_arm_0_catid",
      "sales_item_items"."I_CLASS_ID" as "___tvf_arm_0_cid"
  FROM
      "item" as "sales_item_items"
      INNER JOIN "abundant" on "sales_item_items"."I_ITEM_SK" = "abundant"."sales_item_sk"
      INNER JOIN "date_dim" as "sales_sale_date_date" on "abundant"."sales_sale_date_sk" = "sales_sale_date_date"."D_DATE_SK"
  WHERE
      "sales_sale_date_date"."D_YEAR" BETWEEN 1999 AND 2001 and "sales_item_items"."I_BRAND_ID" is not null and "sales_item_items"."I_CLASS_ID" is not null and "sales_item_items"."I_CATEGORY_ID" is not null
  ),
  scrawny as (
  SELECT
      "uneven"."___tvf_arm_0_bid" as "_common_tuples_bid",
      "uneven"."___tvf_arm_0_cid" as "_common_tuples_cid",
      "uneven"."___tvf_arm_0_catid" as "_common_tuples_catid"
  FROM
      "uneven"
  GROUP BY
      1,
      2,
      3
  INTERSECT
  SELECT
      "concerned"."___tvf_arm_1_bid" as "_common_tuples_bid",
      "concerned"."___tvf_arm_1_cid" as "_common_tuples_cid",
      "concerned"."___tvf_arm_1_catid" as "_common_tuples_catid"
  FROM
      "concerned"
  GROUP BY
      1,
      2,
      3
  INTERSECT
  SELECT
      "sweltering"."___tvf_arm_2_bid" as "_common_tuples_bid",
      "sweltering"."___tvf_arm_2_cid" as "_common_tuples_cid",
      "sweltering"."___tvf_arm_2_catid" as "_common_tuples_catid"
  FROM
      "sweltering"
  GROUP BY
      1,
      2,
      3),
  friendly as (
  SELECT
      "scrawny"."_common_tuples_bid" as "common_tuples_bid",
      "scrawny"."_common_tuples_catid" as "common_tuples_catid",
      "scrawny"."_common_tuples_cid" as "common_tuples_cid"
  FROM
      "scrawny"),
  questionable as (
  SELECT
      "cheerful"."sales_channel" as "sales_channel",
      "cheerful"."sales_item_sk" as "sales_item_sk",
      "cheerful"."sales_list_price" as "sales_list_price",
      "cheerful"."sales_order_id" as "sales_order_id",
      "cheerful"."sales_quantity" as "sales_quantity",
      "sales_item_items"."I_BRAND_ID" as "sales_item_brand_id",
      "sales_item_items"."I_CATEGORY_ID" as "sales_item_category_id",
      "sales_item_items"."I_CLASS_ID" as "sales_item_class_id"
  FROM
      "cheerful"
      INNER JOIN "item" as "sales_item_items" on "cheerful"."sales_item_sk" = "sales_item_items"."I_ITEM_SK"
      INNER JOIN "date_dim" as "sales_sale_date_date" on "cheerful"."sales_sale_date_sk" = "sales_sale_date_date"."D_DATE_SK"
  WHERE
      "sales_sale_date_date"."D_YEAR" = 2001 and "sales_sale_date_date"."D_MOY" = 11 and "sales_item_items"."I_BRAND_ID" is not null and "sales_item_items"."I_CLASS_ID" is not null and "sales_item_items"."I_CATEGORY_ID" is not null and exists (select 1 from friendly where friendly."common_tuples_bid" is not distinct from "sales_item_items"."I_BRAND_ID" and friendly."common_tuples_cid" is not distinct from "sales_item_items"."I_CLASS_ID" and friendly."common_tuples_catid" is not distinct from "sales_item_items"."I_CATEGORY_ID")
  ),
  charming as (
  SELECT
      "questionable"."sales_item_brand_id" as "sales_item_brand_id",
      "questionable"."sales_item_category_id" as "sales_item_category_id",
      "questionable"."sales_item_class_id" as "sales_item_class_id",
      LOWER("questionable"."sales_channel")  as "_virt_func_lower_7201072160963045",
      LOWER("questionable"."sales_channel")  as "channel"
  FROM
      "questionable"),
  kaput as (
  SELECT
      "questionable"."sales_channel" as "sales_channel",
      "questionable"."sales_item_brand_id" as "sales_item_brand_id",
      "questionable"."sales_item_category_id" as "sales_item_category_id",
      "questionable"."sales_item_class_id" as "sales_item_class_id",
      "questionable"."sales_item_sk" as "sales_item_sk",
      "questionable"."sales_list_price" as "sales_list_price",
      "questionable"."sales_order_id" as "sales_order_id",
      "questionable"."sales_quantity" as "sales_quantity",
      LOWER("questionable"."sales_channel")  as "_virt_func_lower_7201072160963045"
  FROM
      "questionable"),
  busy as (
  SELECT
      "kaput"."_virt_func_lower_7201072160963045" as "_virt_func_lower_7201072160963045",
      "kaput"."sales_item_brand_id" as "sales_item_brand_id",
      "kaput"."sales_item_category_id" as "sales_item_category_id",
      "kaput"."sales_item_class_id" as "sales_item_class_id",
      count(md5(CONCAT_WS('', coalesce(cast("kaput"."sales_channel" as string),'
  '), coalesce(cast("kaput"."sales_order_id" as string),'
  '), coalesce(cast("kaput"."sales_item_sk" as string),'
  ')))) as "line_item_count",
      grouping("kaput"."sales_item_brand_id") as "_virt_agg_grouping_9963063586325974",
      grouping("kaput"."sales_item_category_id") as "_virt_agg_grouping_9172849048368882",
      grouping("kaput"."sales_item_class_id") as "_virt_agg_grouping_4087352663634991",
      grouping(LOWER("kaput"."sales_channel") ) as "_virt_agg_grouping_8193253771704312",
      sum("kaput"."sales_quantity" * "kaput"."sales_list_price") as "total_sales"
  FROM
      "kaput"
  GROUP BY
      ROLLUP (1, 2, 4, 3))
  SELECT
      "charming"."channel" as "channel",
      "busy"."sales_item_brand_id" as "sales_item_brand_id",
      "busy"."sales_item_class_id" as "sales_item_class_id",
      "busy"."sales_item_category_id" as "sales_item_category_id",
      "busy"."total_sales" as "total_sales",
      "busy"."line_item_count" as "line_item_count"
  FROM
      "busy"
      INNER JOIN "charming" on "busy"."_virt_func_lower_7201072160963045" = "charming"."_virt_func_lower_7201072160963045" AND "busy"."sales_item_brand_id" is not distinct from "charming"."sales_item_brand_id" AND "busy"."sales_item_category_id" is not distinct from "charming"."sales_item_category_id" AND "busy"."sales_item_class_id" is not distinct from "charming"."sales_item_class_id"
      LEFT OUTER JOIN "premium" on 1=1
  WHERE
      ( ( ( "busy"."_virt_agg_grouping_8193253771704312" + "busy"."_virt_agg_grouping_9963063586325974" ) + "busy"."_virt_agg_grouping_4087352663634991" ) + "busy"."_virt_agg_grouping_9172849048368882" = 0 and "busy"."total_sales" > "premium"."_subquery_43_52_overall_avg_overall_average" ) or ( ( "busy"."_virt_agg_grouping_9963063586325974" + "busy"."_virt_agg_grouping_4087352663634991" ) + "busy"."_virt_agg_grouping_9172849048368882" > 0 )

  ORDER BY
      "charming"."channel" asc nulls first,
      "busy"."sales_item_brand_id" asc nulls first,
      "busy"."sales_item_class_id" asc nulls first,
      "busy"."sales_item_category_id" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_765177085.preql`

  ```text
  Resolution error in answer_765177085.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 35). The requested concepts split into 2 disconnected subgraphs: {cat_qty_avg, cat_qty_count, cat_qty_cv, cat_qty_stddev}; {return_qty_avg, return_qty_count, return_qty_cv, return_qty_stddev, store_qty_avg, store_qty_count, store_qty_cv, store_qty_stddev, store_sales_filtered.item_desc, store_sales_filtered.item_id, store_sales_filtered.store_customer_sk, store_sales_filtered.store_item_sk, store_sales_filtered.store_state}. Are you missing a join or merge statement to relate them?
  Note: the membership predicate(s) `(store_sales_filtered.store_customer_sk) in (catalog_sales_filtered.cat_customer_sk)`; `(store_sales_filtered.store_item_sk) in (catalog_sales_filtered.cat_item_sk)` span these subgraphs, but membership only filters rows on its left side — it does not join the two sides, so it cannot relate them for outputs or grouping. To combine values from both sides, author a query-scoped join or a merge on shared keys.
  ```
- `trilogy run answer_765177085.preql`

  ```text
  Syntax error in answer_765177085.preql: Ambiguous reference 'store_agg.item_sk': matches ['store_agg.match_pairs.item_sk', 'store_agg.ss_line.item_sk']. Qualify the full path to disambiguate.
  ```
- `trilogy file read answer_2133330107.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 112 column 3 (char 3784). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file read raw/catalog_store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_2802535988.preql`

  ```text
  Resolution error in answer_2802535988.preql: WHERE input(s) ['ws.return_customer.current_address.state'] cannot be related to the query outputs ['local.birth_country', 'local.birth_day', 'local.birth_month', 'local.birth_year', 'local.customer_code', 'local.email_address', 'local.first_name', 'local.last_name', 'local.last_review_date', 'local.login', 'local.preferred_cust_flag', 'local.salutation', 'local.total_web_return_amount']: no join or merge connects the filter's source to any output-producing source. Add a join/merge relating them, or select a concept from the filter's model.
  ```
- `trilogy run answer_2802535988.preql`

  ```text
  Unexpected error in answer_2802535988.preql: Could not render the query: Missing source reference to ws.return_customer.salutation; Missing source reference to ws.return_customer.first_name; Missing source reference to ws.return_customer.last_name; Missing source reference to ws.return_customer.preferred_cust_flag; Missing source reference to ws.return_customer.birth_day; Missing source reference to ws.return_customer.birth_month; Missing source reference to ws.return_customer.birth_year; Missing source reference to ws.return_customer.birth_country; Missing source reference to ws.return_customer.login; Missing source reference to ws.return_customer.email_address; Missing source reference to ws.return_customer.last_review_date. A planned reference has no backing source CTE -- typically an unsupported cross-rowset or membership shape the planner could not wire. Review the rowset/join structure (or file an issue if the query looks valid).

  Full SQL with sentinel(s):

  WITH
  vacuous as (
  SELECT
      "ws_return_customer_customers"."C_CUSTOMER_ID" as "customer_code",
      "ws_return_customer_customers"."C_CUSTOMER_SK" as "ws_return_customer_sk"
  FROM
      "customer" as "ws_return_customer_customers"),
  uneven as (
  SELECT
      "ws_return_customer_current_address_customer_address"."CA_STATE" as "current_addr_state"
  FROM
      "customer_address" as "ws_return_customer_current_address_customer_address"
  GROUP BY
      1),
  thoughtful as (
  SELECT
      "ws_return_address_customer_address"."CA_STATE" as "ws_return_address_state",
      "ws_web_returns"."WR_RETURNING_CUSTOMER_SK" as "ws_return_customer_sk",
      sum(CASE WHEN "ws_return_date_date"."D_YEAR" = 2002 and ("ws_web_returns"."WR_ORDER_NUMBER" is not null) = True THEN "ws_web_returns"."WR_RETURN_AMT" ELSE NULL END) as "cust_state_return_amt_2002",
      sum(CASE WHEN "ws_return_date_date"."D_YEAR" = 2002 and ("ws_web_returns"."WR_ORDER_NUMBER" is not null) = True THEN "ws_web_returns"."WR_RETURN_AMT" ELSE NULL END) as "total_web_return_amount"
  FROM
      "web_sales" as "ws_web_sales"
      LEFT OUTER JOIN "web_returns" as "ws_web_returns" on "ws_web_sales"."WS_ITEM_SK" = "ws_web_returns"."WR_ITEM_SK" AND "ws_web_sales"."WS_ORDER_NUMBER" = "ws_web_returns"."WR_ORDER_NUMBER"
      FULL JOIN "date_dim" as "ws_return_date_date" on "ws_web_returns"."WR_RETURNED_DATE_SK" = "ws_return_date_date"."D_DATE_SK"
      RIGHT OUTER JOIN "customer_address" as "ws_return_address_customer_address" on "ws_web_returns"."WR_RETURNING_ADDR_SK" = "ws_return_address_customer_address"."CA_ADDRESS_SK"
  WHERE
      "ws_return_address_customer_address"."CA_STATE" is not null

  GROUP BY
      1,
      2),
  abundant as (
  SELECT
      "thoughtful"."ws_return_address_state" as "ws_return_address_state",
      avg("thoughtful"."cust_state_return_amt_2002") as "state_avg_return_amt_2002"
  FROM
      "thoughtful"
  GROUP BY
      1),
  young as (
  SELECT
      "abundant"."state_avg_return_amt_2002" as "state_avg_return_amt_2002",
      "thoughtful"."total_web_return_amount" as "total_web_return_amount",
      "uneven"."current_addr_state" as "current_addr_state",
      "vacuous"."customer_code" as "customer_code"
  FROM
      "thoughtful"
      INNER JOIN "abundant" on "thoughtful"."ws_return_address_state" is not distinct from "abundant"."ws_return_address_state"
      INNER JOIN "uneven" on 1=1
      LEFT OUTER JOIN "vacuous" on "thoughtful"."ws_return_customer_sk" = "vacuous"."ws_return_customer_sk"
  WHERE
      "thoughtful"."total_web_return_amount" > 1.2 * "abundant"."state_avg_return_amt_2002"

  GROUP BY
      1,
      2,
      3,
      4)
  SELECT
      "young"."customer_code" as "customer_code",
      INVALID_REFERENCE_BUG<Missing source reference to ws.return_customer.salutation> as "salutation",
      INVALID_REFERENCE_BUG<Missing source reference to ws.return_customer.first_name> as "first_name",
      INVALID_REFERENCE_BUG<Missing source reference to ws.return_customer.last_name> as "last_name",
      INVALID_REFERENCE_BUG<Missing source reference to ws.return_customer.preferred_cust_flag> as "preferred_cust_flag",
      INVALID_REFERENCE_BUG<Missing source reference to ws.return_customer.birth_day> as "birth_day",
      INVALID_REFERENCE_BUG<Missing source reference to ws.return_customer.birth_month> as "birth_month",
      INVALID_REFERENCE_BUG<Missing source reference to ws.return_customer.birth_year> as "birth_year",
      INVALID_REFERENCE_BUG<Missing source reference to ws.return_customer.birth_country> as "birth_country",
      INVALID_REFERENCE_BUG<Missing source reference to ws.return_customer.login> as "login",
      INVALID_REFERENCE_BUG<Missing source reference to ws.return_customer.email_address> as "email_address",
      INVALID_REFERENCE_BUG<Missing source reference to ws.return_customer.last_review_date> as "last_review_date",
      "young"."current_addr_state" as "current_addr_state",
      "young"."total_web_return_amount" as "total_web_return_amount"
  FROM
      "young"
  WHERE
      "young"."current_addr_state" = 'GA'

  ORDER BY
      "young"."customer_code" asc nulls first,
      "salutation" asc nulls first,
      "first_name" asc nulls first,
      "last_name" asc nulls first,
      "preferred_cust_flag" asc nulls first,
      "birth_day" asc nulls first,
      "birth_month" asc nulls first,
      "birth_year" asc nulls first,
      "birth_country" asc nulls first,
      "login" asc nulls first,
      "email_address" asc nulls first,
      "last_review_date" asc nulls first,
      "young"."total_web_return_amount" asc nulls first
  LIMIT (100)
  ```
- `trilogy run answer_2802535988.preql`

  ```text
  Unexpected error in answer_2802535988.preql: Could not render the query: Missing source reference to ws.return_customer.salutation; Missing source reference to ws.return_customer.first_name; Missing source reference to ws.return_customer.last_name; Missing source reference to ws.return_customer.preferred_cust_flag; Missing source reference to ws.return_customer.birth_day; Missing source reference to ws.return_customer.birth_month; Missing source reference to ws.return_customer.birth_year; Missing source reference to ws.return_customer.birth_country; Missing source reference to ws.return_customer.login; Missing source reference to ws.return_customer.email_address; Missing source reference to ws.return_customer.last_review_date. A planned reference has no backing source CTE -- typically an unsupported cross-rowset or membership shape the planner could not wire. Review the rowset/join structure (or file an issue if the query looks valid).

  Full SQL with sentinel(s):

  WITH
  vacuous as (
  SELECT
      "ws_return_customer_customers"."C_CUSTOMER_ID" as "customer_code",
      "ws_return_customer_customers"."C_CUSTOMER_SK" as "ws_return_customer_sk"
  FROM
      "customer" as "ws_return_customer_customers"),
  uneven as (
  SELECT
      "ws_return_customer_current_address_customer_address"."CA_STATE" as "current_addr_state"
  FROM
      "customer_address" as "ws_return_customer_current_address_customer_address"
  GROUP BY
      1),
  thoughtful as (
  SELECT
      "ws_return_address_customer_address"."CA_STATE" as "ws_return_address_state",
      "ws_web_returns"."WR_RETURNING_CUSTOMER_SK" as "ws_return_customer_sk",
      sum(CASE WHEN "ws_return_date_date"."D_YEAR" = 2002 and ("ws_web_returns"."WR_ORDER_NUMBER" is not null) = True THEN "ws_web_returns"."WR_RETURN_AMT" ELSE NULL END) as "cust_state_return_amt_2002",
      sum(CASE WHEN "ws_return_date_date"."D_YEAR" = 2002 and ("ws_web_returns"."WR_ORDER_NUMBER" is not null) = True THEN "ws_web_returns"."WR_RETURN_AMT" ELSE NULL END) as "total_web_return_amount"
  FROM
      "web_sales" as "ws_web_sales"
      LEFT OUTER JOIN "web_returns" as "ws_web_returns" on "ws_web_sales"."WS_ITEM_SK" = "ws_web_returns"."WR_ITEM_SK" AND "ws_web_sales"."WS_ORDER_NUMBER" = "ws_web_returns"."WR_ORDER_NUMBER"
      FULL JOIN "date_dim" as "ws_return_date_date" on "ws_web_returns"."WR_RETURNED_DATE_SK" = "ws_return_date_date"."D_DATE_SK"
      RIGHT OUTER JOIN "customer_address" as "ws_return_address_customer_address" on "ws_web_returns"."WR_RETURNING_ADDR_SK" = "ws_return_address_customer_address"."CA_ADDRESS_SK"
  WHERE
      "ws_return_address_customer_address"."CA_STATE" is not null

  GROUP BY
      1,
      2),
  abundant as (
  SELECT
      "thoughtful"."ws_return_address_state" as "ws_return_address_state",
      avg("thoughtful"."cust_state_return_amt_2002") as "state_avg_return_amt_2002"
  FROM
      "thoughtful"
  GROUP BY
      1),
  young as (
  SELECT
      "abundant"."state_avg_return_amt_2002" as "state_avg_return_amt_2002",
      "thoughtful"."total_web_return_amount" as "total_web_return_amount",
      "uneven"."current_addr_state" as "current_addr_state",
      "vacuous"."customer_code" as "customer_code"
  FROM
      "thoughtful"
      INNER JOIN "abundant" on "thoughtful"."ws_return_address_state" is not distinct from "abundant"."ws_return_address_state"
      INNER JOIN "uneven" on 1=1
      LEFT OUTER JOIN "vacuous" on "thoughtful"."ws_return_customer_sk" = "vacuous"."ws_return_customer_sk"
  WHERE
      "thoughtful"."total_web_return_amount" > 1.2 * "abundant"."state_avg_return_amt_2002"

  GROUP BY
      1,
      2,
      3,
      4)
  SELECT
      "young"."customer_code" as "customer_code",
      INVALID_REFERENCE_BUG<Missing source reference to ws.return_customer.salutation> as "salutation",
      INVALID_REFERENCE_BUG<Missing source reference to ws.return_customer.first_name> as "first_name",
      INVALID_REFERENCE_BUG<Missing source reference to ws.return_customer.last_name> as "last_name",
      INVALID_REFERENCE_BUG<Missing source reference to ws.return_customer.preferred_cust_flag> as "preferred_cust_flag",
      INVALID_REFERENCE_BUG<Missing source reference to ws.return_customer.birth_day> as "birth_day",
      INVALID_REFERENCE_BUG<Missing source reference to ws.return_customer.birth_month> as "birth_month",
      INVALID_REFERENCE_BUG<Missing source reference to ws.return_customer.birth_year> as "birth_year",
      INVALID_REFERENCE_BUG<Missing source reference to ws.return_customer.birth_country> as "birth_country",
      INVALID_REFERENCE_BUG<Missing source reference to ws.return_customer.login> as "login",
      INVALID_REFERENCE_BUG<Missing source reference to ws.return_customer.email_address> as "email_address",
      INVALID_REFERENCE_BUG<Missing source reference to ws.return_customer.last_review_date> as "last_review_date",
      "young"."current_addr_state" as "current_addr_state",
      "young"."total_web_return_amount" as "total_web_return_amount"
  FROM
      "young"
  WHERE
      "young"."current_addr_state" = 'GA'

  ORDER BY
      "young"."customer_code" asc nulls first,
      "salutation" asc nulls first,
      "first_name" asc nulls first,
      "last_name" asc nulls first,
      "preferred_cust_flag" asc nulls first,
      "birth_day" asc nulls first,
      "birth_month" asc nulls first,
      "birth_year" asc nulls first,
      "birth_country" asc nulls first,
      "login" asc nulls first,
      "email_address" asc nulls first,
      "last_review_date" asc nulls first,
      "young"."total_web_return_amount" asc nulls first
  LIMIT (100)
  ```
- `trilogy run answer_2802535988.preql`

  ```text
  Resolution error in answer_2802535988.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {cust_state_totals.birth_country, cust_state_totals.birth_day, cust_state_totals.birth_month, cust_state_totals.birth_year, cust_state_totals.curr_addr_state, cust_state_totals.cust_id, cust_state_totals.cust_sk, cust_state_totals.email_address, cust_state_totals.first_name, cust_state_totals.last_name, cust_state_totals.last_review_date, cust_state_totals.login, cust_state_totals.preferred_cust_flag, cust_state_totals.ret_state, cust_state_totals.salutation, cust_state_totals.total_return_amt}; {state_avgs.state_avg_amt}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_4207382245.preql`

  ```text
  Resolution error in answer_4207382245.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {store_by_q.county, store_by_q.quarter, web_by_q.county, web_by_q.quarter}
  ```
- `trilogy run answer_4207382245.preql`

  ```text
  Syntax error in answer_4207382245.preql: HAVING clause aggregate `sum(<Filter: ref:web_by_q.web_ext_price where ref:web_by_q.quarter = 2>) by local.county` is not in the SELECT projection and would change the select grain if added (line 23). HAVING can only filter on off-grain or nested aggregates that are also computed in the SELECT. Fix one of: (a) add it to SELECT — prefix with `--` to keep it out of the output rows, e.g. `select ..., --sum(<Filter: ref:web_by_q.web_ext_price where ref:web_by_q.quarter = 2>) by local.county`; (b) move the filter to WHERE — for an aggregate condition on a non-output grain, write the aggregate inline as `agg(x) by grain` directly in WHERE.
  ```
- `trilogy run answer_3809267817.preql`

  ```text
  Syntax error in answer_3809267817.preql: 3 undefined concept references; fix all before re-running:
    - dependent_count (line 42, col 5, in ORDER BY); did you mean: c.current_demographics.dependent_count, ss.pos_customer_demographic.dependent_count, cs.pos_customer_demographic.dependent_count, ss.pos_household_demographic.dependent_count, cs.pos_household_demographic.dependent_count, ss.return_customer_demographic.dependent_count?
    - employed_dependent_count (line 43, col 5, in ORDER BY); did you mean: c.current_demographics.employed_dependent_count, ss.pos_customer_demographic.employed_dependent_count, cs.pos_customer_demographic.employed_dependent_count, ss.return_customer_demographic.employed_dependent_count, ws.return_customer_demographic.employed_dependent_count, cs.return_customer_demographic.employed_dependent_count?
    - college_dependent_count (line 44, col 5, in ORDER BY); did you mean: c.current_demographics.college_dependent_count, ss.pos_customer_demographic.college_dependent_count, cs.pos_customer_demographic.college_dependent_count, ss.return_customer_demographic.college_dependent_count, ws.return_customer_demographic.college_dependent_count, cs.return_customer_demographic.college_dependent_count?
  ```
- `trilogy run answer_4140546834.preql`

  ```text
  Syntax error in answer_4140546834.preql: Undefined concept: _virt_agg_grouping_2599847655796781.
  ```
- `trilogy file read answer_4140546834.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read answer_3770074305.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read answer_142117636.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/item.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_968172492.preql`

  ```text
  Syntax error in answer_968172492.preql: Undefined concept: item.sk. Suggestions: ['ws.item.sk', 'ws.web_site.sk', 'ws.ship_date.sk', 'ws.ship_customer.sk', 'ws.sale_date.sk', 'ws.sale_time.sk']
  ```
- `trilogy run answer_1965638525.preql`

  ```text
  Syntax error in answer_1965638525.preql: 6 undefined concept references; fix all before re-running:
    - local.coupon_amt (line 17, in SELECT); did you mean: ss.coupon_amt, total_coupon_amount?
    - local.net_profit (line 18, in SELECT); did you mean: ss.net_profit, total_net_profit, ss.profit?
    - sale_date.day_name (line 4, col 5, in WHERE); did you mean: ss.sale_date.day_name, sale_date.year, ss.return_date.day_name, ss.customer.first_sales_date.day_name, ss.return_customer.first_sales_date.day_name, ss.customer.first_shipto_date.day_name?
    - sale_date.year (line 5, col 9, in WHERE); did you mean: ss.sale_date.year, sale_date.day_name, ss.return_date.year, ss.customer.first_sales_date.year, ss.return_customer.first_sales_date.year, ss.customer.first_shipto_date.year?
    - pos_household_demographic.dependent_count (line 8, col 9, in WHERE); did you mean: ss.pos_household_demographic.dependent_count, pos_household_demographic.vehicle_count, ss.return_household_demographic.dependent_count, ss.pos_customer_demographic.dependent_count, ss.customer.current_household_demographics.dependent_count, ss.return_customer_demographic.dependent_count?
    - pos_household_demographic.vehicle_count (line 9, col 12, in WHERE); did you mean: ss.pos_household_demographic.vehicle_count, pos_household_demographic.dependent_count, ss.return_household_demographic.vehicle_count, ss.customer.current_household_demographics.vehicle_count, ss.return_customer.current_household_demographics.vehicle_count?
  ```
- `trilogy file read trilogy.toml`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file list assets --recursive`

  ```text
  No such path: assets
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3560698360.preql`

  ```text
  Unexpected error in answer_3560698360.preql: (_duckdb.BinderException) Binder Error: window function calls cannot be nested

  LINE 21:     max(sum("juicy"."_web_daily_daily_web_sales_price") over (parti...
                   ^
  [SQL:
  WITH
  juicy as (
  SELECT
      "web_web_sales"."WS_ITEM_SK" as "web_item_sk",
      cast("web_sale_date_date"."D_DATE" as date) as "web_sale_date_date",
      sum("web_web_sales"."WS_SALES_PRICE") as "_web_daily_daily_web_sales_price"
  FROM
      "web_sales" as "web_web_sales"
      INNER JOIN "date_dim" as "web_sale_date_date" on "web_web_sales"."WS_SOLD_DATE_SK" = "web_sale_date_date"."D_DATE_SK"
  WHERE
      "web_sale_date_date"."D_YEAR" = 2000

  GROUP BY
      1,
      2),
  concerned as (
  SELECT
      "juicy"."web_item_sk" as "web_daily_item_sk",
      "juicy"."web_sale_date_date" as "web_daily_sale_date",
      max(sum("juicy"."_web_daily_daily_web_sales_price") over (partition by "juicy"."web_item_sk" order by "juicy"."web_sale_date_date" asc )) over (partition by "juicy"."web_item_sk" order by "juicy"."web_sale_date_date" asc ) as "_web_running_web_run_max_of_run_total",
      sum("juicy"."_web_daily_daily_web_sales_price") over (partition by "juicy"."web_item_sk" order by "juicy"."web_sale_date_date" asc ) as "_web_running_web_running_total"
  FROM
      "juicy"),
  abhorrent as (
  SELECT
      "concerned"."_web_running_web_run_max_of_run_total" as "web_running_web_run_max_of_run_total",
      "concerned"."_web_running_web_running_total" as "web_running_web_running_total",
      "concerned"."web_daily_item_sk" as "web_running_web_daily_item_sk",
      "concerned"."web_daily_sale_date" as "web_running_web_daily_sale_date"
  FROM
      "concerned"),
  wakeful as (
  SELECT
      "store_store_sales"."SS_ITEM_SK" as "store_item_sk",
      cast("store_sale_date_date"."D_DATE" as date) as "store_sale_date_date",
      sum("store_store_sales"."SS_SALES_PRICE") as "_store_daily_daily_store_sales_price"
  FROM
      "store_sales" as "store_store_sales"
      INNER JOIN "date_dim" as "store_sale_date_date" on "store_store_sales"."SS_SOLD_DATE_SK" = "store_sale_date_date"."D_DATE_SK"
  WHERE
      "store_sale_date_date"."D_YEAR" = 2000

  GROUP BY
      1,
      2),
  thoughtful as (
  SELECT
      "wakeful"."store_item_sk" as "store_daily_item_sk",
      "wakeful"."store_sale_date_date" as "store_daily_sale_date",
      max(sum("wakeful"."_store_daily_daily_store_sales_price") over (partition by "wakeful"."store_item_sk" order by "wakeful"."store_sale_date_date" asc )) over (partition by "wakeful"."store_item_sk" order by "wakeful"."store_sale_date_date" asc ) as "_store_running_store_run_max_of_run_total",
      sum("wakeful"."_store_daily_daily_store_sales_price") over (partition by "wakeful"."store_item_sk" order by "wakeful"."store_sale_date_date" asc ) as "_store_running_store_running_total"
  FROM
      "wakeful"),
  abundant as (
  SELECT
      "thoughtful"."_store_running_store_run_max_of_run_total" as "store_running_store_run_max_of_run_total",
      "thoughtful"."_store_running_store_running_total" as "store_running_store_running_total",
      "thoughtful"."store_daily_item_sk" as "store_running_store_daily_item_sk",
      "thoughtful"."store_daily_sale_date" as "store_running_store_daily_sale_date"
  FROM
      "thoughtful"),
  sweltering as (
  SELECT
      "abhorrent"."web_running_web_run_max_of_run_total" as "web_running_web_run_max_of_run_total",
      "abhorrent"."web_running_web_running_total" as "web_running_web_running_total",
      "abundant"."store_running_store_run_max_of_run_total" as "store_running_store_run_max_of_run_total",
      "abundant"."store_running_store_running_total" as "store_running_store_running_total",
      coalesce("abhorrent"."web_running_web_daily_item_sk","abundant"."store_running_store_daily_item_sk") as "store_running_store_daily_item_sk",
      coalesce("abhorrent"."web_running_web_daily_item_sk","abundant"."store_running_store_daily_item_sk") as "web_running_web_daily_item_sk",
      coalesce("abhorrent"."web_running_web_daily_sale_date","abundant"."store_running_store_daily_sale_date") as "store_running_store_daily_sale_date",
      coalesce("abhorrent"."web_running_web_daily_sale_date","abundant"."store_running_store_daily_sale_date") as "web_running_web_daily_sale_date"
  FROM
      "abhorrent"
      FULL JOIN "abundant" on "abhorrent"."web_running_web_daily_item_sk" = "abundant"."store_running_store_daily_item_sk" AND "abhorrent"."web_running_web_daily_sale_date" is not distinct from "abundant"."store_running_store_daily_sale_date"),
  late as (
  SELECT
      "sweltering"."store_running_store_daily_sale_date" as "store_running_store_daily_sale_date",
      "sweltering"."store_running_store_run_max_of_run_total" as "store_running_store_run_max_of_run_total",
      "sweltering"."store_running_store_running_total" as "store_running_total",
      "sweltering"."web_running_web_daily_sale_date" as "web_running_web_daily_sale_date",
      "sweltering"."web_running_web_run_max_of_run_total" as "web_running_web_run_max_of_run_total",
      "sweltering"."web_running_web_running_total" as "web_running_total",
      coalesce("sweltering"."web_running_web_daily_item_sk","sweltering"."store_running_store_daily_item_sk") as "_virt_func_coalesce_9902205569999979",
      coalesce("sweltering"."web_running_web_daily_item_sk","sweltering"."store_running_store_daily_item_sk") as "item_sk",
      coalesce("sweltering"."web_running_web_daily_sale_date","sweltering"."store_running_store_daily_sale_date") as "sale_date"
  FROM
      "sweltering"),
  macho as (
  SELECT
      "late"."item_sk" as "item_sk",
      "late"."sale_date" as "sale_date",
      "late"."store_running_total" as "store_running_total",
      "late"."web_running_total" as "web_running_total",
      max("late"."store_running_store_run_max_of_run_total") over (partition by "late"."_virt_func_coalesce_9902205569999979" order by coalesce("late"."web_running_web_daily_sale_date","late"."store_running_store_daily_sale_date") asc ) as "store_running_max",
      max("late"."web_running_web_run_max_of_run_total") over (partition by "late"."_virt_func_coalesce_9902205569999979" order by coalesce("late"."web_running_web_daily_sale_date","late"."store_running_store_daily_sale_date") asc ) as "web_running_max"
  FROM
      "late")
  SELECT
      "macho"."item_sk" as "item_sk",
      "macho"."sale_date" as "sale_date",
      "macho"."web_running_total" as "web_running_total",
      "macho"."store_running_total" as "store_running_total",
      "macho"."web_running_max" as "web_running_max",
      "macho"."store_running_max" as "store_running_max"
  FROM
      "macho"
  WHERE
      "macho"."web_running_max" > "macho"."store_running_max"

  ORDER BY
      "macho"."item_sk" asc nulls first,
      "macho"."sale_date" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read trilogy.toml`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3553309440.preql`

  ```text
  Import error in answer_3553309440.preql: Unable to import '.\catalog_sales.preql': [Errno 2] No such file or directory: '.\\catalog_sales.preql'. Did you mean: raw.catalog_sales?
  ```
- `trilogy file read answer_3247656127.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_2986518257.preql`

  ```text
  Import error in answer_2986518257.preql: Unable to import '.\catalog_sales.preql': [Errno 2] No such file or directory: '.\\catalog_sales.preql'. Did you mean: raw.catalog_sales?
  ```
- `trilogy run answer_2986518257.preql`

  ```text
  trilogy error: subprocess timed out after 600s.
  ```
- `trilogy file read answer_2986518257.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_145690531.preql`

  ```text
  Syntax error in answer_145690531.preql: Undefined concept: channel (line 10, col 25, in ORDER BY). Suggestions: ['sales.channel', 'channel_total']
  ```
- `trilogy run answer_145690531.preql`

  ```text
  Syntax error in answer_145690531.preql: Output column 'store_total' renames 'local.store_total' back to the name of an existing concept 'store_total' (defined at line 5) that 'local.store_total' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'store_total_out').
  ```
- `trilogy run answer_145690531.preql`

  ```text
  Syntax error in answer_145690531.preql: Impossible comparison in ref:sales.channel = MISSING: 'MISSING' can never match a declared value of enum<'WEB', 'CATALOG', 'STORE'> — fix the constant, or update the enum declaration if the domain is stale
  ```
- `trilogy run answer_1623435181.preql`

  ```text
  Syntax error in answer_1623435181.preql: Undefined concept: w1.store_key. Suggestions: ['weekly_2001.store_key', 'weekly_2002.store_key', 'ss.store.sk']
  ```
- `trilogy run --import raw/store_sales:ss select ss.store.name, ss.sale_date.week_seq as ws, ss.sale_date.day_name as dn, sum(ss.sales_price) as tot where ss.sale_date.year = 2001 and ss.store.name = 'able' and ss.sale_date.week_seq = 5279 order by ss.sale_date.day_of_week;`

  ```text
  Syntax error in stdin: ORDER BY references 'ss.sale_date.day_of_week', which is not in the SELECT projection (line 2). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --ss.sale_date.day_of_week order by ss.sale_date.day_of_week asc`.
  ```
- `trilogy file read answer_1623435181.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_4037365554.preql`

  ```text
  Syntax error in answer_4037365554.preql: Undefined concept: sale_date.year.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 37 column 12 (char 1284). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run answer_3036656719.preql`

  ```text
  Syntax error in answer_3036656719.preql: All case expressions must have the same output datatype, got {<DataType.STRING: 'string'>, <DataType.INTEGER: 'int'>} from {'ref:ss.item.category': EnumType(type=<DataType.STRING: 'string'>, values=['Books', 'Children', 'Electronics', 'Home', 'Jewelry', 'Men', 'Music', 'Shoes', 'Sports', 'Women']), 'cast(1,INTEGER)': <DataType.INTEGER: 'int'>}
  ```
- `trilogy run --import raw/store_sales:store_sales select store.state, sum(store_sales.net_profit) as total_profit where sale_date.year = 2000 and store.sk is not null order by total_profit desc limit 10;`

  ```text
  Syntax error in stdin: 3 undefined concept references; fix all before re-running:
    - store.state (line 2, col 8, in SELECT); did you mean: store_sales.store.state, store.sk, store_sales.pos_address.state, store_sales.return_store.state, store_sales.return_address.state, store_sales.customer.current_address.state?
    - sale_date.year (line 2, col 71, in WHERE); did you mean: store_sales.sale_date.year, store_sales.return_date.year, store_sales.customer.first_sales_date.year, store_sales.customer.first_shipto_date.year, store_sales.return_customer.first_sales_date.year, store_sales.return_customer.first_shipto_date.year?
    - store.sk (line 2, col 97, in WHERE); did you mean: store_sales.store.sk, store.state, store_sales.item.sk, store_sales.customer.sk, store_sales.sale_date.sk, store_sales.sale_time.sk?
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_1226264875.preql`

  ```text
  Syntax error in answer_1226264875.preql: Nothing was executed: parsed 6 definition statement(s) (3 imports, 3 rowsets) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy file read answer_2910545909.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_426809370.preql`

  ```text
  Syntax error in answer_426809370.preql: Undefined concept: cs.return_customer. Suggestions: ['cs.return_customer.sk', 'cs.return_customer.id', 'cs.return_customer.last_name', 'cs.return_customer.first_name', 'cs.return_customer.preferred_cust_flag', 'cs.return_customer.birth_day']
  ```
- `trilogy run answer_2869182220.preql`

  ```text
  Resolution error in answer_2869182220.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 7). The requested concepts split into 2 disconnected subgraphs: {c.current_address.city, c.current_demographics.sk, c.current_household_demographics.income_band.lower_bound, c.current_household_demographics.income_band.upper_bound, customer_code, full_name}; {ss.is_returned, ss.return_customer_demographic.sk, ss.return_customer_demographic.sk}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read answer_2852230229.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_840315271.preql`

  ```text
  Syntax error in answer_840315271.preql: ORDER BY references 'local._virt_agg_sum_7588202838602336', which is not in the SELECT projection (line 4). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --local._virt_agg_sum_7588202838602336 order by local._virt_agg_sum_7588202838602336 asc`.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3562094594.preql`

  ```text
  Resolution error in answer_3562094594.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {_only_catalog_count_cust_sk}; {_only_catalog_count_item_sk}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_3562094594.preql`

  ```text
  Resolution error in answer_3562094594.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {_only_catalog_c}; {_only_catalog_i}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_3562094594.preql`

  ```text
  Resolution error in answer_3562094594.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {_ca_only_c}; {_ca_only_i}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_3562094594.preql`

  ```text
  Resolution error in answer_3562094594.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {_ca_only_c}; {_ca_only_i}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read answer_3498056762.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy run --import raw/all_sales:all_sales select all_sales.channel, all_sales.channel_dim_id, all_sales.channel_dim_text_id, count(all_sales.order_id) as …23'::date and '2000-09-06'::date and all_sales.channel_dim_id is not null and all_sales.channel in ('STORE','WEB','CATALOG') group by 1,2,3 limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
   in ('STORE','WEB','CATALOG') ??? group by 1,2,3 limit 10;
  ```
- `trilogy file write answer_3705756794.preql --content import raw.all_sales as all_sales;

# Channel label lookup
auto channel_label <- 
    case when all_sale…by rollup (combined.channel_label, combined.entity_id)
order by
    combined.channel_label nulls first,
    combined.entity_id nulls first
limit 100;`

  ```text
  refused to write 'answer_3705756794.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...turns_total, net_profit_total) ???  select     combined.channel_...
  ```
- `trilogy run --import raw/all_sales:all_sales select distinct all_sales.channel where all_sales.sale_date.date between '2000-08-23'::date and '2000-09-06'::date and all_sales.channel_dim_id is not null;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...ll_sales as all_sales; select ??? distinct all_sales.channel whe...
  ```
- `trilogy file write answer_3697706765.preql -e -c parameter zips string;

import raw.store_sales as ss;

# Step 2: find 5-digit ZIPs where >10 preferred custo…tore.zip in param_zips.zip
  and ss.store.zip in qualifying_zips.zip5
  and substring(ss.store.zip, 1, 2) in (substring(qualifying_zips.zip5, 1, 2))
`

  ```text
  refused to write 'answer_3697706765.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
   as select     trim(s) as zip ??? from     split(zips, ',') ;  #...
  ```
- `trilogy file write answer_2524943990.preql --content import raw.catalog_sales as sales;

# Population 1: orders whose line items ship from more than one ware…unt,
    sum(sales.ext_ship_cost) as total_extended_ship_cost,
    sum(sales.net_profit) as total_net_profit
order by
    order_count desc
limit 100;`

  ```text
  refused to write 'answer_2524943990.preql': not syntactically valid Trilogy.

  Parse error:
    --> 10:1
     |
  10 | select
     | ^---
     |
     = expected limit, order_by, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...aving     warehouse_count > 1 ??? select     multi_warehouse_ord...
  ```
- `trilogy file write answer_2524943990.preql --content import raw.catalog_sales as sales;

# Population 1: orders whose line items ship from more than one ware…unt,
    sum(sales.ext_ship_cost) as total_extended_ship_cost,
    sum(sales.net_profit) as total_net_profit
order by
    order_count desc
limit 100;`

  ```text
  refused to write 'answer_2524943990.preql': not syntactically valid Trilogy.

  Parse error:
    --> 10:1
     |
  10 | select
     | ^---
     |
     = expected limit, order_by, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...aving     warehouse_count > 1 ??? select     multi_warehouse_ord...
  ```
- `trilogy file write answer_765177085.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;

# Store sales in 2001 returned in 2001/200…as cat_qty_cv
order by
    ss_base.item_id asc nulls first,
    ss_base.item_desc asc nulls first,
    ss_base.store_state asc nulls first
limit 100;`

  ```text
  refused to write 'answer_765177085.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [226]: Misplaced `subset|union join`. The key looks fine - the join is in the wrong PLACE. A query-scoped join is part of a `select` statement, not a standalone statement and not a pre-`where` clause. Put it right after the select list (preferred, SQL-like): `where <filters> select <cols> subset join a.key = b.key`. The clause order is `where` -> `select` <cols> -> join(s) -> `having` -> `order by` -> `limit`; a join may also sit between `where` and `select`, but never before `where` and never on its own. Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...ase.cust_sk = cs_base.cust_sk ??? union join ss_base.item_sk = c...
  ```
- `trilogy file write answer_765177085.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;

# Store sales in 2001 returned in 2001/200…_qty_cv
order by
    store_agg.item_id asc nulls first,
    store_agg.item_desc asc nulls first,
    store_agg.store_state asc nulls first
limit 100;`

  ```text
  refused to write 'answer_765177085.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...agg.item_id = cat_agg.item_sk ??? union join store_agg.item_desc...
  ```
- `trilogy file write answer_765177085.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;

# Matching store sales
with ss_match as
wh…_qty_cv
order by
    store_agg.item_id asc nulls first,
    store_agg.item_desc asc nulls first,
    store_agg.store_state asc nulls first
limit 100;`

  ```text
  refused to write 'answer_765177085.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...m_id = matching_items.item_sk ??? union join cat_agg.item_sk = m...
  ```
- `trilogy file write answer_765177085.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;

# Store sales matching criteria (at the st…_qty_cv
order by
    store_agg.item_id asc nulls first,
    store_agg.item_desc asc nulls first,
    store_agg.store_state asc nulls first
limit 100;`

  ```text
  refused to write 'answer_765177085.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...gg and cat_agg via union join ??? union join store_agg.item_sk =...
  ```
- `trilogy file write answer_2928586490.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Step 1: Fre…y combined.last_name, combined.first_name
having total_sales > 0
order by combined.last_name, combined.first_name, total_sales nulls first
limit 100;`

  ```text
  refused to write 'answer_2928586490.preql': not syntactically valid Trilogy.

  Parse error:
    --> 12:1
     |
  12 | by ss.item.sk, substring(ss.item.desc, 1, 30) as desc_prefix, ss.sale_date.date as sold_date
     | ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...icket_number)) as order_count ??? by ss.item.sk, substring(ss.it...
  ```
- `trilogy file write answer_2928586490.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Step 1: Fre…ng sum(combined_customers.channel_total) > 0
order by combined_customers.last_name, combined_customers.first_name, total_sales nulls first
limit 100;`

  ```text
  refused to write 'answer_2928586490.preql': not syntactically valid Trilogy.

  Parse error:
    --> 13:1
     |
  13 | select distinct
     | ^---
     |
     = expected limit, order_by, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ..._count having order_count > 4 ??? select distinct     item_sk ;
  ```
- `trilogy file write answer_2928586490.preql -e -c import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Step 1: Frequen…ng sum(combined_customers.channel_total) > 0
order by combined_customers.last_name, combined_customers.first_name, total_sales nulls first
limit 100;`

  ```text
  refused to write 'answer_2928586490.preql': not syntactically valid Trilogy.

  Parse error:
    --> 13:1
     |
  13 | select
     | ^---
     |
     = expected limit, order_by, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ..._count having order_count > 4 ??? select     item_sk ;  # Step 2...
  ```
- `trilogy file write answer_2928586490.preql -e -c import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Step 1: Frequen…al_sales
where combined.total > 0
having sum(combined.total) > 0
order by combined.last_name, combined.first_name, total_sales nulls first
limit 100;`

  ```text
  refused to write 'answer_2928586490.preql': not syntactically valid Trilogy.

  Parse error:
    --> 70:6
     |
  70 |     (feb_cat),
     |      ^---
     |
     = expected select_statement
  Location:
   with combined as union(     ( ??? feb_cat),     (feb_web) ) -> (...
  ```
- `trilogy file write answer_2928586490.preql -e -c import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Step 1: Frequen…al_sales
where combined.total > 0
having sum(combined.total) > 0
order by combined.last_name, combined.first_name, total_sales nulls first
limit 100;`

  ```text
  refused to write 'answer_2928586490.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...st_name string, total numeric) ??? select     combined.last_name...
  ```
- `trilogy run --import raw.store_sales:ss select count(*) as cnt where ss.customer.sk is not null and sum(ss.quantity * ss.sales_price) > 118133.255 limit 10;`

  ```text
  Syntax error in stdin: Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a NON-NULL GRAIN KEY: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). It MUST be a key, and one that is not nullable: `count(x)` skips rows where `x` is NULL, so counting a nullable property (a name, a date, any optional field) silently undercounts. When the grain takes SEVERAL keys, name them with `grain(...)`: `count(grain(order_id, item.id))` counts order+item combinations, and `count_distinct(grain(first_name, last_name, sale_date))` counts distinct combinations - `grain()` is never NULL, so combinations with a missing member still count. For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...raw.store_sales as ss; select ??? count(*) as cnt where ss.custo...
  ```
- `trilogy file write answer_1798498862.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;

# Store sales in Sep 1999, returned by sa…tity) as store_return_qty,
    sum(cs.quantity) as catalog_sale_qty
order by
    item_code,
    item_desc,
    store_code,
    store_name
limit 100;
`

  ```text
  refused to write 'answer_1798498862.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [211]: Expression in `by` clause must be wrapped in parens - write `by (expr1, expr2, ...)`. Bare identifiers (`by a, b`) work without parens, but any function call, cast, or other expression needs them.
  Location:
  ...e sales in Sep 1999, returned ??? by same customer between Sep-D...
  ```
- `trilogy file write answer_1798498862.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;

where ss.sale_date.year = 1999
  and ss.s…ntity) as store_return_qty,
    sum(cs.quantity) as catalog_sale_qty
order by
    item_code,
    item_desc,
    store_code,
    store_name
limit 100;`

  ```text
  refused to write 'answer_1798498862.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [226]: Misplaced `subset|union join`. The key looks fine - the join is in the wrong PLACE. A query-scoped join is part of a `select` statement, not a standalone statement and not a pre-`where` clause. Put it right after the select list (preferred, SQL-like): `where <filters> select <cols> subset join a.key = b.key`. The clause order is `where` -> `select` <cols> -> join(s) -> `having` -> `order by` -> `limit`; a join may also sit between `where` and `select`, but never before `where` and never on its own. Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...r.sk = cs.billing_customer.sk ??? subset join ss.item.sk = cs.it...
  ```
- `trilogy file write answer_4207382245.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;

# Store sales extended price by county and qu…is not null
    # Web ratios exceed store ratios
    and web_q2_q1 > store_q2_q1
    and web_q3_q2 > store_q3_q2
order by
    county asc nulls first;`

  ```text
  refused to write 'answer_4207382245.preql': not syntactically valid Trilogy.

  Parse error:
   --> 5:1
    |
  5 | rowset store_by_q as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...by county and quarter in 2000 ??? rowset store_by_q as where ss....
  ```
- `trilogy file write answer_4207382245.preql --escapes -c true --content import raw.store_sales as ss;
import raw.web_sales as ws;

# Store sales extended pric…is not null
    # Web ratios exceed store ratios
    and web_q2_q1 > store_q2_q1
    and web_q3_q2 > store_q3_q2
order by
    county asc nulls first;`

  ```text
  refused to write 'answer_4207382245.preql': not syntactically valid Trilogy.

  Parse error:
    --> 27:68
     |
  27 |     (sum(web_by_q.web_ext_price ? web_by_q.quarter = 2) by coalesce(store_by_q.county, web_by_q.county))
     |                                                                    ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, window_sql_over, or OVER_COMPONENT_REF
  Location:
  ..._by_q.quarter = 2) by coalesce ??? (store_by_q.county, web_by_q.c...
  ```
- `trilogy file write answer_3530408544.preql --content import raw.store_sales as ss;

# Filter on years 1999, 2000, 2001
# day_of_month between 1-3 or 25-28
# … by
    ss.customer.last_name,
    ss.customer.first_name,
    ss.customer.salutation,
    ss.customer.preferred_cust_flag desc,
    ss.ticket_number`

  ```text
  refused to write 'answer_3530408544.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [202]: Missing closing semicolon? Statements must be terminated with a semicolon `;`.
  Location:
  ...lag desc,     ss.ticket_number ???
  ```
- `trilogy file write answer_4140546834.preql -e -c import raw.store_sales as ss;

# Filter to store sales in year 2001 at stores in TN
where ss.sale_date.year …l or g_cat = 1
order by
    hl desc nulls first,
    case when hl = 0 then ss.item.category end nulls first,
    subgroup_rank nulls first
limit 100;`

  ```text
  refused to write 'answer_4140546834.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block - they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...cepts for the rollup analysis ??? auto total_profit <- sum(ss.ne...
  ```
- `trilogy run --import raw.store_sales:ss select ss.sale_date.year, ss.sale_date.month_of_year, sum(ss.sales_price) as monthly_total by ss.item.category, ss.item.brand_name, ss.store.name, ss.store.company_name, ss.sale_date.year, ss.sale_date.month_of_year, ss.sale_date.month_seq limit 10;`

  ```text
  Syntax error in stdin:  --> 2:92
    |
  2 | select ss.sale_date.year, ss.sale_date.month_of_year, sum(ss.sales_price) as monthly_total by ss.item.category, ss.item.brand_name, ss.store.name, ss.store.company_name, ss.sale_date.year, ss.sale_date.month_of_year, ss.sale_date.month_seq limit 10;
    |                                                                                            ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...sales_price) as monthly_total ??? by ss.item.category, ss.item.b...
  ```
- `trilogy file write answer_3553309440.preql --content import raw/catalog_sales as cs;
import raw/web_sales as ws;
import raw/store_sales as ss;

# Qualifying …re_price / 50) * 50 as segment_times_50
order by
    segment asc nulls first,
    customer_count asc nulls first,
    segment_times_50 asc
limit 100;`

  ```text
  refused to write 'answer_3553309440.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/catalog_sales as cs;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/catalog_sales as cs; impor...
  ```
- `trilogy run --import raw/date:date select date.year, min(date.week_seq) as min_ws, max(date.week_seq) as max_ws where date.year in (2001,2002) group by date.year order by date.year;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...here date.year in (2001,2002) ??? group by date.year order by da...
  ```
- `trilogy file write answer_1623435181.preql -e -c import raw/store_sales as ss;

# Per-store, per-week, per-day-of-week sum of unit price
# We need to split b…d as sat_ratio
order by
    pivoted_2001.store_name nulls first,
    pivoted_2001.store_code nulls first,
    pivoted_2001.ws nulls first
limit 100;
`

  ```text
  refused to write 'answer_1623435181.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/store_sales as ss;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/store_sales as ss;  # Per-...
  ```
- `trilogy run --import raw/date:d select d.year, d.week_seq, --d.sk where d.week_seq in (5279, 5331) group by d.year, d.week_seq order by d.week_seq;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...re d.week_seq in (5279, 5331) ??? group by d.year, d.week_seq or...
  ```
- `trilogy file write answer_3544057080.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;

# ---- Step 1-2: Per-item catalog cumulat…g_sales.sale_lines as count1,
  agg_sales.wholesale_sum as wholesale1,
  agg_sales.list_price_sum as list_price1,
  agg_sales.coupon_sum as coupon1
;`

  ```text
  refused to write 'answer_3544057080.preql': not syntactically valid Trilogy.

  Parse error:
    --> 51:29
     |
  51 |                             select agg_sales2.item_sk)
     |                             ^---
     |
     = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...p                             ??? select agg_sales2.item_sk) sel...
  ```
- `trilogy file write answer_3544057080.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;

# ---- Step 1-2: Per-item catalog cumulat…yr2000.cnt as cnt2000
order by
  yr1999.product_name asc,
  yr1999.store_name asc,
  yr2000.cnt asc,
  yr1999.wholesale asc,
  yr2000.wholesale asc
;`

  ```text
  refused to write 'answer_3544057080.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [226]: Misplaced `subset|union join`. The key looks fine - the join is in the wrong PLACE. A query-scoped join is part of a `select` statement, not a standalone statement and not a pre-`where` clause. Put it right after the select list (preferred, SQL-like): `where <filters> select <cols> subset join a.key = b.key`. The clause order is `where` -> `select` <cols> -> join(s) -> `having` -> `order by` -> `limit`; a join may also sit between `where` and `select`, but never before `where` and never on its own. Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...tore_name = yr2000.store_name ??? union join yr1999.store_zip =
  ```
- `trilogy file write answer_3063407983.preql --content import raw.store_sales as ss;

where 
    ss.sale_date.year in (1999, 2000, 2001)
    and ss.sale_date.d…list_price,
    sum(ss.ext_tax) as total_ext_tax

order by
    ss.customer.last_name asc nulls first,
    ss.ticket_number asc nulls first
limit 100
`

  ```text
  refused to write 'answer_3063407983.preql': not syntactically valid Trilogy.

  Parse error:
    --> 23:1
     |
  23 | order by
     | ^---
     |
     = expected metadata, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...ss.ext_tax) as total_ext_tax  ??? order by     ss.customer.last_...
  ```
- `trilogy run --import raw/store_sales:store_sales select store.state, store.county, sum(store_sales.net_profit) as total_profit where sale_date.year = 2000 and store.sk is not null by store.state order by total_profit desc limit 10;`

  ```text
  Syntax error in stdin: Syntax [213]: A `by <grain>` clause must follow an aggregate, but the expression before it has none. If the `by` sits inside an aggregate's parentheses (`max(x by *)`), move it outside the call: `max(x) by *`. To take each distinct value once per grain, wrap it in `group(...)` - e.g. `group(item.current_price) by item.id, item.category`. For a reduction, use an aggregate: `sum(x) by ...`, `avg(x) by ...`, `max(x) by ...`.
  Location:
  ...2000 and store.sk is not null ??? by store.state order by total_...
  ```
- `trilogy file write answer_3840922834.preql --content import raw.store_sales as store;
import raw.web_sales as web;

# Rowset: store sales net paid by custome…ode,
    store_totals.store_net_paid as st_2001,
    web_totals.web_net_paid as wb_2001
union join store_totals.customer_id = web_totals.customer_id
`

  ```text
  refused to write 'answer_3840922834.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...otals.web_net_paid as wb_2001 ??? union join store_totals.custom...
  ```
- `trilogy file write answer_3046445280.preql --content import raw.all_sales as sales;

# Filter to Books category only
where sales.item.category = 'Books'

# C…
    sales.item.brand_id,
    sales.item.class_id,
    sales.item.category_id,
    sales.item.manufacturer_id,
    total_net_qty,
    total_net_amt
;`

  ```text
  refused to write 'answer_3046445280.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block - they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...s (treating null return as 0) ??? auto net_qty_line <- coalesce(...
  ```
- `trilogy file write answer_747155668.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sales as cs;

with combined …  combined.missing_ref nulls first,
         combined.yr nulls first,
         combined.qtr nulls first,
         combined.cat nulls first
limit 100;`

  ```text
  refused to write 'answer_747155668.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ..., line_count, total_ext_price) ???  select     combined.channel,...
  ```
- `trilogy file write answer_2869182220.preql --content import raw.customer as c;
import raw.store_sales as ss;

# Step 1: Find the customer demographic keys fr…--ss.ticket_number as hidden_ticket,
    --ss.item.sk as hidden_item_sk
    ss.ticket_number,
    ss.item.sk
order by c.id asc nulls first
limit 100;`

  ```text
  refused to write 'answer_2869182220.preql': not syntactically valid Trilogy.

  Parse error:
    --> 21:5
     |
  21 |     ss.ticket_number,
     |     ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...item.sk as hidden_item_sk     ??? ss.ticket_number,     ss.item....
  ```
- `trilogy file write answer_2869182220.preql --content import raw.customer as c;
import raw.store_sales as ss;

# Find store returns, match by demographic key …icket,
    ss.item.sk as h_item_sk
subset join c.current_demographics.sk = ss.return_customer_demographic.sk
order by c.id asc nulls first
limit 100;`

  ```text
  refused to write 'answer_2869182220.preql': not syntactically valid Trilogy.

  Parse error:
    --> 18:5
     |
  18 |     ss.ticket_number as h_ticket,
     |     ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...--ss.item.sk as h_item_sk     ??? ss.ticket_number as h_ticket,
  ```
- `trilogy run --import raw.web_sales:ws select sum(ws.net_paid) as total, ws.item.category, ws.item.class, grouping(ws.item.category) + grouping(ws.item.class)…item.class) where ws.sale_date.year = 2000 order by level desc nulls first, ws.item.category asc nulls first, ws.item.class asc nulls first limit 20;`

  ```text
  Syntax error in stdin:  --> 2:174
    |
  2 | select sum(ws.net_paid) as total, ws.item.category, ws.item.class, grouping(ws.item.category) + grouping(ws.item.class) as level by rollup (ws.item.category, ws.item.class) where ws.sale_date.year = 2000 order by level desc nulls first, ws.item.category asc nulls first, ws.item.class asc nulls first limit 20;
    |                                                                                                                                                                              ^---
    |
    = expected limit, order_by, or having
  Location:
  ...item.category, ws.item.class) ??? where ws.sale_date.year = 2000...
  ```
- `trilogy file write answer_210925266.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as…ring?, first_name string?, sale_date date);

select count(grain(only_store.last_name, only_store.first_name, only_store.sale_date)) as unique_combos;`

  ```text
  refused to write 'answer_210925266.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...xcept(     (select ln, fn, sd ??? from store_combos),     (selec...
  ```
- `trilogy file write answer_840315271.preql --content import raw.store_sales as ss;

with monthly_totals as
where ss.sale_date.year = 1999
  and (
    (ss.item…ory,
  monthly_totals.class,
  monthly_totals.brand,
  monthly_totals.company_name,
  monthly_totals.month_of_year,
  monthly_totals.total
limit 100;`

  ```text
  refused to write 'answer_840315271.preql': not syntactically valid Trilogy.

  Parse error:
    --> 21:4
     |
  21 |   -- compute average across months for same (category, brand, store_name, company_name)
     |    ^---
     |
     = expected access_chain
  Location:
   monthly_totals.total != 0   - ??? - compute average across month...
  ```
- `trilogy run --import raw.store_sales:ss select ss.customer.sk, sum(ss.quantity * ss.sales_price) as adj_sum where ss.return_reason.desc = 'reason 28' group by ss.customer.sk order by adj_sum asc nulls first, ss.customer.sk asc nulls first limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...urn_reason.desc = 'reason 28' ??? group by ss.customer.sk order
  ```
- `trilogy file write answer_3562094594.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;

# Store sales: unique (customer.sk, item.…nly_store.cust_sk) as only_store_count,
    count(only_catalog.cust_sk) as only_catalog_count,
    count(both_sets.cust_sk) as both_count
limit 100;
`

  ```text
  refused to write 'answer_3562094594.preql': not syntactically valid Trilogy.

  Parse error:
    --> 21:28
     |
  21 | with only_store as except((store_combos), (catalog_combos)) -> (cust_sk, item_sk);
     |                            ^---
     |
     = expected select_statement
  Location:
  ...ns with only_store as except(( ??? store_combos), (catalog_combos...
  ```

### `cli-misuse`

- `trilogy explore raw/ --show all`

  ```text
  Invalid value for 'PATH': File 'raw/' is a directory.
  ```
- `trilogy run --import raw.catalog_sales:catalog_sales select catalog_sales.order_number, catalog_sales.ship_date.date, catalog_sales.ship_date.year limit 10;`

  ```text
  'catalog_sales.order_number, catalog_sales.ship_date.date, catalog_sales.ship_date.year limit 10;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy explore assets/root/root.preql`

  ```text
  Invalid value for 'PATH': File 'assets/root/root.preql' does not exist.
  ```
- `trilogy run --import raw/catalog_sales:cs --import raw/web_sales:ws --import raw/store_sales:ss auto cat_qual_cust <- cs.billing_customer.sk ? (cs.sale_date.…ry = 'Women' and ws.item.class = 'maternity'); select count(ss.customer.sk) where ss.customer.sk in cat_qual_cust or ss.customer.sk in web_qual_cust;`

  ```text
  'auto web_qual_cust <- ws.billing_customer.sk ? (ws.sale_date.month_seq = 1187 and ws.item.category = 'Women' and ws.item.class = 'maternity');' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy run --import raw.catalog_sales:cs --import raw.web_sales:ws --import raw.store_sales:ss auto cat_qual_cust <- cs.billing_customer.sk ? (cs.sale_date.month_seq = 1187 and cs.item.category = 'Women' and cs.item.class = 'maternity'); select count(cat_qual_cust) ;`

  ```text
  'select count(cat_qual_cust) ;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy run --import raw.catalog_sales:cs --import raw.web_sales:ws --import raw.store_sales:ss auto cat_qual_cust <- cs.billing_customer.sk ? (cs.sale_date.…t(ss.customer.sk) where ss.customer.sk in cat_qual_cust or ss.customer.sk in web_qual_cust and ss.sale_date.month_seq between 1188 and 1190 limit 10;`

  ```text
  'auto web_qual_cust <- ws.billing_customer.sk ? (ws.sale_date.month_seq = 1187 and ws.item.category = 'Women' and ws.item.class = 'maternity');' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy file write -e -c import raw.store_sales as store;\nimport raw.catalog_sales as catalog;\nimport raw.web_sales as web;\n\n# Store never-returned sales…g_customer.sk as customer_sk,\n    sum(web.quantity) as web_qty,\n    sum(web.wholesale_cost) as web_wc_sum,\n    sum(web.sales_price) as web_sp_sum;`

  ```text
  Missing argument 'PATH'.
  ```
- `trilogy explore raw/catalog_returns.preql`

  ```text
  Invalid value for 'PATH': File 'raw/catalog_returns.preql' does not exist.
  ```
- `trilogy explore raw/web_returns.preql`

  ```text
  Invalid value for 'PATH': File 'raw/web_returns.preql' does not exist.
  ```

### `syntax-missing-alias`

- `trilogy file write answer_3979964698.preql --content import raw.all_sales as all_sales;

# Filtered aggregates for WEB channel: monthly sales = quantity * ex…(12) + @monthly_net_cat(12) as net_dec
order by
    all_sales.warehouse.name asc nulls first,
    all_sales.sale_date.year asc nulls first
limit 100;`

  ```text
  refused to write 'answer_3979964698.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `not displayed
      all_sales.warehouse.name as not_displayed_all_sales_warehouse_name`
  Location:
   is hidden, not displayed     ??? all_sales.warehouse.name as wa...
  ```
- `trilogy file write answer_2869182220.preql --content import raw.customer as c;
import raw.store_sales as ss;

# Step 1: Find the customer demographic keys fr…, ', c.first_name) as full_name,
    -- hidden fields to maintain grain
    ss.ticket_number,
    ss.item.sk
order by c.id asc nulls first
limit 100;`

  ```text
  refused to write 'answer_2869182220.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `hidden fields to maintain grain
      ss.ticket_number as hidden_fields_to_maintain_grain_ss_ticke`
  Location:
  ...) as full_name,     -- hidden ??? fields to maintain grain     s...
  ```

### `join-resolution`

- `trilogy run answer_4140546834.preql`

  ```text
  [v4] group-graph lineage cycle, skipping concept-set pass: [('grp:basic:d*:ss.item.sk|ss.ticket_number:sig:41aa9940280e', 'grp:window:d0:ss.item.category|ss.item.class'), ('grp:window:d0:ss.item.category|ss.item.class', 'grp:basic:d*:ss.item.sk|ss.ticket_number:sig:41aa9940280e')]
  [v4] group-graph lineage cycle, skipping concept-set pass: [('grp:basic:d*:ss.item.sk|ss.ticket_number:sig:41aa9940280e', 'grp:window:d0:ss.item.category|ss.item.class'), ('grp:window:d0:ss.item.category|ss.item.class', 'grp:basic:d*:ss.item.sk|ss.ticket_number:sig:41aa9940280e')]
  [v4] group-graph cycle, abandoning strategy build: [('grp:basic:d*:ss.item.sk|ss.ticket_number:sig:41aa9940280e', 'grp:window:d0:ss.item.category|ss.item.class'), ('grp:window:d0:ss.item.category|ss.item.class', 'grp:basic:d*:ss.item.sk|ss.ticket_number:sig:41aa9940280e')]
  ```

### `planner-recursion`

- `trilogy run answer_4140546834.preql`

  ```text
  Resolution error in answer_4140546834.preql: query could not be planned; this is a bug.
  ```
