# Trilogy failure analysis — 20260713-213446

- Run `enriched_scope_v2_full_20260713-173444` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 1252 | failed: 126 (10%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 72 | 57% |
| `syntax-parse` | 42 | 33% |
| `cli-misuse` | 5 | 4% |
| `planner-recursion` | 4 | 3% |
| `join-resolution` | 1 | 1% |
| `syntax-missing-alias` | 1 | 1% |
| `type-error` | 1 | 1% |

## Detail

### `other`

- `trilogy run answer_3863442186.preql`

  ```text
  Resolution error in answer_3863442186.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 84). The requested concepts split into 6 disconnected subgraphs: {catalog_pivot.cid, catalog_pivot.v2001}; {catalog_pivot2.cid, catalog_pivot2.v2002}; {store_pivot.cid, store_pivot.v2001}; {store_pivot2.cid, store_pivot2.v2002}; {web_pivot.cid, web_pivot.v2001}; {web_pivot2.cid, web_pivot2.v2002}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read answer_3863442186.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3705756794.preql`

  ```text
  Syntax error in answer_3705756794.preql: ORDER BY references 'sales_agg.all_sales.channel', a measure at a finer grain (Grain<returns_agg.all_sales.channel>) than the select grain (Grain<local.channel_type,local.entity>); it has no single value per output row. Project it (prefix with `--` to keep it out of the rows) and order by that alias instead.
  ```
- `trilogy run answer_3705756794.preql`

  ```text
  Syntax error in answer_3705756794.preql: ORDER BY references 'sales_agg.all_sales.channel', a measure at a finer grain (Grain<returns_agg.all_sales.channel>) than the select grain (Grain<local.channel_type,local.entity>); it has no single value per output row. Project it (prefix with `--` to keep it out of the rows) and order by that alias instead.
  ```
- `trilogy run answer_3705756794.preql`

  ```text
  Syntax error in answer_3705756794.preql: ORDER BY references 'combined.chan', a measure at a finer grain (Grain<combined.chan>) than the select grain (Grain<local.channel_type,local.entity>); it has no single value per output row. Project it (prefix with `--` to keep it out of the rows) and order by that alias instead.
  ```
- `trilogy run answer_3705756794.preql`

  ```text
  Syntax error in answer_3705756794.preql: ORDER BY references 'combined.chan', a measure at a finer grain (Grain<combined.chan>) than the select grain (Grain<local.channel_type,local.entity>); it has no single value per output row. Project it (prefix with `--` to keep it out of the rows) and order by that alias instead.
  ```
- `trilogy run answer_883027685.preql`

  ```text
  Unexpected error in answer_883027685.preql: (_duckdb.BinderException) Binder Error: column ss_item_current_price must appear in the GROUP BY clause or be used in an aggregate function
  [SQL:
  WITH
  cooperative as (
  SELECT
      "ss_customer_address_customer_address"."CA_STATE" as "ss_customer_address_state",
      "ss_item_items"."I_CATEGORY" as "ss_item_category",
      "ss_item_items"."I_CURRENT_PRICE" as "ss_item_current_price",
      "ss_item_items"."I_ITEM_SK" as "ss_item_sk",
      "ss_store_sales"."SS_TICKET_NUMBER" as "ss_ticket_number"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
      INNER JOIN "item" as "ss_item_items" on "ss_store_sales"."SS_ITEM_SK" = "ss_item_items"."I_ITEM_SK"
      INNER JOIN "customer" as "ss_customer_customers" on "ss_store_sales"."SS_CUSTOMER_SK" = "ss_customer_customers"."C_CUSTOMER_SK"
      INNER JOIN "customer_address" as "ss_customer_address_customer_address" on "ss_customer_customers"."C_CURRENT_ADDR_SK" = "ss_customer_address_customer_address"."CA_ADDRESS_SK"
  WHERE
      "ss_date_date"."D_YEAR" = 2001 and "ss_date_date"."D_MOY" = 1 and "ss_item_items"."I_CATEGORY" is not null and "ss_customer_address_customer_address"."CA_ADDRESS_SK" is not null
  ),
  questionable as (
  SELECT
      "ss_item_items"."I_CATEGORY" as "ss_item_category",
      avg("ss_item_items"."I_CURRENT_PRICE") as "cat_avg_price"
  FROM
      "item" as "ss_item_items"
  WHERE
      "ss_item_items"."I_CATEGORY" is not null

  GROUP BY
      1)
  SELECT
      "cooperative"."ss_customer_address_state" as "ss_customer_address_state",
      count(CONCAT(cast("cooperative"."ss_ticket_number" as string), '-', cast("cooperative"."ss_item_sk" as string))) as "qualifying_line_items"
  FROM
      "cooperative"
      INNER JOIN "questionable" on "cooperative"."ss_item_category" is not distinct from "questionable"."ss_item_category"
  GROUP BY
      1
  HAVING
      ("cooperative"."ss_item_current_price" > 1.2 * "questionable"."cat_avg_price") and "qualifying_line_items" >= 10

  ORDER BY
      "qualifying_line_items" asc nulls first,
      "cooperative"."ss_customer_address_state" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run answer_3697440276.preql`

  ```text
  Syntax error in answer_3697440276.preql: 6 undefined concept references; fix all before re-running:
    - local.s01_rev (line 76, col 7, in WHERE); did you mean: s01.s01_rev, w01_rev, s02_rev?
    - local.s02_rev (line 77, col 7, in WHERE); did you mean: s02.s02_rev, w02_rev, s01_rev?
    - local.w01_rev (line 78, col 7, in WHERE); did you mean: w01.w01_rev, w02_rev, s01_rev?
    - local.w02_rev (line 79, col 7, in WHERE); did you mean: w02.w02_rev, w01_rev, s02_rev?
    - local.s01_rev (line 65, in WHERE); did you mean: s01.s01_rev, w01_rev, s02_rev?
    - local.w01_rev (line 65, in WHERE); did you mean: w01.w01_rev, w02_rev, s01_rev?
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 44 column 12 (char 2430). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 44 column 12 (char 2223). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run answer_219203908.preql`

  ```text
  Syntax error in answer_219203908.preql: `by rollup (…)` cannot re-aggregate the bare measure reference `survivors.nov_leaf.total_sales`. Alias it with an explicit aggregate (e.g. `sum(survivors.nov_leaf.total_sales) as total_sales_total`).
  ```
- `trilogy run --import raw.all_sales:all_sales select count(all_sales.order_id) as cnt where all_sales.date.year between 1999 and 2001 and all_sales.channel = …es.item.brand_id, all_sales.item.class_id, all_sales.item.category_id where all_sales.date.year between 1999 and 2001 and all_sales.channel = 'WEB');`

  ```text
  Syntax error in stdin: a `(select ...)` subquery used as a scalar value or membership set must select exactly one column; project only the key/value consumed by the outer expression (line 2, column 207)
  ```
- `trilogy database describe catalog_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_765177085.preql duckdb`

  ```text
  Resolution error in answer_765177085.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 18). The requested concepts split into 2 disconnected subgraphs: {ret_qty_avg, ret_qty_count, ret_qty_stddev, ss_qty_avg, ss_qty_count, ss_qty_stddev, ss.item.desc, ss.item.id, ss.store.state}; {cs_qty_avg, cs_qty_count, cs_qty_stddev}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_765177085.preql duckdb`

  ```text
  Syntax error in answer_765177085.preql: Undefined concept: ss_stats.ss.customer.sk. Suggestions: ['ss_stats.store_state', 'ss_stats.ss_qty', 'ss.customer.sk', 'cs.ship_customer.sk', 'ss.return_customer.sk', 'ss.customer.address.sk']
  ```
- `trilogy run answer_765177085.preql duckdb`

  ```text
  Resolution error in answer_765177085.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 18). The requested concepts split into 2 disconnected subgraphs: {cs.billing_customer.sk, cs.item.sk, cs.sold_date.year, cs_qty_avg, cs_qty_count, cs_qty_cv, cs_qty_stddev}; {item_desc, item_id, ret_qty_avg, ret_qty_count, ret_qty_cv, ret_qty_stddev, ss_qty_avg, ss_qty_count, ss_qty_cv, ss_qty_stddev, store_state, ss.customer.sk, ss.date.year, ss.return_customer.sk, ss.return_date.year}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_2604809012.preql`

  ```text
  Syntax error in answer_2604809012.preql: Undefined concept: item.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 78 column 4 (char 2844). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_374591292.preql`

  ```text
  Syntax error in answer_374591292.preql: 2 undefined concept references; fix all before re-running:
    - best_product (line 56, col 5, in ORDER BY); did you mean: best_performers.best_product?
    - worst_product (line 57, col 5, in ORDER BY); did you mean: worst_performers.worst_product?
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/item.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read answer_2940558602.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3553309440.preql`

  ```text
  Syntax error in answer_3553309440.preql: Nothing was executed: parsed 5 definition statement(s) (4 imports, 1 rowset) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy file read answer_3553309440.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.all_sales:s --import raw.item:item select s.item.id, s.ext_sales_price, s.date.year, s.date.month_of_year, s.pos_bill_address.gmt_of…e s.date.year = 2001 and s.date.month_of_year = 2 and s.pos_bill_address.gmt_offset = -5 and item.color in ('slate','blanched','burnished') limit 10;`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {item.color}; {s.date.month_of_year, s.date.year, s.ext_sales_price, s.item.id, s.pos_bill_address.gmt_offset}.
    - `item.color` is disconnected — did you mean `s.item.color`? (connected to the other concepts)
  These look like separately-imported copies of models already reachable through a connected import; chain through that path (e.g. `s.item.color`) instead of importing a second, disconnected copy.
  ```
- `trilogy run --import raw.all_sales:s select s.item.id, sum(s.ext_sales_price) as total where s.date.year = 2001 and s.date.month_of_year = 2 and s.pos_bill_a…ess.gmt_offset = -5 and s.item.id in (select s2.item.id where s2.item.color in ('slate','blanched','burnished')) order by total, s.item.id limit 100;`

  ```text
  Syntax error in stdin: 2 undefined concept references; fix all before re-running:
    - s2.item.id (line 2, col 169, in SELECT); did you mean: s2.item.color, s.item.id, s.date.id, s.time.id, s.ship_mode.id, s.ship_customer.id?
    - s2.item.color (line 2, col 186, in WHERE); did you mean: s2.item.id, s.item.color, s.item.class, s.item.category?
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 61 column 4 (char 2276). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 83 column 12 (char 2323). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run answer_1623435181.preql`

  ```text
  Syntax error in answer_1623435181.preql: Undefined concept: date.year. Suggestions: ['ss.date.year', 'ss.store.date.year', 'ss.return_store.date.year', 'ss.return_date.year', 'ss.customer.first_sales_date.year', 'ss.customer.first_shipto_date.year']
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.store_sales:store_sales select item.brand_name limit 20;`

  ```text
  Syntax error in stdin: Undefined concept: item.brand_name (line 2, col 8, in SELECT). Suggestions: ['store_sales.item.brand_name']
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 60 column 2 (char 2680). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 1 column 2198 (char 2197). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run answer_3544057080.preql`

  ```text
  Resolution error in answer_3544057080.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 24). The requested concepts split into 2 disconnected subgraphs: {catalog_qualifying_item}; {cust_city, cust_street_name, cust_street_number, cust_zip, first_sales_year, first_ship_year, has_store_return, item_color_ok, item_id, item_price_ok, marital_diff_ok, product_name, sale_city, sale_line_count, sale_street_name, sale_street_number, sale_year, sale_zip, store_name, store_zip, total_coupon_amt, total_list_price, total_wholesale_cost}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_3544057080.preql`

  ```text
  Syntax error in answer_3544057080.preql: 2 undefined concept references; fix all before re-running:
    - base_agg_x.item_id (line 57, col 16, in SELECT); did you mean: base_agg_x.sale_year, base_agg.item_id, base_agg.store_zip, base_agg.sale_zip?
    - base_agg_x.sale_year (line 58, col 15, in WHERE); did you mean: base_agg_x.item_id, base_agg.sale_year, base_agg.first_sales_year, base_agg.sale_city?
  ```
- `trilogy run answer_3544057080.preql`

  ```text
  Syntax error in answer_3544057080.preql: Join chain repeats source `yr1999` (keys `yr1999.item_id` and `yr1999.store_name`). A `=` chain joins ONE key across distinct sources; join a composite key with `and` or separate clauses (e.g. `a.k1 = b.k1 and a.k2 = b.k2`). (line 107, column 1)
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_1256642802.preql`

  ```text
  Syntax error in answer_1256642802.preql: ORDER BY references 'ss.store.name', which is not in the SELECT projection (line 21). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --ss.store.name order by ss.store.name asc`.
  ```
- `trilogy run answer_1256642802.preql`

  ```text
  Resolution error in answer_1256642802.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 21). The requested concepts split into 2 disconnected subgraphs: {ss.date.year, ss.item.brand_name, ss.item.current_price, ss.item.desc, ss.item.wholesale_cost, ss.store.id, ss.store.name}; {store_avg.store_avg_revenue, store_item_rev.item_desc, store_item_rev.item_revenue, store_item_rev.store_name}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_1256642802.preql`

  ```text
  Syntax error in answer_1256642802.preql: Conflicting join types (full, left outer) on keys joined into one group: a FULL/UNION join cannot be mixed with another type on the same key (it is ambiguous whether the key is required or one-sided). Make the whole group one type (e.g. `UNION JOIN a = b = c`), or use a distinct key. (line 21, column 1)
  ```
- `trilogy run answer_1256642802.preql`

  ```text
  Syntax error in answer_1256642802.preql: ORDER BY references 'ss.store.name', which is not in the SELECT projection (line 21). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --ss.store.name order by ss.store.name asc`.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 42 column 12 (char 1322). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3046445280.preql`

  ```text
  Syntax error in answer_3046445280.preql: 5 undefined concept references; fix all before re-running:
    - yr2002.qty_2001 (line 59, in SELECT); did you mean: yr2002.qty_2002, yr2002.amt_2001, yr2002.amt_2002, yr2001.qty_2001?
    - yr2002.amt_2001 (line 60, in SELECT); did you mean: yr2002.amt_2002, yr2002.qty_2001, yr2002.qty_2002, yr2001.amt_2001?
    - yr2002.qty_2001 (line 47, col 7, in WHERE); did you mean: yr2002.qty_2002, yr2002.amt_2001, yr2002.amt_2002, yr2001.qty_2001?
    - yr2001.qty_2002 (line 48, col 7, in WHERE); did you mean: yr2001.qty_2001, yr2001.amt_2001, yr2001.category_id, yr2002.qty_2002, yr2002.qty_2001?
    - yr2002.yearly.net_qty (line 47, in WHERE); did you mean: yearly.net_qty, yearly.net_amt?
  ```
- `trilogy run answer_3210116865.preql`

  ```text
  Unexpected error in answer_3210116865.preql: Could not render the query: Missing source reference to ws.web_page.sk; Missing source reference to ss.store.sk; Missing source reference to cs.call_center.sk. A planned reference has no backing source CTE -- typically an unsupported cross-rowset or membership shape the planner could not wire. Review the rowset/join structure (or file an issue if the query looks valid).

  Full SQL with sentinel(s):

  WITH
  bewildered as (
  SELECT
      "ws_web_returns"."WR_WEB_PAGE_SK" as "_ws_returns_outlet_sk",
      sum("ws_web_returns"."WR_NET_LOSS") as "_ws_returns_r_loss",
      sum("ws_web_returns"."WR_RETURN_AMT") as "_ws_returns_returns"
  FROM
      "web_sales" as "ws_web_sales"
      INNER JOIN "web_returns" as "ws_web_returns" on "ws_web_sales"."WS_ITEM_SK" = "ws_web_returns"."WR_ITEM_SK" AND "ws_web_sales"."WS_ORDER_NUMBER" = "ws_web_returns"."WR_ORDER_NUMBER"
      INNER JOIN "date_dim" as "ws_return_date_date" on "ws_web_returns"."WR_RETURNED_DATE_SK" = "ws_return_date_date"."D_DATE_SK"
  WHERE
      cast("ws_return_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-22' and "ws_web_returns"."WR_WEB_PAGE_SK" is not null

  GROUP BY
      1),
  courageous as (
  SELECT
      "ws_web_sales"."WS_WEB_PAGE_SK" as "_ws_sales_outlet_sk",
      sum("ws_web_sales"."WS_EXT_SALES_PRICE") as "_ws_sales_sales",
      sum("ws_web_sales"."WS_NET_PROFIT") as "_ws_sales_s_profit"
  FROM
      "web_sales" as "ws_web_sales"
      INNER JOIN "date_dim" as "ws_date_date" on "ws_web_sales"."WS_SOLD_DATE_SK" = "ws_date_date"."D_DATE_SK"
  WHERE
      cast("ws_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-22' and "ws_web_sales"."WS_WEB_PAGE_SK" is not null

  GROUP BY
      1),
  charming as (
  SELECT
      "ss_store_returns"."SR_STORE_SK" as "_ss_returns_outlet_sk",
      sum("ss_store_returns"."SR_NET_LOSS") as "_ss_returns_r_loss",
      sum("ss_store_returns"."SR_RETURN_AMT") as "_ss_returns_returns"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "store_returns" as "ss_store_returns" on "ss_store_sales"."SS_ITEM_SK" = "ss_store_returns"."SR_ITEM_SK" AND "ss_store_sales"."SS_TICKET_NUMBER" = "ss_store_returns"."SR_TICKET_NUMBER"
      INNER JOIN "date_dim" as "ss_return_date_date" on "ss_store_returns"."SR_RETURNED_DATE_SK" = "ss_return_date_date"."D_DATE_SK"
  WHERE
      cast("ss_return_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-22'

  GROUP BY
      1),
  scrawny as (
  SELECT
      "ss_store_sales"."SS_STORE_SK" as "_ss_sales_outlet_sk",
      sum("ss_store_sales"."SS_EXT_SALES_PRICE") as "_ss_sales_sales",
      sum("ss_store_sales"."SS_NET_PROFIT") as "_ss_sales_s_profit"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
  WHERE
      cast("ss_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-22' and "ss_store_sales"."SS_STORE_SK" is not null

  GROUP BY
      1),
  abundant as (
  SELECT
      "cs_catalog_sales"."CS_CALL_CENTER_SK" as "_cs_sales_outlet_sk",
      sum("cs_catalog_sales"."CS_EXT_SALES_PRICE") as "_cs_sales_sales",
      sum("cs_catalog_sales"."CS_NET_PROFIT") as "_cs_sales_s_profit"
  FROM
      "catalog_sales" as "cs_catalog_sales"
      INNER JOIN "date_dim" as "cs_sold_date_date" on "cs_catalog_sales"."CS_SOLD_DATE_SK" = "cs_sold_date_date"."D_DATE_SK"
  WHERE
      cast("cs_sold_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-22' and "cs_catalog_sales"."CS_CALL_CENTER_SK" is not null

  GROUP BY
      1),
  cheerful as (
  SELECT
      "cs_catalog_returns"."CR_CALL_CENTER_SK" as "_cs_returns_outlet_sk",
      sum("cs_catalog_returns"."CR_NET_LOSS") as "_cs_returns_r_loss",
      sum("cs_catalog_returns"."CR_RETURN_AMOUNT") as "_cs_returns_returns"
  FROM
      "catalog_sales" as "cs_catalog_sales"
      INNER JOIN "catalog_returns" as "cs_catalog_returns" on "cs_catalog_sales"."CS_ITEM_SK" = "cs_catalog_returns"."CR_ITEM_SK" AND "cs_catalog_sales"."CS_ORDER_NUMBER" = "cs_catalog_returns"."CR_ORDER_NUMBER"
      INNER JOIN "date_dim" as "cs_return_date_date" on "cs_catalog_returns"."CR_RETURNED_DATE_SK" = "cs_return_date_date"."D_DATE_SK"
  WHERE
      cast("cs_return_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-22' and "cs_catalog_returns"."CR_CALL_CENTER_SK" is not null

  GROUP BY
      1),
  wooden as (
  SELECT
      "bewildered"."_ws_returns_outlet_sk" as "ws_returns_outlet_sk",
      "bewildered"."_ws_returns_r_loss" as "ws_returns_r_loss",
      "bewildered"."_ws_returns_returns" as "ws_returns_returns"
  FROM
      "bewildered"),
  cool as (
  SELECT
      "courageous"."_ws_sales_outlet_sk" as "ws_sales_outlet_sk",
      "courageous"."_ws_sales_s_profit" as "ws_sales_s_profit",
      "courageous"."_ws_sales_sales" as "ws_sales_sales"
  FROM
      "courageous"),
  premium as (
  SELECT
      "charming"."_ss_returns_outlet_sk" as "ss_returns_outlet_sk",
      "charming"."_ss_returns_r_loss" as "ss_returns_r_loss",
      "charming"."_ss_returns_returns" as "ss_returns_returns"
  FROM
      "charming"),
  kaput as (
  SELECT
      "scrawny"."_ss_sales_outlet_sk" as "ss_sales_outlet_sk",
      "scrawny"."_ss_sales_s_profit" as "ss_sales_s_profit",
      "scrawny"."_ss_sales_sales" as "ss_sales_sales"
  FROM
      "scrawny"),
  yummy as (
  SELECT
      "abundant"."_cs_sales_outlet_sk" as "cs_sales_outlet_sk",
      "abundant"."_cs_sales_s_profit" as "cs_sales_s_profit",
      "abundant"."_cs_sales_sales" as "cs_sales_sales"
  FROM
      "abundant"),
  cooperative as (
  SELECT
      "cheerful"."_cs_returns_outlet_sk" as "cs_returns_outlet_sk",
      "cheerful"."_cs_returns_r_loss" as "cs_returns_r_loss",
      "cheerful"."_cs_returns_returns" as "cs_returns_returns"
  FROM
      "cheerful"),
  ceaseless as (
  SELECT
      "cool"."ws_sales_s_profit" as "ws_sales_s_profit",
      "cool"."ws_sales_sales" as "ws_sales_sales",
      "wooden"."ws_returns_r_loss" as "ws_returns_r_loss",
      "wooden"."ws_returns_returns" as "ws_returns_returns",
      coalesce(coalesce("cool"."ws_sales_outlet_sk","wooden"."ws_returns_outlet_sk"),coalesce("cool"."ws_sales_outlet_sk","wooden"."ws_returns_outlet_sk")) as "_web_combined_outlet_sk"
  FROM
      "wooden"
      INNER JOIN "cool" on "wooden"."ws_returns_outlet_sk" = "cool"."ws_sales_outlet_sk"
  WHERE
      "cool"."ws_sales_sales" is not null
  ),
  puzzled as (
  SELECT
      "kaput"."ss_sales_s_profit" as "ss_sales_s_profit",
      "kaput"."ss_sales_sales" as "ss_sales_sales",
      "premium"."ss_returns_r_loss" as "ss_returns_r_loss",
      "premium"."ss_returns_returns" as "ss_returns_returns",
      coalesce(coalesce("kaput"."ss_sales_outlet_sk","premium"."ss_returns_outlet_sk"),coalesce("kaput"."ss_sales_outlet_sk","premium"."ss_returns_outlet_sk")) as "_store_combined_outlet_sk"
  FROM
      "premium"
      INNER JOIN "kaput" on "premium"."ss_returns_outlet_sk" = "kaput"."ss_sales_outlet_sk"
  WHERE
      "kaput"."ss_sales_sales" is not null
  ),
  juicy as (
  SELECT
      "cooperative"."cs_returns_r_loss" as "cs_returns_r_loss",
      "cooperative"."cs_returns_returns" as "cs_returns_returns",
      "yummy"."cs_sales_s_profit" as "cs_sales_s_profit",
      "yummy"."cs_sales_sales" as "cs_sales_sales",
      coalesce(coalesce("cooperative"."cs_returns_outlet_sk","yummy"."cs_sales_outlet_sk"),coalesce("cooperative"."cs_returns_outlet_sk","yummy"."cs_sales_outlet_sk")) as "_catalog_combined_outlet_sk"
  FROM
      "cooperative"
      INNER JOIN "yummy" on "cooperative"."cs_returns_outlet_sk" = "yummy"."cs_sales_outlet_sk"
  WHERE
      "yummy"."cs_sales_sales" is not null
  ),
  tearful as (
  SELECT
      "ceaseless"."_web_combined_outlet_sk" as "_web_combined_outlet_sk",
      sum("ceaseless"."ws_returns_r_loss") as "_virt_agg_sum_5545753915334516",
      sum("ceaseless"."ws_returns_returns") as "_virt_agg_sum_3209324604424010",
      sum("ceaseless"."ws_sales_s_profit") as "_virt_agg_sum_9873176105043517",
      sum("ceaseless"."ws_sales_sales") as "_web_combined_sales"
  FROM
      "ceaseless"
  GROUP BY
      1),
  waggish as (
  SELECT
      "puzzled"."_store_combined_outlet_sk" as "_store_combined_outlet_sk",
      sum("puzzled"."ss_returns_r_loss") as "_virt_agg_sum_4487605461376389",
      sum("puzzled"."ss_returns_returns") as "_virt_agg_sum_4547400320995823",
      sum("puzzled"."ss_sales_s_profit") as "_virt_agg_sum_7037919280901074",
      sum("puzzled"."ss_sales_sales") as "_store_combined_sales"
  FROM
      "puzzled"
  GROUP BY
      1),
  vacuous as (
  SELECT
      "juicy"."_catalog_combined_outlet_sk" as "_catalog_combined_outlet_sk",
      sum("juicy"."cs_returns_r_loss") as "_virt_agg_sum_3696040952721772",
      sum("juicy"."cs_returns_returns") as "_virt_agg_sum_9983577437841970",
      sum("juicy"."cs_sales_s_profit") as "_virt_agg_sum_4412993304104810",
      sum("juicy"."cs_sales_sales") as "_catalog_combined_sales"
  FROM
      "juicy"
  GROUP BY
      1),
  cloudy as (
  SELECT
      "tearful"."_web_combined_outlet_sk" as "___tvf_arm_2_outlet_sk",
      "tearful"."_web_combined_sales" as "___tvf_arm_2_sales",
      :_web_combined_channel as "web_combined_channel",
      coalesce("tearful"."_virt_agg_sum_3209324604424010",0) as "___tvf_arm_2_returns",
      coalesce("tearful"."_virt_agg_sum_9873176105043517",0) - coalesce("tearful"."_virt_agg_sum_5545753915334516",0) as "___tvf_arm_2_profit"
  FROM
      "tearful"
  WHERE
      coalesce(INVALID_REFERENCE_BUG<Missing source reference to ws.web_page.sk>) is not null
  ),
  rambunctious as (
  SELECT
      "waggish"."_store_combined_outlet_sk" as "___tvf_arm_0_outlet_sk",
      "waggish"."_store_combined_sales" as "___tvf_arm_0_sales",
      :_store_combined_channel as "store_combined_channel",
      coalesce("waggish"."_virt_agg_sum_4547400320995823",0) as "___tvf_arm_0_returns",
      coalesce("waggish"."_virt_agg_sum_7037919280901074",0) - coalesce("waggish"."_virt_agg_sum_4487605461376389",0) as "___tvf_arm_0_profit"
  FROM
      "waggish"
  WHERE
      coalesce(INVALID_REFERENCE_BUG<Missing source reference to ss.store.sk>) is not null
  ),
  concerned as (
  SELECT
      "vacuous"."_catalog_combined_outlet_sk" as "___tvf_arm_1_outlet_sk",
      "vacuous"."_catalog_combined_sales" as "___tvf_arm_1_sales",
      :_catalog_combined_channel as "catalog_combined_channel",
      coalesce("vacuous"."_virt_agg_sum_4412993304104810",0) - coalesce("vacuous"."_virt_agg_sum_3696040952721772",0) as "___tvf_arm_1_profit",
      coalesce("vacuous"."_virt_agg_sum_9983577437841970",0) as "___tvf_arm_1_returns"
  FROM
      "vacuous"
  WHERE
      coalesce(INVALID_REFERENCE_BUG<Missing source reference to cs.call_center.sk>) is not null
  ),
  flashy as (
  SELECT
      "cloudy"."web_combined_channel" as "___tvf_arm_2_channel"
  FROM
      "cloudy"
  GROUP BY
      1),
  hard as (
  SELECT
      "rambunctious"."store_combined_channel" as "___tvf_arm_0_channel"
  FROM
      "rambunctious"
  GROUP BY
      1),
  sparkling as (
  SELECT
      "concerned"."catalog_combined_channel" as "___tvf_arm_1_channel"
  FROM
      "concerned"
  GROUP BY
      1),
  trite as (
  SELECT
      "cloudy"."___tvf_arm_2_outlet_sk" as "___tvf_arm_2_outlet_sk",
      "cloudy"."___tvf_arm_2_profit" as "___tvf_arm_2_profit",
      "cloudy"."___tvf_arm_2_returns" as "___tvf_arm_2_returns",
      "cloudy"."___tvf_arm_2_sales" as "___tvf_arm_2_sales",
      "flashy"."___tvf_arm_2_channel" as "___tvf_arm_2_channel"
  FROM
      "flashy"
      FULL JOIN "cloudy" on 1=1),
  sedate as (
  SELECT
      "hard"."___tvf_arm_0_channel" as "___tvf_arm_0_channel",
      "rambunctious"."___tvf_arm_0_outlet_sk" as "___tvf_arm_0_outlet_sk",
      "rambunctious"."___tvf_arm_0_profit" as "___tvf_arm_0_profit",
      "rambunctious"."___tvf_arm_0_returns" as "___tvf_arm_0_returns",
      "rambunctious"."___tvf_arm_0_sales" as "___tvf_arm_0_sales"
  FROM
      "hard"
      FULL JOIN "rambunctious" on 1=1),
  abhorrent as (
  SELECT
      "concerned"."___tvf_arm_1_outlet_sk" as "___tvf_arm_1_outlet_sk",
      "concerned"."___tvf_arm_1_profit" as "___tvf_arm_1_profit",
      "concerned"."___tvf_arm_1_returns" as "___tvf_arm_1_returns",
      "concerned"."___tvf_arm_1_sales" as "___tvf_arm_1_sales",
      "sparkling"."___tvf_arm_1_channel" as "___tvf_arm_1_channel"
  FROM
      "sparkling"
      FULL JOIN "concerned" on 1=1),
  nondescript as (
  SELECT
      "abhorrent"."___tvf_arm_1_channel" as "_combined_channel",
      "abhorrent"."___tvf_arm_1_outlet_sk" as "_combined_outlet_sk",
      "abhorrent"."___tvf_arm_1_sales" as "_combined_sales",
      "abhorrent"."___tvf_arm_1_returns" as "_combined_returns",
      "abhorrent"."___tvf_arm_1_profit" as "_combined_profit"
  FROM
      "abhorrent"
  UNION ALL
  SELECT
      "sedate"."___tvf_arm_0_channel" as "_combined_channel",
      "sedate"."___tvf_arm_0_outlet_sk" as "_combined_outlet_sk",
      "sedate"."___tvf_arm_0_sales" as "_combined_sales",
      "sedate"."___tvf_arm_0_returns" as "_combined_returns",
      "sedate"."___tvf_arm_0_profit" as "_combined_profit"
  FROM
      "sedate"
  UNION ALL
  SELECT
      "trite"."___tvf_arm_2_channel" as "_combined_channel",
      "trite"."___tvf_arm_2_outlet_sk" as "_combined_outlet_sk",
      "trite"."___tvf_arm_2_sales" as "_combined_sales",
      "trite"."___tvf_arm_2_returns" as "_combined_returns",
      "trite"."___tvf_arm_2_profit" as "_combined_profit"
  FROM
      "trite"),
  round as (
  SELECT
      "nondescript"."_combined_channel" as "_combined_channel",
      "nondescript"."_combined_outlet_sk" as "_combined_outlet_sk",
      "nondescript"."_combined_profit" as "_combined_profit",
      "nondescript"."_combined_returns" as "_combined_returns",
      "nondescript"."_combined_sales" as "_combined_sales"
  FROM
      "nondescript"),
  slow as (
  SELECT
      "round"."_combined_channel" as "combined_channel",
      "round"."_combined_outlet_sk" as "combined_outlet_sk",
      "round"."_combined_profit" as "combined_profit",
      "round"."_combined_returns" as "combined_returns",
      "round"."_combined_sales" as "combined_sales"
  FROM
      "round")
  SELECT
      "slow"."combined_channel" as "channel",
      "slow"."combined_outlet_sk" as "outlet",
      sum("slow"."combined_sales") as "sales",
      sum("slow"."combined_returns") as "returns",
      sum("slow"."combined_profit") as "profit",
      grouping("slow"."combined_channel") + grouping("slow"."combined_outlet_sk") as "_level"
  FROM
      "slow"
  GROUP BY
      ROLLUP (1, 2)
  ORDER BY
      "_level" asc,
      "channel" asc nulls first,
      "outlet" asc nulls first,
      "returns" desc
  LIMIT (100)
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_1226264875.preql`

  ```text
  Syntax error in answer_1226264875.preql: Conflicting join types (full, left outer) on keys joined into one group: a FULL/UNION join cannot be mixed with another type on the same key (it is ambiguous whether the key is required or one-sided). Make the whole group one type (e.g. `UNION JOIN a = b = c`), or use a distinct key. (line 60, column 1)
  ```
- `trilogy run answer_1226264875.preql`

  ```text
  Syntax error in answer_1226264875.preql: Conflicting join types (full, left outer) on keys joined into one group: a FULL/UNION join cannot be mixed with another type on the same key (it is ambiguous whether the key is required or one-sided). Make the whole group one type (e.g. `UNION JOIN a = b = c`), or use a distinct key. (line 45, column 1)
  ```
- `trilogy run answer_1226264875.preql`

  ```text
  Syntax error in answer_1226264875.preql: Undefined concept: web_groups.year. Suggestions: ['web_groups.web_qty', 'web_groups.item_sk', 'web_groups.web_sprice', 'store_groups.year', 'catalog_groups.year', 'web.date.year']
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 53 column 12 (char 1697). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run answer_4111870542.preql`

  ```text
  Syntax error in answer_4111870542.preql: HAVING references 'local.store_return_rows', 'local.catalog_return_rows', 'local.web_return_rows', which are not defined (line 40). Check for a typo or import the relevant concept.
  ```
- `trilogy run answer_4111870542.preql`

  ```text
  Syntax error in answer_4111870542.preql: Output column 'store_pct' renames 'local.store_pct' back to the name of an existing concept 'store_pct' (defined at line 62) that 'local.store_pct' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'store_pct_out').
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.store_sales:store_sales select store.name, store_sales.line_item limit 5;`

  ```text
  Syntax error in stdin: Undefined concept: store.name (line 2, col 8, in SELECT). Suggestions: ['store_sales.store.name', 'store_sales.return_store.name']
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read answer_2822608529.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

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
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe store_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run answer_3734509910.preql`

  ```text
  Syntax error in answer_3734509910.preql: 4 undefined concept references; fix all before re-running:
    - ss2.ext_sales_price (line 17, in SELECT); did you mean: ss.ext_sales_price, ss.sales_price, ss.ext_list_price?
    - ss2.date.date (line 18, col 13, in WHERE); did you mean: ss.date.date, ss.store.date.date, ss.return_date.date, ss.return_store.date.date, ss.customer.first_sales_date.date, ss.customer.first_shipto_date.date?
    - ss2.item.class (line 19, col 13, in WHERE); did you mean: ss2.item.category, ss.item.class, class, ss.item.class_id, ss.item.sk?
    - ss2.item.category (line 20, col 13, in WHERE); did you mean: ss2.item.class, ss.item.category, category, ss.item.category_id, ss.item.color?
  ```

### `syntax-parse`

- `trilogy run --import raw.all_sales:all_sales select all_sales.channel, all_sales.channel_dim_text_id, sum(all_sales.ext_sales_price) as total_sales where all…'2000-08-23'::date and '2000-09-06'::date and all_sales.channel_dim_id is not null group by all_sales.channel, all_sales.channel_dim_text_id limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...es.channel_dim_id is not null ??? group by all_sales.channel, al...
  ```
- `trilogy run --import raw.all_sales:all_sales with sales_agg as where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date and all_sales.chan…(*) as cnt union join sales_agg.channel = returns_agg.channel union join sales_agg.entity_id = returns_agg.entity_id order by sales_agg.channel desc;`

  ```text
  Syntax error in stdin: Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...ns; select sales_agg.channel, ??? count(*) as cnt union join sal...
  ```
- `trilogy file write answer_3705756794.preql --content import raw.all_sales as all_sales;

# Sales aggregation: Sum ext_sales_price and net_profit by channel a…s_agg.channel) = 1 and grouping(sales_agg.entity_id) = 1)
order by sales_agg.channel asc nulls first, sales_agg.entity_id asc nulls first
limit 100;
`

  ```text
  refused to write 'answer_3705756794.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...channel = returns_agg.channel ??? union join sales_agg.entity_id...

  Write stats: received 2220 chars / 2220 bytes; tail: …'nulls first, sales_agg.entity_id asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.channel, count(*) as cnt where all_sales.return_date.date between '2000-08-23'::date and '2000-09-06'::date and all_sales.return_channel_dim_id is not null;`

  ```text
  Syntax error in stdin: Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...es; select all_sales.channel, ??? count(*) as cnt where all_sale...
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.channel, count(all_sales.order_id) as cnt group by all_sales.channel;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...nt(all_sales.order_id) as cnt ??? group by all_sales.channel;
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.channel, all_sales.date.year, all_sales.item.brand_id, all_sales.item.class_id, all_sales.item.category_id, count(all_sales.order_id) as cnt group by all_sales.channel, all_sales.date.year;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...nt(all_sales.order_id) as cnt ??? group by all_sales.channel, al...
  ```
- `trilogy run --import raw.all_sales:all_sales select count(*) as cnt;`

  ```text
  Syntax error in stdin: Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...ll_sales as all_sales; select ??? count(*) as cnt;
  ```
- `trilogy file write answer_1078396760.preql --content import raw/catalog_sales as cs;

# Filter to relevant items and date range
where cs.sold_date.date betwe…st,
    item_class asc nulls first,
    item_code asc nulls first,
    item_desc asc nulls first,
    pct_of_class_sales asc nulls first

limit 100;
`

  ```text
  refused to write 'answer_1078396760.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/catalog_sales as cs;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/catalog_sales as cs;  # Fi...

  Write stats: received 783 chars / 783 bytes; tail: …'s first,\\n    pct_of_class_sales asc nulls first\\n\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_2928586490.preql`

  ```text
  refused to write 'answer_2928586490.preql': not syntactically valid Trilogy.

  Parse error:
    --> 10:30
     |
  10 |     count(store_sales.item.sk, substring(store_sales.item.desc, 1, 30), store_sales.date.sk) as cnt
     |                              ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
       count(store_sales.item.sk ??? , substring(store_sales.item.d...

  Write stats: received 2802 chars / 2802 bytes; tail: …'e asc nulls first, total_sales asc nulls first\r\\nlimit 100;\r\\n'.
  ```
- `trilogy file write answer_1798498862.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;

# Store sales in Sept…es.sold_date.year in (1999, 2000, 2001)

order by store_sales.item.id, store_sales.item.desc, store_sales.store.id, store_sales.store.name
limit 100;`

  ```text
  refused to write 'answer_1798498862.preql': not syntactically valid Trilogy.

  Parse error:
   --> 6:20
    |
  6 |   and month_of_year(store_sales.date.date) = 9
    |                    ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
  ...te) = 1999   and month_of_year ??? (store_sales.date.date) = 9

  Write stats: received 1240 chars / 1240 bytes; tail: …'esc, store_sales.store.id, store_sales.store.name\\nlimit 100;'.
  ```
- `trilogy file write answer_1798498862.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;

# Define the set of (…antity,
    sum(catalog_pairs.cat_qty) as total_catalog_sale_ticket_quantity

order by item_code, item_description, store_code, store_name
limit 100;`

  ```text
  refused to write 'answer_1798498862.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...k = catalog_pairs.cat_item_sk ??? union join store_sales.custome...

  Write stats: received 1644 chars / 1644 bytes; tail: …'em_code, item_description, store_code, store_name\\nlimit 100;'.
  ```
- `trilogy file write answer_1798498862.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;

# Store sales in Sep …lect count_distinct(qualifying_store_sales.item_code) as distinct_items;
select count_distinct(qualifying_store_sales.store_code) as distinct_stores;`

  ```text
  refused to write 'answer_1798498862.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...tity) as return_qty ;  select ??? count(*) as cnt; select count_...

  Write stats: received 991 chars / 991 bytes; tail: …'tinct(qualifying_store_sales.store_code) as distinct_stores;'.
  ```
- `trilogy file write answer_1663418777.preql --content import raw.item as item;

auto profile_manus <- item.manufact ? (
    (item.category = 'Books' and item.…d between 1 and 500
    and item.manufact in profile_manus
select distinct
    item.product_name as product_name
order by
    product_name
limit 100;`

  ```text
  refused to write 'answer_1663418777.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...ufact in profile_manus select ??? distinct     item.product_name...

  Write stats: received 1097 chars / 1097 bytes; tail: …'ct_name as product_name\\norder by\\n    product_name\\nlimit 100;'.
  ```
- `trilogy file write answer_3079393690.preql --content import raw.store_sales as ss;

# Sum of unit price (sales_price, not ext_sales_price) per day of week
# …s wednesday,
    @thu_sales as thursday,
    @fri_sales as friday,
    @sat_sales as saturday
order by ss.store.name asc, ss.store.id asc
limit 100;
`

  ```text
  refused to write 'answer_3079393690.preql': not syntactically valid Trilogy.

  Parse error:
    --> 18:5
     |
  18 |     @sun_sales as sunday,
     |     ^---
     |
     = expected select_item, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...s.store.id as store_code,     ??? @sun_sales as sunday,     @mon...

  Write stats: received 1003 chars / 1003 bytes; tail: …'rday\\norder by ss.store.name asc, ss.store.id asc\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_71623752.preql --content import raw.store_sales as ss;

# Items in profile A: Books/Children/Electronics, specific classes, specifi…arter_total,
    per_quarter.manufacturer_id
limit 100
select
    final.manufacturer_id,
    final.per_quarter_total,
    final.avg_quarterly_sales
;`

  ```text
  refused to write 'answer_71623752.preql': not syntactically valid Trilogy.

  Parse error:
    --> 40:1
     |
  40 | limit 100
     | ^---
     |
     = expected ORDERING_DIRECTION, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
    per_quarter.manufacturer_id ??? limit 100 select     final.man...

  Write stats: received 1593 chars / 1593 bytes; tail: …'    final.per_quarter_total,\\n    final.avg_quarterly_sales\\n;'.
  ```
- `trilogy file write answer_3553309440.preql --content import raw.catalog_sales as cs;
import raw.web_sales as ws;
import raw.store_sales as ss;
import raw.cus…e
select
    ss.customer.id as customer_id,
    round(sum(ss.ext_sales_price) / 50.0) as segment,
    sum(ss.ext_sales_price) as total_store_price
;
`

  ```text
  refused to write 'answer_3553309440.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
   cat_cust.c_sk, cat_cust.c_id ??? from cat_cust),     (select we...

  Write stats: received 1503 chars / 1503 bytes; tail: …'segment,\\n    sum(ss.ext_sales_price) as total_store_price\\n;\\n'.
  ```
- `trilogy run --import raw.item:item select distinct item.color limit 20;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...port raw.item as item; select ??? distinct item.color limit 20;
  ```
- `trilogy run --import raw.address:addr select distinct addr.gmt_offset limit 20;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...t raw.address as addr; select ??? distinct addr.gmt_offset limit...
  ```
- `trilogy file write answer_2986518257.preql --content import raw.catalog_sales as cs;

# Step 1: monthly totals per (category, brand, call center, year, month…_name,
    s.cc_name,
    s.year,
    s.month_of_year,
    s.avg_monthly,
    s.monthly_total,
    prior_month_total,
    next_month_total
limit 100;`

  ```text
  refused to write 'answer_2986518257.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...monthly_total) as avg_monthly ??? from monthly_totals mt group b...

  Write stats: received 1921 chars / 1921 bytes; tail: …'otal,\\n    prior_month_total,\\n    next_month_total\\nlimit 100;'.
  ```
- `trilogy file write answer_2986518257.preql --content import raw.catalog_sales as cs;

# Step 1: monthly totals per (category, brand, call center, year, month…category,
    monthly_totals.brand_name,
    monthly_totals.cc_name,
    monthly_totals.year,
    avg(monthly_totals.monthly_total) as avg_monthly
;
`

  ```text
  refused to write 'answer_2986518257.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...monthly_total) as avg_monthly ??? from monthly_totals mt subset

  Write stats: received 857 chars / 857 bytes; tail: …'ear,\\n    avg(monthly_totals.monthly_total) as avg_monthly\\n;\\n'.
  ```
- `trilogy run --import raw/store_sales:ss select ss.item.id, sum(ss.ext_sales_price) as store_total where ss.date.week_seq = 5218 group by ss.item.id limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...where ss.date.week_seq = 5218 ??? group by ss.item.id limit 5;
  ```
- `trilogy file write answer_1623435181.preql --content import raw.store_sales as ss;

# Weekly unit-price sums per store, split by day of week, for both years
…on by wk.id order by wk.week_seq) = wk.week_seq + 52
order by wk.name asc nulls first, wk.id asc nulls first, wk.week_seq asc nulls first
limit 100;
`

  ```text
  refused to write 'answer_1623435181.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [211]: Expression in `by` clause must be wrapped in parens - write `by (expr1, expr2, ...)`. Bare identifiers (`by a, b`) work without parens, but any function call, cast, or other expression needs them.
  Location:
  ...t-price sums per store, split ??? by day of week, for both years...

  Write stats: received 3693 chars / 3693 bytes; tail: …'.id asc nulls first, wk.week_seq asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_1623435181.preql --content import raw.store_sales as ss;

# Weekly unit-price sums per store, split by day of week, for both years
…k.year = 2001
having nxt_week_seq = wk.week_seq + 52
order by wk.name asc nulls first, wk.id asc nulls first, wk.week_seq asc nulls first
limit 100;
`

  ```text
  refused to write 'answer_1623435181.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [211]: Expression in `by` clause must be wrapped in parens - write `by (expr1, expr2, ...)`. Bare identifiers (`by a, b`) work without parens, but any function call, cast, or other expression needs them.
  Location:
  ...t-price sums per store, split ??? by day of week, for both years...

  Write stats: received 1545 chars / 1545 bytes; tail: …'.id asc nulls first, wk.week_seq asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_1623435181.preql --content import raw.store_sales as ss;

rowset wk as
where ss.date.year in (2001, 2002)
select
    ss.store.id,
 …k.year = 2001
having nxt_week_seq = wk.week_seq + 52
order by wk.name asc nulls first, wk.id asc nulls first, wk.week_seq asc nulls first
limit 100;
`

  ```text
  refused to write 'answer_1623435181.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [105]: A `rowset`/`auto`/`metric`/`property` definition connects its name to its expression with `<-`, not `as` - write `rowset base <- select ...;` (a `rowset` may also use the `with base as select ...;` form). For `auto`/`metric`/`property` only `<-` is valid, e.g. `auto total <- sum(sales.amount);`.
  Location:
  ...store_sales as ss;  rowset wk ??? as where ss.date.year in (2001...

  Write stats: received 1471 chars / 1471 bytes; tail: …'.id asc nulls first, wk.week_seq asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw.store_sales:store_sales select distinct item.brand_name limit 20;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ..._sales as store_sales; select ??? distinct item.brand_name limit...
  ```
- `trilogy run --import raw.store_sales:ss select ss.item.product_name, ss.item.color, ss.item.current_price, count(ss.line_item) as cnt 
where ss.item.color in…tal_status != ss.customer.demographics.marital_status
group by ss.item.product_name, ss.item.color, ss.item.current_price
order by cnt desc
limit 20;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...r.demographics.marital_status ??? group by ss.item.product_name,...
  ```
- `trilogy file write answer_2091535883.preql --content import raw.catalog_sales as cs;
import raw.inventory as inv;

where cs.sold_date.year = 1999
  and cs.po…ld_date.week_seq = inv.date.week_seq
where inv.quantity_on_hand < cs.quantity
order by all_rows desc, item_desc, warehouse_name, sold_week
limit 100;`

  ```text
  refused to write 'answer_2091535883.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
   end) as with_promo_rows,     ??? count(*) as all_rows subset jo...

  Write stats: received 745 chars / 745 bytes; tail: …'l_rows desc, item_desc, warehouse_name, sold_week\\nlimit 100;'.
  ```
- `trilogy file write answer_3046445280.preql --content import raw/all_sales as s;

# Filter to Books category and compute per-row net values
# net_qty: sold qu…,
    yr2002.qty_2002 - yr2002.qty_2001 as qty_diff,
    yr2002.amt_2002 - yr2002.amt_2001 as amt_diff
order by qty_diff asc, amt_diff asc
limit 100;`

  ```text
  refused to write 'answer_3046445280.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/all_sales as s;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/all_sales as s;  # Filter

  Write stats: received 2002 chars / 2002 bytes; tail: …'1 as amt_diff\\norder by qty_diff asc, amt_diff asc\\nlimit 100;'.
  ```
- `trilogy file write answer_3046445280.preql --content import raw.all_sales as s;

# Filter to Books category and compute per-row net values
# net_qty: sold qu…,
    yr2002.qty_2002 - yr2001.qty_2001 as qty_diff,
    yr2002.amt_2002 - yr2001.amt_2001 as amt_diff
order by qty_diff asc, amt_diff asc
limit 100;`

  ```text
  refused to write 'answer_3046445280.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...egory_id = yr2001.category_id ??? union join yr2002.manufacturer...

  Write stats: received 2203 chars / 2203 bytes; tail: …'1 as amt_diff\\norder by qty_diff asc, amt_diff asc\\nlimit 100;'.
  ```
- `trilogy file write answer_3046445280.preql --content import raw.all_sales as s;

# Filter to Books category and compute per-row net values
# net_qty: sold qu…,
    yr2002.qty_2002 - yr2001.qty_2001 as qty_diff,
    yr2002.amt_2002 - yr2001.amt_2001 as amt_diff
order by qty_diff asc, amt_diff asc
limit 100;`

  ```text
  refused to write 'answer_3046445280.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...ping only those in BOTH years ??? union join yr2002.brand_id = y...

  Write stats: received 2188 chars / 2188 bytes; tail: …'1 as amt_diff\\norder by qty_diff asc, amt_diff asc\\nlimit 100;'.
  ```
- `trilogy file write answer_3046445280.preql --content import raw.all_sales as s;

# Filter to Books category and compute per-row net values
# net_qty: sold qu…,
    yr2002.qty_2002 - yr2001.qty_2001 as qty_diff,
    yr2002.amt_2002 - yr2001.amt_2001 as amt_diff
order by qty_diff asc, amt_diff asc
limit 100;`

  ```text
  refused to write 'answer_3046445280.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...egory_id = yr2001.category_id ??? union join yr2002.manufacturer...

  Write stats: received 2203 chars / 2203 bytes; tail: …'1 as amt_diff\\norder by qty_diff asc, amt_diff asc\\nlimit 100;'.
  ```
- `trilogy file write answer_747155668.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;
import raw.catalog_sales as cat…ls first,
  combined.missing_ref nulls first,
  combined.year nulls first,
  combined.quarter nulls first,
  combined.category nulls first
limit 100;`

  ```text
  refused to write 'answer_747155668.preql': not syntactically valid Trilogy.

  Parse error:
   --> 8:5
    |
  8 |     auto missing_line_flag <- sum(
    |     ^---
    |
    = expected select_statement
  Location:
  ...tore sk (ss_store_sk)   (     ??? auto missing_line_flag <- sum(...

  Write stats: received 2729 chars / 2729 bytes; tail: …'rter nulls first,\\n  combined.category nulls first\\nlimit 100;'.
  ```
- `trilogy file write answer_747155668.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;
import raw.catalog_sales as cat…ls first,
  combined.missing_ref nulls first,
  combined.year nulls first,
  combined.quarter nulls first,
  combined.category nulls first
limit 100;`

  ```text
  refused to write 'answer_747155668.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ..._count, total_ext_sales_price) ??? select   combined.channel,

  Write stats: received 2694 chars / 2694 bytes; tail: …'rter nulls first,\\n  combined.category nulls first\\nlimit 100;'.
  ```
- `trilogy file write answer_3210116865.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# --- Store c…ombined.outlet as outlet,
    web_combined.sales as sales,
    web_combined.returns as returns,
    web_combined.profit as profit
from web_combined
;`

  ```text
  refused to write 'answer_3210116865.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...ore_combined.profit as profit ??? from store_combined union all

  Write stats: received 3652 chars / 3652 bytes; tail: …'turns,\\n    web_combined.profit as profit\\nfrom web_combined\\n;'.
  ```
- `trilogy run --import raw/all_sales:all_sales select all_sales.channel, all_sales.channel_dim_text_id where all_sales.channel = 'STORE' and all_sales.channel_dim_id <= 5 group by 1,2 limit 20;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...all_sales.channel_dim_id <= 5 ??? group by 1,2 limit 20;
  ```
- `trilogy run --import raw/all_sales:all_sales select distinct all_sales.channel, all_sales.channel_dim_text_id where all_sales.channel = 'STORE' and all_sales.channel_dim_id <= 5 order by 2 limit 20;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...ll_sales as all_sales; select ??? distinct all_sales.channel, al...
  ```
- `trilogy file write answer_943796012.preql --content import raw/all_sales as all_sales;

where 
    all_sales.date.date between '2000-08-23'::date and '2000-0…ales.return_net_loss), 0)
    end as profit
by rollup (channel_label, outlet_id)
order by channel_label nulls first, outlet_id nulls first
limit 100;`

  ```text
  refused to write 'answer_943796012.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/all_sales as all_sales;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/all_sales as all_sales;  w...

  Write stats: received 1180 chars / 1180 bytes; tail: …' channel_label nulls first, outlet_id nulls first\\nlimit 100;'.
  ```
- `trilogy file write answer_4111870542.preql --content import raw/store_sales as ss;
import raw/catalog_sales as cs;
import raw/web_sales as ws;

# Rowset: sto…_return_rows > 0
    and catalog_return_rows > 0
    and web_return_rows > 0
order by item_code nulls first, store_return_qty nulls first
limit 100;
`

  ```text
  refused to write 'answer_4111870542.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/store_sales as ss;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/store_sales as ss; import

  Write stats: received 2507 chars / 2507 bytes; tail: …'m_code nulls first, store_return_qty nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_4111870542.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Rowset: sto…then null
        else coalesce(joined.web_qty, 0)
    end as web_return_qty
order by item_code nulls first, store_return_qty nulls first
limit 100;
`

  ```text
  refused to write 'answer_4111870542.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...e = catalog_returns.item_code ??? union join store_returns.item_...

  Write stats: received 2167 chars / 2167 bytes; tail: …'m_code nulls first, store_return_qty nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_4111870542.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Rowset: sto…en null
        else coalesce(combined.web_qty, 0)
    end as web_return_qty
order by item_code nulls first, store_return_qty nulls first
limit 100;
`

  ```text
  refused to write 'answer_4111870542.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...e = catalog_returns.item_code ??? union join store_returns.item_...

  Write stats: received 2198 chars / 2198 bytes; tail: …'m_code nulls first, store_return_qty nulls first\\nlimit 100;\\n'.
  ```
- `trilogy run answer_2869182220.preql`

  ```text
  Syntax error in answer_2869182220.preql: Syntax [220]: Filter or stray clause after a `join`? A query-scoped join `inner|left|full join <a> = <b>` takes only key equalities - to join on multiple keys, chain `= c` (one equivalence group) or separate distinct groups with `and` (`a = b and c = d`); STACK another `join` clause for a different join type. Note `and` joins KEY EQUALITIES only, not filters. Joins go right after the `select` list (preferred, SQL-like) or before `select`; the order is `where` -> `select` <cols> -> join(s) -> `having` -> `order by` -> `limit`. Filter input rows in `where` (before `select`); filter a joined or aggregated RESULT in `having` (select the field, hide it with a leading `--`). Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...tomer's current household and ??? cust.household_demographic.inc...
  ```
- `trilogy run --import raw/web_sales:ws select ws.time.hour, count(ws.line_item) as c where ws.pos_ship_household_demographic.dependent_count = 6 and ws.web_page.char_count between 5000 and 5200 group by ws.time.hour order by ws.time.hour limit 30;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...r_count between 5000 and 5200 ??? group by ws.time.hour order by...
  ```

### `cli-misuse`

- `trilogy explore raw/catalog_returns.preql`

  ```text
  Invalid value for 'PATH': File 'raw/catalog_returns.preql' does not exist.
  ```
- `trilogy explore raw/`

  ```text
  Invalid value for 'PATH': File 'raw/' is a directory.
  ```
- `trilogy explore raw/`

  ```text
  Invalid value for 'PATH': File 'raw/' is a directory.
  ```
- `trilogy explore raw/`

  ```text
  Invalid value for 'PATH': File 'raw/' is a directory.
  ```
- `trilogy explore raw/web_returns.preql`

  ```text
  Invalid value for 'PATH': File 'raw/web_returns.preql' does not exist.
  ```

### `planner-recursion`

- `trilogy run answer_1623435181.preql`

  ```text
  Resolution error in answer_1623435181.preql: query could not be planned; this is a bug.
  ```
- `trilogy run answer_1623435181.preql`

  ```text
  Resolution error in answer_1623435181.preql: query could not be planned; this is a bug.
  ```
- `trilogy run answer_1623435181.preql`

  ```text
  Resolution error in answer_1623435181.preql: query could not be planned; this is a bug.
  ```
- `trilogy run answer_1623435181.preql`

  ```text
  Resolution error in answer_1623435181.preql: query could not be planned; this is a bug.
  ```

### `join-resolution`

- `trilogy run answer_1623435181.preql`

  ```text
  Resolution error in answer_1623435181.preql: Could not resolve connections for query with output ['_subquery_67_32.weekly.ss.date.week_seq<Purpose.PROPERTY>Derivation.ROWSET>'] from current model.
  ```

### `syntax-missing-alias`

- `trilogy file write answer_2910545909.preql --content import raw.store_sales as ss;

where
    ss.date.year between 1999 and 2001
    and ss.date.day_name = '…,
    first_name asc nulls first,
    display_city asc nulls first,
    total_net_profit asc nulls first,
    ticket_number asc nulls last
limit 100;`

  ```text
  refused to write 'answer_2910545909.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `hidden keys for grain: ss.customer.sk as hidden_keys_for_grain_ss_customer_sk`
  Location:
  ...s ticket_number,     --hidden ??? keys for grain: ss.customer.sk...

  Write stats: received 840 chars / 840 bytes; tail: …'asc nulls first,\\n    ticket_number asc nulls last\\nlimit 100;'.
  ```

### `type-error`

- `trilogy run answer_1197120511.preql`

  ```text
  Syntax error in answer_1197120511.preql: Cannot use BETWEEN with incompatible types DATE and STRING (low)
  ```
