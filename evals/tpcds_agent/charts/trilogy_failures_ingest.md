# Trilogy failure analysis — 20260717-173332

- Run `20260717-173332_ingest` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 1576 | failed: 193 (12%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 139 | 72% |
| `syntax-parse` | 42 | 22% |
| `join-resolution` | 6 | 3% |
| `cli-misuse` | 4 | 2% |
| `undefined-concept` | 1 | 1% |
| `syntax-missing-alias` | 1 | 1% |

## Detail

### `other`

- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_1858999935.preql`

  ```text
  Syntax error in answer_1858999935.preql: ORDER BY references 'weekly_sales.combined.dow', which is not in the SELECT projection (line 15). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --weekly_sales.combined.dow order by weekly_sales.combined.dow asc`.
  ```
- `trilogy run answer_1858999935.preql`

  ```text
  Syntax error in answer_1858999935.preql: ORDER BY references 'weekly_sales.combined.dow', which is not in the SELECT projection (line 24). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --weekly_sales.combined.dow order by weekly_sales.combined.dow asc`.
  ```
- `trilogy run answer_3805575480.preql`

  ```text
  Resolution error in answer_3805575480.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {item.manufact_id, brand_id, brand_name}; {total_ext_sales_price, year, store_sales.date_dim.moy}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 1 column 3864 (char 3863). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run answer_3863442186.preql`

  ```text
  Syntax error in answer_3863442186.preql: Undefined concept: sv01.customer_id. Suggestions: ['s.customer.customer_id', 'cs.bill_customer.customer_id', 'cs.ship_customer.customer_id', 'ws.bill_customer.customer_id', 'ws.ship_customer.customer_id', 'store_vals.s.customer.customer_id']
  ```
- `trilogy run answer_3863442186.preql`

  ```text
  Syntax error in answer_3863442186.preql: Undefined concept: sv02.store_val. Suggestions: ['store_vals.store_val']
  ```
- `trilogy run --import raw.catalog_sales:cs --import raw.store_sales:s --import raw.web_sales:ws select s.customer.customer_id, s.customer.first_name, s.custom…_2002 > 0 and c_2002 > 0 and w_2002 > 0 and c_2002/c_2001 > s_2002/s_2001 and c_2002/c_2001 > w_2002/w_2001 order by s.customer.customer_id limit 10;`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 3 disconnected subgraphs: {cs.bill_customer.customer_id, c_2001, c_2002}; {s_2001, s_2002, s.customer.customer_id, s.customer.first_name, s.customer.last_name, s.customer.preferred_cust_flag}; {w_2001, w_2002, ws.bill_customer.customer_id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read answer_3863442186.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/customer.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.item:i with qual_items as select i.item_sk as sk, i.category as cat, avg(i.current_price) by i.category as cat_avg where i.category is not null and i.current_price > 1.2 * (avg(i.current_price) by i.category);`

  ```text
  Syntax error in stdin: Nothing was executed: parsed 2 definition statement(s) (1 import, 1 rowset) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy file read answer_883027685.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_689898357.preql`

  ```text
  Syntax error in answer_689898357.preql: Comparison `ss.promotion.channel_email = 'N'` matches every value of nullable enum field 'ss.promotion.channel_email', which contains only these values: 'N'. It only excludes nulls; simplify it to 'ss.promotion.channel_email is not null'.
  ```
- `trilogy run answer_3263796749.preql`

  ```text
  Syntax error in answer_3263796749.preql: Undefined concept: item.item_sk. Suggestions: ['store_sales.item.item_sk', 'store_sales.promotion.item.item_sk']
  ```
- `trilogy run answer_219203908.preql`

  ```text
  Syntax error in answer_219203908.preql: `by rollup (…)` cannot re-aggregate the bare measure reference `leaf_groups.total_sales`. Alias it with an explicit aggregate (e.g. `sum(leaf_groups.total_sales) as total_sales_total`).
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_2524943990.preql`

  ```text
  Syntax error in answer_2524943990.preql: Comparison `catalog_sales.call_center.county = 'Williamson County'` matches every value of enum field 'catalog_sales.call_center.county', which contains only these values: 'Williamson County'. It is always true and should be removed.
  ```
- `trilogy file read raw/item.preql`

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
- `trilogy run answer_4254732537.preql`

  ```text
  Syntax error in answer_4254732537.preql: Comparison `cs.promotion.channel_email = 'N'` matches every value of nullable enum field 'cs.promotion.channel_email', which contains only these values: 'N'. It only excludes nulls; simplify it to 'cs.promotion.channel_email is not null'.
  ```
- `trilogy file read answer_4080205480.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_1798498862.preql`

  ```text
  Resolution error in answer_1798498862.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 9). The requested concepts split into 3 disconnected subgraphs: {cs.bill_customer.customer_sk, cs.item.item_sk, cs.sold_date.year, total_catalog_sales_qty}; {item_code, item_description, store_code, store_name, total_store_sales_qty, ss.customer.customer_sk, ss.date_dim.moy, ss.date_dim.year, ss.item.item_sk}; {total_store_return_qty, sr.customer.customer_sk, sr.date_dim.moy, sr.date_dim.year}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 108 column 3 (char 3803). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file read answer_4207382245.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read trilogy.toml`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file list assets --recursive`

  ```text
  No such path: assets
  ```
- `trilogy run answer_3530408544.preql`

  ```text
  Syntax error in answer_3530408544.preql: Comparison `ss.store.county = 'Williamson County'` matches every value of enum field 'ss.store.county', which contains only these values: 'Williamson County'. It is always true and should be removed.
  ```
- `trilogy run answer_4140546834.preql`

  ```text
  Unexpected error in answer_4140546834.preql: (_duckdb.ConversionException) Conversion Error: Casting value "103919104.54" to type DECIMAL(12,4) failed: value is out of range!

  LINE 41: ...tive"."_virt_agg_sum_2722582045378473" as numeric(12,4)) / cast(nullif("cooperative"."_virt_agg_sum_6719372088781090...
                                                                         ^
  [SQL:
  WITH
  thoughtful as (
  SELECT
      "sales_item_item"."i_category" as "sales_item_category",
      "sales_item_item"."i_class" as "sales_item_class",
      "sales_store_sales"."ss_ext_sales_price" as "sales_ext_sales_price",
      "sales_store_sales"."ss_net_profit" as "sales_net_profit"
  FROM
      "store_sales" as "sales_store_sales"
      INNER JOIN "item" as "sales_item_item" on "sales_store_sales"."ss_item_sk" = "sales_item_item"."i_item_sk"
      INNER JOIN "store" as "sales_store_store" on "sales_store_sales"."ss_store_sk" = "sales_store_store"."s_store_sk"
      INNER JOIN "date_dim" as "sales_date_dim_date_dim" on "sales_store_sales"."ss_sold_date_sk" = "sales_date_dim_date_dim"."d_date_sk"
  WHERE
      "sales_date_dim_date_dim"."d_year" = 2001 and "sales_store_store"."s_state" = 'TN'
  ),
  cooperative as (
  SELECT
      "thoughtful"."sales_item_category" as "sales_item_category",
      "thoughtful"."sales_item_class" as "sales_item_class",
      CASE
  	WHEN grouping("thoughtful"."sales_item_category") + grouping("thoughtful"."sales_item_class") = 0 THEN "thoughtful"."sales_item_category"
  	ELSE null
  	END as "_virt_func_case_8529937465942981",
      grouping("thoughtful"."sales_item_category") as "_virt_agg_grouping_1670065760616147",
      grouping("thoughtful"."sales_item_class") as "_virt_agg_grouping_8417677704977793",
      sum("thoughtful"."sales_ext_sales_price") as "_virt_agg_sum_6719372088781090",
      sum("thoughtful"."sales_net_profit") as "_virt_agg_sum_2722582045378473"
  FROM
      "thoughtful"
  GROUP BY
      ROLLUP (1, 2)),
  questionable as (
  SELECT
      "cooperative"."_virt_agg_grouping_1670065760616147" as "_virt_agg_grouping_1670065760616147",
      "cooperative"."_virt_agg_grouping_8417677704977793" as "_virt_agg_grouping_8417677704977793",
      "cooperative"."_virt_agg_sum_2722582045378473" as "_virt_agg_sum_2722582045378473",
      "cooperative"."_virt_agg_sum_6719372088781090" as "_virt_agg_sum_6719372088781090",
      "cooperative"."sales_item_category" as "sales_item_category",
      "cooperative"."sales_item_class" as "sales_item_class",
      rank() over (partition by "cooperative"."_virt_func_case_8529937465942981" order by cast("cooperative"."_virt_agg_sum_2722582045378473" as numeric(12,4)) / cast(nullif("cooperative"."_virt_agg_sum_6719372088781090",0) as numeric(12,4)) asc ) as "within_subgroup_rank"
  FROM
      "cooperative")
  SELECT
      cast("questionable"."_virt_agg_sum_2722582045378473" as numeric(12,4)) / cast(nullif("questionable"."_virt_agg_sum_6719372088781090",0) as numeric(12,4)) as "gross_margin",
      "questionable"."sales_item_category" as "sales_item_category",
      "questionable"."sales_item_class" as "sales_item_class",
      "questionable"."_virt_agg_grouping_1670065760616147" + "questionable"."_virt_agg_grouping_8417677704977793" as "hierarchy_level",
      "questionable"."within_subgroup_rank" as "within_subgroup_rank"
  FROM
      "questionable"
  ORDER BY
      "hierarchy_level" desc nulls first,
      "questionable"."sales_item_category" asc nulls first,
      "questionable"."within_subgroup_rank" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/9h9h)
  ```
- `trilogy run answer_3770074305.preql`

  ```text
  Resolution error in answer_3770074305.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 5). The requested concepts split into 2 disconnected subgraphs: {item.current_price, item.item_desc, item.item_id, item.item_sk, item.manufact_id}; {_virt_agg_sum_716388299314788}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_3770074305.preql`

  ```text
  Resolution error in answer_3770074305.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 12). The requested concepts split into 2 disconnected subgraphs: {item.current_price, item.item_desc, item.item_id, item.item_sk, item.manufact_id}; {_virt_agg_sum_1579400739239040}. Are you missing a join or merge statement to relate them?
  Note: the membership predicate(s) `(item.item_sk) in (inv_summary.inventory.item.item_sk)` span these subgraphs, but membership only filters rows on its left side — it does not join the two sides, so it cannot relate them for outputs or grouping. To combine values from both sides, author a query-scoped join or a merge on shared keys.
  ```
- `trilogy run answer_3770074305.preql`

  ```text
  Resolution error in answer_3770074305.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 5). The requested concepts split into 2 disconnected subgraphs: {item.current_price, item.item_desc, item.item_id, item.item_sk, item.manufact_id}; {_virt_agg_sum_716388299314788}. Are you missing a join or merge statement to relate them?
  Note: the membership predicate(s) `(item.item_sk) in (inventory.item.item_sk)` span these subgraphs, but membership only filters rows on its left side — it does not join the two sides, so it cannot relate them for outputs or grouping. To combine values from both sides, author a query-scoped join or a merge on shared keys.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_142117636.preql`

  ```text
  Resolution error in answer_142117636.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 8). The requested concepts split into 4 disconnected subgraphs: {dd.year, _inventory_stats_month_num}; {_inventory_stats_avg_qoh, _inventory_stats_std_qoh}; {_inventory_stats_it_sk}; {_inventory_stats_wh_sk}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 1 column 1686 (char 1685). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run answer_630074061.preql`

  ```text
  Syntax error in answer_630074061.preql: Undefined concept: cr.order_number. Suggestions: ['cs.order_number']
  ```
- `trilogy run --import raw.item:item select item.product_name where item.manufact_id between 1 and 500 and item.manufact in (select item2.manufact where (item2… or (item2.category = 'Electronics' and item2.color = 'mint' and item2.units = 'Gross' and item2.size = 'N/A')) order by item.product_name limit 100;`

  ```text
  Syntax error in stdin: 5 undefined concept references; fix all before re-running:
    - item2.manufact (line 2, col 96, in SELECT); did you mean: item2.units, item.manufact, item.manufact_id?
    - item2.category (line 2, col 118, in WHERE); did you mean: item2.color, item.category, item.category_id?
    - item2.color (line 2, col 147, in WHERE); did you mean: item2.category, item.color, item.class?
    - item2.units (line 2, col 171, in WHERE); did you mean: item2.size, item2.manufact, item.units, item.item_sk?
    - item2.size (line 2, col 194, in WHERE); did you mean: item2.units, item.size, item.item_sk?
  ```
- `trilogy run answer_968172492.preql`

  ```text
  Syntax error in answer_968172492.preql: 2 undefined concept references; fix all before re-running:
    - item.item_id (line 13, col 20, in SELECT); did you mean: ws.item.item_id, ws.promotion.item.item_id, item.item_sk, ws.item.item_desc?
    - item.item_sk (line 14, col 19, in WHERE); did you mean: ws.item.item_sk, ws.promotion.item.item_sk, item.item_id, ws.item.item_desc?
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe store_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_2940558602.preql`

  ```text
  Syntax error in answer_2940558602.preql: 2 undefined concept references; fix all before re-running:
    - sr.return_amount (line 17, in SELECT); did you mean: sr.return_amt, sr.return_quantity, sr.return_tax?
    - sr.return_amount (line 11, col 7, in WHERE); did you mean: sr.return_amt, sr.return_quantity, sr.return_tax?
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3560698360.preql`

  ```text
  Resolution error in answer_3560698360.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced: {store_run.item_sk, store_run.running_total, store_run.sale_date, web_run.item_sk, web_run.running_total, web_run.sale_date}; still unresolved: {local.store_running_max, local.web_running_max}
  ```
- `trilogy run answer_3560698360.preql`

  ```text
  Resolution error in answer_3560698360.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced: {store_run.item_sk, store_run.running_total, store_run.sale_date, web_run.item_sk, web_run.running_total, web_run.sale_date}; still unresolved: {local.store_running_max, local.web_running_max}
  ```
- `trilogy run answer_3560698360.preql`

  ```text
  Resolution error in answer_3560698360.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {store_daily.sale_date, web_daily.daily_total, web_daily.item_sk, web_daily.sale_date}
  ```
- `trilogy run answer_3560698360.preql`

  ```text
  Resolution error in answer_3560698360.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced: {store_run.item_sk, store_run.sale_date, web_run.daily_total}; still unresolved: {local.web_running_total}
  ```
- `trilogy run answer_71623752.preql`

  ```text
  Resolution error in answer_71623752.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 30). The requested concepts split into 2 disconnected subgraphs: {manuf_avg.avg_quarterly_sales, manuf_avg.manuf_id}; {quarterly_totals.manuf_id, quarterly_totals.total_unit_price}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_3553309440.preql --param dec_ms=1187`

  ```text
  Syntax error in answer_3553309440.preql: Unsupported datatype BIGINT for parameter dec_ms. (line 6, column 1)
  ```
- `trilogy run --import raw/store_sales:ss select ss.customer.customer_sk, sum(ss.ext_sales_price) as tot, ss.store.county, ss.store.state, ss.customer.customer…sold_date.month_seq=1187) and ss.store.county = ss.customer.customer_address.county and ss.store.state = ss.customer.customer_address.state limit 10;`

  ```text
  Syntax error in stdin: 4 undefined concept references; fix all before re-running:
    - cs.bill_customer.customer_sk (line 2, col 259, in SELECT); did you mean: ss.customer.customer_sk, ss.customer.customer_id, ss.customer.customer_address.zip?
    - cs.item.category (line 2, col 294, in WHERE); did you mean: cs.item.class, ss.item.category, ss.promotion.item.category, ss.item.category_id?
    - cs.item.class (line 2, col 323, in WHERE); did you mean: cs.item.category, ss.item.class, ss.promotion.item.class, ss.item.class_id, ss.item.color?
    - cs.sold_date.month_seq (line 2, col 353, in WHERE); did you mean: ss.date_dim.month_seq, ss.store.date_dim.month_seq, ss.promotion.end_date.month_seq, ss.customer.first_sales_date.month_seq, ss.customer.last_review_date.month_seq, ss.promotion.start_date.month_seq?
  ```
- `trilogy file read answer_3553309440.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw/catalog_sales:cs --import raw/store_sales:ss --import raw/web_sales:ws select ss.item.item_id, sum(ss.ext_sales_price) as store_tot,…_tot, sum(ws.ext_sales_price) as web_tot where ss.date_dim.week_seq = 5218 and cs.sold_date.week_seq = 5218 and ws.sold_date.week_seq = 5218 limit 5;`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 3 disconnected subgraphs: {cs.sold_date.week_seq, cat_tot}; {store_tot, ss.date_dim.week_seq, ss.item.item_id}; {web_tot, ws.sold_date.week_seq}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file write --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Rowsets: per-item totals by channel… catalog_totals.total and 1.1 * catalog_totals.total
order by store_totals.item_id, store_totals.total nulls first
limit 100;
 answer_145690531.preql`

  ```text
  trilogy file write: `--content` takes a SINGLE string argument. Your args list put 2 separate tokens after --content (treating it like a shell command). In a tool call, pass the entire file body as one string element after --content, with newlines embedded literally — e.g.
    {"args": ["file", "write", "query70.preql", "--content", "import raw.store_sales as store_sales;\n\nselect ..."]}
  Alternatively use `--escapes` with a single-line `\n`-escaped string.
  ```
- `trilogy run answer_1623435181.preql`

  ```text
  Resolution error in answer_1623435181.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {next_yr.pivoted.fri_total, next_yr.pivoted.mon_total, next_yr.pivoted.sat_total, next_yr.pivoted.sun_total, next_yr.pivoted.thu_total, next_yr.pivoted.tue_total, next_yr.pivoted.wed_total, next_yr.pivoted.weekly_sales.s_code, next_yr.pivoted.weekly_sales.s_name, next_yr.pivoted.weekly_sales.wk_seq, this_yr.pivoted.fri_total, this_yr.pivoted.mon_total, this_yr.pivoted.sat_total, this_yr.pivoted.sun_total, this_yr.pivoted.thu_total, this_yr.pivoted.tue_total, this_yr.pivoted.wed_total, this_yr.pivoted.weekly_sales.s_code, this_yr.pivoted.weekly_sales.s_name, this_yr.pivoted.weekly_sales.wk_seq}
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 42 column 12 (char 1431). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_1484301313.preql`

  ```text
  Syntax error in answer_1484301313.preql: Comparison `store_sales.promotion.channel_email = 'Y'` can never match enum field 'store_sales.promotion.channel_email', which contains only these values: 'N'. It is always false and should be removed.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Unterminated string starting at: line 1 column 64 (char 63). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run answer_3544057080.preql`

  ```text
  Syntax error in answer_3544057080.preql: Nothing was executed: parsed 10 definition statement(s) (5 imports, 3 concepts, 2 rowsets) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy run answer_3544057080.preql`

  ```text
  Syntax error in answer_3544057080.preql: ORDER BY references 'y2000.sl_00', which is not in the SELECT projection (line 97). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --y2000.sl_00 order by y2000.sl_00 asc`.
  ```
- `trilogy run answer_3544057080.preql`

  ```text
  Resolution error in answer_3544057080.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 25). The requested concepts split into 2 disconnected subgraphs: {cs.item.item_sk, item_total_cs_ext_list_price}; {item_total_cr_refund}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_1256642802.preql`

  ```text
  Resolution error in answer_1256642802.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 12). The requested concepts split into 3 disconnected subgraphs: {item.brand, item.current_price, item.item_desc, item.wholesale_cost}; {per_item_revenue_1998}; {store.store_name, store.store_sk}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_1256642802.preql`

  ```text
  Resolution error in answer_1256642802.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 7). The requested concepts split into 4 disconnected subgraphs: {dd.year}; {item.brand, item.current_price, item.item_desc, item.item_sk, item.wholesale_cost}; {_per_item_item_revenue}; {store.store_name, store.store_sk}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_1256642802.preql`

  ```text
  Resolution error in answer_1256642802.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {per_item.item_revenue, per_item.ss.item.brand, per_item.ss.item.current_price, per_item.ss.item.item_desc, per_item.ss.item.wholesale_cost, per_item.ss.store.store_name, store_avg.store_item_avg}
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/warehouse.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe time_dim`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_2874833976.preql`

  ```text
  Syntax error in answer_2874833976.preql: Undefined concept: _virt_agg_grouping_4453574046401692.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/inventory.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file write --content import raw.catalog_sales as cs;

where
  cs.sold_date.year = 1999
  and cs.ship_customer.household_demographics.buy_potential = …) > 5

select
  cs.item.item_desc as item_description,
  cs.sold_date.week_seq as week_sequence,
  count(*) as cnt
limit 10;
 answer_2091535883.preql`

  ```text
  trilogy file write: `--content` takes a SINGLE string argument. Your args list put 2 separate tokens after --content (treating it like a shell command). In a tool call, pass the entire file body as one string element after --content, with newlines embedded literally — e.g.
    {"args": ["file", "write", "query70.preql", "--content", "import raw.store_sales as store_sales;\n\nselect ..."]}
  Alternatively use `--escapes` with a single-line `\n`-escaped string.
  ```
- `trilogy database describe store`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run answer_1772060640.preql`

  ```text
  Syntax error in answer_1772060640.preql: Comparison `ss.store.county in ('Orange County', 'Bronx County', 'Franklin Parish', 'Williamson County')` matches every value of enum field 'ss.store.county', which contains only these values: 'Williamson County'. It is always true and should be removed.
  ```
- `trilogy run answer_1772060640.preql`

  ```text
  Syntax error in answer_1772060640.preql: Comparison `ss.store.county = 'Williamson County'` matches every value of enum field 'ss.store.county', which contains only these values: 'Williamson County'. It is always true and should be removed.
  ```
- `trilogy run answer_3840922834.preql`

  ```text
  Syntax error in answer_3840922834.preql: Undefined concept: local.cust_sk. Suggestions: ['web_by_cust_year.cust_sk', 'store_by_cust_year.cust_sk', 'c.customer_sk']
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 107 column 12 (char 3617). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run answer_3046445280.preql`

  ```text
  Resolution error in answer_3046445280.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 11). The requested concepts split into 2 disconnected subgraphs: {item.category, _store_ch_brand_id, _store_ch_cat_id, _store_ch_class_id, _store_ch_manu_id}; {_store_ch_net_amt, _store_ch_net_qty, _store_ch_yr}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe store_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy database describe web_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy database describe catalog_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 109 column 12 (char 3759). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_1226264875.preql`

  ```text
  Syntax error in answer_1226264875.preql: 10 undefined concept references; fix all before re-running:
    - local.cat_qty (line 74, in SELECT); did you mean: catalog_nr.cat_qty, cat_wc, cat_sp?
    - local.web_qty (line 74, in SELECT); did you mean: web_nr.web_qty, web_wc, web_sp?
    - local.cat_qty (line 78, in SELECT); did you mean: catalog_nr.cat_qty, cat_wc, cat_sp?
    - local.web_qty (line 78, in SELECT); did you mean: web_nr.web_qty, web_wc, web_sp?
    - local.cat_wc (line 79, in SELECT); did you mean: catalog_nr.cat_wc, cat_sp, cat_qty?
    - local.web_wc (line 79, in SELECT); did you mean: web_nr.web_wc, web_sp, web_qty?
    - local.cat_sp (line 80, in SELECT); did you mean: catalog_nr.cat_sp, cat_wc, cat_qty?
    - local.web_sp (line 80, in SELECT); did you mean: web_nr.web_sp, web_wc, web_qty?
    - item_sk (line 91, col 5, in ORDER BY); did you mean: ss.item.item_sk, sr.item.item_sk, cs.item.item_sk, cr.item.item_sk, ws.item.item_sk, wr.item.item_sk?
    - cust_sk (line 92, col 5, in ORDER BY); did you mean: store_nr.cust_sk, web_nr.cust_sk3, cat_sp?
  ```
- `trilogy run answer_1226264875.preql`

  ```text
  Resolution error in answer_1226264875.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {catweb_never.ot_qty, catweb_never.ot_sp, catweb_never.ot_wc, store_never.cust_sk, store_never.item_sk, store_never.st_qty, store_never.st_sp, store_never.st_wc, store_never.yr}
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_2910545909.preql`

  ```text
  Syntax error in answer_2910545909.preql: Duplicate select output for ss.ticket_number; Line: 3
  ```
- `trilogy run answer_943796012.preql`

  ```text
  Syntax error in answer_943796012.preql: Comparison `s.promotion.channel_tv = 'N'` matches every value of nullable enum field 's.promotion.channel_tv', which contains only these values: 'N'. It only excludes nulls; simplify it to 's.promotion.channel_tv is not null'.
  ```
- `trilogy run answer_943796012.preql`

  ```text
  Syntax error in answer_943796012.preql: `by rollup (…)` requires at least one aggregate (or re-aggregable pre-aggregated measure) in the select to group; found none.
  ```
- `trilogy run answer_943796012.preql`

  ```text
  Syntax error in answer_943796012.preql: 2 undefined concept references; fix all before re-running:
    - channel (line 55, col 10, in ORDER BY); did you mean: combined.channel?
    - outlet_id (line 55, col 31, in ORDER BY); did you mean: combined.outlet_id?
  ```
- `trilogy run answer_943796012.preql`

  ```text
  Resolution error in answer_943796012.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced: {local.___tvf_arm_0_channel, local.___tvf_arm_0_outlet_id}; still unresolved: {local.___tvf_arm_0_total_returns, local.___tvf_arm_0_total_sales, local.___tvf_arm_0_total_scoped_profit}
  ```
- `trilogy run answer_943796012.preql`

  ```text
  Unexpected error in answer_943796012.preql: Missing sr.item.item_sk in {'s.date_dim.date': ['hard'], 's.date_dim.date_sk': ['hard'], 's.item.current_price': ['hard'], 's.item.item_sk': ['hard'], 's.net_paid': ['hard'], 's.promotion.channel_tv': ['hard'], 's.promotion.promo_sk': ['hard'], 's.store.store_id': ['hard'], 's.store.store_sk': ['hard'], 's.ticket_number': ['hard'], 'local.___tvf_arm_0_outlet_id': []}, source map dict_keys(['s.date_dim.date', 's.date_dim.date_sk', 's.item.current_price', 's.item.item_sk', 's.net_paid', 's.promotion.channel_tv', 's.promotion.promo_sk', 's.store.store_id', 's.store.store_sk', 's.ticket_number', 'local.___tvf_arm_0_outlet_id'])
  ```
- `trilogy run answer_943796012.preql`

  ```text
  Unexpected error in answer_943796012.preql: Missing sr.item.item_sk in {'s.date_dim.date': ['hard'], 's.date_dim.date_sk': ['hard'], 's.item.current_price': ['hard'], 's.item.item_sk': ['hard'], 's.net_paid': ['hard'], 's.promotion.channel_tv': ['hard'], 's.promotion.promo_sk': ['hard'], 's.store.store_id': ['hard'], 's.store.store_sk': ['hard'], 's.ticket_number': ['hard'], 'local.___tvf_arm_0_outlet_id': []}, source map dict_keys(['s.date_dim.date', 's.date_dim.date_sk', 's.item.current_price', 's.item.item_sk', 's.net_paid', 's.promotion.channel_tv', 's.promotion.promo_sk', 's.store.store_id', 's.store.store_sk', 's.ticket_number', 'local.___tvf_arm_0_outlet_id'])
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_943796012.preql`

  ```text
  Unexpected error in answer_943796012.preql: Missing sr.item.item_sk in {'s.date_dim.date': ['hard'], 's.date_dim.date_sk': ['hard'], 's.item.current_price': ['hard'], 's.item.item_sk': ['hard'], 's.net_paid': ['hard'], 's.promotion.channel_tv': ['hard'], 's.promotion.promo_sk': ['hard'], 's.store.store_id': ['hard'], 's.store.store_sk': ['hard'], 's.ticket_number': ['hard'], 'local.___tvf_arm_0_outlet_id': []}, source map dict_keys(['s.date_dim.date', 's.date_dim.date_sk', 's.item.current_price', 's.item.item_sk', 's.net_paid', 's.promotion.channel_tv', 's.promotion.promo_sk', 's.store.store_id', 's.store.store_sk', 's.ticket_number', 'local.___tvf_arm_0_outlet_id'])
  ```
- `trilogy run answer_943796012.preql`

  ```text
  Syntax error in answer_943796012.preql: Undefined concept: local.oid. Suggestions: ['ss.oid', 'srr.oid', 'css.oid', 'crr.oid', 'wss.oid']
  ```
- `trilogy run answer_943796012.preql`

  ```text
  Syntax error in answer_943796012.preql: Missing source map entry for sr.item.item_sk with pseudonyms set(), have map: {'s.item.item_sk': {BuildDatasource(name='item', columns=[BuildColumnAssignment(alias='i_item_sk', concept=s.item.item_sk@Grain<s.item.item_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='i_item_id', concept=s.item.item_id@Grain<s.item.item_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='i_rec_start_date', concept=s.item.rec_start_date@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_rec_end_date', concept=s.item.rec_end_date@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_item_desc', concept=s.item.item_desc@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_current_price', concept=s.item.current_price@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_wholesale_cost', concept=s.item.wholesale_cost@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_brand_id', concept=s.item.brand_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_brand', concept=s.item.brand@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_class_id', concept=s.item.class_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_class', concept=s.item.class@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_category_id', concept=s.item.category_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_category', concept=s.item.category@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manufact_id', concept=s.item.manufact_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manufact', concept=s.item.manufact@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_size', concept=s.item.size@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_formulation', concept=s.item.formulation@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_color', concept=s.item.color@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_units', concept=s.item.units@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_container', concept=s.item.container@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manager_id', concept=s.item.manager_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_product_name', concept=s.item.product_name@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='item', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'s.item.item_sk'}, where_clause=None, _str='Grain<s.item.item_sk>', _str_no_condition='Grain<s.item.item_sk>', abstract=False), namespace='s.item', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses=set())}, 's.item.item_id': {BuildDatasource(name='item', columns=[BuildColumnAssignment(alias='i_item_sk', concept=s.item.item_sk@Grain<s.item.item_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='i_item_id', concept=s.item.item_id@Grain<s.item.item_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='i_rec_start_date', concept=s.item.rec_start_date@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_rec_end_date', concept=s.item.rec_end_date@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_item_desc', concept=s.item.item_desc@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_current_price', concept=s.item.current_price@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_wholesale_cost', concept=s.item.wholesale_cost@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_brand_id', concept=s.item.brand_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_brand', concept=s.item.brand@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_class_id', concept=s.item.class_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_class', concept=s.item.class@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_category_id', concept=s.item.category_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_category', concept=s.item.category@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manufact_id', concept=s.item.manufact_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manufact', concept=s.item.manufact@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_size', concept=s.item.size@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_formulation', concept=s.item.formulation@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_color', concept=s.item.color@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_units', concept=s.item.units@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_container', concept=s.item.container@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manager_id', concept=s.item.manager_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_product_name', concept=s.item.product_name@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='item', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'s.item.item_sk'}, where_clause=None, _str='Grain<s.item.item_sk>', _str_no_condition='Grain<s.item.item_sk>', abstract=False), namespace='s.item', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses=set())}, 's.item.rec_start_date': {BuildDatasource(name='item', columns=[BuildColumnAssignment(alias='i_item_sk', concept=s.item.item_sk@Grain<s.item.item_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='i_item_id', concept=s.item.item_id@Grain<s.item.item_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='i_rec_start_date', concept=s.item.rec_start_date@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_rec_end_date', concept=s.item.rec_end_date@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_item_desc', concept=s.item.item_desc@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_current_price', concept=s.item.current_price@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_wholesale_cost', concept=s.item.wholesale_cost@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_brand_id', concept=s.item.brand_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_brand', concept=s.item.brand@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_class_id', concept=s.item.class_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_class', concept=s.item.class@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_category_id', concept=s.item.category_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_category', concept=s.item.category@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manufact_id', concept=s.item.manufact_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manufact', concept=s.item.manufact@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_size', concept=s.item.size@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_formulation', concept=s.item.formulation@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_color', concept=s.item.color@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_units', concept=s.item.units@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_container', concept=s.item.container@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manager_id', concept=s.item.manager_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_product_name', concept=s.item.product_name@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='item', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'s.item.item_sk'}, where_clause=None, _str='Grain<s.item.item_sk>', _str_no_condition='Grain<s.item.item_sk>', abstract=False), namespace='s.item', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses=set())}, 's.item.rec_end_date': {BuildDatasource(name='item', columns=[BuildColumnAssignment(alias='i_item_sk', concept=s.item.item_sk@Grain<s.item.item_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='i_item_id', concept=s.item.item_id@Grain<s.item.item_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='i_rec_start_date', concept=s.item.rec_start_date@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_rec_end_date', concept=s.item.rec_end_date@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_item_desc', concept=s.item.item_desc@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_current_price', concept=s.item.current_price@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_wholesale_cost', concept=s.item.wholesale_cost@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_brand_id', concept=s.item.brand_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_brand', concept=s.item.brand@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_class_id', concept=s.item.class_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_class', concept=s.item.class@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_category_id', concept=s.item.category_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_category', concept=s.item.category@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manufact_id', concept=s.item.manufact_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manufact', concept=s.item.manufact@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_size', concept=s.item.size@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_formulation', concept=s.item.formulation@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_color', concept=s.item.color@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_units', concept=s.item.units@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_container', concept=s.item.container@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manager_id', concept=s.item.manager_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_product_name', concept=s.item.product_name@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='item', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'s.item.item_sk'}, where_clause=None, _str='Grain<s.item.item_sk>', _str_no_condition='Grain<s.item.item_sk>', abstract=False), namespace='s.item', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses=set())}, 's.item.item_desc': {BuildDatasource(name='item', columns=[BuildColumnAssignment(alias='i_item_sk', concept=s.item.item_sk@Grain<s.item.item_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='i_item_id', concept=s.item.item_id@Grain<s.item.item_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='i_rec_start_date', concept=s.item.rec_start_date@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_rec_end_date', concept=s.item.rec_end_date@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_item_desc', concept=s.item.item_desc@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_current_price', concept=s.item.current_price@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_wholesale_cost', concept=s.item.wholesale_cost@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_brand_id', concept=s.item.brand_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_brand', concept=s.item.brand@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_class_id', concept=s.item.class_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_class', concept=s.item.class@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_category_id', concept=s.item.category_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_category', concept=s.item.category@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manufact_id', concept=s.item.manufact_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manufact', concept=s.item.manufact@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_size', concept=s.item.size@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_formulation', concept=s.item.formulation@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_color', concept=s.item.color@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_units', concept=s.item.units@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_container', concept=s.item.container@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manager_id', concept=s.item.manager_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_product_name', concept=s.item.product_name@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='item', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'s.item.item_sk'}, where_clause=None, _str='Grain<s.item.item_sk>', _str_no_condition='Grain<s.item.item_sk>', abstract=False), namespace='s.item', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses=set())}, 's.item.current_price': {BuildDatasource(name='item', columns=[BuildColumnAssignment(alias='i_item_sk', concept=s.item.item_sk@Grain<s.item.item_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='i_item_id', concept=s.item.item_id@Grain<s.item.item_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='i_rec_start_date', concept=s.item.rec_start_date@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_rec_end_date', concept=s.item.rec_end_date@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_item_desc', concept=s.item.item_desc@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_current_price', concept=s.item.current_price@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_wholesale_cost', concept=s.item.wholesale_cost@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_brand_id', concept=s.item.brand_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_brand', concept=s.item.brand@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_class_id', concept=s.item.class_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_class', concept=s.item.class@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_category_id', concept=s.item.category_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_category', concept=s.item.category@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manufact_id', concept=s.item.manufact_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manufact', concept=s.item.manufact@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_size', concept=s.item.size@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_formulation', concept=s.item.formulation@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_color', concept=s.item.color@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_units', concept=s.item.units@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_container', concept=s.item.container@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manager_id', concept=s.item.manager_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_product_name', concept=s.item.product_name@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='item', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'s.item.item_sk'}, where_clause=None, _str='Grain<s.item.item_sk>', _str_no_condition='Grain<s.item.item_sk>', abstract=False), namespace='s.item', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses=set())}, 's.item.wholesale_cost': {BuildDatasource(name='item', columns=[BuildColumnAssignment(alias='i_item_sk', concept=s.item.item_sk@Grain<s.item.item_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='i_item_id', concept=s.item.item_id@Grain<s.item.item_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='i_rec_start_date', concept=s.item.rec_start_date@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_rec_end_date', concept=s.item.rec_end_date@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_item_desc', concept=s.item.item_desc@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_current_price', concept=s.item.current_price@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_wholesale_cost', concept=s.item.wholesale_cost@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_brand_id', concept=s.item.brand_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_brand', concept=s.item.brand@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_class_id', concept=s.item.class_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_class', concept=s.item.class@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_category_id', concept=s.item.category_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_category', concept=s.item.category@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manufact_id', concept=s.item.manufact_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manufact', concept=s.item.manufact@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_size', concept=s.item.size@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_formulation', concept=s.item.formulation@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_color', concept=s.item.color@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_units', concept=s.item.units@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_container', concept=s.item.container@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manager_id', concept=s.item.manager_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_product_name', concept=s.item.product_name@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='item', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'s.item.item_sk'}, where_clause=None, _str='Grain<s.item.item_sk>', _str_no_condition='Grain<s.item.item_sk>', abstract=False), namespace='s.item', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses=set())}, 's.item.brand_id': {BuildDatasource(name='item', columns=[BuildColumnAssignment(alias='i_item_sk', concept=s.item.item_sk@Grain<s.item.item_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='i_item_id', concept=s.item.item_id@Grain<s.item.item_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='i_rec_start_date', concept=s.item.rec_start_date@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_rec_end_date', concept=s.item.rec_end_date@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_item_desc', concept=s.item.item_desc@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_current_price', concept=s.item.current_price@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_wholesale_cost', concept=s.item.wholesale_cost@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_brand_id', concept=s.item.brand_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_brand', concept=s.item.brand@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_class_id', concept=s.item.class_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_class', concept=s.item.class@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_category_id', concept=s.item.category_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_category', concept=s.item.category@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manufact_id', concept=s.item.manufact_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manufact', concept=s.item.manufact@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_size', concept=s.item.size@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_formulation', concept=s.item.formulation@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_color', concept=s.item.color@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_units', concept=s.item.units@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_container', concept=s.item.container@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manager_id', concept=s.item.manager_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_product_name', concept=s.item.product_name@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='item', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'s.item.item_sk'}, where_clause=None, _str='Grain<s.item.item_sk>', _str_no_condition='Grain<s.item.item_sk>', abstract=False), namespace='s.item', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses=set())}, 's.item.brand': {BuildDatasource(name='item', columns=[BuildColumnAssignment(alias='i_item_sk', concept=s.item.item_sk@Grain<s.item.item_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='i_item_id', concept=s.item.item_id@Grain<s.item.item_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='i_rec_start_date', concept=s.item.rec_start_date@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_rec_end_date', concept=s.item.rec_end_date@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_item_desc', concept=s.item.item_desc@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_current_price', concept=s.item.current_price@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_wholesale_cost', concept=s.item.wholesale_cost@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_brand_id', concept=s.item.brand_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_brand', concept=s.item.brand@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_class_id', concept=s.item.class_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_class', concept=s.item.class@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_category_id', concept=s.item.category_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_category', concept=s.item.category@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manufact_id', concept=s.item.manufact_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manufact', concept=s.item.manufact@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_size', concept=s.item.size@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_formulation', concept=s.item.formulation@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_color', concept=s.item.color@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_units', concept=s.item.units@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_container', concept=s.item.container@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manager_id', concept=s.item.manager_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_product_name', concept=s.item.product_name@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='item', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'s.item.item_sk'}, where_clause=None, _str='Grain<s.item.item_sk>', _str_no_condition='Grain<s.item.item_sk>', abstract=False), namespace='s.item', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses=set())}, 's.item.class_id': {BuildDatasource(name='item', columns=[BuildColumnAssignment(alias='i_item_sk', concept=s.item.item_sk@Grain<s.item.item_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='i_item_id', concept=s.item.item_id@Grain<s.item.item_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='i_rec_start_date', concept=s.item.rec_start_date@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_rec_end_date', concept=s.item.rec_end_date@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_item_desc', concept=s.item.item_desc@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_current_price', concept=s.item.current_price@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_wholesale_cost', concept=s.item.wholesale_cost@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_brand_id', concept=s.item.brand_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_brand', concept=s.item.brand@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_class_id', concept=s.item.class_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_class', concept=s.item.class@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_category_id', concept=s.item.category_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_category', concept=s.item.category@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manufact_id', concept=s.item.manufact_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manufact', concept=s.item.manufact@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_size', concept=s.item.size@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_formulation', concept=s.item.formulation@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_color', concept=s.item.color@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_units', concept=s.item.units@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_container', concept=s.item.container@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manager_id', concept=s.item.manager_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_product_name', concept=s.item.product_name@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='item', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'s.item.item_sk'}, where_clause=None, _str='Grain<s.item.item_sk>', _str_no_condition='Grain<s.item.item_sk>', abstract=False), namespace='s.item', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses=set())}, 's.item.class': {BuildDatasource(name='item', columns=[BuildColumnAssignment(alias='i_item_sk', concept=s.item.item_sk@Grain<s.item.item_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='i_item_id', concept=s.item.item_id@Grain<s.item.item_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='i_rec_start_date', concept=s.item.rec_start_date@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_rec_end_date', concept=s.item.rec_end_date@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_item_desc', concept=s.item.item_desc@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_current_price', concept=s.item.current_price@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_wholesale_cost', concept=s.item.wholesale_cost@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_brand_id', concept=s.item.brand_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_brand', concept=s.item.brand@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_class_id', concept=s.item.class_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_class', concept=s.item.class@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_category_id', concept=s.item.category_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_category', concept=s.item.category@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manufact_id', concept=s.item.manufact_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manufact', concept=s.item.manufact@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_size', concept=s.item.size@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_formulation', concept=s.item.formulation@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_color', concept=s.item.color@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_units', concept=s.item.units@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_container', concept=s.item.container@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manager_id', concept=s.item.manager_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_product_name', concept=s.item.product_name@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='item', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'s.item.item_sk'}, where_clause=None, _str='Grain<s.item.item_sk>', _str_no_condition='Grain<s.item.item_sk>', abstract=False), namespace='s.item', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses=set())}, 's.item.category_id': {BuildDatasource(name='item', columns=[BuildColumnAssignment(alias='i_item_sk', concept=s.item.item_sk@Grain<s.item.item_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='i_item_id', concept=s.item.item_id@Grain<s.item.item_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='i_rec_start_date', concept=s.item.rec_start_date@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_rec_end_date', concept=s.item.rec_end_date@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_item_desc', concept=s.item.item_desc@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_current_price', concept=s.item.current_price@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_wholesale_cost', concept=s.item.wholesale_cost@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_brand_id', concept=s.item.brand_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_brand', concept=s.item.brand@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_class_id', concept=s.item.class_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_class', concept=s.item.class@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_category_id', concept=s.item.category_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_category', concept=s.item.category@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manufact_id', concept=s.item.manufact_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manufact', concept=s.item.manufact@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_size', concept=s.item.size@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_formulation', concept=s.item.formulation@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_color', concept=s.item.color@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_units', concept=s.item.units@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_container', concept=s.item.container@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manager_id', concept=s.item.manager_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_product_name', concept=s.item.product_name@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='item', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'s.item.item_sk'}, where_clause=None, _str='Grain<s.item.item_sk>', _str_no_condition='Grain<s.item.item_sk>', abstract=False), namespace='s.item', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses=set())}, 's.item.category': {BuildDatasource(name='item', columns=[BuildColumnAssignment(alias='i_item_sk', concept=s.item.item_sk@Grain<s.item.item_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='i_item_id', concept=s.item.item_id@Grain<s.item.item_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='i_rec_start_date', concept=s.item.rec_start_date@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_rec_end_date', concept=s.item.rec_end_date@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_item_desc', concept=s.item.item_desc@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_current_price', concept=s.item.current_price@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_wholesale_cost', concept=s.item.wholesale_cost@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_brand_id', concept=s.item.brand_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_brand', concept=s.item.brand@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_class_id', concept=s.item.class_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_class', concept=s.item.class@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_category_id', concept=s.item.category_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_category', concept=s.item.category@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manufact_id', concept=s.item.manufact_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manufact', concept=s.item.manufact@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_size', concept=s.item.size@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_formulation', concept=s.item.formulation@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_color', concept=s.item.color@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_units', concept=s.item.units@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_container', concept=s.item.container@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manager_id', concept=s.item.manager_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_product_name', concept=s.item.product_name@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='item', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'s.item.item_sk'}, where_clause=None, _str='Grain<s.item.item_sk>', _str_no_condition='Grain<s.item.item_sk>', abstract=False), namespace='s.item', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses=set())}, 's.item.manufact_id': {BuildDatasource(name='item', columns=[BuildColumnAssignment(alias='i_item_sk', concept=s.item.item_sk@Grain<s.item.item_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='i_item_id', concept=s.item.item_id@Grain<s.item.item_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='i_rec_start_date', concept=s.item.rec_start_date@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_rec_end_date', concept=s.item.rec_end_date@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_item_desc', concept=s.item.item_desc@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_current_price', concept=s.item.current_price@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_wholesale_cost', concept=s.item.wholesale_cost@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_brand_id', concept=s.item.brand_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_brand', concept=s.item.brand@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_class_id', concept=s.item.class_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_class', concept=s.item.class@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_category_id', concept=s.item.category_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_category', concept=s.item.category@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manufact_id', concept=s.item.manufact_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manufact', concept=s.item.manufact@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_size', concept=s.item.size@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_formulation', concept=s.item.formulation@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_color', concept=s.item.color@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_units', concept=s.item.units@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_container', concept=s.item.container@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manager_id', concept=s.item.manager_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_product_name', concept=s.item.product_name@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='item', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'s.item.item_sk'}, where_clause=None, _str='Grain<s.item.item_sk>', _str_no_condition='Grain<s.item.item_sk>', abstract=False), namespace='s.item', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses=set())}, 's.item.manufact': {BuildDatasource(name='item', columns=[BuildColumnAssignment(alias='i_item_sk', concept=s.item.item_sk@Grain<s.item.item_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='i_item_id', concept=s.item.item_id@Grain<s.item.item_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='i_rec_start_date', concept=s.item.rec_start_date@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_rec_end_date', concept=s.item.rec_end_date@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_item_desc', concept=s.item.item_desc@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_current_price', concept=s.item.current_price@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_wholesale_cost', concept=s.item.wholesale_cost@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_brand_id', concept=s.item.brand_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_brand', concept=s.item.brand@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_class_id', concept=s.item.class_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_class', concept=s.item.class@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_category_id', concept=s.item.category_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_category', concept=s.item.category@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manufact_id', concept=s.item.manufact_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manufact', concept=s.item.manufact@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_size', concept=s.item.size@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_formulation', concept=s.item.formulation@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_color', concept=s.item.color@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_units', concept=s.item.units@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_container', concept=s.item.container@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manager_id', concept=s.item.manager_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_product_name', concept=s.item.product_name@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='item', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'s.item.item_sk'}, where_clause=None, _str='Grain<s.item.item_sk>', _str_no_condition='Grain<s.item.item_sk>', abstract=False), namespace='s.item', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses=set())}, 's.item.size': {BuildDatasource(name='item', columns=[BuildColumnAssignment(alias='i_item_sk', concept=s.item.item_sk@Grain<s.item.item_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='i_item_id', concept=s.item.item_id@Grain<s.item.item_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='i_rec_start_date', concept=s.item.rec_start_date@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_rec_end_date', concept=s.item.rec_end_date@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_item_desc', concept=s.item.item_desc@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_current_price', concept=s.item.current_price@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_wholesale_cost', concept=s.item.wholesale_cost@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_brand_id', concept=s.item.brand_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_brand', concept=s.item.brand@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_class_id', concept=s.item.class_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_class', concept=s.item.class@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_category_id', concept=s.item.category_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_category', concept=s.item.category@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manufact_id', concept=s.item.manufact_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manufact', concept=s.item.manufact@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_size', concept=s.item.size@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_formulation', concept=s.item.formulation@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_color', concept=s.item.color@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_units', concept=s.item.units@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_container', concept=s.item.container@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manager_id', concept=s.item.manager_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_product_name', concept=s.item.product_name@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='item', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'s.item.item_sk'}, where_clause=None, _str='Grain<s.item.item_sk>', _str_no_condition='Grain<s.item.item_sk>', abstract=False), namespace='s.item', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses=set())}, 's.item.formulation': {BuildDatasource(name='item', columns=[BuildColumnAssignment(alias='i_item_sk', concept=s.item.item_sk@Grain<s.item.item_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='i_item_id', concept=s.item.item_id@Grain<s.item.item_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='i_rec_start_date', concept=s.item.rec_start_date@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_rec_end_date', concept=s.item.rec_end_date@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_item_desc', concept=s.item.item_desc@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_current_price', concept=s.item.current_price@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_wholesale_cost', concept=s.item.wholesale_cost@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_brand_id', concept=s.item.brand_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_brand', concept=s.item.brand@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_class_id', concept=s.item.class_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_class', concept=s.item.class@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_category_id', concept=s.item.category_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_category', concept=s.item.category@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manufact_id', concept=s.item.manufact_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manufact', concept=s.item.manufact@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_size', concept=s.item.size@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_formulation', concept=s.item.formulation@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_color', concept=s.item.color@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_units', concept=s.item.units@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_container', concept=s.item.container@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manager_id', concept=s.item.manager_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_product_name', concept=s.item.product_name@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='item', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'s.item.item_sk'}, where_clause=None, _str='Grain<s.item.item_sk>', _str_no_condition='Grain<s.item.item_sk>', abstract=False), namespace='s.item', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses=set())}, 's.item.color': {BuildDatasource(name='item', columns=[BuildColumnAssignment(alias='i_item_sk', concept=s.item.item_sk@Grain<s.item.item_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='i_item_id', concept=s.item.item_id@Grain<s.item.item_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='i_rec_start_date', concept=s.item.rec_start_date@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_rec_end_date', concept=s.item.rec_end_date@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_item_desc', concept=s.item.item_desc@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_current_price', concept=s.item.current_price@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_wholesale_cost', concept=s.item.wholesale_cost@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_brand_id', concept=s.item.brand_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_brand', concept=s.item.brand@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_class_id', concept=s.item.class_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_class', concept=s.item.class@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_category_id', concept=s.item.category_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_category', concept=s.item.category@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manufact_id', concept=s.item.manufact_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manufact', concept=s.item.manufact@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_size', concept=s.item.size@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_formulation', concept=s.item.formulation@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_color', concept=s.item.color@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_units', concept=s.item.units@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_container', concept=s.item.container@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manager_id', concept=s.item.manager_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_product_name', concept=s.item.product_name@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='item', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'s.item.item_sk'}, where_clause=None, _str='Grain<s.item.item_sk>', _str_no_condition='Grain<s.item.item_sk>', abstract=False), namespace='s.item', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses=set())}, 's.item.units': {BuildDatasource(name='item', columns=[BuildColumnAssignment(alias='i_item_sk', concept=s.item.item_sk@Grain<s.item.item_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='i_item_id', concept=s.item.item_id@Grain<s.item.item_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='i_rec_start_date', concept=s.item.rec_start_date@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_rec_end_date', concept=s.item.rec_end_date@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_item_desc', concept=s.item.item_desc@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_current_price', concept=s.item.current_price@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_wholesale_cost', concept=s.item.wholesale_cost@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_brand_id', concept=s.item.brand_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_brand', concept=s.item.brand@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_class_id', concept=s.item.class_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_class', concept=s.item.class@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_category_id', concept=s.item.category_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_category', concept=s.item.category@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manufact_id', concept=s.item.manufact_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manufact', concept=s.item.manufact@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_size', concept=s.item.size@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_formulation', concept=s.item.formulation@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_color', concept=s.item.color@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_units', concept=s.item.units@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_container', concept=s.item.container@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manager_id', concept=s.item.manager_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_product_name', concept=s.item.product_name@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='item', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'s.item.item_sk'}, where_clause=None, _str='Grain<s.item.item_sk>', _str_no_condition='Grain<s.item.item_sk>', abstract=False), namespace='s.item', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses=set())}, 's.item.container': {BuildDatasource(name='item', columns=[BuildColumnAssignment(alias='i_item_sk', concept=s.item.item_sk@Grain<s.item.item_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='i_item_id', concept=s.item.item_id@Grain<s.item.item_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='i_rec_start_date', concept=s.item.rec_start_date@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_rec_end_date', concept=s.item.rec_end_date@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_item_desc', concept=s.item.item_desc@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_current_price', concept=s.item.current_price@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_wholesale_cost', concept=s.item.wholesale_cost@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_brand_id', concept=s.item.brand_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_brand', concept=s.item.brand@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_class_id', concept=s.item.class_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_class', concept=s.item.class@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_category_id', concept=s.item.category_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_category', concept=s.item.category@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manufact_id', concept=s.item.manufact_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manufact', concept=s.item.manufact@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_size', concept=s.item.size@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_formulation', concept=s.item.formulation@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_color', concept=s.item.color@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_units', concept=s.item.units@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_container', concept=s.item.container@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manager_id', concept=s.item.manager_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_product_name', concept=s.item.product_name@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='item', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'s.item.item_sk'}, where_clause=None, _str='Grain<s.item.item_sk>', _str_no_condition='Grain<s.item.item_sk>', abstract=False), namespace='s.item', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses=set())}, 's.item.manager_id': {BuildDatasource(name='item', columns=[BuildColumnAssignment(alias='i_item_sk', concept=s.item.item_sk@Grain<s.item.item_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='i_item_id', concept=s.item.item_id@Grain<s.item.item_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='i_rec_start_date', concept=s.item.rec_start_date@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_rec_end_date', concept=s.item.rec_end_date@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_item_desc', concept=s.item.item_desc@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_current_price', concept=s.item.current_price@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_wholesale_cost', concept=s.item.wholesale_cost@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_brand_id', concept=s.item.brand_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_brand', concept=s.item.brand@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_class_id', concept=s.item.class_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_class', concept=s.item.class@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_category_id', concept=s.item.category_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_category', concept=s.item.category@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manufact_id', concept=s.item.manufact_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manufact', concept=s.item.manufact@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_size', concept=s.item.size@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_formulation', concept=s.item.formulation@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_color', concept=s.item.color@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_units', concept=s.item.units@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_container', concept=s.item.container@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manager_id', concept=s.item.manager_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_product_name', concept=s.item.product_name@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='item', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'s.item.item_sk'}, where_clause=None, _str='Grain<s.item.item_sk>', _str_no_condition='Grain<s.item.item_sk>', abstract=False), namespace='s.item', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses=set())}, 's.item.product_name': {BuildDatasource(name='item', columns=[BuildColumnAssignment(alias='i_item_sk', concept=s.item.item_sk@Grain<s.item.item_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='i_item_id', concept=s.item.item_id@Grain<s.item.item_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='i_rec_start_date', concept=s.item.rec_start_date@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_rec_end_date', concept=s.item.rec_end_date@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_item_desc', concept=s.item.item_desc@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_current_price', concept=s.item.current_price@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_wholesale_cost', concept=s.item.wholesale_cost@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_brand_id', concept=s.item.brand_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_brand', concept=s.item.brand@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_class_id', concept=s.item.class_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_class', concept=s.item.class@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_category_id', concept=s.item.category_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_category', concept=s.item.category@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manufact_id', concept=s.item.manufact_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manufact', concept=s.item.manufact@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_size', concept=s.item.size@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_formulation', concept=s.item.formulation@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_color', concept=s.item.color@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_units', concept=s.item.units@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_container', concept=s.item.container@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_manager_id', concept=s.item.manager_id@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='i_product_name', concept=s.item.product_name@Grain<s.item.item_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='item', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'s.item.item_sk'}, where_clause=None, _str='Grain<s.item.item_sk>', _str_no_condition='Grain<s.item.item_sk>', abstract=False), namespace='s.item', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses=set())}}
  ```
- `trilogy run answer_426809370.preql`

  ```text
  Syntax error in answer_426809370.preql: Undefined concept: cr.returning_customer. Suggestions: ['cr.returning_customer.customer_sk', 'cr.returning_customer.customer_id', 'cr.returning_customer.salutation', 'cr.returning_customer.first_name', 'cr.returning_customer.last_name', 'cr.returning_customer.preferred_cust_flag']
  ```
- `trilogy file read answer_426809370.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_4111870542.preql`

  ```text
  Resolution error in answer_4111870542.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {catalog_agg.catalog_qty, store_agg.item_id, store_agg.store_qty, web_agg.web_qty}
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_2852230229.preql`

  ```text
  Import error in answer_2852230229.preql: Unable to import '.\raw\sold_date.preql': [Errno 2] No such file or directory: '.\\raw\\sold_date.preql'.
  ```
- `trilogy run answer_2852230229.preql`

  ```text
  Syntax error in answer_2852230229.preql: Undefined concept: _virt_agg_grouping_7561668700388452.
  ```
- `trilogy run answer_2852230229.preql`

  ```text
  Resolution error in answer_2852230229.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {item.category, item.class, hierarchy_level}; {total_net_paid, ws.sold_date.year}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_2852230229.preql`

  ```text
  Syntax error in answer_2852230229.preql: Undefined concept: _virt_agg_grouping_164218295708188.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_32504671.preql`

  ```text
  Syntax error in answer_32504671.preql: Comparison `store_sales.household_demographics.vehicle_count <= 6` matches every value of enum field 'store_sales.household_demographics.vehicle_count', which contains only these values: -1, 0, 1, 2, 3, 4. It is always true and should be removed.
  ```
- `trilogy run answer_32504671.preql`

  ```text
  Syntax error in answer_32504671.preql: 16 undefined concept references; fix all before re-running:
    - item.item_sk (line 17, in SELECT); did you mean: store_sales.item.item_sk, store_sales.promotion.item.item_sk?
    - local.ticket_number (line 17, in SELECT); did you mean: store_sales.ticket_number?
    - item.item_sk (line 18, in SELECT); did you mean: store_sales.item.item_sk, store_sales.promotion.item.item_sk?
    - local.ticket_number (line 18, in SELECT); did you mean: store_sales.ticket_number?
    - item.item_sk (line 19, in SELECT); did you mean: store_sales.item.item_sk, store_sales.promotion.item.item_sk?
    - local.ticket_number (line 19, in SELECT); did you mean: store_sales.ticket_number?
    - item.item_sk (line 20, in SELECT); did you mean: store_sales.item.item_sk, store_sales.promotion.item.item_sk?
    - local.ticket_number (line 20, in SELECT); did you mean: store_sales.ticket_number?
    - item.item_sk (line 21, in SELECT); did you mean: store_sales.item.item_sk, store_sales.promotion.item.item_sk?
    - local.ticket_number (line 21, in SELECT); did you mean: store_sales.ticket_number?
    - item.item_sk (line 22, in SELECT); did you mean: store_sales.item.item_sk, store_sales.promotion.item.item_sk?
    - local.ticket_number (line 22, in SELECT); did you mean: store_sales.ticket_number?
    - item.item_sk (line 23, in SELECT); did you mean: store_sales.item.item_sk, store_sales.promotion.item.item_sk?
    - local.ticket_number (line 23, in SELECT); did you mean: store_sales.ticket_number?
    - item.item_sk (line 24, in SELECT); did you mean: store_sales.item.item_sk, store_sales.promotion.item.item_sk?
    - local.ticket_number (line 24, in SELECT); did you mean: store_sales.ticket_number?
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 40 column 12 (char 1430). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read answer_1827331030.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3281742578.preql`

  ```text
  Unexpected error in answer_3281742578.preql: Could not render the query: Missing source CTE for wr.order_number. A planned reference has no backing source CTE -- typically an unsupported cross-rowset or membership shape the planner could not wire. Review the rowset/join structure (or file an issue if the query looks valid).

  Full SQL with sentinel(s):

  WITH
  cheerful as (
  SELECT
      "ws_web_sales"."ws_order_number" as "ws_order_number",
      "ws_web_sales"."ws_warehouse_sk" as "ws_warehouse_warehouse_sk"
  FROM
      "web_sales" as "ws_web_sales"
  GROUP BY
      1,
      2),
  quizzical as (
  SELECT
      "wr_web_returns"."wr_order_number" as "wr_order_number"
  FROM
      "web_returns" as "wr_web_returns"
  GROUP BY
      1),
  thoughtful as (
  SELECT
      "cheerful"."ws_order_number" as "ws_order_number",
      (exists (select 1 from INVALID_REFERENCE_BUG<Missing source CTE for wr.order_number> where INVALID_REFERENCE_BUG<Missing source CTE for wr.order_number>."wr_order_number" is not distinct from "cheerful"."ws_order_number")) as "order_has_return"
  FROM
      "cheerful"
  GROUP BY
      1,
      2
  HAVING
      count(distinct "cheerful"."ws_warehouse_warehouse_sk") > 1
  ),
  cooperative as (
  SELECT
      "thoughtful"."ws_order_number" as "ws_order_number"
  FROM
      "thoughtful"
  WHERE
      coalesce("thoughtful"."order_has_return",False) = False

  GROUP BY
      1),
  abundant as (
  SELECT
      "cooperative"."ws_order_number" as "candidate_orders_ws_order_number"
  FROM
      "cooperative"),
  juicy as (
  SELECT
      "ws_web_sales"."ws_order_number" as "ws_order_number"
  FROM
      "web_sales" as "ws_web_sales"
      INNER JOIN "web_site" as "ws_web_site_web_site" on "ws_web_sales"."ws_web_site_sk" = "ws_web_site_web_site"."web_site_sk"
      INNER JOIN "date_dim" as "ws_ship_date_date_dim" on "ws_web_sales"."ws_ship_date_sk" = "ws_ship_date_date_dim"."d_date_sk"
      INNER JOIN "customer_address" as "ws_ship_addr_customer_address" on "ws_web_sales"."ws_ship_addr_sk" = "ws_ship_addr_customer_address"."ca_address_sk"
  WHERE
      "ws_web_site_web_site"."web_company_name" = 'pri' and "ws_ship_date_date_dim"."d_date" BETWEEN date '1999-02-01' AND date '1999-04-02' and "ws_ship_addr_customer_address"."ca_state" = 'IL' and exists (select 1 from abundant where abundant."candidate_orders_ws_order_number" is not distinct from "ws_web_sales"."ws_order_number")

  GROUP BY
      1),
  sparkling as (
  SELECT
      "ws_web_sales"."ws_ext_ship_cost" as "ws_ext_ship_cost",
      "ws_web_sales"."ws_net_profit" as "ws_net_profit"
  FROM
      "web_sales" as "ws_web_sales"
      INNER JOIN "web_site" as "ws_web_site_web_site" on "ws_web_sales"."ws_web_site_sk" = "ws_web_site_web_site"."web_site_sk"
      INNER JOIN "date_dim" as "ws_ship_date_date_dim" on "ws_web_sales"."ws_ship_date_sk" = "ws_ship_date_date_dim"."d_date_sk"
      INNER JOIN "customer_address" as "ws_ship_addr_customer_address" on "ws_web_sales"."ws_ship_addr_sk" = "ws_ship_addr_customer_address"."ca_address_sk"
  WHERE
      exists (select 1 from abundant where abundant."candidate_orders_ws_order_number" is not distinct from "ws_web_sales"."ws_order_number") and "ws_ship_date_date_dim"."d_date" BETWEEN date '1999-02-01' AND date '1999-04-02' and "ws_ship_addr_customer_address"."ca_state" = 'IL' and "ws_web_site_web_site"."web_company_name" = 'pri'

  GROUP BY
      1,
      2,
      "ws_web_sales"."ws_item_sk",
      "ws_web_sales"."ws_order_number"),
  young as (
  SELECT
      count(md5(CONCAT_WS('', coalesce(cast("juicy"."ws_order_number" as string),'
  ')))) as "order_count"
  FROM
      "juicy"),
  abhorrent as (
  SELECT
      sum("sparkling"."ws_ext_ship_cost") as "total_ext_ship_cost",
      sum("sparkling"."ws_net_profit") as "total_net_profit"
  FROM
      "sparkling")
  SELECT
      coalesce("young"."order_count",0) as "order_count",
      "abhorrent"."total_ext_ship_cost" as "total_ext_ship_cost",
      "abhorrent"."total_net_profit" as "total_net_profit"
  FROM
      "young"
      FULL JOIN "abhorrent" on 1=1
  ORDER BY
      coalesce("young"."order_count",0) asc
  LIMIT (100)
  ```
- `trilogy run answer_569612608.preql`

  ```text
  Syntax error in answer_569612608.preql: Undefined concept: web_returns.order_number. Suggestions: ['ws.order_number', 'eligible_orders.ws.order_number']
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy file write answer_1858999935.preql --content import raw.catalog_sales as catalog;
import raw.web_sales as web;

with combined as union(
    (select c…) as sales)
) -> (ws, dow, sales)

select combined.ws, combined.dow, sum(combined.sales) as total_sales
order by combined.ws, combined.dow
limit 20;
`

  ```text
  refused to write 'answer_1858999935.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...s sales) ) -> (ws, dow, sales) ???  select combined.ws, combined...

  Write stats: received 433 chars / 433 bytes; tail: …'as total_sales\\norder by combined.ws, combined.dow\\nlimit 20;\\n'.
  ```
- `trilogy file write answer_1858999935.preql --content import raw.catalog_sales as catalog;
import raw.web_sales as web;
import raw.date_dim as d;

# Combined … 0, 53) over (partition by weekly_sales.dow order by weekly_sales.ws), 2) as ratio_sun
where weekly_sales.ws in ws_2001.ws
order by weekly_sales.ws;
`

  ```text
  refused to write 'answer_1858999935.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...n 2001 with ws_2001 as select ??? distinct d.week_seq as ws wher...

  Write stats: received 974 chars / 974 bytes; tail: …'ere weekly_sales.ws in ws_2001.ws\\norder by weekly_sales.ws;\\n'.
  ```
- `trilogy file write answer_3863442186.preql -e -c import raw.store_sales as s;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Compute annual c…store_annual.first_name asc nulls first,
    store_annual.last_name asc nulls first,
    store_annual.preferred_cust_flag asc nulls first
limit 100
;`

  ```text
  refused to write 'answer_3863442186.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...in (2001, 2002) ;  select     ??? distinct store_annual.customer...

  Write stats: received 2080 chars / 2080 bytes; tail: …'store_annual.preferred_cust_flag asc nulls first\\nlimit 100\\n;'.
  ```
- `trilogy run --import raw/store_returns:sr select sr.store.store_id as entity_id, sum(sr.return_amt) as total_returns, sum(sr.net_loss) as total_loss where sr.date_dim.date between '2000-08-23'::date and '2000-09-06'::date and sr.store.store_id is not null group by entity_id limit 5`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...sr.store.store_id is not null ??? group by entity_id limit 5;
  ```
- `trilogy run --import raw.store_sales:s select s.item.category, avg(s.item.current_price) as avg_price where s.item.category is not null group by s.item.category limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...e s.item.category is not null ??? group by s.item.category limit...
  ```
- `trilogy file write answer_3263796749.preql --content import raw.store_sales as store_sales;

select
    case
        when count(grain(item.item_sk, ticket_nu…re_sales.quantity between 81 and 100)
        else avg(store_sales.net_paid ? store_sales.quantity between 81 and 100)
    end as bucket_81_100
by *;`

  ```text
  refused to write 'answer_3263796749.preql': not syntactically valid Trilogy.

  Parse error:
    --> 29:1
     |
  29 | by *;
     | ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...100)     end as bucket_81_100 ??? by *;

  Write stats: received 1583 chars / 1583 bytes; tail: …'.quantity between 81 and 100)\\n    end as bucket_81_100\\nby *;'.
  ```
- `trilogy file write answer_4077069387.preql --content import raw.web_sales as ws;

# Filter period first
where ws.sold_date.date between '1999-02-22'::date an…0)) * 100.0 as pct_of_class
order by
    ws.item.category,
    ws.item.class,
    ws.item.item_id,
    ws.item.item_desc,
    pct_of_class
limit 100;`

  ```text
  refused to write 'answer_4077069387.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block - they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...otal per item (at item grain) ??? auto item_total <- sum(ws.ext_...

  Write stats: received 840 chars / 840 bytes; tail: …'.item_id,\\n    ws.item.item_desc,\\n    pct_of_class\\nlimit 100;'.
  ```
- `trilogy file write answer_219203908.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Step 1: Find…y leaf_groups.channel nulls first, leaf_groups.brand_id nulls first, leaf_groups.class_id nulls first, leaf_groups.category_id nulls first
limit 100;`

  ```text
  refused to write 'answer_219203908.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...99 and 2001)) by * as avg_val ??? from ss),   (select avg(cs.qua...

  Write stats: received 4069 chars / 4069 bytes; tail: …' nulls first, leaf_groups.category_id nulls first\\nlimit 100;'.
  ```
- `trilogy file write answer_765177085.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;

# Items so…s.store_sk = ss.store.store_sk
order by item_id asc nulls first,
         item_desc asc nulls first,
         store_state asc nulls first
limit 100;
`

  ```text
  refused to write 'answer_765177085.preql': not syntactically valid Trilogy.

  Parse error:
   --> 6:1
    |
  6 | rowset store_sales_set as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...# Items sold in store in 2001 ??? rowset store_sales_set as wher...

  Write stats: received 3112 chars / 3112 bytes; tail: …'ulls first,\\n         store_state asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_2604809012.preql --content import raw.inventory as inv;

# Filter items by current_price between 0.99 and 1.49
where inv.item.curre…::numeric) between (2.0/3.0) and (3.0/2.0)
order by
    inv.warehouse.warehouse_name asc nulls first,
    inv.item.item_id asc nulls first
limit 100;`

  ```text
  refused to write 'answer_2604809012.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [211]: Expression in `by` clause must be wrapped in parens - write `by (expr1, expr2, ...)`. Bare identifiers (`by a, b`) work without parens, but any function call, cast, or other expression needs them.
  Location:
  ...ntory as inv;  # Filter items ??? by current_price between 0.99

  Write stats: received 977 chars / 977 bytes; tail: …'nulls first,\\n    inv.item.item_id asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write answer_2604809012.preql --content import raw.inventory as inv;

# Restrict to items whose current price is between 0.99 and 1.49
where inv…r_total / before_total::numeric <= 3.0/2.0
order by
    inv.warehouse.warehouse_name asc nulls first,
    inv.item.item_id asc nulls first
limit 100;`

  ```text
  refused to write 'answer_2604809012.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:1
    |
  7 | where inv.date_dim.date >= '2000-02-10'::date and inv.date_dim.date <= '2000-04-10'::date
    | ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
  ...ange that covers both windows ??? where inv.date_dim.date >= '20...

  Write stats: received 889 chars / 889 bytes; tail: …'nulls first,\\n    inv.item.item_id asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write answer_4207382245.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;

# Store sales quarterly total…ombined.web_q3 / combined.web_q2 as web_q3_over_q2,
    combined.store_q3 / combined.store_q2 as store_q3_over_q2
order by
    combined.county asc
;
`

  ```text
  refused to write 'answer_4207382245.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...store_q.county = web_q.county ??? subset join store_q.q = web_q....

  Write stats: received 1924 chars / 1924 bytes; tail: …'e_q2 as store_q3_over_q2\\norder by\\n    combined.county asc\\n;\\n'.
  ```
- `trilogy file write answer_4207382245.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;

# Store sales quarterly total…ombined.web_q3 / combined.web_q2 as web_q3_over_q2,
    combined.store_q3 / combined.store_q2 as store_q3_over_q2
order by
    combined.county asc
;
`

  ```text
  refused to write 'answer_4207382245.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...store_q.county = web_q.county ??? subset join store_q.q = web_q....

  Write stats: received 1924 chars / 1924 bytes; tail: …'e_q2 as store_q3_over_q2\\norder by\\n    combined.county asc\\n;\\n'.
  ```
- `trilogy file write answer_142117636.preql --content import raw.inventory as inv;
import raw.date_dim as dd;
import raw.warehouse as wh;
import raw.item as it…sk
order by jan.wh_sk asc, jan.it_sk asc, jan.month_num asc, jan.avg_qoh asc, jan.cv asc, feb.month_num asc, feb.avg_qoh asc, feb.cv asc
nulls first;`

  ```text
  refused to write 'answer_142117636.preql': not syntactically valid Trilogy.

  Parse error:
    --> 10:1
     |
  10 | with inventory_stats as
     | ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
  ...dev, coefficient of variation ??? with inventory_stats as select...

  Write stats: received 1544 chars / 1544 bytes; tail: …' feb.month_num asc, feb.avg_qoh asc, feb.cv asc\\nnulls first;'.
  ```
- `trilogy run --import raw.item:item select distinct item.category limit 20;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...port raw.item as item; select ??? distinct item.category limit 2...
  ```
- `trilogy file write answer_3560698360.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;

# Per-item, per-day running t…te = web_run.sale_date
having
    web_running_max > store_running_max
order by
    item_sk asc nulls first,
    sale_date asc nulls first
limit 100
;`

  ```text
  refused to write 'answer_3560698360.preql': not syntactically valid Trilogy.

  Parse error:
    --> 10:118
     |
  10 |     sum(sum(store_sales.sales_price)) over (partition by store_sales.item.item_sk order by store_sales.date_dim.date rows between unbounded preceding and current row) as running_total
     |                                                                                                                      ^---
     |
     = expected ORDER_DIRECTION_LA, ORDERING_DIRECTION, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
   by store_sales.date_dim.date ??? rows between unbounded precedi...

  Write stats: received 1883 chars / 1883 bytes; tail: …'k asc nulls first,\\n    sale_date asc nulls first\\nlimit 100\\n;'.
  ```
- `trilogy run --import raw/date_dim:date_dim select distinct date_dim.month_seq where date_dim.year = 1998 and date_dim.moy = 12;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ....date_dim as date_dim; select ??? distinct date_dim.month_seq wh...
  ```
- `trilogy run --import raw/store_sales:ss --import raw/catalog_sales:cs select ss.customer.customer_sk, sum(ss.ext_sales_price) as tot where ss.date_dim.month_…nd ss.store.state = ss.customer.customer_address.state group by ss.customer.customer_sk having sum(ss.ext_sales_price) > 0 order by tot desc limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...stomer.customer_address.state ??? group by ss.customer.customer_...
  ```
- `trilogy run --import raw/store_sales:ss select distinct ss.store.county, ss.store.state limit 10;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...raw.store_sales as ss; select ??? distinct ss.store.county, ss.s...
  ```
- `trilogy file write answer_3247656127.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Find item_i…e,
    sum(combined.total_sale) as total_sale_price
order by
    total_sale_price asc nulls first,
    combined.item_code asc nulls first
limit 100
;`

  ```text
  refused to write 'answer_3247656127.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...) ) -> (item_code, total_sale) ??? select     combined.item_code...

  Write stats: received 1768 chars / 1768 bytes; tail: …'ls first,\\n    combined.item_code asc nulls first\\nlimit 100\\n;'.
  ```
- `trilogy run --import raw/store_sales:store select store.item.item_id, sum(store.ext_sales_price) as tot where store.date_dim.week_seq = 5218 group by store.item.item_id limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...tore.date_dim.week_seq = 5218 ??? group by store.item.item_id li...
  ```
- `trilogy file write answer_145690531.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Rowsets: per…als.total between 0.9 * catalog_totals.total and 1.1 * catalog_totals.total
order by store_totals.item_id, store_totals.total nulls first
limit 100;
`

  ```text
  refused to write 'answer_145690531.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...total ;  # Join them together ??? subset join store_totals.item_...

  Write stats: received 1854 chars / 1854 bytes; tail: …'e_totals.item_id, store_totals.total nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_3544057080.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import r…as sale_lines,
    sum(ss.wholesale_cost) as wholesale_cost_sum,
    sum(ss.list_price) as list_price_sum,
    sum(ss.coupon_amt) as coupon_amt_sum
;`

  ```text
  refused to write 'answer_3544057080.preql': not syntactically valid Trilogy.

  Parse error:
    --> 23:1
     |
  23 | subset join sr.item.item_sk = ss.item.item_sk
     | ^---
     |
     = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
   sr.ticket_number is not null ??? subset join sr.item.item_sk =

  Write stats: received 3844 chars / 3844 bytes; tail: …'s list_price_sum,\\n    sum(ss.coupon_amt) as coupon_amt_sum\\n;'.
  ```
- `trilogy file write answer_2874833976.preql --content import raw.store_sales as ss;

# Top 5 states by store net profit in 2000
with top_states as
where ss.da…s.date_dim.year = 2000
  and ss.store.state is not null
  and ss.store.state in top_states.state
order by level desc, ss.store.state, rnk
limit 100;
`

  ```text
  refused to write 'answer_2874833976.preql': not syntactically valid Trilogy.

  Parse error:
    --> 29:1
     |
  29 | where ss.date_dim.year = 2000
     | ^---
     |
     = expected limit, order_by, or having
  Location:
  ...store.state, ss.store.county) ??? where ss.date_dim.year = 2000

  Write stats: received 1001 chars / 1001 bytes; tail: …'s.state\\norder by level desc, ss.store.state, rnk\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw.store_sales:ss select ss.store.state, sum(ss.net_profit) as state_profit where ss.date_dim.year = 2000 and ss.store.state is not null group by ss.store.state order by state_profit desc limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...nd ss.store.state is not null ??? group by ss.store.state order
  ```
- `trilogy file write answer_42596196.preql --content import raw.catalog_sales as cs;
import raw.web_sales as ws;
import raw.store_sales as ss;

with combined a…order by
  total_extended_sales_price desc nulls first,
  combined.brand_id nulls first,
  combined.hour nulls first,
  combined.minute nulls first
;`

  ```text
  refused to write 'answer_42596196.preql': not syntactically valid Trilogy.

  Parse error:
   --> 6:4
    |
  6 |   (import raw.catalog_sales as cs
    |    ^---
    |
    = expected select_statement
  Location:
  ...;  with combined as union(   ( ??? import raw.catalog_sales as cs...

  Write stats: received 1588 chars / 1588 bytes; tail: …'  combined.hour nulls first,\\n  combined.minute nulls first\\n;'.
  ```
- `trilogy file write answer_42596196.preql --content import raw.catalog_sales as cs;
import raw.web_sales as ws;
import raw.store_sales as ss;

with combined a…order by
  total_extended_sales_price desc nulls first,
  combined.brand_id nulls first,
  combined.hour nulls first,
  combined.minute nulls first
;`

  ```text
  refused to write 'answer_42596196.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
   minute bigint, total numeric) ???  select   combined.brand_id,

  Write stats: received 1492 chars / 1492 bytes; tail: …'  combined.hour nulls first,\\n  combined.minute nulls first\\n;'.
  ```
- `trilogy file write answer_2091535883.preql --content import raw.catalog_sales as cs;

where
  cs.sold_date.year = 1999
  and cs.ship_customer.household_demog…, cs.sold_date.date, day) > 5

select
  cs.item.item_desc as item_description,
  cs.sold_date.week_seq as week_sequence,
  count(*) as cnt
limit 10;
`

  ```text
  refused to write 'answer_2091535883.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a NON-NULL GRAIN KEY: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). It MUST be a key, and one that is not nullable: `count(x)` skips rows where `x` is NULL, so counting a nullable property (a name, a date, any optional field) silently undercounts. When the grain takes SEVERAL keys, name them with `grain(...)`: `count(grain(order_id, item.id))` counts order+item combinations, and `count_distinct(grain(first_name, last_name, sale_date))` counts distinct combinations - `grain()` is never NULL, so combinations with a missing member still count. For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ....week_seq as week_sequence,   ??? count(*) as cnt limit 10;

  Write stats: received 385 chars / 385 bytes; tail: …'date.week_seq as week_sequence,\\n  count(*) as cnt\\nlimit 10;\\n'.
  ```
- `trilogy file write answer_2091535883.preql --content import raw.catalog_sales as cs;

select
  cs.sold_date.year as yr,
  count(cs.order_number) as cnt
group by cs.sold_date.year
order by yr;
`

  ```text
  refused to write 'answer_2091535883.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...count(cs.order_number) as cnt ??? group by cs.sold_date.year ord...

  Write stats: received 139 chars / 139 bytes; tail: …'rder_number) as cnt\\ngroup by cs.sold_date.year\\norder by yr;\\n'.
  ```
- `trilogy file write answer_2091535883.preql --content import raw.catalog_sales as cs;

where 
  cs.sold_date.year = 1999
  and cs.ship_customer.household_demographics.buy_potential = '>10000'

select
  count(cs.order_number) as cnt
by *;
`

  ```text
  refused to write 'answer_2091535883.preql': not syntactically valid Trilogy.

  Parse error:
   --> 9:1
    |
  9 | by *;
    | ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...count(cs.order_number) as cnt ??? by *;

  Write stats: received 184 chars / 184 bytes; tail: …"al = '>10000'\\n\\nselect\\n  count(cs.order_number) as cnt\\nby *;\\n".
  ```
- `trilogy file write answer_3840922834.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.customer as c;

# Rowset 1: store s…_2002 / pivoted.store_2001)
select
    pivoted.cust_code,
    pivoted.fname,
    pivoted.lname
order by pivoted.cust_code asc nulls first
limit 100;
`

  ```text
  refused to write 'answer_3840922834.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [226]: Misplaced `subset|union join`. The key looks fine - the join is in the wrong PLACE. A query-scoped join is part of a `select` statement, not a standalone statement and not a pre-`where` clause. Put it right after the select list (preferred, SQL-like): `where <filters> select <cols> subset join a.key = b.key`. The clause order is `where` -> `select` <cols> -> join(s) -> `having` -> `order by` -> `limit`; a join may also sit between `where` and `select`, but never before `where` and never on its own. Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...columns with cust_combined as ??? subset join store_by_cust_year...

  Write stats: received 2050 chars / 2050 bytes; tail: …'lname\\norder by pivoted.cust_code asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_747155668.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sales as cs;

with combined …ls first,
         combined.sale_year nulls first,
         combined.sale_quarter nulls first,
         combined.item_category nulls first
limit 100;`

  ```text
  refused to write 'answer_747155668.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...total_ext_sales_price numeric) ???  select   combined.channel,

  Write stats: received 2171 chars / 2171 bytes; tail: …'irst,\\n         combined.item_category nulls first\\nlimit 100;'.
  ```
- `trilogy file write answer_3210116865.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw…annel is null then 0 else 1 end,
    combined.channel asc nulls first,
    combined.outlet asc nulls first,
    sum(combined.returns) desc
limit 100;`

  ```text
  refused to write 'answer_3210116865.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...turns numeric, profit numeric) ???  select     combined.channel

  Write stats: received 4501 chars / 4501 bytes; tail: …'t asc nulls first,\\n    sum(combined.returns) desc\\nlimit 100;'.
  ```
- `trilogy run --import raw/store_sales:ss select ss.date_dim.year, ss.item.item_sk, ss.customer.customer_sk, sum(ss.quantity) as qty where ss.date_dim.year = 2000 and ss.customer.customer_sk is not null group by year, item_sk, cust_sk limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...tomer.customer_sk is not null ??? group by year, item_sk, cust_s...
  ```
- `trilogy file write answer_1226264875.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw…ast,
    combined_qty asc nulls first,
    combined_wc_sum asc nulls first,
    combined_sp_sum asc nulls first,
    ratio asc nulls first
limit 100;`

  ```text
  refused to write 'answer_1226264875.preql': not syntactically valid Trilogy.

  Parse error:
    --> 33:1
     |
  33 | union all
     | ^---
     |
     = expected join_clause, metadata, limit, order_by, where, having, or select_grouping
  Location:
  ...sum(cs.sales_price) as cat_sp ??? union all where ws.sold_date.y...

  Write stats: received 2482 chars / 2482 bytes; tail: …'sp_sum asc nulls first,\\n    ratio asc nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw/store_sales:ss --import raw/store_returns:sr select ss.item.item_sk, ss.customer.customer_sk, sum(ss.quantity) as qty, sum(ss.wholes…ustomer.customer_sk is not null and (ss.ticket_number, ss.item.item_sk) not in (sr.ticket_number, sr.item.item_sk) group by item_sk, cust_sk limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...cket_number, sr.item.item_sk) ??? group by item_sk, cust_sk limi...
  ```
- `trilogy run --import raw/store_returns:sr select sr.item.item_id, sr.return_quantity where sr.item.item_id in (select sr2.item.item_id where sr2.date_dim.week_seq in (5244, 5257, 5264) group by sr2.item.item_id having sum(sr2.return_quantity) is null) and sr.date_dim.week_seq in (5244, 5257, 5264) limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...eek_seq in (5244, 5257, 5264) ??? group by sr2.item.item_id havi...
  ```
- `trilogy file write answer_2869182220.preql --content import raw.store_returns as sr;
import raw.customer as c;

# Get the set of customer_demographic keys (d…dden grain fields
    --return_demo_profiles.ticket_number,
    --return_demo_profiles.item.item_sk
order by c.customer_id asc nulls first
limit 100;`

  ```text
  refused to write 'answer_2869182220.preql': not syntactically valid Trilogy.

  Parse error:
    --> 19:3
     |
  19 | --return_demo_profiles.profile_demo_sk as h_profile,
     |   ^---
     |
     = expected PURPOSE, PROPERTY, UNIQUE, or AUTO
  Location:
  ...urn as hidden grain fields  -- ??? return_demo_profiles.profile_d...

  Write stats: received 1457 chars / 1457 bytes; tail: …'em.item_sk\\norder by c.customer_id asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write answer_927797426.preql --content import raw.web_sales as ws;
import raw.web_returns as wr;

# Filter web sales to year 2000
where ws.sold_…_refunded_cash,
    avg(wr.fee) as avg_return_fee
order by
    reason_desc_20,
    avg_sold_qty,
    avg_refunded_cash,
    avg_return_fee
limit 100;`

  ```text
  refused to write 'answer_927797426.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...rder_number = wr.order_number ??? union join ws.item.item_sk = w...

  Write stats: received 2990 chars / 2990 bytes; tail: …'ld_qty,\\n    avg_refunded_cash,\\n    avg_return_fee\\nlimit 100;'.
  ```
- `trilogy file write answer_927797426.preql --content import raw.web_sales as ws;
import raw.web_returns as wr;

# Filter web sales to year 2000
where ws.sold_…_refunded_cash,
    avg(wr.fee) as avg_return_fee
order by
    reason_desc_20,
    avg_sold_qty,
    avg_refunded_cash,
    avg_return_fee
limit 100;`

  ```text
  refused to write 'answer_927797426.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
   grain: item and order_number ??? union join ws.order_number = w...

  Write stats: received 2374 chars / 2374 bytes; tail: …'ld_qty,\\n    avg_refunded_cash,\\n    avg_return_fee\\nlimit 100;'.
  ```
- `trilogy file write answer_927797426.preql --content import raw.web_sales as ws;
import raw.web_returns as wr;

# Filter web sales to year 2000
where ws.sold_…')
     and ws.net_profit between 50 and 250)
  )
order by
    reason_desc_20,
    avg_sold_qty,
    avg_refunded_cash,
    avg_return_fee
limit 100;`

  ```text
  refused to write 'answer_927797426.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...avg(wr.fee) as avg_return_fee ??? union join ws.order_number = w...

  Write stats: received 2209 chars / 2209 bytes; tail: …'ld_qty,\\n    avg_refunded_cash,\\n    avg_return_fee\\nlimit 100;'.
  ```
- `trilogy file write answer_3281742578.preql --content import raw.web_sales as ws;
import raw.web_returns as wr;

# Step 1: Identify candidate orders that have…as order_count,
    sum(ws.ext_ship_cost) as total_ext_ship_cost,
    sum(ws.net_profit) as total_net_profit
order by
    order_count asc
limit 100
;`

  ```text
  refused to write 'answer_3281742578.preql': not syntactically valid Trilogy.

  Parse error:
    --> 22:11
     |
  22 |     exist wr.order_number
     |           ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
  ...urn_orders as where     exist ??? wr.order_number select     wr....
  ```

### `join-resolution`

- `trilogy run --import raw.store_sales:s --import raw.item:i with qualifying_items as select i.category as cat, avg(i.current_price) by i.category as cat_avg; …t_price > 1.2 * qualifying_items.cat_avg as is_qualifying where i.category is not null and i.current_price > 1.2 * qualifying_items.cat_avg limit 10;`

  ```text
  Resolution error in stdin: Could not resolve connections for query with output ['i.item_sk<Purpose.KEY>Derivation.ROOT>', 'i.category<Purpose.PROPERTY>Derivation.ROOT>', 'i.current_price<Purpose.PROPERTY>Derivation.ROOT>', 'qualifying_items.cat_avg<Purpose.METRIC>Derivation.ROWSET>', 'local.is_qualifying<Purpose.PROPERTY>Derivation.BASIC>'] from current model.
  ```
- `trilogy run --import raw/store_sales:store_sales --import raw/item:item select item.item_sk, count(grain(store_sales.ticket_number, item.item_sk)) as cnt where store_sales.date_dim.year = 2000 and item.item_sk is not null limit 5;`

  ```text
  Resolution error in stdin: Could not resolve connections for query with output ['item.item_sk<Purpose.KEY>Derivation.ROOT>', 'local.cnt<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run answer_1256642802.preql`

  ```text
  Resolution error in answer_1256642802.preql: Could not resolve connections for query with output ['ss.store.store_name<Purpose.PROPERTY>Derivation.ROOT>', 'ss.item.item_desc<Purpose.PROPERTY>Derivation.ROOT>', 'local.per_item_revenue<Purpose.METRIC>Derivation.BASIC>', 'local.item_current_price<Purpose.PROPERTY>Derivation.BASIC>', 'local.item_wholesale_cost<Purpose.PROPERTY>Derivation.BASIC>', 'local.item_brand<Purpose.PROPERTY>Derivation.BASIC>'] from current model.
  ```
- `trilogy run answer_1256642802.preql`

  ```text
  Resolution error in answer_1256642802.preql: Could not resolve connections for query with output ['ss.store.store_name<Purpose.PROPERTY>Derivation.ROOT>', 'ss.item.item_desc<Purpose.PROPERTY>Derivation.ROOT>', 'local.per_item_revenue_1998<Purpose.METRIC>Derivation.AGGREGATE>', 'ss.item.current_price<Purpose.PROPERTY>Derivation.ROOT>', 'ss.item.wholesale_cost<Purpose.PROPERTY>Derivation.ROOT>', 'ss.item.brand<Purpose.PROPERTY>Derivation.ROOT>'] from current model.
  ```
- `trilogy run answer_3046445280.preql`

  ```text
  Resolution error in answer_3046445280.preql: Could not resolve connections for query with output ['local.___tvf_arm_0_yr<Purpose.PROPERTY>Derivation.BASIC>', 'local.___tvf_arm_0_brand_id<Purpose.PROPERTY>Derivation.BASIC>', 'local.___tvf_arm_0_class_id<Purpose.PROPERTY>Derivation.BASIC>', 'local.___tvf_arm_0_cat_id<Purpose.PROPERTY>Derivation.BASIC>', 'local.___tvf_arm_0_manu_id<Purpose.PROPERTY>Derivation.BASIC>', 'local.___tvf_arm_0_net_qty<Purpose.PROPERTY>Derivation.BASIC>', 'local.___tvf_arm_0_net_amt<Purpose.PROPERTY>Derivation.BASIC>'] from current model.
  ```
- `trilogy run answer_2852230229.preql`

  ```text
  Resolution error in answer_2852230229.preql: Could not resolve connections for query with output ['local.total_net_paid<Purpose.METRIC>Derivation.AGGREGATE>', 'item.category<Purpose.PROPERTY>Derivation.ROOT>', 'item.class<Purpose.PROPERTY>Derivation.ROOT>', 'local.hierarchy_level<Purpose.METRIC>Derivation.BASIC>', 'local.rnk<Purpose.PROPERTY>Derivation.WINDOW>', 'local._virt_agg_grouping_7561668700388452<Purpose.METRIC>Derivation.AGGREGATE>', 'local._virt_agg_grouping_1927481247739235<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```

### `cli-misuse`

- `trilogy explore raw/`

  ```text
  Invalid value for 'PATH': File 'raw/' is a directory.
  ```
- `trilogy file write -e -c import raw.web_sales as ws;

# Filter period first
where ws.sold_date.date between '1999-02-22'::date and '1999-03-24'::date
  and w…0)) * 100.0 as pct_of_class
order by
    ws.item.category,
    ws.item.class,
    ws.item.item_id,
    ws.item.item_desc,
    pct_of_class
limit 100;`

  ```text
  Missing argument 'PATH'.
  ```
- `trilogy explore raw/`

  ```text
  Invalid value for 'PATH': File 'raw/' is a directory.
  ```
- `trilogy explore raw/ --regex sales|store_sales|catalog_sales|web_sales`

  ```text
  Invalid value for 'PATH': File 'raw/' is a directory.
  ```

### `undefined-concept`

- `trilogy explore answer_3863442186.preql`

  ```text
  Failed to parse answer_3863442186.preql: (UndefinedConceptException(...), "Undefined concept: sv01.customer_id. Suggestions: ['s.customer.customer_id', 'cs.bill_customer.customer_id', 'cs.ship_customer.customer_id', 'ws.bill_customer.customer_id', 'ws.ship_customer.customer_id', 'store_vals.s.customer.customer_id']")
  ```

### `syntax-missing-alias`

- `trilogy file write answer_3979964698.preql -c import raw.web_sales as web;
import raw.catalog_sales as cat;

with combined as union(
  (
    select
      web…sum(combined.monthly_net ? combined.sale_month = 12) as dec_net
order by
  combined.wh_name nulls first,
  combined.sale_year nulls first
limit 100;
`

  ```text
  refused to write 'answer_3979964698.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `hidden warehouse sk distinguishes but is not displayed
    -- 8 visible grouping fields
    combined.wh_name as hidden_warehouse_sk_distinguishes_but_is`
  Location:
  ...hly_net);  select   -- hidden ??? warehouse sk distinguishes but...

  Write stats: received 5000 chars / 5000 bytes; tail: …'me nulls first,\\n  combined.sale_year nulls first\\nlimit 100;\\n'.
  ```
