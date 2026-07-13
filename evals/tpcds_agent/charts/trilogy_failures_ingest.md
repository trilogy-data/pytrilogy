# Trilogy failure analysis — 20260712-204358

- Run `20260712-204357_ingest` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 1654 | failed: 191 (12%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 136 | 71% |
| `syntax-parse` | 46 | 24% |
| `syntax-missing-alias` | 4 | 2% |
| `type-error` | 2 | 1% |
| `cli-misuse` | 2 | 1% |
| `join-resolution` | 1 | 1% |

## Detail

### `other`

- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe store_returns`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run --import raw.store_returns:store_returns select sr.customer.customer_id, count(sr.ticket_number) as cnt where sr.customer.customer_id is null and sr.date_dim.year = 2000 and sr.store.state = 'TN' limit 5;`

  ```text
  Syntax error in stdin: 5 undefined concept references; fix all before re-running:
    - sr.customer.customer_id (line 2, col 8, in SELECT); did you mean: store_returns.customer.customer_id, store_returns.customer.customer_sk, store_returns.customer.customer_address.zip?
    - sr.ticket_number (line 2, in SELECT); did you mean: store_returns.ticket_number?
    - sr.customer.customer_id (line 2, col 70, in WHERE); did you mean: store_returns.customer.customer_id, store_returns.customer.customer_sk, store_returns.customer.customer_address.zip?
    - sr.date_dim.year (line 2, col 106, in WHERE); did you mean: store_returns.date_dim.year, store_returns.store.date_dim.year, store_returns.customer.first_sales_date.year, store_returns.customer.last_review_date.year, store_returns.customer.first_shipto_date.year, store_returns.date_dim.fy_year?
    - sr.store.state (line 2, col 134, in WHERE); did you mean: store_returns.store.state, store_returns.customer_address.state, store_returns.customer.customer_address.state?
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3705756794.preql`

  ```text
  Syntax error in answer_3705756794.preql: Undefined concept: ss.sold_date.date. Suggestions: ['ss.promotion.end_date.date', 'ss.promotion.end_date.date_sk', 'ss.promotion.end_date.date_id', 'cs.sold_date.date', 'ws.sold_date.date', 'cs.ship_date.date']
  ```
- `trilogy run answer_3705756794.preql`

  ```text
  Resolution error in answer_3705756794.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {store_combined.entity_id, store_combined.ext_sales, store_combined.net_loss, store_combined.return_amt, store_combined.sales_net_profit, store_returns_agg.entity_id, store_sales_agg.entity_id}
  ```
- `trilogy run answer_3705756794.preql`

  ```text
  Resolution error in answer_3705756794.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {store_combined.entity_id, store_combined.ext_sales, store_combined.net_loss, store_combined.return_amt, store_combined.sales_net_profit, store_returns_agg.entity_id, store_sales_agg.entity_id}
  ```
- `trilogy run answer_3705756794.preql`

  ```text
  Syntax error in answer_3705756794.preql: Undefined concept: cr.return_amt. Suggestions: ['cr.return_amount', 'cr.return_tax', 'cr.return_quantity', 'sr.return_amt', 'wr.return_amt', 'store_channel.return_amt']
  ```
- `trilogy run answer_3705756794.preql`

  ```text
  Syntax error in answer_3705756794.preql: ORDER BY contains aggregate `grouping(all_channels.channel_type)` (line 135), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(all_channels.channel_type) as g order by g desc`.
  ```
- `trilogy file read raw/customer.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run -`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query (statement at line 7). The requested concepts split into 2 disconnected subgraphs: {avg_price, cat, is_above, price, ss.customer.customer_address.address_sk, ss.date_dim.moy, ss.date_dim.year, ss.item.category}; {cnt}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_689898357.preql`

  ```text
  Syntax error in answer_689898357.preql: Comparison `store_sales.promotion.channel_email = 'N'` matches every value of nullable enum field 'store_sales.promotion.channel_email', which contains only these values: 'N'. It only excludes nulls; simplify it to 'store_sales.promotion.channel_email is not null'.
  ```
- `trilogy run answer_219203908.preql`

  ```text
  Syntax error in answer_219203908.preql: `by rollup (…)` requires at least one aggregate (or re-aggregable pre-aggregated measure) in the select to group; found none.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_2524943990.preql`

  ```text
  Syntax error in answer_2524943990.preql: Comparison `cs.call_center.county = 'Williamson County'` matches every value of enum field 'cs.call_center.county', which contains only these values: 'Williamson County'. It is always true and should be removed.
  ```
- `trilogy run answer_2524943990.preql`

  ```text
  Resolution error in answer_2524943990.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 10). The requested concepts split into 2 disconnected subgraphs: {cs.ship_addr.state, cs.ship_date.date, warehouse_count}; {has_return}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_765177085.preql`

  ```text
  Syntax error in answer_765177085.preql: Undefined concept: ss.item_id. Suggestions: ['ss.item.item_id', 'ss.promotion.item.item_id', 'ss.item_desc', 'ss.item.class_id', 'sr.item_id', 'item_id']
  ```
- `trilogy run answer_1835050598.preql`

  ```text
  Syntax error in answer_1835050598.preql: ORDER BY contains aggregate `grouping(local.country)` (line 7), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(local.country) as g order by g desc`.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.store_sales:ss - duckdb select ss.item.item_sk, substring(ss.item.item_desc, 1, 30) as d30, ss.date_dim.date, count(ss.item.item_sk) by (ss.item.item_sk, substring(ss.item.item_desc, 1, 30), ss.date_dim.date) as cnt having cnt > 4 limit 10;`

  ```text
  Connection argument 'select ss.item.item_sk, substring(ss.item.item_desc, 1, 30) as d30, ss.date_dim.date, count(ss.item.item_sk) by (ss.item.item_sk, substring(ss.item.item_desc, 1, 30), ss.date_dim.date) as cnt having cnt > 4 limit 10;' has no value; pass connection arguments as 'key value' pairs or 'key=value'
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run --import raw.store_sales:ss select count(ss.ticket_number) as cnt where date_dim.year = 2001 and date_dim.moy = 4;`

  ```text
  Syntax error in stdin: 2 undefined concept references; fix all before re-running:
    - date_dim.year (line 2, col 45, in WHERE); did you mean: ss.date_dim.year, ss.store.date_dim.year, date_dim.moy, ss.promotion.end_date.year, ss.promotion.start_date.year, ss.customer.first_sales_date.year?
    - date_dim.moy (line 2, col 70, in WHERE); did you mean: ss.date_dim.moy, ss.store.date_dim.moy, date_dim.year, ss.promotion.end_date.moy, ss.promotion.start_date.moy, ss.customer.first_sales_date.moy?
  ```
- `trilogy run answer_4254732537.preql`

  ```text
  Syntax error in answer_4254732537.preql: Comparison `cs.promotion.channel_email = 'N'` matches every value of nullable enum field 'cs.promotion.channel_email', which contains only these values: 'N'. It only excludes nulls; simplify it to 'cs.promotion.channel_email is not null'.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_1798498862.preql`

  ```text
  Unexpected error in answer_1798498862.preql: Conflicting join types (full, left outer) on keys joined into one group: a FULL/UNION join cannot be mixed with another type on the same key (it is ambiguous whether the key is required or one-sided). Make the whole group one type (e.g. `UNION JOIN a = b = c`), or use a distinct key.
  ```
- `trilogy file read raw/web_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe web_returns`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 44 column 29 (char 1662). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3347758002.preql`

  ```text
  Syntax error in answer_3347758002.preql: Undefined concept: item.category. Suggestions: ['ss.item.category', 'cs.item.category', 'ws.item.category', 'ss.promotion.item.category', 'cs.promotion.item.category', 'ws.promotion.item.category']
  ```
- `trilogy run answer_3347758002.preql`

  ```text
  Syntax error in answer_3347758002.preql: Undefined concept: ss.sold_date.year. Suggestions: ['ss.date_dim.year', 'ss.store.date_dim.year', 'ss.promotion.end_date.year', 'cs.sold_date.year', 'ws.sold_date.year', 'cs.ship_date.year']
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3530408544.preql`

  ```text
  Syntax error in answer_3530408544.preql: Comparison `store_sales.store.county = 'Williamson County'` matches every value of enum field 'store_sales.store.county', which contains only these values: 'Williamson County'. It is always true and should be removed.
  ```
- `trilogy run answer_3770074305.preql`

  ```text
  Resolution error in answer_3770074305.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 5). The requested concepts split into 2 disconnected subgraphs: {inventory.date_dim.date, inventory.quantity_on_hand}; {item.current_price, item.item_desc, item.item_id, item.item_sk, item.manufact_id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_142117636.preql`

  ```text
  Syntax error in answer_142117636.preql: ORDER BY references 'jan.qualifying.monthly_stats.item_sk', which is not in the SELECT projection (line 54). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --jan.qualifying.monthly_stats.item_sk order by jan.qualifying.monthly_stats.item_sk asc`.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read answer_630074061.preql`

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
- `trilogy run answer_374591292.preql duckdb`

  ```text
  Resolution error in answer_374591292.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 57). The requested concepts split into 2 disconnected subgraphs: {best.best_product, best.pair_rank}; {worst.worst_product}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe store_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 96 column 3 (char 4244). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run answer_2118989494.preql`

  ```text
  Syntax error in answer_2118989494.preql: Nothing was executed: parsed 4 definition statement(s) (3 rowsets, 1 import) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw/store_sales:store_sales --import raw/store_returns:store_returns select store_sales.item.item_sk, sum(store_sales.net_profit) as p, sum(store_sales.net_paid) as r, sum(store_sales.quantity) as q, sum(store_returns.return_amt) as ra, sum(store_returns.return_quantity) as rq limit 10;`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {p, q, r, store_sales.item.item_sk}; {ra, rq}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_2940558602.preql`

  ```text
  Resolution error in answer_2940558602.preql: (AmbiguousRelationshipResolutionException(...), "Multiple possible concept additions (intermediate join keys) found to resolve ['store_returns.item.item_sk', 'store_returns.return_amt', 'store_returns.return_quantity', 'store_sales.date_dim.moy', 'store_sales.date_dim.year', 'store_sales.net_paid', 'store_sales.net_profit', 'store_sales.quantity', 'store_sales.date_dim.date_sk'], have {'store_returns.item.item_sk'} or {'store_returns.ticket_number'}. Different paths are is: [{'store_returns.item.item_sk'}, {'store_returns.ticket_number'}]")
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.store_sales:ss --import raw.store_returns:sr select ss.ticket_number, ss.item.item_sk, sr.ticket_number, sr.item.item_sk where ss.ticket_number = sr.ticket_number and ss.item.item_sk = sr.item.item_sk limit 5;`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {sr.item.item_sk, sr.ticket_number}; {ss.item.item_sk, ss.ticket_number}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read answer_3560698360.preql`

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
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_345752060.preql`

  ```text
  Syntax error in answer_345752060.preql: Undefined concept: item.category. Suggestions: ['ss.item.category', 'cs.item.category', 'ws.item.category', 'ss.promotion.item.category', 'cs.promotion.item.category', 'ws.promotion.item.category']
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3544057080.preql duckdb`

  ```text
  Syntax error in answer_3544057080.preql: Ambiguous reference 'ss_with_sr.zip': matches ['ss_with_sr.ss.customer.customer_address.zip', 'ss_with_sr.ss.customer_address.zip', 'ss_with_sr.ss.store.zip']. Qualify the full path to disambiguate.
  ```
- `trilogy run answer_3544057080.preql duckdb`

  ```text
  Resolution error in answer_3544057080.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 19). The requested concepts split into 2 disconnected subgraphs: {cs.item.item_sk, cs_ext_list_price_by_item}; {cr_refund_by_item}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_1256642802.preql`

  ```text
  Syntax error in answer_1256642802.preql: Output column 'per_item_revenue' renames 'local.per_item_revenue' back to the name of an existing concept 'per_item_revenue' (defined at line 4) that 'local.per_item_revenue' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'per_item_revenue_out').
  ```
- `trilogy run answer_1256642802.preql`

  ```text
  Syntax error in answer_1256642802.preql: ORDER BY references 'ss.store.store_sk', which is not in the SELECT projection (line 9). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --ss.store.store_sk order by ss.store.store_sk asc`.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3979964698.preql`

  ```text
  Syntax error in answer_3979964698.preql: Undefined concept: ws.sold_date.month. Suggestions: ['ws.sold_date.month_seq', 'ws.sold_date.moy', 'ws.sold_date.qoy', 'cs.sold_date.month_seq']
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read answer_3036656719.preql`

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
- `trilogy run answer_755724379.preql`

  ```text
  Resolution error in answer_755724379.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 29). The requested concepts split into 2 disconnected subgraphs: {cs.ship_customer.customer_sk}; {ws.sold_date.moy, ws.sold_date.year}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/store.preql`

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
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read answer_2091535883.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_1772060640.preql`

  ```text
  Syntax error in answer_1772060640.preql: Comparison `ss.store.county in ('Orange County', 'Bronx County', 'Franklin Parish', 'Williamson County')` matches every value of enum field 'ss.store.county', which contains only these values: 'Williamson County'. It is always true and should be removed.
  ```
- `trilogy run answer_1772060640.preql`

  ```text
  Syntax error in answer_1772060640.preql: ORDER BY references 'ss.customer.customer_sk', which is not in the SELECT projection (line 3). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --ss.customer.customer_sk order by ss.customer.customer_sk asc`.
  ```
- `trilogy run answer_3046445280.preql`

  ```text
  Syntax error in answer_3046445280.preql: Undefined concept: y2002.qty_2001 (line 110, col 7, in WHERE). Suggestions: ['y2002.qty_2002', 'y2002.amt_2002', 'y2001.qty_2001']
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
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 117 column 13 (char 3852). Re-issue the call with valid JSON arguments.
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
- `trilogy run answer_1226264875.preql`

  ```text
  Unexpected error in answer_1226264875.preql: Cannot join `catalog_base.cat_year` to itself (`catalog_base.cat_year` resolves to the same key `catalog_base.cat_year`), which degenerates to `1=1`. Join distinct keys (e.g. separate rowset outputs or distinct expressions).
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe store_returns`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy database describe catalog_returns`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy database describe web_returns`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/customer.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_2869182220.preql`

  ```text
  Resolution error in answer_2869182220.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {c.customer_address.city, c.customer_demographics.demo_sk, c.household_demographics.income_band.lower_bound, c.household_demographics.income_band.upper_bound, customer_code, full_name}; {sr.customer.customer_demographics.demo_sk}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy database describe web_returns`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/web_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

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
- `trilogy run --import raw.store_sales:store_sales select store.store_name limit 5;`

  ```text
  Syntax error in stdin: Undefined concept: store.store_name (line 2, col 8, in SELECT). Suggestions: ['store_sales.store.store_name', 'store_sales.store.street_name']
  ```
- `trilogy run --import raw.time_dim where time_dim.hour = 8 and time_dim.minute = 30 select time_dim.time limit 5;`

  ```text
  Syntax error in stdin: 3 undefined concept references; fix all before re-running:
    - time_dim.time (line 2, col 57, in SELECT); did you mean: time_dim.minute, time_dim.hour, time, time_id?
    - time_dim.hour (line 2, col 7, in WHERE); did you mean: time_dim.minute, time_dim.time, hour, time_id?
    - time_dim.minute (line 2, col 29, in WHERE); did you mean: time_dim.time, time_dim.hour, minute?
  ```
- `trilogy run --import raw.time_dim where time_dim.hour = 8 and time_dim.minute = 30 select time_dim.time limit 5;`

  ```text
  Syntax error in stdin: 3 undefined concept references; fix all before re-running:
    - time_dim.time (line 2, col 57, in SELECT); did you mean: time_dim.minute, time_dim.hour, time, time_id?
    - time_dim.hour (line 2, col 7, in WHERE); did you mean: time_dim.minute, time_dim.time, hour, time_id?
    - time_dim.minute (line 2, col 29, in WHERE); did you mean: time_dim.time, time_dim.hour, minute?
  ```
- `trilogy run answer_32504671.preql`

  ```text
  Syntax error in answer_32504671.preql: Comparison `ss.household_demographics.vehicle_count <= 6` matches every value of enum field 'ss.household_demographics.vehicle_count', which contains only these values: -1, 0, 1, 2, 3, 4. It is always true and should be removed.
  ```
- `trilogy run answer_840315271.preql`

  ```text
  Syntax error in answer_840315271.preql: Undefined concept: monthly_totals.avg_total (line 29, col 7, in WHERE). Suggestions: ['monthly_totals.total', 'monthly_totals.month_of_year', 'monthly_totals.ss.store.store_name', 'avg_by_group.avg_total']
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run answer_2822608529.preql`

  ```text
  Syntax error in answer_2822608529.preql: Undefined concept: time_dim.hour (line 2, col 8, in SELECT). Suggestions: ['ws.time_dim.hour', 'ws.time_dim.shift', 'ws.time_dim.second']
  ```
- `trilogy file read raw/catalog_returns.preql`

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
- `trilogy run answer_569612608.preql`

  ```text
  Resolution error in answer_569612608.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 7). The requested concepts split into 2 disconnected subgraphs: {has_return, return_flag}; {ws.order_number}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_2333598260.preql`

  ```text
  Syntax error in answer_2333598260.preql: Undefined concept: store_sales.line_item_flag (line 11, in SELECT). Suggestions: ['store_sales.item.item_sk', 'store_sales.item.item_id', 'store_sales.item.class', 'line_item_flag']
  ```
- `trilogy run answer_2333598260.preql`

  ```text
  Syntax error in answer_2333598260.preql: Aggregate concept local.line_item_count cannot reference itself. If defining a new concept in a select, use a new name.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read answer_3498056762.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy run --import raw.store_returns:store_returns select store_returns.store.state, count(store_returns.ticket_number) as cnt where store_returns.date_dim.year = 2000 group by store_returns.store.state limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ..._returns.date_dim.year = 2000 ??? group by store_returns.store.s...
  ```
- `trilogy file write answer_3705756794.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw…d.ext_sales, 0) as ext_sales,
    coalesce(store_combined.returns, 0) as returns,
    coalesce(store_combined.net_profit, 0) as net_profit
limit 100;`

  ```text
  refused to write 'answer_3705756794.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...eturns with store_combined as ??? union join ss.sold_date.date_s...

  Write stats: received 896 chars / 896 bytes; tail: …'lesce(store_combined.net_profit, 0) as net_profit\\nlimit 100;'.
  ```
- `trilogy file write answer_3705756794.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw…_id is not null
select
    sr.store.store_id as store_id,
    sum(sr.return_amt) as return_amt,
    sum(sr.net_loss) as net_loss
;

select * limit 1;`

  ```text
  refused to write 'answer_3705756794.preql': not syntactically valid Trilogy.

  Parse error:
    --> 30:8
     |
  30 | select * limit 1;
     |        ^---
     |
     = expected select_item
  Location:
  ...t_loss) as net_loss ;  select ??? * limit 1;

  Write stats: received 797 chars / 797 bytes; tail: …'n_amt,\\n    sum(sr.net_loss) as net_loss\\n;\\n\\nselect * limit 1;'.
  ```
- `trilogy file write answer_3705756794.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw…    'store' || coalesce(store_combined.entity_id, store_sales_agg.entity_id, store_returns_agg.entity_id) as entity_id,
from store_combined
limit 10;`

  ```text
  refused to write 'answer_3705756794.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ..._agg.entity_id) as entity_id, ??? from store_combined limit 10;

  Write stats: received 1404 chars / 1404 bytes; tail: …'s_agg.entity_id) as entity_id,\\nfrom store_combined\\nlimit 10;'.
  ```
- `trilogy file write answer_219203908.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Step 1: Find…       nov2001_detail.brand_id nulls first,
         nov2001_detail.class_id nulls first,
         nov2001_detail.category_id nulls first
limit 100;
`

  ```text
  refused to write 'answer_219203908.preql': not syntactically valid Trilogy.

  Parse error:
    --> 38:20
     |
  38 |          -- hidden grain fields for counting distinct line items
     |                    ^---
     |
     = expected limit, order_by, where, having, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, select_grouping, or JOIN_TYPE
  Location:
   sale_val,          -- hidden ??? grain fields for counting dist...

  Write stats: received 3984 chars / 3984 bytes; tail: …'\\n         nov2001_detail.category_id nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_219203908.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Step 1: Find…first,
         nov2001_raw.brand_id nulls first,
         nov2001_raw.class_id nulls first,
         nov2001_raw.category_id nulls first
limit 100;
`

  ```text
  refused to write 'answer_219203908.preql': not syntactically valid Trilogy.

  Parse error:
    --> 54:39
     |
  54 | auto grp_items <- count(nov2001_raw.g1, nov2001_raw.g2) by nov2001_raw.channel, nov2001_raw.brand_id, nov2001_raw.class_id, nov2001_raw.category_id;
     |                                       ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ..._items <- count(nov2001_raw.g1 ??? , nov2001_raw.g2) by nov2001_r...

  Write stats: received 3511 chars / 3511 bytes; tail: …'st,\\n         nov2001_raw.category_id nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_2928586490.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Step 1: Fre…ned.ch_sales) as total_sales
having total_sales > 0
order by combined.last_name asc, combined.first_name asc, total_sales asc nulls first
limit 100;
`

  ```text
  refused to write 'answer_2928586490.preql': not syntactically valid Trilogy.

  Parse error:
    --> 80:6
     |
  80 |     (cat_arm),
     |      ^---
     |
     = expected select_statement
  Location:
   with combined as union(     ( ??? cat_arm),     (web_arm) ) -> (...

  Write stats: received 2818 chars / 2818 bytes; tail: …'ined.first_name asc, total_sales asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_2928586490.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Step 1: Fre…_sales) as total_sales
having total_sales > 0
order by final_sales.last_name asc, final_sales.first_name asc, total_sales asc nulls first
limit 100;
`

  ```text
  refused to write 'answer_2928586490.preql': not syntactically valid Trilogy.

  Parse error:
    --> 12:60
     |
  12 | having count(ss.item.item_sk) by ss.item.item_sk, substring(ss.item.item_desc, 1, 30), ss.date_dim.date > 4
     |                                                            ^---
     |
     = expected limit, order_by, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, window_sql_over, or OVER_COMPONENT_REF
  Location:
   by ss.item.item_sk, substring ??? (ss.item.item_desc, 1, 30), ss...

  Write stats: received 2680 chars / 2680 bytes; tail: …'ales.first_name asc, total_sales asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_2928586490.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Step 1: Fre…_sales) as total_sales
having total_sales > 0
order by final_sales.last_name asc, final_sales.first_name asc, total_sales asc nulls first
limit 100;
`

  ```text
  refused to write 'answer_2928586490.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [211]: Expression in `by` clause must be wrapped in parens - write `by (expr1, expr2, ...)`. Bare identifiers (`by a, b`) work without parens, but any function call, cast, or other expression needs them.
  Location:
  ...k,     count(ss.item.item_sk) ??? by ss.item.item_sk, substring(...

  Write stats: received 2645 chars / 2645 bytes; tail: …'ales.first_name asc, total_sales asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw.store_sales:ss select ss.store.date_dim.year, ss.store.date_dim.moy, count(ss.ticket_number) as cnt group by 1, 2 order by 1, 2 limit 20;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...ount(ss.ticket_number) as cnt ??? group by 1, 2 order by 1, 2 li...
  ```
- `trilogy run --import raw.store_sales:ss select ss.store.date_dim.date_sk, count(ss.ticket_number) as cnt where ss.store.date_dim.date_sk is not null group by 1 order by cnt desc limit 20;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ....date_dim.date_sk is not null ??? group by 1 order by cnt desc l...
  ```
- `trilogy file write answer_4207382245.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;

# For each county and calendar quarter in yea…quarter = 2), 0) as store_q3_over_q2
having
    web_q2_over_q1 > store_q2_over_q1
    and web_q3_over_q2 > store_q3_over_q2
order by
    county asc
;`

  ```text
  refused to write 'answer_4207382245.preql': not syntactically valid Trilogy.

  Parse error:
    --> 29:12
     |
  29 |     exists (select 1 where store_agg.quarter = 1 and web_agg.quarter = 1
     |            ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
  ...ata for Q1, Q2, Q3     exists ??? (select 1 where store_agg.quar...

  Write stats: received 1828 chars / 1828 bytes; tail: …' web_q3_over_q2 > store_q3_over_q2\\norder by\\n    county asc\\n;'.
  ```
- `trilogy file write answer_3347758002.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# First, defi…
  combined.manuf_id as manufacturer_id,
  sum(combined.total_ext_price) as total_sales
order by
  total_sales asc,
  manufacturer_id asc
limit 100;
`

  ```text
  refused to write 'answer_3347758002.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...nt?, total_ext_price numeric?) ???  select   combined.manuf_id a...

  Write stats: received 1389 chars / 1389 bytes; tail: …'rder by\\n  total_sales asc,\\n  manufacturer_id asc\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_3809267817.preql --content # Answer 3809267817: Customers with store sales AND (web OR catalog) sales in Q1-Q3 2002
# with demograp…ital_status asc nulls first,
    dep_count asc nulls first,
    dep_employed_count asc nulls first,
    dep_college_count asc nulls first
limit 100;
`

  ```text
  refused to write 'answer_3809267817.preql': not syntactically valid Trilogy.

  Parse error:
    --> 10:33
     |
  10 | where date_part(ss.date_dim.year) = 2002
     |                                 ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...ere date_part(ss.date_dim.year ??? ) = 2002   and date_part(ss.da...

  Write stats: received 2943 chars / 2943 bytes; tail: …'lls first,\\n    dep_college_count asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw.item:item select distinct item.units order by item.units;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...port raw.item as item; select ??? distinct item.units order by i...
  ```
- `trilogy run --import raw.item:item select item.manufact, count(item.item_sk) as cnt where item.manufact in (select item.manufact where (item.category = 'Book…nics' and item.color = 'mint' and item.units = 'Gross' and item.size = 'N/A')) group by item.manufact having cnt = 1 order by item.manufact limit 20;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...ross' and item.size = 'N/A')) ??? group by item.manufact having
  ```
- `trilogy file write answer_3079393690.preql --content import raw.store_sales as ss;

def sun_total -> sum(ss.sales_price ? ss.date_dim.day_name = 'Sunday');
d…
    @thu_total as thursday,
    @fri_total as friday,
    @sat_total as saturday
order by
    ss.store.store_name,
    ss.store.store_id
limit 100;
`

  ```text
  refused to write 'answer_3079393690.preql': not syntactically valid Trilogy.

  Parse error:
    --> 16:5
     |
  16 |     @sun_total as sunday,
     |     ^---
     |
     = expected select_item, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...e,     ss.store.store_id,     ??? @sun_total as sunday,     @mon...

  Write stats: received 916 chars / 916 bytes; tail: …'y\\n    ss.store.store_name,\\n    ss.store.store_id\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_3079393690.preql`

  ```text
  refused to write 'answer_3079393690.preql': not syntactically valid Trilogy.

  Parse error:
    --> 16:5
     |
  16 |     @sun_total as sunday,
     |     ^---
     |
     = expected select_item, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:

       ss.store.store_id,
       ??? @sun_total as sunday,
       @mo...

  Write stats: received 942 chars / 942 bytes; tail: …'   ss.store.store_name,\r\\n    ss.store.store_id\r\\nlimit 100;\r\\n'.
  ```
- `trilogy file write answer_1965638525.preql -e -c import raw.store_sales as store_sales;

where
  store_sales.date_dim.day_name in ('Saturday', 'Sunday')
  an…c nulls first,
  first_name asc nulls first,
  current_city asc nulls first,
  sale_city asc nulls first,
  ticket_number asc nulls first

limit 100
`

  ```text
  refused to write 'answer_1965638525.preql': not syntactically valid Trilogy.

  Parse error:
    --> 20:1
     |
  20 | order by
     | ^---
     |
     = expected metadata, where, having, select_grouping, or JOIN_TYPE
  Location:
  ..._profit) as total_net_profit  ??? order by   last_name asc nulls...

  Write stats: received 970 chars / 970 bytes; tail: …'asc nulls first,\\n  ticket_number asc nulls first\\n\\nlimit 100\\n'.
  ```
- `trilogy file write answer_1965638525.preql -e -c import raw.store_sales as store_sales;

where
  store_sales.date_dim.day_name in ('Saturday', 'Sunday')
  an…c nulls first,
  first_name asc nulls first,
  current_city asc nulls first,
  sale_city asc nulls first,
  ticket_number asc nulls first

limit 100
`

  ```text
  refused to write 'answer_1965638525.preql': not syntactically valid Trilogy.

  Parse error:
    --> 20:1
     |
  20 | order by
     | ^---
     |
     = expected metadata, where, having, select_grouping, or JOIN_TYPE
  Location:
  ..._profit) as total_net_profit  ??? order by   last_name asc nulls...

  Write stats: received 970 chars / 970 bytes; tail: …'asc nulls first,\\n  ticket_number asc nulls first\\n\\nlimit 100\\n'.
  ```
- `trilogy file write answer_1965638525.preql -e -c import raw.store_sales as store_sales;

where
  store_sales.date_dim.day_name in ('Saturday', 'Sunday')
  an…sales.ticket_number as ticket_number,
  sum(store_sales.coupon_amt) as total_coupon_amt,
  sum(store_sales.net_profit) as total_net_profit
limit 100
`

  ```text
  refused to write 'answer_1965638525.preql': not syntactically valid Trilogy.

  Parse error:
    --> 19:1
     |
  19 | limit 100
     | ^---
     |
     = expected metadata, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...t_profit) as total_net_profit ??? limit 100

  Write stats: received 807 chars / 807 bytes; tail: …'  sum(store_sales.net_profit) as total_net_profit\\nlimit 100\\n'.
  ```
- `trilogy run --import raw.store_sales:ss --import raw.store_returns:sr select count(*) as total, count(ss.store.store_sk) as with_store, count(sr.date_dim.moy…join ss.ticket_number = sr.ticket_number subset join ss.item.item_sk = sr.item.item_sk subset join ss.customer.customer_sk = sr.customer.customer_sk;`

  ```text
  Syntax error in stdin: Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...w.store_returns as sr; select ??? count(*) as total, count(ss.st...
  ```
- `trilogy run --import raw.store_sales:ss --import raw.store_returns:sr select count(ss.ticket_number) as total where sr.date_dim.moy = 8 and sr.date_dim.year …join ss.ticket_number = sr.ticket_number subset join ss.item.item_sk = sr.item.item_sk subset join ss.customer.customer_sk = sr.customer.customer_sk;`

  ```text
  Syntax error in stdin:  --> 3:129
    |
  3 | select count(ss.ticket_number) as total where sr.date_dim.moy = 8 and sr.date_dim.year = 2001 and ss.store.store_sk is not null subset join ss.ticket_number = sr.ticket_number subset join ss.item.item_sk = sr.item.item_sk subset join ss.customer.customer_sk = sr.customer.customer_sk;
    |                                                                                                                                 ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...ss.store.store_sk is not null ??? subset join ss.ticket_number =...
  ```
- `trilogy run --import raw.item:item select distinct item.color where item.color in ('slate','blanched','burnished') limit 20;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...port raw.item as item; select ??? distinct item.color where item...
  ```
- `trilogy run --import raw/store_sales:ss --import raw/catalog_sales:cs --import raw/web_sales:ws with combined as union((select ss.item.item_id as item_id, ss…_date.moy = 2 and ws.bill_addr.gmt_offset = -5 limit 3)) -> (item_id string, ext_price numeric) select combined.item_id, combined.ext_price limit 10;`

  ```text
  Syntax error in stdin: Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ..._id string, ext_price numeric) ??? select combined.item_id, comb...
  ```
- `trilogy run --import raw/store_sales:ss select ss.item.item_id, sum(ss.ext_sales_price) as store_total where ss.date_dim.week_seq = 5218 group by ss.item.item_id limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...e ss.date_dim.week_seq = 5218 ??? group by ss.item.item_id limit...
  ```
- `trilogy run --import raw.catalog_sales:cs select distinct cs.ship_mode.carrier limit 20;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...w.catalog_sales as cs; select ??? distinct cs.ship_mode.carrier
  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.store.state, store_sales.store.county, sum(store_sales.net_profit) as total_np from store…m.year = 2000 and store_sales.store.store_sk is not null group by store_sales.store.state, store_sales.store.county order by total_np desc limit 100;`

  ```text
  Syntax error in stdin: Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...sales.net_profit) as total_np ??? from store_sales where store_s...
  ```
- `trilogy file write answer_42596196.preql --content import raw.catalog_sales as cs;
import raw.web_sales as ws;
import raw.store_sales as ss;

with combined a…xtended_sales_price
order by
    total_extended_sales_price desc,
    brand_id asc nulls first,
    hour asc nulls first,
    minute asc nulls first;`

  ```text
  refused to write 'answer_42596196.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...e string?, manager_id bigint?) ???  where     combined.manager_i...

  Write stats: received 1855 chars / 1855 bytes; tail: …'first,\\n    hour asc nulls first,\\n    minute asc nulls first;'.
  ```
- `trilogy file write answer_747155668.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sales as cs;

with combined …ined.total_ext_sales_price
order by combined.channel, combined.missing_ref, combined.year, combined.quarter, combined.category nulls first
limit 100;`

  ```text
  refused to write 'answer_747155668.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block - they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ....ticket_number is not null    ??? auto ss_missing_flag <- sum(ca...

  Write stats: received 2526 chars / 2526 bytes; tail: …', combined.quarter, combined.category nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw/catalog_returns:cr select cr.item.item_id, sum(cr.return_quantity) as qty, count(cr.order_number) as rows where cr.date_dim.week_seq = 5244 group by cr.item.item_id having sum(cr.return_quantity) is null limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...e cr.date_dim.week_seq = 5244 ??? group by cr.item.item_id havin...
  ```
- `trilogy run --import raw/item:i select i.item_id, count(i.item_sk) as versions where i.item_id in (select sr.item.item_id from raw.store_returns as sr group by sr.item.item_id limit 5);`

  ```text
  Syntax error in stdin: Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...id in (select sr.item.item_id ??? from raw.store_returns as sr g...
  ```
- `trilogy run --import raw/item:i select i.item_id, count(i.item_sk) as versions group by i.item_id having versions > 1 limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
   count(i.item_sk) as versions ??? group by i.item_id having vers...
  ```
- `trilogy file write answer_2869182220.preql --content import raw.store_returns as sr;
import raw.customer as c;

# Find customers in Edgewood with matching de…omer.customer_sk as --customer_sk_ret,
  sr.ticket_number as --ticket,
  sr.item.item_sk as --item_sk
order by
  c.customer_id nulls first
limit 100;`

  ```text
  refused to write 'answer_2869182220.preql': not syntactically valid Trilogy.

  Parse error:
    --> 14:30
     |
  14 |   sr.customer.customer_sk as --customer_sk_ret,
     |                              ^---
     |
     = expected IDENTIFIER
  Location:
     sr.customer.customer_sk as ??? --customer_sk_ret,   sr.ticket...

  Write stats: received 643 chars / 643 bytes; tail: …'as --item_sk\\norder by\\n  c.customer_id nulls first\\nlimit 100;'.
  ```
- `trilogy file write answer_2869182220.preql --content import raw.store_returns as sr;

# Get returns with their customer's demographic profile
with return_dem…e) as full_name,
  return_demos.ticket_number as --ticket_number,
  return_demos.item_sk as --item_sk
order by
  customer_code nulls first
limit 100;`

  ```text
  refused to write 'answer_2869182220.preql': not syntactically valid Trilogy.

  Parse error:
    --> 15:1
     |
  15 | import raw.customer as c
     | ^---
     |
     = expected select_statement, tvf_union_invocation, tvf_except_invocation, or tvf_intersect_invocation
  Location:
  ...ia with edgewood_customers as ??? import raw.customer as c where...

  Write stats: received 1138 chars / 1138 bytes; tail: …'as --item_sk\\norder by\\n  customer_code nulls first\\nlimit 100;'.
  ```
- `trilogy file write answer_2869182220.preql --content import raw.store_returns as sr;
import raw.customer as c;

where
  sr.customer.customer_demographics.dem…--customer_sk
subset join sr.customer.customer_demographics.demo_sk = c.customer_demographics.demo_sk
order by
  customer_code nulls first
limit 100;`

  ```text
  refused to write 'answer_2869182220.preql': not syntactically valid Trilogy.

  Parse error:
    --> 13:23
     |
  13 |   sr.ticket_number as --ticket_number,
     |                       ^---
     |
     = expected IDENTIFIER
  Location:
  ...l_name,   sr.ticket_number as ??? --ticket_number,   sr.item.ite...

  Write stats: received 678 chars / 678 bytes; tail: …'hics.demo_sk\\norder by\\n  customer_code nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.web_returns:wr select distinct wr.returning_customer.customer_demographics.marital_status;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...raw.web_returns as wr; select ??? distinct wr.returning_customer...
  ```
- `trilogy run --import raw.web_returns:wr select distinct wr.returning_customer.customer_demographics.education_status;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...raw.web_returns as wr; select ??? distinct wr.returning_customer...
  ```
- `trilogy run --import raw.web_returns:wr select distinct wr.refunded_customer.customer_address.country;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...raw.web_returns as wr; select ??? distinct wr.refunded_customer....
  ```
- `trilogy run --import raw.web_returns:wr --import raw.web_sales:ws select ws.order_number, ws.sales_price, ws.net_profit, wr.refunded_customer.customer_addres…omer.customer_demographics.education_status = '2 yr Degree' and ws.sales_price between 150 and 200)) order by ws.order_number asc limit 26 offset 25;`

  ```text
  Syntax error in stdin: Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...tem.item_sk = wr.item.item_sk ??? union join ws.order_number = w...
  ```
- `trilogy file write answer_2852230229.preql --content import raw.web_sales as ws;

# Filter to year 2000
where ws.sold_date.year = 2000

# Define reusable con…l desc nulls first,
         case when level = 0 then ws.item.category else null end asc nulls first,
         final_rank asc nulls first
limit 100;
`

  ```text
  refused to write 'answer_2852230229.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block - they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
   concepts for rollup analysis ??? auto total_net_paid <- sum(ws....

  Write stats: received 1282 chars / 1282 bytes; tail: …'nulls first,\\n         final_rank asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_840315271.preql -e -c import raw.store_sales as ss;

# Filter to year 1999
where ss.date_dim.year = 1999

# Items in specified cate…order by
    month_of_year,
    ss.item.category,
    ss.item.class,
    ss.item.brand,
    ss.store.store_name,
    ss.store.company_name
limit 500;`

  ```text
  refused to write 'answer_840315271.preql': not syntactically valid Trilogy.

  Parse error:
   --> 9:1
    |
  9 | where (ss.item.category in ('Books','Electronics','Sports') and ss.item.class in ('computers','stereo','football'))
    | ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
  ...('shirts','birdal','dresses') ??? where (ss.item.category in ('B...

  Write stats: received 880 chars / 880 bytes; tail: …'   ss.store.store_name,\\n    ss.store.company_name\\nlimit 500;'.
  ```
- `trilogy file write answer_2822608529.preql --content import raw.web_sales as ws;

select ws.time_dim.hour, count(ws.order_number) as cnt
where ws.ship_hdemo.dep_count = 6
  and ws.web_page.char_count between 5000 and 5200
group by ws.time_dim.hour
order by ws.time_dim.hour asc
limit 25;`

  ```text
  refused to write 'answer_2822608529.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...r_count between 5000 and 5200 ??? group by ws.time_dim.hour orde...

  Write stats: received 234 chars / 234 bytes; tail: …' by ws.time_dim.hour\\norder by ws.time_dim.hour asc\\nlimit 25;'.
  ```
- `trilogy run --import raw.store_sales:ss --import raw.store_returns:sr select ss.quantity, ss.sales_price, sr.return_quantity, ss.customer.customer_sk where sr.reason.desc = 'reason 28' limit 5 union join ss.ticket_number = sr.ticket_number union join ss.item.item_sk = sr.item.item_sk;`

  ```text
  Syntax error in stdin:  --> 3:116
    |
  3 | select ss.quantity, ss.sales_price, sr.return_quantity, ss.customer.customer_sk where sr.reason.desc = 'reason 28' limit 5 union join ss.ticket_number = sr.ticket_number union join ss.item.item_sk = sr.item.item_sk;
    |                                                                                                                    ^---
    |
    = expected order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
   sr.reason.desc = 'reason 28' ??? limit 5 union join ss.ticket_n...
  ```
- `trilogy run --import raw.store_sales:ss --import raw.store_returns:sr where sr.reason.desc = 'reason 28' select ss.quantity as qty, ss.sales_price as price, …y as ret_qty, ss.customer.customer_sk as cust_id limit 5 union join ss.ticket_number = sr.ticket_number union join ss.item.item_sk = sr.item.item_sk;`

  ```text
  Syntax error in stdin:  --> 3:154
    |
  3 | where sr.reason.desc = 'reason 28' select ss.quantity as qty, ss.sales_price as price, sr.return_quantity as ret_qty, ss.customer.customer_sk as cust_id limit 5 union join ss.ticket_number = sr.ticket_number union join ss.item.item_sk = sr.item.item_sk;
    |                                                                                                                                                          ^---
    |
    = expected metadata, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...stomer.customer_sk as cust_id ??? limit 5 union join ss.ticket_n...
  ```
- `trilogy file write answer_3498056762.preql --content import raw.catalog_sales as cs;

where
    cs.ship_date.year = 2000
    and cs.warehouse.warehouse_sk is…20_plus
order by
    warehouse_name_20 asc nulls first,
    shipping_mode_type asc nulls first,
    call_center_name_lower asc nulls first
limit 100;`

  ```text
  refused to write 'answer_3498056762.preql': not syntactically valid Trilogy.

  Parse error:
    --> 12:26
     |
  12 |     count(cs.order_number, cs.item.item_sk) ? date_diff(cs.sold_date.date, cs.ship_date.date, day) <= 30 as lag_30_or_less,
     |                          ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...wer,     count(cs.order_number ??? , cs.item.item_sk) ? date_diff...

  Write stats: received 1182 chars / 1182 bytes; tail: …'first,\\n    call_center_name_lower asc nulls first\\nlimit 100;'.
  ```

### `syntax-missing-alias`

- `trilogy run -`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `ss.item.category is not null and ss.customer.customer_address.address_sk is not null as ss_item_category_is_not_null_and_ss_cust`
  Location:
   ss.item.category is not null ??? and ss.customer.customer_addre...
  ```
- `trilogy run -`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `ss.item.category is not null and ss.customer.customer_address.address_sk is not null as ss_item_category_is_not_null_and_ss_cust`
  Location:
   ss.item.category is not null ??? and ss.customer.customer_addre...
  ```
- `trilogy file write answer_1835050598.preql --content import raw.catalog_sales as cs;

# Project dimension properties to the line grain so each line is weight…ping(cs.bill_customer.customer_address.state) asc nulls first,
         grouping(cs.bill_customer.customer_address.county) asc nulls first
limit 100;`

  ```text
  refused to write 'answer_1835050598.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `dimensions for display
      cs.bill_customer.customer_address.country as dimensions_for_display_cs_bill_customer_`
  Location:
  ...null  select     --dimensions ??? for display     cs.bill_custom...

  Write stats: received 2079 chars / 2079 bytes; tail: …'customer.customer_address.county) asc nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.store_sales:ss --import raw.store_returns:sr where sr.reason.desc = 'reason 28' select ss.quantity, ss.sales_price, sr.return_quantity, ss.customer.customer_sk limit 5 union join ss.ticket_number = sr.ticket_number union join ss.item.item_sk = sr.item.item_sk;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `ss.customer.customer_sk as ss_customer_customer_sk`
  Location:
  ...tity, ss.customer.customer_sk ??? limit 5 union join ss.ticket_n...
  ```

### `type-error`

- `trilogy run answer_219203908.preql`

  ```text
  Type error in answer_219203908.preql: Invalid argument type 'BIGINT' passed into CONCAT_STRICT function in position 1 from concept: ss.ticket_number. Valid: 'STRING'.
  ```
- `trilogy run answer_3498056762.preql`

  ```text
  Type error in answer_3498056762.preql: Invalid argument type 'BIGINT' passed into CONCAT_STRICT function in position 1 from concept: cs.order_number. Valid: 'STRING'.
  ```

### `cli-misuse`

- `trilogy run - --import raw.store_sales:ss select ss.item.item_sk, substring(ss.item.item_desc, 1, 30), ss.date_dim.date, count(ss.item.item_sk) by (ss.item.item_sk, substring(ss.item.item_desc, 1, 30), ss.date_dim.date) as cnt having cnt > 4 limit 10;`

  ```text
  'select ss.item.item_sk, substring(ss.item.item_desc, 1, 30), ss.date_dim.date, count(ss.item.item_sk) by (ss.item.item_sk, substring(ss.item.item_desc, 1, 30), ss.date_dim.date) as cnt having cnt > 4 limit 10;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy explore raw/`

  ```text
  Invalid value for 'PATH': File 'raw/' is a directory.
  ```

### `join-resolution`

- `trilogy run -`

  ```text
  Resolution error in stdin: Could not resolve connections for query with output ['local.cat<Purpose.PROPERTY>Derivation.BASIC>', 'local.avg_price<Purpose.METRIC>Derivation.BASIC>', 'local.price<Purpose.PROPERTY>Derivation.BASIC>', 'local.is_above<Purpose.PROPERTY>Derivation.BASIC>'] from current model.
  ```
