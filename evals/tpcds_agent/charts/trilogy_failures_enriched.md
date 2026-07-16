# Trilogy failure analysis — 20260715-153056

- Run `20260715-153056` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 1236 | failed: 134 (11%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 72 | 54% |
| `syntax-parse` | 43 | 32% |
| `cli-misuse` | 18 | 13% |
| `type-error` | 1 | 1% |

## Detail

### `other`

- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3863442186.preql`

  ```text
  Syntax error in answer_3863442186.preql: Undefined concept: date.year. Suggestions: ['store.date.year', 'catalog.date.year', 'web.date.year', 'web.ship_date.year', 'web.return_date.year', 'store.return_date.year']
  ```
- `trilogy run answer_3863442186.preql`

  ```text
  Syntax error in answer_3863442186.preql: Undefined concept: local.cid. Suggestions: ['web_annual.cid', 'store_annual.cid', 'catalog_annual.cid']
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_4199102535.preql`

  ```text
  Syntax error in answer_4199102535.preql: 3 undefined concept references; fix all before re-running:
    - ss.customer.sk (line 9, col 12, in SELECT); did you mean: cst.sk, cst.current_household_demographics.sk, cst.first_shipto_date.sk, cst.current_demographics.sk, cst.first_sales_date.sk, cst.current_household_demographics.income_band.sk?
    - ss.date.year (line 10, col 11, in WHERE); did you mean: ss.date.month_of_year, cst.first_sales_date.year, cst.first_shipto_date.year?
    - ss.date.month_of_year (line 11, col 11, in WHERE); did you mean: ss.date.year, cst.first_sales_date.month_of_year, cst.first_shipto_date.month_of_year?
  ```
- `trilogy run answer_3697440276.preql`

  ```text
  Syntax error in answer_3697440276.preql: 3 undefined concept references; fix all before re-running:
    - first_name (line 57, col 5, in ORDER BY); did you mean: store.customer.first_name, web.ship_customer.first_name, web.return_customer.first_name, web.billing_customer.first_name, store.return_customer.first_name, web.return_refund_customer.first_name?
    - last_name (line 58, col 5, in ORDER BY); did you mean: store.customer.last_name, web.ship_customer.last_name, web.return_customer.last_name, web.billing_customer.last_name, store.return_customer.last_name, web.return_refund_customer.last_name?
    - preferred_cust_flag (line 59, col 5, in ORDER BY); did you mean: store.customer.preferred_cust_flag, web.ship_customer.preferred_cust_flag, web.return_customer.preferred_cust_flag, web.billing_customer.preferred_cust_flag, store.return_customer.preferred_cust_flag, web.return_refund_customer.preferred_cust_flag?
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_219203908.preql`

  ```text
  Syntax error in answer_219203908.preql: `by rollup (…)` cannot re-aggregate the bare measure reference `nov2001_leaves.total_sales`. Alias it with an explicit aggregate (e.g. `sum(nov2001_leaves.total_sales) as total_sales_total`).
  ```
- `trilogy run answer_219203908.preql`

  ```text
  Syntax error in answer_219203908.preql: 4 undefined concept references; fix all before re-running:
    - channel (line 64, col 10, in ORDER BY); did you mean: sales.channel, nov2001_leaves.channel, kept_leaves.nov2001_leaves.channel?
    - brand_id (line 64, col 35, in ORDER BY); did you mean: sales.item.brand_id, nov2001_leaves.brand_id, qualifying_tuples.brand_id, kept_leaves.nov2001_leaves.brand_id?
    - class_id (line 64, col 61, in ORDER BY); did you mean: sales.item.class_id, nov2001_leaves.class_id, qualifying_tuples.class_id, kept_leaves.nov2001_leaves.class_id?
    - category_id (line 64, col 87, in ORDER BY); did you mean: sales.item.category_id, nov2001_leaves.category_id, qualifying_tuples.category_id, kept_leaves.nov2001_leaves.category_id?
  ```
- `trilogy run answer_219203908.preql`

  ```text
  Resolution error in answer_219203908.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 15). The requested concepts split into 2 disconnected subgraphs: {channel_items.item_sk}; {_qualifying_tuples_brand_id, _qualifying_tuples_category_id, _qualifying_tuples_class_id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read answer_219203908.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read answer_1455459008.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw/catalog_sales:catalog_sales select count_distinct(catalog_sales.order_number) as multi_wh_orders where catalog_sales.ship_date.date …ll_center.county = 'Williamson County' and catalog_sales.order_number in (select order_number where count_distinct(catalog_sales.warehouse.sk) >= 2);`

  ```text
  Syntax error in stdin: Undefined concept: local.order_number (line 2, col 331, in SELECT). Suggestions: ['catalog_sales.order_number']
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run answer_2604809012.preql`

  ```text
  Resolution error in answer_2604809012.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 5). The requested concepts split into 3 disconnected subgraphs: {item.current_price, item_code}; {after_total, before_total}; {wh_name}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read answer_2928586490.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_1798498862.preql`

  ```text
  Syntax error in answer_1798498862.preql: Comparison `ss.return_date.month_of_year <= 12` matches every value of enum field 'ss.return_date.month_of_year', which contains only these values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12. It is always true and should be removed.
  ```
- `trilogy file read answer_1798498862.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3347758002.preql`

  ```text
  Syntax error in answer_3347758002.preql: Undefined concept: items.category. Suggestions: ['sales.item.category', 'sales.item.category_id']
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_142117636.preql`

  ```text
  Resolution error in answer_142117636.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 8). The requested concepts split into 4 disconnected subgraphs: {d.year, _monthly_stats_mon}; {_monthly_stats_item_sk}; {_monthly_stats_mn, _monthly_stats_sd}; {_monthly_stats_warehouse_sk}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe catalog_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run --import raw.item:item select item.category, item.color, item.units, item.size, count(item.sk) as cnt where (item.category = 'Books' and item.col…'N/A') or (item.category = 'Electronics' and item.color = 'mint' and item.units = 'Gross' and item.size = 'N/A') order by item.product_name limit 20;`

  ```text
  Syntax error in stdin: ORDER BY references 'item.product_name', which is not in the SELECT projection (line 2). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --item.product_name order by item.product_name asc`.
  ```
- `trilogy run answer_968172492.preql`

  ```text
  Syntax error in answer_968172492.preql: 2 undefined concept references; fix all before re-running:
    - item.id (line 11, col 14, in SELECT); did you mean: ws.item.id, item.sk, ws.date.id, ws.time.id, ws.web_site.id, ws.ship_date.id?
    - item.sk (line 12, col 13, in WHERE); did you mean: ws.item.sk, item.id, ws.date.sk, ws.time.sk, ws.web_site.sk, ws.ship_date.sk?
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3560698360.preql`

  ```text
  Resolution error in answer_3560698360.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced: {web_run.web_daily.d, web_run.web_daily.item_sk, web_run.web_rt}; still unresolved: {local.web_rm}
  ```
- `trilogy run answer_3560698360.preql`

  ```text
  Resolution error in answer_3560698360.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced: {web_run.web_daily.d, web_run.web_daily.item_sk, web_run.web_rt}; still unresolved: {local.web_rm}
  ```
- `trilogy file read answer_3560698360.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Unterminated string starting at: line 1 column 62 (char 61). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file read answer_71623752.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3553309440.preql`

  ```text
  Resolution error in answer_3553309440.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 49). The requested concepts split into 2 disconnected subgraphs: {customer_sk, total_store_sales, ss.customer.current_address.county, ss.customer.current_address.state, ss.customer.sk, ss.date.month_seq, ss.store.county, ss.store.state}; {ms_max, ms_min}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_3553309440.preql`

  ```text
  Resolution error in answer_3553309440.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 55). The requested concepts split into 2 disconnected subgraphs: {customer_sk, total_store_sales, ss.customer.current_address.county, ss.customer.current_address.state, ss.customer.sk, ss.date.month_seq, ss.store.county, ss.store.state}; {window_end, window_start}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_3553309440.preql`

  ```text
  Resolution error in answer_3553309440.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 20). The requested concepts split into 2 disconnected subgraphs: {cs.item.category, cs.item.class, cs.sold_date.month_seq, _catalog_qualifiers_cust_sk}; {dec1998_month.ms}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read answer_3247656127.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run answer_2986518257.preql`

  ```text
  Resolution error in answer_2986518257.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {sequenced.monthly_totals.brand, sequenced.monthly_totals.call_center_name, sequenced.monthly_totals.category, sequenced.monthly_totals.mo, sequenced.monthly_totals.monthly_total, sequenced.monthly_totals.yr, yearly_avgs.avg_monthly_sales}
  ```
- `trilogy run answer_1623435181.preql`

  ```text
  Syntax error in answer_1623435181.preql: 9 undefined concept references; fix all before re-running:
    - local.store_name (line 15, col 5, in SELECT); did you mean: weekly_prices.store_name, ss.store.name, store_code, ss.store.street_name?
    - local.store_code (line 16, col 5, in SELECT); did you mean: weekly_prices.store_code, store_name, ss.store_credit?
    - local.week_seq (line 17, col 5, in SELECT); did you mean: ss.date.week_seq, weekly_prices.week_seq, ss.return_date.week_seq, ss.customer.first_sales_date.week_seq, ss.customer.first_shipto_date.week_seq, ss.return_customer.first_sales_date.week_seq?
    - local.year (line 18, col 5, in SELECT); did you mean: ss.date.year, weekly_prices.year, ss.return_date.year, ss.customer.first_sales_date.year, ss.customer.first_shipto_date.year, ss.return_customer.first_sales_date.year?
    - local.dow (line 19, col 5, in SELECT); did you mean: weekly_prices.dow?
    - local.unit_price_sum (line 20, col 5, in SELECT); did you mean: weekly_prices.unit_price_sum?
    - local.store_name (line 21, col 10, in ORDER BY); did you mean: weekly_prices.store_name, ss.store.name, store_code, ss.store.street_name?
    - local.store_code (line 21, col 22, in ORDER BY); did you mean: weekly_prices.store_code, store_name, ss.store_credit?
    - local.week_seq (line 21, col 34, in ORDER BY); did you mean: ss.date.week_seq, weekly_prices.week_seq, ss.return_date.week_seq, ss.customer.first_sales_date.week_seq, ss.customer.first_shipto_date.week_seq, ss.return_customer.first_sales_date.week_seq?
  ```
- `trilogy run answer_345752060.preql`

  ```text
  Syntax error in answer_345752060.preql: Undefined concept: item.category. Suggestions: ['sales.item.category', 'sales.item.category_id']
  ```
- `trilogy database describe web_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 1 column 1208 (char 1207). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run answer_3544057080.preql`

  ```text
  Syntax error in answer_3544057080.preql: Ambiguous reference 'store_agg_1999.zip': matches ['store_agg_1999.store_sales.customer.current_address.zip', 'store_agg_1999.store_sales.pos_address.zip', 'store_agg_1999.store_sales.store.zip']. Qualify the full path to disambiguate.
  ```
- `trilogy file read answer_3979964698.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_755724379.preql`

  ```text
  Resolution error in answer_755724379.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 36). The requested concepts split into 2 disconnected subgraphs: {cnt1, cnt2, cnt3, store_customers.cust_sk}; {credit_rating, education_status, gender, marital_status, purchase_estimate}. Are you missing a join or merge statement to relate them?
  Note: the membership predicate(s) `(store_customers.cust_sk) not in (web_billing_custs.cust_sk)`; `(store_customers.cust_sk) not in (catalog_ship_custs.cust_sk)` span these subgraphs, but membership only filters rows on its left side — it does not join the two sides, so it cannot relate them for outputs or grouping. To combine values from both sides, author a query-scoped join or a merge on shared keys.
  ```
- `trilogy file read raw/all_sales.preql`

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
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw/store_sales:ss --import raw/item:item select item.id, sum(ss.return_quantity) as store_qty, count(ss.is_returned ? ss.return_date.week_seq in (5244, 5257, 5264)) as store_return_count where ss.is_returned and ss.return_date.week_seq in (5244, 5257, 5264);`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {item.id}; {store_qty, store_return_count, ss.is_returned, ss.return_date.week_seq}.
    - `item.id` is disconnected — did you mean `ss.item.id`? (connected to the other concepts)
  These look like separately-imported copies of models already reachable through a connected import; chain through that path (e.g. `ss.item.id`) instead of importing a second, disconnected copy.
  ```
- `trilogy run answer_4111870542.preql`

  ```text
  Syntax error in answer_4111870542.preql: 3 undefined concept references; fix all before re-running:
    - local.store_records (line 40, col 9, in WHERE); did you mean: store_returns.store_records, web_records, catalog_records?
    - local.catalog_records (line 41, col 9, in WHERE); did you mean: catalog_returns.catalog_records, store_records, catalog_pct, catalog_return_qty?
    - local.web_records (line 42, col 9, in WHERE); did you mean: web_returns.web_records, store_records, catalog_records?
  ```
- `trilogy run answer_2869182220.preql`

  ```text
  Resolution error in answer_2869182220.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 9). The requested concepts split into 2 disconnected subgraphs: {c.current_address.city, c.current_demographics.sk, c.current_household_demographics.income_band.lower_bound, c.current_household_demographics.income_band.upper_bound, customer_code, full_name}; {ss.return_customer_demographic.sk}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read answer_2869182220.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
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
- `trilogy run --import raw.store_sales:store_sales select return_reason.desc limit 30;`

  ```text
  Syntax error in stdin: Undefined concept: return_reason.desc (line 2, col 8, in SELECT). Suggestions: ['store_sales.return_reason.desc', 'store_sales.item.desc', 'store_sales.return_reason.sk', 'store_sales.return_reason.id']
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw/web_sales:ws select ws.order_number, count(ws.line_item) as total_lines where ws.order_number in (select ws2.order_number where ws2.….ship_date.date between '1999-02-01'::date and '1999-04-02'::date and ws.pos_ship_address.state = 'IL' and ws.web_site.company_name = 'pri' limit 10;`

  ```text
  Syntax error in stdin: 3 undefined concept references; fix all before re-running:
    - ws2.order_number (line 2, col 93, in SELECT); did you mean: ws.order_number?
    - ws2.is_returned (line 2, col 116, in WHERE); did you mean: ws.is_returned, ws.return_fee, ws.return_time.id?
    - ws2.warehouse.sk (line 2, in WHERE); did you mean: ws.warehouse.sk, ws.web_site.sk, ws.date.sk, ws.return_date.sk, ws.return_time.sk, ws.web_page.sk?
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run raw/web_sales.preql duckdb`

  ```text
  Syntax error in raw\web_sales.preql: Nothing was executed: parsed 38 definition statement(s) (28 imports, 7 concepts, 2 datasources, 1 property) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```

### `syntax-parse`

- `trilogy run --import raw/all_sales:all_sales select all_sales.channel, all_sales.channel_dim_text_id, count(all_sales.item.sk) as cnt by all_sales.channel, all_sales.channel_dim_text_id limit 20;`

  ```text
  Syntax error in stdin:  --> 2:90
    |
  2 | select all_sales.channel, all_sales.channel_dim_text_id, count(all_sales.item.sk) as cnt by all_sales.channel, all_sales.channel_dim_text_id limit 20;
    |                                                                                          ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...unt(all_sales.item.sk) as cnt ??? by all_sales.channel, all_sale...
  ```
- `trilogy run --import raw/all_sales:all_sales with combined as union((
  select all_sales.channel, all_sales.channel_dim_text_id as entity_id,
    sum(all_sal…)
select combined.channel, combined.entity_id, sum(combined.s) as s, sum(combined.r) as r, sum(combined.np) as np, sum(combined.rnl) as rnl limit 10;`

  ```text
  Syntax error in stdin: Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
   r float, np float, rnl float) ??? select combined.channel, comb...
  ```
- `trilogy file write answer_4199102535.preql --content import raw.customer as cst;

# Customers whose current address is in one of the specified counties
where…demographics.dependent_count,
    cst.current_demographics.employed_dependent_count,
    cst.current_demographics.college_dependent_count
limit 100;
`

  ```text
  refused to write 'answer_4199102535.preql': not syntactically valid Trilogy.

  Parse error:
   --> 9:12
    |
  9 |     import raw.store_sales as ss;
    |            ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
     and cst.sk in (     import ??? raw.store_sales as ss;     sel...

  Write stats: received 2031 chars / 2031 bytes; tail: …'cst.current_demographics.college_dependent_count\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_219203908.preql -e -c import raw.all_sales as sales;

# Step 1: Find (brand_id, class_id, category_id) tuples for items that
# appe…eaves.category_id) = 1)
order by channel asc nulls first, brand_id asc nulls first, class_id asc nulls first, category_id asc nulls first
limit 100;
`

  ```text
  refused to write 'answer_219203908.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ....sk as item_sk) ) -> (item_sk) ???  # Step 2: Get distinct (bran...

  Write stats: received 3016 chars / 3016 bytes; tail: …'_id asc nulls first, category_id asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw/catalog_sales:catalog_sales select count_distinct(catalog_sales.order_number) as order_count, sum(catalog_sales.ext_ship_cost) as to…count, sum(case when catalog_sales._returned_order_number is not null then 1 else 0 end) as return_count
having wh_count >= 2 and return_count = 0
);`

  ```text
  Syntax error in stdin:  --> 8:1
    |
  8 | select catalog_sales.order_number, count_distinct(catalog_sales.warehouse.sk) as wh_count, sum(case when catalog_sales._returned_order_number is not null then 1 else 0 end) as return_count
    | ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ....county = 'Williamson County' ??? select catalog_sales.order_num...
  ```
- `trilogy file write answer_765177085.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.item as item;
import raw.store a…store_sales_in_2001.store.state,
    store_sales_in_2001.quantity as store_qty,
    store_returns_in_2001_2002.return_quantity as store_return_qty
;
`

  ```text
  refused to write 'answer_765177085.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [226]: Misplaced `subset|union join`. The key looks fine - the join is in the wrong PLACE. A query-scoped join is part of a `select` statement, not a standalone statement and not a pre-`where` clause. Put it right after the select list (preferred, SQL-like): `where <filters> select <cols> subset join a.key = b.key`. The clause order is `where` -> `select` <cols> -> join(s) -> `having` -> `order by` -> `limit`; a join may also sit between `where` and `select`, but never before `where` and never on its own. Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...nditions) with store_items as ??? union join store_sales_in_2001...

  Write stats: received 2150 chars / 2150 bytes; tail: …'_returns_in_2001_2002.return_quantity as store_return_qty\\n;\\n'.
  ```
- `trilogy run --import raw.store_sales:store select store.item.sk where store.date.year in (2000,2001,2002,2003) and count(store.ticket_number) by store.item.sk, store.item.id, substring(store.item.desc,1,30), store.date.sk > 4 select distinct store.item.sk limit 100;`

  ```text
  Syntax error in stdin:  --> 2:142
    |
  2 | select store.item.sk where store.date.year in (2000,2001,2002,2003) and count(store.ticket_number) by store.item.sk, store.item.id, substring(store.item.desc,1,30), store.date.sk > 4 select distinct store.item.sk limit 100;
    |                                                                                                                                              ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, select_grouping, window_sql_over, or OVER_COMPONENT_REF
  Location:
  ...m.sk, store.item.id, substring ??? (store.item.desc,1,30), store....
  ```
- `trilogy run --import raw.store_sales:store rowset frequent_groups <- where store.date.year in (2000,2001,2002,2003) select store.item.sk as item_sk, store.it…t(store.ticket_number) > 4; rowset frequent_items <- select distinct frequent_groups.item_sk as item_sk; select count(frequent_items.item_sk) as cnt;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...wset frequent_items <- select ??? distinct frequent_groups.item_...
  ```
- `trilogy run --import raw.store_sales:store rowset frequent_groups <- where store.date.year in (2000,2001,2002,2003) select store.item.id as item_id, substrin… order_count having count(store.ticket_number) > 4; where store.item.id in frequent_groups.item_id select distinct store.item.sk as item_sk limit 10;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...requent_groups.item_id select ??? distinct store.item.sk as item...
  ```
- `trilogy run --import raw/store_sales:ss --import raw/catalog_sales:cs select ss.item.id, ss.quantity, cs.quantity, cs.sold_date.year, ss.store.id where ss.da…f_year>=9 and cs.sold_date.year in (1999, 2000, 2001) union join ss.customer.sk = cs.billing_customer.sk union join ss.item.sk = cs.item.sk limit 10;`

  ```text
  Syntax error in stdin:  --> 3:273
    |
  3 | select ss.item.id, ss.quantity, cs.quantity, cs.sold_date.year, ss.store.id where ss.date.year=1999 and ss.date.month_of_year=9 and ss.return_quantity is not null and ss.return_date.year=1999 and ss.return_date.month_of_year>=9 and cs.sold_date.year in (1999, 2000, 2001) union join ss.customer.sk = cs.billing_customer.sk union join ss.item.sk = cs.item.sk limit 10;
    |                                                                                                                                                                                                                                                                                 ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...te.year in (1999, 2000, 2001) ??? union join ss.customer.sk = cs...
  ```
- `trilogy file write answer_751385098.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

with matching … ws.billing_customer.first_name,
      ws.date.date
  )
) -> (last_name, first_name, sale_date)

select
  count(matching.sale_date) as cnt
limit 100;`

  ```text
  refused to write 'answer_751385098.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...t_name, first_name, sale_date) ???  select   count(matching.sale...

  Write stats: received 758 chars / 758 bytes; tail: …'_date)\\n\\nselect\\n  count(matching.sale_date) as cnt\\nlimit 100;'.
  ```
- `trilogy file write answer_1663418777.preql`

  ```text
  refused to write 'answer_1663418777.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
   target_manufacturers
   select ??? distinct item.product_name as

  Write stats: received 1427 chars / 1427 bytes; tail: …'name as product_name\r\\norder by item.product_name\r\\nlimit 100;'.
  ```
- `trilogy run --import raw.item:item select item.category, item.color, item.units, item.size, count(item.sk) as cnt where item.category in ('Books','Electronics','Men') group by item.category, item.color, item.units, item.size order by cnt desc limit 50;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...('Books','Electronics','Men') ??? group by item.category, item.c...
  ```
- `trilogy file write answer_2940558602.preql --content import raw.store_sales as store;
import raw.web_sales as web;
import raw.catalog_sales as catalog;

# --…rn_rank <= 10 or currency_rank <= 10
order by
    combined.channel asc,
    return_rank asc,
    currency_rank asc,
    combined.item asc
limit 100;
`

  ```text
  refused to write 'answer_2940558602.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
   return_ratio, currency_ratio) ???  select     combined.channel,...

  Write stats: received 2362 chars / 2362 bytes; tail: …'sc,\\n    currency_rank asc,\\n    combined.item asc\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_3560698360.preql -e -c import raw.web_sales as ws;
import raw.store_sales as ss;

# Web: per-item per-date aggregated unit sales pr…n.item_sk
union join web_run.d = store_run.d
having web_rm > store_rm
order by web_run.item_sk asc nulls first, web_run.d asc nulls first
limit 100;
`

  ```text
  refused to write 'answer_3560698360.preql': not syntactically valid Trilogy.

  Parse error:
    --> 44:60
     |
  44 |     max(web_run.web_rt) over (partition by web_run.item_sk order by web_run.d asc rows between unbounded preceding and current row) as web_rm,
     |                                                            ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...(partition by web_run.item_sk ??? order by web_run.d asc rows be...

  Write stats: received 1625 chars / 1625 bytes; tail: …'em_sk asc nulls first, web_run.d asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_3560698360.preql -e -c import raw.web_sales as ws;
import raw.store_sales as ss;

# Web: per-item per-date aggregated unit sales pr…union join web_maxed.d = store_maxed.d
having web_maxed.web_rm > store_maxed.store_rm
order by item_sk asc nulls first, d asc nulls first
limit 100;
`

  ```text
  refused to write 'answer_3560698360.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...n web_maxed.d = store_maxed.d ??? union join web_maxed.item_sk =...

  Write stats: received 2087 chars / 2087 bytes; tail: …'er by item_sk asc nulls first, d asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw.store_sales:ss select distinct ss.item.brand_name where ss.item.category in ('Books', 'Children', 'Electronics') limit 20;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...raw.store_sales as ss; select ??? distinct ss.item.brand_name wh...
  ```
- `trilogy file write answer_3553309440.preql --content # answer_3553309440.preql
# Identify customers who bought Women/maternity items via catalog or web in De…xt_sales_price / 50) * 50 as segment_times_50
order by
  segment asc nulls first,
  customer_count asc nulls first,
  segment_times_50 asc
limit 100;`

  ```text
  refused to write 'answer_3553309440.preql': not syntactically valid Trilogy.

  Parse error:
    --> 75:38
     |
  75 |   count(grain(customer_totals.cust_sk)) as customer_count,
     |                                      ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...(grain(customer_totals.cust_sk ??? )) as customer_count,   round(...

  Write stats: received 2699 chars / 2699 bytes; tail: …'mer_count asc nulls first,\\n  segment_times_50 asc\\nlimit 100;'.
  ```
- `trilogy file write answer_3553309440.preql --content # answer_3553309440.preql
# Identify customers who bought Women/maternity items via catalog or web in De…ty
  and ss.store.state = ss.customer.current_address.state
select
  ss.customer.sk as customer_sk,
  sum(ss.ext_sales_price) as total_store_sales
;
`

  ```text
  refused to write 'answer_3553309440.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [226]: Misplaced `subset|union join`. The key looks fine - the join is in the wrong PLACE. A query-scoped join is part of a `select` statement, not a standalone statement and not a pre-`where` clause. Put it right after the select list (preferred, SQL-like): `where <filters> select <cols> subset join a.key = b.key`. The clause order is `where` -> `select` <cols> -> join(s) -> `having` -> `order by` -> `limit`; a join may also sit between `where` and `select`, but never before `where` and never on its own. Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
   ext_sales_price per customer ??? union join dec1998_m.ms = ss.d...

  Write stats: received 1926 chars / 1926 bytes; tail: …'stomer_sk,\\n  sum(ss.ext_sales_price) as total_store_sales\\n;\\n'.
  ```
- `trilogy file write answer_3553309440.preql --content # answer_3553309440.preql
# Identify customers who bought Women/maternity items via catalog or web in De…l_store_sales / 50) * 50 as segment_times_50
order by
  segment asc nulls first,
  customer_count asc nulls first,
  segment_times_50 asc
limit 100;
`

  ```text
  refused to write 'answer_3553309440.preql': not syntactically valid Trilogy.

  Parse error:
    --> 58:39
     |
  58 |   count(grain(store_totals.customer_sk)) as customer_count,
     |                                       ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...grain(store_totals.customer_sk ??? )) as customer_count,   round(...

  Write stats: received 2149 chars / 2149 bytes; tail: …'er_count asc nulls first,\\n  segment_times_50 asc\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw.store_sales:ss --import raw.catalog_sales:cs --import raw.web_sales:ws select ss.customer.sk, sum(ss.ext_sales_price) as total where… = 'maternity' and cs.sold_date.year = 1998 and cs.sold_date.month_of_year = 12) and ss.date.month_seq between 1188 and 1190 group by ss.customer.sk;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...nth_seq between 1188 and 1190 ??? group by ss.customer.sk;
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.item.color, count(grain(all_sales.item.sk)) as cnt where all_sales.item.color in ('slate', 'blanched', 'burnished') limit 5;`

  ```text
  Syntax error in stdin:  --> 2:59
    |
  2 | select all_sales.item.color, count(grain(all_sales.item.sk)) as cnt where all_sales.item.color in ('slate', 'blanched', 'burnished') limit 5;
    |                                                           ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
   count(grain(all_sales.item.sk ??? )) as cnt where all_sales.item...
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.item.color, count(grain(all_sales.item.sk)) as cnt group by all_sales.item.color order by all_sales.item.color limit 100;`

  ```text
  Syntax error in stdin:  --> 2:59
    |
  2 | select all_sales.item.color, count(grain(all_sales.item.sk)) as cnt group by all_sales.item.color order by all_sales.item.color limit 100;
    |                                                           ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
   count(grain(all_sales.item.sk ??? )) as cnt group by all_sales.i...
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.item.id, all_sales.item.color where all_sales.item.color in ('slate', 'blanched', 'burnished') group by all_sales.item.id, all_sales.item.color limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...te', 'blanched', 'burnished') ??? group by all_sales.item.id, al...
  ```
- `trilogy run --import raw/all_sales.preql:sales select sales.channel, sum(sales.ext_sales_price) as total, sales.item.id where sales.date.week_seq = 5218 group by sales.channel, sales.item.id limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...re sales.date.week_seq = 5218 ??? group by sales.channel, sales....
  ```
- `trilogy file write answer_145690531.preql --content import raw/all_sales.preql as sales;

# Get the week_seq for 2000-01-03
auto target_week <- 5218;

# Per-…total as web_total
where store_totals.store_total is not null
  and catalog_totals.catalog_total is not null
  and web_totals.web_total is not null
;`

  ```text
  refused to write 'answer_145690531.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/all_sales.preql as sales;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/all_sales.preql as sales;

  Write stats: received 1119 chars / 1119 bytes; tail: …'g_total is not null\\n  and web_totals.web_total is not null\\n;'.
  ```
- `trilogy file write answer_1623435181.preql --content import raw.store_sales as ss;

def day_sum(d) -> sum(ss.sales_price ? ss.date.day_of_week = d);
def day_… weekly_prices.year = 2001 or weekly_prices.year = 2002
order by weekly_prices.store_name, weekly_prices.store_code, weekly_prices.week_seq
limit 30;`

  ```text
  refused to write 'answer_1623435181.preql': not syntactically valid Trilogy.

  Parse error:
    --> 13:24
     |
  13 |     case when day_count(0) > 0 then day_sum(0) end as sun_total,
     |                        ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
   year,     case when day_count ??? (0) > 0 then day_sum(0) end as...

  Write stats: received 1050 chars / 1050 bytes; tail: …', weekly_prices.store_code, weekly_prices.week_seq\\nlimit 30;'.
  ```
- `trilogy file write answer_1623435181.preql --content import raw.store_sales as ss;

# Per (store, week_seq) sum of unit price by day-of-week, null when no sa… weekly_prices.year = 2001 or weekly_prices.year = 2002
order by weekly_prices.store_name, weekly_prices.store_code, weekly_prices.week_seq
limit 30;`

  ```text
  refused to write 'answer_1623435181.preql': not syntactically valid Trilogy.

  Parse error:
    --> 21:1
     |
  21 | where weekly_prices.year = 2001 or weekly_prices.year = 2002
     | ^---
     |
     = expected access_chain
  Location:
  ...;  select     weekly_prices.* ??? where weekly_prices.year = 200...

  Write stats: received 1384 chars / 1384 bytes; tail: …', weekly_prices.store_code, weekly_prices.week_seq\\nlimit 30;'.
  ```
- `trilogy file write answer_1623435181.preql --content import raw.store_sales as ss;

# Per (store, week_seq) sum of unit price by day-of-week, null when no sa…or weekly_prices.year = 2002
select
    weekly_prices.*
order by weekly_prices.store_name, weekly_prices.store_code, weekly_prices.week_seq
limit 30;`

  ```text
  refused to write 'answer_1623435181.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [213]: A `by <grain>` clause must follow an aggregate, but the expression before it has none. If the `by` sits inside an aggregate's parentheses (`max(x by *)`), move it outside the call: `max(x) by *`. To take each distinct value once per grain, wrap it in `group(...)` - e.g. `group(item.current_price) by item.id, item.category`. For a reduction, use an aggregate: `sum(x) by ...`, `avg(x) by ...`, `max(x) by ...`.
  Location:
  ...ect     weekly_prices.* order ??? by weekly_prices.store_name, w...

  Write stats: received 1384 chars / 1384 bytes; tail: …', weekly_prices.store_code, weekly_prices.week_seq\\nlimit 30;'.
  ```
- `trilogy file write answer_4037365554.preql --content import raw.store_sales as ss;

# Filter conditions that apply to ALL computations
where ss.date.year = 2…nthly_sales
having
    avg_monthly_sales > 0
    and rel_dev > 0.1
order by
    ss.item.manager_id,
    avg_monthly_sales,
    month_total
limit 100;`

  ```text
  refused to write 'answer_4037365554.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block - they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...#1', 'importoamalg #1'))   )  ??? auto month_total <- sum(ss.sal...

  Write stats: received 1136 chars / 1136 bytes; tail: …'anager_id,\\n    avg_monthly_sales,\\n    month_total\\nlimit 100;'.
  ```
- `trilogy file write answer_3544057080.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;

# Rowset 1: per-item …duct_name asc,
    store_agg_1999.name asc,
    store_agg_2000.cnt asc,
    store_agg_1999.wholesale_sum asc,
    store_agg_2000.wholesale_sum asc
;
`

  ```text
  refused to write 'answer_3544057080.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [226]: Misplaced `subset|union join`. The key looks fine - the join is in the wrong PLACE. A query-scoped join is part of a `select` statement, not a standalone statement and not a pre-`where` clause. Put it right after the select list (preferred, SQL-like): `where <filters> select <cols> subset join a.key = b.key`. The clause order is `where` -> `select` <cols> -> join(s) -> `having` -> `order by` -> `limit`; a join may also sit between `where` and `select`, but never before `where` and never on its own. Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...'s count <= first row's count ??? union join store_agg_1999.id =...

  Write stats: received 4177 chars / 4177 bytes; tail: …'9.wholesale_sum asc,\\n    store_agg_2000.wholesale_sum asc\\n;\\n'.
  ```
- `trilogy file write answer_3979964698.preql --content import raw.all_sales as s;

# Channel-specific line-level values
# Web: monthly sales = quantity * ext_s…t(10) as oct_net,
    @mon_net(11) as nov_net,
    @mon_net(12) as dec_net
order by s.warehouse.name nulls first, s.date.year nulls first
limit 100;
`

  ```text
  refused to write 'answer_3979964698.preql': not syntactically valid Trilogy.

  Parse error:
    --> 21:35
     |
  21 | def mon_sales_sqft(m) -> mon_sales(m) / s.warehouse.square_feet;
     |                                   ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...mon_sales_sqft(m) -> mon_sales ??? (m) / s.warehouse.square_feet;...

  Write stats: received 2757 chars / 2757 bytes; tail: …'ehouse.name nulls first, s.date.year nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_755724379.preql --content import raw.store_sales as sales;
import raw.web_sales as ws;
import raw.catalog_sales as cs;

# Customers…grain(store_customers.cust_sk)) as cnt3
order by
   gender,
   marital_status,
   education_status,
   purchase_estimate,
   credit_rating
limit 100;`

  ```text
  refused to write 'answer_755724379.preql': not syntactically valid Trilogy.

  Parse error:
    --> 45:39
     |
  45 |    count(grain(store_customers.cust_sk)) as cnt1,
     |                                       ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...(grain(store_customers.cust_sk ??? )) as cnt1,    count(grain(sto...

  Write stats: received 1659 chars / 1659 bytes; tail: …'on_status,\\n   purchase_estimate,\\n   credit_rating\\nlimit 100;'.
  ```
- `trilogy file write answer_2091535883.preql -c import raw.catalog_sales as cs;
import raw.inventory as inv;

# Filter catalog sales for 1999 with household bu…_seq)) as total_count
order by
  total_count desc,
  matched.item_desc asc,
  matched.wh_name asc,
  matched.sold_week_seq asc nulls first
limit 100;`

  ```text
  refused to write 'answer_2091535883.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [226]: Misplaced `subset|union join`. The key looks fine - the join is in the wrong PLACE. A query-scoped join is part of a `select` statement, not a standalone statement and not a pre-`where` clause. Put it right after the select list (preferred, SQL-like): `where <filters> select <cols> subset join a.key = b.key`. The clause order is `where` -> `select` <cols> -> join(s) -> `having` -> `order by` -> `limit`; a join may also sit between `where` and `select`, but never before `where` and never on its own. Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...sale quantity with matched as ??? union join cs_filtered.item_sk...

  Write stats: received 1555 chars / 1555 bytes; tail: …'name asc,\\n  matched.sold_week_seq asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write answer_3046445280.preql --content import raw.all_sales as sales;

# Filter to Books category
where sales.item.category = 'Books'

# Comput…amt as amt_diff
having
    y2001.qty_2001 > 0
    and (y2002.qty_2002 / y2001.qty_2001) < 0.9
order by
    qty_diff asc,
    amt_diff asc
limit 100;
`

  ```text
  refused to write 'answer_3046445280.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block - they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
   # Compute net values per row ??? auto net_qty <- case when sale...

  Write stats: received 2061 chars / 2061 bytes; tail: …' 0.9\\norder by\\n    qty_diff asc,\\n    amt_diff asc\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_3046445280.preql --content import raw.all_sales as sales;

# Compute net values per row
auto net_qty <- case when sales.quantity is…001 as amt_diff
having
    y2001.qty_2001 > 0
    and (y2002.qty_2002 / y2001.qty_2001) < 0.9
order by
    qty_diff asc,
    amt_diff asc
limit 100;
`

  ```text
  refused to write 'answer_3046445280.preql': not syntactically valid Trilogy.

  Parse error:
    --> 11:1
     |
  11 | with yearly as
     | ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
  ...totals per year and dimension ??? with yearly as select     sale...

  Write stats: received 2059 chars / 2059 bytes; tail: …' 0.9\\norder by\\n    qty_diff asc,\\n    amt_diff asc\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_747155668.preql --content import raw.store_sales as store;
import raw.web_sales as web;
import raw.catalog_sales as catalog;

with …nulls first,
    combined.missing_ref nulls first,
    combined.yr nulls first,
    combined.qtr nulls first,
    combined.cat nulls first
limit 100;`

  ```text
  refused to write 'answer_747155668.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block - they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...icket_number is not null      ??? auto store_line_flag <- sum(st...

  Write stats: received 2324 chars / 2324 bytes; tail: …'ned.qtr nulls first,\\n    combined.cat nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw/store_sales:store select distinct store.store.id limit 10;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ....store_sales as store; select ??? distinct store.store.id limit
  ```
- `trilogy run --import raw/all_sales:sales select distinct sales.channel_dim_text_id where sales.channel = 'STORE' limit 10;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...aw.all_sales as sales; select ??? distinct sales.channel_dim_tex...
  ```
- `trilogy file write answer_4111870542.preql --content import raw/store_sales as ss;
import raw/catalog_sales as cs;
import raw/web_sales as ws;

# Define the …turns.item_id
union join store_returns.item_id = web_returns.item_id
order by item_code asc nulls first, store_return_qty asc nulls first
limit 100
;`

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

  Write stats: received 2762 chars / 2762 bytes; tail: …'sc nulls first, store_return_qty asc nulls first\\nlimit 100\\n;'.
  ```
- `trilogy run --import raw.store_sales:store_sales select distinct return_reason.desc limit 30;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ..._sales as store_sales; select ??? distinct return_reason.desc li...
  ```
- `trilogy run --import raw/web_sales:ws select ws.order_number, count_distinct(ws.warehouse.sk) as num_warehouses, sum(case when ws.is_returned then 1 else 0 end) as num_returns, count(ws.line_item) as num_lines group by ws.order_number order by ws.order_number limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...nt(ws.line_item) as num_lines ??? group by ws.order_number order...
  ```
- `trilogy run --import raw/web_sales:ws 
# First: identify qualifying orders (at least 2 different warehouses, no returns)
with qualifying_orders as
select 
  …etrics on qualifying orders with additional filters
select
    count(qualifying_orders.order_number) as order_count
from qualifying_orders
limit 10;
`

  ```text
  Syntax error in stdin: Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ....order_number) as order_count ??? from qualifying_orders limit 1...
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
- `trilogy explore raw/`

  ```text
  Invalid value for 'PATH': File 'raw/' is a directory.
  ```
- `trilogy explore raw/store_returns.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_returns.preql' does not exist.
  ```
- `trilogy syntax example nested-aggregate-group-average`

  ```text
  No such command 'syntax'.
  ```
- `trilogy explore raw/`

  ```text
  Invalid value for 'PATH': File 'raw/' is a directory.
  ```
- `trilogy explore raw/sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/sales.preql' does not exist.
  ```
- `trilogy explore raw/`

  ```text
  Invalid value for 'PATH': File 'raw/' is a directory.
  ```
- `trilogy explore raw/`

  ```text
  Invalid value for 'PATH': File 'raw/' is a directory.
  ```
- `trilogy explore raw/warehouse_inventory.preql`

  ```text
  Invalid value for 'PATH': File 'raw/warehouse_inventory.preql' does not exist.
  ```
- `trilogy run --import raw/store_sales:store_sales select store_sales.date.date store_sales.date.sk store_sales.return_date.date store_sales.store.sk store_sal…_store.sk store_sales.ticket_number store_sales.ext_sales_price store_sales.return_amount store_sales.net_profit store_sales.return_net_loss limit 5;`

  ```text
  'store_sales.date.sk' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy explore raw/`

  ```text
  Invalid value for 'PATH': File 'raw/' is a directory.
  ```
- `trilogy explore raw/`

  ```text
  Invalid value for 'PATH': File 'raw/' is a directory.
  ```
- `trilogy run raw/web_sales.preql --param import=raw.web_sales:ws --param query select ws.order_number, ws.warehouse.sk limit 5`

  ```text
  'select ws.order_number, ws.warehouse.sk limit 5' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy explore raw`

  ```text
  Invalid value for 'PATH': File 'raw' is a directory.
  ```

### `type-error`

- `trilogy run answer_3210116865.preql`

  ```text
  Syntax error in answer_3210116865.preql: Cannot use BETWEEN with incompatible types DATE and STRING (low)
  ```
