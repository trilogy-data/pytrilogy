# Trilogy failure analysis — 20260715-033012

- Run `20260715-033011_enriched` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 1147 | failed: 95 (8%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 55 | 58% |
| `syntax-parse` | 32 | 34% |
| `cli-misuse` | 4 | 4% |
| `syntax-missing-alias` | 3 | 3% |
| `type-error` | 1 | 1% |

## Detail

### `other`

- `trilogy run answer_1858999935.preql`

  ```text
  Syntax error in answer_1858999935.preql: Undefined concept: date.week_seq. Suggestions: ['web.date.week_seq', 'cat.date.week_seq', 'web.ship_date.week_seq', 'cat.ship_date.week_seq', 'cat.sold_date.week_seq', 'web.return_date.week_seq']
  ```
- `trilogy run answer_3863442186.preql`

  ```text
  Resolution error in answer_3863442186.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 88). The requested concepts split into 6 disconnected subgraphs: {catalog_2001.catalog_val_2001, catalog_2001.cid}; {catalog_2002.catalog_val_2002, catalog_2002.cid}; {store_2001.cid, store_2001.store_val_2001}; {store_2002.cid, store_2002.store_val_2002}; {web_2001.cid, web_2001.web_val_2001}; {web_2002.cid, web_2002.web_val_2002}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 53 column 12 (char 2181). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run answer_3705756794.preql`

  ```text
  Syntax error in answer_3705756794.preql: grouping() requires a concept (column) reference as its argument, not an inline expression like 'coalesce(ref:sales_side.ch,ref:returns_side.ch)'. Assign the expression to a named concept and use that concept in both the grouping key and grouping() - e.g. `auto channel <- coalesce(a, b); select ..., grouping(channel) ... by rollup (channel)`.
  ```
- `trilogy run answer_3705756794.preql`

  ```text
  Syntax error in answer_3705756794.preql: Join chain repeats source `sales_side` (keys `sales_side.ch` and `sales_side.eid`). A `=` chain joins ONE key across distinct sources; join a composite key with `and` or separate clauses (e.g. `a.k1 = b.k1 and a.k2 = b.k2`). (line 26, column 1)
  ```
- `trilogy run answer_883027685.preql`

  ```text
  Resolution error in answer_883027685.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 8). The requested concepts split into 2 disconnected subgraphs: {item.category, item.current_price, avg_price_by_category}; {line_item_count, state, ss.customer.current_address.sk, ss.date.month_of_year, ss.date.year}.
    - `item.category` is disconnected — did you mean `ss.item.category`? (connected to the other concepts)
    - `item.current_price` is disconnected — did you mean `ss.item.current_price`? (connected to the other concepts)
  These look like separately-imported copies of models already reachable through a connected import; chain through that path (e.g. `ss.item.category`) instead of importing a second, disconnected copy.
  ```
- `trilogy file read answer_689898357.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_1455459008.preql`

  ```text
  Syntax error in answer_1455459008.preql: Cannot compare STRING and value-list element 85669 (INTEGER) with operator ComparisonOperator.IN in SubselectComparison(left=substring(ref:cs.billing_customer.current_address.zip,1,5), right=(85669, 86197, 88274, 83405, 86475, 85392, 85460, 80348, 81792), operator=<ComparisonOperator.IN: 'in'>)
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read answer_1835050598.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_2604809012.preql`

  ```text
  Resolution error in answer_2604809012.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 13). The requested concepts split into 3 disconnected subgraphs: {item.current_price, item_code}; {after_total, before_total}; {warehouse.name}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read answer_3849221871.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_2844519538.preql`

  ```text
  Syntax error in answer_2844519538.preql: Undefined concept: local.peach_total.
  ```
- `trilogy run answer_1798498862.preql`

  ```text
  Syntax error in answer_1798498862.preql: Comparison `ss.return_date.month_of_year <= 12` matches every value of enum field 'ss.return_date.month_of_year', which contains only these values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12. It is always true and should be removed.
  ```
- `trilogy run answer_4207382245.preql`

  ```text
  Resolution error in answer_4207382245.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 22). The requested concepts split into 2 disconnected subgraphs: {ss_q1, ss_q2, ss_q3, ss.pos_address.county}; {ws_q1, ws_q2, ws_q3}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_4140546834.preql`

  ```text
  Syntax error in answer_4140546834.preql: Undefined concept: _virt_agg_grouping_2599847655796781.
  ```
- `trilogy run answer_3770074305.preql`

  ```text
  Resolution error in answer_3770074305.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 5). The requested concepts split into 2 disconnected subgraphs: {inv.date.date, inv.quantity_on_hand}; {item.current_price, item.manufacturer_id, item.sk, current_price, item_code, item_description}. Are you missing a join or merge statement to relate them?
  Note: the membership predicate(s) `(item.sk) in (inv.item.sk)` span these subgraphs, but membership only filters rows on its left side — it does not join the two sides, so it cannot relate them for outputs or grouping. To combine values from both sides, author a query-scoped join or a merge on shared keys.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_968172492.preql`

  ```text
  Syntax error in answer_968172492.preql: 2 undefined concept references; fix all before re-running:
    - item.id (line 8, col 14, in SELECT); did you mean: ws.item.id, item.sk, ws.date.id, ws.time.id, ws.web_site.id, ws.ship_date.id?
    - item.sk (line 9, col 13, in WHERE); did you mean: ws.item.sk, item.id, ws.date.sk, ws.time.sk, ws.web_site.sk, ws.ship_date.sk?
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read check14.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3560698360.preql`

  ```text
  Resolution error in answer_3560698360.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced: {store_run.store_day.item_sk, store_run.store_day.sale_date, web_run.run_total}; still unresolved: {local._combined_web_run_max}
  ```
- `trilogy run --import raw/catalog_sales:cs select billing_customer.id, billing_customer.sk, sold_date.month_seq, sold_date.month_of_year, sold_date.year, item.category, item.class where sold_date.year = 1998 and sold_date.month_of_year = 12 and item.category = 'Women' and item.class = 'maternity' limit 10;`

  ```text
  Syntax error in stdin: 11 undefined concept references; fix all before re-running:
    - billing_customer.id (line 2, col 8, in SELECT); did you mean: cs.billing_customer.id, cs.billing_customer.current_address.id, cs.billing_customer.first_sales_date.id, cs.billing_customer.first_shipto_date.id, billing_customer.sk, cs.ship_customer.id?
    - billing_customer.sk (line 2, col 29, in SELECT); did you mean: cs.billing_customer.sk, cs.billing_customer.current_demographics.sk, cs.billing_customer.current_household_demographics.sk, cs.billing_customer.current_address.sk, cs.billing_customer.first_sales_date.sk, cs.billing_customer.first_shipto_date.sk?
    - sold_date.month_seq (line 2, col 50, in SELECT); did you mean: cs.sold_date.month_seq, sold_date.month_of_year, sold_date.year, cs.date.month_seq, cs.ship_date.month_seq, cs.return_date.month_seq?
    - sold_date.month_of_year (line 2, col 71, in SELECT); did you mean: cs.sold_date.month_of_year, sold_date.month_seq, sold_date.year, cs.date.month_of_year, cs.ship_date.month_of_year, cs.return_date.month_of_year?
    - sold_date.year (line 2, col 96, in SELECT); did you mean: cs.sold_date.year, sold_date.month_of_year, sold_date.month_seq, cs.date.year, cs.ship_date.year, cs.return_date.year?
    - item.category (line 2, col 112, in SELECT); did you mean: cs.item.category, item.class, cs.item.category_id, cs.item.color?
    - item.class (line 2, col 127, in SELECT); did you mean: cs.item.class, item.category, cs.item.class_id?
    - sold_date.year (line 2, col 144, in WHERE); did you mean: cs.sold_date.year, sold_date.month_of_year, sold_date.month_seq, cs.date.year, cs.ship_date.year, cs.return_date.year?
    - sold_date.month_of_year (line 2, col 170, in WHERE); did you mean: cs.sold_date.month_of_year, sold_date.month_seq, sold_date.year, cs.date.month_of_year, cs.ship_date.month_of_year, cs.return_date.month_of_year?
    - item.category (line 2, col 203, in WHERE); did you mean: cs.item.category, item.class, cs.item.category_id, cs.item.color?
    - item.class (line 2, col 231, in WHERE); did you mean: cs.item.class, item.category, cs.item.class_id?
  ```
- `trilogy file read answer_3553309440.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_4037365554.preql`

  ```text
  Syntax error in answer_4037365554.preql: Output column 'monthly_total' renames 'local.monthly_total' back to the name of an existing concept 'monthly_total' (defined at line 18) that 'local.monthly_total' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'monthly_total_out').
  ```
- `trilogy run answer_3544057080.preql`

  ```text
  Resolution error in answer_3544057080.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 14). The requested concepts split into 2 disconnected subgraphs: {cur_city, cur_street_name, cur_street_num, cur_zip, first_sale_yr, first_ship_yr, item_id, line_count, pos_city, pos_street_name, pos_street_num, pos_zip, product_name, sale_year, store_name, store_zip, sum_coupon, sum_list, sum_wholesale, store_sales.customer.current_demographics.marital_status, store_sales.is_returned, store_sales.item.color, store_sales.item.current_price, store_sales.pos_customer_demographic.marital_status}; {item_cumul_catalog_ext_list_price, item_cumul_catalog_refund_total}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_3544057080.preql`

  ```text
  Resolution error in answer_3544057080.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 43). The requested concepts split into 2 disconnected subgraphs: {agg_store_sales.cur_city, agg_store_sales.cur_street_name, agg_store_sales.cur_street_num, agg_store_sales.cur_zip, agg_store_sales.first_sale_yr, agg_store_sales.first_ship_yr, agg_store_sales.item_id, agg_store_sales.line_count, agg_store_sales.pos_city, agg_store_sales.pos_street_name, agg_store_sales.pos_street_num, agg_store_sales.pos_zip, agg_store_sales.product_name, agg_store_sales.sale_year, agg_store_sales.store_name, agg_store_sales.store_zip, agg_store_sales.sum_coupon, agg_store_sales.sum_list, agg_store_sales.sum_wholesale}; {catalog_item_stats.cum_ext_list_price, catalog_item_stats.cum_refund_total, catalog_item_stats.item_id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_3544057080.preql`

  ```text
  trilogy error: subprocess timed out after 600s.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3036656719.preql`

  ```text
  Syntax error in answer_3036656719.preql: Undefined concept: date.year. Suggestions: ['ss.date.year', 'ss.store.date.year', 'ss.return_store.date.year', 'ss.return_date.year', 'ss.customer.first_sales_date.year', 'ss.customer.first_shipto_date.year']
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read check.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read check14.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 141 column 2 (char 6055). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 113 column 12 (char 4177). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run answer_1226264875.preql`

  ```text
  Syntax error in answer_1226264875.preql: Undefined concept: cat.yr. Suggestions: ['catalog_agg.yr', 'web_agg.yr', 'store_agg.yr', 'c.date.year']
  ```
- `trilogy run answer_2910545909.preql`

  ```text
  Syntax error in answer_2910545909.preql: Undefined concept: ss.pos_household_demographic (line 8, col 9, in WHERE). Suggestions: ['ss.pos_household_demographic.sk', 'ss.pos_household_demographic.income_band_id', 'ss.pos_household_demographic.buy_potential', 'ss.pos_household_demographic.dependent_count', 'ss.pos_household_demographic.vehicle_count', 'ss.pos_household_demographic.income_band.sk']
  ```
- `trilogy run answer_2869182220.preql`

  ```text
  Resolution error in answer_2869182220.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {c.current_address.city, c.current_demographics.sk, c.current_household_demographics.income_band.lower_bound, c.current_household_demographics.income_band.upper_bound, customer_code, full_name}; {ss.is_returned, ss.return_customer_demographic.sk}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_2869182220.preql`

  ```text
  Resolution error in answer_2869182220.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {c.current_address.city, c.current_demographics.sk, c.current_household_demographics.income_band.lower_bound, c.current_household_demographics.income_band.upper_bound, customer_code, full_name}; {ss.is_returned, ss.return_customer_demographic.sk}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_927797426.preql`

  ```text
  Syntax error in answer_927797426.preql: Undefined concept: local.sales_price. Suggestions: ['ws.sales_price', 'ws.ext_sales_price', 'ws.list_price']
  ```
- `trilogy run answer_2852230229.preql`

  ```text
  Syntax error in answer_2852230229.preql: Undefined concept: _virt_agg_grouping_5549377253046357.
  ```
- `trilogy run answer_2822608529.preql`

  ```text
  Syntax error in answer_2822608529.preql: 2 undefined concept references; fix all before re-running:
    - pos_ship_household_demographic.dependent_count (line 4, col 3, in WHERE); did you mean: web.pos_ship_household_demographic.dependent_count, web.pos_bill_household_demographic.dependent_count, web.return_household_demographic.dependent_count, web.return_refund_household_demographic.dependent_count, web.ship_customer.current_household_demographics.dependent_count, web.ship_customer.current_demographics.dependent_count?
    - web_page.char_count (line 5, col 7, in WHERE); did you mean: web.web_page.char_count, web.return_web_page.char_count, web.row_counter?
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.store_sales:store_sales select return_reason.id, return_reason.desc limit 20;`

  ```text
  Syntax error in stdin: 2 undefined concept references; fix all before re-running:
    - return_reason.id (line 2, col 8, in SELECT); did you mean: store_sales.return_reason.id, return_reason.desc, store_sales.return_address.id, store_sales.return_store.id, store_sales.return_store.date.id, store_sales.return_date.id?
    - return_reason.desc (line 2, col 26, in SELECT); did you mean: store_sales.return_reason.desc, return_reason.id, store_sales.item.desc, store_sales.return_reason.sk?
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe web_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/repro.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.store_sales:store_sales select item.id, item.desc, item.category, item.class, item.current_price limit 5;`

  ```text
  Syntax error in stdin: 5 undefined concept references; fix all before re-running:
    - item.id (line 2, col 8, in SELECT); did you mean: store_sales.item.id, item.desc, store_sales.customer.id, store_sales.date.id, store_sales.time.id, store_sales.store.id?
    - item.desc (line 2, col 17, in SELECT); did you mean: store_sales.item.desc, item.id, item.class, store_sales.return_reason.desc?
    - item.category (line 2, col 28, in SELECT); did you mean: store_sales.item.category, item.class, store_sales.item.category_id?
    - item.class (line 2, col 43, in SELECT); did you mean: store_sales.item.class, item.desc, item.category?
    - item.current_price (line 2, col 55, in SELECT); did you mean: store_sales.item.current_price?
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe catalog_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```

### `syntax-parse`

- `trilogy run --import raw/all_sales:all_sales select distinct all_sales.channel_dim_text_id as id limit 5;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...ll_sales as all_sales; select ??? distinct all_sales.channel_dim...
  ```
- `trilogy file write answer_3705756794.preql --content import raw/all_sales as all_sales;

# Sales aggregation by (channel, entity)
with sales_side as
where al…h, returns_side.ch), coalesce(sales_side.eid, returns_side.eid))
order by channel_label asc nulls first, entity_identifier asc nulls first
limit 100;`

  ```text
  refused to write 'answer_3705756794.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/all_sales as all_sales;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/all_sales as all_sales;  #...

  Write stats: received 2215 chars / 2215 bytes; tail: …'sc nulls first, entity_identifier asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write answer_3705756794.preql --content import raw.all_sales as all_sales;

# Sales aggregation by (channel, entity)
with sales_side as
where al…oss, 0) as net_profit
by rollup (combined_channel, combined_eid)
order by channel_label asc nulls first, entity_identifier asc nulls first
limit 100;`

  ```text
  refused to write 'answer_3705756794.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...les_side.ch = returns_side.ch ??? union join sales_side.eid = re...

  Write stats: received 2069 chars / 2069 bytes; tail: …'sc nulls first, entity_identifier asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write answer_3705756794.preql --content import raw.all_sales as all_sales;

# Sales aggregation by (channel, entity)
with sales_side as
where al…ret_loss, 0) as net_profit
by rollup (combined_ch, combined_eid)
order by channel_label asc nulls first, entity_identifier asc nulls first
limit 100;`

  ```text
  refused to write 'answer_3705756794.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...uter join on channel + entity ??? union join sales_side.ch = ret...

  Write stats: received 2040 chars / 2040 bytes; tail: …'sc nulls first, entity_identifier asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write answer_765177085.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;

# Build two rowsets and join them

# Rowse…iltered.item_id asc nulls first,
    store_sales_filtered.item_desc asc nulls first,
    store_sales_filtered.store_state asc nulls first
limit 100;
`

  ```text
  refused to write 'answer_765177085.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block - they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...customer, return in 2001/2002 ??? rowset store_sales_filtered as...

  Write stats: received 2037 chars / 2037 bytes; tail: …'store_sales_filtered.store_state asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_1835050598.preql --content import raw.catalog_sales as cs;

# Project dimension properties to the line grain (order_number, item.sk…ent_demographics.sk is not null
order by country asc nulls first, state asc nulls first, county asc nulls first, item_code asc nulls first
limit 100;`

  ```text
  refused to write 'answer_1835050598.preql': not syntactically valid Trilogy.

  Parse error:
    --> 24:1
     |
  24 | where cs.sold_date.year = 1998
     | ^---
     |
     = expected limit, order_by, or having
  Location:
  ...tomer.current_address.county) ??? where cs.sold_date.year = 1998...

  Write stats: received 1753 chars / 1753 bytes; tail: …'county asc nulls first, item_code asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write answer_2928586490.preql --content # Business question 2928586490
import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.…sales) as total
order by combined_sales.last_name nulls first,
         combined_sales.first_name nulls first,
         total nulls first
limit 100;
`

  ```text
  refused to write 'answer_2928586490.preql': not syntactically valid Trilogy.

  Parse error:
    --> 20:1
     |
  20 | with customer_totals as
     | ^---
     |
     = expected select_statement, tvf_union_invocation, tvf_except_invocation, or tvf_intersect_invocation
  Location:
   2000-2003 total per customer ??? with customer_totals as select...

  Write stats: received 2350 chars / 2350 bytes; tail: …'rst_name nulls first,\\n         total nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write check6.preql --content import raw.store_sales as ss;

rowset customer_totals_ss <-
select
    ss.customer.sk as cust_sk,
    sum(ss.ext_sa…tal > 18265082.48)) as cnt_high,
    count(grain(customer_totals_ss.cust_sk ? customer_totals_ss.all_time_total > 0.5 * 36530164.96)) as cnt_high2
;
`

  ```text
  refused to write 'check6.preql': not syntactically valid Trilogy.

  Parse error:
    --> 12:93
     |
  12 |     count(grain(customer_totals_ss.cust_sk ? customer_totals_ss.all_time_total > 18265082.48)) as cnt_high,
     |                                                                                             ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...s.all_time_total > 18265082.48 ??? )) as cnt_high,     count(grai...

  Write stats: received 521 chars / 521 bytes; tail: …'tals_ss.all_time_total > 0.5 * 36530164.96)) as cnt_high2\\n;\\n'.
  ```
- `trilogy file write answer_751385098.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

with intersect…
        and ws.date.year = 2000)
) -> (last_name, first_name, sale_date);

select
    count(grain(intersection.sale_date)) as combo_count
limit 100;`

  ```text
  refused to write 'answer_751385098.preql': not syntactically valid Trilogy.

  Parse error:
    --> 32:39
     |
  32 |     count(grain(intersection.sale_date)) as combo_count
     |                                       ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...t(grain(intersection.sale_date ??? )) as combo_count limit 100;

  Write stats: received 950 chars / 950 bytes; tail: …'unt(grain(intersection.sale_date)) as combo_count\\nlimit 100;'.
  ```
- `trilogy file write answer_751385098.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

with intersect…not null
        and ws.date.year = 2000)
) -> (last_name, first_name, sale_date);

select
    count(grain(intersection.sale_date)) as cnt
limit 100;`

  ```text
  refused to write 'answer_751385098.preql': not syntactically valid Trilogy.

  Parse error:
    --> 32:39
     |
  32 |     count(grain(intersection.sale_date)) as cnt
     |                                       ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...t(grain(intersection.sale_date ??? )) as cnt limit 100;

  Write stats: received 942 chars / 942 bytes; tail: …'t\\n    count(grain(intersection.sale_date)) as cnt\\nlimit 100;'.
  ```
- `trilogy file write answer_1663418777.preql --content import raw.item as item;

# Items matching any of the 8 attribute profiles
auto profile_items <- item.id…nufacturer_id between 1 and 500
  and item.manufact in profile_manufs
select distinct
    item.product_name
order by
    item.product_name
limit 100;`

  ```text
  refused to write 'answer_1663418777.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...fact in profile_manufs select ??? distinct     item.product_name...

  Write stats: received 1330 chars / 1330 bytes; tail: …' item.product_name\\norder by\\n    item.product_name\\nlimit 100;'.
  ```
- `trilogy file write answer_3560698360.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;
import raw.item as item;
impor…run_max
where combined.web_run_max > combined.store_run_max
order by combined.item_sk asc nulls first, combined.sale_date asc nulls first
limit 100;
`

  ```text
  refused to write 'answer_3560698360.preql': not syntactically valid Trilogy.

  Parse error:
    --> 47:96
     |
  47 |     max(store_run.run_total) over (partition by store_run.item_sk order by store_run.sale_date rows between unbounded preceding and current row) as store_run_max,
     |                                                                                                ^---
     |
     = expected ORDER_DIRECTION_LA, ORDERING_DIRECTION, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
   order by store_run.sale_date ??? rows between unbounded precedi...

  Write stats: received 3031 chars / 3031 bytes; tail: …' nulls first, combined.sale_date asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw.store_sales:ss select distinct ss.item.brand_name where ss.item.category in ('Books','Children','Electronics','Women','Music','Men') limit 20;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...raw.store_sales as ss; select ??? distinct ss.item.brand_name wh...
  ```
- `trilogy run --import raw.store_sales:ss select count(ss.ticket_number) where ss.store.sk is null, count(ss.ticket_number) where ss.store.sk is not null;`

  ```text
  Syntax error in stdin:  --> 2:57
    |
  2 | select count(ss.ticket_number) where ss.store.sk is null, count(ss.ticket_number) where ss.store.sk is not null;
    |                                                         ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...ber) where ss.store.sk is null ??? , count(ss.ticket_number) wher...
  ```
- `trilogy file write answer_3553309440.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;
import raw.dat…get_customers as
select
    catalog_targets.c_sk as customer_sk
;

select
    all_target_customers.customer_sk,
    catalog_targets.c_sk
;

limit 10;`

  ```text
  refused to write 'answer_3553309440.preql': not syntactically valid Trilogy.

  Parse error:
    --> 39:1
     |
  39 | limit 10;
     | ^---
     |
     = expected EOI, block, or show_statement
  Location:
  ...,     catalog_targets.c_sk ;  ??? limit 10;

  Write stats: received 869 chars / 869 bytes; tail: …'customers.customer_sk,\\n    catalog_targets.c_sk\\n;\\n\\nlimit 10;'.
  ```
- `trilogy file write answer_3553309440.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Target: Dec…mer.current_address.state
select
    ss.customer.id as customer_id,
    sum(ss.ext_sales_price) as total_price
having
    total_price > 0
limit 1000;`

  ```text
  refused to write 'answer_3553309440.preql': not syntactically valid Trilogy.

  Parse error:
    --> 10:1
     |
  10 | catalog_qualifying_customer_sk <- cs.billing_customer.sk ?
     | ^---
     |
     = expected EOI, block, or show_statement
  Location:
  ...t Women/maternity in Dec 1998 ??? catalog_qualifying_customer_sk...

  Write stats: received 1217 chars / 1217 bytes; tail: …'price) as total_price\\nhaving\\n    total_price > 0\\nlimit 1000;'.
  ```
- `trilogy run --import raw/store_sales:ss select ss.date.month_seq, ss.date.year, ss.date.month_of_year, count(ss.ticket_number) as cnt where ss.date.month_seq between 1187 and 1195 group by ss.date.month_seq, ss.date.year, ss.date.month_of_year order by ss.date.month_seq limit 20;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...nth_seq between 1187 and 1195 ??? group by ss.date.month_seq, ss...
  ```
- `trilogy run --import raw/store_sales:ss select ss.customer.id, ss.store.county, ss.customer.current_address.county, ss.store.state, ss.customer.current_addre…k in (1187) group by ss.customer.id, ss.store.county, ss.customer.current_address.county, ss.store.state, ss.customer.current_address.state limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
   and ss.customer.sk in (1187) ??? group by ss.customer.id, ss.st...
  ```
- `trilogy run --import raw.item:item select item.id, count(item.sk) as num_sk where item.id in ('AAAAAAAAEHEBAAAA', 'AAAAAAAAFDKBAAAA', 'AAAAAAAAGOPDAAAA', 'AAAAAAAAOMOAAAAA', 'AAAAAAAAPGOCAAAA') group by item.id;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...MOAAAAA', 'AAAAAAAAPGOCAAAA') ??? group by item.id;
  ```
- `trilogy file write answer_1623435181.preql --content import raw.store_sales as ss;

# Step 1: Aggregate unit prices by (store, week, day_of_week)
with weekly…k_seq is not null
order by
    this_year.store_name nulls first,
    this_year.store_code nulls first,
    this_year.week_seq nulls first
limit 100
;`

  ```text
  refused to write 'answer_1623435181.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...ar.sat_total, 0) as sat_ratio ??? union join this_year = next_ye...

  Write stats: received 2426 chars / 2426 bytes; tail: …' nulls first,\\n    this_year.week_seq nulls first\\nlimit 100\\n;'.
  ```
- `trilogy file write answer_4037365554.preql --content import raw.store_sales as ss;

# Item profile 1
item_profile1 <- ss.item.category in ('Books','Children'…nager_avg.avg_monthly as avg
order by
    monthly_totals.mgr_id asc,
    manager_avg.avg_monthly asc,
    monthly_totals.monthly_total asc
limit 100;`

  ```text
  refused to write 'answer_4037365554.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:1
    |
  4 | item_profile1 <- ss.item.category in ('Books','Children','Electronics')
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...ales as ss;  # Item profile 1 ??? item_profile1 <- ss.item.categ...

  Write stats: received 1460 chars / 1460 bytes; tail: …'monthly asc,\\n    monthly_totals.monthly_total asc\\nlimit 100;'.
  ```
- `trilogy run --import raw.catalog_sales:catalog_sales select catalog_sales.item.id, count(catalog_sales.line_item) as num_lines, sum(catalog_sales.ext_list_pr…ales.store_credit) as cum_store_credit where catalog_sales.item.id in ('AAAAAAAAKPOCAAAA','AAAAAAAANHLDAAAA') group by catalog_sales.item.id limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...KPOCAAAA','AAAAAAAANHLDAAAA') ??? group by catalog_sales.item.id...
  ```
- `trilogy file write answer_2091535883.preql --content import raw.catalog_sales as cs;
import raw.inventory as inv;

# Join catalog_sales to inventory on item.…em.sk, inv.warehouse.sk, inv.date.sk)) as all_rows
order by
    all_rows desc,
    item_description,
    warehouse_name,
    week_sequence
limit 100;`

  ```text
  refused to write 'answer_2091535883.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...join cs.item.sk = inv.item.sk ??? union join cs.sold_date.week_s...

  Write stats: received 1186 chars / 1186 bytes; tail: …'escription,\\n    warehouse_name,\\n    week_sequence\\nlimit 100;'.
  ```
- `trilogy run --import raw.store_sales:ss select ss.customer.id, ss.customer.first_name, ss.customer.last_name, count(grain(ss.ticket_number, ss.item.sk)) as num_lines group by ss.customer.id having num_lines > 1 limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...er, ss.item.sk)) as num_lines ??? group by ss.customer.id having...
  ```
- `trilogy file write answer_2910545909.preql --content import raw/store_sales as ss;

where
    ss.date.day_name = 'Monday'
    and ss.date.year between 1999 a…name nulls first,
    first_name nulls first,
    city_display nulls first,
    total_net_profit nulls first,
    ticket_number nulls last
limit 100;`

  ```text
  refused to write 'answer_2910545909.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/store_sales as ss;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/store_sales as ss;  where

  Write stats: received 937 chars / 937 bytes; tail: …'_profit nulls first,\\n    ticket_number nulls last\\nlimit 100;'.
  ```
- `trilogy file write answer_943796012.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

with store_arm…d.profit) as profit
by rollup (combined.channel, combined.outlet_id)
order by combined.channel nulls first, combined.outlet_id nulls first
limit 100;`

  ```text
  refused to write 'answer_943796012.preql': not syntactically valid Trilogy.

  Parse error:
    --> 48:4
     |
  48 |   (store_arm),
     |    ^---
     |
     = expected select_statement
  Location:
  ...;  with combined as union(   ( ??? store_arm),   (catalog_arm),

  Write stats: received 1946 chars / 1946 bytes; tail: …'annel nulls first, combined.outlet_id nulls first\\nlimit 100;'.
  ```
- `trilogy file write answer_4111870542.preql --content import raw.all_sales as s;

# Filter to returns whose returned date falls in weeks containing the specif…_qty,
    sum(s.return_quantity ? s.channel = 'CATALOG') as catalog_return_qty,
    sum(s.return_quantity ? s.channel = 'WEB') as web_return_qty
    `

  ```text
  refused to write 'answer_4111870542.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [202]: Missing closing semicolon? Statements must be terminated with a semicolon `;`.
  Location:
   'WEB') as web_return_qty     ???

  Write stats: received 484 chars / 484 bytes; tail: …".return_quantity ? s.channel = 'WEB') as web_return_qty\\n    ".
  ```
- `trilogy file write answer_4111870542.preql --content import raw.all_sales as s;

# First rowset: per-item, per-channel aggregate return rows
# Only consider …       select item_code where item_channel_agg.channel = 'WEB' and item_channel_agg.return_row_count > 0
    )
select
    item_channel_agg.item_code;`

  ```text
  refused to write 'answer_4111870542.preql': not syntactically valid Trilogy.

  Parse error:
    --> 27:1
     |
  27 | select
     | ^---
     |
     = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...gg.return_row_count > 0     ) ??? select     item_channel_agg.it...

  Write stats: received 959 chars / 959 bytes; tail: …'n_row_count > 0\\n    )\\nselect\\n    item_channel_agg.item_code;'.
  ```
- `trilogy file write answer_210925266.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Distinct (la…os)
) -> (ln, fn, sale_date)
;

# Count the unique combinations
select count(grain(only_ss.ln, only_ss.fn, only_ss.sale_date)) as store_only_combos
;`

  ```text
  refused to write 'answer_210925266.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
      (select ln, fn, sale_date ??? from ss_combos),     (select l...

  Write stats: received 1390 chars / 1390 bytes; tail: …'s.ln, only_ss.fn, only_ss.sale_date)) as store_only_combos\\n;'.
  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.return_reason.id, store_sales.return_reason.desc, count(grain(store_sales.item.sk, store_…t where store_sales.is_returned = true group by store_sales.return_reason.id, store_sales.return_reason.desc order by store_sales.return_reason.desc;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...tore_sales.is_returned = true ??? group by store_sales.return_re...
  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.customer.sk as cid, sum(case when store_sales.return_quantity is not null then (store_sal…es.sales_price end) as adj_amt where store_sales.return_reason.desc = 'reason 28' order by adj_amt asc nulls first, cid asc nulls first limit 261, 5;`

  ```text
  Syntax error in stdin:  --> 2:297
    |
  2 | select store_sales.customer.sk as cid, sum(case when store_sales.return_quantity is not null then (store_sales.quantity - store_sales.return_quantity) * store_sales.sales_price else store_sales.quantity * store_sales.sales_price end) as adj_amt where store_sales.return_reason.desc = 'reason 28' order by adj_amt asc nulls first, cid asc nulls first limit 261, 5;
    |                                                                                                                                                                                                                                                                                                         ^---
    |
    = expected having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...urn_reason.desc = 'reason 28' ??? order by adj_amt asc nulls fir...
  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.customer.sk as cid, sum(case when store_sales.return_quantity is not null then (store_sal…_price end) as adj_amt where store_sales.return_reason.desc = 'reason 28' order by adj_amt asc nulls first, cid asc nulls first limit 260 offset 260;`

  ```text
  Syntax error in stdin:  --> 2:297
    |
  2 | select store_sales.customer.sk as cid, sum(case when store_sales.return_quantity is not null then (store_sales.quantity - store_sales.return_quantity) * store_sales.sales_price else store_sales.quantity * store_sales.sales_price end) as adj_amt where store_sales.return_reason.desc = 'reason 28' order by adj_amt asc nulls first, cid asc nulls first limit 260 offset 260;
    |                                                                                                                                                                                                                                                                                                         ^---
    |
    = expected having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...urn_reason.desc = 'reason 28' ??? order by adj_amt asc nulls fir...
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

### `syntax-missing-alias`

- `trilogy run --import raw.all_sales:all_sales with sales_side as where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date and all_sales.cha…s_side.ch is null and returns_side.ch is not null as only_ret, count(grain(sales_side.ch, returns_side.ch, sales_side.eid, returns_side.eid)) as cnt;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `sales_side.ch is null and returns_side.ch is not null as sales_side_ch_is_null_and_returns_side_c`
  Location:
   select sales_side.ch is null ??? and returns_side.ch is not nul...
  ```
- `trilogy file write answer_3979964698.preql --content import raw.web_sales as web;
import raw.catalog_sales as cat;

# Combine web and catalog sales with chan…ned.mo = 11) as nov_net,
  sum(combined.net ? combined.mo = 12) as dec_net
order by combined.wh_name nulls first, combined.yr nulls first
limit 100;
`

  ```text
  refused to write 'answer_3979964698.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `hidden warehouse surrogate key (not displayed)
    --combined.wh_sk as hidden_warehouse_surrogate_key_not_displ`
  Location:
  ...les, net);  select   --hidden ??? warehouse surrogate key (not d...

  Write stats: received 4387 chars / 4387 bytes; tail: …'ned.wh_name nulls first, combined.yr nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_2869182220.preql --content import raw.store_sales as ss;
import raw.customer as c;

where
    c.current_address.city = 'Edgewood'
 …name, c.first_name) as full_name,
    -- hide grain fields
    --ss.ticket_number,
    --ss.item.id
order by
    customer_code nulls first
limit 100;`

  ```text
  refused to write 'answer_2869182220.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `hide grain fields
      --ss.ticket_number as hide_grain_fields_ss_ticket_number`
  Location:
  ...me) as full_name,     -- hide ??? grain fields     --ss.ticket_n...

  Write stats: received 661 chars / 661 bytes; tail: …'ss.item.id\\norder by\\n    customer_code nulls first\\nlimit 100;'.
  ```

### `type-error`

- `trilogy run answer_1078396760.preql`

  ```text
  Syntax error in answer_1078396760.preql: Cannot use BETWEEN with incompatible types DATE and STRING (low)
  ```
