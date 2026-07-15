# Trilogy failure analysis — 20260715-000128

- Run `20260714_200120_enriched` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 1070 | failed: 113 (11%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 67 | 59% |
| `syntax-parse` | 38 | 34% |
| `cli-misuse` | 6 | 5% |
| `type-error` | 1 | 1% |
| `syntax-missing-alias` | 1 | 1% |

## Detail

### `other`

- `trilogy file read trilogy.toml`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 1 column 2249 (char 2248). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run answer_3697706765.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Syntax error in answer_3697706765.preql: Undefined concept: store_sales.customer.preferred_cust_flag.
  ```
- `trilogy run answer_3263796749.preql`

  ```text
  Syntax error in answer_3263796749.preql: SELECT output 'local.bucket_21_40' is defined by an expression that references 'local.bucket_21_40' itself (line 21). This is a recursive self-reference: an alias cannot redefine a name its own calculation reads. Rename the output to a distinct name (e.g. `... as bucket_21_40_out`).
  ```
- `trilogy run answer_3697440276.preql`

  ```text
  Syntax error in answer_3697440276.preql: Nothing was executed: parsed 6 definition statement(s) (4 rowsets, 2 imports) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy file read answer_4077069387.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_219203908.preql`

  ```text
  Syntax error in answer_219203908.preql: `by rollup (…)` cannot re-aggregate the bare measure reference `leaf_agg.sales`. Alias it with an explicit aggregate (e.g. `sum(leaf_agg.sales) as sales_total`).
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_1835050598.preql`

  ```text
  Syntax error in answer_1835050598.preql: Undefined concept: _virt_agg_grouping_9329039142760275.
  ```
- `trilogy file read answer_1835050598.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read answer_1078396760.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_2604809012.preql`

  ```text
  Syntax error in answer_2604809012.preql: Undefined concept: answer_2604809012.warehouse_name. Suggestions: ['after.warehouse_name', 'warehouse_name', 'inv.warehouse.name']
  ```
- `trilogy file read answer_2844519538.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/promotion.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_1798498862.preql`

  ```text
  Syntax error in answer_1798498862.preql: Undefined concept: ss.return_date. Suggestions: ['ss.return_date.sk', 'ss.return_date.id', 'ss.return_date.date', 'ss.return_date.year', 'ss.return_date.day_of_week', 'ss.return_date.day_of_month']
  ```
- `trilogy run answer_1798498862.preql`

  ```text
  Syntax error in answer_1798498862.preql: Comparison `ss.return_date.month_of_year <= 12` matches every value of enum field 'ss.return_date.month_of_year', which contains only these values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12. It is always true and should be removed.
  ```
- `trilogy run answer_1798498862.preql`

  ```text
  Syntax error in answer_1798498862.preql: 4 undefined concept references; fix all before re-running:
    - local.item_code (line 44, col 10, in ORDER BY); did you mean: ss_ret.item_code, item_desc, store_code?
    - local.item_desc (line 44, col 21, in ORDER BY); did you mean: ss_ret.item_desc, item_code, ss.item.desc, cs.item.desc?
    - local.store_code (line 44, col 32, in ORDER BY); did you mean: ss_ret.store_code, item_code, store_name?
    - local.store_name (line 44, col 44, in ORDER BY); did you mean: ss_ret.store_name, ss.store.name, store_code?
  ```
- `trilogy file read answer_4207382245.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3770074305.preql`

  ```text
  Resolution error in answer_3770074305.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 6). The requested concepts split into 2 disconnected subgraphs: {inv.date.date, inv.quantity_on_hand}; {item.current_price, item.manufacturer_id, item.sk, current_price, description, item_code}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_142117636.preql`

  ```text
  Resolution error in answer_142117636.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {feb_stats.feb_cv, feb_stats.feb_mean, feb_stats.feb_mo, feb_stats.qualifying.monthly_stats.item_sk, feb_stats.qualifying.monthly_stats.wh_sk, jan_stats.jan_cv, jan_stats.jan_mean, jan_stats.jan_mo, jan_stats.qualifying.monthly_stats.item_sk, jan_stats.qualifying.monthly_stats.wh_sk}
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read answer_2940558602.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_1809796058.preql`

  ```text
  Syntax error in answer_1809796058.preql: 5 undefined concept references; fix all before re-running:
    - ss.date (line 21, in SELECT); did you mean: ss.date.sk, ss.date.id, ss.date.date, ss.date.year, ss.date.day_of_week, ss.date.day_of_month?
    - ss.date (line 22, in SELECT); did you mean: ss.date.sk, ss.date.id, ss.date.date, ss.date.year, ss.date.day_of_week, ss.date.day_of_month?
    - ss.date (line 23, in SELECT); did you mean: ss.date.sk, ss.date.id, ss.date.date, ss.date.year, ss.date.day_of_week, ss.date.day_of_month?
    - ss.date (line 24, in SELECT); did you mean: ss.date.sk, ss.date.id, ss.date.date, ss.date.year, ss.date.day_of_week, ss.date.day_of_month?
    - ss.date (line 25, in SELECT); did you mean: ss.date.sk, ss.date.id, ss.date.date, ss.date.year, ss.date.day_of_week, ss.date.day_of_month?
  ```
- `trilogy run answer_3560698360.preql`

  ```text
  Resolution error in answer_3560698360.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced: {store_run.store_daily.item_sk, store_run.store_daily.sale_date, store_run.store_running_total, web_run.web_daily.item_sk, web_run.web_daily.sale_date, web_run.web_running_total}; still unresolved: {local.store_running_max, local.web_running_max}
  ```
- `trilogy run answer_3553309440.preql`

  ```text
  Syntax error in answer_3553309440.preql: Undefined concept: date.year. Suggestions: ['ss.date.year', 'cs.date.year', 'ws.date.year', 'ss.store.date.year', 'ss.return_store.date.year', 'cs.ship_date.year']
  ```
- `trilogy run answer_3553309440.preql`

  ```text
  Syntax error in answer_3553309440.preql: Undefined concept: ws.sold_date.month_seq. Suggestions: ['ws.date.month_seq', 'ws.ship_date.month_seq', 'ws.return_date.month_seq', 'cs.sold_date.month_seq', 'ss.date.month_seq', 'cs.date.month_seq']
  ```
- `trilogy run answer_3553309440.preql`

  ```text
  Resolution error in answer_3553309440.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 33). The requested concepts split into 2 disconnected subgraphs: {dec1998.dec_seq}; {_store_sales_qual_cust_id, _store_sales_qual_total_spent, ss.customer.address.county, ss.customer.address.state, ss.customer.id, ss.date.month_seq, ss.store.county, ss.store.state}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_3553309440.preql`

  ```text
  Resolution error in answer_3553309440.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 35). The requested concepts split into 2 disconnected subgraphs: {dec98_seq.ms}; {_store_sales_qual_cust_id, _store_sales_qual_total_spent, ss.customer.address.county, ss.customer.address.state, ss.customer.id, ss.date.month_seq, ss.store.county, ss.store.state}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_3553309440.preql`

  ```text
  Resolution error in answer_3553309440.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 36). The requested concepts split into 2 disconnected subgraphs: {dec98_seq.ms}; {_store_sales_qual_cust_id, _store_sales_qual_total_spent, ss.customer.address.county, ss.customer.address.state, ss.customer.id, ss.date.month_seq, ss.store.county, ss.store.state}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_2986518257.preql`

  ```text
  Syntax error in answer_2986518257.preql: Undefined concept: sold_date.year. Suggestions: ['cs.sold_date.year', 'cs.date.year', 'cs.ship_date.year', 'cs.return_date.year', 'cs.ship_customer.first_sales_date.year', 'cs.return_customer.first_sales_date.year']
  ```
- `trilogy run answer_145690531.preql`

  ```text
  Syntax error in answer_145690531.preql: Undefined concept: local.store_totals. Suggestions: ['store_totals.item_code', 'catalog_totals', 'store_totals.store_total']
  ```
- `trilogy run answer_145690531.preql`

  ```text
  Syntax error in answer_145690531.preql: Undefined concept: local.store_only. Suggestions: ['store_total', 'store_only.item_code', 'catalog_only']
  ```
- `trilogy run answer_1623435181.preql`

  ```text
  Resolution error in answer_1623435181.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 8). The requested concepts split into 3 disconnected subgraphs: {_weekly_data_dow, _weekly_data_week_seq, _weekly_data_year}; {_weekly_data_store_code, _weekly_data_store_name}; {_weekly_data_weekly_unit_price}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_3544057080.preql`

  ```text
  Resolution error in answer_3544057080.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {cat_returns_by_item.cum_refund, cat_sales_by_item.cum_ext_list_price, cat_sales_by_item.item_id}
  ```
- `trilogy run answer_3544057080.preql`

  ```text
  Resolution error in answer_3544057080.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {}
  ```
- `trilogy run answer_3544057080.preql`

  ```text
  Resolution error in answer_3544057080.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {}
  ```
- `trilogy run answer_3544057080.preql`

  ```text
  Resolution error in answer_3544057080.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {}
  ```
- `trilogy file read raw/catalog_sales.preql`

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
- `trilogy run --import raw/web_sales:ws where ws.sold_date.year = 2001 and ws.time.time between 30838 and 59638 and ws.ship_mode.carrier in ('DHL','BARIAN') an…house.square_feet, ws.ship_mode.carrier, ws.time.time, ws.sold_date.month_of_year, sum(ws.ext_sales_price) as sales, sum(ws.net_paid) as net limit 10`

  ```text
  Syntax error in stdin: 2 undefined concept references; fix all before re-running:
    - ws.sold_date.month_of_year (line 2, col 255, in SELECT); did you mean: ws.date.month_of_year, ws.ship_date.month_of_year, ws.return_date.month_of_year, ws.ship_customer.first_sales_date.month_of_year, ws.return_customer.first_sales_date.month_of_year, ws.billing_customer.first_sales_date.month_of_year?
    - ws.sold_date.year (line 2, col 7, in WHERE); did you mean: ws.date.year, ws.ship_date.year, ws.sold_date.month_of_year, ws.return_date.year, ws.ship_customer.first_sales_date.year, ws.return_customer.first_sales_date.year?
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid \escape: line 1 column 58 (char 57). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run answer_1772060640.preql`

  ```text
  Syntax error in answer_1772060640.preql: ORDER BY references 'store_sales.customer.sk', which is not in the SELECT projection (line 3). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --store_sales.customer.sk order by store_sales.customer.sk asc`.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3046445280.preql`

  ```text
  Resolution error in answer_3046445280.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 13). The requested concepts split into 3 disconnected subgraphs: {dt.year}; {item.brand_id, item.category, item.category_id, item.class_id, item.manufacturer_id}; {_yearly_total_amt, _yearly_total_qty}. Are you missing a join or merge statement to relate them?
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
- `trilogy run answer_747155668.preql`

  ```text
  Syntax error in answer_747155668.preql: Undefined concept: store.sk. Suggestions: ['store.item.sk', 'store.date.sk', 'store.return_date.sk', 'store.time.sk', 'store.return_time.sk', 'store.customer.sk']
  ```
- `trilogy run --import raw.answer_747155668:ans - duckdb`

  ```text
  Import error in stdin: Unable to import 'C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260714_200120_enriched\workspace\_worker_0\raw\answer_747155668.preql': [Errno 2] No such file or directory: 'C:\\Users\\ethan\\coding_projects\\pytrilogy_two\\evals\\tpcds_agent\\results\\20260714_200120_enriched\\workspace\\_worker_0\\raw\\answer_747155668.preql'. Did you mean: answer_747155668?
  ```
- `trilogy run --import raw.answer_747155668:ans - duckdb`

  ```text
  Import error in stdin: Unable to import 'C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260714_200120_enriched\workspace\_worker_0\raw\answer_747155668.preql': [Errno 2] No such file or directory: 'C:\\Users\\ethan\\coding_projects\\pytrilogy_two\\evals\\tpcds_agent\\results\\20260714_200120_enriched\\workspace\\_worker_0\\raw\\answer_747155668.preql'. Did you mean: answer_747155668?
  ```
- `trilogy run answer_3210116865.preql`

  ```text
  Syntax error in answer_3210116865.preql: Undefined concept: sr.store_return_amt. Suggestions: ['store_returns_agg.store_return_amt', 'ws.return_amount', 'ss.return_amount']
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_2374450308.preql`

  ```text
  Resolution error in answer_2374450308.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 5). The requested concepts split into 2 disconnected subgraphs: {inv.date.date, inv.quantity_on_hand}; {item.current_price, item.manufacturer_id, item.sk, item_code, item_description}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_4111870542.preql`

  ```text
  Import error in answer_4111870542.preql: Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: raw.store_sales?
  ```
- `trilogy run --import raw/customer:c --import raw/store_sales:ss select c.id, c.address.city, c.demographics.sk, ss.return_customer_demographic.sk limit 5;`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {c.address.city, c.demographics.sk, c.id}; {ss.return_customer_demographic.sk}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read answer_2869182220.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 44 column 3 (char 1677). Re-issue the call with valid JSON arguments.
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
- `trilogy database describe web_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```

### `syntax-parse`

- `trilogy run --import raw.date:d select d.year, min(d.week_seq), max(d.week_seq) group by d.year order by d.year;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...(d.week_seq), max(d.week_seq) ??? group by d.year order by d.yea...
  ```
- `trilogy run --import raw/all_sales:all_sales select all_sales.channel, count(all_sales.return_channel_dim_text_id) as cnt where all_sales.return_channel_dim_text_id is not null group by all_sales.channel;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...annel_dim_text_id is not null ??? group by all_sales.channel;
  ```
- `trilogy file write answer_883027685.preql --content import raw.store_sales as sales;

# Per-category average current price (at item grain)
with cat_avg_price…es.line_item) as line_item_count
order by
    line_item_count asc nulls first,
    state asc nulls first
limit 100
having
    line_item_count >= 10
;`

  ```text
  refused to write 'answer_883027685.preql': not syntactically valid Trilogy.

  Parse error:
    --> 22:1
     |
  22 | order by
     | ^---
     |
     = expected metadata, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...line_item) as line_item_count ??? order by     line_item_count a...

  Write stats: received 797 chars / 797 bytes; tail: …'asc nulls first\\nlimit 100\\nhaving\\n    line_item_count >= 10\\n;'.
  ```
- `trilogy file write answer_3697706765.preql --content parameter zips string;

# Step 1: Split the parameter into a list of curated ZIPs
auto curated_zips <- s…_prefixes
select
    store_sales.store.name as store_name,
    sum(store_sales.net_profit) as total_net_profit
order by
    store_name asc
limit 100;`

  ```text
  refused to write 'answer_3697706765.preql': not syntactically valid Trilogy.

  Parse error:
    --> 14:1
     |
  14 | select zip;
     | ^---
     |
     = expected limit, order_by, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...nt having     cust_count > 10 ??? select zip;  # Step 3: Qualify...

  Write stats: received 1190 chars / 1190 bytes; tail: …') as total_net_profit\\norder by\\n    store_name asc\\nlimit 100;'.
  ```
- `trilogy file write answer_3697706765.preql --content parameter zips string;

# Step 1: Find 5-digit ZIPs where >10 preferred customers have their current hom…,')
    )
select
    store_sales.store.name as store_name,
    sum(store_sales.net_profit) as total_net_profit
order by
    store_name asc
limit 100;`

  ```text
  refused to write 'answer_3697706765.preql': not syntactically valid Trilogy.

  Parse error:
    --> 11:1
     |
  11 | select zip;
     | ^---
     |
     = expected limit, order_by, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...nt having     cust_count > 10 ??? select zip;  # Step 2: Main qu...

  Write stats: received 927 chars / 927 bytes; tail: …') as total_net_profit\\norder by\\n    store_name asc\\nlimit 100;'.
  ```
- `trilogy file write answer_3263796749.preql --content import raw.store_sales as ss;

auto bucket_1_20 <- avg(ss.ext_discount_amount ? ss.quantity between 1 an…ket_61_80 else avg_net_61_80 end as bucket_61_80,
    case when cnt_81_100 > 165306 then bucket_81_100 else avg_net_81_100 end as bucket_81_100
by *;`

  ```text
  refused to write 'answer_3263796749.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [213]: A `by <grain>` clause must follow an aggregate, but the expression before it has none. If the `by` sits inside an aggregate's parentheses (`max(x by *)`), move it outside the call: `max(x) by *`. To take each distinct value once per grain, wrap it in `group(...)` - e.g. `group(item.current_price) by item.id, item.category`. For a reduction, use an aggregate: `sum(x) by ...`, `avg(x) by ...`, `max(x) by ...`.
  Location:
  ...t_81_100 end as bucket_81_100 ??? by *;

  Write stats: received 1738 chars / 1738 bytes; tail: …'bucket_81_100 else avg_net_81_100 end as bucket_81_100\\nby *;'.
  ```
- `trilogy file write answer_4199102535.preql --content import raw.customer as customer;
import raw.store_sales as store_sales;
import raw.web_sales as web_sale…ographics.dependent_count,
         customer.demographics.employed_dependent_count,
         customer.demographics.college_dependent_count
limit 100;`

  ```text
  refused to write 'answer_4199102535.preql': not syntactically valid Trilogy.

  Parse error:
    --> 32:28
     |
  32 |     count(grain(customer.sk)) as cnt1,
     |                            ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...s,     count(grain(customer.sk ??? )) as cnt1,     customer.demog...

  Write stats: received 2102 chars / 2102 bytes; tail: …'    customer.demographics.college_dependent_count\\nlimit 100;'.
  ```
- `trilogy file write answer_1835050598.preql --content import raw.catalog_sales as cs;

# --- Project dimension properties to the catalog-sale line grain ---
a…,
    grouping(bc_home_country) asc nulls first,
    grouping(bc_home_state) asc nulls first,
    grouping(bc_home_county) asc nulls first
limit 100;`

  ```text
  refused to write 'answer_1835050598.preql': not syntactically valid Trilogy.

  Parse error:
    --> 43:5
     |
  43 |     --grouping(bc_home_country) + grouping(bc_home_state) + grouping(bc_home_county) as _gri
     |     ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...ng(bc_home_county) as _g3     ??? --grouping(bc_home_country) +

  Write stats: received 2170 chars / 2170 bytes; tail: …'rst,\\n    grouping(bc_home_county) asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write answer_2928586490.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Step 1: Fre…um(combined.sales) as total_sales
order by
  combined.last_name nulls first,
  combined.first_name nulls first,
  total_sales nulls first
limit 100;
`

  ```text
  refused to write 'answer_2928586490.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...customer_period.period_total) ??? from customer_period)   select...

  Write stats: received 2854 chars / 2854 bytes; tail: …'irst_name nulls first,\\n  total_sales nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_2928586490.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Step 1: Fre…um(combined.sales) as total_sales
order by
  combined.last_name nulls first,
  combined.first_name nulls first,
  total_sales nulls first
limit 100;
`

  ```text
  refused to write 'answer_2928586490.preql': not syntactically valid Trilogy.

  Parse error:
    --> 78:4
     |
  78 |   (cat_part),
     |    ^---
     |
     = expected select_statement
  Location:
  ...on with combined as union(   ( ??? cat_part),   (web_part) ) -> (...

  Write stats: received 2701 chars / 2701 bytes; tail: …'irst_name nulls first,\\n  total_sales nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_2928586490.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Step 1: Fre…ned.channel_sales) as total_sales
order by
  combined.last_name nulls first,
  combined.first_name nulls first,
  total_sales nulls first
limit 100;
`

  ```text
  refused to write 'answer_2928586490.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...nt_triples.sold_date   select ??? distinct     ss.item.sk as ite...

  Write stats: received 3013 chars / 3013 bytes; tail: …'irst_name nulls first,\\n  total_sales nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_4080205480.preql --content import raw.store_sales as ss;

where
    ss.date.year = 2002
    and ss.store.state = 'TN'
    and ss.po…    avg(ss.sales_price) as avg_unit_price
by rollup (ss.item.id, ss.store.state)
order by item_code asc nulls first, state asc nulls first
limit 100;`

  ```text
  refused to write 'answer_4080205480.preql': not syntactically valid Trilogy.

  Parse error:
    --> 12:24
     |
  12 |     grouping(ss.item.id, ss.store.state) as grp_indicator,
     |                        ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...state,     grouping(ss.item.id ??? , ss.store.state) as grp_indic...

  Write stats: received 659 chars / 659 bytes; tail: …' item_code asc nulls first, state asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write answer_1798498862.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;

# Store sales in Sep 1999 with returns in…urns.return_quantity) as total_return_qty,
    sum(cs.quantity) as total_catalog_qty
order by item_code, item_desc, store_code, store_name
limit 100;`

  ```text
  refused to write 'answer_1798498862.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ..._returns.item.sk = cs.item.sk ??? union join ss_returns.customer...

  Write stats: received 1159 chars / 1159 bytes; tail: …'r by item_code, item_desc, store_code, store_name\\nlimit 100;'.
  ```
- `trilogy file write answer_751385098.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Distinct (cu…e, ws_combos.first_name, ws_combos.sale_date)
) -> (last_name, first_name, sale_date)
select
    count(all_three.sale_date) as combo_count
limit 100;`

  ```text
  refused to write 'answer_751385098.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...t_name, first_name, sale_date) ??? select     count(all_three.sa...

  Write stats: received 1820 chars / 1820 bytes; tail: …'ect\\n    count(all_three.sale_date) as combo_count\\nlimit 100;'.
  ```
- `trilogy file write answer_1663418777.preql --content import raw.item as item;

# Find manufacturers that have at least one item matching any of the 8 profile…500
  and item.manufact in matching_manufacturers.manufact
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
  ...manufacturers.manufact select ??? distinct     item.product_name...

  Write stats: received 1475 chars / 1475 bytes; tail: …'ct_name as product_name\\norder by\\n    product_name\\nlimit 100;'.
  ```
- `trilogy file write answer_2940558602.preql --content import raw.all_sales as sales;

# December 2001 transactions only
where sales.date.year = 2001
  and sal… sales.return_rank <= 10 or sales.currency_rank <= 10

order by
    channel asc,
    return_rank asc,
    currency_rank asc,
    item asc

limit 100
`

  ```text
  refused to write 'answer_2940558602.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block - they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...r to the SAME transaction row ??? auto qualifying_transaction <-...

  Write stats: received 1729 chars / 1729 bytes; tail: …'rn_rank asc,\\n    currency_rank asc,\\n    item asc\\n\\nlimit 100\\n'.
  ```
- `trilogy file write answer_2940558602.preql --content import raw.all_sales as sales;

# Define auto concepts BEFORE the where/select block

# December 2001 tr… sales.return_rank <= 10 or sales.currency_rank <= 10

order by
    channel asc,
    return_rank asc,
    currency_rank asc,
    item asc

limit 100
`

  ```text
  refused to write 'answer_2940558602.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block - they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
   least one high-value return) ??? auto qualifying_transaction <-...

  Write stats: received 2000 chars / 2000 bytes; tail: …'rn_rank asc,\\n    currency_rank asc,\\n    item asc\\n\\nlimit 100\\n'.
  ```
- `trilogy file write answer_2940558602.preql`

  ```text
  refused to write 'answer_2940558602.preql': not syntactically valid Trilogy.

  Parse error:
    --> 22:1
     |
  22 | order by
     | ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
   10 or currency_rank <= 10

   ??? order by
       channel asc,


  Write stats: received 923 chars / 923 bytes; tail: …'nk asc,\r\\n    currency_rank asc,\r\\n    item asc\r\\n\r\\nlimit 100\r\\n'.
  ```
- `trilogy file write answer_2940558602.preql`

  ```text
  refused to write 'answer_2940558602.preql': not syntactically valid Trilogy.

  Parse error:
    --> 19:1
     |
  19 | order by
     | ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...m(sales.net_paid) asc) <= 10
   ??? order by
       channel asc,


  Write stats: received 1164 chars / 1164 bytes; tail: …'rank asc,\r\\n    currency_rank asc,\r\\n    item asc\r\\nlimit 100\r\\n'.
  ```
- `trilogy file write answer_2940558602.preql`

  ```text
  refused to write 'answer_2940558602.preql': not syntactically valid Trilogy.

  Parse error:
    --> 19:1
     |
  19 | order by
     | ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...<= 10 or currency_rank <= 10
   ??? order by
       channel asc,


  Write stats: received 960 chars / 960 bytes; tail: …'rank asc,\r\\n    currency_rank asc,\r\\n    item asc\r\\nlimit 100\r\\n'.
  ```
- `trilogy file write answer_2940558602.preql`

  ```text
  refused to write 'answer_2940558602.preql': not syntactically valid Trilogy.

  Parse error:
    --> 12:1
     |
  12 | order by channel asc
     | ^---
     |
     = expected metadata, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...er(sales.channel) as channel
   ??? order by channel asc
   limit 5

  Write stats: received 300 chars / 300 bytes; tail: …'r(sales.channel) as channel\r\\norder by channel asc\r\\nlimit 5\r\\n'.
  ```
- `trilogy run --import raw/date:date select distinct date.month_seq, date.year, date.month_of_year where date.year = 1998 and date.month_of_year = 12;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...port raw.date as date; select ??? distinct date.month_seq, date....
  ```
- `trilogy file write answer_3553309440.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Find month_…omer_count,
    round(store_sales_qual.total_spent / 50)::int * 50 as segment_times_50
order by segment, customer_count, segment_times_50
limit 100;
`

  ```text
  refused to write 'answer_3553309440.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...ate.month_of_year = 12 select ??? distinct date.month_seq as dec...

  Write stats: received 1606 chars / 1606 bytes; tail: …'der by segment, customer_count, segment_times_50\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_3553309440.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Find month_…omer_count,
    round(store_sales_qual.total_spent / 50)::int * 50 as segment_times_50
order by segment, customer_count, segment_times_50
limit 100;
`

  ```text
  refused to write 'answer_3553309440.preql': not syntactically valid Trilogy.

  Parse error:
    --> 23:5
     |
  23 |   ) -> (cid)
     |     ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
  ...t ws.billing_customer.id)   ) ??? -> (cid)   and ss.store.county...

  Write stats: received 1349 chars / 1349 bytes; tail: …'der by segment, customer_count, segment_times_50\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_3544057080.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.customer as cust;

# Step 1: It…9.count1999
order by
    yr1999.product_name asc,
    yr1999.store_name asc,
    yr2000.count2000 asc,
    yr1999.wc1999 asc,
    yr2000.wc2000 asc
;`

  ```text
  refused to write 'answer_3544057080.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...tore_name = yr2000.store_name ??? subset join yr1999.store_zip =...

  Write stats: received 5521 chars / 5521 bytes; tail: …'ount2000 asc,\\n    yr1999.wc1999 asc,\\n    yr2000.wc2000 asc\\n;'.
  ```
- `trilogy file write answer_3979964698.preql --content import raw/catalog_sales as cs;
import raw/web_sales as ws;

# web sales per warehouse per month in 2001…th = 11) as nov_net,
  sum(combined.net ? combined.month = 12) as dec_net
order by combined.wh_name nulls first, combined.year nulls first
limit 100;`

  ```text
  refused to write 'answer_3979964698.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/catalog_sales as cs;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/catalog_sales as cs; impor...

  Write stats: received 4491 chars / 4491 bytes; tail: …'ed.wh_name nulls first, combined.year nulls first\\nlimit 100;'.
  ```
- `trilogy file write answer_3046445280.preql --content import raw.all_sales as s;
import raw.item as item;
import raw.date as dt;

# Net sales quantity per sal…otal_qty is not null
  and y2001.total_qty is not null
  and (y2002.total_qty / y2001.total_qty) < 0.9
order by qty_diff asc, amt_diff asc
limit 100;`

  ```text
  refused to write 'answer_3046445280.preql': not syntactically valid Trilogy.

  Parse error:
    --> 15:1
     |
  15 | with yearly as
     | ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
  ...oks'  # Year-level aggregates ??? with yearly as select     dt.y...

  Write stats: received 1753 chars / 1753 bytes; tail: …'al_qty) < 0.9\\norder by qty_diff asc, amt_diff asc\\nlimit 100;'.
  ```
- `trilogy file write answer_3046445280.preql --content import raw.all_sales as s;
import raw.item as item;
import raw.date as dt;

# Net sales quantity per sal…otal_qty is not null
  and y2001.total_qty is not null
  and (y2002.total_qty / y2001.total_qty) < 0.9
order by qty_diff asc, amt_diff asc
limit 100;`

  ```text
  refused to write 'answer_3046445280.preql': not syntactically valid Trilogy.

  Parse error:
    --> 15:1
     |
  15 | with yearly as
     | ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
  ...oks'  # Year-level aggregates ??? with yearly as select     dt.y...

  Write stats: received 1753 chars / 1753 bytes; tail: …'al_qty) < 0.9\\norder by qty_diff asc, amt_diff asc\\nlimit 100;'.
  ```
- `trilogy file write answer_747155668.preql --content import raw.store_sales as store;
import raw.web_sales as web;
import raw.catalog_sales as catalog;

# Sto…,
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
  ...total_ext_sales_price numeric) ???  select     combined.channel,...

  Write stats: received 3780 chars / 3780 bytes; tail: …'er nulls first,\\n    combined.category nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.store_sales:store - duckdb`

  ```text
  Syntax error in stdin: Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...se 0 end) as missing_store_sk ??? from (select store.ticket_numb...
  ```
- `trilogy run --import raw.store_sales:store - duckdb`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
   where store.store.sk is null ??? group by store.date.year, stor...
  ```
- `trilogy run --import raw.answer_747155668:ans - duckdb`

  ```text
  Syntax error in stdin: Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a NON-NULL GRAIN KEY: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). It MUST be a key, and one that is not nullable: `count(x)` skips rows where `x` is NULL, so counting a nullable property (a name, a date, any optional field) silently undercounts. When the grain takes SEVERAL keys, name them with `grain(...)`: `count(grain(order_id, item.id))` counts order+item combinations, and `count_distinct(grain(first_name, last_name, sale_date))` counts distinct combinations - `grain()` is never NULL, so combinations with a missing member still count. For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...el, ans.combined.missing_ref, ??? count(*) as cnt group by ans.c...
  ```
- `trilogy run --import raw.answer_747155668:ans - duckdb`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...ns.combined.category)) as cnt ??? group by ans.combined.channel,...
  ```
- `trilogy file write answer_2910545909.preql --content import raw.store_sales as ss;

where 
    ss.date.day_name = 'Monday'
    and ss.date.year between 1999 … --ss.store.city
order by
    1 asc nulls first,
    2 asc nulls first,
    3 asc nulls first,
    5 asc nulls first,
    4 asc nulls last
limit 100;`

  ```text
  refused to write 'answer_2910545909.preql': not syntactically valid Trilogy.

  Parse error:
    --> 16:5
     |
  16 |     --ss.customer.sk,
     |     ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...ofit) as total_net_profit     ??? --ss.customer.sk,     --ss.pos...

  Write stats: received 731 chars / 731 bytes; tail: …'irst,\\n    5 asc nulls first,\\n    4 asc nulls last\\nlimit 100;'.
  ```
- `trilogy file write answer_2910545909.preql --content import raw.store_sales as ss;

where 
    ss.date.day_name = 'Monday'
    and ss.date.year between 1999 …s.pos_address.sk
order by
    1 asc nulls first,
    2 asc nulls first,
    3 asc nulls first,
    5 asc nulls first,
    4 asc nulls last
limit 100;`

  ```text
  refused to write 'answer_2910545909.preql': not syntactically valid Trilogy.

  Parse error:
    --> 16:5
     |
  16 |     --ss.customer.sk,
     |     ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...ofit) as total_net_profit     ??? --ss.customer.sk,     --ss.pos...

  Write stats: received 710 chars / 710 bytes; tail: …'irst,\\n    5 asc nulls first,\\n    4 asc nulls last\\nlimit 100;'.
  ```
- `trilogy file write answer_943796012.preql --content import raw.all_sales as all_sales;

select
    all_sales.channel,
    all_sales.channel_dim_text_id,
    count(*) as cnt
limit 10;`

  ```text
  refused to write 'answer_943796012.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a NON-NULL GRAIN KEY: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). It MUST be a key, and one that is not nullable: `count(x)` skips rows where `x` is NULL, so counting a nullable property (a name, a date, any optional field) silently undercounts. When the grain takes SEVERAL keys, name them with `grain(...)`: `count(grain(order_id, item.id))` counts order+item combinations, and `count_distinct(grain(first_name, last_name, sale_date))` counts distinct combinations - `grain()` is never NULL, so combinations with a missing member still count. For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...ales.channel_dim_text_id,     ??? count(*) as cnt limit 10;

  Write stats: received 130 chars / 130 bytes; tail: …'all_sales.channel_dim_text_id,\\n    count(*) as cnt\\nlimit 10;'.
  ```
- `trilogy file write answer_4111870542.preql --content import raw/store_sales as ss;
import raw/catalog_sales as cs;
import raw/web_sales as ws;

# Target week…og_ret_rows > 0
    and web_ret.web_ret_rows > 0
order by
    store_ret.item_code asc nulls first,
    store_ret.store_qty asc nulls first
limit 100;`

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

  Write stats: received 2227 chars / 2227 bytes; tail: …'ls first,\\n    store_ret.store_qty asc nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.reason:reason select distinct reason.desc where reason.desc like '%reason 28%' or reason.desc like '%28%' limit 20;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
   raw.reason as reason; select ??? distinct reason.desc where rea...
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
- `trilogy run - --import raw.store_sales:store select store.store.sk limit 1;`

  ```text
  'select store.store.sk limit 1;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy explore raw/`

  ```text
  Invalid value for 'PATH': File 'raw/' is a directory.
  ```
- `trilogy explore raw/catalog_returns.preql`

  ```text
  Invalid value for 'PATH': File 'raw/catalog_returns.preql' does not exist.
  ```

### `type-error`

- `trilogy run answer_2524943990.preql`

  ```text
  Syntax error in answer_2524943990.preql: Cannot use BETWEEN with incompatible types DATE and STRING (low)
  ```

### `syntax-missing-alias`

- `trilogy file write answer_3979964698.preql --content import raw.catalog_sales as cs;
import raw.web_sales as ws;

# web sales per warehouse per month in 2001…th = 11) as nov_net,
  sum(combined.net ? combined.month = 12) as dec_net
order by combined.wh_name nulls first, combined.year nulls first
limit 100;`

  ```text
  refused to write 'answer_3979964698.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `12 monthly sales columns
    sum(combined.sales ? combined.month = 1) as value_12_monthly_sales_columns_sum_combined_sa`
  Location:
  ...ers,   combined.year,   -- 12 ??? monthly sales columns   sum(co...

  Write stats: received 4491 chars / 4491 bytes; tail: …'ed.wh_name nulls first, combined.year nulls first\\nlimit 100;'.
  ```
