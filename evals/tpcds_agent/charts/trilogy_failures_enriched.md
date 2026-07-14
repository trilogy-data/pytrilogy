# Trilogy failure analysis — 20260714-023942

- Run `enriched_full_20260713` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 1228 | failed: 117 (10%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 58 | 50% |
| `syntax-parse` | 40 | 34% |
| `cli-misuse` | 11 | 9% |
| `join-resolution` | 4 | 3% |
| `syntax-missing-alias` | 3 | 3% |
| `type-error` | 1 | 1% |

## Detail

### `other`

- `trilogy run answer_3863442186.preql`

  ```text
  Resolution error in answer_3863442186.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {catalog_values.catalog_2001_val, catalog_values.customer_channel_annual.customer_id, catalog_values_2002.catalog_2002_val, catalog_values_2002.customer_channel_annual.customer_id, store_values.customer_channel_annual.customer_id, store_values.customer_channel_annual.first_name, store_values.customer_channel_annual.last_name, store_values.store_2001_val, store_values_2002.customer_channel_annual.customer_id, store_values_2002.store_2002_val, web_values.customer_channel_annual.customer_id, web_values.web_2001_val, web_values_2002.customer_channel_annual.customer_id, web_values_2002.web_2002_val}
  ```
- `trilogy run answer_3705756794.preql`

  ```text
  Syntax error in answer_3705756794.preql: ORDER BY contains aggregate `grouping(combined.channel_label)` (line 34), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(combined.channel_label) as g order by g desc`.
  ```
- `trilogy file read answer_883027685.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read answer_3697706765.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3697440276.preql`

  ```text
  failed to pin block of size 256.0 KiB (25.0 GiB/25.0 GiB used)\n\nPossible solutions:\n* Reducing the number of threads (SET threads=X)\n* Disabling insertion-order preservation (SET preserve_insertion_order=false)\n* Increasing the memory limit (SET memory_limit='...GB')\n\nSee also https://duckdb.org/docs/stable/guides/performance/how_to_tune_workloads\n[SQL: \nWITH \nyummy as (\nSELECT\n    \"ws_billing_customer_customers\".\"C_CUSTOMER_ID\" as \"_w02_cid\",\n    sum(\"ws_web_sales\".\"WS_EXT_LIST_PRICE\" - \"ws_web_sales\".\"WS_EXT_DISCOUNT_AMT\") as \"_w02_w_rev\"\nFROM\n    \"web_sales\
  …
  y.\"_virt_filter_lname_9047770017398956\" is not null and friendly.\"_virt_filter_cid_7186477337277761\" is not null)\n\nORDER BY \n    \"billing_customer_code\" asc nulls first,\n    \"first_name\" asc nulls first,\n    \"last_name\" asc nulls first,\n    \"preferred_cust_flag\" asc nulls first\nLIMIT (100)]\n(Background on this error at: https://sqlalche.me/e/20/e3q8)",
    "error_type": "OperationalError"
  }
  {
    "event": "output_truncated",
    "dropped_events": 2,
    "note": "Output exceeded the tool cap; trailing events dropped. Narrow the call (--regex, --show, fewer rows) to see the rest."
  }
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.channel, all_sales.item.id, order_id limit 10;`

  ```text
  Syntax error in stdin: Undefined concept: local.order_id (line 2, col 46, in SELECT). Suggestions: ['all_sales.order_id']
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read answer_765177085.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_1835050598.preql`

  ```text
  Syntax error in answer_1835050598.preql: ORDER BY contains aggregate `grouping(local.country)` (line 9), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(local.country) as g order by g desc`.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 45 column 3 (char 1631). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run answer_2928586490.preql`

  ```text
  Resolution error in answer_2928586490.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {all_time_store_totals.all_time_total, local._best_customer_sks_max_2000_2003_total, store_2000_2003.store_2000_2003_total, store_2000_2003.store_sales.customer.sk}
  ```
- `trilogy run answer_2928586490.preql`

  ```text
  Syntax error in answer_2928586490.preql: Undefined concept: all_sales.line_item. Suggestions: ['all_sales.item.sk', 'all_sales.item.id', 'all_sales.net_profit', 'catalog_sales.line_item', 'web_sales.line_item', 'store_sales.line_item']
  ```
- `trilogy file read answer_2928586490.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_1798498862.preql`

  ```text
  Syntax error in answer_1798498862.preql: Comparison `ss.return_date.month_of_year <= 12` matches every value of enum field 'ss.return_date.month_of_year', which contains only these values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12. It is always true and should be removed.
  ```
- `trilogy run --import raw.store_sales:ss --import raw.catalog_sales:cs select count_distinct(ss.customer.sk) as cc where ss.date.year=1999 and ss.date.month_o…s.return_date.year=1999 and ss.return_date.month_of_year>=9 and ss.customer.sk in cs.billing_customer.sk and cs.sold_date.year in (1999, 2000, 2001);`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {cs.sold_date.year}; {ss.customer.sk, ss.date.month_of_year, ss.date.year, ss.is_returned, ss.return_date.month_of_year, ss.return_date.year}. Are you missing a join or merge statement to relate them?
  Note: the membership predicate(s) `(ss.customer.sk) in (cs.billing_customer.sk)` span these subgraphs, but membership only filters rows on its left side — it does not join the two sides, so it cannot relate them for outputs or grouping. To combine values from both sides, author a query-scoped join or a merge on shared keys.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3347758002.preql`

  ```text
  Syntax error in answer_3347758002.preql: Undefined concept: item.category. Suggestions: ['s.item.category', 's.item.category_id', 's.item.color']
  ```
- `trilogy run answer_142117636.preql`

  ```text
  Resolution error in answer_142117636.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {feb_combos.combo_stats.cv, feb_combos.combo_stats.inv_2001.item_sk, feb_combos.combo_stats.inv_2001.mean_qoh, feb_combos.combo_stats.inv_2001.month_of_year, feb_combos.combo_stats.inv_2001.warehouse_sk, jan_combos.combo_stats.cv, jan_combos.combo_stats.inv_2001.item_sk, jan_combos.combo_stats.inv_2001.mean_qoh, jan_combos.combo_stats.inv_2001.month_of_year, jan_combos.combo_stats.inv_2001.warehouse_sk}
  ```
- `trilogy run answer_374591292.preql`

  ```text
  Resolution error in answer_374591292.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 34). The requested concepts split into 2 disconnected subgraphs: {best_name}; {worst_name}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_2118989494.preql`

  ```text
  Resolution error in answer_2118989494.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 31). The requested concepts split into 2 disconnected subgraphs: {next_month_total, prior_month_total, monthly_totals.brand, monthly_totals.category, monthly_totals.company, monthly_totals.month, monthly_totals.month_total, monthly_totals.store_name, monthly_totals.year}; {yearly_avg.avg_monthly}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_1809796058.preql`

  ```text
  Resolution error in answer_1809796058.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {days_0_to_30, days_31_to_60, days_61_to_90, days_91_to_120, days_over_120, ss.customer.sk, ss.is_returned, ss.return_customer.sk, ss.return_date.month_of_year, ss.return_date.year, ss.store.sk}; {st.city, st.company_id, st.county, st.name, st.state, st.street_name, st.street_number, st.street_type, st.suite_number, st.zip}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_3560698360.preql`

  ```text
  Syntax error in answer_3560698360.preql: None
  ```
- `trilogy file read answer_3560698360.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy --debug run answer_3560698360.preql`

  ```text
  Syntax error in answer_3560698360.preql: None
  ```
- `trilogy run answer_3560698360.preql`

  ```text
  Syntax error in answer_3560698360.preql: 2 undefined concept references; fix all before re-running:
    - combined.web_running_max (line 42, col 7, in WHERE); did you mean: combined.store_running_max, web_running_max, store_running_max?
    - combined.store_running_max (line 42, col 34, in WHERE); did you mean: combined.web_running_max, combined.store_daily_total, store_running_max, web_running_max?
  ```
- `trilogy run answer_3553309440.preql`

  ```text
  Resolution error in answer_3553309440.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 28). The requested concepts split into 2 disconnected subgraphs: {_customer_store_sales_cust_sk}; {_customer_store_sales_total_ext_price}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_3553309440.preql`

  ```text
  Resolution error in answer_3553309440.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 28). The requested concepts split into 2 disconnected subgraphs: {cust.address.county, cust.address.state}; {_customer_store_sales_cust_sk, _customer_store_sales_store_cust_sk, _customer_store_sales_total_ext_price, store.date.month_seq, store.store.county, store.store.state}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run raw/date.preql`

  ```text
  Syntax error in raw\date.preql: Nothing was executed: parsed 4 definition statement(s) (1 concept, 1 datasource, 1 import, 1 property) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy file read raw/web_sales.preql`

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
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 138 column 12 (char 5833). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run answer_3979964698.preql`

  ```text
  Syntax error in answer_3979964698.preql: union arm 0 projects 12 column(s) but the output signature declares 11. Each arm must project exactly one column per output item, in order.
  ```
- `trilogy file read answer_3979964698.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_2091535883.preql`

  ```text
  Syntax error in answer_2091535883.preql: 3 undefined concept references; fix all before re-running:
    - joined.row_counter (line 58, in SELECT); did you mean: joined.promo_sk, cs.row_counter?
    - joined.row_counter (line 59, in SELECT); did you mean: joined.promo_sk, cs.row_counter?
    - joined.row_counter (line 60, in SELECT); did you mean: joined.promo_sk, cs.row_counter?
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_1226264875.preql`

  ```text
  Syntax error in answer_1226264875.preql: Undefined concept: sa.customer_sk. Suggestions: ['sa.item_sk', 'sa.store_qty', 'ca.customer_sk', 'customer_sk', 'store_agg.customer_sk', 'web_agg.customer_sk']
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw/all_sales:all_sales --import raw/store:store select all_sales.channel, all_sales.channel_dim_id, store.id, all_sales.channel_dim_text_id, store.sk where all_sales.channel = 'STORE' and all_sales.channel_dim_id = 1 limit 5;`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {all_sales.channel, all_sales.channel_dim_id, all_sales.channel_dim_text_id}; {store.id, store.sk}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_2374450308.preql`

  ```text
  Syntax error in answer_2374450308.preql: Undefined concept: items_in_store_sales.item.manufacturer_id (line 25, col 9, in WHERE). Suggestions: ['items_in_store_sales.item.id', 'items_in_store_sales.item.current_price', 'items_in_store_sales.item.desc', 'ss.item.manufacturer_id', 'inv.item.manufacturer_id', 'item.manufacturer_id']
  ```
- `trilogy run - duckdb . --import raw/date:date select date.date, date.week_seq from date where date.date in ('2000-06-30'::date, '2000-09-27'::date, '2000-11-17'::date);`

  ```text
  Configuration error: Unknown DuckDB connection parameters: .. Valid parameters: path, enable_python_datasources, enable_gcs, enable_spatial, gcs_cache_bust
  ```
- `trilogy file read trilogy.toml`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run - duckdb --import raw/date:date select date.date, date.week_seq from date where date.date in ('2000-06-30'::date, '2000-09-27'::date, '2000-11-17'::date);`

  ```text
  Connection argument 'select date.date, date.week_seq from date where date.date in ('2000-06-30'::date, '2000-09-27'::date, '2000-11-17'::date);' has no value; pass connection arguments as 'key value' pairs or 'key=value'
  ```
- `trilogy run raw/date.preql duckdb`

  ```text
  Syntax error in raw\date.preql: Nothing was executed: parsed 4 definition statement(s) (1 concept, 1 datasource, 1 import, 1 property) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy run --import raw/date:date select date.date, date.week_seq where date.date in ('2000-06-30', '2000-09-27', '2000-11-17'); duckdb`

  ```text
  Syntax error in stdin: Cannot compare DATE and value-list element 2000-06-30 (STRING) with operator ComparisonOperator.IN in SubselectComparison(left=ref:date.date, right=('2000-06-30', '2000-09-27', '2000-11-17'), operator=<ComparisonOperator.IN: 'in'>)
  ```
- `trilogy run --import raw/date:date select date.date, date.week_seq where date.date in ('2000-06-30', '2000-09-27', '2000-11-17'); duckdb`

  ```text
  Syntax error in stdin: Cannot compare DATE and value-list element 2000-06-30 (STRING) with operator ComparisonOperator.IN in SubselectComparison(left=ref:date.date, right=('2000-06-30', '2000-09-27', '2000-11-17'), operator=<ComparisonOperator.IN: 'in'>)
  ```
- `trilogy run --import raw/all_sales:all select all.item.id, all.channel, count(all.row_counter) as cnt, sum(all.return_quantity) as total_qty, count(case when…as non_null_qty_cnt where all.is_returned is not null and all.return_date.week_seq in (5244, 5257, 5264) and all.item.id = 'AAAAAAAAGJMBAAAA'; duckdb`

  ```text
  Syntax error in stdin: Undefined concept: all.row_counter (line 2, in SELECT). Suggestions: ['all.return_amount', 'all.coupon_amt', 'all.warehouse.country']
  ```
- `trilogy run answer_2869182220.preql`

  ```text
  Resolution error in answer_2869182220.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 10). The requested concepts split into 2 disconnected subgraphs: {c.address.city, c.demographics.sk, c.household_demographic.income_band.lower_bound, c.household_demographic.income_band.upper_bound, customer_code, full_name}; {ss.is_returned, ss.item.sk, ss.return_customer_demographic.sk, ss.ticket_number}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_2852230229.preql`

  ```text
  Syntax error in answer_2852230229.preql: ORDER BY references 'local.g_class', which is not in the SELECT projection (line 14). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --local.g_class order by local.g_class asc`.
  ```
- `trilogy file read answer_2852230229.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_840315271.preql`

  ```text
  Syntax error in answer_840315271.preql: 6 undefined concept references; fix all before re-running:
    - name (line 48, col 3, in ORDER BY); did you mean: ss.store.name, ss.return_store.name, monthly_totals.ss.store.name, avgs.monthly_totals.ss.store.name?
    - category (line 49, col 3, in ORDER BY); did you mean: ss.item.category, monthly_totals.ss.item.category, avgs.monthly_totals.ss.item.category?
    - class (line 50, col 3, in ORDER BY); did you mean: ss.item.class, monthly_totals.ss.item.class, avgs.monthly_totals.ss.item.class?
    - brand_name (line 51, col 3, in ORDER BY); did you mean: ss.item.brand_name, monthly_totals.ss.item.brand_name, avgs.monthly_totals.ss.item.brand_name?
    - company_name (line 52, col 3, in ORDER BY); did you mean: ss.store.company_name, ss.return_store.company_name, monthly_totals.ss.store.company_name, avgs.monthly_totals.ss.store.company_name?
    - month_of_year (line 53, col 3, in ORDER BY); did you mean: ss.date.month_of_year, ss.store.date.month_of_year, ss.return_date.month_of_year, ss.return_store.date.month_of_year, monthly_totals.ss.date.month_of_year, avgs.monthly_totals.ss.date.month_of_year?
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
- `trilogy run --import raw.store_sales:ss --import raw.catalog_sales:cs select count(ss.customer.sk) where ss.date.year = 2000 and (ss.customer.sk, ss.item.sk) in (select cs.billing_customer.sk, cs.item.sk where cs.sold_date.year = 2000) limit 5;`

  ```text
  Syntax error in stdin: a `(select ...)` subquery used as a scalar value or membership set must select exactly one column; project only the key/value consumed by the outer expression (line 3, column 92)
  ```

### `syntax-parse`

- `trilogy file write answer_3863442186.preql --content import raw.all_sales as all;

# Compute per-customer per-channel per-year value
# Value per line: ext_li…  Customer_ID asc nulls first,
    First_Name asc nulls first,
    Last_Name asc nulls first,
    Preferred_Customer_Flag asc nulls first
limit 100;
`

  ```text
  refused to write 'answer_3863442186.preql': not syntactically valid Trilogy.

  Parse error:
    --> 49:12
     |
  49 |     subset join s01.cid = s02.cid
     |            ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
  ...n, ann.pcf;  where     subset ??? join s01.cid = s02.cid     and...

  Write stats: received 2076 chars / 2076 bytes; tail: …'rst,\\n    Preferred_Customer_Flag asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_3705756794.preql --content import raw/all_sales as sales;

with combined as union(
    (where sales.date.date between '2000-08-23':…el_label) + grouping(combined.entity_id) asc,
         combined.channel_label asc nulls first,
         combined.entity_id asc nulls first
limit 100;`

  ```text
  refused to write 'answer_3705756794.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/all_sales as sales;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/all_sales as sales;  with

  Write stats: received 2245 chars / 2245 bytes; tail: …'irst,\\n         combined.entity_id asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write answer_3263796749.preql --content import raw.store_sales as store_sales;

# 5 quantity buckets
# Bucket thresholds: 74129, 122840, 56580, …_61_80 else avg_net_61_80 end as bucket_61_80,
    case when cnt_81_100 > 165306 then avg_disc_81_100 else avg_net_81_100 end as bucket_81_100
by *;
`

  ```text
  refused to write 'answer_3263796749.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [213]: A `by <grain>` clause must follow an aggregate, but the expression before it has none. If the `by` sits inside an aggregate's parentheses (`max(x by *)`), move it outside the call: `max(x) by *`. To take each distinct value once per grain, wrap it in `group(...)` - e.g. `group(item.current_price) by item.id, item.category`. For a reduction, use an aggregate: `sum(x) by ...`, `avg(x) by ...`, `max(x) by ...`.
  Location:
  ...t_81_100 end as bucket_81_100 ??? by *;

  Write stats: received 2292 chars / 2292 bytes; tail: …'_disc_81_100 else avg_net_81_100 end as bucket_81_100\\nby *;\\n'.
  ```
- `trilogy file write answer_3263796749.preql --content import raw.store_sales as store_sales;

# Count of lines in each bucket (using count of line_item, count…_41_60,
    avg_disc_61_80,
    avg_disc_81_100,
    avg_net_1_20,
    avg_net_21_40,
    avg_net_41_60,
    avg_net_61_80,
    avg_net_81_100
by *;
`

  ```text
  refused to write 'answer_3263796749.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [213]: A `by <grain>` clause must follow an aggregate, but the expression before it has none. If the `by` sits inside an aggregate's parentheses (`max(x by *)`), move it outside the call: `max(x) by *`. To take each distinct value once per grain, wrap it in `group(...)` - e.g. `group(item.current_price) by item.id, item.category`. For a reduction, use an aggregate: `sum(x) by ...`, `avg(x) by ...`, `max(x) by ...`.
  Location:
  ...net_61_80,     avg_net_81_100 ??? by *;

  Write stats: received 2071 chars / 2071 bytes; tail: …' avg_net_41_60,\\n    avg_net_61_80,\\n    avg_net_81_100\\nby *;\\n'.
  ```
- `trilogy file write answer_3263796749.preql --content import raw.store_sales as store_sales;

select
    avg(store_sales.ext_discount_amount ? store_sales.qua…antity between 61 and 80) as bucket_61_80,
    avg(store_sales.ext_discount_amount ? store_sales.quantity between 81 and 100) as bucket_81_100
by *;
`

  ```text
  refused to write 'answer_3263796749.preql': not syntactically valid Trilogy.

  Parse error:
   --> 9:1
    |
  9 | by *;
    | ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
   81 and 100) as bucket_81_100 ??? by *;

  Write stats: received 547 chars / 547 bytes; tail: …'e_sales.quantity between 81 and 100) as bucket_81_100\\nby *;\\n'.
  ```
- `trilogy file write answer_2928586490.preql --content import raw.all_sales as all_sales;
import raw.store_sales as store_sales;
import raw.catalog_sales as ca…rder by
    combined_feb2000.last_name asc nulls first,
    combined_feb2000.first_name asc nulls first,
    total_sales asc nulls first
limit 100
;
`

  ```text
  refused to write 'answer_2928586490.preql': not syntactically valid Trilogy.

  Parse error:
    --> 15:1
     |
  15 | by
     | ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ..._sales.item.id) as triple_cnt ??? by     all_sales.item.sk,

  Write stats: received 3032 chars / 3032 bytes; tail: …'sc nulls first,\\n    total_sales asc nulls first\\nlimit 100\\n;\\n'.
  ```
- `trilogy file write answer_2928586490.preql --content import raw.all_sales as all_sales;
import raw.store_sales as store_sales;
import raw.catalog_sales as ca…rder by
    combined_feb2000.last_name asc nulls first,
    combined_feb2000.first_name asc nulls first,
    total_sales asc nulls first
limit 100
;
`

  ```text
  refused to write 'answer_2928586490.preql': not syntactically valid Trilogy.

  Parse error:
    --> 48:1
     |
  48 | union join all_time_store.sk = store_03.sk
     | ^---
     |
     = expected limit, order_by, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...me_store_total > 0.5 * max_03 ??? union join all_time_store.sk =...

  Write stats: received 3020 chars / 3020 bytes; tail: …'sc nulls first,\\n    total_sales asc nulls first\\nlimit 100\\n;\\n'.
  ```
- `trilogy file write test_freq.preql --content import raw.all_sales as all_sales;

where all_sales.date.year between 2000 and 2003
  and all_sales.sale_line_it…count(all_sales.date.date) as distinct_date_cnt
by all_sales.item.sk
having
    distinct_date_cnt > 4
order by
    distinct_date_cnt desc
limit 10
;
`

  ```text
  refused to write 'test_freq.preql': not syntactically valid Trilogy.

  Parse error:
   --> 8:1
    |
  8 | by all_sales.item.sk
    | ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...te.date) as distinct_date_cnt ??? by all_sales.item.sk having

  Write stats: received 318 chars / 318 bytes; tail: …'date_cnt > 4\\norder by\\n    distinct_date_cnt desc\\nlimit 10\\n;\\n'.
  ```
- `trilogy file write answer_4080205480.preql --content import raw.store_sales as ss;

select
    ss.item.id as item_code,
    ss.store.state as state,
    case…_demographic.gender = 'M'
  and ss.pos_customer_demographic.education_status = 'College'
order by item_code nulls first, state nulls first
limit 100;`

  ```text
  refused to write 'answer_4080205480.preql': not syntactically valid Trilogy.

  Parse error:
    --> 12:1
     |
  12 | where ss.date.year = 2002
     | ^---
     |
     = expected limit, order_by, or having
  Location:
   (ss.item.id, ss.store.state) ??? where ss.date.year = 2002   an...

  Write stats: received 655 chars / 655 bytes; tail: …'order by item_code nulls first, state nulls first\\nlimit 100;'.
  ```
- `trilogy file write answer_1663418777.preql --content import raw.item as item;

# Items matching any of the 8 attribute profiles
auto profile_match <- 
  (ite…tween 1 and 500
  and item.manufact in shared_manufacturer
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
  ...in shared_manufacturer select ??? distinct     item.product_name...

  Write stats: received 1410 chars / 1410 bytes; tail: …'ct_name as product_name\\norder by\\n    product_name\\nlimit 100;'.
  ```
- `trilogy file write answer_968172492.preql --content import raw.web_sales as web;

# Find the item.id values that belong to items whose sk is in the prime lis… zip_code,
    web.billing_customer.address.city as city,
    sum(web.sales_price) as total_web_unit_price
order by
    zip_code,
    city
limit 100;`

  ```text
  refused to write 'answer_968172492.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:37
    |
  4 | auto target_item_ids <- web.item.id where web.item.sk in (2, 3, 5, 7, 11, 13, 17, 19, 23, 29);
    |                                     ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...arget_item_ids <- web.item.id ??? where web.item.sk in (2, 3, 5,...

  Write stats: received 693 chars / 693 bytes; tail: …'al_web_unit_price\\norder by\\n    zip_code,\\n    city\\nlimit 100;'.
  ```
- `trilogy run --import raw.store_sales:store_sales select distinct store_sales.item.brand_name limit 20;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ..._sales as store_sales; select ??? distinct store_sales.item.bran...
  ```
- `trilogy file write answer_3553309440.preql --content import raw.store_sales as store;
import raw.catalog_sales as catalog;
import raw.web_sales as web;
impor…les.total_ext_price / 50) * 50 as segment_times_50
order by segment asc nulls first, customer_count asc nulls first, segment_times_50 asc
limit 100;
`

  ```text
  refused to write 'answer_3553309440.preql': not syntactically valid Trilogy.

  Parse error:
    --> 31:1
     |
  31 | union
     | ^---
     |
     = expected join_clause, metadata, limit, order_by, where, having, or select_grouping
  Location:
  ...al_catalog.cust_sk as cust_sk ??? union select     qual_web.cust...

  Write stats: received 1847 chars / 1847 bytes; tail: …'omer_count asc nulls first, segment_times_50 asc\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_3553309440.preql --content import raw.store_sales as store;
import raw.catalog_sales as catalog;
import raw.web_sales as web;
impor…ustomers using union stacking
with qualifying_customers as
select
    qual_catalog.cust_sk as cust_sk
union
select
    qual_web.cust_sk as cust_sk
;
`

  ```text
  refused to write 'answer_3553309440.preql': not syntactically valid Trilogy.

  Parse error:
    --> 31:1
     |
  31 | union
     | ^---
     |
     = expected join_clause, metadata, limit, order_by, where, having, or select_grouping
  Location:
  ...al_catalog.cust_sk as cust_sk ??? union select     qual_web.cust...

  Write stats: received 883 chars / 883 bytes; tail: …'k as cust_sk\\nunion\\nselect\\n    qual_web.cust_sk as cust_sk\\n;\\n'.
  ```
- `trilogy run --import raw.store_sales:store --import raw.customer:cust select store.customer.sk, store.date.month_seq, store.date.month_of_year, store.date.ye…88 and 1190 and store.store.county = cust.address.county and store.store.state = cust.address.state;
subset join store.customer.sk = cust.sk
limit 5;`

  ```text
  Syntax error in stdin: Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...e.state = cust.address.state; ??? subset join store.customer.sk
  ```
- `trilogy run --import raw.store_sales:store select store.customer.sk, store.date.month_seq, store.date.month_of_year, store.date.year, sum(store.ext_sales_pri…th_seq between 1188 and 1190 group by store.customer.sk, store.date.month_seq, store.date.month_of_year, store.date.year order by total desc limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...nth_seq between 1188 and 1190 ??? group by store.customer.sk, st...
  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.date.year, min(store_sales.date.week_seq), max(store_sales.date.week_seq) group by store_sales.date.year order by store_sales.date.year;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...ax(store_sales.date.week_seq) ??? group by store_sales.date.year...
  ```
- `trilogy run --import raw.store_sales:store_sales select distinct store_sales.date.day_of_week, store_sales.date.day_name limit 10;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ..._sales as store_sales; select ??? distinct store_sales.date.day_...
  ```
- `trilogy file write answer_3544057080.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;

# Compute per-item catalog aggregates for…4
    and ss.pos_customer_demographic.marital_status != ss.customer.demographics.marital_status
select
    ss.date.year as yr
order by yr;
limit 100;`

  ```text
  refused to write 'answer_3544057080.preql': not syntactically valid Trilogy.

  Parse error:
    --> 30:1
     |
  30 | limit 100;
     | ^---
     |
     = expected EOI, block, or show_statement
  Location:
  ....date.year as yr order by yr; ??? limit 100;

  Write stats: received 1182 chars / 1182 bytes; tail: …'status\\nselect\\n    ss.date.year as yr\\norder by yr;\\nlimit 100;'.
  ```
- `trilogy file write answer_3046445280.preql --content import raw.all_sales as s;

# Filter to Books category items only
where s.item.category = 'Books'

# Com…
    yr2002.curr_qty - yr2001.prev_qty as qty_diff,
    yr2002.curr_amt - yr2001.prev_amt as amt_diff
order by qty_diff asc, amt_diff asc
limit 100;
`

  ```text
  refused to write 'answer_3046445280.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block - they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
   # Compute per-row net values ??? auto net_qty <- case when s.qu...

  Write stats: received 1939 chars / 1939 bytes; tail: …' as amt_diff\\norder by qty_diff asc, amt_diff asc\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_3046445280.preql --content import raw.all_sales as s;

# Define net concepts at the top level
auto net_qty <- case when s.quantity …
    yr2002.curr_qty - yr2001.prev_qty as qty_diff,
    yr2002.curr_amt - yr2001.prev_amt as amt_diff
order by qty_diff asc, amt_diff asc
limit 100;
`

  ```text
  refused to write 'answer_3046445280.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...egory_id = yr2002.category_id ??? union join yr2001.manufacturer...

  Write stats: received 1910 chars / 1910 bytes; tail: …' as amt_diff\\norder by qty_diff asc, amt_diff asc\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_3046445280.preql --content import raw.all_sales as s;

# Define net concepts at the top level
auto net_qty <- case when s.quantity …
    yr2002.curr_qty - yr2001.prev_qty as qty_diff,
    yr2002.curr_amt - yr2001.prev_amt as amt_diff
order by qty_diff asc, amt_diff asc
limit 100;
`

  ```text
  refused to write 'answer_3046445280.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...category, manufacturer) combo ??? union join yr2001.brand_id = y...

  Write stats: received 1837 chars / 1837 bytes; tail: …' as amt_diff\\norder by qty_diff asc, amt_diff asc\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_3046445280.preql --content import raw.all_sales as s;

# Define net concepts at the top level
auto net_qty <- case when s.quantity …
    yr2002.curr_qty - yr2001.prev_qty as qty_diff,
    yr2002.curr_amt - yr2001.prev_amt as amt_diff
order by qty_diff asc, amt_diff asc
limit 100;
`

  ```text
  refused to write 'answer_3046445280.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...category, manufacturer) combo ??? union join yr2001.brand_id = y...

  Write stats: received 1843 chars / 1843 bytes; tail: …' as amt_diff\\norder by qty_diff asc, amt_diff asc\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_747155668.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sales as cs;

with combined …,
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
  ...y, line_flag, ext_sales_price) ???  select     combined.channel,...

  Write stats: received 2253 chars / 2253 bytes; tail: …'er nulls first,\\n    combined.category nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw/all_sales:all_sales select distinct all_sales.channel, all_sales.channel_dim_text_id limit 20;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...ll_sales as all_sales; select ??? distinct all_sales.channel, al...
  ```
- `trilogy file write answer_943796012.preql --content import raw.all_sales as all_sales;

# Channel label based on all_sales.channel enum
channel_label as case…es,
    returns,
    profit
by rollup (channel_label, outlet_identifier)
order by channel_label nulls first, outlet_identifier nulls first
limit 100;`

  ```text
  refused to write 'answer_943796012.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:1
    |
  4 | channel_label as case all_sales.channel
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...sed on all_sales.channel enum ??? channel_label as case all_sale...

  Write stats: received 1441 chars / 1441 bytes; tail: …'_label nulls first, outlet_identifier nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw/all_sales:all select distinct all.channel limit 10; duckdb`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
   raw.all_sales as all; select ??? distinct all.channel limit 10;...
  ```
- `trilogy run --import raw/all_sales:all select all.channel group by all.channel limit 10; duckdb`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...es as all; select all.channel ??? group by all.channel limit 10;...
  ```
- `trilogy file write answer_4111870542.preql --content import raw.all_sales as all;

# Compute per-item per-channel return quantities for the target weeks
with…b_qty) / 3.0
        else null
    end as three_channel_avg
order by pivoted.item_code asc nulls first, pivoted.store_qty asc nulls first
limit 100;
`

  ```text
  refused to write 'answer_4111870542.preql': not syntactically valid Trilogy.

  Parse error:
    --> 11:1
     |
  11 | by all.item.id, all.channel
     | ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...eturn_quantity) as return_qty ??? by all.item.id, all.channel ;

  Write stats: received 3127 chars / 3127 bytes; tail: …'c nulls first, pivoted.store_qty asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw/all_sales:all select all.item.id, all.channel, count(all.row_counter) as cnt, sum(all.return_quantity) as total_qty, count(all.retur…eturned is not null and all.return_date.week_seq in (5244, 5257, 5264) and all.item.id = 'AAAAAAAAGJMBAAAA' group by all.item.id, all.channel; duckdb`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ....item.id = 'AAAAAAAAGJMBAAAA' ??? group by all.item.id, all.chan...
  ```
- `trilogy file write answer_2852230229.preql --content import raw.web_sales as ws;

where ws.date.year = 2000
select
    ws.item.category,
    ws.item.class,
 …vel desc nulls first,
    case when hierarchy_level = 0 then ws.item.category else null end nulls first,
    cat_subtotal_rank nulls first
limit 100;`

  ```text
  refused to write 'answer_2852230229.preql': not syntactically valid Trilogy.

  Parse error:
   --> 8:30
    |
  8 |     grouping(ws.item.category, ws.item.class) as hierarchy_level,
    |                              ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
       grouping(ws.item.category ??? , ws.item.class) as hierarchy_...

  Write stats: received 605 chars / 605 bytes; tail: …'nd nulls first,\\n    cat_subtotal_rank nulls first\\nlimit 100;'.
  ```
- `trilogy file write answer_210925266.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

with store_onl…,
            ws.billing_customer.first_name as fname,
            ws.date.date as sdate
    )
) -> (lname, fname, sdate)

select count(*) as answer;`

  ```text
  refused to write 'answer_210925266.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
    ) ) -> (lname, fname, sdate) ???  select count(*) as answer;

  Write stats: received 907 chars / 907 bytes; tail: …'    )\\n) -> (lname, fname, sdate)\\n\\nselect count(*) as answer;'.
  ```
- `trilogy file write answer_210925266.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

with store_onl…
            ws.billing_customer.first_name as fname,
            ws.date.date as sdate
    )
) -> (lname, fname, sdate);

select count(*) as answer;`

  ```text
  refused to write 'answer_210925266.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...lname, fname, sdate);  select ??? count(*) as answer;

  Write stats: received 908 chars / 908 bytes; tail: …'   )\\n) -> (lname, fname, sdate);\\n\\nselect count(*) as answer;'.
  ```
- `trilogy file write answer_501431752.preql --content import raw.catalog_sales as cs;

# Catalog returns in 11th month of 1998, filtered by returning customer …s.return_customer.demographics.marital_status as ms,
    --cs.return_customer.demographics.education_status as edu
order by
    total_net_loss desc
;`

  ```text
  refused to write 'answer_501431752.preql': not syntactically valid Trilogy.

  Parse error:
    --> 18:5
     |
  18 |     --cs.return_customer.demographics.marital_status as ms,
     |     ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...t_loss) as total_net_loss     ??? --cs.return_customer.demograph...

  Write stats: received 944 chars / 944 bytes; tail: …'s.education_status as edu\\norder by\\n    total_net_loss desc\\n;'.
  ```
- `trilogy file write answer_501431752.preql --content import raw.catalog_sales as cs;

# Catalog returns in 11th month of 1998, filtered by returning customer …t_loss
    --cs.return_customer.demographics.marital_status
    --cs.return_customer.demographics.education_status
order by
    total_net_loss desc
;`

  ```text
  refused to write 'answer_501431752.preql': not syntactically valid Trilogy.

  Parse error:
    --> 18:5
     |
  18 |     --cs.return_customer.demographics.marital_status
     |     ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...t_loss) as total_net_loss     ??? --cs.return_customer.demograph...

  Write stats: received 930 chars / 930 bytes; tail: …'graphics.education_status\\norder by\\n    total_net_loss desc\\n;'.
  ```
- `trilogy file write answer_3281742578.preql --content import raw.web_sales as ws;

# Identify candidate orders: at least 2 different warehouses, no returns
wi… bool_or(ws.is_returned) as has_return
having
    warehouse_count > 1
    and (has_return = false or has_return is null)
select
    ws.order_number
;`

  ```text
  refused to write 'answer_3281742578.preql': not syntactically valid Trilogy.

  Parse error:
    --> 12:1
     |
  12 | select
     | ^---
     |
     = expected limit, order_by, LOGICAL_OR, or LOGICAL_AND
  Location:
   false or has_return is null) ??? select     ws.order_number ;

  Write stats: received 363 chars / 363 bytes; tail: …' = false or has_return is null)\\nselect\\n    ws.order_number\\n;'.
  ```
- `trilogy file write answer_569612608.preql -e -c import raw.web_sales as ws;

# Step 1: Identify eligible orders using ALL lines (no constraints on ship date,…ount,
    sum(ws.ext_ship_cost) as total_ext_ship_cost,
    sum(ws.net_profit) as total_net_profit
order by
    eligible_order_count desc
limit 100;
`

  ```text
  refused to write 'answer_569612608.preql': not syntactically valid Trilogy.

  Parse error:
    --> 12:1
     |
  12 | select
     | ^---
     |
     = expected limit, order_by, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...> 1     and has_return = true ??? select     ws.order_number as

  Write stats: received 941 chars / 941 bytes; tail: …'et_profit\\norder by\\n    eligible_order_count desc\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_3562094594.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;

# Store sales combos: (customer, item.sk)…unt(only_store.cust_sk) as store_only_count,
    count(only_catalog.cust_sk) as catalog_only_count,
    count(both.cust_sk) as both_count
limit 100;
`

  ```text
  refused to write 'answer_3562094594.preql': not syntactically valid Trilogy.

  Parse error:
    --> 23:6
     |
  23 |     (import raw.store_sales as ss2;
     |      ^---
     |
     = expected select_statement
  Location:
  ...th only_store as except(     ( ??? import raw.store_sales as ss2;...

  Write stats: received 1498 chars / 1498 bytes; tail: …'nly_count,\\n    count(both.cust_sk) as both_count\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw.store_sales:ss select count(*) from (where ss.date.year = 2000 select ss.customer.sk, ss.item.sk) limit 5;`

  ```text
  Syntax error in stdin: Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...raw.store_sales as ss; select ??? count(*) from (where ss.date.y...
  ```
- `trilogy run --import raw.store_sales:ss select ss.item.class, sum(ss.ext_sales_price) as class_total group by ss.item.class order by ss.item.class;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...t_sales_price) as class_total ??? group by ss.item.class order b...
  ```

### `cli-misuse`

- `trilogy explore raw/store_returns.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_returns.preql' does not exist.
  ```
- `trilogy explore raw/`

  ```text
  Invalid value for 'PATH': File 'raw/' is a directory.
  ```
- `trilogy explore raw`

  ```text
  Invalid value for 'PATH': File 'raw' is a directory.
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
- `trilogy run -c import raw.store_sales as ss;
where
    ss.date.date between '1999-02-22'::date and '1999-03-24'::date
    and ss.item.category in ('Sports', 'Books', 'Home')
select
    ss.item.class,
    sum(ss.ext_sales_price) as class_total
order by ss.item.class;
`

  ```text
  'import raw.store_sales as ss;
  where
      ss.date.date between '1999-02-22'::date and '1999-03-24'::date
      and ss.item.category in ('Sports', 'Books', 'Home')
  select
      ss.item.class,
      sum(ss.ext_sales_price) as class_total
  order by ss.item.class;
  ' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy explore raw/`

  ```text
  Invalid value for 'PATH': File 'raw/' is a directory.
  ```

### `join-resolution`

- `trilogy run --import raw.all_sales:all_sales select all_sales.item.id, all_sales.channel, all_sales.order_id, all_sales.item.sk limit 10;`

  ```text
  Resolution error in stdin: Query is unresolvable: no complete sources found for output concepts {'all_sales.order_id'}. These concepts could only be resolved from partial sources.
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.channel, all_sales.order_id, all_sales.item.id limit 10;`

  ```text
  Resolution error in stdin: Query is unresolvable: no complete sources found for output concepts {'all_sales.order_id'}. These concepts could only be resolved from partial sources.
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.channel, all_sales.item.sk, all_sales.order_id limit 10;`

  ```text
  Resolution error in stdin: Query is unresolvable: no complete sources found for output concepts {'all_sales.item.sk', 'all_sales.order_id'}. These concepts could only be resolved from partial sources.
  ```
- `trilogy run --import raw.all_sales:all_sales --format json select all_sales.channel, all_sales.item.sk limit 5;`

  ```text
  Resolution error in stdin: Query is unresolvable: no complete sources found for output concepts {'all_sales.item.sk'}. These concepts could only be resolved from partial sources.
  ```

### `syntax-missing-alias`

- `trilogy file write answer_3560698360.preql --content import raw.web_sales as web;
import raw.store_sales as store;

# Web: per-item, per-day running total of…   combined.web_running_max,
    combined.store_running_max
order by combined.item_sk asc nulls first, combined.sale_date asc nulls first
limit 100;
`

  ```text
  refused to write 'answer_3560698360.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `running max forward
      max(web_daily_agg.web_running_total) over (partition by coalesce(web_daily_agg.item_sk, store_daily_agg.item_sk) order by coalesce(web_daily_agg.sale_date, store_daily_agg.sale_date)) as running_max_forward_max_web_daily_agg_we`
  Location:
  ...ore_displayed,     -- running ??? max forward     max(web_daily_...

  Write stats: received 3755 chars / 3755 bytes; tail: …' nulls first, combined.sale_date asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_2910545909.preql --content import raw.store_sales as ss;

# Filter conditions
where ss.date.day_name = 'Monday'
  and ss.date.year …first_name asc nulls first,
    store_city_first30 asc nulls first,
    total_net_profit asc nulls first,
    ticket_number asc nulls last
limit 100;`

  ```text
  refused to write 'answer_2910545909.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `Hide grouping keys: customer.sk as hide_grouping_keys_customer_sk`
  Location:
  ...as ticket_number,     -- Hide ??? grouping keys: customer.sk, po...

  Write stats: received 908 chars / 908 bytes; tail: …'asc nulls first,\\n    ticket_number asc nulls last\\nlimit 100;'.
  ```
- `trilogy file write answer_569612608.preql --content import raw.web_sales as ws;

# Step 1: Identify eligible orders using ALL lines (no constraints on ship d…ount,
    sum(ws.ext_ship_cost) as total_ext_ship_cost,
    sum(ws.net_profit) as total_net_profit
order by
    eligible_order_count desc
limit 100;
`

  ```text
  refused to write 'answer_569612608.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `More than one warehouse
      count_distinct(ws.warehouse.sk) as more_than_one_warehouse_count_distinct_w`
  Location:
   ws.order_number,     -- More ??? than one warehouse     count_d...

  Write stats: received 1003 chars / 1003 bytes; tail: …'et_profit\\norder by\\n    eligible_order_count desc\\nlimit 100;\\n'.
  ```

### `type-error`

- `trilogy run answer_3697706765.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Type error in answer_3697706765.preql: Invalid argument type 'ArrayType<STRING>' passed into SUBSTRING function in position 1 from concept: local.qualifying_zip. Valid: 'STRING'.
  ```
