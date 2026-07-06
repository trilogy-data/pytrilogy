# Trilogy failure analysis — 20260706-135543

- Run `20260706-135542_ingest` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 1533 | failed: 200 (13%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 140 | 70% |
| `syntax-parse` | 43 | 22% |
| `type-error` | 7 | 4% |
| `join-resolution` | 5 | 2% |
| `cli-misuse` | 3 | 2% |
| `planner-recursion` | 1 | 0% |
| `syntax-missing-alias` | 1 | 0% |

## Detail

### `other`

- `trilogy file read query02.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query04.preql`

  ```text
  Syntax error in query04.preql: 6 undefined concept references; fix all before re-running:
    - local.catalog_val_2002 (line 92, in SELECT); did you mean: catalog_val_2001, store_val_2002, catalog_2002.catalog_val_2002?
    - local.catalog_val_2001 (line 92, in SELECT); did you mean: catalog_val_2002, store_val_2001, catalog_2001.catalog_val_2001?
    - local.store_val_2002 (line 92, in SELECT); did you mean: store_val_2001, web_val_2002, catalog_val_2002, store_2002.store_val_2002?
    - local.store_val_2001 (line 92, in SELECT); did you mean: store_val_2002, web_val_2001, catalog_val_2001, store_2001.store_val_2001?
    - local.web_val_2002 (line 92, in SELECT); did you mean: web_val_2001, store_val_2002, web_2002.web_val_2002?
    - local.web_val_2001 (line 92, in SELECT); did you mean: web_val_2002, store_val_2001, web_2001.web_val_2001?
  ```
- `trilogy file read raw/web_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe web_returns`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: Undefined concept: ss.sold_date.date. Suggestions: ['ss.promotion.end_date.date', 'ss.promotion.end_date.date_sk', 'ss.promotion.end_date.date_id', 'ws.sold_date.date', 'cs.sold_date.date', 'ws.sold_date.date_sk']
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: Undefined concept: cr.return_amt. Suggestions: ['cr.return_amount', 'cr.return_tax', 'cr.return_quantity', 'wr.return_amt', 'sr.return_amt']
  ```
- `trilogy run query05.preql`

  ```text
  Unexpected error in query05.preql: Missing wr.order_number in {'ws.item.item_sk': ['gullible'], 'ws.net_profit': ['gullible'], 'ws.order_number': ['gullible'], 'ws.sold_date.date': ['gullible'], 'ws.sold_date.date_sk': ['gullible'], 'ws.web_site.site_id': ['gullible'], 'ws.web_site.site_sk': ['gullible']}, source map dict_keys(['ws.item.item_sk', 'ws.net_profit', 'ws.order_number', 'ws.sold_date.date', 'ws.sold_date.date_sk', 'ws.web_site.site_id', 'ws.web_site.site_sk'])
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: Undefined concept: local.order_number. Suggestions: ['ws.order_number', 'wr.order_number', 'cs.order_number', 'cr.order_number']
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: Undefined concept: _virt_presence_396835967734333.
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: Undefined concept: _virt_presence_3725585189991433.
  ```
- `trilogy run query07.preql`

  ```text
  Syntax error in query07.preql: Comparison `ss.promotion.channel_email = 'N'` matches every value of nullable enum field 'ss.promotion.channel_email', which contains only these values: 'N'. It only excludes nulls; simplify it to 'ss.promotion.channel_email is not null'.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Unterminated string starting at: line 1 column 54 (char 53). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query13.preql`

  ```text
  Syntax error in query13.preql: 16 undefined concept references; fix all before re-running:
    - date_dim.year (line 4, col 3, in WHERE); did you mean: ss.date_dim.year, ss.store.date_dim.year, ss.date_dim.fy_year, ss.customer.first_sales_date.year, ss.customer.first_shipto_date.year, ss.customer.last_review_date.year?
    - customer_demographics.marital_status (line 8, col 7, in WHERE); did you mean: ss.customer.customer_demographics.marital_status, ss.customer_demographics.marital_status, customer_demographics.education_status?
    - customer_demographics.education_status (line 9, col 11, in WHERE); did you mean: ss.customer.customer_demographics.education_status, ss.customer_demographics.education_status, customer_demographics.marital_status?
    - household_demographics.dep_count (line 11, col 11, in WHERE); did you mean: ss.customer.household_demographics.dep_count, ss.household_demographics.dep_count, ss.household_demographics.vehicle_count, ss.customer.customer_demographics.dep_count, ss.customer_demographics.dep_count?
    - customer_demographics.marital_status (line 15, col 7, in WHERE); did you mean: ss.customer.customer_demographics.marital_status, ss.customer_demographics.marital_status, customer_demographics.education_status?
    - customer_demographics.education_status (line 16, col 11, in WHERE); did you mean: ss.customer.customer_demographics.education_status, ss.customer_demographics.education_status, customer_demographics.marital_status?
    - household_demographics.dep_count (line 18, col 11, in WHERE); did you mean: ss.customer.household_demographics.dep_count, ss.household_demographics.dep_count, ss.household_demographics.vehicle_count, ss.customer.customer_demographics.dep_count, ss.customer_demographics.dep_count?
    - customer_demographics.marital_status (line 22, col 7, in WHERE); did you mean: ss.customer.customer_demographics.marital_status, ss.customer_demographics.marital_status, customer_demographics.education_status?
    - customer_demographics.education_status (line 23, col 11, in WHERE); did you mean: ss.customer.customer_demographics.education_status, ss.customer_demographics.education_status, customer_demographics.marital_status?
    - household_demographics.dep_count (line 25, col 11, in WHERE); did you mean: ss.customer.household_demographics.dep_count, ss.household_demographics.dep_count, ss.household_demographics.vehicle_count, ss.customer.customer_demographics.dep_count, ss.customer_demographics.dep_count?
    - customer_address.country (line 31, col 7, in WHERE); did you mean: ss.customer.customer_address.country, ss.customer_address.country, customer_address.state, ss.customer_address.county, ss.customer_address.city, ss.store.country?
    - customer_address.state (line 32, col 11, in WHERE); did you mean: ss.customer.customer_address.state, ss.customer_address.state, customer_address.country, ss.customer_address.city, ss.store.state?
    - customer_address.country (line 37, col 7, in WHERE); did you mean: ss.customer.customer_address.country, ss.customer_address.country, customer_address.state, ss.customer_address.county, ss.customer_address.city, ss.store.country?
    - customer_address.state (line 38, col 11, in WHERE); did you mean: ss.customer.customer_address.state, ss.customer_address.state, customer_address.country, ss.customer_address.city, ss.store.state?
    - customer_address.country (line 43, col 7, in WHERE); did you mean: ss.customer.customer_address.country, ss.customer_address.country, customer_address.state, ss.customer_address.county, ss.customer_address.city, ss.store.country?
    - customer_address.state (line 44, col 11, in WHERE); did you mean: ss.customer.customer_address.state, ss.customer_address.state, customer_address.country, ss.customer_address.city, ss.store.state?
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: ORDER BY contains aggregate `grouping(local.channel)` (line 145), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(local.channel) as g order by g desc`.
  ```
- `trilogy run query14.preql`

  ```text
  Resolution error in query14.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 124). The requested concepts split into 2 disconnected subgraphs: {combined.brand_id, combined.category_id, combined.channel, combined.class_id, combined.total_number, combined.total_sales}; {overall_stats.sale_val}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query14.preql`

  ```text
  Resolution error in query14.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {combined.brand_id, combined.category_id, combined.class_id, combined.total_number, combined.total_sales, local.total_sales, overall_avg_row.overall_avg_sale}
  ```
- `trilogy run query14.preql`

  ```text
  Cannot mix values of type VARCHAR and DOUBLE in COALESCE operator - an explicit cast is required\n[SQL: \nWITH \nkaput as (\nSELECT\n    \"web_sales_web_sales\".\"ws_item_sk\" as \"web_sales_item_item_sk\",\n    \"web_sales_web_sales\".\"ws_sold_date_sk\" as \"web_sales_sold_date_date_sk\"\nFROM\n    \"web_sales\" as \"web_sales_web_sales\"\nGROUP BY\n    1,\n    2),\nconcerned as (\nSELECT\n    \"store_sales_store_sales\".\"ss_item_sk\" as \"store_sales_item_item_sk\",\n    \"store_sales_store_sales\".\"ss_sold_date_sk\" as \"store_sales_date_dim_date_sk\"\nFROM\n    \"store_sales\" as \"sto
  …
  9\"\nORDER BY \n    \"channel\" asc nulls first,\n    \"brand_id\" asc nulls first,\n    \"class_id\" asc nulls first,\n    \"category_id\" asc nulls first\nLIMIT (100)]\n[parameters: ('catalog', 'web', 'store')]\n(Background on this error at: https://sqlalche.me/e/20/f405)",
    "error_type": "ProgrammingError"
  }
  {
    "event": "summary",
    "statements": 1,
    "duration_ms": 418.801,
    "ok": false,
    "rows": 0
  }
  {
    "event": "output_truncated",
    "dropped_events": 1,
    "note": "Output exceeded the tool cap; trailing events dropped. Narrow the call (--regex, --show, fewer rows) to see the rest."
  }
  ```
- `trilogy run query16.preql`

  ```text
  Syntax error in query16.preql: Comparison `cs.call_center.county = 'Williamson County'` matches every value of enum field 'cs.call_center.county', which contains only these values: 'Williamson County'. It is always true and should be removed.
  ```
- `trilogy file read query16.preql`

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
- `trilogy run query17.preql`

  ```text
  Resolution error in query17.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 6). The requested concepts split into 3 disconnected subgraphs: {sr_return_quantity_avg, sr_return_quantity_count, sr_return_quantity_stddev}; {cs_quantity_avg, cs_quantity_count, cs_quantity_stddev}; {ss_quantity_avg, ss_quantity_count, ss_quantity_stddev, ss.item.item_desc, ss.item.item_id, ss.store.state}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query18.preql`

  ```text
  Syntax error in query18.preql: Comparison `cs.bill_cdemo.gender = 'Female'` can never match enum field 'cs.bill_cdemo.gender', which contains only these values: 'F', 'M'. It is always false and should be removed.
  ```
- `trilogy file read query18.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query21.preql`

  ```text
  trilogy error: subprocess timed out after 600s.
  ```
- `trilogy run query23.preql duckdb`

  ```text
  Syntax error in query23.preql: Undefined concept: ss.customer. Suggestions: ['ss.customer.customer_address.address_sk', 'ss.customer.customer_address.address_id', 'ss.customer.customer_address.street_number', 'ss.customer.customer_address.street_name', 'ss.customer.customer_address.street_type', 'ss.customer.customer_address.suite_number']
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query24.preql`

  ```text
  Syntax error in query24.preql: Ambiguous reference 'matched_sales.state': matches ['matched_sales.ss.customer.customer_address.state', 'matched_sales.ss.store.state']. Qualify the full path to disambiguate.
  ```
- `trilogy file read query24.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query26.preql`

  ```text
  Syntax error in query26.preql: Comparison `cs.promotion.channel_email = 'N'` matches every value of nullable enum field 'cs.promotion.channel_email', which contains only these values: 'N'. It only excludes nulls; simplify it to 'cs.promotion.channel_email is not null'.
  ```
- `trilogy file read query26.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read trilogy.toml`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run raw/store_sales.preql duckdb`

  ```text
  Syntax error in raw\store_sales.preql: Nothing was executed: parsed 12 definition statement(s) (9 imports, 1 concept, 1 datasource, 1 property) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy run --config trilogy.toml raw/store_sales.preql duckdb`

  ```text
  Syntax error in raw\store_sales.preql: Nothing was executed: parsed 12 definition statement(s) (9 imports, 1 concept, 1 datasource, 1 property) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy run --import raw.store_sales:store_sales with q as where store_sales.date_dim.year = 2002 and store_sales.store.state = 'TN' and store_sales.customer…y rollup (store_sales.item.item_id, store_sales.store.state) order by store_sales.item.item_id, store_sales.store.state nulls first limit 100; duckdb`

  ```text
  Syntax error in stdin: Nothing was executed: parsed 2 definition statement(s) (1 import, 1 rowset) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy file read raw/web_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 46 column 3 (char 2414). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query34.preql`

  ```text
  Syntax error in query34.preql: Comparison `sale.store.county = 'Williamson County'` matches every value of enum field 'sale.store.county', which contains only these values: 'Williamson County'. It is always true and should be removed.
  ```
- `trilogy run query35.preql`

  ```text
  Resolution error in query35.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {catalog_cust.cust_sk, local.customer_count, store_cust.dep_college_count, store_cust.dep_count, store_cust.dep_employed_count, store_cust.gender, store_cust.marital_status, store_cust.state, web_cust.cust_sk}
  ```
- `trilogy file read query35.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file write query37.preql --content import raw.catalog_sales as cs;

# Items that appear in catalog sales, with price and manufacturer filters
# AND h…ntory.date_dim.date between '2000-02-01'::date and '2000-04-01'::date
    and cs.item.item_sk is not null
order by
    cs.item.item_id asc
limit 100;`

  ```text
  cannot import name 'detect_having_after_grouping' from 'trilogy.parsing.v2.errors' (C:\Users\ethan\coding_projects\pytrilogy_two\trilogy\parsing\v2\errors.py)
  ```
- `trilogy run query37.preql`

  ```text
  Syntax error in query37.preql: Undefined concept: raw.inventory.item.item_sk. Suggestions: ['cs.item.item_sk', 'cs.promotion.item.item_sk']
  ```
- `trilogy run query37.preql`

  ```text
  Resolution error in query37.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 5). The requested concepts split into 2 disconnected subgraphs: {inv.date_dim.date, inv.quantity_on_hand}; {item.current_price, item.item_sk, item.manufact_id, current_price, description, item_code}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read query37.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file list raw --recursive`

  ```text
  cannot import name 'detect_having_after_grouping' from 'trilogy.parsing.v2.errors' (C:\Users\ethan\coding_projects\pytrilogy_two\trilogy\parsing\v2\errors.py)
  ```
- `trilogy run query39.preql`

  ```text
  Resolution error in query39.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {feb.feb_cv, feb.feb_mean, feb.feb_moy, jan.jan_cv, jan.jan_mean, jan.jan_moy, jan.qualifying.monthly_stats.inv.item.item_sk, jan.qualifying.monthly_stats.inv.warehouse.warehouse_sk}
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe catalog_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read query40.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query44.preql`

  ```text
  Syntax error in query44.preql: Undefined concept: qualifying_items.ss_item_product_name. Suggestions: ['qualifying_items.ss.item.product_name', 'qualifying_items.item_avg', '_qualifying_items_item_avg']
  ```
- `trilogy run query44.preql`

  ```text
  Resolution error in query44.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {best_ranked.product_name, best_ranked.rnk, worst_ranked.product_name}
  ```
- `trilogy run query45.preql`

  ```text
  Syntax error in query45.preql: Undefined concept: item.item_sk. Suggestions: ['ws.item.item_sk', 'ws.promotion.item.item_sk', 'ws.item.item_desc', 'ws.item.item_id']
  ```
- `trilogy file read query45.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file write --content import raw.store_sales as ss;
import raw.store as store;
import raw.item as item;

# Monthly totals by category, brand, store, c… as monthly_total
order by item.category, item.brand, store.store_name, store.company_name, ss.date_dim.year, ss.date_dim.moy
limit 10; query47.preql`

  ```text
  trilogy file write: `--content` takes a SINGLE string argument. Your args list put 2 separate tokens after --content (treating it like a shell command). In a tool call, pass the entire file body as one string element after --content, with newlines embedded literally — e.g.
    {"args": ["file", "write", "query70.preql", "--content", "import raw.store_sales as store_sales;\n\nselect ..."]}
  Alternatively use `--escapes` with a single-line `\n`-escaped string.
  ```
- `trilogy run query47.preql`

  ```text
  Resolution error in query47.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 5). The requested concepts split into 3 disconnected subgraphs: {item.brand, item.category}; {monthly_total, ss.date_dim.moy, ss.date_dim.year}; {store.company_name, store.store_name}.
    - `item.brand` is disconnected — did you mean `ss.item.brand`? (connected to the other concepts)
    - `item.category` is disconnected — did you mean `ss.item.category`? (connected to the other concepts)
    - `store.company_name` is disconnected — did you mean `ss.store.company_name`? (connected to the other concepts)
    - `store.store_name` is disconnected — did you mean `ss.store.store_name`? (connected to the other concepts)
  These look like separately-imported copies of models already reachable through a connected import; chain through that path (e.g. `ss.item.brand`) instead of importing a second, disconnected copy.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query49.preql duckdb`

  ```text
  Syntax error in query49.preql: Undefined concept: local.ret_qty.
  ```
- `trilogy run query49.preql duckdb`

  ```text
  Syntax error in query49.preql: Undefined concept: web.item. Suggestions: ['web.item.item_sk', 'web.item.item_id', 'web.item.rec_start_date', 'web.item.rec_end_date', 'web.item.item_desc', 'web.item.current_price']
  ```
- `trilogy run test_join.preql duckdb`

  ```text
  Syntax error in test_join.preql: Undefined concept: web.item. Suggestions: ['web.item.item_sk', 'web.item.item_id', 'web.item.rec_start_date', 'web.item.rec_end_date', 'web.item.item_desc', 'web.item.current_price']
  ```
- `trilogy run test_join3.preql duckdb`

  ```text
  Syntax error in test_join3.preql: Undefined concept: web.item_sk. Suggestions: ['web.item.item_sk', 'web.promotion.item.item_sk', 'web.item.size', 'web.item.units', 'wr.item_sk', 'item_sk']
  ```
- `trilogy run query49.preql duckdb`

  ```text
  Resolution error in query49.preql: (AmbiguousRelationshipResolutionException(...), "Multiple possible concept additions (intermediate join keys) found to resolve ['web.net_paid', 'web.net_profit', 'web.quantity', 'web.sold_date.moy', 'web.sold_date.year', 'wr.item.item_sk', 'wr.return_amt', 'wr.return_quantity', 'web.sold_date.date_sk'], have {'wr.item.item_sk'} or {'wr.order_number'}. Different paths are is: [{'wr.item.item_sk'}, {'wr.order_number'}]")
  ```
- `trilogy run query49.preql duckdb`

  ```text
  Resolution error in query49.preql: (AmbiguousRelationshipResolutionException(...), "Multiple possible concept additions (intermediate join keys) found to resolve ['web.net_paid', 'web.net_profit', 'web.quantity', 'web.sold_date.moy', 'web.sold_date.year', 'wr.item.item_sk', 'wr.return_amt', 'wr.return_quantity', 'web.sold_date.date_sk'], have {'wr.item.item_sk'} or {'wr.order_number'}. Different paths are is: [{'wr.item.item_sk'}, {'wr.order_number'}]")
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query54.preql`

  ```text
  Unexpected error in query54.preql: Unsupported datatype BIGINT for parameter dec_1998_month_seq.
  ```
- `trilogy run query54.preql`

  ```text
  Resolution error in query54.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 15). The requested concepts split into 2 disconnected subgraphs: {cs.item.category, cs.item.class, cs.sold_date.month_seq, _qualifying_customers_cust_sk}; {ws.item.category, ws.item.class, ws.sold_date.month_seq}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read query54.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw/store_sales:store_sales select store_sales.item.item_id, sum(store_sales.ext_sales_price) as store_total where date_dim.week_seq = 5218 limit 5;`

  ```text
  Syntax error in stdin: Undefined concept: date_dim.week_seq (line 2, col 88, in WHERE). Suggestions: ['store_sales.date_dim.week_seq', 'store_sales.store.date_dim.week_seq', 'store_sales.date_dim.fy_week_seq', 'store_sales.customer.first_sales_date.week_seq', 'store_sales.customer.first_shipto_date.week_seq', 'store_sales.customer.last_review_date.week_seq']
  ```
- `trilogy file read query59.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query61.preql`

  ```text
  Syntax error in query61.preql: Comparison `base_sales.store_sales.promotion.channel_email = 'Y'` can never match enum field 'base_sales.store_sales.promotion.channel_email', which contains only these values: 'N'. It is always false and should be removed.
  ```
- `trilogy file read query62.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file write query64.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.store_r…s.cust_addr_city,
    base_sales.cust_addr_zip,
    base_sales.sale_year,
    base_sales.first_sales_year,
    base_sales.first_ship_year
;
 False -e`

  ```text
  trilogy error: 'args' must be a list of strings.
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Undefined concept: yr1999.filtered_ss.street_number. Suggestions: ['yr1999.filtered_ss.ca_street_number', 'yr1999.filtered_ss.ca_street_name', 'yr1999.store_street_number', 'yr2000.filtered_ss.ca_street_number', 'ss.customer.customer_address.street_number', 'ss.customer_address.street_number']
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 23). The requested concepts split into 2 disconnected subgraphs: {_agg_sales_ca_city, _agg_sales_ca_street_name, _agg_sales_ca_street_number, _agg_sales_ca_zip, _agg_sales_line_count, _agg_sales_total_coupon_amt, _agg_sales_total_list_price, _agg_sales_total_wholesale_cost, cust_first_sales_yr, cust_first_ship_yr, sale_yr, ss.customer.customer_demographics.marital_status, ss.customer_demographics.marital_status, ss.item.color, ss.item.current_price, ss.item.item_sk, ss.item.product_name, ss.store.city, ss.store.state, ss.store.store_name, ss.store.street_name, ss.store.street_number, ss.store.zip, ss.ticket_number}; {sr.item.item_sk, sr.ticket_number}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 23). The requested concepts split into 2 disconnected subgraphs: {cs.item.item_sk, cat_list_price_total}; {cat_refund_total}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query65.preql`

  ```text
  Syntax error in query65.preql: ORDER BY references 'ss.store.store_sk', which is not in the SELECT projection (line 9). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --ss.store.store_sk order by ss.store.store_sk asc`.
  ```
- `trilogy file read query65.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query70.preql`

  ```text
  Syntax error in query70.preql: Output column 'level' renames 'rolled.lvl' back to the name of an existing concept 'level' (defined at line 6) that 'rolled.lvl' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'level_out').
  ```
- `trilogy file read query70.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query72.preql`

  ```text
  Resolution error in query72.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 8). The requested concepts split into 3 disconnected subgraphs: {cs.bill_cdemo.marital_status, cs.bill_hdemo.buy_potential, cs.quantity, cs.ship_date.date, cs.sold_date.date, cs.sold_date.week_seq, cs.sold_date.year, inv.quantity_on_hand, no_promo_orders, promo_orders, total_orders}; {item.item_desc}; {wh.warehouse_name}.
    - `item.item_desc` is disconnected — did you mean `cs.item.item_desc`? (connected to the other concepts)
  These look like separately-imported copies of models already reachable through a connected import; chain through that path (e.g. `cs.item.item_desc`) instead of importing a second, disconnected copy.
  ```
- `trilogy run query73.preql`

  ```text
  Syntax error in query73.preql: Comparison `ss.store.county in ('Orange County', 'Bronx County', 'Franklin Parish', 'Williamson County')` matches every value of enum field 'ss.store.county', which contains only these values: 'Williamson County'. It is always true and should be removed.
  ```
- `trilogy run query74.preql`

  ```text
  Syntax error in query74.preql: Undefined concept: store_sales.customer. Suggestions: ['store_sales.customer.customer_address.address_sk', 'store_sales.customer.customer_address.address_id', 'store_sales.customer.customer_address.street_number', 'store_sales.customer.customer_address.street_name', 'store_sales.customer.customer_address.street_type', 'store_sales.customer.customer_address.suite_number']
  ```
- `trilogy run query75.preql`

  ```text
  Syntax error in query75.preql: Undefined concept: local.yr2002. Suggestions: ['yr2001', 'yr2002.qty', 'yr2002.amt']
  ```
- `trilogy run query75.preql`

  ```text
  Syntax error in query75.preql: Undefined concept: local.yr2002. Suggestions: ['yr2001', 'yr2002.qty', 'yr2002.amt']
  ```
- `trilogy run query75.preql`

  ```text
  Syntax error in query75.preql: Undefined concept: local.yr2002. Suggestions: ['yr2001', 'yr2002.qty', 'yr2002.amt']
  ```
- `trilogy run query75.preql`

  ```text
  Resolution error in query75.preql: (AmbiguousRelationshipResolutionException(...), "Multiple possible concept additions (intermediate join keys) found to resolve ['sr.return_amt', 'sr.return_quantity', 'st.date_dim.year', 'st.ext_sales_price', 'st.item.brand_id', 'st.item.category', 'st.item.category_id', 'st.item.class_id', 'st.item.item_sk', 'st.item.manufact_id', 'st.quantity', 'st.ticket_number', 'st.date_dim.date_sk'], have {'local.___tvf_arm_0_net_amt'} or {'local.___tvf_arm_0_net_qty'}. Different paths are is: [{'local.___tvf_arm_0_net_amt'}, {'local.___tvf_arm_0_net_qty'}]")
  ```
- `trilogy run query75.preql`

  ```text
  Resolution error in query75.preql: (AmbiguousRelationshipResolutionException(...), "Multiple possible concept additions (intermediate join keys) found to resolve ['sr.return_amt', 'sr.return_quantity', 'st.date_dim.year', 'st.ext_sales_price', 'st.item.brand_id', 'st.item.category', 'st.item.category_id', 'st.item.class_id', 'st.item.item_sk', 'st.item.manufact_id', 'st.quantity', 'st.ticket_number', 'st.date_dim.date_sk'], have {'local.___tvf_arm_0_nval'} or {'local.___tvf_arm_0_nqty'}. Different paths are is: [{'local.___tvf_arm_0_nval'}, {'local.___tvf_arm_0_nqty'}]")
  ```
- `trilogy run query75.preql`

  ```text
  Resolution error in query75.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {st_net.bid, st_net.catid, st_net.cid, st_net.isk, st_net.mid, st_net.ok, st_net.yr}
  ```
- `trilogy run query75.preql`

  ```text
  Syntax error in query75.preql: Nothing was executed: parsed 18 definition statement(s) (6 concepts, 6 imports, 6 rowsets) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy run query75.preql`

  ```text
  Resolution error in query75.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {st_lines.bid, st_lines.catid, st_lines.cid, st_lines.mid, st_lines.yr}
  ```
- `trilogy run query75.preql`

  ```text
  Syntax error in query75.preql: Undefined concept: local.st_by_yr. Suggestions: ['cs_by_yr', 'st_by_yr.yr', 'st_by_yr.val']
  ```
- `trilogy run query75.preql`

  ```text
  Resolution error in query75.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 73). The requested concepts split into 2 disconnected subgraphs: {cs_lines.bid, cs_lines.catid, cs_lines.cid, cs_lines.mid, cs_lines.yr}; {_virt_agg_sum_6520591768854391, _virt_agg_sum_7611099850255923, _virt_agg_sum_8389620658037963, _virt_agg_sum_9290770134487144}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query75.preql`

  ```text
  Syntax error in query75.preql: Undefined concept: wr.return_amount. Suggestions: ['wr.return_amt', 'wr.return_quantity', 'wr.return_tax', 'cr.return_amount', 'sr.return_amt']
  ```
- `trilogy run query75.preql`

  ```text
  Syntax error in query75.preql: Undefined concept: st_agg. Suggestions: ['st_agg.yr', 'st_agg.val', 'st_agg.qty']
  ```
- `trilogy run query75.preql`

  ```text
  Resolution error in query75.preql: (AmbiguousRelationshipResolutionException(...), "Multiple possible concept additions (intermediate join keys) found to resolve ['cr.item.item_sk', 'cr.order_number', 'cr.return_amount', 'cr.return_quantity', 'cs.ext_sales_price', 'cs.item.brand_id', 'cs.item.category', 'cs.item.category_id', 'cs.item.class_id', 'cs.item.item_sk', 'cs.item.manufact_id', 'cs.order_number', 'cs.quantity', 'cs.sold_date.year', 'cs.sold_date.date_sk'], have {'local.cs_nval'} or {'local.cs_nqty'}. Different paths are is: [{'local.cs_nval'}, {'local.cs_nqty'}]")
  ```
- `trilogy run query75.preql`

  ```text
  Syntax error in query75.preql: Undefined concept: st.
  ```
- `trilogy run query75.preql`

  ```text
  Syntax error in query75.preql: Undefined concept: st.
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
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query77.preql`

  ```text
  Resolution error in query77.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced: {local._store_channel_channel, local._store_channel_sales}; still unresolved: {local._store_channel_outlet_id, local._store_channel_profit, local._store_channel_returns}
  ```
- `trilogy run query77.preql`

  ```text
  Syntax error in query77.preql: ORDER BY contains aggregate `sum(all_channels.returns)` (line 120), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --sum(all_channels.returns) as g order by g desc`.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query80.preql`

  ```text
  Syntax error in query80.preql: Comparison `store_sales.promotion.channel_tv = 'N'` matches every value of nullable enum field 'store_sales.promotion.channel_tv', which contains only these values: 'N'. It only excludes nulls; simplify it to 'store_sales.promotion.channel_tv is not null'.
  ```
- `trilogy run query80.preql`

  ```text
  Unexpected error in query80.preql: Join chain repeats source `store_sales` (keys `store_sales.ticket_number` and `store_sales.item.item_sk`). A `=` chain joins ONE key across distinct sources; join a composite key with `and` or separate clauses (e.g. `a.k1 = b.k1 and a.k2 = b.k2`).
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
- `trilogy run raw/date_dim.preql --import raw/date_dim:date_dim select date_dim.date, date_dim.week_seq where date_dim.date in ('2000-06-30'::date, '2000-09-27'::date, '2000-11-17'::date);`

  ```text
  --import only applies to inline queries, not file/directory inputs.
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.income_band --format json select income_band.lower_bound, income_band.upper_bound order by income_band.lower_bound;`

  ```text
  Syntax error in stdin: 3 undefined concept references; fix all before re-running:
    - income_band.lower_bound (line 2, col 8, in SELECT); did you mean: income_band.upper_bound, income_band_sk, lower_bound?
    - income_band.upper_bound (line 2, col 33, in SELECT); did you mean: income_band.lower_bound, income_band_sk, upper_bound?
    - income_band.lower_bound (line 2, col 66, in ORDER BY); did you mean: income_band.upper_bound, income_band_sk, lower_bound?
  ```
- `trilogy file read query84.preql`

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
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw/store_sales:store_sales select count_distinct(store_sales.ticket_number) as c where store_sales.store.store_name = 'ese' and ((store…unt <= 4) or (store_sales.customer.household_demographics.dep_count = 0 and store_sales.customer.household_demographics.vehicle_count <= 2)) limit 5;`

  ```text
  Syntax error in stdin: Comparison `store_sales.customer.household_demographics.vehicle_count <= 6` matches every value of enum field 'store_sales.customer.household_demographics.vehicle_count', which contains only these values: -1, 0, 1, 2, 3, 4. It is always true and should be removed.
  ```
- `trilogy file read raw/web_sales.preql`

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
- `trilogy file read raw/reason.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe store_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
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
- `trilogy run query97.preql`

  ```text
  Syntax error in query97.preql: Undefined concept: in_store.
  ```
- `trilogy file read query97.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy run --import raw.catalog_sales:cs select distinct cs.sold_date.dow, cs.sold_date.day_name order by cs.sold_date.dow limit 10;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...w.catalog_sales as cs; select ??? distinct cs.sold_date.dow, cs....
  ```
- `trilogy file write query11.preql --content import raw.store_sales as store;
import raw.web_sales as web;

# Store revenue per customer by year
rowset store_r…ling_customer_code asc nulls first,
    first_name asc nulls first,
    last_name asc nulls first,
    preferred_cust_flag asc nulls first
limit 100;`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
   --> 5:1
    |
  5 | rowset store_rev as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
   revenue per customer by year ??? rowset store_rev as select

  Write stats: received 2573 chars / 2573 bytes; tail: …'ls first,\\n    preferred_cust_flag asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query14.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…lls first,
    per_channel.brand_id asc nulls first,
    per_channel.class_id asc nulls first,
    per_channel.category_id asc nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...ales.channel = 'store' select ??? distinct     item.brand_id,

  Write stats: received 5857 chars / 5857 bytes; tail: …'irst,\\n    per_channel.category_id asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query14.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…el asc nulls first,
    combined.brand_id asc nulls first,
    combined.class_id asc nulls first,
    combined.category_id asc nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...les > (         select avg(s) ??? from (             (where stor...

  Write stats: received 5613 chars / 5613 bytes; tail: …'s first,\\n    combined.category_id asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Frequent items: items…d,
having sum(channel_combined.total_sales) > 0
order by customer_last_name asc nulls first,
         customer_first_name asc nulls first
limit 100;
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
    --> 50:67
     |
  50 | auto max_total <- sum(customer_lifetime_totals.total_amount) by * as max_all;
     |                                                                   ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or window_sql_over
  Location:
  ...ime_totals.total_amount) by * ??? as max_all;  # Best customers:...

  Write stats: received 3478 chars / 3478 bytes; tail: …'st,\\n         customer_first_name asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query23_check3.preql --content import raw.store_sales as ss;

rowset customer_lifetime <- 
where ss.customer.customer_sk is not null
  and…rst_name,
    sum(ss.quantity * ss.sales_price) as total_amount,
;

select max(customer_lifetime.total_amount) as max_amount from customer_lifetime;
`

  ```text
  refused to write 'query23_check3.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...e.total_amount) as max_amount ??? from customer_lifetime;

  Write stats: received 434 chars / 434 bytes; tail: …'ifetime.total_amount) as max_amount from customer_lifetime;\\n'.
  ```
- `trilogy file write query24.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;

# First stage: sub-totals per line grouped by detai…tem.item_sk = sr.item.item_sk
having reagg_total > 0.05 * avg_sub_total
order by ss.customer.last_name, ss.customer.first_name, ss.store.store_name;
`

  ```text
  refused to write 'query24.preql': not syntactically valid Trilogy.

  Parse error:
    --> 23:48
     |
  23 | subset join ss.ticket_number = sr.ticket_number,
     |                                                ^---
     |
     = expected limit, order_by, where, having, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, select_grouping, or JOIN_TYPE
  Location:
  ...cket_number = sr.ticket_number ??? , subset join ss.item.item_sk

  Write stats: received 1399 chars / 1399 bytes; tail: …'mer.last_name, ss.customer.first_name, ss.store.store_name;\\n'.
  ```
- `trilogy file write query24.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;

# First stage: sub-totals per line grouped by detai…tem.item_sk = sr.item.item_sk
having reagg_total > 0.05 * avg_sub_total
order by ss.customer.last_name, ss.customer.first_name, ss.store.store_name;
`

  ```text
  refused to write 'query24.preql': not syntactically valid Trilogy.

  Parse error:
    --> 23:48
     |
  23 | subset join ss.ticket_number = sr.ticket_number,
     |                                                ^---
     |
     = expected limit, order_by, where, having, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, select_grouping, or JOIN_TYPE
  Location:
  ...cket_number = sr.ticket_number ??? ,     subset join ss.item.item...

  Write stats: received 1402 chars / 1402 bytes; tail: …'mer.last_name, ss.customer.first_name, ss.store.store_name;\\n'.
  ```
- `trilogy file write query24.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;

# Stage 1: sub-totals per line with all filters and…first_name,
    stage2.store_name,
having stage2.reagg_total > 0.05 * avg_sub_total
order by stage2.last_name, stage2.first_name, stage2.store_name;
`

  ```text
  refused to write 'query24.preql': not syntactically valid Trilogy.

  Parse error:
    --> 22:1
     |
  22 | union join ss.ticket_number = sr.ticket_number
     | ^---
     |
     = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
   sr.ticket_number is not null ??? union join ss.ticket_number =

  Write stats: received 1262 chars / 1262 bytes; tail: …' by stage2.last_name, stage2.first_name, stage2.store_name;\\n'.
  ```
- `trilogy run --config trilogy.toml duckdb`

  ```text
  Syntax error in stdin:  --> 1:1
    |
  1 | duckdb;
    | ^---
    |
    = expected start
  Location:
   ??? duckdb;
  ```
- `trilogy file write query28.preql --content import raw.store_sales as ss;

select
    avg(ss.list_price ? (ss.quantity between 0 and 5 and (ss.list_price betw…(ss.list_price between 154 and 164 or ss.coupon_amt between 7326 and 8326 or ss.wholesale_cost between 7 and 27))) as bucket6_unique_list_price
by *;`

  ```text
  refused to write 'query28.preql': not syntactically valid Trilogy.

  Parse error:
    --> 27:1
     |
  27 | by *;
     | ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
   as bucket6_unique_list_price ??? by *;

  Write stats: received 3767 chars / 3767 bytes; tail: …'_cost between 7 and 27))) as bucket6_unique_list_price\\nby *;'.
  ```
- `trilogy file write query28.preql --content import raw.store_sales as ss;

select
    avg(ss.list_price ? (ss.quantity between 0 and 5 and (ss.list_price betw…(ss.list_price between 154 and 164 or ss.coupon_amt between 7326 and 8326 or ss.wholesale_cost between 7 and 27))) as bucket6_unique_list_price
by *;`

  ```text
  refused to write 'query28.preql': not syntactically valid Trilogy.

  Parse error:
    --> 22:1
     |
  22 | by *;
     | ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
   as bucket6_unique_list_price ??? by *;

  Write stats: received 3762 chars / 3762 bytes; tail: …'_cost between 7 and 27))) as bucket6_unique_list_price\\nby *;'.
  ```
- `trilogy run --import raw/store_sales:ss select count(ss.customer.customer_sk) as cnt, count(ss.customer.customer_id) as cnt2, count(*) as cnt3 limit 5;`

  ```text
  Syntax error in stdin: Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...ustomer.customer_id) as cnt2, ??? count(*) as cnt3 limit 5;
  ```
- `trilogy file write query40.preql --content import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

# Items whose current price is between 0.99 and…_on_or_after_0311
having
  unit_price_before_0311 != 0 or unit_price_on_or_after_0311 != 0
order by
  warehouse_state asc,
  item_code asc
limit 100;`

  ```text
  refused to write 'query40.preql': not syntactically valid Trilogy.

  Parse error:
    --> 15:3
     |
  15 |   ? item_filter
     |   ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...e.date < '2000-03-11'::date   ??? ? item_filter   ? warehouse_re...

  Write stats: received 1344 chars / 1344 bytes; tail: …'0\\norder by\\n  warehouse_state asc,\\n  item_code asc\\nlimit 100;'.
  ```
- `trilogy file write query44.preql -e -c import raw.store_sales as ss;

# Average net profit per item at store_sk = 1
auto item_avg_profit <- avg(ss.net_profit…= 1
select
  ss.item.item_sk,
  ss.item.product_name,
  item_avg_profit
having
  item_avg_profit > threshold
order by
  item_avg_profit asc
limit 10;`

  ```text
  refused to write 'query44.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:63
    |
  4 | auto item_avg_profit <- avg(ss.net_profit) by ss.item.item_sk as item_avg;
    |                                                               ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, window_sql_over, or OVER_COMPONENT_REF
  Location:
  ...et_profit) by ss.item.item_sk ??? as item_avg;  # Average net pr...

  Write stats: received 702 chars / 702 bytes; tail: …'_profit > threshold\\norder by\\n  item_avg_profit asc\\nlimit 10;'.
  ```
- `trilogy file write query49.preql --content import raw.web_sales as web;
import raw.web_returns as wr;
import raw.catalog_sales as cat;
import raw.catalog_ret…r by
    channel asc nulls first,
    return_rank asc nulls first,
    currency_rank asc nulls first,
    per_item.item_sk asc nulls first
limit 100;`

  ```text
  refused to write 'query49.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...m(net_paid) as currency_ratio ??? from (   # --- web channel ---...

  Write stats: received 2854 chars / 2854 bytes; tail: …'nulls first,\\n    per_item.item_sk asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query54.preql --content import raw.catalog_sales as cs;
import raw.web_sales as ws;
import raw.store_sales as ss;

# December 1998 month_s… and ss.store.state = ss.customer.customer_address.state
select
    ss.customer.customer_sk as cust_sk,
    sum(ss.ext_sales_price) as total_price
;
`

  ```text
  refused to write 'query54.preql': not syntactically valid Trilogy.

  Parse error:
    --> 24:8
     |
  24 | union((cat_cust), (web_cust)) -> (cust_sk)
     |        ^---
     |
     = expected select_statement
  Location:
  ...qualifying_cust_raw as union(( ??? cat_cust), (web_cust)) -> (cus...

  Write stats: received 1139 chars / 1139 bytes; tail: …'sk as cust_sk,\\n    sum(ss.ext_sales_price) as total_price\\n;\\n'.
  ```
- `trilogy run --import raw/date_dim:date_dim select date_dim.year, min(date_dim.week_seq) as min_ws, max(date_dim.week_seq) as max_ws, count(distinct date_dim.week_seq) as num_weeks where date_dim.year in (2000, 2001, 2002) group by date_dim.year order by date_dim.year;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...im.year in (2000, 2001, 2002) ??? group by date_dim.year order b...
  ```
- `trilogy file write query59.preql --content import raw.store_sales as ss;

# Filtered aggregates per day of week (unit price sum) for a given year
def sun(yr)…= ny.store_code and ty.week_seq = ny.week_seq - 52
order by ty.store_name nulls first, ty.store_code nulls first, ty.week_seq nulls first
limit 100;
`

  ```text
  refused to write 'query59.preql': not syntactically valid Trilogy.

  Parse error:
    --> 55:20
     |
  55 | union join ty = ny on ty.store_code = ny.store_code and ty.week_seq = ny.week_seq - 52
     |                    ^---
     |
     = expected limit, order_by, where, having, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, select_grouping, or JOIN_TYPE
  Location:
   sat_ratio union join ty = ny ??? on ty.store_code = ny.store_co...

  Write stats: received 2007 chars / 2007 bytes; tail: …'.store_code nulls first, ty.week_seq nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query59.preql --content import raw.store_sales as ss;

# Filtered aggregates per day of week (unit price sum) for a given year
def sun(yr)…= ny.store_code and ty.week_seq = ny.week_seq - 52
order by ty.store_name nulls first, ty.store_code nulls first, ty.week_seq nulls first
limit 100;
`

  ```text
  refused to write 'query59.preql': not syntactically valid Trilogy.

  Parse error:
    --> 55:20
     |
  55 | union join ty = ny on ty.store_code = ny.store_code and ty.week_seq = ny.week_seq - 52
     |                    ^---
     |
     = expected limit, order_by, where, having, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, select_grouping, or JOIN_TYPE
  Location:
   sat_ratio union join ty = ny ??? on ty.store_code = ny.store_co...

  Write stats: received 2007 chars / 2007 bytes; tail: …'.store_code nulls first, ty.week_seq nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query59.preql --content import raw.store_sales as ss;

# Filtered aggregates per day of week (unit price sum) for a given year
def sun(yr)…= ny.store_code and ty.week_seq = ny.week_seq - 52
order by ty.store_name nulls first, ty.store_code nulls first, ty.week_seq nulls first
limit 100;
`

  ```text
  refused to write 'query59.preql': not syntactically valid Trilogy.

  Parse error:
    --> 55:20
     |
  55 | union join ty = ny on ty.store_code = ny.store_code and ty.week_seq = ny.week_seq - 52
     |                    ^---
     |
     = expected limit, order_by, where, having, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, select_grouping, or JOIN_TYPE
  Location:
   sat_ratio union join ty = ny ??? on ty.store_code = ny.store_co...

  Write stats: received 2007 chars / 2007 bytes; tail: …'.store_code nulls first, ty.week_seq nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query59.preql --content import raw.store_sales as ss;

# Filtered aggregates per day of week (unit price sum) for a given year
def sun(yr)…ore_code is not null and ny.store_code is not null
order by ty.store_name nulls first, ty.store_code nulls first, ty.week_seq nulls first
limit 100;
`

  ```text
  refused to write 'query59.preql': not syntactically valid Trilogy.

  Parse error:
    --> 55:20
     |
  55 | union join ty = ny on ty.store_code = ny.store_code and ty.week_seq = ny.week_seq - 52
     |                    ^---
     |
     = expected limit, order_by, where, having, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, select_grouping, or JOIN_TYPE
  Location:
   sat_ratio union join ty = ny ??? on ty.store_code = ny.store_co...

  Write stats: received 2069 chars / 2069 bytes; tail: …'.store_code nulls first, ty.week_seq nulls first\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw/store_sales:ss select ss.store.store_name, ss.date_dim.week_seq, ss.date_dim.dow, sum(ss.sales_price) as day_total where ss.date_dim…le' group by ss.store.store_name, ss.date_dim.week_seq, ss.date_dim.dow order by ss.store.store_name, ss.date_dim.week_seq, ss.date_dim.dow limit 20;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
   ss.store.store_name = 'able' ??? group by ss.store.store_name,
  ```
- `trilogy file write query60.preql --content import raw.item as item;
import raw.catalog_sales as catalog;
import raw.store_sales as store;
import raw.web_sale…meric)

select
  combined.item_code,
  sum(combined.total) as total
order by
  combined.item_code asc nulls first,
  total asc nulls first
limit 100;`

  ```text
  refused to write 'query60.preql': not syntactically valid Trilogy.

  Parse error:
    --> 12:5
     |
  12 |     import raw.catalog_sales as catalog;
     |     ^---
     |
     = expected select_statement
  Location:
  ...th combined as union(   (     ??? import raw.catalog_sales as ca...

  Write stats: received 1346 chars / 1346 bytes; tail: …'tem_code asc nulls first,\\n  total asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query60.preql --content import raw.item as item;
import raw.catalog_sales as catalog;
import raw.store_sales as store;
import raw.web_sale…meric)

select
  combined.item_code,
  sum(combined.total) as total
order by
  combined.item_code asc nulls first,
  total asc nulls first
limit 100;`

  ```text
  refused to write 'query60.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...em_code string, total numeric) ???  select   combined.item_code,...

  Write stats: received 1235 chars / 1235 bytes; tail: …'tem_code asc nulls first,\\n  total asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query62.preql -e -c import raw.web_sales as ws;

# Filter: ship date in year 2000, and all three dims recorded
where ws.ship_date.year = 2…ket_0_to_30 is not null  # placeholder - actually need coalesce
order by wh_name nulls first, ship_type nulls first, site_name nulls first
limit 100;`

  ```text
  refused to write 'query62.preql': not syntactically valid Trilogy.

  Parse error:
    --> 14:84
     |
  14 |         where date_diff(ws.sold_date.date, ws.ship_date.date, day) between 0 and 30,
     |                                                                                    ^---
     |
     = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...te.date, day) between 0 and 30 ??? ,     count(ws.order_number) a...

  Write stats: received 1197 chars / 1197 bytes; tail: …'rst, ship_type nulls first, site_name nulls first\\nlimit 100;'.
  ```
- `trilogy file write query71.preql --content import raw/store_sales as ss;
import raw/catalog_sales as cs;
import raw/web_sales as ws;

with combined as union(…rice
order by
  total_ext_sales_price desc nulls first,
  combined.brand_id nulls first,
  combined.hour nulls first,
  combined.minute nulls first
;`

  ```text
  refused to write 'query71.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/store_sales as ss;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/store_sales as ss; import

  Write stats: received 1521 chars / 1521 bytes; tail: …'  combined.hour nulls first,\\n  combined.minute nulls first\\n;'.
  ```
- `trilogy file write query71.preql`

  ```text
  refused to write 'query71.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...hour, minute, ext_sales_price) ???

   select
     combined.brand_i...

  Write stats: received 1575 chars / 1575 bytes; tail: …'combined.hour nulls first,\r\\n  combined.minute nulls first\r\\n;'.
  ```
- `trilogy file write query72.preql --content import raw.catalog_sales as cs;
import raw.inventory as inv;
import raw.item as item;
import raw.warehouse as wh;
…   total_orders desc nulls first,
    item.item_desc nulls first,
    wh.warehouse_name nulls first,
    cs.sold_date.week_seq nulls first
limit 100;`

  ```text
  refused to write 'query72.preql': not syntactically valid Trilogy.

  Parse error:
    --> 17:1
     |
  17 | where inv.quantity_on_hand < cs.quantity
     | ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
  ...hand < catalog_sales quantity ??? where inv.quantity_on_hand < c...

  Write stats: received 1061 chars / 1061 bytes; tail: …'ulls first,\\n    cs.sold_date.week_seq nulls first\\nlimit 100;'.
  ```
- `trilogy file write query74.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;

select
    store_sales.customer.custome…  and (ws_2002 / ws_2001) > (ss_2002 / ss_2001)
select
    customer_code,
    first_name,
    last_name
order by customer_code nulls first
limit 100;`

  ```text
  refused to write 'query74.preql': not syntactically valid Trilogy.

  Parse error:
    --> 17:1
     |
  17 | select
     | ^---
     |
     = expected limit, order_by, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...s_2001) > (ss_2002 / ss_2001) ??? select     customer_code,

  Write stats: received 862 chars / 862 bytes; tail: …'\\n    last_name\\norder by customer_code nulls first\\nlimit 100;'.
  ```
- `trilogy file write query75.preql --content import raw.store_sales as st;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r…_id = y2001.brand_id
  and y2002.class_id = y2001.class_id
  and y2002.category_id = y2001.category_id
  and y2002.manufact_id = y2001.manufact_id)
;`

  ```text
  refused to write 'query75.preql': not syntactically valid Trilogy.

  Parse error:
    --> 85:3
     |
  85 |   on (combined_dedup.year = catalog_lines.year
     |   ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
  ...bined_dedup = catalog_lines   ??? on (combined_dedup.year = cata...

  Write stats: received 5606 chars / 5606 bytes; tail: …'1.category_id\\n  and y2002.manufact_id = y2001.manufact_id)\\n;'.
  ```
- `trilogy file write query75.preql --content import raw.store_sales as st;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r…001.bid
  and yr2002.cid = yr2001.cid
  and yr2002.catid = yr2001.catid
  and yr2002.mid = yr2001.mid
order by qty_diff asc, amt_diff asc
limit 100;
`

  ```text
  refused to write 'query75.preql': not syntactically valid Trilogy.

  Parse error:
    --> 74:1
     |
  74 | with yearly_agg as
     | ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
  ...brand/class/category/manufact ??? with yearly_agg as select

  Write stats: received 3454 chars / 3454 bytes; tail: …'= yr2001.mid\\norder by qty_diff asc, amt_diff asc\\nlimit 100;\\n'.
  ```
- `trilogy file write query77.preql --content # Query 77: Channel sales, returns, and profit report
# Period: 2000-08-23 through 2000-09-22

import raw.date_dim…p (combined.channel, combined.outlet_id)
order by combined.channel nulls first, combined.outlet_id nulls first, sum(combined.returns) desc
limit 100;`

  ```text
  refused to write 'query77.preql': not syntactically valid Trilogy.

  Parse error:
   --> 8:6
    |
  8 |     (import raw.store_sales as ss;
    |      ^---
    |
    = expected select_statement
  Location:
  ...ion(     # Store channel     ( ??? import raw.store_sales as ss;

  Write stats: received 2236 chars / 2236 bytes; tail: …'outlet_id nulls first, sum(combined.returns) desc\\nlimit 100;'.
  ```
- `trilogy run --import raw/store_returns:sr select sr.item.item_id, sum(sr.return_quantity) as store_qty where sr.date_dim.week_seq in (5244, 5257, 5264) and s… in (select sr2.item.item_id from raw/store_returns.preql as sr2 where sr2.date_dim.week_seq in (5244, 5257, 5264)) having store_qty is null limit 5;`

  ```text
  Syntax error in stdin: Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...d in (select sr2.item.item_id ??? from raw/store_returns.preql a...
  ```
- `trilogy file write query85.preql --content import raw.web_sales as ws;
import raw.web_returns as wr;

# Join web returns to web sales by item and order numbe…d_cash) as avg_refunded_cash,
    avg(wr.fee) as avg_fee
order by
    r_reason_desc,
    avg_quantity,
    avg_refunded_cash,
    avg_fee
limit 100;
`

  ```text
  refused to write 'query85.preql': not syntactically valid Trilogy.

  Parse error:
   --> 6:45
    |
  6 | union join ws.item.item_sk = wr.item.item_sk;
    |                                             ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
  ...item.item_sk = wr.item.item_sk ??? ; union join ws.order_number =...

  Write stats: received 2485 chars / 2485 bytes; tail: …'avg_quantity,\\n    avg_refunded_cash,\\n    avg_fee\\nlimit 100;\\n'.
  ```
- `trilogy file write query86.preql --content import raw.web_sales as ws;

# Filter to web sales in year 2000
where ws.sold_date.year = 2000
select
    ws.item.…ithin_parent_rank is not null)
order by
    hierarchy_level desc nulls first,
    category nulls first,
    within_parent_rank nulls first
limit 100;`

  ```text
  refused to write 'query86.preql': not syntactically valid Trilogy.

  Parse error:
    --> 10:39
     |
  10 |         when grouping(ws.item.category, ws.item.class) = 3 then 2
     |                                       ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...when grouping(ws.item.category ??? , ws.item.class) = 3 then 2

  Write stats: received 964 chars / 964 bytes; tail: …'y nulls first,\\n    within_parent_rank nulls first\\nlimit 100;'.
  ```
- `trilogy file write query87.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Distinct (last_name, …_combos.sale_date)
select
    count_distinct(store_combos.last_name || store_combos.first_name || store_combos.sale_date::string) as unique_combos
;
`

  ```text
  refused to write 'query87.preql': not syntactically valid Trilogy.

  Parse error:
   --> 6:1
    |
  6 | rowset store_combos as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...mbos from store sales in 2000 ??? rowset store_combos as where s...

  Write stats: received 1413 chars / 1413 bytes; tail: …'_name || store_combos.sale_date::string) as unique_combos\\n;\\n'.
  ```
- `trilogy file write query91.preql --content import raw.catalog_returns as catalog_returns;

where
  catalog_returns.date_dim.moy = 11
  and catalog_returns.da…mer.customer_demographics.marital_status,
  catalog_returns.returning_customer.customer_demographics.education_status
order by
  total_net_loss desc;`

  ```text
  refused to write 'query91.preql': not syntactically valid Trilogy.

  Parse error:
    --> 17:1
     |
  17 | by
     | ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...s.net_loss) as total_net_loss ??? by   catalog_returns.call_cent...

  Write stats: received 1219 chars / 1219 bytes; tail: …'emographics.education_status\\norder by\\n  total_net_loss desc;'.
  ```
- `trilogy file write query94.preql --content import raw.web_sales as ws;
import raw.web_returns as wr;

# Orders that have any web return
with orders_with_retu…er) as order_count,
    sum(ws.ext_ship_cost) as total_ext_ship_cost,
    sum(ws.net_profit) as total_net_profit
order by order_count asc
limit 100
;`

  ```text
  refused to write 'query94.preql': not syntactically valid Trilogy.

  Parse error:
    --> 22:1
     |
  22 | by ws.order_number
     | ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...ehouse_sk) as warehouse_count ??? by ws.order_number ;  # Now fi...

  Write stats: received 922 chars / 922 bytes; tail: …'it) as total_net_profit\\norder by order_count asc\\nlimit 100\\n;'.
  ```
- `trilogy file write query96.preql -e -c import raw.store_sales as ss;

# Count store-sale line items at the full grain
# Each distinct (item, ticket_number) =…tem_sk,
    ss.ticket_number
;

select
    count(line_items.ticket_number) as line_item_count
from line_items
order by
    line_item_count
limit 100;`

  ```text
  refused to write 'query96.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...et_number) as line_item_count ??? from line_items order by     l...

  Write stats: received 471 chars / 471 bytes; tail: …'ount\\nfrom line_items\\norder by\\n    line_item_count\\nlimit 100;'.
  ```
- `trilogy run --import raw.store_sales:ss --format json select sum(items) as total_line_items from (select ss.ticket_number, count(ss.item.item_sk) as items where ss.store.store_name = 'ese' and ss.time_dim.hour = 20 and ss.time_dim.minute >= 30 and ss.household_demographics.dep_count = 7);`

  ```text
  Syntax error in stdin: Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...um(items) as total_line_items ??? from (select ss.ticket_number,...
  ```
- `trilogy file write query96.preql -e -c import raw.store_sales as ss;

# Count store-sale line items at the full grain
# Each distinct (item_sk, ticket_number…aphics.dep_count = 7
select
    ss.ticket_number,
    ss.item.item_sk
;

select
    sum(1) as line_item_count
order by
    line_item_count
limit 100;`

  ```text
  refused to write 'query96.preql': not syntactically valid Trilogy.

  Parse error:
   --> 6:1
    |
  6 | The rowset detail selects the full grain; count rows via sum(1)
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...cket_number) = one line item  ??? The rowset detail selects the

  Write stats: received 494 chars / 494 bytes; tail: …') as line_item_count\\norder by\\n    line_item_count\\nlimit 100;'.
  ```
- `trilogy file write query99.preql --content import raw.catalog_sales as cs;

# Only catalog sales with ship date in year 2000, and warehouse/ship_mode/call_ce… 120 as bucket_over_120
order by
  warehouse_name_20 asc nulls first,
  ship_mode_type asc nulls first,
  call_center_name asc nulls first
limit 100;`

  ```text
  refused to write 'query99.preql': not syntactically valid Trilogy.

  Parse error:
    --> 13:24
     |
  13 |   count(cs.order_number, cs.item.item_sk) ? date_diff(cs.sold_date.date, cs.ship_date.date, day) <= 30 as bucket_30_or_fewer,
     |                        ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ..._name,   count(cs.order_number ??? , cs.item.item_sk) ? date_diff...

  Write stats: received 1247 chars / 1247 bytes; tail: …'c nulls first,\\n  call_center_name asc nulls first\\nlimit 100;'.
  ```

### `type-error`

- `trilogy run --import raw/store_sales:ss where ss.date_dim.year = 2000 and ss.customer.customer_sk is not null select count(ss.customer.last_name || ss.customer.first_name || ss.date_dim.date) as cnt_distinct limit 5;`

  ```text
  Type error in stdin: Invalid argument type 'DATE' passed into CONCAT function in position 2 from concept: ss.date_dim.date. Valid: 'STRING'.
  ```
- `trilogy run --import raw/catalog_sales:cs where cs.sold_date.year = 2000 and cs.bill_customer.customer_sk is not null select count(cs.bill_customer.last_name || cs.bill_customer.first_name || cs.sold_date.date) as cnt_distinct limit 5;`

  ```text
  Type error in stdin: Invalid argument type 'DATE' passed into CONCAT function in position 2 from concept: cs.sold_date.date. Valid: 'STRING'.
  ```
- `trilogy run --import raw/web_sales:ws where ws.sold_date.year = 2000 and ws.bill_customer.customer_sk is not null select count(ws.bill_customer.last_name || ws.bill_customer.first_name || ws.sold_date.date) as cnt_distinct limit 5;`

  ```text
  Type error in stdin: Invalid argument type 'DATE' passed into CONCAT function in position 2 from concept: ws.sold_date.date. Valid: 'STRING'.
  ```
- `trilogy run --import raw/date_dim:date_dim select date_dim.date, date_dim.year, date_dim.week_seq, date_dim.dow, date_dim.day_name where date_dim.date between '2001-12-24' and '2002-01-10' order by date_dim.date;`

  ```text
  Syntax error in stdin: Cannot use BETWEEN with incompatible types DATE and STRING (low)
  ```
- `trilogy run query62.preql`

  ```text
  Type error in query62.preql: Invalid argument type 'BIGINT' passed into CONCAT function in position 1 from concept: ws.order_number. Valid: 'STRING'.
  ```
- `trilogy run query80.preql`

  ```text
  Syntax error in query80.preql: Cannot use BETWEEN with incompatible types DATE and STRING (low)
  ```
- `trilogy run query87.preql`

  ```text
  Unexpected error in query87.preql: Tuple elements have incompatible types STRING and DATE
  ```

### `join-resolution`

- `trilogy run query04.preql`

  ```text
  Resolution error in query04.preql: Query is unresolvable: no complete sources found for output concepts {'store_2001.channel_values.customer_id'}. These concepts could only be resolved from partial sources.
  ```
- `trilogy run query75.preql`

  ```text
  Resolution error in query75.preql: Could not resolve connections for query with output ['local._cs_agg_yr<Purpose.PROPERTY>Derivation.BASIC>', 'local._cs_agg_bid<Purpose.PROPERTY>Derivation.BASIC>', 'local._cs_agg_cid<Purpose.PROPERTY>Derivation.BASIC>', 'local._cs_agg_catid<Purpose.PROPERTY>Derivation.BASIC>', 'local._cs_agg_mid<Purpose.PROPERTY>Derivation.BASIC>', 'local._cs_agg_qty<Purpose.METRIC>Derivation.AGGREGATE>', 'local._cs_agg_val<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run query75.preql`

  ```text
  Resolution error in query75.preql: Could not resolve connections for query with output ['local._st_line_yr<Purpose.PROPERTY>Derivation.BASIC>', 'local._st_line_isk<Purpose.KEY>Derivation.BASIC>', 'local._st_line_nqty<Purpose.PROPERTY>Derivation.BASIC>', 'local._st_line_nval<Purpose.PROPERTY>Derivation.BASIC>'] from current model.
  ```
- `trilogy run query75.preql`

  ```text
  Resolution error in query75.preql: Could not resolve connections for query with output ['local._st_agg_yr<Purpose.PROPERTY>Derivation.BASIC>', 'local._st_agg_bid<Purpose.PROPERTY>Derivation.BASIC>', 'local._st_agg_cid<Purpose.PROPERTY>Derivation.BASIC>', 'local._st_agg_catid<Purpose.PROPERTY>Derivation.BASIC>', 'local._st_agg_mid<Purpose.PROPERTY>Derivation.BASIC>', 'local._st_agg_qty<Purpose.METRIC>Derivation.AGGREGATE>', 'local._st_agg_val<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run query75.preql`

  ```text
  Resolution error in query75.preql: Could not resolve connections for query with output ['local.yr<Purpose.PROPERTY>Derivation.BASIC>', 'local.bid<Purpose.PROPERTY>Derivation.BASIC>', 'local.cid<Purpose.PROPERTY>Derivation.BASIC>', 'local.catid<Purpose.PROPERTY>Derivation.BASIC>', 'local.mid<Purpose.PROPERTY>Derivation.BASIC>', 'local.qty<Purpose.METRIC>Derivation.AGGREGATE>', 'local.val<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```

### `cli-misuse`

- `trilogy explore raw/sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/sales.preql' does not exist.
  ```
- `trilogy explore raw/raw_model.preql`

  ```text
  Invalid value for 'PATH': File 'raw/raw_model.preql' does not exist.
  ```
- `trilogy explore raw/`

  ```text
  Invalid value for 'PATH': File 'raw/' is a directory.
  ```

### `planner-recursion`

- `trilogy run query14.preql`

  ```text
  Resolution error in query14.preql: query could not be planned; this is a bug.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw/store_sales:store_sales select count_distinct(store_sales.ticket_number) limit 5;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count_distinct(store_sales.ticket_number) as ticket_number_count_distinct`
  Location:
  ...ct(store_sales.ticket_number) ??? limit 5;
  ```
