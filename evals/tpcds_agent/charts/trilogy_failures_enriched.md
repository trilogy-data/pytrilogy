# Trilogy failure analysis — 20260706-001731

- Run `20260706-001731` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 1169 | failed: 102 (9%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 61 | 60% |
| `syntax-parse` | 31 | 30% |
| `cli-misuse` | 4 | 4% |
| `syntax-missing-alias` | 3 | 3% |
| `type-error` | 2 | 2% |
| `undefined-concept` | 1 | 1% |

## Detail

### `other`

- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Unexpected error in query08.preql: Could not render the query: Missing source reference to qualifying_zips.zip. A planned reference has no backing source CTE -- typically an unsupported cross-rowset or membership shape the planner could not wire. Review the rowset/join structure (or file an issue if the query looks valid).

  Full SQL with sentinel(s):

  WITH
  quizzical as (
  SELECT
      SUBSTRING(STRING_SPLIT( :zips , ',' ),1,2) as "match_prefix"

  WHERE
      STRING_SPLIT( :zips , ',' ) in (select INVALID_REFERENCE_BUG<Missing source reference to qualifying_zips.zip>."qualifying_zips_zip" from INVALID_REFERENCE_BUG<Missing source reference to qualifying_zips.zip> where INVALID_REFERENCE_BUG<Missing source reference to qualifying_zips.zip>."qualifying_zips_zip" is not null)
  )
  SELECT
      "ss_store_store"."S_STORE_NAME" as "store_name",
      sum("ss_store_sales"."SS_NET_PROFIT") as "total_net_profit"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
      INNER JOIN "store" as "ss_store_store" on "ss_store_sales"."SS_STORE_SK" = "ss_store_store"."S_STORE_SK"
  WHERE
      "ss_date_date"."D_YEAR" = 1998 and "ss_date_date"."D_QOY" = 2 and SUBSTRING("ss_store_store"."S_ZIP",1,2) in (select quizzical."match_prefix" from quizzical where quizzical."match_prefix" is not null)

  GROUP BY
      1
  ORDER BY
      "store_name" asc
  LIMIT (100)
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Syntax error in query08.preql: 3 undefined concept references; fix all before re-running:
    - local.zip_val (line 14, in WHERE)
    - local.zip_val (line 20, col 9, in WHERE)
    - local.zip_val (line 21, col 13, in WHERE)
  ```
- `trilogy run query10.preql`

  ```text
  Syntax error in query10.preql: Undefined concept: customer.address.county. Suggestions: ['ss.customer.address.county', 'ss.return_customer.address.county', 'ss.store.county', 'ss.return_store.county', 'ss.sale_address.county', 'ws.billing_customer.address.county']
  ```
- `trilogy run query10.preql`

  ```text
  Resolution error in query10.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 20). The requested concepts split into 2 disconnected subgraphs: {cat_cust_2002, web_cust_2002}; {college_dependent_count, credit_rating, customer_count, customer_count2, customer_count3, customer_count4, customer_count5, customer_count6, dependent_count, education_status, employed_dependent_count, gender, marital_status, purchase_estimate, store_cust_2002, target_counties, ss.customer.id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query10.preql`

  ```text
  Syntax error in query10.preql: Duplicate select output for local.customer_count; Line: 17
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: ORDER BY contains aggregate `grouping(filtered_results.all.channel)` (line 50), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(filtered_results.all.channel) as g order by g desc`.
  ```
- `trilogy run query29.preql`

  ```text
  Syntax error in query29.preql: Comparison `sr.return_date.month_of_year <= 12` matches every value of enum field 'sr.return_date.month_of_year', which contains only these values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12. It is always true and should be removed.
  ```
- `trilogy run query44.preql`

  ```text
  Resolution error in query44.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {best_ranked.qualifying_items.ss.item.product_name, best_ranked.rank_asc, worst_ranked.qualifying_items.ss.item.product_name}
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 17). The requested concepts split into 4 disconnected subgraphs: {cat_ext_list_price_by_item}; {cat_refund_by_item}; {sale_line_count, sum_coupon_amt, sum_list_price, sum_wholesale_cost, ss.customer.address.city, ss.customer.address.street_name, ss.customer.address.street_number, ss.customer.address.zip, ss.customer.demographics.marital_status, ss.customer.first_sales_date.year, ss.customer.first_shipto_date.year, ss.customer_demographic.marital_status, ss.date.year, ss.item.color, ss.item.current_price, ss.item.id, ss.item.product_name, ss.sale_address.city, ss.sale_address.street_name, ss.sale_address.street_number, ss.sale_address.zip, ss.store.name, ss.store.zip, ss.ticket_number}; {sr.item.id, sr.ticket_number}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Nothing was executed: parsed 7 definition statement(s) (4 imports, 3 rowsets) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 23). The requested concepts split into 2 disconnected subgraphs: {sale_line_count, sum_coupon_amt, sum_list_price, sum_wholesale_cost, ss.customer.address.city, ss.customer.address.street_name, ss.customer.address.street_number, ss.customer.address.zip, ss.customer.demographics.marital_status, ss.customer.first_sales_date.year, ss.customer.first_shipto_date.year, ss.customer_demographic.marital_status, ss.date.year, ss.item.color, ss.item.current_price, ss.item.id, ss.item.product_name, ss.sale_address.city, ss.sale_address.street_name, ss.sale_address.street_number, ss.sale_address.zip, ss.store.name, ss.store.zip, ss.ticket_number}; {sr.ticket_number}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Ambiguous reference 'yearly_agg.year': matches ['yearly_agg.ss.customer.first_sales_date.year', 'yearly_agg.ss.customer.first_shipto_date.year', 'yearly_agg.ss.date.year']. Qualify the full path to disambiguate.
  ```
- `trilogy run --import raw.store_sales:ss --import raw.web_sales:ws select ss.item.text_id, ss.customer.text_id, ws.item.text_id, ws.billing_customer.text_id w…ustomer.id is not null and ws.is_returned!=true and ss.customer.text_id = ws.billing_customer.text_id and ss.item.text_id = ws.item.text_id limit 10;`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {ss.customer.id, ss.customer.text_id, ss.date.year, ss.is_returned, ss.item.text_id}; {ws.billing_customer.id, ws.billing_customer.text_id, ws.date.year, ws.is_returned, ws.item.text_id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/all_sales.preql`

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
- `trilogy database describe store_returns`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy database describe reason`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run query04.preql duckdb`

  ```text
  Syntax error in query04.preql: Nothing was executed: parsed 7 definition statement(s) (4 rowsets, 3 imports) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy run query04.preql duckdb`

  ```text
  Syntax error in query04.preql: Nothing was executed: parsed 10 definition statement(s) (7 rowsets, 3 imports) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy run query04.preql duckdb`

  ```text
  Unexpected error in query04.preql: Conflicting join types (full, left outer) on keys joined into one group: a FULL/UNION join cannot be mixed with another type on the same key (it is ambiguous whether the key is required or one-sided). Make the whole group one type (e.g. `UNION JOIN a = b = c`), or use a distinct key.
  ```
- `trilogy run query13.preql`

  ```text
  Syntax error in query13.preql: 6 undefined concept references; fix all before re-running:
    - store_sales.customer_demographics.marital_status (line 7, col 8, in WHERE); did you mean: store_sales.customer.demographics.marital_status, store_sales.return_customer.demographics.marital_status, store_sales.customer_demographic.marital_status?
    - store_sales.customer_demographics.education_status (line 8, col 12, in WHERE); did you mean: store_sales.customer.demographics.education_status, store_sales.return_customer.demographics.education_status, store_sales.customer_demographic.education_status?
    - store_sales.customer_demographics.marital_status (line 12, col 8, in WHERE); did you mean: store_sales.customer.demographics.marital_status, store_sales.return_customer.demographics.marital_status, store_sales.customer_demographic.marital_status?
    - store_sales.customer_demographics.education_status (line 13, col 12, in WHERE); did you mean: store_sales.customer.demographics.education_status, store_sales.return_customer.demographics.education_status, store_sales.customer_demographic.education_status?
    - store_sales.customer_demographics.marital_status (line 17, col 8, in WHERE); did you mean: store_sales.customer.demographics.marital_status, store_sales.return_customer.demographics.marital_status, store_sales.customer_demographic.marital_status?
    - store_sales.customer_demographics.education_status (line 18, col 12, in WHERE); did you mean: store_sales.customer.demographics.education_status, store_sales.return_customer.demographics.education_status, store_sales.customer_demographic.education_status?
  ```
- `trilogy run query16.preql`

  ```text
  Syntax error in query16.preql: Undefined concept: raw.catalog_returns.order_number (line 16, col 30, in WHERE). Suggestions: ['cs.order_number', 'multi_warehouse.cs.order_number']
  ```
- `trilogy run query18.preql`

  ```text
  Syntax error in query18.preql: ORDER BY contains aggregate `grouping(cs.bill_customer.address.country)` (line 3), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(cs.bill_customer.address.country) as g order by g desc`.
  ```
- `trilogy run query20.preql`

  ```text
  Syntax error in query20.preql: 4 undefined concept references; fix all before re-running:
    - item_category (line 28, col 5, in ORDER BY); did you mean: item_totals.item_category, item.category, item.category_id, cs.item.category?
    - item_class (line 29, col 5, in ORDER BY); did you mean: item_totals.item_class, item.class, item.class_id, cs.item.class?
    - item_code (line 30, col 5, in ORDER BY); did you mean: item_totals.item_code, item.desc, item.color, item.id?
    - item_desc (line 31, col 5, in ORDER BY); did you mean: item_totals.item_desc, item.desc, cs.item.desc, item.id?
  ```
- `trilogy file read query25.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query26.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query27.preql`

  ```text
  Syntax error in query27.preql: Comparison `ss.customer_demographic.marital_status = 'Single'` can never match enum field 'ss.customer_demographic.marital_status', which contains only these values: 'M', 'S', 'D', 'W', 'U'. It is always false and should be removed.
  ```
- `trilogy run query31.preql`

  ```text
  Resolution error in query31.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 23). The requested concepts split into 2 disconnected subgraphs: {store_by_county_q.county, store_by_county_q.quarter, store_by_county_q.store_ext_total}; {web_by_county_q.county}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read query31.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query32.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query37.preql`

  ```text
  Syntax error in query37.preql: 5 undefined concept references; fix all before re-running:
    - item.current_price (line 14, col 3, in SELECT); did you mean: cs.item.current_price, inv.item.current_price, item.text_id?
    - item.current_price (line 6, col 3, in WHERE); did you mean: cs.item.current_price, inv.item.current_price, item.text_id?
    - item.manufacturer_id (line 7, col 7, in WHERE); did you mean: cs.item.manufacturer_id, inv.item.manufacturer_id, cs.item.manager_id?
    - item.id (line 10, col 7, in WHERE); did you mean: cs.item.id, inv.item.id, cs.date.id, cs.ship_date.id, cs.sold_date.id, cs.time.id?
    - item.text_id (line 16, col 3, in ORDER BY); did you mean: cs.item.text_id, inv.item.text_id, cs.date.text_id, cs.ship_date.text_id, cs.sold_date.text_id, cs.time.text_id?
  ```
- `trilogy file read query37.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw/store_sales:ss --import raw/catalog_sales:cs select count(ss.customer.last_name) as cnt where ss.date.year = 2000 and cs.sold_date.y…stomer.last_name = cs.bill_customer.last_name and ss.customer.first_name = cs.bill_customer.first_name and ss.date.date = cs.sold_date.date limit 10;`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {cs.bill_customer.first_name, cs.bill_customer.id, cs.bill_customer.last_name, cs.sold_date.date, cs.sold_date.year}; {ss.customer.first_name, ss.customer.id, ss.customer.last_name, ss.date.date, ss.date.year}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/item.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw/store_sales:store_sales select date.day_of_week limit 100;`

  ```text
  Syntax error in stdin: Undefined concept: date.day_of_week (line 2, col 8, in SELECT). Suggestions: ['store_sales.date.day_of_week', 'store_sales.store.date.day_of_week', 'store_sales.return_store.date.day_of_week', 'store_sales.return_date.day_of_week', 'store_sales.customer.first_sales_date.day_of_week', 'store_sales.customer.first_shipto_date.day_of_week']
  ```
- `trilogy file read query49.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query50.preql`

  ```text
  Syntax error in query50.preql: Undefined concept: ss.return_date. Suggestions: ['ss.return_date.id', 'ss.return_date.text_id', 'ss.return_date._date_string', 'ss.return_date.date', 'ss.return_date.year', 'ss.return_date.day_of_week']
  ```
- `trilogy file read query52.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query53.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query54.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query57.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query62.preql`

  ```text
  Syntax error in query62.preql: 5 undefined concept references; fix all before re-running:
    - warehouse.name (line 4, in SELECT); did you mean: web.warehouse.name, web.web_site.name, web_site.name, warehouse.id, warehouse_name_prefix?
    - ship_date.year (line 5, col 3, in WHERE); did you mean: web.ship_date.year, web.date.year, web.return_date.year, web.billing_customer.first_sales_date.year, web.billing_customer.first_shipto_date.year, web.ship_customer.first_sales_date.year?
    - warehouse.id (line 6, col 7, in WHERE); did you mean: web.warehouse.id, web.item.id, web.date.id, web.ship_date.id, web.return_date.id, web.time.id?
    - ship_mode.id (line 7, col 7, in WHERE); did you mean: web.ship_mode.id, web.item.id, web.date.id, web.ship_date.id, web.return_date.id, web.time.id?
    - web_site.id (line 8, col 7, in WHERE); did you mean: web.web_site.id, web.item.id, web.date.id, web.ship_date.id, web.return_date.id, web.time.id?
  ```
- `trilogy run query65.preql`

  ```text
  Syntax error in query65.preql: ORDER BY references 'ss.store.id', which is not in the SELECT projection (line 6). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --ss.store.id order by ss.store.id asc`.
  ```
- `trilogy file read query69.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query72.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query74.preql`

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
- `trilogy run query77.preql duckdb`

  ```text
  Syntax error in query77.preql: Undefined concept: s.channel_label (line 12, in SELECT). Suggestions: ['channel_label', 's.channel', 's.channel_dim_id']
  ```
- `trilogy run query82.preql`

  ```text
  Resolution error in query82.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {inv.date.date, inv.quantity_on_hand}; {ss.item.current_price, ss.item.desc, ss.item.manufacturer_id, ss.item.text_id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query82.preql`

  ```text
  Resolution error in query82.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 5). The requested concepts split into 2 disconnected subgraphs: {inv.date.date, inv.quantity_on_hand}; {item.current_price, item.manufacturer_id, item.text_id, description, item_code, price}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query82.preql`

  ```text
  Resolution error in query82.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 5). The requested concepts split into 2 disconnected subgraphs: {inv.date.date, inv.quantity_on_hand}; {item.current_price, item.manufacturer_id, item.text_id, description, item_code, price}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read query82.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query84.preql`

  ```text
  Unexpected error in query84.preql: Cannot join `ret.ticket_number` to itself (`ret.ticket_number` resolves to the same key `ret.ticket_number`), which degenerates to `1=1`. Join distinct keys (e.g. separate rowset outputs or distinct expressions).
  ```
- `trilogy run query84.preql`

  ```text
  Resolution error in query84.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {cust.address.city, cust.demographics.id, cust.household_demographic.income_band.lower_bound, cust.household_demographic.income_band.upper_bound, customer_code, full_name}; {ret.customer_demographic.id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query85.preql duckdb`

  ```text
  Syntax error in query85.preql: Comparison `wr.returning_demographic.marital_status = 'Married'` can never match enum field 'wr.returning_demographic.marital_status', which contains only these values: 'M', 'S', 'D', 'W', 'U'. It is always false and should be removed.
  ```
- `trilogy file read query87.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query95.preql`

  ```text
  Syntax error in query95.preql: HAVING filters on a dimension outside the SELECT projection, but the select has no grain key to anchor a post-aggregation semijoin (line 9). Move the filter to WHERE to filter before aggregation.
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

- `trilogy run --import raw/all_sales:s select s.channel, count(s.order_id) as cnt where s.return_date.date between '2000-08-23'::date and '2000-09-06'::date and s.return_channel_dim_text_id is not null by s.channel;`

  ```text
  Syntax error in stdin: Syntax [213]: A `by <grain>` clause must follow an aggregate, but the expression before it has none. To take each distinct value once per grain, wrap it in `group(...)` - e.g. `group(item.current_price) by item.id, item.category`. For a reduction, use an aggregate: `sum(x) by ...`, `avg(x) by ...`, `max(x) by ...`.
  Location:
  ...annel_dim_text_id is not null ??? by s.channel;
  ```
- `trilogy file write query08.preql --content import raw.store_sales as ss;

parameter zips string;

# Preferred customers with their home address ZIP
auto pref… matching_stores.store_name
select
    ss.store.name as store_name,
    sum(ss.net_profit) as total_net_profit
order by
    store_name asc
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
    --> 15:1
     |
  15 | select
     | ^---
     |
     = expected limit, order_by, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...nt having     cust_count > 10 ??? select     qualifying_zips.zip...

  Write stats: received 1180 chars / 1180 bytes; tail: …') as total_net_profit\\norder by\\n    store_name asc\\nlimit 100;'.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as all;
import raw.item as item;

# Step 1: Find (brand, class, category) combos that appear …y
    all.channel nulls first,
    all.item.brand_id nulls first,
    all.item.class_id nulls first,
    all.item.category_id nulls first
limit 100;
`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
    --> 14:5
     |
  14 |     by all.item.brand_id, all.item.class_id, all.item.category_id;
     |     ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...te) between 1999 and 2001     ??? by all.item.brand_id, all.item...

  Write stats: received 1533 chars / 1533 bytes; tail: …'ulls first,\\n    all.item.category_id nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as all;

# Step 1: Find (brand_id, class_id, category_id) combos that appear in all 3 channel…y
    all.channel nulls first,
    all.item.brand_id nulls first,
    all.item.class_id nulls first,
    all.item.category_id nulls first
limit 100;
`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...verall_stats.overall_avg_sale ??? from overall_stats) order by

  Write stats: received 1773 chars / 1773 bytes; tail: …'ulls first,\\n    all.item.category_id nulls first\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw.all_sales:all select all.channel, count(all.item.id) by rollup (all.channel) limit 10;`

  ```text
  Syntax error in stdin:  --> 2:43
    |
  2 | select all.channel, count(all.item.id) by rollup (all.channel) limit 10;
    |                                           ^---
    |
    = unexpected GROUPING_LEAD
  Location:
  ...hannel, count(all.item.id) by ??? rollup (all.channel) limit 10;...
  ```
- `trilogy file write query23.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Step 1: Frequent item…um(combined.sales_value) as total_sales
order by
    combined.last_name asc,
    combined.first_name asc,
    total_sales asc nulls first
limit 100
;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
    --> 15:21
     |
  15 |     count(ss.item.id, ss.date.date) > 4
     |                     ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...te having     count(ss.item.id ??? , ss.date.date) > 4 select

  Write stats: received 2282 chars / 2282 bytes; tail: …'.first_name asc,\\n    total_sales asc nulls first\\nlimit 100\\n;'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Step 1: Frequent item…um(combined.sales_value) as total_sales
order by
    combined.last_name asc,
    combined.first_name asc,
    total_sales asc nulls first
limit 100
;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block - they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...m.desc, 1, 30) as desc_prefix ??? auto pair_count <- count(item_...

  Write stats: received 2239 chars / 2239 bytes; tail: …'.first_name asc,\\n    total_sales asc nulls first\\nlimit 100\\n;'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Frequent items: desc_…um(combined.sales_value) as total_sales
order by
    combined.last_name asc,
    combined.first_name asc,
    total_sales asc nulls first
limit 100
;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...item_id, cust_id, sales_value) ???  where combined.item_id in fr...

  Write stats: received 1966 chars / 1966 bytes; tail: …'.first_name asc,\\n    total_sales asc nulls first\\nlimit 100\\n;'.
  ```
- `trilogy file write query44.preql --content import raw.store_sales as ss;

# Average net profit per item at store id = 1
auto avg_net_profit <- avg(ss.net_pro…ct_name as worst_performer_product_name
order by
    rank asc,
    best_performer_product_name desc,
    worst_performer_product_name desc
limit 100;`

  ```text
  refused to write 'query44.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:80
    |
  7 | auto threshold <- 0.9 * avg(ss.net_profit ? ss.sale_address.id is null) by by *;
    |                                                                                ^---
    |
    = expected access_chain
  Location:
  ...le_address.id is null) by by * ??? ;  # Items whose avg net profi...

  Write stats: received 1360 chars / 1360 bytes; tail: …'_name desc,\\n    worst_performer_product_name desc\\nlimit 100;'.
  ```
- `trilogy run --import raw.store_sales:ss select ss.date.year, ss.item.text_id, ss.customer.text_id, sum(ss.quantity) as store_qty where ss.date.year = 2000 an…e.year = 2000 and ws.billing_customer.id is not null and ws.is_returned != true) group by ss.date.year, ss.item.text_id, ss.customer.text_id limit 5;`

  ```text
  Syntax error in stdin: Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
   ws.billing_customer.text_id) ??? from raw.web_sales ws where ws...
  ```
- `trilogy run --import raw.customer:c select c.text_id where c.text_id in (select ss.customer.text_id from raw.store_sales ss where ss.date.year=2000 and ss.cu…s.billing_customer.text_id from raw.web_sales ws where ws.date.year=2000 and ws.billing_customer.id is not null and ws.is_returned != true) limit 10;`

  ```text
  Syntax error in stdin: Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...n (select ss.customer.text_id ??? from raw.store_sales ss where
  ```
- `trilogy run --import raw.store_sales:ss select ss.customer.text_id where ss.date.year=2000 and ss.customer.id is not null and ss.is_returned != true and ss.c…s.billing_customer.text_id from raw.web_sales ws where ws.date.year=2000 and ws.billing_customer.id is not null and ws.is_returned != true) limit 10;`

  ```text
  Syntax error in stdin: Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...t ws.billing_customer.text_id ??? from raw.web_sales ws where ws...
  ```
- `trilogy run --import raw.store_sales:ss --import raw.web_sales:ws select ss.customer.text_id where ss.date.year=2000 and ss.customer.id is not null and ss.is…customer.text_id in (ws.billing_customer.text_id where ws.date.year=2000 and ws.billing_customer.id is not null and ws.is_returned != true) limit 10;`

  ```text
  Syntax error in stdin:  --> 3:166
    |
  3 | select ss.customer.text_id where ss.date.year=2000 and ss.customer.id is not null and ss.is_returned != true and ss.customer.text_id in (ws.billing_customer.text_id where ws.date.year=2000 and ws.billing_customer.id is not null and ws.is_returned != true) limit 10;
    |                                                                                                                                                                      ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
   (ws.billing_customer.text_id ??? where ws.date.year=2000 and ws...
  ```
- `trilogy file write query80.preql --content import raw/store_sales as ss;
import raw/catalog_sales as cs;
import raw/web_sales as ws;

with combined as union(…,
    combined.sales,
    combined.returns,
    combined.profit
order by combined.channel asc nulls first, combined.outlet asc nulls first
limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/store_sales as ss;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/store_sales as ss; import

  Write stats: received 1808 chars / 1808 bytes; tail: …' asc nulls first, combined.outlet asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query01.preql --content import raw.store_returns as sr;

auto cust_store_returns <- sum(sr.return_amount) by sr.billing_customer.id, sr.st…d
    --cust_store_returns,
    --store_avg_return
having
    cust_store_returns > 1.2 * store_avg_return
order by
    billing_customer_id
limit 100;`

  ```text
  refused to write 'query01.preql': not syntactically valid Trilogy.

  Parse error:
    --> 11:5
     |
  11 |     --cust_store_returns,
     |     ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...id as billing_customer_id     ??? --cust_store_returns,     --st...

  Write stats: received 464 chars / 464 bytes; tail: …'store_avg_return\\norder by\\n    billing_customer_id\\nlimit 100;'.
  ```
- `trilogy file write query04.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Store: annual value p…f_flag,
union join store_annual.cust_id = catalog_annual.cust_id = web_annual.cust_id
union join store_annual.yr = catalog_annual.yr = web_annual.yr
`

  ```text
  refused to write 'query04.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [202]: Missing closing semicolon? Statements must be terminated with a semicolon `;`.
  Location:
  ...log_annual.yr = web_annual.yr ???

  Write stats: received 1360 chars / 1360 bytes; tail: …'on join store_annual.yr = catalog_annual.yr = web_annual.yr\\n'.
  ```
- `trilogy file write query13.preql --content import raw.store_sales as store_sales;

# Year 2001
where store_sales.date.year = 2001

# Demographic-and-price cl…ext_sales_price,
  avg(store_sales.ext_wholesale_cost) as avg_ext_wholesale_cost,
  sum(store_sales.ext_wholesale_cost) as total_ext_wholesale_cost
;`

  ```text
  refused to write 'query13.preql': not syntactically valid Trilogy.

  Parse error:
   --> 8:1
    |
  8 | where (
    | ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
  ...+ state/net_profit conditions ??? where (   # Combination 1: mar...

  Write stats: received 1945 chars / 1945 bytes; tail: …'tore_sales.ext_wholesale_cost) as total_ext_wholesale_cost\\n;'.
  ```
- `trilogy file write query16.preql --content import raw.catalog_sales as cs;

# Orders that ship from more than one warehouse
auto multi_warehouse_orders <- cs…orders,
    sum(cs.ext_ship_cost) as total_extended_ship_cost,
    sum(cs.net_profit) as total_net_profit
order by
    unique_orders desc
limit 100;
`

  ```text
  refused to write 'query16.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:41
    |
  7 | auto returned_orders <- cs.order_number where cs.order_number in raw.catalog_returns.order_number;
    |                                         ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...ned_orders <- cs.order_number ??? where cs.order_number in raw.c...

  Write stats: received 785 chars / 785 bytes; tail: …'total_net_profit\\norder by\\n    unique_orders desc\\nlimit 100;\\n'.
  ```
- `trilogy file write query21.preql --content import raw/inventory as inv;

# Before window: 2000-02-10 <= date < 2000-03-11
auto inv_before <- sum(inv.quantity…fore >= 2.0/3.0
  and inv_after / inv_before <= 3.0/2.0
order by
  inv.warehouse.name asc nulls first,
  inv.item.text_id asc nulls first
limit 100;
`

  ```text
  refused to write 'query21.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/inventory as inv;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/inventory as inv;  # Befor...

  Write stats: received 864 chars / 864 bytes; tail: …' nulls first,\\n  inv.item.text_id asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query26.preql --content import raw/catalog_sales as cs;

where cs.sold_date.year = 2000
  and cs.bill_customer_demographic.gender = 'M'
  …rice) as avg_list_price,
    avg(cs.coupon_amt) as avg_coupon_amt,
    avg(cs.sales_price) as avg_sales_price
order by
    cs.item.text_id
limit 100;`

  ```text
  refused to write 'query26.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/catalog_sales as cs;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/catalog_sales as cs;  wher...

  Write stats: received 549 chars / 549 bytes; tail: …') as avg_sales_price\\norder by\\n    cs.item.text_id\\nlimit 100;'.
  ```
- `trilogy file write query28.preql --content import raw.store_sales as store_sales;

# Bucket 1: ticket quantity 0 to 5, and (list price 8-18 OR coupon 459-145…list_price_5,
    bucket5.unique_list_price_5,
    bucket6.avg_list_price_6,
    bucket6.count_list_price_6,
    bucket6.unique_list_price_6
limit 1;`

  ```text
  refused to write 'query28.preql': not syntactically valid Trilogy.

  Parse error:
    --> 13:1
     |
  13 | by *;
     | ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...price) as unique_list_price_1 ??? by *;  # Bucket 2: ticket quan...

  Write stats: received 3726 chars / 3726 bytes; tail: …'count_list_price_6,\\n    bucket6.unique_list_price_6\\nlimit 1;'.
  ```
- `trilogy file write query38.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Store sales tuples: c…ame
union join ss_tuples.sale_date = ws_tuples.sale_date
where
    cs_tuples.last_name is not null
    and ws_tuples.last_name is not null
limit 100;`

  ```text
  refused to write 'query38.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...aring in all three select     ??? count(*) as combo_count union

  Write stats: received 1545 chars / 1545 bytes; tail: …' not null\\n    and ws_tuples.last_name is not null\\nlimit 100;'.
  ```
- `trilogy run --import raw.web_sales:ws select distinct substring(ws.billing_customer.address.zip, 1, 5) as zip_prefix where substring(ws.billing_customer.address.zip, 1, 5) in ('85669','86197','88274','83405','86475','85392','85460','80348','81792') limit 10;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...t raw.web_sales as ws; select ??? distinct substring(ws.billing_...
  ```
- `trilogy run --import raw/store_sales:store_sales select distinct date.day_of_week order by 1 limit 100;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ..._sales as store_sales; select ??? distinct date.day_of_week orde...
  ```
- `trilogy file write query47.preql --content import raw.store_sales as ss;

# Monthly total of unit price per (category, brand, store, company, year, month)
au…c,
    ss.date.year asc,
    ss.date.month_of_year asc,
    avg_monthly asc,
    monthly_total asc,
    prev_total asc,
    next_total asc
limit 100;`

  ```text
  refused to write 'query47.preql': not syntactically valid Trilogy.

  Parse error:
    --> 36:1
     |
  36 | select
     | ^---
     |
     = expected limit, order_by, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
     and next_total is not null ??? select     ss.item.category,

  Write stats: received 2334 chars / 2334 bytes; tail: …'total asc,\\n    prev_total asc,\\n    next_total asc\\nlimit 100;'.
  ```
- `trilogy file write query51.preql --content import raw.web_sales as ws;
import raw.store_sales as ss;

# Per-item, per-day: sum of sales_price (unit price) fo…rows between unbounded preceding and current row) as store_running_max
order by combined.item asc nulls first, combined.dt asc nulls first
limit 100;`

  ```text
  refused to write 'query51.preql': not syntactically valid Trilogy.

  Parse error:
    --> 38:80
     |
  38 |     sum(coalesce(combined.web_unit_price, 0)) over (partition by combined.item order by combined.dt asc rows between unbounded preceding and current row) as web_running_total,
     |                                                                                ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...r (partition by combined.item ??? order by combined.dt asc rows

  Write stats: received 1984 chars / 1984 bytes; tail: …'item asc nulls first, combined.dt asc nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.store_sales:ss where ss.customer.id = (select ss2.customer.id from raw.store_sales as ss2 where ss2.date.month_seq between 1188 and … and ss2.item.category = 'Women' limit 1) and ss.date.month_seq between 1188 and 1190 select ss.customer.id, sum(ss.ext_sales_price) as tot limit 10;`

  ```text
  Syntax error in stdin: Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ....id = (select ss2.customer.id ??? from raw.store_sales as ss2 wh...
  ```
- `trilogy file write query59.preql --content import raw.store_sales as ss;

# Define a macro to sum sales_price for a given day-of-week (0=Sunday .. 6=Saturday…not null
order by this_yr.store_name asc nulls first,
         this_yr.store_code asc nulls first,
         this_yr.wk_seq asc nulls first
limit 100;`

  ```text
  refused to write 'query59.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:1
    |
  7 | rowset this_yr as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
   data, pivoted by day-of-week ??? rowset this_yr as where ss.dat...

  Write stats: received 2079 chars / 2079 bytes; tail: …'ls first,\\n         this_yr.wk_seq asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query65.preql --content import raw.store_sales as ss;

auto revenue_per_store_item <- sum(ss.sales_price) by ss.store.id, ss.item.id;
auto…aving revenue_per_store_item <= 0.1 * store_avg_revenue
order by ss.store.name, ss.item.desc, ss.store.id, ss.item.id
    with nulls first
limit 100;`

  ```text
  refused to write 'query65.preql': not syntactically valid Trilogy.

  Parse error:
    --> 17:5
     |
  17 |     with nulls first
     |     ^---
     |
     = expected limit, ORDERING_DIRECTION, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ..., ss.store.id, ss.item.id     ??? with nulls first limit 100;

  Write stats: received 534 chars / 534 bytes; tail: …'esc, ss.store.id, ss.item.id\\n    with nulls first\\nlimit 100;'.
  ```
- `trilogy file write query76.preql --content import raw.store_sales as store;
import raw.web_sales as web;
import raw.catalog_sales as catalog;

with combined …tal_ext_sales_price
order by
  combined.channel,
  combined.missing_ref_label,
  combined.yr,
  combined.qtr,
  combined.cat
  nulls first
limit 100;`

  ```text
  refused to write 'query76.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ..._count, total_ext_sales_price) ???  select   combined.channel,

  Write stats: received 1494 chars / 1494 bytes; tail: …'.yr,\\n  combined.qtr,\\n  combined.cat\\n  nulls first\\nlimit 100;'.
  ```
- `trilogy file write query82.preql --content import raw.store_sales as ss;
import raw.inventory as inv;
import raw.item as item;

subset join item.text_id = ss…07-24'::date
select
    item.text_id as item_code,
    item.desc as description,
    item.current_price as price
order by
    item.text_id
limit 100;`

  ```text
  refused to write 'query82.preql': not syntactically valid Trilogy.

  Parse error:
   --> 8:1
    |
  8 | where
    | ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
  ...m.text_id = inv.item.text_id  ??? where     item.current_price b...

  Write stats: received 530 chars / 530 bytes; tail: …'.current_price as price\\norder by\\n    item.text_id\\nlimit 100;'.
  ```

### `cli-misuse`

- `trilogy explore raw/date_dim.preql`

  ```text
  Invalid value for 'PATH': File 'raw/date_dim.preql' does not exist.
  ```
- `trilogy explore ./raw/sales/sales.preql`

  ```text
  Invalid value for 'PATH': File './raw/sales/sales.preql' does not exist.
  ```
- `trilogy explore raw/customer_demographics.preql`

  ```text
  Invalid value for 'PATH': File 'raw/customer_demographics.preql' does not exist.
  ```
- `trilogy explore raw/household_demographics.preql`

  ```text
  Invalid value for 'PATH': File 'raw/household_demographics.preql' does not exist.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw/all_sales:s select s.channel, count(s.order_id) where s.return_date.date between '2000-08-23'::date and '2000-09-06'::date and s.return_channel_dim_text_id is not null by s.channel;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(s.order_id) as order_id_count`
  Location:
   s.channel, count(s.order_id) ??? where s.return_date.date betwe...
  ```
- `trilogy run --import raw.catalog_returns:cr select count(cr.order_number), count(cr.order_number ? cr.order_number is not null) as non_null, count(cr.order_number ? cr.order_number is null) as null_count;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(cr.order_number) as order_number_count`
  Location:
   select count(cr.order_number) ??? , count(cr.order_number ? cr.o...
  ```
- `trilogy run --import raw.all_sales:sales select sales.date.year, sales.date.month_of_year, count(sales.order_id), sum(sales.return_amount) where sales.return_amount > 10000 group by sales.date.year, sales.date.month_of_year order by sales.date.year, sales.date.month_of_year;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(sales.order_id) as order_id_count`
  Location:
  ...of_year, count(sales.order_id) ??? , sum(sales.return_amount) whe...
  ```

### `type-error`

- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Type error in query08.preql: Invalid argument type 'ArrayType<STRING>' passed into SUBSTRING function in position 1 from concept: local.matching_param_zips. Valid: 'STRING'.
  ```
- `trilogy run query38.preql`

  ```text
  Type error in query38.preql: Invalid argument type 'DATE' passed into CONCAT function in position 2 from concept: ss_tuples.ss.date.date. Valid: 'STRING'.
  ```

### `undefined-concept`

- `trilogy explore query64.preql`

  ```text
  Failed to parse query64.preql: (UndefinedConceptException(...), "Ambiguous reference 'yearly_agg.year': matches ['yearly_agg.ss.customer.first_sales_date.year', 'yearly_agg.ss.customer.first_shipto_date.year', 'yearly_agg.ss.date.year']. Qualify the full path to disambiguate.")
  ```
