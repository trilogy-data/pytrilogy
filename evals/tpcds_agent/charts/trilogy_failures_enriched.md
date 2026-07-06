# Trilogy failure analysis — 20260706-115356

- Run `20260706-115356` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 1111 | failed: 87 (8%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 50 | 57% |
| `syntax-parse` | 28 | 32% |
| `cli-misuse` | 6 | 7% |
| `syntax-missing-alias` | 3 | 3% |

## Detail

### `other`

- `trilogy run --import raw/all_sales:all select billing_customer.text_id, all.channel, all.date.year limit 5;`

  ```text
  Syntax error in stdin: Undefined concept: billing_customer.text_id (line 2, col 8, in SELECT). Suggestions: ['all.billing_customer.address.text_id', 'all.billing_customer.first_sales_date.text_id', 'all.billing_customer.first_shipto_date.text_id', 'all.billing_customer.text_id', 'all.item.text_id', 'all.date.text_id']
  ```
- `trilogy run query04.preql`

  ```text
  Syntax error in query04.preql: 3 undefined concept references; fix all before re-running:
    - first_name (line 31, col 39, in ORDER BY); did you mean: all.billing_customer.first_name, all.ship_customer.first_name, all.purchasing_customer.first_name?
    - last_name (line 31, col 67, in ORDER BY); did you mean: all.billing_customer.last_name, all.ship_customer.last_name, all.purchasing_customer.last_name?
    - preferred_cust_flag (line 31, col 94, in ORDER BY); did you mean: all.billing_customer.preferred_cust_flag, all.ship_customer.preferred_cust_flag, all.purchasing_customer.preferred_cust_flag?
  ```
- `trilogy file read query04.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query23.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query44.preql`

  ```text
  Syntax error in query44.preql: Undefined concept: best.rnk. Suggestions: ['best_performers.rnk', 'worst_performers.rnk', 'worst.rnk']
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
- `trilogy file read query93.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Resolution error in query08.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 17). The requested concepts split into 2 disconnected subgraphs: {total_net_profit, ss.date.quarter, ss.date.year}; {st.name, st.zip}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read query10.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query29.preql`

  ```text
  Syntax error in query29.preql: Comparison `sr.return_date.month_of_year <= 12` matches every value of enum field 'sr.return_date.month_of_year', which contains only these values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12. It is always true and should be removed.
  ```
- `trilogy file write -c import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.store_sales as ss;
import raw.item as item;

# Item-level:…ext_list > 2 * cum_refund
;

select qualifying_items.item_text_id, qualifying_items.cum_ext_list, qualifying_items.cum_refund
limit 10; query64.preql`

  ```text
  trilogy file write: `--content` takes a SINGLE string argument. Your args list put 2 separate tokens after --content (treating it like a shell command). In a tool call, pass the entire file body as one string element after --content, with newlines embedded literally — e.g.
    {"args": ["file", "write", "query70.preql", "--content", "import raw.store_sales as store_sales;\n\nselect ..."]}
  Alternatively use `--escapes` with a single-line `\n`-escaped string.
  ```
- `trilogy run query64.preql duckdb`

  ```text
  Syntax error in query64.preql: Undefined concept: local.cum_ext_list. Suggestions: ['item_cs_extlist.cum_ext_list', 'cs.ext_list_price']
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
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
- `trilogy run query77.preql duckdb`

  ```text
  Syntax error in query77.preql: Undefined concept: s.channel_label (line 12, in SELECT). Suggestions: ['channel_label', 's.channel', 's.channel_dim_id']
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

- `trilogy run --import raw/all_sales:all select all.channel, all.date.year, sum(all.ext_list_price) as total_list_price, sum(all.ext_wholesale_cost) as total_w…_price, count(all.item.id) as line_count where all.date.year in (2001, 2002) group by all.channel, all.date.year order by all.channel, all.date.year;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...all.date.year in (2001, 2002) ??? group by all.channel, all.date...
  ```
- `trilogy run --import raw/all_sales:sales select sales.channel, count(sales.channel_dim_id) as cnt by sales.channel;`

  ```text
  Syntax error in stdin:  --> 2:58
    |
  2 | select sales.channel, count(sales.channel_dim_id) as cnt by sales.channel;
    |                                                          ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...(sales.channel_dim_id) as cnt ??? by sales.channel;
  ```
- `trilogy file write query05.preql --content import raw/all_sales as sales;

# Sales aggregation: group by channel, sale entity
with sales_agg as
where sales.d…y_id = returns_agg.entity_id
by rollup (channel_label, entity_label)
order by channel_label asc nulls first, entity_label asc nulls first
limit 100;
`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/all_sales as sales;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/all_sales as sales;  # Sal...

  Write stats: received 2525 chars / 2525 bytes; tail: …'el asc nulls first, entity_label asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw/all_sales:s select s.channel, s.date.year, sum(s.quantity * s.list_price) as total_sales, count(*) as cnt where s.date.year between 1999 and 2001 and s.date.month_of_year = 11 group by 1,2 limit 5;`

  ```text
  Syntax error in stdin: Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...s.list_price) as total_sales, ??? count(*) as cnt where s.date.y...
  ```
- `trilogy file write query14.preql --content import raw.all_sales as s;

# Step 1: Find (brand, class, category) combos appearing in all 3 channels during 1999…_id, s.item.category_id)
order by channel asc nulls first, brand_id asc nulls first, class_id asc nulls first, category_id asc nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...l_sales > (select overall_avg ??? from overall_stats) by rollup

  Write stats: received 1412 chars / 1412 bytes; tail: …'s_id asc nulls first, category_id asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Step 1: Frequent item…) as total_sales
order by
    combined.last_name asc nulls first,
    combined.first_name asc nulls first,
    total_sales asc nulls first
limit 100;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [213]: A `by <grain>` clause must follow an aggregate, but the expression before it has none. To take each distinct value once per grain, wrap it in `group(...)` - e.g. `group(item.current_price) by item.id, item.category`. For a reduction, use an aggregate: `sum(x) by ...`, `avg(x) by ...`, `max(x) by ...`.
  Location:
  ...ustomer_totals.lifetime_total ??? by *);  rowset best_customers

  Write stats: received 2358 chars / 2358 bytes; tail: …' asc nulls first,\\n    total_sales asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query38.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Distinct (last_name, …tuples.first_name = cs_tuples.first_name = ws_tuples.first_name
union join ss_tuples.sale_date = cs_tuples.sale_date = ws_tuples.sale_date
limit 100;`

  ```text
  refused to write 'query38.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...ence in all three. select     ??? count(*) as combo_count union

  Write stats: received 1442 chars / 1442 bytes; tail: …'_date = cs_tuples.sale_date = ws_tuples.sale_date\\nlimit 100;'.
  ```
- `trilogy run --import raw.store_sales:ss select ss.item.product_name, avg(ss.net_profit) as avg_profit where ss.store.id = 1 group by ss.item.product_name order by avg_profit desc limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ..._profit where ss.store.id = 1 ??? group by ss.item.product_name
  ```
- `trilogy run --import raw.store_sales:ss select ss.item.product_name, avg(ss.net_profit) as avg_profit where ss.store.id = 1 group by ss.item.product_name order by avg_profit asc limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ..._profit where ss.store.id = 1 ??? group by ss.item.product_name
  ```
- `trilogy file write query76.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sales as cs;

with combined as
  unio…issing_ref asc nulls first,
    combined.year asc nulls first,
    combined.quarter asc nulls first,
    combined.category asc nulls first
limit 100;`

  ```text
  refused to write 'query76.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...t,     total_sales numeric   ) ???  select     combined.channel,...

  Write stats: received 1634 chars / 1634 bytes; tail: …'ulls first,\\n    combined.category asc nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw/store_sales:sales select distinct sales.customer.text_id limit 10;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ....store_sales as sales; select ??? distinct sales.customer.text_i...
  ```
- `trilogy run --import raw/store_sales:sales select count(sales.customer.id) as cnt1 where sales.return_reason.desc = 'reason 28' and sales.customer.id is not null, count(sales.customer.id) as cnt2 where sales.return_reason.desc = 'reason 28' and sales.customer.id is null;`

  ```text
  Syntax error in stdin:  --> 2:119
    |
  2 | select count(sales.customer.id) as cnt1 where sales.return_reason.desc = 'reason 28' and sales.customer.id is not null, count(sales.customer.id) as cnt2 where sales.return_reason.desc = 'reason 28' and sales.customer.id is null;
    |                                                                                                                       ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
   sales.customer.id is not null ??? , count(sales.customer.id) as
  ```
- `trilogy file write query02.preql --content import raw.web_sales as web;
import raw.catalog_sales as cat;

# Combine web and catalog sales per date
with web_b…es / dow_with_next.next_year_sales, 2) as ratio_sun,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?
where
    ...
order by
    dow_with_next.week_seq asc
;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
    --> 51:5
     |
  51 |     ?,
     |     ^---
     |
     = expected select_item, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...r_sales, 2) as ratio_sun,     ??? ?,     ?,     ?,     ?,     ?,...

  Write stats: received 1452 chars / 1452 bytes; tail: …'   ?\\nwhere\\n    ...\\norder by\\n    dow_with_next.week_seq asc\\n;'.
  ```
- `trilogy file write query02.preql --content import raw.web_sales as web;
import raw.catalog_sales as cat;
import raw.date as d;

# Web sales by date
with web_…ratio_fri,
    round(sum(agg.tot ? agg.dow = 6) / sum(agg.fut ? agg.dow = 6), 2) as ratio_sat
where
    agg.ws in wk2001.ws
order by
    agg.ws asc
;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...as ws where     d.year = 2001 ??? group by d.week_seq ;  # Now p...

  Write stats: received 1726 chars / 1726 bytes; tail: …'_sat\\nwhere\\n    agg.ws in wk2001.ws\\norder by\\n    agg.ws asc\\n;'.
  ```
- `trilogy file write query64.preql`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:1
    |
  7 | rowset item_cs_extlist as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...ist_price from catalog_sales
   ??? rowset item_cs_extlist as
   sel...

  Write stats: received 997 chars / 997 bytes; tail: …'g_items.cum_ext_list, qualifying_items.cum_refund\r\\nlimit 10;'.
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

### `cli-misuse`

- `trilogy explore raw/customer_demographics.preql`

  ```text
  Invalid value for 'PATH': File 'raw/customer_demographics.preql' does not exist.
  ```
- `trilogy explore raw/customer_address.preql`

  ```text
  Invalid value for 'PATH': File 'raw/customer_address.preql' does not exist.
  ```
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

- `trilogy run --import raw/store_sales:sales select count(sales.customer.id) where sales.return_reason.desc = 'reason 28' and sales.customer.id is not null, count(sales.customer.id) where sales.return_reason.desc = 'reason 28' and sales.customer.id is null;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(sales.customer.id) as id_count`
  Location:
  ...lect count(sales.customer.id) ??? where sales.return_reason.desc...
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
