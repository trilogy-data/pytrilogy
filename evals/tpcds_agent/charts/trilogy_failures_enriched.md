# Trilogy failure analysis — 20260620-034651

- Run `20260620-034651` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 365 | failed: 84 (23%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 54 | 64% |
| `syntax-parse` | 13 | 15% |
| `join-resolution` | 11 | 13% |
| `syntax-missing-alias` | 5 | 6% |
| `cli-misuse` | 1 | 1% |

## Detail

### `other`

- `trilogy run query02.preql duckdb`

  ```text
  Resolution error in query02.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {sales_2001.ws, sales_2002.dow, sales_2002.total, sales_2002.ws}
  ```
- `trilogy run query02.preql duckdb`

  ```text
  Resolution error in query02.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {s2001.total, s2002.ws}
  ```
- `trilogy run query02.preql duckdb`

  ```text
  Syntax error in query02.preql: Undefined concept: agg_2001. Suggestions: ['agg_2001.ws', 'agg_2001.dow', '_agg_2001_ws']
  ```
- `trilogy run query02.preql duckdb`

  ```text
  Syntax error in query02.preql: HAVING references 'agg.yr', which is not in the SELECT projection (line 30). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --agg.yr
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query02.preql duckdb`

  ```text
  Syntax error in query02.preql: Output column 'yr' aliases 'agg.yr', which is itself the 'yr' output of a union(...)/rowset, so the rename refers back to itself. Use a distinct output name (e.g. 'yr_out').
  ```
- `trilogy run query02.preql duckdb`

  ```text
  Syntax error in query02.preql: Undefined concept: s01. Suggestions: ['s01.ws', 's01.dow', '_s01_ws']
  ```
- `trilogy run query02.preql duckdb`

  ```text
  Syntax error in query02.preql: Undefined concept: sun. Suggestions: ['sun.yr', 'sun.ws', '_sun_yr']
  ```
- `trilogy run query02.preql duckdb`

  ```text
  Syntax error in query02.preql: HAVING references 'agg.yr', which is not in the SELECT projection (line 30). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --agg.yr
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query02.preql duckdb`

  ```text
  Resolution error in query02.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {web.date.week_seq, web.date.year}
  ```
- `trilogy run query02.preql duckdb`

  ```text
  Resolution error in query02.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {web.date.week_seq}
  ```
- `trilogy run query02.preql duckdb`

  ```text
  Syntax error in query02.preql: Output column 'ws' aliases 'pivoted.ws', which is itself the 'ws' output of a union(...)/rowset, so the rename refers back to itself. Use a distinct output name (e.g. 'ws_out').
  ```
- `trilogy run query02.preql duckdb`

  ```text
  Syntax error in query02.preql: Output column 'yr' aliases 'pivoted.yr', which is itself the 'yr' output of a union(...)/rowset, so the rename refers back to itself. Use a distinct output name (e.g. 'yr_out').
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 1 column 3055 (char 3054). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales; 
 
# Sales union arm: sales filtered by sale date and non-null channel_dim_id 
…::float as gross_sales, 
    coalesce(sum(all_sales.return_amount), 0) as returns, 
    coalesce(-sum(all_sales.return_net_loss), 0) as net_profit 
;`

  ```text
  trilogy file write: `--content` takes a SINGLE string argument. Your args list put 41 separate tokens after --content (treating it like a shell command). In a tool call, pass the entire file body as one string element after --content, with newlines embedded literally — e.g.
    {"args": ["file", "write", "query70.preql", "--content", "import raw.store_sales as store_sales;\n\nselect ..."]}
  Alternatively use `--escapes` with a single-line `\n`-escaped string.
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: Output column 'channel_name' aliases 'combined.channel_name', which is itself the 'channel_name' output of a union(...)/rowset, so the rename refers back to itself. Use a distinct output name (e.g. 'channel_name_out').
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: Output column 'channel_name' aliases 'combined.channel_name', which is itself the 'channel_name' output of a union(...)/rowset, so the rename refers back to itself. Use a distinct output name (e.g. 'channel_name_out').
  ```
- `trilogy run query14.preql`

  ```text
  Unexpected error in query14.preql: Tuple must have same type for all elements
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: HAVING references 'local.overall_avg_sale', which is not in the SELECT projection (line 11). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.overall_avg_sale
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run --import raw/all_sales:as select count_distinct(asales.channel) as chans where asales.date.year = 2001 and asales.date.month_of_year = 11 limit 5;`

  ```text
  Syntax error in stdin: 3 undefined concept references; fix all before re-running:
    - asales.channel (line 2, in SELECT); did you mean: as.channel, as.channel_dim_id, asales.date.year?
    - asales.date.year (line 2, col 54, in WHERE); did you mean: as.date.year, as.return_date.year, as.billing_customer.first_sales_date.year, as.billing_customer.first_shipto_date.year, as.ship_customer.first_sales_date.year, as.ship_customer.first_shipto_date.year?
    - asales.date.month_of_year (line 2, col 82, in WHERE); did you mean: as.date.month_of_year, as.return_date.month_of_year, as.billing_customer.first_sales_date.month_of_year, as.billing_customer.first_shipto_date.month_of_year, as.ship_customer.first_sales_date.month_of_year, as.ship_customer.first_shipto_date.month_of_year?
  ```
- `trilogy file read query14.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query30.preql`

  ```text
  Syntax error in query30.preql: 11 undefined concept references; fix all before re-running:
    - salutation (line 31, col 3, in ORDER BY); did you mean: web_returns.web_sales.billing_customer.salutation, web_returns.web_sales.ship_customer.salutation, web_returns.web_sales.return_customer.salutation, web_returns.web_sales.return_refund_customer.salutation, web_returns.billing_customer.salutation, web_returns.refunded_customer.salutation?
    - first_name (line 32, col 3, in ORDER BY); did you mean: web_returns.web_sales.billing_customer.first_name, web_returns.web_sales.ship_customer.first_name, web_returns.web_sales.return_customer.first_name, web_returns.web_sales.return_refund_customer.first_name, web_returns.billing_customer.first_name, web_returns.refunded_customer.first_name?
    - last_name (line 33, col 3, in ORDER BY); did you mean: web_returns.web_sales.billing_customer.last_name, web_returns.web_sales.ship_customer.last_name, web_returns.web_sales.return_customer.last_name, web_returns.web_sales.return_refund_customer.last_name, web_returns.billing_customer.last_name, web_returns.refunded_customer.last_name?
    - preferred_cust_flag (line 34, col 3, in ORDER BY); did you mean: web_returns.web_sales.billing_customer.preferred_cust_flag, web_returns.web_sales.ship_customer.preferred_cust_flag, web_returns.web_sales.return_customer.preferred_cust_flag, web_returns.web_sales.return_refund_customer.preferred_cust_flag, web_returns.billing_customer.preferred_cust_flag, web_returns.refunded_customer.preferred_cust_flag?
    - birth_day (line 35, col 3, in ORDER BY); did you mean: web_returns.web_sales.billing_customer.birth_day, web_returns.web_sales.ship_customer.birth_day, web_returns.web_sales.return_customer.birth_day, web_returns.web_sales.return_refund_customer.birth_day, web_returns.billing_customer.birth_day, web_returns.refunded_customer.birth_day?
    - birth_month (line 36, col 3, in ORDER BY); did you mean: web_returns.web_sales.billing_customer.birth_month, web_returns.web_sales.ship_customer.birth_month, web_returns.web_sales.return_customer.birth_month, web_returns.web_sales.return_refund_customer.birth_month, web_returns.billing_customer.birth_month, web_returns.refunded_customer.birth_month?
    - birth_year (line 37, col 3, in ORDER BY); did you mean: web_returns.web_sales.billing_customer.birth_year, web_returns.web_sales.ship_customer.birth_year, web_returns.web_sales.return_customer.birth_year, web_returns.web_sales.return_refund_customer.birth_year, web_returns.billing_customer.birth_year, web_returns.refunded_customer.birth_year?
    - birth_country (line 38, col 3, in ORDER BY); did you mean: web_returns.web_sales.billing_customer.birth_country, web_returns.web_sales.ship_customer.birth_country, web_returns.web_sales.return_customer.birth_country, web_returns.web_sales.return_refund_customer.birth_country, web_returns.billing_customer.birth_country, web_returns.refunded_customer.birth_country?
    - login (line 39, col 3, in ORDER BY); did you mean: web_returns.web_sales.billing_customer.login, web_returns.web_sales.ship_customer.login, web_returns.web_sales.return_customer.login, web_returns.web_sales.return_refund_customer.login, web_returns.billing_customer.login, web_returns.refunded_customer.login?
    - email_address (line 40, col 3, in ORDER BY); did you mean: web_returns.web_sales.billing_customer.email_address, web_returns.web_sales.ship_customer.email_address, web_returns.web_sales.return_customer.email_address, web_returns.web_sales.return_refund_customer.email_address, web_returns.billing_customer.email_address, web_returns.refunded_customer.email_address?
    - last_review_date (line 41, col 3, in ORDER BY); did you mean: web_returns.web_sales.billing_customer.last_review_date, web_returns.web_sales.ship_customer.last_review_date, web_returns.web_sales.return_customer.last_review_date, web_returns.web_sales.return_refund_customer.last_review_date, web_returns.billing_customer.last_review_date, web_returns.refunded_customer.last_review_date?
  ```
- `trilogy run query30.preql`

  ```text
  Syntax error in query30.preql: HAVING references 'local.state_avg_return', which is not in the SELECT projection (line 9). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.state_avg_return
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query44.preql`

  ```text
  Syntax error in query44.preql: HAVING references 'local.threshold_avg', which is not in the SELECT projection (line 13). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.threshold_avg
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query44.preql`

  ```text
  Syntax error in query44.preql: HAVING references 'ps.sale_address.id', 'ps.store.id', '__preql_internal.all_rows', which are not in the SELECT projection (line 6). To filter output rows, add them to SELECT — prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --ps.sale_address.id, --ps.store.id, --__preql_internal.all_rows
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query44.preql`

  ```text
  Syntax error in query44.preql: HAVING references 'local.threshold_null_addr', which is not in the SELECT projection (line 11). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.threshold_null_addr
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query44.preql`

  ```text
  Syntax error in query44.preql: HAVING references 'local.thr', which is not in the SELECT projection (line 9). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.thr
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query44.preql`

  ```text
  Syntax error in query44.preql: 2 undefined concept references; fix all before re-running:
    - qualified_items.product_name (line 20, col 5, in SELECT); did you mean: qualified_items.item.product_name, ps.item.product_name, item.product_name, qualified_items.thr?
    - qualified_items.product_name (line 19, in SELECT); did you mean: qualified_items.item.product_name, ps.item.product_name, item.product_name, qualified_items.thr?
  ```
- `trilogy run query44.preql`

  ```text
  Syntax error in query44.preql: HAVING references 'local.is_qualified', which is not in the SELECT projection (line 15). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.is_qualified
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query44.preql`

  ```text
  Syntax error in query44.preql: HAVING references 'local.thr', which is not in the SELECT projection (line 8). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.thr
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run --import raw.physical_sales:ps --import raw.item:item where ps.store.id = 1 select item.product_name as name, avg(ps.net_profit) as avg_profit order by avg_profit asc limit 5;`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {local.avg_profit, ps.store.id}; {local.name}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query44.preql`

  ```text
  Resolution error in query44.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {best_10.product_name, best_10.rank, worst_10.product_name}
  ```
- `trilogy run --import raw.physical_sales:ps where ps.store.id = 1 select avg(ps.net_profit) as thr where ps.sale_address.id is null;`

  ```text
  Unexpected error in stdin: Multiple where clauses are not supported
  ```
- `trilogy run --import raw.physical_sales:ps select count(ps.item.product_name) as cnt where ps.store.id = 1 having avg(ps.net_profit) > -738.04;`

  ```text
  Syntax error in stdin: HAVING references 'ps.net_profit', which is not in the SELECT projection (line 2). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --ps.net_profit
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run --import raw.physical_sales:ps select ps.item.product_name as name, --avg(ps.net_profit) as avg_profit having avg(ps.net_profit) > -738.04 and ps.store.id = 1;`

  ```text
  Syntax error in stdin: HAVING references 'ps.store.id', which is not in the SELECT projection (line 2). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --ps.store.id
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run --import raw.physical_sales:ps select ps.item.product_name as name, avg(ps.net_profit) as avg_profit where ps.store.id = 1 having avg(ps.net_profit) > 0.9 * avg(ps.net_profit ? ps.sale_address.id is null and ps.store.id = 1) by * order by avg_profit asc limit 10;`

  ```text
  Syntax error in stdin: HAVING references 'ps.sale_address.id', 'ps.store.id', '__preql_internal.all_rows', which are not in the SELECT projection (line 2). To filter output rows, add them to SELECT — prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --ps.sale_address.id, --ps.store.id, --__preql_internal.all_rows
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy file read query44.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {cs.item.color, cs.item.current_price, cs.item.id, local.cr_item_refund, local.cs_item_price}
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Undefined concept: y2.item_id. Suggestions: ['sale_agg.item_id', 'ss.item.id', 'pr.item.id', 'cr.item.id']
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Undefined concept: y1999.item_id. Suggestions: ['y1999.sale_agg.item_id', 'sale_agg.item_id', 'y2000.sale_agg.item_id', 'y1999.store_zip', 'y1999.sale_zip']
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: 2 undefined concept references; fix all before re-running:
    - y1999.item_id (line 13, col 8, in SELECT); did you mean: y1999.sale_agg.item_id, sale_agg.item_id, y1999.store_name, ss.item.id?
    - y1999.store_name (line 13, col 23, in SELECT); did you mean: y1999.sale_agg.store_name, sale_agg.store_name, ss.store.name?
  ```
- `trilogy file read raw/physical_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query78.preql`

  ```text
  Resolution error in query78.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {ca.ca_qty, ca.ca_sp, ca.ca_w, st.cust, st.item, st.st_qty, st.st_sp, st.st_w, st.yr, we.we_qty, we.we_sp, we.we_w}
  ```
- `trilogy run query78.preql`

  ```text
  Syntax error in query78.preql: Undefined concept: store.customer. Suggestions: ['store.customer.demographics.id', 'store.customer.demographics.gender', 'store.customer.demographics.marital_status', 'store.customer.demographics.education_status', 'store.customer.demographics.purchase_estimate', 'store.customer.demographics.credit_rating']
  ```
- `trilogy run query78.preql`

  ```text
  Resolution error in query78.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {ca.ca_qty, ca.ca_sp, ca.ca_w, st.cust, st.item, st.st_qty, st.st_sp, st.st_w, st.yr, we.we_qty, we.we_sp, we.we_w}
  ```
- `trilogy run query78.preql`

  ```text
  Syntax error in query78.preql: Undefined concept: other_agg.yr. Suggestions: ['other_agg.other_ch.yr', 'st.yr', 'yr', 'other_ch.yr', 'other_agg.o_qty', 'other_agg.o_wh']
  ```
- `trilogy run query78.preql`

  ```text
  Resolution error in query78.preql: (AmbiguousRelationshipResolutionException(...), "Multiple possible concept additions (intermediate join keys) found to resolve ['catalog.bill_customer.id', 'catalog.ext_sales_price', 'catalog.order_number', 'catalog.quantity', 'catalog.return_amount', 'catalog.wholesale_cost', 'web.date.year', 'web.item.id'], have {'web.date.id'} or {'catalog.date.id'} or {'store.date.id'}. Different paths are is: [{'web.date.id'}, {'catalog.date.id'}, set()]")
  ```
- `trilogy run query78.preql`

  ```text
  Resolution error in query78.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {ca.ca_qty, ca.ca_sp, ca.ca_w, st.item, st.st_qty, st.st_sp, st.st_w, st.yr, we.we_qty, we.we_sp, we.we_w}
  ```
- `trilogy run query78.preql`

  ```text
  Resolution error in query78.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {ca.ca_qty, ca.ca_sp, ca.ca_w, st.cust, st.item, st.st_qty, st.st_sp, st.st_w, st.yr, we.we_qty, we.we_sp, we.we_w}
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 21 column 12 (char 1142). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query83.preql`

  ```text
  Syntax error in query83.preql: HAVING references 'sales.order_id', which is not in the SELECT projection (line 3). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --sales.order_id
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query83.preql`

  ```text
  Syntax error in query83.preql: Output column 'store_return_qty' aliases 'local.store_return_qty', which is itself the 'store_return_qty' output of a union(...)/rowset, so the rename refers back to itself. Use a distinct output name (e.g. 'store_return_qty_out').
  ```
- `trilogy file read query83.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy file write query02.preql --content import raw.web_sales as web;
import raw.catalog_sales as catalog;

# First check: what week_seq values exist in 20…select
    web.date.week_seq as ws,
    web.date.year as yr
where web.date.year = 2001
group by web.date.week_seq, web.date.year
order by ws
limit 5;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...yr where web.date.year = 2001 ??? group by web.date.week_seq, we...

  Write stats: received 267 chars / 267 bytes; tail: …'oup by web.date.week_seq, web.date.year\\norder by ws\\nlimit 5;'.
  ```
- `trilogy file write query02.preql --content import raw.web_sales as web;
import raw.catalog_sales as catalog;

# Combined sales per (week_seq, day_of_week) fr…um(s2001.total ? s2001.dow = 6) / sum(s2002.total ? s2002.dow = 6), 2) as Saturday
inner join s2001.ws = s2002.ws - 53
order by week_seq nulls first;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
    --> 56:32
     |
  56 | inner join s2001.ws = s2002.ws - 53
     |                                ^---
     |
     = expected limit, order_by, where, having, or JOIN_TYPE
  Location:
  ...nner join s2001.ws = s2002.ws ??? - 53 order by week_seq nulls f...

  Write stats: received 1728 chars / 1728 bytes; tail: …'join s2001.ws = s2002.ws - 53\\norder by week_seq nulls first;'.
  ```
- `trilogy file write query02.preql --content import raw.web_sales as web;
import raw.catalog_sales as catalog;

# Combined web + catalog sales by (week_seq, da…? agg_2001.dow = 6) / sum(agg_2002.total ? agg_2002.dow = 6), 2) as Saturday
inner join agg_2001.ws = agg_2002.ws - 53
order by week_seq nulls first;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
    --> 60:38
     |
  60 | inner join agg_2001.ws = agg_2002.ws - 53
     |                                      ^---
     |
     = expected limit, order_by, where, having, or JOIN_TYPE
  Location:
  ...oin agg_2001.ws = agg_2002.ws ??? - 53 order by week_seq nulls f...

  Write stats: received 2046 chars / 2046 bytes; tail: …'gg_2001.ws = agg_2002.ws - 53\\norder by week_seq nulls first;'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

# Sales union arm: sales filtered by sale date and non-null channel_dim_id
wit…  0::float as gross_sales,
    coalesce(sum(all_sales.return_amount), 0) as returns,
    coalesce(-sum(all_sales.return_net_loss), 0) as net_profit
;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
    --> 40:14
     |
  40 |     coalesce(-sum(all_sales.return_net_loss), 0) as net_profit
     |              ^---
     |
     = expected access_chain
  Location:
  ..., 0) as returns,     coalesce( ??? -sum(all_sales.return_net_loss...

  Write stats: received 1904 chars / 1904 bytes; tail: …'coalesce(-sum(all_sales.return_net_loss), 0) as net_profit\\n;'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

# Sales union arm: sales filtered by sale date and non-null channel_dim_id
wit…   @rollup_sum(combined.net_profit) as net_profit
order by
    combined.channel_name asc nulls last,
    combined.entity_id asc nulls last
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
    --> 50:52
     |
  50 | def rollup_sum(metric) -> coalesce(sum(metric), 0) by rollup combined.channel_name, combined.entity_id;
     |                                                    ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...) -> coalesce(sum(metric), 0) ??? by rollup combined.channel_nam...

  Write stats: received 2766 chars / 2766 bytes; tail: …'nulls last,\\n    combined.entity_id asc nulls last\\nlimit 100;'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

# Sales union arm: sales filtered by sale date and non-null channel_dim_id
wit…   @rollup_sum(combined.net_profit) as net_profit
order by
    combined.channel_name asc nulls last,
    combined.entity_id asc nulls last
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
    --> 49:52
     |
  49 | def rollup_sum(metric) -> coalesce(sum(metric), 0) by rollup combined.channel_name, combined.entity_id;
     |                                                    ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...) -> coalesce(sum(metric), 0) ??? by rollup combined.channel_nam...

  Write stats: received 2721 chars / 2721 bytes; tail: …'nulls last,\\n    combined.entity_id asc nulls last\\nlimit 100;'.
  ```
- `trilogy run --import raw/all_sales:as select as.channel, sum(as.quantity * as.list_price) as total_sales, count(as.order_id) as num_sales where as.date.year = 2001 and as.date.month_of_year = 11 group by as.channel;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...nd as.date.month_of_year = 11 ??? group by as.channel;
  ```
- `trilogy file write query44.preql --content import raw.physical_sales as ps;
import raw.item as item;

# Global threshold: avg net profit at store 1 where sal…_rank,
    rank(qualified_items.product_name) over (order by qualified_items.item_avg_profit desc) as worst_rank
order by
    best_rank asc
limit 10;`

  ```text
  refused to write 'query44.preql': not syntactically valid Trilogy.

  Parse error:
   --> 6:5
    |
  6 |     where ps.store.id = 1 and ps.sale_address.id is null;
    |     ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or window_sql_over
  Location:
  ...- avg(ps.net_profit) by *     ??? where ps.store.id = 1 and ps.s...

  Write stats: received 829 chars / 829 bytes; tail: …'fit desc) as worst_rank\\norder by\\n    best_rank asc\\nlimit 10;'.
  ```
- `trilogy file write query64.preql -e -c import raw.physical_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.physical…,'floral','medium')
  and ss.item.current_price between 65 and 74
  and item_cat_ext_list_price > 2 * item_cat_refund
select ss.item.id
;

limit 10;
`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
    --> 26:1
     |
  26 | limit 10;
     | ^---
     |
     = expected EOI, block, or show_statement
  Location:
  ...t_refund select ss.item.id ;  ??? limit 10;

  Write stats: received 1166 chars / 1166 bytes; tail: …'_price > 2 * item_cat_refund\\nselect ss.item.id\\n;\\n\\nlimit 10;\\n'.
  ```
- `trilogy file write query64.preql -e -c import raw.physical_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.physical…e_cnt,
  sum(ss.ext_wholesale_cost) as ws_cost,
  sum(ss.ext_list_price) as list_price_sum,
  sum(ss.coupon_amt) as coupon_sum
;

select *
limit 20;
`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
    --> 57:8
     |
  57 | select *
     |        ^---
     |
     = expected select_item
  Location:
  ..._amt) as coupon_sum ;  select ??? * limit 20;

  Write stats: received 2320 chars / 2320 bytes; tail: …'m,\\n  sum(ss.coupon_amt) as coupon_sum\\n;\\n\\nselect *\\nlimit 20;\\n'.
  ```
- `trilogy file write query64.preql -e -c import raw.catalog_returns as cr;
import raw.physical_sales as ss;
import raw.physical_returns as pr;

# Per-item aggr… list_price_sum,
  sum(ss.coupon_amt) as coupon_sum
;

rowset y1999 <- where sale_agg.sale_year = 1999 select *;

select y1999.product_name limit 5;
`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
    --> 45:56
     |
  45 | rowset y1999 <- where sale_agg.sale_year = 1999 select *;
     |                                                        ^---
     |
     = expected select_item
  Location:
  ...e_agg.sale_year = 1999 select ??? *;  select y1999.product_name

  Write stats: received 1850 chars / 1850 bytes; tail: …'e_year = 1999 select *;\\n\\nselect y1999.product_name limit 5;\\n'.
  ```
- `trilogy file write query78.preql --content import raw.physical_sales as store;
import raw.catalog_sales as catalog;
import raw.web_sales as web;

merge store…b.ext_sales_price),0)+coalesce(sum(catalog.ext_sales_price),0)) asc, round(store_qty/nullif(coalesce(web_qty,0)+coalesce(cat_qty,0),0),2)
limit 100;
`

  ```text
  refused to write 'query78.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [220]: Filter or stray clause after a `join`? A query-scoped join `inner|left|full join <a> = <b>` takes only key equalities — to join on multiple keys, STACK another `join` clause (or chain `= c`), never `and`. Joins go right after the `select` list (preferred, SQL-like) or before `select`; the order is `where` -> `select` <cols> -> join(s) -> `having` -> `order by` -> `limit`. Filter input rows in `where` (before `select`); filter a joined or aggregated RESULT in `having` (select the field, hide it with a leading `--`). Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...ore.item.id = catalog.item.id ??? left join store.date.year = ca...

  Write stats: received 1644 chars / 1644 bytes; tail: …'if(coalesce(web_qty,0)+coalesce(cat_qty,0),0),2)\\nlimit 100;\\n'.
  ```
- `trilogy file write query97.preql --content import raw.physical_sales as store;
import raw.catalog_sales as catalog;

# Store sales: unique (customer, item) c…then 1 else 0 end) as in_both
full join store_pairs.cust_id = catalog_pairs.cust_id
full join store_pairs.item_id = catalog_pairs.item_id
limit 100;
`

  ```text
  refused to write 'query97.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block — they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...00 and customer is identified ??? rowset store_pairs as where ye...

  Write stats: received 1198 chars / 1198 bytes; tail: …'join store_pairs.item_id = catalog_pairs.item_id\\nlimit 100;\\n'.
  ```

### `join-resolution`

- `trilogy run query44.preql`

  ```text
  Resolution error in query44.preql: Could not resolve connections for query with output ['item.product_name<Purpose.PROPERTY>Derivation.ROOT>', 'local.item_avg_profit<Purpose.METRIC>Derivation.AGGREGATE>', 'local.best_rank<Purpose.PROPERTY>Derivation.WINDOW>', 'local.worst_rank<Purpose.PROPERTY>Derivation.WINDOW>', 'local.threshold_avg<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run query44.preql`

  ```text
  Resolution error in query44.preql: Could not resolve connections for query with output ['item.product_name<Purpose.PROPERTY>Derivation.ROOT>', 'local.item_avg_profit<Purpose.METRIC>Derivation.AGGREGATE>', 'local.best_rank<Purpose.PROPERTY>Derivation.WINDOW>', 'local.worst_rank<Purpose.PROPERTY>Derivation.WINDOW>', 'local.threshold_null_addr<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run query44.preql`

  ```text
  Resolution error in query44.preql: Could not resolve connections for query with output ['item.product_name<Purpose.PROPERTY>Derivation.ROOT>', 'local.item_avg_profit<Purpose.METRIC>Derivation.AGGREGATE>', 'local.best_rank<Purpose.PROPERTY>Derivation.WINDOW>', 'local.worst_rank<Purpose.PROPERTY>Derivation.WINDOW>'] from current model.
  ```
- `trilogy run query44.preql`

  ```text
  Resolution error in query44.preql: Could not resolve connections for query with output ['item.product_name<Purpose.PROPERTY>Derivation.ROOT>', 'local._qualified_items_item_avg_profit<Purpose.METRIC>Derivation.AGGREGATE>', 'local.thr<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run query44.preql`

  ```text
  Resolution error in query44.preql: Could not resolve connections for query with output ['item.product_name<Purpose.PROPERTY>Derivation.ROOT>', 'local.item_avg_profit<Purpose.METRIC>Derivation.AGGREGATE>', 'local.best_rank<Purpose.PROPERTY>Derivation.WINDOW>', 'local.worst_rank<Purpose.PROPERTY>Derivation.WINDOW>', 'local.is_qualified<Purpose.PROPERTY>Derivation.BASIC>'] from current model.
  ```
- `trilogy run query44.preql`

  ```text
  Resolution error in query44.preql: Could not resolve connections for query with output ['item.product_name<Purpose.PROPERTY>Derivation.ROOT>', 'local.item_avg_profit<Purpose.METRIC>Derivation.AGGREGATE>', 'local.best_rank<Purpose.PROPERTY>Derivation.WINDOW>', 'local.worst_rank<Purpose.PROPERTY>Derivation.WINDOW>'] from current model.
  ```
- `trilogy run query44.preql`

  ```text
  Resolution error in query44.preql: Could not resolve connections for query with output ['item.product_name<Purpose.PROPERTY>Derivation.ROOT>', 'local.item_avg_profit<Purpose.METRIC>Derivation.AGGREGATE>', 'local.best_rank<Purpose.PROPERTY>Derivation.WINDOW>', 'local.worst_rank<Purpose.PROPERTY>Derivation.WINDOW>', 'local.thr<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run query44.preql`

  ```text
  Resolution error in query44.preql: Could not resolve connections for query with output ['item.product_name<Purpose.PROPERTY>Derivation.ROOT>', 'local.item_avg_profit<Purpose.METRIC>Derivation.AGGREGATE>', 'local.best_rank<Purpose.PROPERTY>Derivation.WINDOW>', 'local.worst_rank<Purpose.PROPERTY>Derivation.WINDOW>'] from current model.
  ```
- `trilogy run --import raw.physical_sales:ps --import raw.item:item where ps.store.id = 1 select item.product_name as name, avg(ps.net_profit) as avg_profit, rank(item.product_name) over (order by avg(ps.net_profit) asc) as rnk order by rnk asc limit 5;`

  ```text
  Resolution error in stdin: Could not resolve connections for query with output ['local.name<Purpose.PROPERTY>Derivation.BASIC>', 'local.avg_profit<Purpose.METRIC>Derivation.AGGREGATE>', 'local.rnk<Purpose.PROPERTY>Derivation.WINDOW>'] from current model.
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Could not resolve connections for query with output ['local._catalog_matched_item_id<Purpose.KEY>Derivation.BASIC>', 'local._catalog_matched_cat_list_price<Purpose.PROPERTY>Derivation.BASIC>', 'local._catalog_matched_cat_refund<Purpose.PROPERTY>Derivation.BASIC>'] from current model.
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Could not resolve connections for query with output ['local._matched_catalog_item_id<Purpose.KEY>Derivation.BASIC>', 'local._matched_catalog_list_price<Purpose.PROPERTY>Derivation.BASIC>', 'local._matched_catalog_refund<Purpose.PROPERTY>Derivation.BASIC>'] from current model.
  ```

### `syntax-missing-alias`

- `trilogy file write query02.preql --content import raw.web_sales as web;
import raw.catalog_sales as catalog;

# Find all distinct week_seq values in 2001
select
    distinct web.date.week_seq as ws
where web.date.year = 2001
order by ws;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `distinct web.date.week_seq as distinct_web_date_week_seq`
  Location:
  ...s in 2001 select     distinct ??? web.date.week_seq as ws where

  Write stats: received 194 chars / 194 bytes; tail: …'.date.week_seq as ws\\nwhere web.date.year = 2001\\norder by ws;'.
  ```
- `trilogy run --import raw.all_sales:all_sales select distinct all_sales.channel, all_sales.channel_dim_text_id limit 10;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `distinct all_sales.channel as distinct_all_sales_channel`
  Location:
  ...as all_sales; select distinct ??? all_sales.channel, all_sales.c...
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

# Sales union arm: sales filtered by sale date and non-null channel_dim_id
wit…ed.channel_name, combined.entity_id as net_profit
order by
    combined.channel_name asc nulls last,
    combined.entity_id asc nulls last
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `coalesce(sum(combined.gross_sales), 0) by rollup combined.channel_name as coalesce_sum_combined_gross_sales_0_by_r`
  Location:
  ...sum(combined.gross_sales), 0) ??? by rollup combined.channel_nam...

  Write stats: received 2787 chars / 2787 bytes; tail: …'nulls last,\\n    combined.entity_id asc nulls last\\nlimit 100;'.
  ```
- `trilogy run --import raw.physical_sales:ps select ps.store.id, count(ps.ticket_number) limit 5;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(ps.ticket_number) as ticket_number_count`
  Location:
  ...e.id, count(ps.ticket_number) ??? limit 5;
  ```
- `trilogy file write query80.preql --content import raw.all_sales as s;

# Filter conditions
where s.date.date between '2000-08-23'::date and '2000-09-22'::dat…net_loss, 0)) as profit,
    by rollup channel_label, outlet_identifier

order by channel_label nulls first, outlet_identifier nulls first
limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `by rollup channel_label as by_rollup_channel_label`
  Location:
  ...t_loss, 0)) as profit,     by ??? rollup channel_label, outlet_i...

  Write stats: received 941 chars / 941 bytes; tail: …'_label nulls first, outlet_identifier nulls first\\nlimit 100;'.
  ```

### `cli-misuse`

- `trilogy explore raw/store_sales.preql --show all`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
