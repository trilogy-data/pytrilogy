# Trilogy failure analysis — 20260626-031753

- Run `20260626-031753` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 315 | failed: 55 (17%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 36 | 65% |
| `syntax-parse` | 7 | 13% |
| `undefined-concept` | 5 | 9% |
| `join-resolution` | 3 | 5% |
| `cli-misuse` | 2 | 4% |
| `syntax-missing-alias` | 2 | 4% |

## Detail

### `other`

- `trilogy run query02.preql`

  ```text
  Syntax error in query02.preql: Undefined concept: weekly_data.year. Suggestions: ['weekly_data.s.date.year', 's.date.year', 's.return_date.year', 's.billing_customer.first_sales_date.year', 's.billing_customer.first_shipto_date.year', 's.ship_customer.first_sales_date.year']
  ```
- `trilogy run query02.preql`

  ```text
  Resolution error in query02.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 8 disconnected subgraphs: {d2001.weekly_data.s.date.week_seq, d2002.sales_2002}; {local._virt_filter_sales_2002_3048615491469917}; {local._virt_filter_sales_2002_313225635007784}; {local._virt_filter_sales_2002_4579520442892726}; {local._virt_filter_sales_2002_502268311739685}; {local._virt_filter_sales_2002_6919717362710578}; {local._virt_filter_sales_2002_7768903282575206}; {local._virt_filter_sales_2002_7940303590208798}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query02.preql`

  ```text
  Syntax error in query02.preql: Undefined concept: joined.sales_2001. Suggestions: ['joined.d2001.sales_2001', 'd2001.sales_2001', 'joined.d2002.sales_2002']
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: HAVING references 'local.level', which is not in the SELECT projection (line 45). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.level
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: HAVING references 'local.level', which is not in the SELECT projection (line 45). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.level
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: HAVING references 'local.level', 'local.is_subtotal', 'local.is_grand_total', which are not in the SELECT projection (line 50). To filter output rows, add them to SELECT — prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.level, --local.is_subtotal, --local.is_grand_total
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 1 column 2468 (char 2467). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query11.preql`

  ```text
  Syntax error in query11.preql: 2 undefined concept references; fix all before re-running:
    - store.first_name (line 83, col 5, in ORDER BY); did you mean: store.customer.first_name, store.return_customer.first_name, web.billing_customer.first_name, web.ship_customer.first_name, web.return_customer.first_name, web.return_refund_customer.first_name?
    - store.last_name (line 84, col 5, in ORDER BY); did you mean: store.customer.last_name, store.return_customer.last_name, web.billing_customer.last_name, web.ship_customer.last_name, web.return_customer.last_name, web.return_refund_customer.last_name?
  ```
- `trilogy run query11.preql`

  ```text
  Syntax error in query11.preql: Undefined concept: s01.customer_id. Suggestions: ['s01.store_rev.customer_id', 'store_rev.customer_id', 'web_rev.customer_id', 'w01.web_rev.customer_id', 'store.customer.id']
  ```
- `trilogy run query11.preql`

  ```text
  Resolution error in query11.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 9 disconnected subgraphs: {local.billing_customer_code, local.first_name, local.last_name}; {local.preferred_cust_flag}; {local.s_growth}; {local.s_rev_01}; {local.s_rev_02}; {local.w_growth}; {local.w_rev_01}; {local.w_rev_02}; {s01.rev, w01.rev}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query11.preql`

  ```text
  Resolution error in query11.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {s01.fname, s01.lname, s01.login, s01.pcf, s01.rev, s02.rev, w01.rev, w02.rev}
  ```
- `trilogy run query11.preql`

  ```text
  Resolution error in query11.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {s01.fname, s01.lname, s01.login, s01.pcf, s01.rev, s02.rev, w01.rev, w02.rev}
  ```
- `trilogy run query11.preql`

  ```text
  Resolution error in query11.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 11 disconnected subgraphs: {local.billing_customer_code}; {local.first_name}; {local.last_name}; {local.preferred_cust_flag}; {local.s_growth}; {local.s_rev_01}; {local.s_rev_02}; {local.w_growth}; {local.w_rev_01}; {local.w_rev_02}; {store_rev_01.rev, web_rev_01.rev}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query11_test.preql`

  ```text
  Resolution error in query11_test.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {s01.cust_id, s01.rev, s02.rev, w01.rev, w02.rev}
  ```
- `trilogy run query11.preql`

  ```text
  Resolution error in query11.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 11 disconnected subgraphs: {local.billing_customer_code}; {local.first_name}; {local.last_name}; {local.preferred_cust_flag}; {local.s_growth}; {local.s_rev_01}; {local.s_rev_02}; {local.w_growth}; {local.w_rev_01}; {local.w_rev_02}; {s01.rev, w01.rev}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query11.preql`

  ```text
  Syntax error in query11.preql: Undefined concept: both_2001.cust_id. Suggestions: ['both_2001.sc_2001.cust_id', 'sc_2001.cust_id', 'wc_2001.cust_id']
  ```
- `trilogy run query11.preql`

  ```text
  Resolution error in query11.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 11 disconnected subgraphs: {local.billing_customer_code}; {local.first_name}; {local.last_name}; {local.preferred_cust_flag}; {local.s_growth}; {local.s_rev_01}; {local.s_rev_02}; {local.w_growth}; {local.w_rev_01}; {local.w_rev_02}; {s01.rev, w01.rev}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query11.preql`

  ```text
  Resolution error in query11.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 11 disconnected subgraphs: {local.billing_customer_code}; {local.first_name}; {local.last_name}; {local.preferred_cust_flag}; {local.s_growth}; {local.s_rev_01}; {local.s_rev_02}; {local.w_growth}; {local.w_rev_01}; {local.w_rev_02}; {sr01.rev, wr01.rev}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read query11.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: HAVING references 'bcc_channels.channel_count', which is not in the SELECT projection (line 20). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --bcc_channels.channel_count
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: Undefined concept: qualifying_bcc.bcc (line 30, col 18, in WHERE). Suggestions: ['qualifying_bcc.bcc_channels.bcc', 'bcc_channels.bcc', 'qualifying_bcc.bcc_channels.channel_count']
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: HAVING references 'local.overall_avg', which is not in the SELECT projection (line 28). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.overall_avg
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query17.preql`

  ```text
  Syntax error in query17.preql: Undefined concept: csr.row_one (line 3, in SELECT).
  ```
- `trilogy file read raw/date.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query17.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: HAVING references 'local.best_threshold', which is not in the SELECT projection (line 28). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.best_threshold
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: HAVING references 'local.max_customer_total', which is not in the SELECT projection (line 26). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.max_customer_total
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: HAVING references '__preql_internal.all_rows', which is not in the SELECT projection (line 23). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --__preql_internal.all_rows
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: Have {'RowsetNode<customer_totals.customer_id,customer_totals.customer_lifetime_total>': None} and need customer_totals.customer_lifetime_total > multiply(0.5,local.max_customer_total@Grain<Abstract>)
  ```
- `trilogy file read query64.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query75.preql`

  ```text
  Syntax error in query75.preql: Undefined concept: agg_by_year.yr. Suggestions: ['agg_by_year.lines_deduped.yr', 'lines_deduped.yr', 'agg_by_year.yr_net_qty', 'agg_by_year.yr_net_amt', '_agg_by_year_yr_net_qty']
  ```
- `trilogy run query75.preql`

  ```text
  Syntax error in query75.preql: Undefined concept: y2001.brand_id. Suggestions: ['y2001.agg_by_year.lines_deduped.brand_id', 'all_sales.item.brand_id', 'lines_deduped.brand_id', 'agg_by_year.lines_deduped.brand_id', 'y2002.agg_by_year.lines_deduped.brand_id', 'y2002.brand_id']
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 48 column 12 (char 2108). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query80.preql`

  ```text
  Syntax error in query80.preql: Undefined concept: cs.return_net_loss. Suggestions: ['ss.return_net_loss', 'cs.return_amount', 'ss.return_store.gmt_offset']
  ```

### `syntax-parse`

- `trilogy file write query11_debug.preql`

  ```text
  refused to write 'query11_debug.preql': not syntactically valid Trilogy.

  Parse error:
    --> 20:37
     |
  20 |   and sales.billing_customer.id in (where sales.channel = 'WEB' and sales.date.year = 2001 select sales.billing_customer.id)
     |                                     ^---
     |
     = expected sum_operator or literal
  Location:
  ...sales.billing_customer.id in ( ??? where sales.channel = 'WEB' an...

  Write stats: received 721 chars / 721 bytes; tail: …'count_distinct(sales.billing_customer.id) as both_count\r\\n;\r\\n'.
  ```
- `trilogy file write query11.preql`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:6
    |
  7 |     (where sales.channel = 'WEB' and sales.date.year = 2001 select sales.billing_customer.id)
    |      ^---
    |
    = expected sum_operator or literal
  Location:
  ....billing_customer.id in
       ( ??? where sales.channel = 'WEB' an...

  Write stats: received 3104 chars / 3104 bytes; tail: …'irst,\r\\n    preferred_cust_flag asc nulls first\r\\nlimit 100;\r\\n'.
  ```
- `trilogy file write query17.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;

where
  ss.date.quarter_name = '2001Q1'
  and ss.re…inner join ss.item.id = cs.item.id
order by
  item_code asc nulls first,
  item_description asc nulls first,
  store_state asc nulls first
limit 100;`

  ```text
  refused to write 'query17.preql': not syntactically valid Trilogy.

  Parse error:
    --> 13:9
     |
  13 |   count(*) as cnt,
     |         ^---
     |
     = expected sum_operator
  Location:
  ...state as store_state,   count( ??? *) as cnt,   avg(ss.quantity)

  Write stats: received 1096 chars / 1096 bytes; tail: …'on asc nulls first,\\n  store_state asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…sum(combined.total_sales) as total_sales
order by combined.last_name nulls first, combined.first_name nulls first, total_sales nulls first
limit 100;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:1
    |
  7 | rowset frequent_items_def as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...store-sale pairs in 2000-2003 ??? rowset frequent_items_def as w...

  Write stats: received 2531 chars / 2531 bytes; tail: …'d.first_name nulls first, total_sales nulls first\\nlimit 100;'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…sum(combined.total_sales) as total_sales
order by combined.last_name nulls first, combined.first_name nulls first, total_sales nulls first
limit 100;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
    --> 18:5
     |
  18 |     where store_sales.date.year >= 2000 and store_sales.date.year <= 2003
     |     ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or window_sql_over
  Location:
  ...les.customer.id)     by *     ??? where store_sales.date.year >=...

  Write stats: received 2567 chars / 2567 bytes; tail: …'d.first_name nulls first, total_sales nulls first\\nlimit 100;'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…sum(combined.total_sales) as total_sales
order by combined.last_name nulls first, combined.first_name nulls first, total_sales nulls first
limit 100;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy does not support subqueries — joins are auto-resolved from dotted paths. To filter on a value that lives on a related dimension, reference its dot-path directly. Example: instead of `where ss.store_id in (select store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...log_sales.bill_customer.id in ??? (select customer_totals.custom...

  Write stats: received 2716 chars / 2716 bytes; tail: …'d.first_name nulls first, total_sales nulls first\\nlimit 100;'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…sum(combined.total_sales) as total_sales
order by combined.last_name nulls first, combined.first_name nulls first, total_sales nulls first
limit 100;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` — a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...name, first_name, total_sales) ??? select     combined.last_name...

  Write stats: received 2573 chars / 2573 bytes; tail: …'d.first_name nulls first, total_sales nulls first\\nlimit 100;'.
  ```

### `undefined-concept`

- `trilogy explore query02.preql --show concepts`

  ```text
  Failed to parse query02.preql: (UndefinedConceptException(...), "Undefined concept: weekly_data.year. Suggestions: ['weekly_data.s.date.year', 's.date.year', 's.return_date.year', 's.billing_customer.first_sales_date.year', 's.billing_customer.first_shipto_date.year', 's.ship_customer.first_sales_date.year']")
  ```
- `trilogy explore query02.preql --show imports`

  ```text
  Failed to parse query02.preql: (UndefinedConceptException(...), "Undefined concept: joined.sales_2001. Suggestions: ['joined.d2001.sales_2001', 'd2001.sales_2001', 'joined.d2002.sales_2002']")
  ```
- `trilogy run query11.preql`

  ```text
  Syntax error in query11.preql: All arguments to coalesce must be of the same type, have {<DataType.STRING: 'string'>, <DataType.UNKNOWN: 'unknown'>} for [ref:cinfo.login, UndefinedConcept(address='w01.web.billing_customer.login', datatype=<DataType.UNKNOWN: 'unknown'>, metadata=None)]
  ```
- `trilogy run query11.preql`

  ```text
  Syntax error in query11.preql: All arguments to coalesce must be of the same type, have {<DataType.UNKNOWN: 'unknown'>, <DataType.STRING: 'string'>} for [ref:cinfo.login, UndefinedConcept(address='s01.store.customer.login', datatype=<DataType.UNKNOWN: 'unknown'>, metadata=None)]
  ```
- `trilogy run query11.preql`

  ```text
  Syntax error in query11.preql: All arguments to coalesce must be of the same type, have {<DataType.UNKNOWN: 'unknown'>, <DataType.STRING: 'string'>} for [ref:ci.login, UndefinedConcept(address='sr01.sales.billing_customer.login', datatype=<DataType.UNKNOWN: 'unknown'>, metadata=None)]
  ```

### `join-resolution`

- `trilogy run query11.preql`

  ```text
  Resolution error in query11.preql: Could not resolve connections for query with output ['local.billing_customer_code<Purpose.PROPERTY>Derivation.BASIC>', 'local.first_name<Purpose.PROPERTY>Derivation.BASIC>', 'local.last_name<Purpose.PROPERTY>Derivation.BASIC>', 'local.preferred_cust_flag<Purpose.PROPERTY>Derivation.BASIC>', 'local.s_rev_01<Purpose.CONSTANT>Derivation.BASIC>', 'local.s_rev_02<Purpose.CONSTANT>Derivation.BASIC>', 'local.w_rev_01<Purpose.CONSTANT>Derivation.BASIC>', 'local.w_rev_02<Purpose.CONSTANT>Derivation.BASIC>', 'local.s_growth<Purpose.CONSTANT>Derivation.BASIC>', 'local.w_growth<Purpose.CONSTANT>Derivation.BASIC>'] from current model.
  ```
- `trilogy run query54.preql`

  ```text
  Resolution error in query54.preql: Could not resolve connections for query with output ['local.segment<Purpose.PROPERTY>Derivation.BASIC>', 'local.customer_count<Purpose.METRIC>Derivation.AGGREGATE>', 'local.segment_times_50<Purpose.PROPERTY>Derivation.BASIC>'] from current model.
  ```
- `trilogy run query54.preql`

  ```text
  Resolution error in query54.preql: Could not resolve connections for query with output ['local.segment<Purpose.METRIC>Derivation.BASIC>', 'local.customer_count<Purpose.METRIC>Derivation.AGGREGATE>', 'local.segment_times_50<Purpose.PROPERTY>Derivation.BASIC>'] from current model.
  ```

### `cli-misuse`

- `trilogy run --format json --import raw.all_sales:all_sales select all_sales.channel, all_sales.channel_dim_text_id, all_sales.return_channel_dim_text_id, all…te as sd, all_sales.return_date.date as rd where all_sales.channel_dim_text_id is null and all_sales.return_channel_dim_text_id is not null limit 10;`

  ```text
  'json' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy run query54.preql --format rich`

  ```text
  '--format' is not a valid dialect. The dialect argument comes after the input file and any options.
    Try: trilogy run <input> [<dialect>]
  ```

### `syntax-missing-alias`

- `trilogy file write query11.preql`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `customer info from store (2002 store record for preferred_cust_flag)
      web.billing_customer.login as customer_info_from_store_2002_store_reco`
  Location:
  ...r_id
   select
       -- customer ??? info from store (2002 store re...

  Write stats: received 3060 chars / 3060 bytes; tail: …'irst,\r\\n    preferred_cust_flag asc nulls first\r\\nlimit 100;\r\\n'.
  ```
- `trilogy file write query11.preql`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `distinct
      sales.billing_customer.id as distinct_sales_billing_customer_id`
  Location:
  ...cust_id
   select distinct
       ??? sales.billing_customer.id as c...

  Write stats: received 3071 chars / 3071 bytes; tail: …'irst,\r\\n    preferred_cust_flag asc nulls first\r\\nlimit 100;\r\\n'.
  ```
