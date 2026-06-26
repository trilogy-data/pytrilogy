# Trilogy failure analysis — 20260626-193952

- Run `20260626-193952` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 357 | failed: 58 (16%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 34 | 59% |
| `syntax-parse` | 21 | 36% |
| `syntax-missing-alias` | 1 | 2% |
| `join-resolution` | 1 | 2% |
| `cli-misuse` | 1 | 2% |

## Detail

### `other`

- `trilogy run query02.preql`

  ```text
  Syntax error in query02.preql: Undefined concept: daily.day_of_week. Suggestions: ['s.date.day_of_week', 's.return_date.day_of_week', 's.billing_customer.first_sales_date.day_of_week', 's.billing_customer.first_shipto_date.day_of_week', 's.ship_customer.first_sales_date.day_of_week', 's.ship_customer.first_shipto_date.day_of_week']
  ```
- `trilogy file read query02.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: SELECT output 'local.total_gross_sales' is defined by an expression that references 'local.total_gross_sales' itself (line 33). This is a recursive self-reference: an alias cannot redefine a name its own calculation reads. Rename the output to a distinct name (e.g. `... as total_gross_sales_out`).
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: Aggregate concept local.entity_id cannot reference itself. If defining a new concept in a select, use a new name.
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: Undefined concept: local.rollup_channel (line 23, in SELECT).
  ```
- `trilogy run --import raw/store_sales:ss --import raw/web_sales:ws select ss.date.year, ws.date.year, ss.customer.id, ws.billing_customer.id limit 5;`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {ss.customer.id, ss.date.year}; {ws.billing_customer.id, ws.date.year}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read query11.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: HAVING references 's.channel', which is not in the SELECT projection (line 11). To make them available, you may add it to  the SELECT. Prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --s.channel
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: HAVING references 's.channel', which is not in the SELECT projection (line 11). To make them available, you may add it to  the SELECT. Prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --s.channel
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: HAVING references 's.channel', which is not in the SELECT projection (line 11). To make them available, you may add it to  the SELECT. Prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --s.channel
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run --import raw.all_sales:s 
with combo_test as
select 
    concat(s.item.brand_id::string, '-', s.item.class_id::string, '-', s.item.category_id::s…ing, '-', s.item.class_id::string, '-', s.item.category_id::string) in combo_test.key and s.date.year = 2001 and s.date.month_of_year = 11
limit 10;
`

  ```text
  Syntax error in stdin: HAVING references 's.channel', 's.date.year', which are not in the SELECT projection (line 4). To make them available, you may add them to  the SELECT. Prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --s.channel, --s.date.year
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: HAVING references 'local.overall_avg_sale', which is not in the SELECT projection (line 20). To make them available, you may add it to  the SELECT. Prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.overall_avg_sale
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run --import raw.all_sales:s select concat(s.item.brand_id::string, '-', s.item.class_id::string, '-', s.item.category_id::string) as combo_key, --s.channel having count_distinct(s.channel ? s.date.year between 1999 and 2001) = 3 limit 5;`

  ```text
  Syntax error in stdin: HAVING references 's.date.year', which is not in the SELECT projection (line 2). To make them available, you may add it to  the SELECT. Prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --s.date.year
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy file write --content # Query 17
# For store sales sold in fiscal quarter 2001Q1, joined to matching store returns
# and catalog sales, report per (it…) as cs_qty_cv
order by
    item_code asc nulls first,
    item_description asc nulls first,
    store_state asc nulls first
limit 100; query17.preql`

  ```text
  trilogy file write: `--content` takes a SINGLE string argument. Your args list put 2 separate tokens after --content (treating it like a shell command). In a tool call, pass the entire file body as one string element after --content, with newlines embedded literally — e.g.
    {"args": ["file", "write", "query70.preql", "--content", "import raw.store_sales as store_sales;\n\nselect ..."]}
  Alternatively use `--escapes` with a single-line `\n`-escaped string.
  ```
- `trilogy file read raw/catalog_store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file write --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sales;

# Frequent…st_name,
  combined.first_name,
  sum(combined.total) as total
order by
  combined.last_name,
  combined.first_name,
  total
limit 100; query23.preql`

  ```text
  trilogy file write: `--content` takes a SINGLE string argument. Your args list put 2 separate tokens after --content (treating it like a shell command). In a tool call, pass the entire file body as one string element after --content, with newlines embedded literally — e.g.
    {"args": ["file", "write", "query70.preql", "--content", "import raw.store_sales as store_sales;\n\nselect ..."]}
  Alternatively use `--escapes` with a single-line `\n`-escaped string.
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: HAVING references 'store_sales.item.id', 'local._virt_func_substring_2877672313873438', 'store_sales.date.id', which are not in the SELECT projection (line 8). To make them available, you may add them to  the SELECT. Prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --store_sales.item.id, --local._virt_func_substring_2877672313873438, --store_sales.date.id
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: HAVING references 'store_sales.item.id', 'local._virt_func_substring_2877672313873438', 'store_sales.date.id', which are not in the SELECT projection (line 8). To make them available, you may add them to  the SELECT. Prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --store_sales.item.id, --local._virt_func_substring_2877672313873438, --store_sales.date.id
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: Have {'RowsetNode<customer_totals.cid,customer_totals.cust_total>': None} and need customer_totals.cust_total > multiply(0.5,local.max_cust_total@Grain<Abstract>)
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: Have {'RowsetNode<customer_totals.cid,customer_totals.cust_total>': None} and need customer_totals.cust_total > multiply(0.5,local.max_cust_total@Grain<Abstract>)
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: Have {'RowsetNode<customer_totals.cid,customer_totals.cust_total>': None} and need customer_totals.cust_total > multiply(0.5,parenthetical(local._virt_agg_max_702225200423246@Grain<Abstract>))
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: HAVING references '__preql_internal.all_rows', which is not in the SELECT projection (line 15). To make them available, you may add it to  the SELECT. Prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --__preql_internal.all_rows
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy file read query23.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run /tmp/_find_month_seq.preql`

  ```text
  No dialect specified. Provide dialect as argument or set engine.dialect in config file.
  ```
- `trilogy run /tmp/_find_month_seq.preql duckdb`

  ```text
  Unexpected error in \tmp\_find_month_seq.preql: Unable to import '\tmp\raw\date.preql': [Errno 2] No such file or directory: '\\tmp\\raw\\date.preql'.
  ```
- `trilogy file read trilogy.toml`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: 2 undefined concept references; fix all before re-running:
    - local.prod_name (line 115, col 10, in ORDER BY); did you mean: store_agg.prod_name, y1999.store_agg.prod_name, y2000.store_agg.prod_name, store_name, ss.item.product_name?
    - local.store_name (line 115, col 21, in ORDER BY); did you mean: store_agg.store_name, y1999.store_agg.store_name, y2000.store_agg.store_name, ss.store.name, store_agg.prod_name?
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 53 column 12 (char 2433). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 53 column 12 (char 2474). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 48 column 12 (char 2007). Re-issue the call with valid JSON arguments.

  [guidance] You have issued this identical call 3 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy file read query80.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy file write query02.preql --content import raw.all_sales as s;

where s.channel in ('WEB', 'CATALOG');

# Define filtered set of week_seq values that …ay,
    @day_ratio(4) as Thursday,
    @day_ratio(5) as Friday,
    @day_ratio(6) as Saturday
order by
    s.date.week_seq asc nulls first
limit 100;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
   --> 3:38
    |
  3 | where s.channel in ('WEB', 'CATALOG');
    |                                      ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
  ....channel in ('WEB', 'CATALOG') ??? ;  # Define filtered set of we...

  Write stats: received 1146 chars / 1146 bytes; tail: …'rday\\norder by\\n    s.date.week_seq asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query02.preql --content import raw.all_sales as s;

# Filter to web and catalog channels only
where s.channel in ('WEB', 'CATALOG')

# Def…ay,
    @day_ratio(4) as Thursday,
    @day_ratio(5) as Friday,
    @day_ratio(6) as Saturday
order by
    s.date.week_seq asc nulls first
limit 100;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block — they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...seq values that occur in 2001 ??? auto weeks_in_2001 <- s.date.w...

  Write stats: received 1127 chars / 1127 bytes; tail: …'rday\\norder by\\n    s.date.week_seq asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query02.preql --content import raw.all_sales as s;

# Per-day sales (one value per date)
auto day_sales <- sum(s.net_paid) by s.date.id;
a…ay,
    @day_ratio(4) as Thursday,
    @day_ratio(5) as Friday,
    @day_ratio(6) as Saturday
order by
    s.date.week_seq asc nulls first
limit 100;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
   --> 5:27
    |
  5 | auto day_week_seq <- first(s.date.week_seq) by s.date.id;
    |                           ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...id; auto day_week_seq <- first ??? (s.date.week_seq) by s.date.id...

  Write stats: received 1077 chars / 1077 bytes; tail: …'rday\\norder by\\n    s.date.week_seq asc nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.all_sales:s select s.date.year, count(s.date.id) as days_with_sales group by 1 limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...s.date.id) as days_with_sales ??? group by 1 limit 10;
  ```
- `trilogy run --import raw.all_sales:s where s.channel in ('WEB', 'CATALOG') select s.date.year, min(s.date.week_seq) as min_ws, max(s.date.week_seq) as max_ws, count_distinct(s.date.week_seq) as weeks group by 1;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...nct(s.date.week_seq) as weeks ??? group by 1;
  ```
- `trilogy file write query05.preql --content import raw.all_sales as sales;

# Define channel display names
auto channel_display <- case
    when sales.channel…_amount) as total_returns,
    @net_profit_calc as net_profit
order by
    channel_display asc nulls first,
    entity_id asc nulls first
limit 100;
`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [212]: A `by <grain>` clause must attach directly to an aggregate, not to an expression that wraps one (e.g. `coalesce(...)`, `round(...)`, arithmetic). Move the grain inside, next to the aggregate — write `coalesce(sum(x) by store.id, 0)` — or compute the grouped aggregate first and wrap it: `auto m <- sum(x) by store.id;` then `coalesce(m, 0)`.
  Location:
  ...nnel_dim_id is not null ), 0) ??? by rollup sales.channel, sales...

  Write stats: received 1515 chars / 1515 bytes; tail: …'y asc nulls first,\\n    entity_id asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw/store_sales:ss select ss.date.year, count(1) as cnt where ss.date.year in (2001, 2002) group by ss.date.year order by ss.date.year;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
   ss.date.year in (2001, 2002) ??? group by ss.date.year order by...
  ```
- `trilogy run --import raw/web_sales:ws select ws.date.year, count(1) as cnt where ws.date.year in (2001, 2002) group by ws.date.year order by ws.date.year;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
   ws.date.year in (2001, 2002) ??? group by ws.date.year order by...
  ```
- `trilogy file write query11.preql -e -c import raw.store_sales as ss;
import raw.web_sales as ws;

# Store revenue by customer and year
rowset store_rev_by_cu…store_rev_by_cust.cust_id = web_rev_by_cust.cust_id
inner join store_rev_by_cust.yr = web_rev_by_cust.yr
order by store_rev_by_cust.cust_id
limit 20;`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
   --> 5:1
    |
  5 | rowset store_rev_by_cust as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
   revenue by customer and year ??? rowset store_rev_by_cust as se...

  Write stats: received 859 chars / 859 bytes; tail: …'_rev_by_cust.yr\\norder by store_rev_by_cust.cust_id\\nlimit 20;'.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as s;
import raw.item as item;

# overall average sale value (quantity * list_price) across a…ulls first,
    s.item.class_id asc nulls first,
    grouping(s.item.category_id) asc nulls first,
    s.item.category_id asc nulls first
limit 100;
`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
   --> 9:18
    |
  9 |   s.item.brand_id, s.item.class_id, s.item.category_id
    |                  ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...ll_three <-    s.item.brand_id ??? , s.item.class_id, s.item.cate...

  Write stats: received 1748 chars / 1748 bytes; tail: …'ls first,\\n    s.item.category_id asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as s;

# Step 1: Find (brand_id, class_id, category_id) combos that appear in all 3 channels …ulls first,
    s.item.class_id asc nulls first,
    grouping(s.item.category_id) asc nulls first,
    s.item.category_id asc nulls first
limit 100;
`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:1
    |
  7 | rowset qualifying_combos as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...s.item.category_id::string);  ??? rowset qualifying_combos as wh...

  Write stats: received 1864 chars / 1864 bytes; tail: …'ls first,\\n    s.item.category_id asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw.all_sales:s select s.item.brand_id, s.item.class_id, s.item.category_id, count_distinct(s.channel) as channels where s.date.year between 1999 and 2001 group by 1,2,3 having channels = 3 limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...te.year between 1999 and 2001 ??? group by 1,2,3 having channels...
  ```
- `trilogy run --import raw.all_sales:s select count_distinct(concat(s.item.brand_id::string, '-', s.item.class_id::string, '-', s.item.category_id::string)) as…tring, '-', s.item.class_id::string, '-', s.item.category_id::string) ? s.date.year between 1999 and 2001) as combos_in_range from all_sales limit 1;`

  ```text
  Syntax error in stdin: Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
   and 2001) as combos_in_range ??? from all_sales limit 1;
  ```
- `trilogy run --import raw.all_sales:s 
with qual as
where s.date.year between 1999 and 2001
select
    concat(s.item.brand_id::string, '-', s.item.class_id::s…', s.item.category_id::string) as combo_key,
    --s.channel
having count_distinct(s.channel) = 3
limit 5
;
select qual.combo_key from qual limit 5;
`

  ```text
  Syntax error in stdin: Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...mit 5 ; select qual.combo_key ??? from qual limit 5;
  ```
- `trilogy run --import raw.catalog_store_returns:csr select csr.store_sale_date.quarter_name, count(csr.ticket_number) as cnt group by 1 order by 1;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...unt(csr.ticket_number) as cnt ??? group by 1 order by 1;
  ```
- `trilogy run --import raw.catalog_store_returns:csr select distinct csr.store_sale_date.quarter_name as q, count(csr.ticket_number) as cnt order by q;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword — a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ..._store_returns as csr; select ??? distinct csr.store_sale_date.q...
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…
  combined.last_name,
  combined.first_name,
  sum(combined.total) as total
order by
  combined.last_name,
  combined.first_name,
  total
limit 100;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
    --> 10:3
     |
  10 |   where store_sales.date.year between 2000 and 2003;
     |   ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ..., store_sales.date.id) > 4)   ??? where store_sales.date.year be...

  Write stats: received 2294 chars / 2294 bytes; tail: …'ombined.last_name,\\n  combined.first_name,\\n  total\\nlimit 100;'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…
  combined.last_name,
  combined.first_name,
  sum(combined.total) as total
order by
  combined.last_name,
  combined.first_name,
  total
limit 100;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword — a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...e_sales.date.id) > 4 select   ??? distinct substring(store_sales...

  Write stats: received 2128 chars / 2128 bytes; tail: …'ombined.last_name,\\n  combined.first_name,\\n  total\\nlimit 100;'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…
  combined.last_name,
  combined.first_name,
  sum(combined.total) as total
order by
  combined.last_name,
  combined.first_name,
  total
limit 100;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
    --> 20:3
     |
  20 |   --max(customer_totals.cust_total) by * as max_total
     |   ^---
     |
     = expected metadata, limit, order_by, where, having, or JOIN_TYPE
  Location:
  ....sales_price) as cust_total   ??? --max(customer_totals.cust_tot...

  Write stats: received 2229 chars / 2229 bytes; tail: …'ombined.last_name,\\n  combined.first_name,\\n  total\\nlimit 100;'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…
  combined.last_name,
  combined.first_name,
  sum(combined.total) as total
order by
  combined.last_name,
  combined.first_name,
  total
limit 100;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy does not support subqueries — joins are auto-resolved from dotted paths. To filter on a value that lives on a related dimension, reference its dot-path directly. Example: instead of `where ss.store_id in (select store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...mer_totals.cust_total > 0.5 * ??? (select max_val from max_total...

  Write stats: received 2175 chars / 2175 bytes; tail: …'ombined.last_name,\\n  combined.first_name,\\n  total\\nlimit 100;'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…ls_last_name
    where customer_totals.cust_total > 0.5 * max_cust_total
  )
order by
  combined.last_name,
  combined.first_name,
  total
limit 100;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy does not support subqueries — joins are auto-resolved from dotted paths. To filter on a value that lives on a related dimension, reference its dot-path directly. Example: instead of `where ss.store_id in (select store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...where   combined.last_name in ??? (     select customer_totals_l...

  Write stats: received 2098 chars / 2098 bytes; tail: …'ombined.last_name,\\n  combined.first_name,\\n  total\\nlimit 100;'.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw.all_sales:s select s.channel, count(s.row_one) limit 5;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(s.row_one) as row_one_count`
  Location:
  ...t s.channel, count(s.row_one) ??? limit 5;
  ```

### `join-resolution`

- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Could not resolve connections for query with output ['local.qualifying_item<Purpose.PROPERTY>Derivation.FILTER>'] from current model.
  ```

### `cli-misuse`

- `trilogy explore raw/store_returns.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_returns.preql' does not exist.
  ```
