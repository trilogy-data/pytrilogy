# Trilogy failure analysis — 20260626-224624

- Run `20260626-224624` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 267 | failed: 44 (16%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 23 | 52% |
| `syntax-parse` | 17 | 39% |
| `cli-misuse` | 2 | 5% |
| `syntax-missing-alias` | 1 | 2% |
| `join-resolution` | 1 | 2% |

## Detail

### `other`

- `trilogy file read trilogy.toml`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.all_sales:all_sales`

  ```text
  --import only applies to inline queries, not file/directory inputs.
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: All arguments to coalesce must be of the same type, have {<DataType.NUMERIC: 'numeric'>, <DataType.FLOAT: 'float'>} for [ref:local.gsales, cast(0,NUMERIC)]
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: SELECT output 'local.net_profit' is defined by an expression that references 'local.net_profit' itself (line 10). This is a recursive self-reference: an alias cannot redefine a name its own calculation reads. Rename the output to a distinct name (e.g. `... as net_profit_out`).
  ```
- `trilogy file read query05.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: HAVING references 'local.overall_avg_sale', which is not in the SELECT projection (line 9). To make them available, you may add it to  the SELECT. Prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.overall_avg_sale
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: HAVING references 'local.overall_avg_sale', which is not in the SELECT projection (line 11). To make them available, you may add it to  the SELECT. Prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.overall_avg_sale
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy file read query14.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.date:date select date.quarter_name, date.quarter, date.year order by date.date limit 5;`

  ```text
  Syntax error in stdin: ORDER BY references 'date.date', which is not in the SELECT projection (line 2). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --date.date order by date.date asc`.
  ```
- `trilogy run --import raw.store_sales:ss --import raw.catalog_sales:cs where ss.item.id = 1711
select ss.item.id, ss.ticket_number, ss.quantity, ss.return_qua… '2001Q1'
  and ss.return_date.quarter_name in ('2001Q1','2001Q2','2001Q3')
  and cs.sold_date.quarter_name in ('2001Q1','2001Q2','2001Q3')
limit 20;`

  ```text
  Unexpected error in stdin: Multiple where clauses are not supported
  ```
- `trilogy file read query17.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: HAVING references 'local.item_date_pair', which is not in the SELECT projection (line 18). To make them available, you may add it to  the SELECT. Prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.item_date_pair
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: HAVING references 'customer_totals.cust_total', 'local.max_cust_total', which are not in the SELECT projection (line 45). To make them available, you may add them to  the SELECT. Prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --customer_totals.cust_total, --local.max_cust_total
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query23.preql`

  ```text
  Resolution error in query23.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 18). The requested concepts split into 2 disconnected subgraphs: {local._frequent_item_groups_item_group}; {local._frequent_item_groups_pair_count, ss.date.year}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read query23.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/repro.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 19). The requested concepts split into 3 disconnected subgraphs: {catalog_item_totals.total_list_price}; {catalog_return_totals.total_refund}; {local.coupon_sum, local.cust_city, local.cust_street_name, local.cust_street_number, local.cust_zip, local.first_sales_year, local.first_ship_year, local.line_count, local.list_price_sum, local.product_name, local.sale_city, local.sale_street_name, local.sale_street_number, local.sale_year, local.sale_zip, local.store_name, local.store_zip, local.wholesale_sum, ss.customer.demographics.marital_status, ss.customer_demographic.marital_status, ss.is_returned, ss.item.color, ss.item.current_price}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Have {'GroupNode<cs.item.id,local._virt_agg_sum_6755395842573629>': None} and need local._virt_agg_sum_6755395842573629 > multiply(2,local._virt_agg_sum_7510806110648841@Grain<cr.item.id>)
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: HAVING references 'local.item_cat_list', 'local.item_cat_refund', which are not in the SELECT projection (line 11). To make them available, you may add them to  the SELECT. Prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.item_cat_list, --local.item_cat_refund
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 11). The requested concepts split into 3 disconnected subgraphs: {local.coupon_sum, local.cust_city, local.cust_street_name, local.cust_street_number, local.cust_zip, local.first_sales_year, local.first_ship_year, local.line_count, local.list_price_sum, local.product_name, local.sale_city, local.sale_street_name, local.sale_street_number, local.sale_year, local.sale_zip, local.store_name, local.store_zip, local.wholesale_sum, ss.customer.demographics.marital_status, ss.customer_demographic.marital_status, ss.is_returned, ss.item.color, ss.item.current_price}; {local.item_cat_list}; {local.item_cat_refund}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: HAVING references 'local.item_cat_list', 'local.item_cat_refund', which are not in the SELECT projection (line 16). To make them available, you may add them to  the SELECT. Prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.item_cat_list, --local.item_cat_refund
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy run --import raw.all_sales:all_sales select all_sales.date.week_seq, all_sales.date.year, count(*) where all_sales.channel in ('WEB', 'CATALOG') and all_sales.date.year between 2000 and 2003 order by all_sales.date.week_seq limit 20;`

  ```text
  Syntax error in stdin: Syntax [223]: `*` is not a valid argument — Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) — e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...eek_seq, all_sales.date.year, ??? count(*) where all_sales.chann...
  ```
- `trilogy file write query02.preql --content import raw.all_sales as all_sales;

# Aggregate web and catalog sales by week_seq and day_of_week
with weekly_dow …ect all_sales.date.week_seq
    where all_sales.channel in ('WEB', 'CATALOG')
      and all_sales.date.year = 2001
)
order by ratios.wk nulls first;
`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
    --> 13:1
     |
  13 | rowset ratios as
     | ^---
     |
     = expected EOI, block, or show_statement
  Location:
   / next year (53 weeks ahead) ??? rowset ratios as select     we...

  Write stats: received 1133 chars / 1133 bytes; tail: …'ll_sales.date.year = 2001\\n)\\norder by ratios.wk nulls first;\\n'.
  ```
- `trilogy file write query02.preql --content import raw.all_sales as all_sales;

# Aggregate web and catalog sales by week_seq and day_of_week
with weekly_dow …ect all_sales.date.week_seq
    where all_sales.channel in ('WEB', 'CATALOG')
      and all_sales.date.year = 2001
)
order by ratios.wk nulls first;
`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy does not support subqueries — joins are auto-resolved from dotted paths. To filter on a value that lives on a related dimension, reference its dot-path directly. Example: instead of `where ss.store_id in (select store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...s saturday where ratios.wk in ??? (     select all_sales.date.we...

  Write stats: received 1131 chars / 1131 bytes; tail: …'ll_sales.date.year = 2001\\n)\\norder by ratios.wk nulls first;\\n'.
  ```
- `trilogy file write query05.preql`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [212]: A `by <grain>` clause must attach directly to an aggregate, not to an expression that wraps one (e.g. `coalesce(...)`, `round(...)`, arithmetic). Move the grain inside, next to the aggregate — write `coalesce(sum(x) by store.id, 0)` — or compute the grouped aggregate first and wrap it: `auto m <- sum(x) by store.id;` then `coalesce(m, 0)`.
  Location:
  ...ext_sales_price), 0::numeric) ??? by rollup all_sales.channel, e...

  Write stats: received 1391 chars / 1391 bytes; tail: …'order by channel nulls first, entity nulls first\r\\nlimit 100;'.
  ```
- `trilogy file write query14.preql -e -c import raw.all_sales as sales;

# Step 1: Find (brand_id, class_id, category_id) combos appearing in all 3 channels in… nulls first,
    sales.item.brand_id asc nulls first,
    sales.item.class_id asc nulls first,
    sales.item.category_id asc nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:43
    |
  4 | auto qualifying_bcc <- sales.item.brand_id, sales.item.class_id, sales.item.category_id
    |                                           ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...ing_bcc <- sales.item.brand_id ??? , sales.item.class_id, sales.i...

  Write stats: received 1100 chars / 1100 bytes; tail: …'first,\\n    sales.item.category_id asc nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.date:date select distinct date.quarter_name order by date.quarter_name limit 10;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword — a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...port raw.date as date; select ??? distinct date.quarter_name ord...
  ```
- `trilogy run --import raw.store_sales:ss select ss.return_date.quarter_name, count(ss.return_quantity) as cnt where ss.return_date.quarter_name is not null group by ss.return_date.quarter_name order by ss.return_date.quarter_name limit 20;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...date.quarter_name is not null ??? group by ss.return_date.quarte...
  ```
- `trilogy run --import raw.store_sales:ss select count(ss.quantity) as cnt, count(*) as total where ss.quantity is null limit 5;`

  ```text
  Syntax error in stdin: Syntax [223]: `*` is not a valid argument — Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) — e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...ct count(ss.quantity) as cnt, ??? count(*) as total where ss.qua...
  ```
- `trilogy run --import raw.store_sales:ss --import raw.catalog_sales:cs select ss.item.id, count(ss.quantity) as ss_cnt where ss.date.quarter_name = '2001Q1' a…001Q2','2001Q3') inner join ss.customer.id = cs.bill_customer.id inner join ss.item.id = cs.item.id group by ss.item.id order by ss.item.id limit 10;`

  ```text
  Syntax error in stdin:  --> 3:213
    |
  3 | select ss.item.id, count(ss.quantity) as ss_cnt where ss.date.quarter_name = '2001Q1' and ss.return_date.quarter_name in ('2001Q1','2001Q2','2001Q3') and cs.sold_date.quarter_name in ('2001Q1','2001Q2','2001Q3') inner join ss.customer.id = cs.bill_customer.id inner join ss.item.id = cs.item.id group by ss.item.id order by ss.item.id limit 10;
    |                                                                                                                                                                                                                     ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
   ('2001Q1','2001Q2','2001Q3') ??? inner join ss.customer.id = cs...
  ```
- `trilogy run --import raw.store_sales:ss --import raw.catalog_sales:cs select ss.item.id, ss.ticket_number, ss.quantity, ss.return_quantity, cs.quantity, cs.o…s.sold_date.quarter_name in ('2001Q1','2001Q2','2001Q3') inner join ss.customer.id = cs.bill_customer.id inner join ss.item.id = cs.item.id limit 20;`

  ```text
  Syntax error in stdin:  --> 3:286
    |
  3 | select ss.item.id, ss.ticket_number, ss.quantity, ss.return_quantity, cs.quantity, cs.order_number where ss.item.id = 1711 and ss.date.quarter_name = '2001Q1' and ss.return_date.quarter_name in ('2001Q1','2001Q2','2001Q3') and cs.sold_date.quarter_name in ('2001Q1','2001Q2','2001Q3') inner join ss.customer.id = cs.bill_customer.id inner join ss.item.id = cs.item.id limit 20;
    |                                                                                                                                                                                                                                                                                              ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
   ('2001Q1','2001Q2','2001Q3') ??? inner join ss.customer.id = cs...
  ```
- `trilogy file write query23.preql --content import raw.store_sales as ss;
import raw.all_sales as al;
import raw.item as item;

# Frequent items: count of (it…istinct (item, sold_date) pairs
having count_distinct(ss.item.id) * count_distinct(ss.date.date) > 4

select
    frequent_items.item_group
limit 100;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
    --> 15:5
     |
  15 |     -- Actually count distinct (item, sold_date) pairs
     |     ^---
     |
     = expected metadata, limit, order_by, where, having, or JOIN_TYPE
  Location:
  ....date.date) as pair_count     ??? -- Actually count distinct (it...

  Write stats: received 695 chars / 695 bytes; tail: …'e.date) > 4\\n\\nselect\\n    frequent_items.item_group\\nlimit 100;'.
  ```
- `trilogy file write query54.preql --content # Query 54: Customers who bought 'Women' 'maternity' via catalog/web in Dec 1998,
# then sum their store sales in …uyers.cust_id
;

# Actually, union should just stack them, and we can use membership.
# Let me rethink - use the rowset approach properly.
 --escapes`

  ```text
  refused to write 'query54.preql': not syntactically valid Trilogy.

  Parse error:
    --> 37:1
     |
  37 | union
     | ^---
     |
     = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...cust_id in web_buyers.cust_id ??? union select     web_buyers.cu...

  Write stats: received 1352 chars / 1352 bytes; tail: …'rship.\\n# Let me rethink - use the rowset approach properly.\\n'.
  ```
- `trilogy file write query75.preql --content import raw.all_sales as sales;

# Deduplicate per-line records before aggregating
rowset deduped <-
where sales.it…y.total_amt as amt_diff
where yearly.yr = 2001
having
    current_qty / yearly.total_qty < 0.9
order by
    qty_diff asc,
    amt_diff asc
limit 100;`

  ```text
  refused to write 'query75.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [211]: Expression in `by` clause must be wrapped in parens — write `by (expr1, expr2, ...)`. Bare identifiers (`by a, b`) work without parens, but any function call, cast, or other expression needs them.
  Location:
   0) as net_amt ;  # Aggregate ??? by year and dimensions rowset

  Write stats: received 1751 chars / 1751 bytes; tail: …'< 0.9\\norder by\\n    qty_diff asc,\\n    amt_diff asc\\nlimit 100;'.
  ```
- `trilogy run --import raw.all_sales:sales select sales.date.year, count(sales.order_id) as cnt where sales.item.category = 'Books' group by sales.date.year order by sales.date.year;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...sales.item.category = 'Books' ??? group by sales.date.year order...
  ```
- `trilogy run --import raw.all_sales:sales select sales.date.year as yr, sales.item.brand_id as brand_id, sum(coalesce(sales.quantity,0) - coalesce(sales.return_quantity,0)) as net_qty where sales.item.category = 'Books' and sales.date.year in (2001,2002) group... order by yr, brand_id limit 10;`

  ```text
  Syntax error in stdin:  --> 2:214
    |
  2 | select sales.date.year as yr, sales.item.brand_id as brand_id, sum(coalesce(sales.quantity,0) - coalesce(sales.return_quantity,0)) as net_qty where sales.item.category = 'Books' and sales.date.year in (2001,2002) group... order by yr, brand_id limit 10;
    |                                                                                                                                                                                                                      ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...ales.date.year in (2001,2002) ??? group... order by yr, brand_id...
  ```
- `trilogy file write query80.preql --content import raw.all_sales as all_sales;

# Build channel label and outlet identifier
auto channel_label <- case when al…   @sales_amt as sales,
    @returns_amt as returns,
    @profit_amt as profit
order by channel nulls first, outlet_identifier nulls first
limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
    --> 23:5
     |
  23 |     @channel_label as channel,
     |     ^---
     |
     = expected select_item
  Location:
  ...let_id is not null select     ??? @channel_label as channel,

  Write stats: received 1452 chars / 1452 bytes; tail: …'hannel nulls first, outlet_identifier nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.channel, all_sales.channel_dim_text_id, count(all_sales.order_id) as cnt where all_sales.outlet…ate and all_sales.item.current_price > 50 and all_sales.promotion.channel_tv = 'N' group by all_sales.channel, all_sales.channel_dim_text_id limit 10`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...es.promotion.channel_tv = 'N' ??? group by all_sales.channel, al...
  ```

### `cli-misuse`

- `trilogy run --import raw.all_sales:all_sales duckdb tpcds.duckdb`

  ```text
  'tpcds.duckdb' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy run --import raw.all_sales:all_sales duck_db tpcds.duckdb`

  ```text
  'tpcds.duckdb' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw.store_sales:ss select ss.date.quarter_name, count(ss.ticket_number) order by ss.date.quarter_name limit 20;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(ss.ticket_number) as ticket_number_count`
  Location:
  ...name, count(ss.ticket_number) ??? order by ss.date.quarter_name
  ```

### `join-resolution`

- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Could not resolve connections for query with output ['local.qualifying_item<Purpose.PROPERTY>Derivation.FILTER>'] from current model.
  ```
