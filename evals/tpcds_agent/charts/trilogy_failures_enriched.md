# Trilogy failure analysis — 20260625-025205

- Run `20260625-025205` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 324 | failed: 54 (17%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 27 | 50% |
| `syntax-parse` | 16 | 30% |
| `cli-misuse` | 4 | 7% |
| `syntax-missing-alias` | 3 | 6% |
| `undefined-concept` | 2 | 4% |
| `join-resolution` | 2 | 4% |

## Detail

### `other`

- `trilogy run query02.preql`

  ```text
  Syntax error in query02.preql: 10 undefined concept references; fix all before re-running:
    - weekly_sums.week_seq (line 22, col 5, in SELECT); did you mean: weekly_sums.sales.date.week_seq, sales.date.week_seq, sales.return_date.week_seq, sales.billing_customer.first_sales_date.week_seq, sales.billing_customer.first_shipto_date.week_seq, sales.ship_customer.first_sales_date.week_seq?
    - weekly_sums.week_seq (line 23, col 83, in SELECT); did you mean: weekly_sums.sales.date.week_seq, sales.date.week_seq, sales.return_date.week_seq, sales.billing_customer.first_sales_date.week_seq, sales.billing_customer.first_shipto_date.week_seq, sales.ship_customer.first_sales_date.week_seq?
    - weekly_sums.week_seq (line 24, col 83, in SELECT); did you mean: weekly_sums.sales.date.week_seq, sales.date.week_seq, sales.return_date.week_seq, sales.billing_customer.first_sales_date.week_seq, sales.billing_customer.first_shipto_date.week_seq, sales.ship_customer.first_sales_date.week_seq?
    - weekly_sums.week_seq (line 25, col 83, in SELECT); did you mean: weekly_sums.sales.date.week_seq, sales.date.week_seq, sales.return_date.week_seq, sales.billing_customer.first_sales_date.week_seq, sales.billing_customer.first_shipto_date.week_seq, sales.ship_customer.first_sales_date.week_seq?
    - weekly_sums.week_seq (line 26, col 83, in SELECT); did you mean: weekly_sums.sales.date.week_seq, sales.date.week_seq, sales.return_date.week_seq, sales.billing_customer.first_sales_date.week_seq, sales.billing_customer.first_shipto_date.week_seq, sales.ship_customer.first_sales_date.week_seq?
    - weekly_sums.week_seq (line 27, col 83, in SELECT); did you mean: weekly_sums.sales.date.week_seq, sales.date.week_seq, sales.return_date.week_seq, sales.billing_customer.first_sales_date.week_seq, sales.billing_customer.first_shipto_date.week_seq, sales.ship_customer.first_sales_date.week_seq?
    - weekly_sums.week_seq (line 28, col 83, in SELECT); did you mean: weekly_sums.sales.date.week_seq, sales.date.week_seq, sales.return_date.week_seq, sales.billing_customer.first_sales_date.week_seq, sales.billing_customer.first_shipto_date.week_seq, sales.ship_customer.first_sales_date.week_seq?
    - weekly_sums.week_seq (line 29, col 83, in SELECT); did you mean: weekly_sums.sales.date.week_seq, sales.date.week_seq, sales.return_date.week_seq, sales.billing_customer.first_sales_date.week_seq, sales.billing_customer.first_shipto_date.week_seq, sales.ship_customer.first_sales_date.week_seq?
    - weekly_sums.week_seq (line 20, col 7, in WHERE); did you mean: weekly_sums.sales.date.week_seq, sales.date.week_seq, sales.return_date.week_seq, sales.billing_customer.first_sales_date.week_seq, sales.billing_customer.first_shipto_date.week_seq, sales.ship_customer.first_sales_date.week_seq?
    - weekly_sums.week_seq (line 30, col 10, in ORDER BY); did you mean: weekly_sums.sales.date.week_seq, sales.date.week_seq, sales.return_date.week_seq, sales.billing_customer.first_sales_date.week_seq, sales.billing_customer.first_shipto_date.week_seq, sales.ship_customer.first_sales_date.week_seq?
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read trilogy.toml`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query05.preql`

  ```text
  Unexpected error in query05.preql: maximum recursion depth exceeded
  ```
- `trilogy run query05.preql`

  ```text
  Unexpected error in query05.preql: maximum recursion depth exceeded
  ```
- `trilogy run query05.preql`

  ```text
  Unexpected error in query05.preql: maximum recursion depth exceeded
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: Undefined concept: aggregated.chan. Suggestions: ['aggregated.combined.chan', 'chan', 'combined.chan', 'aggregated.combined.eid', 'aggregated.total_returns']
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: union(...) requires at least two relational arms.
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: Undefined concept: aggregated.chan. Suggestions: ['aggregated.combined.chan', 'chan', 'combined.chan', 'aggregated.combined.eid', 'aggregated.total_returns']
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: Undefined concept: detail.total_gross_sales. Suggestions: ['detail.aggregated.total_gross_sales', 'aggregated.total_gross_sales', '_aggregated_total_gross_sales']
  ```
- `trilogy run --import raw/all_sales:all_sales select count(channel) as cnt, all_sales.channel where all_sales.channel_dim_id is null and all_sales.return_channel_dim_id is not null and all_sales.return_date.date between '2000-08-23'::date and '2000-09-06'::date limit 10;`

  ```text
  Syntax error in stdin: Undefined concept: local.channel (line 2, in SELECT). Suggestions: ['all_sales.channel']
  ```
- `trilogy file read query05.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 1 column 1631 (char 1630). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query11.preql`

  ```text
  Syntax error in query11.preql: Undefined concept: store_2001.cust_id. Suggestions: ['store_2001.store_rev_by_cust.cust_id', 'store_rev_by_cust.cust_id', 'web_rev_by_cust.cust_id', 'web_2001.web_rev_by_cust.cust_id', 'store_2002.store_rev_by_cust.cust_id', 'web_2002.web_rev_by_cust.cust_id']
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: HAVING references 'local.overall_avg_sale', which is not in the SELECT projection (line 10). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.overall_avg_sale
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: 8 undefined concept references; fix all before re-running:
    - filtered_sales.channel (line 29, col 5, in SELECT); did you mean: filtered_sales.all_sales.channel, all_sales.channel, filtered_sales.class_id?
    - filtered_sales.brand_id (line 30, col 5, in SELECT); did you mean: filtered_sales.all_sales.item.brand_id, all_sales.item.brand_id, filtered_sales.class_id, filtered_sales.category_id, filtered_sales.channel?
    - filtered_sales.class_id (line 31, col 5, in SELECT); did you mean: filtered_sales.all_sales.item.class_id, all_sales.item.class_id, filtered_sales.brand_id, filtered_sales.category_id, filtered_sales.channel?
    - filtered_sales.category_id (line 32, col 5, in SELECT); did you mean: filtered_sales.all_sales.item.category_id, all_sales.item.category_id, filtered_sales.class_id, filtered_sales.brand_id?
    - filtered_sales.channel (line 36, col 5, in ORDER BY); did you mean: filtered_sales.all_sales.channel, all_sales.channel, filtered_sales.class_id?
    - filtered_sales.brand_id (line 37, col 5, in ORDER BY); did you mean: filtered_sales.all_sales.item.brand_id, all_sales.item.brand_id, filtered_sales.class_id, filtered_sales.category_id, filtered_sales.channel?
    - filtered_sales.class_id (line 38, col 5, in ORDER BY); did you mean: filtered_sales.all_sales.item.class_id, all_sales.item.class_id, filtered_sales.brand_id, filtered_sales.category_id, filtered_sales.channel?
    - filtered_sales.category_id (line 39, col 5, in ORDER BY); did you mean: filtered_sales.all_sales.item.category_id, all_sales.item.category_id, filtered_sales.class_id, filtered_sales.brand_id?
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: Have {'RowsetNode<customer_totals.cust_id,customer_totals.store_total>': None} and need customer_totals.store_total > multiply(0.5,local.max_total@Grain<Abstract>)
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: Have {'RowsetNode<cust_totals.cust_id,cust_totals.store_total>': None} and need cust_totals.store_total > multiply(0.5,local.max_store_total@Grain<Abstract>)
  ```
- `trilogy run --import raw/catalog_sales:cs --import raw/catalog_returns:cr select cs.item.id, sum(cs.ext_list_price) as cat_ext_list, sum(coalesce(cr.refunded…ndian','spring','floral','medium') and cs.item.current_price between 65 and 74 having cat_ext_list > 2*cat_refund order by cat_ext_list desc limit 5;`

  ```text
  Syntax error in stdin: Cannot compare INTEGER and ArrayType<STRING> with operator ComparisonOperator.IN in SubselectComparison(left=ref:cs.item.id, right=('purple', 'burlywood', 'indian', 'spring', 'floral', 'medium'), operator=<ComparisonOperator.IN: 'in'>)
  ```
- `trilogy run --import raw/catalog_sales:cs --import raw/catalog_returns:cr --import raw/item:i select cs.item.id, sum(cs.ext_list_price) as cat_ext_list, sum(… 'indian', 'spring', 'floral', 'medium') and i.current_price between 65 and 74 having cat_ext_list > 2*cat_refund order by cat_ext_list desc limit 5;`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 3 disconnected subgraphs: {cs.item.id, local.cat_ext_list}; {i.color, i.current_price}; {local.cat_refund}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run --import raw/catalog_sales:cs --import raw/catalog_returns:cr --import raw/item:i select cs.item.id, sum(cs.ext_list_price) as cat_ext_list, sum(… 'indian', 'spring', 'floral', 'medium') and i.current_price between 65 and 74 having cat_ext_list > 2*cat_refund order by cat_ext_list desc limit 5;`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {cs.item.id, local.cat_ext_list, local.cat_refund}; {i.color, i.current_price}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {cs.item.id, local.cat_ext_list_by_item, local.cat_refund_by_item}
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 50 column 12 (char 2004). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query75.preql`

  ```text
  Syntax error in query75.preql: 8 undefined concept references; fix all before re-running:
    - agg_data.yr (line 30, in SELECT); did you mean: agg_data.deduped.yr, deduped.yr, agg_data.brand_id, agg_data.total_qty?
    - agg_data.brand_id (line 33, col 5, in SELECT); did you mean: agg_data.deduped.brand_id, s.item.brand_id, item.brand_id, deduped.brand_id, agg_data.class_id, agg_data.yr?
    - agg_data.class_id (line 34, col 5, in SELECT); did you mean: agg_data.deduped.class_id, s.item.class_id, item.class_id, deduped.class_id, agg_data.brand_id, agg_data.category_id?
    - agg_data.category_id (line 35, col 5, in SELECT); did you mean: agg_data.deduped.category_id, s.item.category_id, item.category_id, deduped.category_id, agg_data.class_id?
    - agg_data.manufacturer_id (line 36, col 5, in SELECT); did you mean: agg_data.deduped.manufacturer_id, s.item.manufacturer_id, item.manufacturer_id, deduped.manufacturer_id?
    - agg_data.yr (line 37, col 145, in SELECT); did you mean: agg_data.deduped.yr, deduped.yr, agg_data.brand_id, agg_data.total_qty?
    - agg_data.yr (line 39, col 166, in SELECT); did you mean: agg_data.deduped.yr, deduped.yr, agg_data.brand_id, agg_data.total_qty?
    - agg_data.yr (line 40, col 166, in SELECT); did you mean: agg_data.deduped.yr, deduped.yr, agg_data.brand_id, agg_data.total_qty?
  ```
- `trilogy run query75.preql`

  ```text
  Resolution error in query75.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {item.category, local._deduped_brand_id, local._deduped_category_id, local._deduped_class_id, local._deduped_manufacturer_id}; {local._deduped_net_amt, local._deduped_net_qty, local._deduped_yr}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query80.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy run --import raw/all_sales:all_sales select all_sales.channel, all_sales.return_channel_dim_text_id, all_sales.return_channel_dim_id, count_distinct(…ales.channel, all_sales.return_channel_dim_text_id, all_sales.return_channel_dim_id having all_sales.return_channel_dim_text_id is not null limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...) as distinct_return_entities ??? group by all_sales.channel, al...
  ```
- `trilogy run --import raw/all_sales:all_sales select all_sales.channel, all_sales.channel_dim_text_id, all_sales.is_returned, count(all_sales.channel_dim_id) as cnt where all_sales.channel_dim_id is not null group by all_sales.channel, all_sales.channel_dim_text_id, all_sales.is_returned limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...es.channel_dim_id is not null ??? group by all_sales.channel, al...
  ```
- `trilogy file write query05.preql -e -c import raw.all_sales as all_sales;

# Combine sales and return entities
with combined as union(
    # Sales arm: sales…sce(sum(combined.total_returns), 0) as total_returns,
    coalesce(sum(combined.profit), 0) as net_profit
order by channel_type, entity_id
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
    --> 23:13
     |
  23 |         sum(-all_sales.return_net_loss) as profit
     |             ^---
     |
     = expected access_chain
  Location:
  ...as total_returns,         sum( ??? -all_sales.return_net_loss) as...

  Write stats: received 1888 chars / 1888 bytes; tail: …'0) as net_profit\\norder by channel_type, entity_id\\nlimit 100;'.
  ```
- `trilogy file write query05.preql -e -c import raw.all_sales as all_sales;

# Detail rows: aggregate by channel + entity
with detail as
select
    all_sales.c…ly entities where channel_dim_id is null?
# For returns, the channel might always have a channel_dim_id for context.
# Let me test this...

select 1;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
    --> 22:9
     |
  22 | select 1;
     |         ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
   Let me test this...  select 1 ??? ;

  Write stats: received 1540 chars / 1540 bytes; tail: …'channel_dim_id for context.\\n# Let me test this...\\n\\nselect 1;'.
  ```
- `trilogy run --import raw/all_sales:all_sales --all-rows select all_sales.return_channel_dim_text_id, all_sales.return_channel_dim_id where all_sales.return_d…0-09-06'::date and all_sales.return_channel_dim_id is not null and all_sales.date.date not between '2000-08-23'::date and '2000-09-06'::date limit 5;`

  ```text
  Syntax error in stdin:  --> 2:233
    |
  2 | select all_sales.return_channel_dim_text_id, all_sales.return_channel_dim_id where all_sales.return_date.date between '2000-08-23'::date and '2000-09-06'::date and all_sales.return_channel_dim_id is not null and all_sales.date.date not between '2000-08-23'::date and '2000-09-06'::date limit 5;
    |                                                                                                                                                                                                                                         ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
   null and all_sales.date.date ??? not between '2000-08-23'::date...
  ```
- `trilogy file write query11.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;

# Revenue per customer per year: store channel
rowset s… by customer_code asc nulls first,
    first_name asc nulls first,
    last_name asc nulls first,
    preferred_cust_flag asc nulls first
limit 100;
`

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
  ...tomer per year: store channel ??? rowset store_rev_by_cust as se...

  Write stats: received 2325 chars / 2325 bytes; tail: …'s first,\\n    preferred_cust_flag asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw.store_sales:ss select ss.date.quarter_name, count(ss.ticket_number) as cnt group by ss.date.quarter_name order by cnt desc limit 20;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...ount(ss.ticket_number) as cnt ??? group by ss.date.quarter_name
  ```
- `trilogy file write query54.preql -e -c import raw/catalog_sales as cs;
import raw/web_sales as ws;
import raw/store_sales as ss;
import raw/customer as cust;…county
  and ss.store.state = ss.customer.address.state
select
  ss.customer.id as customer_id,
  sum(ss.ext_sales_price) as total_ext_sales_price
;
`

  ```text
  refused to write 'query54.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/catalog_sales as cs;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/catalog_sales as cs; impor...

  Write stats: received 1222 chars / 1222 bytes; tail: …'er_id,\\n  sum(ss.ext_sales_price) as total_ext_sales_price\\n;\\n'.
  ```
- `trilogy run --import raw/store_sales:ss select ss.date.year, count(ss.line_item) as cnt group by ss.date.year order by ss.date.year;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...r, count(ss.line_item) as cnt ??? group by ss.date.year order by...
  ```
- `trilogy file write query64.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.physica…nd r2.cnt <= agg_rows.cnt
order by
    agg_rows.product_name,
    agg_rows.store_name,
    r2.cnt,
    agg_rows.wholesale_sum,
    r2.wholesale_sum;
`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
    --> 80:22
     |
  80 |   and r2.sale_year = 2000
     |                      ^---
     |
     = expected IDENTIFIER
  Location:
  ...tore_zip   and r2.sale_year = ??? 2000   and r2.cnt <= agg_rows....

  Write stats: received 3337 chars / 3337 bytes; tail: …'  r2.cnt,\\n    agg_rows.wholesale_sum,\\n    r2.wholesale_sum;\\n'.
  ```
- `trilogy file write query64.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.physica…  and y2000.cnt <= y1999.cnt
order by
    y1999.product_name,
    y1999.store_name,
    y2000.cnt,
    y1999.wholesale_sum,
    y2000.wholesale_sum;
`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
    --> 23:1
     |
  23 | rowset y1999 as
     | ^---
     |
     = expected EOI, block, or show_statement
  Location:
   aggregated store sales lines ??? rowset y1999 as where ss.item....

  Write stats: received 4017 chars / 4017 bytes; tail: …'2000.cnt,\\n    y1999.wholesale_sum,\\n    y2000.wholesale_sum;\\n'.
  ```
- `trilogy file write query64.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.physica…  and y2000.cnt <= y1999.cnt
order by
    y1999.product_name,
    y1999.store_name,
    y2000.cnt,
    y1999.wholesale_sum,
    y2000.wholesale_sum;
`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [220]: Filter or stray clause after a `join`? A query-scoped join `inner|left|full join <a> = <b>` takes only key equalities — to join on multiple keys, chain `= c` (one equivalence group) or separate distinct groups with `and` (`a = b and c = d`); STACK another `join` clause for a different join type. Note `and` joins KEY EQUALITIES only, not filters. Joins go right after the `select` list (preferred, SQL-like) or before `select`; the order is `where` -> `select` <cols> -> join(s) -> `having` -> `order by` -> `limit`. Filter input rows in `where` (before `select`); filter a joined or aggregated RESULT in `having` (select the field, hide it with a leading `--`). Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...as ca2,     y2000.cnt as cnt2 ??? inner join y1999.item_id = y20...

  Write stats: received 4013 chars / 4013 bytes; tail: …'2000.cnt,\\n    y1999.wholesale_sum,\\n    y2000.wholesale_sum;\\n'.
  ```
- `trilogy file write query64.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.physica…aving y2000.cnt <= y1999.cnt
order by
    y1999.product_name,
    y1999.store_name,
    y2000.cnt,
    y1999.wholesale_sum,
    y2000.wholesale_sum;
`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [220]: Filter or stray clause after a `join`? A query-scoped join `inner|left|full join <a> = <b>` takes only key equalities — to join on multiple keys, chain `= c` (one equivalence group) or separate distinct groups with `and` (`a = b and c = d`); STACK another `join` clause for a different join type. Note `and` joins KEY EQUALITIES only, not filters. Joins go right after the `select` list (preferred, SQL-like) or before `select`; the order is `where` -> `select` <cols> -> join(s) -> `having` -> `order by` -> `limit`. Filter input rows in `where` (before `select`); filter a joined or aggregated RESULT in `having` (select the field, hide it with a leading `--`). Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
   grain with cat_qual_items as ??? inner join cs.item.id = cr.ite...

  Write stats: received 3993 chars / 3993 bytes; tail: …'2000.cnt,\\n    y1999.wholesale_sum,\\n    y2000.wholesale_sum;\\n'.
  ```
- `trilogy file write query64.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.physica…aving y2000.cnt <= y1999.cnt
order by
    y1999.product_name,
    y1999.store_name,
    y2000.cnt,
    y1999.wholesale_sum,
    y2000.wholesale_sum;
`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [220]: Filter or stray clause after a `join`? A query-scoped join `inner|left|full join <a> = <b>` takes only key equalities — to join on multiple keys, chain `= c` (one equivalence group) or separate distinct groups with `and` (`a = b and c = d`); STACK another `join` clause for a different join type. Note `and` joins KEY EQUALITIES only, not filters. Joins go right after the `select` list (preferred, SQL-like) or before `select`; the order is `where` -> `select` <cols> -> join(s) -> `having` -> `order by` -> `limit`. Filter input rows in `where` (before `select`); filter a joined or aggregated RESULT in `having` (select the field, hide it with a leading `--`). Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...rain rowset cat_qual_items <- ??? inner join cs.item.id = cr.ite...

  Write stats: received 3996 chars / 3996 bytes; tail: …'2000.cnt,\\n    y1999.wholesale_sum,\\n    y2000.wholesale_sum;\\n'.
  ```
- `trilogy run --import raw/catalog_sales:cs --import raw/catalog_returns:cr --import raw/item:i rowset cat_items <- where i.color in ('purple','burlywood') and…in cs.item.id = cr.item.id and cs.item.id = i.id having cat_list > 2*cat_refund; select cat_items.item_id, cat_items.cat_list from cat_items limit 5;`

  ```text
  Syntax error in stdin: Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...s.item_id, cat_items.cat_list ??? from cat_items limit 5;
  ```
- `trilogy file write query80.preql --content import raw.all_sales as all_sales;

# Build the outlet identifier with proper prefix per channel
auto outlet_label…les.net_profit - coalesce(all_sales.return_net_loss, 0)
    )
order by
    channel_label asc nulls first,
    outlet_label asc nulls first
limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
    --> 20:58
     |
  20 |     sum(sales_col) by rollup channel_label, outlet_label as sales,
     |                                                          ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, window_sql_over, or OVER_COMPONENT_REF
  Location:
  ...p channel_label, outlet_label ??? as sales,     sum(returns_col)...

  Write stats: received 1562 chars / 1562 bytes; tail: …'asc nulls first,\\n    outlet_label asc nulls first\\nlimit 100;'.
  ```

### `cli-misuse`

- `trilogy run --import raw/all_sales:all_sales --format json select all_sales.date.date as d, all_sales.return_date.date as rd, all_sales.channel, all_sales.channel_dim_id, all_sales.return_channel_dim_id, all_sales.ext_sales_price, all_sales.return_amount limit 2;`

  ```text
  'json' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy run --import raw/all_sales:all_sales duck_db select all_sales.date.date as d, all_sales.return_date.date as rd, all_sales.channel, all_sales.channel_dim_id, all_sales.return_channel_dim_id, all_sales.ext_sales_price, all_sales.return_amount limit 2;`

  ```text
  'select all_sales.date.date as d, all_sales.return_date.date as rd, all_sales.channel, all_sales.channel_dim_id, all_sales.return_channel_dim_id, all_sales.ext_sales_price, all_sales.return_amount limit 2;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy explore raw/sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/sales.preql' does not exist.
  ```
- `trilogy syntax example union-stack-channels`

  ```text
  No such command 'syntax'.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw/all_sales:all_sales select all_sales.channel, sum(all_sales.ext_sales_price) by rollup all_sales.channel limit 10;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `sum(all_sales.ext_sales_price) by rollup all_sales.channel as sum_all_sales_ext_sales_price_by_rollup_`
  Location:
  ...) by rollup all_sales.channel ??? limit 10;
  ```
- `trilogy run --import raw.catalog_sales:cs select distinct cs.sold_date.quarter_name as q order by q limit 10;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `distinct cs.sold_date.quarter_name as distinct_cs_sold_date_quarter_name`
  Location:
  ..._sales as cs; select distinct ??? cs.sold_date.quarter_name as q...
  ```
- `trilogy run --import raw.store_sales:ss select count(ss.line_item);`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(ss.line_item) as line_item_count`
  Location:
  ...ss; select count(ss.line_item) ??? ;
  ```

### `undefined-concept`

- `trilogy explore query05.preql`

  ```text
  Failed to parse query05.preql: (UndefinedConceptException(...), "Undefined concept: aggregated.chan. Suggestions: ['aggregated.combined.chan', 'chan', 'combined.chan', 'aggregated.combined.eid', 'aggregated.total_returns']")
  ```
- `trilogy explore query11.preql`

  ```text
  Failed to parse query11.preql: (UndefinedConceptException(...), "Undefined concept: store_2001.cust_id. Suggestions: ['store_2001.store_rev_by_cust.cust_id', 'store_rev_by_cust.cust_id', 'web_rev_by_cust.cust_id', 'web_2001.web_rev_by_cust.cust_id', 'store_2002.store_rev_by_cust.cust_id', 'web_2002.web_rev_by_cust.cust_id']")
  ```

### `join-resolution`

- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Could not resolve connections for query with output ['local._y1999_product_name<Purpose.PROPERTY>Derivation.BASIC>', 'local._y1999_item_id<Purpose.KEY>Derivation.BASIC>', 'local._y1999_store_name<Purpose.PROPERTY>Derivation.BASIC>', 'local._y1999_store_zip<Purpose.PROPERTY>Derivation.BASIC>', 'local._y1999_sale_street_num<Purpose.PROPERTY>Derivation.BASIC>', 'local._y1999_sale_street_name<Purpose.PROPERTY>Derivation.BASIC>', 'local._y1999_sale_city<Purpose.PROPERTY>Derivation.BASIC>', 'local._y1999_sale_zip<Purpose.PROPERTY>Derivation.BASIC>', 'local._y1999_cust_street_num<Purpose.PROPERTY>Derivation.BASIC>', 'local._y1999_cust_street_name<Purpose.PROPERTY>Derivation.BASIC>', 'local._y1999_cust_city<Purpose.PROPERTY>Derivation.BASIC>', 'local._y1999_cust_zip<Purpose.PROPERTY>Derivation.BASIC>', 'local._y1999_first_sales_yr<Purpose.PROPERTY>Derivation.BASIC>', 'local._y1999_first_ship_yr<Purpose.PROPERTY>Derivation.BASIC>', 'local._y1999_cnt<Purpose.METRIC>Derivation.AGGREGATE>', 'local._y1999_wholesale_sum<Purpose.METRIC>Derivation.AGGREGATE>', 'local._y1999_list_price_sum<Purpose.METRIC>Derivation.AGGREGATE>', 'local._y1999_coupon_sum<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Could not resolve connections for query with output ['local._virt_filter_ticket_number_427237187931465<Purpose.PROPERTY>Derivation.FILTER>'] from current model.
  ```
