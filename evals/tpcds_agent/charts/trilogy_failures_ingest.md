# Trilogy failure analysis — 20260720-005028

- Run `20260720-005028` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 362 | failed: 50 (14%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 27 | 54% |
| `syntax-parse` | 17 | 34% |
| `cli-misuse` | 3 | 6% |
| `syntax-missing-alias` | 3 | 6% |

## Detail

### `other`

- `trilogy file read answer_1835050598.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_4254732537.preql`

  ```text
  Syntax error in answer_4254732537.preql: Comparison `cs.promotion.channel_email = 'N'` matches every value of nullable enum field 'cs.promotion.channel_email', which contains only these values: 'N'. It only excludes nulls; simplify it to 'cs.promotion.channel_email is not null'.
  ```
- `trilogy file read raw/web_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_4207382245.preql`

  ```text
  Syntax error in answer_4207382245.preql: Undefined concept: web_sales_total. Suggestions: ['web_q.web_sales_total', 'store_q.store_sales_total']
  ```
- `trilogy run answer_4207382245.preql`

  ```text
  Resolution error in answer_4207382245.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {store_q.county, web_q.county, web_q.q, web_q.web_sales_total}
  ```
- `trilogy run answer_3347758002.preql`

  ```text
  Syntax error in answer_3347758002.preql: Undefined concept: raw.item.category. Suggestions: ['ws.item.category', 'ss.item.category', 'cs.item.category', 'ss.promotion.item.category', 'cs.promotion.item.category', 'ws.promotion.item.category']
  ```
- `trilogy run answer_3347758002.preql`

  ```text
  Syntax error in answer_3347758002.preql: Undefined concept: mfg_id (line 48, col 3, in ORDER BY). Suggestions: ['combined.mfg_id']
  ```
- `trilogy run answer_3530408544.preql`

  ```text
  Syntax error in answer_3530408544.preql: Comparison `ss.store.county = 'Williamson County'` matches every value of enum field 'ss.store.county', which contains only these values: 'Williamson County'. It is always true and should be removed.
  ```
- `trilogy run answer_345752060.preql`

  ```text
  Syntax error in answer_345752060.preql: Undefined concept: item.category. Suggestions: ['ss.item.category', 'cs.item.category', 'ws.item.category', 'ss.promotion.item.category', 'cs.promotion.item.category', 'ws.promotion.item.category']
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/store.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.store:store select store.county, count(store.store_sk) where store.county in ('Orange County','Bronx County','Franklin Parish','Williamson County');`

  ```text
  Syntax error in stdin: Comparison `store.county in ('Orange County', 'Bronx County', 'Franklin Parish', 'Williamson County')` matches every value of enum field 'store.county', which contains only these values: 'Williamson County'. It is always true and should be removed.
  ```
- `trilogy run answer_1772060640.preql`

  ```text
  Syntax error in answer_1772060640.preql: ORDER BY references 'sales.customer.customer_sk', which is not in the SELECT projection (line 20). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --sales.customer.customer_sk order by sales.customer.customer_sk asc`.
  ```
- `trilogy file read raw/web_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read answer_2333598260.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 1 column 4005 (char 4004). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run answer_3544057080.preql`

  ```text
  Syntax error in answer_3544057080.preql: Nothing was executed: parsed 5 definition statement(s) (3 rowsets, 2 imports) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 1 column 4475 (char 4474). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 97 column 12 (char 5383). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run --import raw.store_sales:store_sales select return_reason.desc, count(store_sales.line_item) as cnt where return_reason.desc like '%reason%' limit 10;`

  ```text
  Syntax error in stdin: 2 undefined concept references; fix all before re-running:
    - return_reason.desc (line 2, col 8, in SELECT); did you mean: store_sales.return_reason.desc, store_sales.item.desc, store_sales.return_reason.sk, store_sales.return_reason.id?
    - return_reason.desc (line 2, col 70, in WHERE); did you mean: store_sales.return_reason.desc, store_sales.item.desc, store_sales.return_reason.sk, store_sales.return_reason.id?
  ```

### `syntax-parse`

- `trilogy file write answer_3347758002.preql --content # For sales across all channels in May 1998 to billing addresses with GMT offset -5,
# for items whose m…xt_price numeric)

select
  combined.mfg_id,
  sum(combined.total_ext_price) as combined_total
order by
  combined_total asc,
  mfg_id asc
limit 100;`

  ```text
  refused to write 'answer_3347758002.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...gint, total_ext_price numeric) ???  select   combined.mfg_id,
  ```
- `trilogy file write answer_3247656127.preql --content # Find items where any version of the item is sold in colors 'slate', 'blanched', or 'burnished'
# For t…  sum(combined_sales.total_price) as total_sale_price
order by total_sale_price nulls first,
         combined_sales.item_code nulls first
limit 100;`

  ```text
  refused to write 'answer_3247656127.preql': not syntactically valid Trilogy.

  Parse error:
    --> 14:4
     |
  14 |   (import raw.store_sales as ss
     |    ^---
     |
     = expected select_statement
  Location:
  ...h combined_sales as union(   ( ??? import raw.store_sales as ss
  ```
- `trilogy file write answer_345752060.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Get item_ids…es.item_code as item_code,
  sum(all_sales.total) as combined_total
order by
  item_code asc nulls first,
  combined_total asc nulls first
limit 100;`

  ```text
  refused to write 'answer_345752060.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...em_code string, total numeric) ???  select   all_sales.item_code...
  ```
- `trilogy run --import raw.store:store select distinct store.county;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...rt raw.store as store; select ??? distinct store.county;
  ```
- `trilogy file write answer_1772060640.preql --content import raw.store_sales as sales;

# customer info
auto customer_identified <- sales.customer.customer_sk…   line_item_count between 1 and 5
order by
    line_item_count desc,
    last_name asc,
    ticket_number asc,
    --sales.customer.customer_sk asc;`

  ```text
  refused to write 'answer_1772060640.preql': not syntactically valid Trilogy.

  Parse error:
    --> 37:6
     |
  37 |     --sales.customer.customer_sk asc;
     |      ^---
     |
     = expected access_chain
  Location:
  ...,     ticket_number asc,     - ??? -sales.customer.customer_sk as...
  ```
- `trilogy file write answer_1772060640.preql --content import raw.store_sales as sales;

# household demographics on the sale
auto buy_potential_ok <- sales.ho…   line_item_count between 1 and 5
order by
    line_item_count desc,
    last_name asc,
    ticket_number asc,
    --sales.customer.customer_sk asc;`

  ```text
  refused to write 'answer_1772060640.preql': not syntactically valid Trilogy.

  Parse error:
    --> 32:6
     |
  32 |     --sales.customer.customer_sk asc;
     |      ^---
     |
     = expected access_chain
  Location:
  ...,     ticket_number asc,     - ??? -sales.customer.customer_sk as...
  ```
- `trilogy file write answer_2910545909.preql --content import raw.store_sales as ss;

where
    ss.date_dim.day_name = 'Monday'
    and ss.date_dim.year betwee…st_name asc nulls first,
    store_city_display asc nulls first,
    total_net_profit asc nulls first,
    ss.ticket_number asc nulls last
limit 100;`

  ```text
  refused to write 'answer_2910545909.preql': not syntactically valid Trilogy.

  Parse error:
    --> 14:32
     |
  14 |     ss.customer.customer_sk as --customer_sk,
     |                                ^---
     |
     = expected IDENTIFIER
  Location:
     ss.customer.customer_sk as ??? --customer_sk,     ss.customer...
  ```
- `trilogy file write answer_2869182220.preql -c import raw.store_returns as store_returns;

# First, get the set of demo_sk values from store returns (returnin…store_returns.customer.household_demographics.income_band.upper_bound <= 88128
order by store_returns.customer.customer_id asc nulls first
limit 100;`

  ```text
  refused to write 'answer_2869182220.preql': not syntactically valid Trilogy.

  Parse error:
    --> 13:3
     |
  13 | --store_returns.ticket_number as tkt,
     |   ^---
     |
     = expected PURPOSE, PROPERTY, UNIQUE, or AUTO
  Location:
  ...e ticket_number and item_sk -- ??? store_returns.ticket_number as...
  ```
- `trilogy file write answer_3705756794.preql --content import raw/all_sales as all_sales;

with combined as union(
    # STORE channel - sales activity
    (wh…(sum(combined.np), 0) as net_profit
by rollup (combined.c, combined.eid)
order by combined.c asc nulls first, combined.eid asc nulls first
limit 100;`

  ```text
  refused to write 'answer_3705756794.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/all_sales as all_sales;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/all_sales as all_sales;  w...

  Write stats: received 3148 chars / 3148 bytes; tail: …'d.c asc nulls first, combined.eid asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write answer_2928586490.preql --content import raw/store_sales as store;
import raw/catalog_sales as catalog;
import raw/web_sales as web;

# St…total_sales
order by
    combined_cat_web.last_name nulls first,
    combined_cat_web.first_name nulls first,
    total_sales nulls first
limit 100;
`

  ```text
  refused to write 'answer_2928586490.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/store_sales as store;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/store_sales as store; impo...

  Write stats: received 3062 chars / 3062 bytes; tail: …'st_name nulls first,\\n    total_sales nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_2928586490.preql --content import raw.store_sales as store;
import raw.catalog_sales as catalog;
import raw.web_sales as web;

# St…total_sales
order by
    combined_cat_web.last_name nulls first,
    combined_cat_web.first_name nulls first,
    total_sales nulls first
limit 100;
`

  ```text
  refused to write 'answer_2928586490.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block - they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
   of item.desc, sold date) > 4 ??? rowset frequent_items as where...

  Write stats: received 2541 chars / 2541 bytes; tail: …'st_name nulls first,\\n    total_sales nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_2928586490.preql --content import raw.store_sales as store;
import raw.catalog_sales as catalog;
import raw.web_sales as web;

# St…total_sales
order by
    combined_cat_web.last_name nulls first,
    combined_cat_web.first_name nulls first,
    total_sales nulls first
limit 100;
`

  ```text
  refused to write 'answer_2928586490.preql': not syntactically valid Trilogy.

  Parse error:
    --> 57:1
     |
  57 | having combined_cat_web.total > 0
     | ^---
     |
     = expected EOI, block, or show_statement
  Location:
  ...st_name, first_name, total);  ??? having combined_cat_web.total

  Write stats: received 2316 chars / 2316 bytes; tail: …'st_name nulls first,\\n    total_sales nulls first\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw/date:d select distinct d.month_seq, d.year, d.month_of_year where d.month_seq between 1188 and 1190 order by d.month_seq;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  import raw.date as d; select ??? distinct d.month_seq, d.year,
  ```
- `trilogy file write answer_3544057080.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;

# ---- Rowset: catalog-level cumulative m….sale_lines <= y1999.sale_lines
order by y1999.product_name, y1999.store_name, y2000.sale_lines, y1999.wholesale_sum, y2000.wholesale_sum
limit 200;
`

  ```text
  refused to write 'answer_3544057080.preql': not syntactically valid Trilogy.

  Parse error:
    --> 52:1
     |
  52 | rowset y1999 as
     | ^---
     |
     = expected EOI, block, or show_statement
  Location:
  ...: self-pair 1999 vs 2000 ---- ??? rowset y1999 as where sales_ag...

  Write stats: received 4199 chars / 4199 bytes; tail: …'_lines, y1999.wholesale_sum, y2000.wholesale_sum\\nlimit 200;\\n'.
  ```
- `trilogy run --import raw.catalog_sales:cs select cs.is_returned as ret, cs.return_quantity as qty, cs.return_date.week_seq as wk, cs.item.id as iid limit 5 where cs.is_returned = true and cs.return_date.week_seq in (5244, 5257, 5264);`

  ```text
  Syntax error in stdin:  --> 2:107
    |
  2 | select cs.is_returned as ret, cs.return_quantity as qty, cs.return_date.week_seq as wk, cs.item.id as iid limit 5 where cs.is_returned = true and cs.return_date.week_seq in (5244, 5257, 5264);
    |                                                                                                           ^---
    |
    = expected metadata, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ..._seq as wk, cs.item.id as iid ??? limit 5 where cs.is_returned =...
  ```
- `trilogy run --import raw.web_sales:ws select ws.is_returned as ret, ws.return_quantity as qty, ws.return_date.week_seq as wk, ws.item.id as iid limit 5 where ws.is_returned = true and ws.return_date.week_seq in (5244, 5257, 5264);`

  ```text
  Syntax error in stdin:  --> 2:107
    |
  2 | select ws.is_returned as ret, ws.return_quantity as qty, ws.return_date.week_seq as wk, ws.item.id as iid limit 5 where ws.is_returned = true and ws.return_date.week_seq in (5244, 5257, 5264);
    |                                                                                                           ^---
    |
    = expected metadata, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ..._seq as wk, ws.item.id as iid ??? limit 5 where ws.is_returned =...
  ```
- `trilogy run --import raw.store_sales:store_sales select return_reason.desc, count(store_sales.line_item) as cnt where return_reason.desc like '%reason%' group by return_reason.desc limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...n_reason.desc like '%reason%' ??? group by return_reason.desc li...
  ```

### `cli-misuse`

- `trilogy explore raw/facts/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/facts/store_sales.preql' does not exist.
  ```
- `trilogy explore raw/facts/store_returns.preql`

  ```text
  Invalid value for 'PATH': File 'raw/facts/store_returns.preql' does not exist.
  ```
- `trilogy explore raw/facts/catalog_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/facts/catalog_sales.preql' does not exist.
  ```

### `syntax-missing-alias`

- `trilogy file write answer_3979964698.preql --content import raw.all_sales as s;

# Filter conditions: year 2001, time between 30838-59638, DHL/BARIAN carrier…then s.quantity * s.net_paid_inc_tax
        else 0 end) as m12_net
order by s.warehouse.name asc nulls first, s.date.year asc nulls first
limit 100;`

  ```text
  refused to write 'answer_3979964698.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `hidden warehouse sk (not displayed)
      --s.warehouse.sk as hidden_warehouse_sk_not_displayed_s_ware`
  Location:
  ...TALOG')  select     -- hidden ??? warehouse sk (not displayed)

  Write stats: received 9748 chars / 9748 bytes; tail: …'name asc nulls first, s.date.year asc nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.catalog_sales:cs select cs.is_returned, cs.return_quantity, cs.return_date.week_seq, cs.item.id limit 5 where cs.is_returned = true and cs.return_date.week_seq in (5244, 5257, 5264);`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `cs.item.id as cs_item_id`
  Location:
  ...urn_date.week_seq, cs.item.id ??? limit 5 where cs.is_returned =...
  ```
- `trilogy run --import raw.web_sales:ws select ws.is_returned, ws.return_quantity, ws.return_date.week_seq, ws.item.id limit 5 where ws.is_returned = true and ws.return_date.week_seq in (5244, 5257, 5264);`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `ws.item.id as ws_item_id`
  Location:
  ...urn_date.week_seq, ws.item.id ??? limit 5 where ws.is_returned =...
  ```
