# Trilogy failure analysis — 20260626-125555

- Run `20260626-125555` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 208 | failed: 33 (16%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 25 | 76% |
| `syntax-parse` | 5 | 15% |
| `syntax-missing-alias` | 2 | 6% |
| `cli-misuse` | 1 | 3% |

## Detail

### `other`

- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: ORDER BY contains aggregate `grouping(local.channel_type)` (line 27), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(local.channel_type) as g order by g desc`.
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: grouping()/grouping_id() requires a `by rollup`/`by cube`/grouping-sets aggregate in the enclosing select; it has no meaning without a grouping set (e.g. add `by rollup <dim>` to an aggregate in the select).
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: grouping()/grouping_id() requires a `by rollup`/`by cube`/grouping-sets aggregate in the enclosing select; it has no meaning without a grouping set (e.g. add `by rollup <dim>` to an aggregate in the select).
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: HAVING references 'local.overall_avg', which is not in the SELECT projection (line 16). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.overall_avg
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query14.preql`

  ```text
  Resolution error in query14.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {item.brand_id, item.category_id, item.class_id}; {local.qualifies, local.total_count, local.total_sales, s.channel, s.date.date}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 10 column 14 (char 497). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: HAVING references 'ss.date.id', 'local._virt_func_substring_1502455396794287', which are not in the SELECT projection (line 8). To filter output rows, add them to SELECT — prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --ss.date.id, --local._virt_func_substring_1502455396794287
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: HAVING references 'ss.date.id', 'local._virt_func_substring_1502455396794287', which are not in the SELECT projection (line 8). To filter output rows, add them to SELECT — prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --ss.date.id, --local._virt_func_substring_1502455396794287
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: HAVING references 'customer_totals.cust_total', 'local.max_cust_total', which are not in the SELECT projection (line 30). To filter output rows, add them to SELECT — prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --customer_totals.cust_total, --local.max_cust_total
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: HAVING references 'ss.date.id', 'local._virt_func_substring_1502455396794287', which are not in the SELECT projection (line 8). To filter output rows, add them to SELECT — prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --ss.date.id, --local._virt_func_substring_1502455396794287
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run --import raw.store_sales:store_sales select date.month_seq, date.year, date.month_of_year where date.year = 1998 and date.month_of_year = 12 limit 5;`

  ```text
  Syntax error in stdin: 5 undefined concept references; fix all before re-running:
    - date.month_seq (line 2, col 8, in SELECT); did you mean: store_sales.date.month_seq, store_sales.store.date.month_seq, store_sales.return_store.date.month_seq, store_sales.return_date.month_seq, store_sales.customer.first_sales_date.month_seq, store_sales.customer.first_shipto_date.month_seq?
    - date.year (line 2, col 24, in SELECT); did you mean: store_sales.date.year, store_sales.store.date.year, store_sales.return_store.date.year, store_sales.return_date.year, store_sales.customer.first_sales_date.year, store_sales.customer.first_shipto_date.year?
    - date.month_of_year (line 2, col 35, in SELECT); did you mean: store_sales.date.month_of_year, store_sales.store.date.month_of_year, store_sales.return_store.date.month_of_year, store_sales.return_date.month_of_year, store_sales.customer.first_sales_date.month_of_year, store_sales.customer.first_shipto_date.month_of_year?
    - date.year (line 2, col 60, in WHERE); did you mean: store_sales.date.year, store_sales.store.date.year, store_sales.return_store.date.year, store_sales.return_date.year, store_sales.customer.first_sales_date.year, store_sales.customer.first_shipto_date.year?
    - date.month_of_year (line 2, col 81, in WHERE); did you mean: store_sales.date.month_of_year, store_sales.store.date.month_of_year, store_sales.return_store.date.month_of_year, store_sales.return_date.month_of_year, store_sales.customer.first_sales_date.month_of_year, store_sales.customer.first_shipto_date.month_of_year?
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 51 column 2 (char 2053). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query75.preql`

  ```text
  Syntax error in query75.preql: Undefined concept: per_line.brand_id. Suggestions: ['per_line.item.brand_id', 'all_sales.item.brand_id', 'item.brand_id', 'per_line.net_qty']
  ```
- `trilogy run query75.preql`

  ```text
  Syntax error in query75.preql: Undefined concept: by_year_brand.sale_year. Suggestions: ['by_year_brand.per_line.sale_year', 'per_line.sale_year', 'by_year_brand.qty', 'by_year_brand.amt']
  ```
- `trilogy run query75.preql`

  ```text
  Resolution error in query75.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 3 disconnected subgraphs: {date.year}; {item.category, local._yr2001_brand_id, local._yr2001_category_id, local._yr2001_class_id, local._yr2001_manufacturer_id}; {local._yr2001_amt, local._yr2001_qty}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query75.preql`

  ```text
  Resolution error in query75.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 3 disconnected subgraphs: {date.year}; {item.category, local._yr2001_brand_id, local._yr2001_category_id, local._yr2001_class_id, local._yr2001_manufacturer_id}; {local._yr2001_amt, local._yr2001_qty}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query75.preql`

  ```text
  Resolution error in query75.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 3 disconnected subgraphs: {date.year}; {item.category, local._yr2001_brand_id, local._yr2001_category_id, local._yr2001_class_id, local._yr2001_manufacturer_id}; {local._yr2001_amt, local._yr2001_qty}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query75.preql`

  ```text
  Resolution error in query75.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 3 disconnected subgraphs: {date.year}; {item.category, local._yr2001_brand_id, local._yr2001_category_id, local._yr2001_class_id, local._yr2001_manufacturer_id}; {local._yr2001_amt, local._yr2001_qty}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query75.preql`

  ```text
  Resolution error in query75.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 3 disconnected subgraphs: {date.year}; {item.category, local._yr2001_brand_id}; {local._yr2001_amt, local._yr2001_qty}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query75.preql`

  ```text
  Resolution error in query75.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {date.year}; {item.category, local._yr2001_brand_id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query75.preql`

  ```text
  Resolution error in query75.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 3 disconnected subgraphs: {date.year}; {item.category, local.brand_id}; {local.qty}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read query75.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy run --import raw/date:date select date.week_seq, min(date.year) as min_year, max(date.year) as max_year, count(date.id) as days group by date.week_seq order by date.week_seq limit 10; duckdb`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ..._year, count(date.id) as days ??? group by date.week_seq order b...
  ```
- `trilogy run --import raw/date:date select date.week_seq, min(date.year) as min_year, max(date.year) as max_year, count(date.id) as days date.year in (1998,1999,2000,2001,2002,2003) order by date.week_seq limit 80; duckdb`

  ```text
  Syntax error in stdin:  --> 2:102
    |
  2 | select date.week_seq, min(date.year) as min_year, max(date.year) as max_year, count(date.id) as days date.year in (1998,1999,2000,2001,2002,2003) order by date.week_seq limit 80;
    |                                                                                                      ^---
    |
    = expected metadata, limit, order_by, where, having, or JOIN_TYPE
  Location:
  ..._year, count(date.id) as days ??? date.year in (1998,1999,2000,2...
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all;

with combined as union(
    # Sales arm: entities with sales in the period
    (wher…store_identifier as net_profit
order by
    grouping(channel_type),
    channel_type,
    grouping(store_identifier),
    store_identifier
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [212]: A `by <grain>` clause must attach directly to an aggregate, not to an expression that wraps one (e.g. `coalesce(...)`, `round(...)`, arithmetic). Move the grain inside, next to the aggregate — write `coalesce(sum(x) by store.id, 0)` — or compute the grouped aggregate first and wrap it: `auto m <- sum(x) by store.id;` then `coalesce(m, 0)`.
  Location:
  ...sum(combined.gross_sales), 0) ??? by rollup channel_type, store_...

  Write stats: received 2073 chars / 2073 bytes; tail: …' grouping(store_identifier),\\n    store_identifier\\nlimit 100;'.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as s;
import raw.item as item;

# Overall average sale value (quantity * list_price) across a…row_one) as total_count
order by s.channel nulls first, item.brand_id nulls first, item.class_id nulls first, item.category_id nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
    --> 26:1
     |
  26 | select
     | ^---
     |
     = expected limit, order_by, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...* s.list_price) > overall_avg ??? select     s.channel,     item...

  Write stats: received 1553 chars / 1553 bytes; tail: …'lass_id nulls first, item.category_id nulls first\\nlimit 100;'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Step 1: Frequent item…sales.total_sales) as total_sales
order by
    last_name asc nulls first,
    first_name asc nulls first,
    total_sales asc nulls first
limit 100
;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
    --> 41:6
     |
  41 |     (import raw.catalog_sales as cs2;
     |      ^---
     |
     = expected select_statement
  Location:
  ...combined_sales as union(     ( ??? import raw.catalog_sales as cs...

  Write stats: received 2356 chars / 2360 bytes; tail: …'asc nulls first,\\n    total_sales asc nulls first\\nlimit 100\\n;'.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw/date:date where date.year = 2001 select distinct date.week_seq order by date.week_seq; duckdb`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `distinct date.week_seq as distinct_date_week_seq`
  Location:
  ...e.year = 2001 select distinct ??? date.week_seq order by date.we...
  ```
- `trilogy file write query64.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

# Per-item catalo…p
order by
    y1999.product_name,
    y1999.store_name,
    y2000.sale_line_count,
    y1999.total_wholesale_cost,
    y2000.total_wholesale_cost
;
`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `distinct
      cat_item_agg.item_id as distinct_cat_item_agg_item_id`
  Location:
  ...65 and 74 select distinct     ??? cat_item_agg.item_id,     ss.i...

  Write stats: received 4475 chars / 4475 bytes; tail: …'1999.total_wholesale_cost,\\n    y2000.total_wholesale_cost\\n;\\n'.
  ```

### `cli-misuse`

- `trilogy explore raw/`

  ```text
  Invalid value for 'PATH': File 'raw/' is a directory.
  ```
