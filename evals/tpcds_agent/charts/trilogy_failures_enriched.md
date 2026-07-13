# Trilogy failure analysis — 20260713-211349

- Run `scope_ab_final_20260713-164029_off_5` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 142 | failed: 21 (15%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 15 | 71% |
| `syntax-parse` | 4 | 19% |
| `cli-misuse` | 2 | 10% |

## Detail

### `other`

- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_219203908.preql`

  ```text
  Syntax error in answer_219203908.preql: Undefined concept: s.row_counter.
  ```
- `trilogy run answer_219203908.preql`

  ```text
  Resolution error in answer_219203908.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 33). The requested concepts split into 2 disconnected subgraphs: {overall_avg_sales}; {nov2001_agg.brand_id, nov2001_agg.category_id, nov2001_agg.channel, nov2001_agg.class_id, nov2001_agg.line_item_count, nov2001_agg.total_sales}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_219203908.preql`

  ```text
  Syntax error in answer_219203908.preql: `by rollup (…)` cannot re-aggregate the bare measure reference `surviving_leaves.total_sales`. Alias it with an explicit aggregate (e.g. `sum(surviving_leaves.total_sales) as total_sales_total`).
  ```
- `trilogy run answer_2118989494.preql`

  ```text
  Syntax error in answer_2118989494.preql: Output column 'next_month_total' renames 'local.next_month_total' back to the name of an existing concept 'next_month_total' (defined at line 53) that 'local.next_month_total' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'next_month_total_out').
  ```
- `trilogy file read answer_2118989494.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_2986518257.preql`

  ```text
  Resolution error in answer_2986518257.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {monthly_totals.brand, monthly_totals.call_center_name, monthly_totals.category, monthly_totals.month_of_year, monthly_totals.year, yearly_avg.avg_monthly_sales}
  ```
- `trilogy run answer_2986518257.preql`

  ```text
  Syntax error in answer_2986518257.preql: 2 undefined concept references; fix all before re-running:
    - local.monthly_total (line 17, in ORDER BY); did you mean: monthly_totals.monthly_total, monthly_totals.year, monthly_totals.brand, prev_month_total?
    - local.monthly_total (line 41, col 5, in ORDER BY); did you mean: monthly_totals.monthly_total, monthly_totals.year, monthly_totals.brand, prev_month_total?
  ```
- `trilogy run answer_1197120511.preql`

  ```text
  Resolution error in answer_1197120511.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 14). The requested concepts split into 2 disconnected subgraphs: {item_benchmark.avg_discount, item_benchmark.item_sk}; {ws.date.date, ws.ext_discount_amount, ws.item.manufacturer_id, ws.item.sk}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid \escape: line 1 column 76 (char 75). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file read answer_569612608.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy file write answer_219203908.preql --content import raw.all_sales as s;

# Step 1: Find (brand_id, class_id, category_id) combos that appear in ALL TH…lls first,
    nov2001_agg.brand_id asc nulls first,
    nov2001_agg.class_id asc nulls first,
    nov2001_agg.category_id asc nulls first
limit 100;`

  ```text
  refused to write 'answer_219203908.preql': not syntactically valid Trilogy.

  Parse error:
    --> 30:5
     |
  30 |     -- Note: count line items = count of (order_id, item.sk) combos, which is row_counter
     |     ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...r), 0) as line_item_count     ??? -- Note: count line items = co...

  Write stats: received 2322 chars / 2322 bytes; tail: …'irst,\\n    nov2001_agg.category_id asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write answer_2802535988.preql --content import raw.web_sales as ws;

# Total web return amount per returning customer per return-location state …  8 asc nulls first,
    9 asc nulls first,
    10 asc nulls first,
    11 asc nulls first,
    12 asc nulls first,
    13 asc nulls first
limit 100;`

  ```text
  refused to write 'answer_2802535988.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...,     cust_returns.cust_total ??? group by     cust_returns.sk o...

  Write stats: received 1925 chars / 1925 bytes; tail: …'t,\\n    12 asc nulls first,\\n    13 asc nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.web_sales:ws select ws.item.manufacturer_id, count(ws.line_item) as cnt, sum(ws.ext_discount_amount) as total_disc where ws.date.date >= '2000-01-27'::date and ws.date.date <= '2000-04-26'::date group by 1 order by 2 desc limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...te.date <= '2000-04-26'::date ??? group by 1 order by 2 desc lim...
  ```
- `trilogy run --import raw/web_sales:ws select count_distinct(ws.order_number) as cnt where ws.order_number in (select ws2.order_number from (import raw.web_sa…ip_date.date >= '1999-02-01'::date and ws.ship_date.date <= '1999-04-02'::date and ws.ship_address.state = 'IL' and ws.web_site.company_name = 'pri';`

  ```text
  Syntax error in stdin: Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...r in (select ws2.order_number ??? from (import raw.web_sales as
  ```

### `cli-misuse`

- `trilogy explore raw/`

  ```text
  Invalid value for 'PATH': File 'raw/' is a directory.
  ```
- `trilogy explore raw/inventory/preql_inventory__web_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/inventory/preql_inventory__web_sales.preql' does not exist.
  ```
