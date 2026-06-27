# Trilogy failure analysis — 20260627-010809

- Run `20260627-010809` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 345 | failed: 53 (15%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 32 | 60% |
| `syntax-parse` | 15 | 28% |
| `undefined-concept` | 4 | 8% |
| `syntax-missing-alias` | 1 | 2% |
| `cli-misuse` | 1 | 2% |

## Detail

### `other`

- `trilogy run query02.preql duckdb`

  ```text
  Syntax error in query02.preql: Ambiguous reference 'with_future.day_of_week': matches ['with_future.daily_sales.day_of_week', 'with_future.future.day_of_week']. Qualify the full path to disambiguate.
  ```
- `trilogy run query02.preql duckdb`

  ```text
  Syntax error in query02.preql: Undefined concept: future.day_sales. Suggestions: ['with_future.future.day_sales', 'with_future.daily_sales.day_sales', 'daily_sales.day_sales', 'with_future.daily_sales.day_of_week']
  ```
- `trilogy run query02.preql duckdb`

  ```text
  Syntax error in query02.preql: Undefined concept: with_future.future.future_sales.
  ```
- `trilogy run query02.preql duckdb`

  ```text
  Syntax error in query02.preql: Undefined concept: future.day_sales. Suggestions: ['daily_sales.day_sales']
  ```
- `trilogy run query02.preql duckdb`

  ```text
  Syntax error in query02.preql: Ambiguous reference 'daily_sales_future.week_seq': matches ['daily_sales_future.daily_sales.sales.date.week_seq', 'daily_sales_future.daily_sales.week_seq']. Qualify the full path to disambiguate.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy agent-info syntax example rollup`

  ```text
  Unknown syntax example: 'rollup'

  Available Trilogy syntax examples — print one with `trilogy agent-info syntax example <name>`:

  - `query-structure` — the clause order of a query (`where` -> `select` <cols> -> join(s) -> `having` -> `order by` -> `limit`) and how to define a rowset (a NAME then a select); joins sit right after the select list — filter a joined or aggregated RESULT in `having`, input rows in `where`
  - `filtered-aggregate` — `sum(x ? cond)` / `count(x ? cond)` aggregate just the matching rows; to COUNT ROWS count the unique grain/row key, not a non-unique sub-key; `by <grain>`
  …
  sum — the `distinct`/UNION substitute
  - `rank-over-rollup` — rank rollup subtotals/leaves with a SINGLE `rank(a,b) over (partition by level, parent ...)` — not separate ranks per level
  - `staged-membership` — compute a membership set in a `rowset` (keys meeting a count/HAVING), then filter the main query with `<key> in <rowset>.<col>`
  - `correlated-exists-via-grouped-counts` — translate `EXISTS other` / `NOT EXISTS other matching` over the same model into two `count(...) by <grain>` compared in `where` (`> 1` = another exists, `= 1` = no other matches) — don't filter on a boolean-of-aggregate
  ```
- `trilogy run query11.preql`

  ```text
  Syntax error in query11.preql: Undefined concept: date.year. Suggestions: ['store.date.year', 'store.store.date.year', 'store.return_store.date.year', 'web.date.year', 'store.return_date.year', 'store.customer.first_sales_date.year']
  ```
- `trilogy run query11.preql`

  ```text
  Resolution error in query11.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 13). The requested concepts split into 2 disconnected subgraphs: {local.billing_customer_code, local.store_rev_2001, local.store_rev_2002, store.customer.first_name, store.customer.id, store.customer.last_name, store.customer.preferred_cust_flag}; {local.web_rev_2001, local.web_rev_2002, web.billing_customer.id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read query11.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: Comparison `sales.channel in ('STORE', 'CATALOG', 'WEB')` matches every value of enum field 'sales.channel', which contains only these values: 'WEB', 'CATALOG', 'STORE'. It is always true and should be removed.
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: HAVING references 'local.overall_avg_sale', which is not in the SELECT projection (line 12). To make them available, you may add it to  the SELECT. Prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.overall_avg_sale
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy file read raw/catalog_store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: HAVING references '__preql_internal.all_rows', which is not in the SELECT projection (line 20). To make them available, you may add it to  the SELECT. Prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --__preql_internal.all_rows
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: HAVING references 'max_totals.max_total', which is not in the SELECT projection (line 32). To make them available, you may add it to  the SELECT. Prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --max_totals.max_total
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy file read query23.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query54.preql duckdb tpcds.duckdb`

  ```text
  Syntax error in query54.preql: Undefined concept: date.year. Suggestions: ['ss.date.year', 'ss.store.date.year', 'ss.return_store.date.year', 'cs.date.year', 'ws.date.year', 'ss.return_date.year']
  ```
- `trilogy run query54.preql duckdb tpcds.duckdb`

  ```text
  Resolution error in query54.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 41). The requested concepts split into 2 disconnected subgraphs: {dec1998.cs.sold_date.month_seq}; {local._customer_totals_cid, local._customer_totals_total_store_sales, ss.customer.address.county, ss.customer.address.state, ss.customer.id, ss.date.month_seq, ss.store.county, ss.store.state}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query54.preql duckdb tpcds.duckdb`

  ```text
  Resolution error in query54.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 40). The requested concepts split into 2 disconnected subgraphs: {dec1998.d.month_seq}; {local._customer_totals_cid, local._customer_totals_total_store_sales, ss.customer.address.county, ss.customer.address.state, ss.customer.id, ss.date.month_seq, ss.store.county, ss.store.state}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query54.preql duckdb tpcds.duckdb`

  ```text
  Resolution error in query54.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 36). The requested concepts split into 2 disconnected subgraphs: {local._customer_totals_cid, local._customer_totals_total_store_sales, ss.customer.address.county, ss.customer.address.state, ss.customer.id, ss.date.month_seq, ss.store.county, ss.store.state}; {local.dec1998_month_seq}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query54.preql duckdb tpcds.duckdb`

  ```text
  Syntax error in query54.preql: Comparison `ss.date.month_of_year >= 1` matches every value of enum field 'ss.date.month_of_year', which contains only these values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12. It is always true and should be removed.
  ```
- `trilogy run query54.preql duckdb tpcds.duckdb`

  ```text
  Syntax error in query54.preql: Undefined concept: d.
  ```
- `trilogy run --import raw.date:d select d.year, d.month_of_year where d.month_seq between 1188 and 1190 order by d.month_seq; duckdb tpcds.duckdb`

  ```text
  Syntax error in stdin: ORDER BY references 'd.month_seq', which is not in the SELECT projection (line 2). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --d.month_seq order by d.month_seq asc`.
  ```
- `trilogy run query54.preql duckdb tpcds.duckdb`

  ```text
  Resolution error in query54.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 42). The requested concepts split into 2 disconnected subgraphs: {dec1998_row.d.month_seq}; {local._customer_totals_cid, local._customer_totals_total_store_sales, ss.customer.address.county, ss.customer.address.state, ss.customer.id, ss.date.month_seq, ss.store.county, ss.store.state}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query54.preql duckdb tpcds.duckdb`

  ```text
  Resolution error in query54.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 40). The requested concepts split into 2 disconnected subgraphs: {local._customer_totals_cid, local._customer_totals_total_store_sales, ss.customer.address.county, ss.customer.address.state, ss.customer.id, ss.date.month_seq, ss.store.county, ss.store.state}; {window_months.high, window_months.low}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query54.preql duckdb tpcds.duckdb`

  ```text
  Syntax error in query54.preql: Undefined concept: window_start_end. Suggestions: ['window_start_end.end_ms', '_window_start_end_end_ms', 'window_start_end.start_ms']
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 8). The requested concepts split into 2 disconnected subgraphs: {local.item_cat_ext_list_price}; {ss.item.color, ss.item.current_price, ss.item.id, ss.item.product_name}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query75.preql`

  ```text
  Syntax error in query75.preql: Output column 'prev_qty' renames 'local.prev_qty' back to the name of an existing concept 'prev_qty' (defined at line 30) that 'local.prev_qty' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'prev_qty_out').
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy run --import raw.all_sales:all_sales select all_sales.channel, count(*) as cnt by all_sales.channel;`

  ```text
  Syntax error in stdin: Syntax [223]: `*` is not a valid argument — Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) — e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...es; select all_sales.channel, ??? count(*) as cnt by all_sales.c...
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.channel, count(all_sales.channel_dim_id) as cnt by all_sales.channel;`

  ```text
  Syntax error in stdin:  --> 2:66
    |
  2 | select all_sales.channel, count(all_sales.channel_dim_id) as cnt by all_sales.channel;
    |                                                                  ^---
    |
    = expected metadata, limit, order_by, where, having, or JOIN_TYPE
  Location:
  ..._sales.channel_dim_id) as cnt ??? by all_sales.channel;
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.channel, count(all_sales.channel_dim_id) as cnt group by 1;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ..._sales.channel_dim_id) as cnt ??? group by 1;
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.channel, count(all_sales.item.id) as row_cnt by all_sales.channel;`

  ```text
  Syntax error in stdin:  --> 2:63
    |
  2 | select all_sales.channel, count(all_sales.item.id) as row_cnt by all_sales.channel;
    |                                                               ^---
    |
    = expected metadata, limit, order_by, where, having, or JOIN_TYPE
  Location:
  ...all_sales.item.id) as row_cnt ??? by all_sales.channel;
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.channel, all_sales.is_returned, count(all_sales.item.id) as cnt by all_sales.channel, all_sales.is_returned;`

  ```text
  Syntax error in stdin:  --> 2:82
    |
  2 | select all_sales.channel, all_sales.is_returned, count(all_sales.item.id) as cnt by all_sales.channel, all_sales.is_returned;
    |                                                                                  ^---
    |
    = expected metadata, limit, order_by, where, having, or JOIN_TYPE
  Location:
  ...unt(all_sales.item.id) as cnt ??? by all_sales.channel, all_sale...
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.channel, count(*) as cnt where all_sales.row_one is null by all_sales.channel;`

  ```text
  Syntax error in stdin: Syntax [223]: `*` is not a valid argument — Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) — e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...es; select all_sales.channel, ??? count(*) as cnt where all_sale...
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.row_one, all_sales.channel, count(all_sales.item.id) as cnt by all_sales.row_one, all_sales.channel;`

  ```text
  Syntax error in stdin:  --> 2:78
    |
  2 | select all_sales.row_one, all_sales.channel, count(all_sales.item.id) as cnt by all_sales.row_one, all_sales.channel;
    |                                                                              ^---
    |
    = expected metadata, limit, order_by, where, having, or JOIN_TYPE
  Location:
  ...unt(all_sales.item.id) as cnt ??? by all_sales.row_one, all_sale...
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

# Channel label
auto channel_label <-
    case
        when all_sales.channel …s.return_channel_dim_text_id is not null),
        0
    ) as net_profit
order by
    channel_label nulls first,
    entity_id nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block — they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...09-06'::date)  # Rollup macro ??? def rollup_channel(metric) ->

  Write stats: received 2061 chars / 2061 bytes; tail: …'nnel_label nulls first,\\n    entity_id nulls first\\nlimit 100;'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

# Channel label
auto channel_label <-
    case
        when all_sales.channel …s.return_channel_dim_text_id is not null),
        0
    ) as net_profit
order by
    channel_label nulls first,
    entity_id nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [212]: A `by <grain>` clause must attach directly to an aggregate, not to an expression that wraps one (e.g. `coalesce(...)`, `round(...)`, arithmetic). Move the grain inside, next to the aggregate — write `coalesce(sum(x) by store.id, 0)` — or compute the grouped aggregate first and wrap it: `auto m <- sum(x) by store.id;` then `coalesce(m, 0)`.
  Location:
  ...) -> coalesce(sum(metric), 0) ??? by rollup channel_label, entit...

  Write stats: received 2121 chars / 2121 bytes; tail: …'nnel_label nulls first,\\n    entity_id nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.channel_dim_text_id, all_sales.return_channel_dim_text_id, count(*) as cnt where all_sales.chan…nd '2000-09-06'::date and all_sales.channel_dim_text_id != all_sales.return_channel_dim_text_id and all_sales.return_channel_dim_text_id is not null;`

  ```text
  Syntax error in stdin: Syntax [223]: `*` is not a valid argument — Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) — e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...s.return_channel_dim_text_id, ??? count(*) as cnt where all_sale...
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store;
import raw.catalog_sales as catalog;
import raw.web_sales as web;

# Global max o….sales_total) as total_sales
order by
    combined.last_name nulls first,
    combined.first_name nulls first,
    total_sales nulls first
limit 100;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
   --> 6:120
    |
  6 | auto store_cust_totals <- sum(store.quantity * store.sales_price ? store.customer.id is not null) by store.customer.id where store.date.year between 2000 and 2003;
    |                                                                                                                        ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, window_sql_over, or OVER_COMPONENT_REF
  Location:
  ...ot null) by store.customer.id ??? where store.date.year between

  Write stats: received 2235 chars / 2235 bytes; tail: …'rst_name nulls first,\\n    total_sales nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.date:d select distinct d.year, d.month_of_year where d.month_seq between 1188 and 1190 order by d.month_seq; duckdb tpcds.duckdb`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword — a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  import raw.date as d; select ??? distinct d.year, d.month_of_ye...
  ```
- `trilogy file write query64.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

# Compute per-ite…check the item colors
select distinct ss.item.color
where ss.item.color in ('purple', 'burlywood', 'indian', 'spring', 'floral', 'medium')
limit 20;
`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword — a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
   check the item colors select ??? distinct ss.item.color where s...

  Write stats: received 719 chars / 719 bytes; tail: …"rlywood', 'indian', 'spring', 'floral', 'medium')\\nlimit 20;\\n".
  ```
- `trilogy file write query64.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

# Merge item conc…,
  p.sale_line_count as cnt2
order by
  agg.product_name,
  agg.store_name,
  p.sale_line_count,
  agg.wholesale_cost_sum,
  p.wholesale_cost_sum
;
`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [220]: Filter or stray clause after a `join`? A query-scoped join `inner|left|full join <a> = <b>` takes only key equalities — to join on multiple keys, chain `= c` (one equivalence group) or separate distinct groups with `and` (`a = b and c = d`); STACK another `join` clause for a different join type. Note `and` joins KEY EQUALITIES only, not filters. Joins go right after the `select` list (preferred, SQL-like) or before `select`; the order is `where` -> `select` <cols> -> join(s) -> `having` -> `order by` -> `limit`. Filter input rows in `where` (before `select`); filter a joined or aggregated RESULT in `having` (select the field, hide it with a leading `--`). Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...n agg.store_zip = p.store_zip ??? inner join agg.item_id = p.ite...

  Write stats: received 2612 chars / 2612 bytes; tail: …'e_count,\\n  agg.wholesale_cost_sum,\\n  p.wholesale_cost_sum\\n;\\n'.
  ```
- `trilogy file write query75.preql --content import raw.all_sales as all_sales;

# Deduplicate at the per-line grain before aggregating
rowset deduped_lines as…qty,
    yearly.total_qty - prev_qty as qty_diff,
    yearly.total_amt - prev_amt as amt_diff
order by
    qty_diff asc,
    amt_diff asc
limit 100;
`

  ```text
  refused to write 'query75.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:1
    |
  4 | rowset deduped_lines as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...line grain before aggregating ??? rowset deduped_lines as select...

  Write stats: received 1736 chars / 1736 bytes; tail: …'diff\\norder by\\n    qty_diff asc,\\n    amt_diff asc\\nlimit 100;\\n'.
  ```

### `undefined-concept`

- `trilogy explore query02.preql`

  ```text
  Failed to parse query02.preql: (UndefinedConceptException(...), "Ambiguous reference 'with_future.day_of_week': matches ['with_future.daily_sales.day_of_week', 'with_future.future.day_of_week']. Qualify the full path to disambiguate.")
  ```
- `trilogy explore query02.preql --regex with_future`

  ```text
  Failed to parse query02.preql: (UndefinedConceptException(...), 'Undefined concept: with_future.future.future_sales.')
  ```
- `trilogy explore query02.preql`

  ```text
  Failed to parse query02.preql: (UndefinedConceptException(...), "Undefined concept: future.day_sales. Suggestions: ['daily_sales.day_sales']")
  ```
- `trilogy explore query02.preql`

  ```text
  Failed to parse query02.preql: (UndefinedConceptException(...), "Undefined concept: paired.future.day_sales (line 36, col 55, in SELECT). Suggestions: ['daily.day_sales', 'future.day_sales', 'paired.daily.day_sales', 'paired.future_sales', '_paired_future_sales']")
  ```

### `syntax-missing-alias`

- `trilogy run --import raw.item:item select item.category, item.class, count(item.id) where item.category = 'Women' and item.class = 'maternity'; duckdb tpcds.duckdb`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(item.id) as id_count`
  Location:
  ...y, item.class, count(item.id) ??? where item.category = 'Women'
  ```

### `cli-misuse`

- `trilogy explore raw`

  ```text
  Invalid value for 'PATH': File 'raw' is a directory.
  ```
