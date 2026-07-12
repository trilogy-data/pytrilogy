# Trilogy failure analysis — 20260712-151850

- Run `20260712-151850` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 206 | failed: 25 (12%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 18 | 72% |
| `syntax-parse` | 6 | 24% |
| `cli-misuse` | 1 | 4% |

## Detail

### `other`

- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 72 column 12 (char 2424). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 37 column 12 (char 1728). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read answer_3979964698.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_2910545909.preql`

  ```text
  Syntax error in answer_2910545909.preql: Undefined concept: ticket_number (line 26, col 3, in ORDER BY). Suggestions: ['ss.ticket_number']
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read answer_751385098.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_374591292.preql`

  ```text
  Resolution error in answer_374591292.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 42). The requested concepts split into 2 disconnected subgraphs: {best_performers.rank_num, best_performers.ss.item.product_name}; {worst_performers.ss.item.product_name}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3560698360.preql duckdb`

  ```text
  Syntax error in answer_3560698360.preql: None
  ```
- `trilogy run answer_3560698360.preql duckdb --debug`

  ```text
  Syntax error in answer_3560698360.preql: None
  ```
- `trilogy run answer_3560698360.preql duckdb`

  ```text
  Syntax error in answer_3560698360.preql: None
  ```
- `trilogy run answer_3560698360.preql`

  ```text
  Syntax error in answer_3560698360.preql: None
  ```
- `trilogy run test_simple.preql`

  ```text
  Syntax error in test_simple.preql: None
  ```

### `syntax-parse`

- `trilogy file write answer_747155668.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sales as cs;

with combined …ined.total_ext_sales_price
order by combined.channel, combined.missing_ref, combined.year, combined.quarter, combined.category
nulls first
limit 100;`

  ```text
  refused to write 'answer_747155668.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:5
    |
  7 |     auto store_missing_flag <- sum(case when ss.store.sk is null then 1 else 0 end) by ss.ticket_number, ss.item.sk;
    |     ^---
    |
    = expected select_statement
  Location:
  ...th combined as union(   (     ??? auto store_missing_flag <- sum...

  Write stats: received 2101 chars / 2101 bytes; tail: …', combined.quarter, combined.category\\nnulls first\\nlimit 100;'.
  ```
- `trilogy file write answer_747155668.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sales as cs;

# Stage 1: def…ined.total_ext_sales_price
order by combined.channel, combined.missing_ref, combined.year, combined.quarter, combined.category
nulls first
limit 100;`

  ```text
  refused to write 'answer_747155668.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ..._count, total_ext_sales_price) ??? select     combined.channel,

  Write stats: received 2171 chars / 2171 bytes; tail: …', combined.quarter, combined.category\\nnulls first\\nlimit 100;'.
  ```
- `trilogy file write answer_1835050598.preql --content import raw.catalog_sales as cs;

# Project dimension properties to the line grain first
auto row_birth_y…,
         cs.item.id nulls first,
         --grp_country nulls first,
         --grp_state nulls first,
         --grp_county nulls first
limit 100;`

  ```text
  refused to write 'answer_1835050598.preql': not syntactically valid Trilogy.

  Parse error:
    --> 34:11
     |
  34 |          --grp_country nulls first,
     |           ^---
     |
     = expected access_chain
  Location:
  ...tem.id nulls first,          - ??? -grp_country nulls first,

  Write stats: received 1748 chars / 1748 bytes; tail: …'te nulls first,\\n         --grp_county nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.store_sales:ss select ss.date.month_of_year, count(ss.ticket_number) as cnt where ss.date.year = 2000 and ss.store.sk is not null group by ss.date.month_of_year order by ss.date.month_of_year limit 20;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...0 and ss.store.sk is not null ??? group by ss.date.month_of_year...
  ```
- `trilogy file write answer_2910545909.preql --content import raw.store_sales as ss;

where
  ss.date.day_name = 'Monday'
  and ss.date.year between 1999 and 2…  last_name nulls first,
  first_name nulls first,
  city_display nulls first,
  total_net_profit nulls first,
  ticket_number nulls last

limit 100;`

  ```text
  refused to write 'answer_2910545909.preql': not syntactically valid Trilogy.

  Parse error:
    --> 17:3
     |
  17 |   -- hide the grouping keys:
     |   ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...profit) as total_net_profit   ??? -- hide the grouping keys:   -...

  Write stats: received 788 chars / 788 bytes; tail: …'t_profit nulls first,\\n  ticket_number nulls last\\n\\nlimit 100;'.
  ```
- `trilogy run --import raw.store_sales:ss with tr as where ss.store.sk = 1 and ss.sale_address.sk is null select avg(ss.net_profit) as threshold_np; select tr.threshold_np from tr;`

  ```text
  Syntax error in stdin: Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...ld_np; select tr.threshold_np ??? from tr;
  ```

### `cli-misuse`

- `trilogy explore raw/`

  ```text
  Invalid value for 'PATH': File 'raw/' is a directory.
  ```
