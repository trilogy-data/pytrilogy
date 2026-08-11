# Trilogy failure analysis — 20260811-015537

- Run `20260811-015536_enriched_aggregates` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 278 | failed: 23 (8%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `disabled-tool` | 6 | 26% |
| `undefined-concept` | 5 | 22% |
| `other` | 4 | 17% |
| `cli-misuse` | 4 | 17% |
| `syntax-parse` | 4 | 17% |

## Detail

### `disabled-tool`

- `trilogy file read raw/_q04_agent_rowset_union_join.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read diag_2524943990.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `undefined-concept`

- `trilogy run answer_3705756794.preql`

  ```text
  Syntax error in answer_3705756794.preql: Undefined concept: s.channel_label. Suggestions: ['s.channel', 's.channel_dim_id', 's.channel_dim_text_id', 'channel_label']
  ```
- `trilogy run answer_3697440276.preql`

  ```text
  Syntax error in answer_3697440276.preql: 2 undefined concept references; fix all before re-running:
    - first_name (line 43, col 10, in ORDER BY); did you mean: store_2002.first_name, ss.customer.first_name, ws.ship_customer.first_name, ss.return_customer.first_name, ws.return_customer.first_name, ws.billing_customer.first_name?
    - last_name (line 44, col 10, in ORDER BY); did you mean: store_2002.last_name, ss.customer.last_name, ws.ship_customer.last_name, ss.return_customer.last_name, ws.return_customer.last_name, ws.billing_customer.last_name?
  ```
- `trilogy run answer_2524943990.preql`

  ```text
  Syntax error in answer_2524943990.preql: Undefined concept: catalog_sales.warehouse.sk. Suggestions: ['catalog_page.sk', 'warehouse.sk', 'return_warehouse.sk', 'return_catalog_page.sk', 'ship_customer.first_sales_date.sk', 'sale_date.sk']
  ```
- `trilogy run answer_2524943990.preql`

  ```text
  Syntax error in answer_2524943990.preql: Undefined concept: catalog_sales.order_number. Suggestions: ['order_number']
  ```
- `trilogy run answer_2524943990.preql`

  ```text
  Syntax error in answer_2524943990.preql: Undefined concept: warehouse.sk. Suggestions: ['cs.warehouse.sk', 'cs.return_warehouse.sk', 'cs.return_customer.sk', 'cs.return_date.sk', 'cs.return_time.sk', 'cs.return_ship_mode.sk']
  ```

### `other`

- `trilogy run answer_507046194.preql`

  ```text
  Resolution error in answer_507046194.preql: WHERE input(s) ['ss.return_store.state'] cannot be related to the query outputs ['ss.return_customer.id', 'ss.return_customer.sk', 'ss.return_store.sk']: no join or merge connects the filter's source to any output-producing source. Add a join/merge relating them, or select a concept from the filter's model.
  ```
- `trilogy agent-info syntax example union-join`

  ```text
  Unknown syntax example: 'union-join'

  Available Trilogy syntax examples - print one with `trilogy agent-info syntax example <name>`:

  - `python-datasource` - run a local Python script as a datasource: wrap a function in `trilogy.io.run`, which writes the Arrow IPC stream to stdout for you from a table, dataframe, or list of dicts; declare concepts, map script columns in `datasource (...)`, use `grain (...) file `path.py`;`, then reference locally declared concepts WITHOUT the datasource name as a prefix
  - `query-structure` - the clause order of a query (`where` -> `select` <cols> -> join(s) -
  …
  subtotal/total rows and to sort by level
  - `rank-over-rollup` - rank rollup subtotals/leaves with a SINGLE `rank(a,b) over (partition by level, parent ...)` - not separate ranks per level
  - `staged-membership` - compute a membership set in a `rowset` (keys meeting a count/HAVING), then filter the main query with `<key> in <rowset>.<col>`
  - `correlated-exists-via-grouped-counts` - translate `EXISTS other` / `NOT EXISTS other matching` over the same model into two `count(...) by <grain>` compared in `where` (`> 1` = another exists, `= 1` = no other matches) - pin the correlation grain with `by`
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 1 column 65 (char 64). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run /dev/stdin`

  ```text
  No dialect specified. Provide dialect as argument or set engine.dialect in config file.
  ```

### `cli-misuse`

- `trilogy solve raw/store_sales.preql`

  ```text
  No such command 'solve'.
  ```
- `trilogy file cat raw/all_sales.preql`

  ```text
  No such command 'cat'.
  ```
- `trilogy raw catalog_store_returns.preql`

  ```text
  No such command 'raw'.
  ```
- `trilogy file cat raw/catalog_store_returns.preql`

  ```text
  No such command 'cat'.
  ```

### `syntax-parse`

- `trilogy file write answer_883027685.preql`

  ```text
  refused to write 'answer_883027685.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...nt(ss.line_item) as cnt,
       ??? subset join cat_avg.category =...
  ```
- `trilogy file write verify.preql`

  ```text
  refused to write 'verify.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...pt.line_items) as line_items
   ??? group by kept.channel;
  ```
- `trilogy file write diag_2524943990.preql`

  ```text
  refused to write 'diag_2524943990.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...nt(base.order_number) as n
     ??? group by base.in_cat, base.in_...
  ```
- `trilogy file write diag_2524943990.preql`

  ```text
  refused to write 'diag_2524943990.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [212]: A `by <grain>` clause must attach directly to an aggregate, not to an expression that wraps one (e.g. `coalesce(...)`, `round(...)`, arithmetic). Move the grain inside, next to the aggregate - write `coalesce(sum(x) by store.id, 0)` - or compute the grouped aggregate first and wrap it: `auto m <- sum(x) by store.id;` then `coalesce(m, 0)`.
  Location:
  ...counter ? cs.is_returned), 0) ??? by cs.order_number as ret_line...
  ```
