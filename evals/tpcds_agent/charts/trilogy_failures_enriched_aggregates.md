# Trilogy failure analysis — 20260810-200846

- Run `20260810-200843_enriched_aggregates` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 255 | failed: 12 (5%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `disabled-tool` | 4 | 33% |
| `syntax-parse` | 3 | 25% |
| `other` | 2 | 17% |
| `undefined-concept` | 1 | 8% |
| `cli-misuse` | 1 | 8% |
| `file-not-found` | 1 | 8% |

## Detail

### `disabled-tool`

- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read answer_2524943990.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy run --explore-db`

  ```text
  Syntax error in stdin:  --> 1:3
    |
  1 | --explore-db;
    |   ^---
    |
    = expected PURPOSE, PROPERTY, UNIQUE, or AUTO
  Location:
  -- ??? explore-db;
  ```
- `trilogy file write tmp_explore.preql`

  ```text
  refused to write 'tmp_explore.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a NON-NULL GRAIN KEY: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). It MUST be a key, and one that is not nullable: `count(x)` skips rows where `x` is NULL, so counting a nullable property (a name, a date, any optional field) silently undercounts. When the grain takes SEVERAL keys, name them with `grain(...)`: `count(grain(order_id, item.id))` counts order+item combinations, and `count_distinct(grain(first_name, last_name, sale_date))` counts distinct combinations - `grain()` is never NULL, so combinations with a missing member still count. For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...d, s.ext_sales, s.net_profit, ??? count(*) over() as total limit...
  ```
- `trilogy file write tmp_verify.preql`

  ```text
  refused to write 'tmp_verify.preql': not syntactically valid Trilogy.

  Parse error:
   --> 9:1
    |
  9 | by *;
    | ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...s_price),0) as all_ext_sales
   ??? by *;
  ```

### `other`

- `trilogy run tmp_explore.preql`

  ```text
  trilogy error: subprocess timed out after 600s.
  ```
- `trilogy agent-info syntax example intersect`

  ```text
  Unknown syntax example: 'intersect'

  Available Trilogy syntax examples - print one with `trilogy agent-info syntax example <name>`:

  - `python-datasource` - run a local Python script as a datasource: wrap a function in `trilogy.io.run`, which writes the Arrow IPC stream to stdout for you from a table, dataframe, or list of dicts; declare concepts, map script columns in `datasource (...)`, use `grain (...) file `path.py`;`, then reference locally declared concepts WITHOUT the datasource name as a prefix
  - `query-structure` - the clause order of a query (`where` -> `select` <cols> -> join(s) ->
  …
  subtotal/total rows and to sort by level
  - `rank-over-rollup` - rank rollup subtotals/leaves with a SINGLE `rank(a,b) over (partition by level, parent ...)` - not separate ranks per level
  - `staged-membership` - compute a membership set in a `rowset` (keys meeting a count/HAVING), then filter the main query with `<key> in <rowset>.<col>`
  - `correlated-exists-via-grouped-counts` - translate `EXISTS other` / `NOT EXISTS other matching` over the same model into two `count(...) by <grain>` compared in `where` (`> 1` = another exists, `= 1` = no other matches) - pin the correlation grain with `by`
  ```

### `undefined-concept`

- `trilogy run answer_3863442186.preql`

  ```text
  Syntax error in answer_3863442186.preql: Undefined concept: store_sales.customer.customer.id. Suggestions: ['store_sales.customer.id', 'store_sales.customer.last_name', 'store_sales.customer.sk', 'store_sales.return_customer.id', 'store_sales.customer.first_shipto_date.id', 'store_sales.customer.current_address.id']
  ```

### `cli-misuse`

- `trilogy explore`

  ```text
  Missing argument 'PATH'.
  ```

### `file-not-found`

- `trilogy file list ./root --recursive`

  ```text
  No such path: ./root
  ```
