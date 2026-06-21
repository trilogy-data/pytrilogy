# Trilogy failure analysis — 20260613-041233

- Run `20260613-041229_ingest` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 1714 | failed: 320 (19%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 181 | 57% |
| `syntax-parse` | 72 | 22% |
| `undefined-concept` | 46 | 14% |
| `syntax-missing-alias` | 8 | 2% |
| `join-resolution` | 7 | 2% |
| `file-not-found` | 4 | 1% |
| `cli-misuse` | 2 | 1% |

## Detail

### `other`

- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query01.preql`

  ```text

  ```
- `trilogy run query02.preql`

  ```text

  ```
- `trilogy run query02.preql`

  ```text

  ```
- `trilogy run query02.preql`

  ```text

  ```
- `trilogy run query02.preql`

  ```text

  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query05.preql`

  ```text

  ```
- `trilogy file read query05.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query06.preql`

  ```text

  ```
- `trilogy file read query06.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file write --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;
import raw.item as item;

# Per-line s….quantity * ss.list_price ? ss.date_dim.year between 1999 and 2001
) by *;

# Hmm, that only gets store_sales. We need ALL channels.
 ./query14.preql`

  ```text
  trilogy file write: `--content` takes a SINGLE string argument. Your args list put 2 separate tokens after --content (treating it like a shell command). In a tool call, pass the entire file body as one string element after --content, with newlines embedded literally — e.g.
    {"args": ["file", "write", "query70.preql", "--content", "import raw.store_sales as store_sales;\n\nselect ..."]}
  Alternatively use `--escapes` with a single-line `\n`-escaped string.
  ```
- `trilogy run ./query14.preql`

  ```text

  ```
- `trilogy run ./query14.preql`

  ```text

  ```
- `trilogy run ./query14.preql`

  ```text

  ```
- `trilogy run ./query14.preql`

  ```text

  ```
- `trilogy run ./query14.preql`

  ```text

  ```
- `trilogy run ./query14.preql`

  ```text

  ```
- `trilogy run ./query14.preql`

  ```text

  ```
- `trilogy run ./query14.preql`

  ```text

  ```
- `trilogy run ./query14.preql`

  ```text

  ```
- `trilogy run ./query14.preql`

  ```text

  ```
- `trilogy run ./query14.preql`

  ```text

  ```
- `trilogy run ./query14.preql`

  ```text

  ```
- `trilogy run ./query14.preql`

  ```text

  ```
- `trilogy run query16.preql`

  ```text

  ```
- `trilogy run query16.preql`

  ```text

  ```
- `trilogy run query18.preql`

  ```text

  ```
- `trilogy file read query18.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query23.preql`

  ```text

  ```
- `trilogy run query23.preql`

  ```text

  ```
- `trilogy run query24.preql`

  ```text

  ```
- `trilogy run query24.preql`

  ```text

  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query30.preql`

  ```text

  ```
- `trilogy file read query30.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query31.preql`

  ```text

  ```
- `trilogy run query31.preql`

  ```text

  ```
- `trilogy run query31.preql`

  ```text

  ```
- `trilogy file read query31.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query34.preql`

  ```text

  ```
- `trilogy run query34.preql`

  ```text

  ```
- `trilogy file read query35.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy run test_best3.preql`

  ```text

  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy file read query44.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query45.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query49.preql`

  ```text

  ```
- `trilogy run query49.preql`

  ```text

  ```
- `trilogy run query49.preql`

  ```text

  ```
- `trilogy run query49.preql`

  ```text

  ```
- `trilogy run query49.preql`

  ```text

  ```
- `trilogy file read query49.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query56.preql`

  ```text

  ```
- `trilogy run query56.preql`

  ```text

  ```
- `trilogy run query56.preql`

  ```text

  ```
- `trilogy explore query56.preql`

  ```text
  Cannot render type <class 'trilogy.core.models.author.UnionSelectLineage'>
  ```
- `trilogy run query56.preql`

  ```text

  ```
- `trilogy run query56.preql`

  ```text

  ```
- `trilogy file read query56.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read trilogy.toml`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query60.preql`

  ```text

  ```
- `trilogy run query62_check3.preql`

  ```text

  ```
- `trilogy run query62_check3.preql`

  ```text

  ```
- `trilogy database describe web_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read query62.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query64.preql`

  ```text

  ```
- `trilogy run query64.preql`

  ```text

  ```
- `trilogy run query64.preql`

  ```text

  ```
- `trilogy run query64.preql`

  ```text

  ```
- `trilogy run query64.preql`

  ```text

  ```
- `trilogy run query64.preql`

  ```text

  ```
- `trilogy run query64.preql`

  ```text

  ```
- `trilogy run query64.preql`

  ```text

  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query65.preql`

  ```text

  ```
- `trilogy run query65.preql`

  ```text

  ```
- `trilogy file read query65.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run query66.preql`

  ```text

  ```
- `trilogy run query66.preql`

  ```text

  ```
- `trilogy run query66.preql`

  ```text

  ```
- `trilogy run --import raw.warehouse:w --import raw.date_dim:dd select w.warehouse_name, dd.moy order by w.warehouse_name asc, dd.moy asc limit 48;`

  ```text

  ```
- `trilogy run query66.preql`

  ```text

  ```
- `trilogy run query66.preql`

  ```text

  ```
- `trilogy run query66.preql`

  ```text

  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run query73.preql`

  ```text

  ```
- `trilogy file read query73.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query75.preql`

  ```text

  ```
- `trilogy run query75.preql`

  ```text

  ```
- `trilogy run query75.preql`

  ```text

  ```
- `trilogy run query75.preql`

  ```text

  ```
- `trilogy run query75.preql`

  ```text

  ```
- `trilogy run query75.preql`

  ```text

  ```
- `trilogy run query75.preql`

  ```text

  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy agent-info syntax example rollup-grouping-sets`

  ```text

  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 1 column 2207 (char 2206). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query77.preql`

  ```text

  ```
- `trilogy run query77.preql duckdb`

  ```text

  ```
- `trilogy run query77.preql duckdb`

  ```text

  ```
- `trilogy run query77.preql duckdb`

  ```text

  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query78.preql`

  ```text

  ```
- `trilogy run query78.preql`

  ```text

  ```
- `trilogy run query78.preql`

  ```text

  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query80.preql`

  ```text

  ```
- `trilogy run query80.preql`

  ```text

  ```
- `trilogy run query80.preql`

  ```text

  ```
- `trilogy run query80.preql`

  ```text

  ```
- `trilogy run query80.preql`

  ```text

  ```
- `trilogy run query80.preql`

  ```text

  ```
- `trilogy run query80.preql`

  ```text

  ```
- `trilogy run query80.preql`

  ```text

  ```
- `trilogy run query80.preql`

  ```text

  ```
- `trilogy run query80.preql`

  ```text

  ```
- `trilogy run query80.preql`

  ```text

  ```
- `trilogy run query80.preql`

  ```text

  ```
- `trilogy run query80.preql`

  ```text

  ```
- `trilogy run query80.preql`

  ```text

  ```
- `trilogy run query80.preql`

  ```text

  ```
- `trilogy run query81.preql`

  ```text

  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query83.preql`

  ```text

  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run --import raw.store_returns:sr --import raw.customer:cust select sr.customer_demographics.demo_sk, cust.customer_id limit 5;`

  ```text

  ```
- `trilogy run test_left4.preql`

  ```text

  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe time_dim`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run query88.preql`

  ```text

  ```
- `trilogy run query88.preql`

  ```text

  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query89.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query90.preql`

  ```text

  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query93.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query94.preql`

  ```text

  ```
- `trilogy run query94.preql`

  ```text

  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read query94.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query99.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy run --import raw/date_dim:date_dim select date_dim.week_seq, date_dim.year, date_dim.dow, count(date_dim.date_sk) as cnt group by 1,2,3 order by 1,2,3 limit 30;`

  ```text

  ```
- `trilogy file write query02.preql --content # Combine all web and catalog 2001 sales by (week_seq, year, dow)
with all_sales as union(
    (import raw.catalog…seq, year, dow, sales_price);

select all_sales.week_seq, all_sales.year, all_sales.dow, sum(all_sales.sales_price) as total
order by 1,2,3
limit 20;`

  ```text

  ```
- `trilogy run --import raw/catalog_sales:cs --import raw/web_sales:ws where cs.sold_date.year in (2000, 2001, 2002) select cs.sold_date.week_seq, cs.sold_date.…e) as total left join cs.sold_date.date_sk = ws.sold_date.date_sk where ws.sold_date.year in (2000, 2001, 2002) group by 1,2,3 order by 1,2 limit 15;`

  ```text

  ```
- `trilogy file write query02.preql --content import raw.catalog_sales as cs;
import raw.web_sales as ws;
import raw.date_dim as dd;

# Daily sales union of cat…  dd.date_sk,
    daily_totals.tot
;

select sales_2001.dd.week_seq, sales_2001.dd.dow, sales_2001.dd.date_sk, sales_2001.tot
order by 1,2
limit 20;
`

  ```text

  ```
- `trilogy run --import raw/date_dim:dd1 --import raw/date_dim:dd2 where dd1.date between '2001-01-02'::date and '2001-01-08'::date select dd1.date, dd1.date_sk, dd1.same_day_ly, dd2.date_sk, dd2.date from dd1 left join dd1.same_day_ly = dd2.date_sk limit 7;`

  ```text

  ```
- `trilogy file write query05.preql -e -c import raw.store_sales as store_sales;
import raw.store_returns as store_returns;
import raw.catalog_sales as catalog_…turns,
  coalesce(sum(combined.net_profit), 0) as net_profit
order by combined.channel asc nulls last, combined.entity_id asc nulls first
limit 100;
`

  ```text

  ```
- `trilogy file write query06.preql --content import raw.store_sales as ss;

# Average current price of distinct items per category (each item once)
auto distin…ull
having
    line_item_count >= 10
order by
    line_item_count asc nulls first,
    ss.customer.customer_address.state asc nulls first
limit 100;
`

  ```text

  ```
- `trilogy file write ./query14.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Per-combo counts pe… channels in 1999-2001
auto overall_avg_sale_val <- avg(
    (ss.quantity * ss.list_price ? ss.date_dim.year between 1999 and 2001)
) by *;

wait...
`

  ```text

  ```
- `trilogy run --import raw.store_sales:ss select count(ss.item.item_sk) as cnt, ss.item.brand_id, ss.item.class_id, ss.item.category_id group by 2,3,4 limit 10;`

  ```text

  ```
- `trilogy run --import raw.store_sales:ss --import raw.catalog_sales:cs --import raw.web_sales:ws select count(*) as cnt, 'store' as ch where ss.date_dim.year = 2001 and ss.date_dim.moy = 11;`

  ```text

  ```
- `trilogy run --import raw.store_sales:ss select sum(ss.quantity * ss.list_price) as tot, count(*) as cnt where ss.date_dim.year = 2001 and ss.date_dim.moy = 11;`

  ```text

  ```
- `trilogy run --import raw.store_sales:ss --import raw.catalog_sales:cs --import raw.web_sales:ws select count(ss.item.item_sk) as cnt where ss.date_dim.year =…lect concat(ss2.item.brand_id::string,'-',ss2.item.class_id::string,'-',ss2.item.category_id::string) where ss2.date_dim.year between 1999 and 2001);`

  ```text

  ```
- `trilogy run --import raw.store_sales:ss --import raw.catalog_sales:cs --import raw.web_sales:ws select count(ss.item.item_sk) as cnt where ss.date_dim.year = 2001 and ss.date_dim.moy = 11 and ss.item.brand_id in (select ss2.item.brand_id where ss2.date_dim.year between 1999 and 2001);`

  ```text

  ```
- `trilogy file write query23.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…combined_sales.sales_amt) as total_sales
order by
  last_name asc nulls first,
  first_name asc nulls first,
  total_sales asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query27.preql --content import raw.store_sales as store_sales;

where
    store_sales.date_dim.year = 2002
    and store_sales.store.state…re_sales.sales_price) as avg_unit_price
order by
    store_sales.item.item_id asc nulls first,
    store_sales.store.state asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query31.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;

# Store sales extended sales price by county and quarte…nullif(store_q(2), 0) as store_q3_over_q2
having
    web_q2_over_q1 > store_q2_over_q1
    and web_q3_over_q2 > store_q3_over_q2
order by county asc;`

  ```text

  ```
- `trilogy file write query38.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# For each sales channe…irst_name, combined.sale_date)
    ) as unique_combinations_in_all_three
where
    in_store > 0
    and in_catalog > 0
    and in_web > 0
limit 100;
`

  ```text

  ```
- `trilogy file write query41.preql --content import raw.item as item;

# Items matching any of the eight attribute profiles
auto profile_matches <- item.item_s… 500
  and item.manufact in manuf_with_profile
  and item.manufact in manuf_shared
select
  item.product_name
order by
  item.product_name
limit 100;`

  ```text

  ```
- `trilogy run --import raw.store_sales:s select s.store.store_sk, count(s.ticket_number) as cnt, count(s.customer_address.address_sk ? s.customer_address.address_sk is null) as null_addr_cnt group by s.store.store_sk limit 20;`

  ```text

  ```
- `trilogy file write query44.preql --content import raw.store_sales as ss;

# Average net profit at store 1 where address_sk is null (global)
auto avg_null_add…me as worst_performer
inner join best.pair_rank = worst.pair_rank
order by
    rank asc,
    best_performer desc,
    worst_performer desc
limit 100;`

  ```text

  ```
- `trilogy file write query54.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Find month_seq for De…eq + 3
    and ss.store.county = ss.customer.customer_address.county
    and ss.store.state = ss.customer.customer_address.state
group by customer_sk`

  ```text

  ```
- `trilogy file write query56.preql --content import raw.item as item;

# Qualifying items: those with color in ('slate', 'blanched', 'burnished')
with qualifyi…lifying_items.item_id as item_code, combined_totals.total
order by combined_totals.total nulls first, qualifying_items.item_id nulls first
limit 100;`

  ```text

  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.item.item_id, sum(store_sales.ext_sales_price) as store_total where store_sales.date_dim.week_seq = 5218 group by store_sales.item.item_id limit 5;`

  ```text

  ```
- `trilogy run --import raw.store_sales:store_sales --import raw.catalog_sales:catalog_sales select store_sales.item.item_id, sum(store_sales.ext_sales_price) a…_sales.date_dim.week_seq = 5218 and catalog_sales.sold_date.week_seq = 5218 inner join store_sales.item.item_id = catalog_sales.item.item_id limit 5;`

  ```text

  ```
- `trilogy file write query59.preql -e -c import raw.store_sales as store_sales;

# Filtered aggregate macros for each day of week
def sun_price(p) -> sum(p ? s…seq = next_year.week_seq - 52
order by this_year.store_name nulls first, this_year.store_code nulls first, this_year.week_seq nulls first
limit 100;
`

  ```text

  ```
- `trilogy file write query59.preql -e -c import raw.store_sales as store_sales;

# Filtered aggregate macros for each day of week
def sun_price(p) -> sum(p ? s…q = (next_year.week_seq - 52)
order by this_year.store_name nulls first, this_year.store_code nulls first, this_year.week_seq nulls first
limit 100;
`

  ```text

  ```
- `trilogy file write query62.preql --content import raw.web_sales as web_sales;

# Only web sales with ship date in 2000, and all three of warehouse/ship_mode/…as bucket_gt_120
order by
    warehouse_name_prefix asc nulls first,
    ship_mode_type asc nulls first,
    web_site_name asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query62_check3.preql --content import raw.web_sales as web_sales;

select
    web_sales.ship_date.year as ship_year,
    count(web_sales.order_number) as --line_items
having --line_items > 0
order by
    web_sales.ship_date.year asc nulls first
limit 20;`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r…   sum(ss_matched.list_price) as list_price_sum,
    sum(ss_matched.coupon_amt) as coupon_amt_sum
where
    ss_matched.item_id in q_item_sk
limit 10;`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r…<- cs.item.item_sk ? cat_price_by_item > 2 * cat_refund_by_item;

select
    cs.item.item_sk,
    cat_price_by_item,
    cat_refund_by_item
limit 10;`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r…atus != ss.customer_demographics.marital_status
inner join ss.ticket_number = sr.ticket_number
inner join ss.item.item_sk = sr.item.item_sk
limit 10;`

  ```text

  ```
- `trilogy file write query66.preql --content import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Predefine all 12 months to ensure zero-fill
with al….warehouse_name), 0) as monthly_net
order by distinct_warehouses.warehouse_name asc nulls first, distinct_warehouses.year asc nulls first
limit 100;
`

  ```text

  ```
- `trilogy file write query66.preql --content import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Generate all months 1-12 using generate_array and u… = all_months.month),
      0) as monthly_net
order by warehouse_list.warehouse_name asc nulls first, warehouse_list.year asc nulls first
limit 100;
`

  ```text

  ```
- `trilogy run --import raw.warehouse:w select unnest(generate_array(1, 12)) as month;`

  ```text

  ```
- `trilogy run --import raw.warehouse:w select unnest(generate_array(1,12)) as month;`

  ```text

  ```
- `trilogy run --import raw.warehouse:w select unnest(generate_array(1, 12)) as month limit 12;`

  ```text

  ```
- `trilogy run --import raw.warehouse:w select unnest(generate_array(1, 12)) as month;`

  ```text

  ```
- `trilogy run --import raw.warehouse:w select generate_array(1, 12) as months;`

  ```text

  ```
- `trilogy run --import raw.warehouse:w select array(1, 2, 3) as arr;`

  ```text

  ```
- `trilogy run --import raw.warehouse:w select unnest(generate_array(1, 12)) as month;`

  ```text

  ```
- `trilogy run --import raw.warehouse:w select unnest(generate_array(1; 12)) as month;`

  ```text

  ```
- `trilogy run --import raw.warehouse:w select unnest(generate_array(1)) as month;`

  ```text

  ```
- `trilogy run --import raw.warehouse:w select generate_array(1) as month;`

  ```text

  ```
- `trilogy run --import raw.warehouse:w select generate_array(1,12) as month;`

  ```text

  ```
- `trilogy run --import raw.warehouse:w select length('test') as l;`

  ```text

  ```
- `trilogy run --import raw.warehouse:w select array_sort(greatest(1, 2, 3), true) as val;`

  ```text

  ```
- `trilogy run --import raw.warehouse:w select array_sort(greatest(1, 2, 3), 'asc') as val;`

  ```text

  ```
- `trilogy run --import raw.warehouse:w select array_sort(array(1, 2, 3), 'asc') as val;`

  ```text

  ```
- `trilogy run --import raw.warehouse:w select unnest(generate_array(1, 12)) as names;`

  ```text

  ```
- `trilogy run --import raw.customer_address:ca select ca.county, ca.state, count(ca.address_sk) as cnt group by ca.county, ca.state limit 20;`

  ```text

  ```
- `trilogy run --import raw.store_sales:ss select ss.ticket_number, ss.customer.last_name, ss.customer.first_name, ss.customer.customer_sk, count(ss.item.item_s….household_demographics.vehicle_count > 1 group by ss.ticket_number, ss.customer.last_name, ss.customer.first_name, ss.customer.customer_sk limit 20;`

  ```text

  ```
- `trilogy file write query75.preql --content # Query 75

import raw.store_sales as ss;

# Store channel: sales + returns per line, Books only
rowset store_chan…ct store_channel.yr, store_channel.bid, store_channel.cid, store_channel.catid, store_channel.mid, store_channel.s_qty, store_channel.s_amt limit 10;`

  ```text

  ```
- `trilogy file write query75.preql --content # Query 75

with combined as union(
    (
        import raw.store_sales as ss;
        import raw.store_returns a…elect combined.yr, combined.bid, combined.cid, combined.catid, combined.mid, combined.s_qty, combined.s_amt, combined.r_qty, combined.r_amt limit 10;`

  ```text

  ```
- `trilogy file write query75.preql --content # Query 75 - test with rowset
import raw.store_sales as ss;
import raw.store_returns as sr;

rowtest store_channel…t_number
left join ss.item.item_sk = sr.item.item_sk
;

select store_channel.yr, store_channel.bid, store_channel.s_qty, store_channel.r_qty limit 5;`

  ```text

  ```
- `trilogy file write query75.preql --content # Quick check on order direction
auto x <- 5;
select 2 as a, 1 as b union all select 1 as a, 2 as b order by a, b;`

  ```text

  ```
- `trilogy file write query77.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r… as returns,
    sum(all_levels.profit) as profit
order by all_levels.channel nulls first, all_levels.outlet_id nulls first, returns desc
limit 100;
`

  ```text

  ```
- `trilogy file write query77.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r…_levels.returns,
    all_levels.profit
order by all_levels.channel nulls first, all_levels.outlet_id nulls first, all_levels.returns desc
limit 100;
`

  ```text

  ```
- `trilogy file write query77.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r…rns,
    sum(store_profit) by rollup channel, store_outlet_id as profit
order by channel nulls first, outlet_id nulls first, returns desc
limit 100;
`

  ```text

  ```
- `trilogy file write query77.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r…_levels.returns,
    all_levels.profit
order by all_levels.channel nulls first, all_levels.outlet_id nulls first, all_levels.returns desc
limit 100;
`

  ```text

  ```
- `trilogy file write query77.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r….store_sk as outlet_id,
    ss_sales as sales,
    ss_returns as returns,
    ss_profit as profit
order by channel, outlet_id nulls first
limit 100;
`

  ```text

  ```
- `trilogy file write query77.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r…   @r_returns() as returns,
    @r_profit() as profit
order by combined.channel nulls first, combined.outlet_id nulls first, returns desc
limit 100;
`

  ```text

  ```
- `trilogy file write query77.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r…s,
    all_rows.returns,
    all_rows.profit
order by all_rows.channel nulls first, all_rows.outlet_id nulls first, all_rows.returns desc
limit 100;
`

  ```text

  ```
- `trilogy file write query77.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r… all_returns as returns,
    all_profit as profit
order by per_outlet.channel nulls first, per_outlet.outlet_id nulls first, returns desc
limit 100;
`

  ```text

  ```
- `trilogy file write query87.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Distinct (last_name, …= web_tuples.first_name
left join store_tuples.sale_date = web_tuples.sale_date
where cat_tuples.last_name is null and web_tuples.last_name is null
;`

  ```text

  ```
- `trilogy file write test_left5.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

rowset store_tuples …_combined.last_name
inner join store_tuples.first_name = cat_web_combined.first_name
inner join store_tuples.sale_date = cat_web_combined.sale_date
;`

  ```text

  ```
- `trilogy file write test_struct.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# For anti-join: co…as key)
) -> (key);

where ss.date_dim.year = 2000
  and store_key not in cat_web.key
select
    count_distinct(store_key) as unique_count
limit 10
;`

  ```text

  ```
- `trilogy file write query87.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Distinct (last_name, …re_tuples.sale_date = cat_web.sale_date;

# Count of store tuples NOT in catalog and NOT in web
select
    total_store - in_cat_web as unique_count
;`

  ```text

  ```
- `trilogy file write query87.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Build composite keys …ss.date_dim.year = 2000
  and store_key not in cat_key
  and store_key not in web_key
select
    count_distinct(store_key) as unique_count
limit 10
;`

  ```text

  ```
- `trilogy file write query93.preql -e -c import raw.store_sales as store_sales;

select
    store_sales.customer.customer_id as customer_id,
    sum(
        c… = store_returns.ticket_number
where store_returns.reason.desc = 'reason 28'
order by adjusted_amount nulls first, customer_id nulls first
limit 100;`

  ```text

  ```
- `trilogy run --import raw.store_sales:store_sales select count_distinct(store_sales.customer.customer_sk * 1000000 + store_sales.item.item_sk) from store_sales where store_sales.date_dim.year = 2000;`

  ```text

  ```
- `trilogy run --import raw.catalog_sales:catalog_sales select count_distinct(catalog_sales.bill_customer.customer_sk * 1000000 + catalog_sales.item.item_sk) from catalog_sales where catalog_sales.sold_date.year = 2000;`

  ```text

  ```
- `trilogy file write query99.preql`

  ```text

  ```

### `undefined-concept`

- `trilogy run query02.preql`

  ```text

  ```
- `trilogy run query02.preql`

  ```text

  ```
- `trilogy run query41.preql`

  ```text

  ```
- `trilogy explore query41.preql`

  ```text

  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy run query45.preql`

  ```text

  ```
- `trilogy run query49.preql`

  ```text

  ```
- `trilogy run query49.preql`

  ```text

  ```
- `trilogy run query53.preql`

  ```text

  ```
- `trilogy run query56.preql`

  ```text

  ```
- `trilogy run query56.preql`

  ```text

  ```
- `trilogy run query56.preql`

  ```text

  ```
- `trilogy run query56.preql`

  ```text

  ```
- `trilogy run query56.preql`

  ```text

  ```
- `trilogy run query56.preql`

  ```text

  ```
- `trilogy run query56.preql`

  ```text

  ```
- `trilogy run --import raw.date_dim select date_dim.week_seq where date_dim.date = '2000-01-03'::date limit 5;`

  ```text

  ```
- `trilogy run query60.preql`

  ```text

  ```
- `trilogy run query64.preql`

  ```text

  ```
- `trilogy run query64.preql`

  ```text

  ```
- `trilogy run query64.preql`

  ```text

  ```
- `trilogy run query64.preql`

  ```text

  ```
- `trilogy run query64.preql`

  ```text

  ```
- `trilogy run query64.preql`

  ```text

  ```
- `trilogy run query64.preql`

  ```text

  ```
- `trilogy run query64.preql`

  ```text

  ```
- `trilogy run query66.preql`

  ```text

  ```
- `trilogy run query66.preql`

  ```text

  ```
- `trilogy run query66.preql`

  ```text

  ```
- `trilogy run query66.preql`

  ```text

  ```
- `trilogy run query67.preql`

  ```text

  ```
- `trilogy run query67.preql`

  ```text

  ```
- `trilogy run query69.preql`

  ```text

  ```
- `trilogy run query75.preql`

  ```text

  ```
- `trilogy run query75.preql`

  ```text

  ```
- `trilogy run query75.preql`

  ```text

  ```
- `trilogy run query78.preql`

  ```text

  ```
- `trilogy run query78.preql`

  ```text

  ```
- `trilogy run query80.preql`

  ```text

  ```
- `trilogy run query81.preql`

  ```text

  ```
- `trilogy run query83.preql`

  ```text

  ```
- `trilogy run query89.preql`

  ```text

  ```
- `trilogy run query93.preql`

  ```text

  ```
- `trilogy run query94.preql`

  ```text

  ```
- `trilogy run query94.preql`

  ```text

  ```
- `trilogy run query94.preql`

  ```text

  ```

### `syntax-missing-alias`

- `trilogy run --import raw/date_dim:date_dim where date_dim.date between '2001-01-01'::date and '2001-01-14'::date select date_dim.week_seq, date_dim.year, week(date_dim.date), date_dim.date, date_dim.dow order by date_dim.date;`

  ```text

  ```
- `trilogy run --import raw.store_sales:ss --import raw.catalog_sales:cs --import raw.web_sales:ws select count(ss.item.item_sk) where ss.date_dim.year between 1999 and 2001;`

  ```text

  ```
- `trilogy run --import raw.item:item select count(item.item_id) where item.color in ('slate', 'blanched', 'burnished');`

  ```text

  ```
- `trilogy run --import raw/date_dim:date_dim select distinct date_dim.dow, date_dim.day_name order by date_dim.dow;`

  ```text

  ```
- `trilogy run --import raw.date_dim:dd select distinct dd.moy as month order by dd.moy asc limit 12;`

  ```text

  ```
- `trilogy run --import raw/catalog_returns:cr select cr.item.item_id, sum(cr.return_quantity), count(cr.return_quantity) where cr.item.item_id = 'AAAAAAAAGJMBAAAA' and cr.date_dim.week_seq in (5244, 5257, 5264);`

  ```text

  ```
- `trilogy file write query94.preql --content import raw.web_sales as ws;

# Web sales with ship date between 1999-02-01 and 1999-04-02
# shipped to IL, sold th…as order_count,
    sum(ws.ext_ship_cost) as total_extended_ship_cost,
    sum(ws.net_profit) as total_net_profit
order by
    order_count
limit 100;`

  ```text

  ```
- `trilogy run --import raw.catalog_sales:catalog_sales where catalog_sales.sold_date.year = 2000 select count_distinct(catalog_sales.bill_customer.customer_sk), count_distinct(catalog_sales.item.item_sk), count(catalog_sales.order_number) as orders limit 100;`

  ```text

  ```

### `join-resolution`

- `trilogy run query06.preql`

  ```text

  ```
- `trilogy run query49.preql`

  ```text

  ```
- `trilogy run query64.preql`

  ```text

  ```
- `trilogy run query64.preql`

  ```text

  ```
- `trilogy run query80.preql`

  ```text

  ```
- `trilogy run query80.preql`

  ```text

  ```
- `trilogy run query80.preql`

  ```text

  ```

### `file-not-found`

- `trilogy run query02.preql`

  ```text

  ```
- `trilogy run query56.preql`

  ```text

  ```
- `trilogy run query64.preql`

  ```text

  ```
- `trilogy run query78.preql`

  ```text

  ```

### `cli-misuse`

- `trilogy run --format json query77.preql`

  ```text
  'json' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy syntax example union-stack-channels`

  ```text
  No such command 'syntax'.
  ```
