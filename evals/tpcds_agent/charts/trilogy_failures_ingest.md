# Trilogy failure analysis — 20260808-162822

- Run `20260808-122821_ingest` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 2013 | failed: 258 (13%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `disabled-tool` | 93 | 36% |
| `other` | 32 | 12% |
| `import-path` | 30 | 12% |
| `syntax-parse` | 27 | 10% |
| `cli-misuse` | 24 | 9% |
| `undefined-concept` | 19 | 7% |
| `file-not-found` | 13 | 5% |
| `join-resolution` | 11 | 4% |
| `no-output` | 5 | 2% |
| `syntax-missing-alias` | 3 | 1% |
| `type-error` | 1 | 0% |

## Detail

### `disabled-tool`

- `trilogy file read root/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/web_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/catalog_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/customer.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/customer.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read trilogy.toml`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/inventory.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read test_cat.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read test_sr.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read test_store.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read probe.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read trilogy.toml`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/web_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read probe.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read trilogy.toml`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read test_store.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read probe.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/customer.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read test_store.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read probe.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read test_ws.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read test_ws2.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read test_store.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read test_web5.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/catalog_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read test_store.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/customer.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/item.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/customer.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read test_ws2.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/web_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under root/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read root/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read test_returns_union.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read debug1.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read test_store.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read probe11.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read probe.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read probe12.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/catalog_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/customer.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/catalog_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/web_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read debug5.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read probe_union8c.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read test_web5.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read test_ws.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read test_ws2.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read probe11.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read probe12.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read probe12.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read probe_store_net.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read t_store.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read probe.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read probe12.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `other`

- `trilogy run answer_3705756794.preql`

  ```text
  Syntax error in answer_3705756794.preql: `by rollup (…)` requires at least one aggregate (or re-aggregable pre-aggregated measure) in the select to group; found none.
  ```
- `trilogy run answer_3705756794.preql`

  ```text
  Syntax error in answer_3705756794.preql: Missing source map entry for cr.catalog_page.catalog_page_id with pseudonyms set(), have map: {'cs.catalog_page.catalog_page_sk': {BuildDatasource(name='catalog_page', columns=[BuildColumnAssignment(alias='cp_catalog_page_sk', concept=cs.catalog_page.catalog_page_sk@Grain<cs.catalog_page.catalog_page_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='cp_catalog_page_id', concept=cs.catalog_page.catalog_page_id@Grain<cs.catalog_page.catalog_page_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='cp_start_date_sk', concept=cs.catalog_page.start_date.date_sk@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_end_date_sk', concept=cs.catalog_page.end_date.date_sk@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_department', concept=cs.catalog_page.department@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_catalog_number', concept=cs.catalog_page.catalog_number@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_catalog_page_number', concept=cs.catalog_page.catalog_page_number@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_description', concept=cs.catalog_page.description@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_type', concept=cs.catalog_page.type@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='catalog_page', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[], env_label=None), grain=BuildGrain(components={'cs.catalog_page.catalog_page_sk'}, where_clause=None, _str='Grain<cs.catalog_page.catalog_page_sk>', _str_no_condition='Grain<cs.catalog_page.catalog_page_sk>', abstract=False), namespace='cs.catalog_page', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'cs.catalog_page.end_date.date_sk', 'cs.catalog_page.start_date.date_sk'})}, 'cs.catalog_page.catalog_page_id': {BuildDatasource(name='catalog_page', columns=[BuildColumnAssignment(alias='cp_catalog_page_sk', concept=cs.catalog_page.catalog_page_sk@Grain<cs.catalog_page.catalog_page_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='cp_catalog_page_id', concept=cs.catalog_page.catalog_page_id@Grain<cs.catalog_page.catalog_page_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='cp_start_date_sk', concept=cs.catalog_page.start_date.date_sk@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_end_date_sk', concept=cs.catalog_page.end_date.date_sk@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_department', concept=cs.catalog_page.department@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_catalog_number', concept=cs.catalog_page.catalog_number@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_catalog_page_number', concept=cs.catalog_page.catalog_page_number@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_description', concept=cs.catalog_page.description@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_type', concept=cs.catalog_page.type@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='catalog_page', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[], env_label=None), grain=BuildGrain(components={'cs.catalog_page.catalog_page_sk'}, where_clause=None, _str='Grain<cs.catalog_page.catalog_page_sk>', _str_no_condition='Grain<cs.catalog_page.catalog_page_sk>', abstract=False), namespace='cs.catalog_page', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'cs.catalog_page.end_date.date_sk', 'cs.catalog_page.start_date.date_sk'})}, 'cs.catalog_page.start_date.date_sk': {BuildDatasource(name='catalog_page', columns=[BuildColumnAssignment(alias='cp_catalog_page_sk', concept=cs.catalog_page.catalog_page_sk@Grain<cs.catalog_page.catalog_page_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='cp_catalog_page_id', concept=cs.catalog_page.catalog_page_id@Grain<cs.catalog_page.catalog_page_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='cp_start_date_sk', concept=cs.catalog_page.start_date.date_sk@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_end_date_sk', concept=cs.catalog_page.end_date.date_sk@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_department', concept=cs.catalog_page.department@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_catalog_number', concept=cs.catalog_page.catalog_number@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_catalog_page_number', concept=cs.catalog_page.catalog_page_number@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_description', concept=cs.catalog_page.description@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_type', concept=cs.catalog_page.type@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='catalog_page', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[], env_label=None), grain=BuildGrain(components={'cs.catalog_page.catalog_page_sk'}, where_clause=None, _str='Grain<cs.catalog_page.catalog_page_sk>', _str_no_condition='Grain<cs.catalog_page.catalog_page_sk>', abstract=False), namespace='cs.catalog_page', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'cs.catalog_page.end_date.date_sk', 'cs.catalog_page.start_date.date_sk'})}, 'cs.catalog_page.end_date.date_sk': {BuildDatasource(name='catalog_page', columns=[BuildColumnAssignment(alias='cp_catalog_page_sk', concept=cs.catalog_page.catalog_page_sk@Grain<cs.catalog_page.catalog_page_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='cp_catalog_page_id', concept=cs.catalog_page.catalog_page_id@Grain<cs.catalog_page.catalog_page_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='cp_start_date_sk', concept=cs.catalog_page.start_date.date_sk@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_end_date_sk', concept=cs.catalog_page.end_date.date_sk@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_department', concept=cs.catalog_page.department@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_catalog_number', concept=cs.catalog_page.catalog_number@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_catalog_page_number', concept=cs.catalog_page.catalog_page_number@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_description', concept=cs.catalog_page.description@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_type', concept=cs.catalog_page.type@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='catalog_page', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[], env_label=None), grain=BuildGrain(components={'cs.catalog_page.catalog_page_sk'}, where_clause=None, _str='Grain<cs.catalog_page.catalog_page_sk>', _str_no_condition='Grain<cs.catalog_page.catalog_page_sk>', abstract=False), namespace='cs.catalog_page', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'cs.catalog_page.end_date.date_sk', 'cs.catalog_page.start_date.date_sk'})}, 'cs.catalog_page.department': {BuildDatasource(name='catalog_page', columns=[BuildColumnAssignment(alias='cp_catalog_page_sk', concept=cs.catalog_page.catalog_page_sk@Grain<cs.catalog_page.catalog_page_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='cp_catalog_page_id', concept=cs.catalog_page.catalog_page_id@Grain<cs.catalog_page.catalog_page_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='cp_start_date_sk', concept=cs.catalog_page.start_date.date_sk@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_end_date_sk', concept=cs.catalog_page.end_date.date_sk@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_department', concept=cs.catalog_page.department@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_catalog_number', concept=cs.catalog_page.catalog_number@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_catalog_page_number', concept=cs.catalog_page.catalog_page_number@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_description', concept=cs.catalog_page.description@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_type', concept=cs.catalog_page.type@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='catalog_page', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[], env_label=None), grain=BuildGrain(components={'cs.catalog_page.catalog_page_sk'}, where_clause=None, _str='Grain<cs.catalog_page.catalog_page_sk>', _str_no_condition='Grain<cs.catalog_page.catalog_page_sk>', abstract=False), namespace='cs.catalog_page', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'cs.catalog_page.end_date.date_sk', 'cs.catalog_page.start_date.date_sk'})}, 'cs.catalog_page.catalog_number': {BuildDatasource(name='catalog_page', columns=[BuildColumnAssignment(alias='cp_catalog_page_sk', concept=cs.catalog_page.catalog_page_sk@Grain<cs.catalog_page.catalog_page_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='cp_catalog_page_id', concept=cs.catalog_page.catalog_page_id@Grain<cs.catalog_page.catalog_page_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='cp_start_date_sk', concept=cs.catalog_page.start_date.date_sk@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_end_date_sk', concept=cs.catalog_page.end_date.date_sk@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_department', concept=cs.catalog_page.department@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_catalog_number', concept=cs.catalog_page.catalog_number@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_catalog_page_number', concept=cs.catalog_page.catalog_page_number@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_description', concept=cs.catalog_page.description@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_type', concept=cs.catalog_page.type@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='catalog_page', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[], env_label=None), grain=BuildGrain(components={'cs.catalog_page.catalog_page_sk'}, where_clause=None, _str='Grain<cs.catalog_page.catalog_page_sk>', _str_no_condition='Grain<cs.catalog_page.catalog_page_sk>', abstract=False), namespace='cs.catalog_page', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'cs.catalog_page.end_date.date_sk', 'cs.catalog_page.start_date.date_sk'})}, 'cs.catalog_page.catalog_page_number': {BuildDatasource(name='catalog_page', columns=[BuildColumnAssignment(alias='cp_catalog_page_sk', concept=cs.catalog_page.catalog_page_sk@Grain<cs.catalog_page.catalog_page_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='cp_catalog_page_id', concept=cs.catalog_page.catalog_page_id@Grain<cs.catalog_page.catalog_page_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='cp_start_date_sk', concept=cs.catalog_page.start_date.date_sk@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_end_date_sk', concept=cs.catalog_page.end_date.date_sk@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_department', concept=cs.catalog_page.department@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_catalog_number', concept=cs.catalog_page.catalog_number@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_catalog_page_number', concept=cs.catalog_page.catalog_page_number@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_description', concept=cs.catalog_page.description@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_type', concept=cs.catalog_page.type@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='catalog_page', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[], env_label=None), grain=BuildGrain(components={'cs.catalog_page.catalog_page_sk'}, where_clause=None, _str='Grain<cs.catalog_page.catalog_page_sk>', _str_no_condition='Grain<cs.catalog_page.catalog_page_sk>', abstract=False), namespace='cs.catalog_page', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'cs.catalog_page.end_date.date_sk', 'cs.catalog_page.start_date.date_sk'})}, 'cs.catalog_page.description': {BuildDatasource(name='catalog_page', columns=[BuildColumnAssignment(alias='cp_catalog_page_sk', concept=cs.catalog_page.catalog_page_sk@Grain<cs.catalog_page.catalog_page_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='cp_catalog_page_id', concept=cs.catalog_page.catalog_page_id@Grain<cs.catalog_page.catalog_page_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='cp_start_date_sk', concept=cs.catalog_page.start_date.date_sk@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_end_date_sk', concept=cs.catalog_page.end_date.date_sk@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_department', concept=cs.catalog_page.department@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_catalog_number', concept=cs.catalog_page.catalog_number@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_catalog_page_number', concept=cs.catalog_page.catalog_page_number@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_description', concept=cs.catalog_page.description@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_type', concept=cs.catalog_page.type@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='catalog_page', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[], env_label=None), grain=BuildGrain(components={'cs.catalog_page.catalog_page_sk'}, where_clause=None, _str='Grain<cs.catalog_page.catalog_page_sk>', _str_no_condition='Grain<cs.catalog_page.catalog_page_sk>', abstract=False), namespace='cs.catalog_page', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'cs.catalog_page.end_date.date_sk', 'cs.catalog_page.start_date.date_sk'})}, 'cs.catalog_page.type': {BuildDatasource(name='catalog_page', columns=[BuildColumnAssignment(alias='cp_catalog_page_sk', concept=cs.catalog_page.catalog_page_sk@Grain<cs.catalog_page.catalog_page_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='cp_catalog_page_id', concept=cs.catalog_page.catalog_page_id@Grain<cs.catalog_page.catalog_page_sk>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='cp_start_date_sk', concept=cs.catalog_page.start_date.date_sk@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_end_date_sk', concept=cs.catalog_page.end_date.date_sk@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_department', concept=cs.catalog_page.department@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_catalog_number', concept=cs.catalog_page.catalog_number@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_catalog_page_number', concept=cs.catalog_page.catalog_page_number@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_description', concept=cs.catalog_page.description@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='cp_type', concept=cs.catalog_page.type@Grain<cs.catalog_page.catalog_page_sk>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='catalog_page', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[], env_label=None), grain=BuildGrain(components={'cs.catalog_page.catalog_page_sk'}, where_clause=None, _str='Grain<cs.catalog_page.catalog_page_sk>', _str_no_condition='Grain<cs.catalog_page.catalog_page_sk>', abstract=False), namespace='cs.catalog_page', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'cs.catalog_page.end_date.date_sk', 'cs.catalog_page.start_date.date_sk'})}}
  ```
- `trilogy agent-info syntax example intersect-except-setops`

  ```text
  Unknown syntax example: 'intersect-except-setops'

  Available Trilogy syntax examples - print one with `trilogy agent-info syntax example <name>`:

  - `python-datasource` - run a local Python script as a datasource: wrap a function in `trilogy.io.run`, which writes the Arrow IPC stream to stdout for you from a table, dataframe, or list of dicts; declare concepts, map script columns in `datasource (...)`, use `grain (...) file `path.py`;`, then reference locally declared concepts WITHOUT the datasource name as a prefix
  - `query-structure` - the clause order of a query (`where` -> `select` <cols>
  …
  subtotal/total rows and to sort by level
  - `rank-over-rollup` - rank rollup subtotals/leaves with a SINGLE `rank(a,b) over (partition by level, parent ...)` - not separate ranks per level
  - `staged-membership` - compute a membership set in a `rowset` (keys meeting a count/HAVING), then filter the main query with `<key> in <rowset>.<col>`
  - `correlated-exists-via-grouped-counts` - translate `EXISTS other` / `NOT EXISTS other matching` over the same model into two `count(...) by <grain>` compared in `where` (`> 1` = another exists, `= 1` = no other matches) - pin the correlation grain with `by`
  ```
- `trilogy run answer_2802535988.preql`

  ```text
  trilogy error: subprocess timed out after 600s.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ':' delimiter: line 1 column 8 (char 7). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run probe.preql`

  ```text
  Unexpected error in probe.preql: Cannot build <class 'int'>
  ```
- `trilogy run probe.preql`

  ```text
  Unexpected error in probe.preql: Cannot build <class 'int'>
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid \escape: line 1 column 83 (char 82). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid \escape: line 1 column 81 (char 80). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run answer_1623435181.preql`

  ```text
  Syntax error in answer_1623435181.preql: Conflicting join types (full, left outer) on keys joined into one group: a FULL/UNION join cannot be mixed with another type on the same key (it is ambiguous whether the key is required or one-sided). Make the whole group one type (e.g. `UNION JOIN a = b = c`), or use a distinct key. (line 26, column 1)
  ```
- `trilogy run answer_1484301313.preql`

  ```text
  Syntax error in answer_1484301313.preql: Impossible comparison in ref:ss.promotion.channel_email = Y: 'Y' can never match a declared value of enum<'N'> — fix the constant, or update the enum declaration if the domain is stale
  ```
- `trilogy run answer_4037365554.preql`

  ```text
  Unexpected error in answer_4037365554.preql: (_duckdb.BinderException) Binder Error: column month_totals_month_total must appear in the GROUP BY clause or be used in an aggregate function
  [SQL:
  WITH
  thoughtful as (
  SELECT
      "ss_date_dim_date_dim"."d_moy" as "month_totals_moy",
      "ss_item_item"."i_manager_id" as "month_totals_mgr",
      sum("ss_store_sales"."ss_sales_price") as "month_totals_month_total"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "item" as "ss_item_item" on "ss_store_sales"."ss_item_sk" = "ss_item_item"."i_item_sk"
      INNER JOIN "store" as "ss_store_store" on "ss_store_sales"."ss_store_sk" = "ss_store_store"."s_store_sk"
      INNER JOIN "date_dim" as "ss_date_dim_date_dim" on "ss_store_sales"."ss_sold_date_sk" = "ss_date_dim_date_dim"."d_date_sk"
  WHERE
      "ss_date_dim_date_dim"."d_year" = 2000 and coalesce("ss_store_sales"."ss_store_sk","ss_store_store"."s_store_sk") is not null and ( ( ("ss_item_item"."i_category" is not null and "ss_item_item"."i_category" in ('Books','Children','Electronics')) and ("ss_item_item"."i_class" is not null and "ss_item_item"."i_class" in ('personal','portable','reference','self-help')) and ("ss_item_item"."i_brand" is not null and "ss_item_item"."i_brand" in ('scholaramalgamalg #14','scholaramalgamalg #7','exportiunivamalg #9','scholaramalgamalg #9')) ) or ( ("ss_item_item"."i_category" is not null and "ss_item_item"."i_category" in ('Women','Music','Men')) and ("ss_item_item"."i_class" is not null and "ss_item_item"."i_class" in ('accessories','classical','fragrances','pants')) and ("ss_item_item"."i_brand" is not null and "ss_item_item"."i_brand" in ('amalgimporto #1','edu packscholar #1','exportiimporto #1','importoamalg #1')) ) )

  GROUP BY
      1,
      2),
  uneven as (
  SELECT
      "thoughtful"."month_totals_mgr" as "month_totals_mgr",
      "thoughtful"."month_totals_moy" as "month_totals_moy"
  FROM
      "thoughtful"),
  yummy as (
  SELECT
      "thoughtful"."month_totals_mgr" as "month_totals_mgr",
      avg("thoughtful"."month_totals_month_total") as "avg_monthly"
  FROM
      "uneven"
      INNER JOIN "thoughtful" on "uneven"."month_totals_mgr" is not distinct from "thoughtful"."month_totals_mgr" AND "uneven"."month_totals_moy" is not distinct from "thoughtful"."month_totals_moy"
  GROUP BY
      1
  HAVING
      avg("thoughtful"."month_totals_month_total") > 0 and abs("thoughtful"."month_totals_month_total" - avg("thoughtful"."month_totals_month_total")) / avg("thoughtful"."month_totals_month_total") > 0.1
  )
  SELECT
      "thoughtful"."month_totals_mgr" as "month_totals_mgr",
      "thoughtful"."month_totals_month_total" as "month_totals_month_total",
      "yummy"."avg_monthly" as "avg_monthly"
  FROM
      "thoughtful"
      INNER JOIN "yummy" on "thoughtful"."month_totals_mgr" is not distinct from "yummy"."month_totals_mgr"
  GROUP BY
      1,
      2,
      3,
      "thoughtful"."month_totals_moy"
  ORDER BY
      "thoughtful"."month_totals_mgr" asc,
      "yummy"."avg_monthly" asc,
      "thoughtful"."month_totals_month_total" asc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run answer_1772060640.preql`

  ```text
  Syntax error in answer_1772060640.preql: Impossible comparison in SubselectComparison(left=ref:ss.store.county, right=('Orange County', 'Bronx County', 'Franklin Parish', 'Williamson County'), operator=<ComparisonOperator.IN: 'in'>): 'Orange County' can never match a declared value of enum<'Williamson County'> — fix the constant, or update the enum declaration if the domain is stale
  ```
- `trilogy run answer_1772060640.preql`

  ```text
  Syntax error in answer_1772060640.preql: ORDER BY references 'ss.customer.customer_sk', which is not in the SELECT projection (line 3). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --ss.customer.customer_sk order by ss.customer.customer_sk asc`.
  ```
- `trilogy run probe_union8b.preql`

  ```text
  Syntax error in probe_union8b.preql: Impossible comparison in divide(ref:y2002.net_qty,ref:y2001.net_qty) < 0.9: declared domain bigint[1..100] has no value < 0.9
  ```
- `trilogy run answer_3210116865.preql`

  ```text
  Unexpected error in answer_3210116865.preql: Could not render the query: Missing source reference to cs.call_center.call_center_sk; Missing source reference to ss.store.store_sk; Missing source reference to ws.web_page.web_page_sk. A planned reference has no backing source CTE -- typically an unsupported cross-rowset or membership shape the planner could not wire. Review the rowset/join structure (or file an issue if the query looks valid).

  Full SQL with sentinel(s):

  WITH
  tearful as (
  SELECT
      "ws_web_sales"."ws_web_page_sk" as "_web_sales_agg_sk",
      sum("ws_web_sales"."ws_ext_sales_price") as "_web_sales_agg_sales",
      sum("ws_web_sales"."ws_net_profit") as "_web_sales_agg_profit"
  FROM
      "web_sales" as "ws_web_sales"
      INNER JOIN "date_dim" as "ws_sold_date_date_dim" on "ws_web_sales"."ws_sold_date_sk" = "ws_sold_date_date_dim"."d_date_sk"
  WHERE
      "ws_sold_date_date_dim"."d_date" BETWEEN date '2000-08-23' AND date '2000-09-22' and "ws_web_sales"."ws_web_page_sk" is not null

  GROUP BY
      1),
  flashy as (
  SELECT
      "tearful"."_web_sales_agg_profit" as "web_sales_agg_profit",
      "tearful"."_web_sales_agg_sales" as "web_sales_agg_sales",
      "tearful"."_web_sales_agg_sk" as "web_sales_agg_sk",
      coalesce("tearful"."_web_sales_agg_sk") as "_virt_presence_4118804935211818"
  FROM
      "tearful"),
  elated as (
  SELECT
      "wr_web_returns"."wr_web_page_sk" as "web_ret_agg_sk",
      sum("wr_web_returns"."wr_net_loss") as "web_ret_agg_r_loss",
      sum("wr_web_returns"."wr_return_amt") as "web_ret_agg_r_amt"
  FROM
      "item" as "wr_item_item"
      LEFT OUTER JOIN "web_returns" as "wr_web_returns" on "wr_item_item"."i_item_sk" = "wr_web_returns"."wr_item_sk"
      RIGHT OUTER JOIN "date_dim" as "wr_date_dim_date_dim" on "wr_web_returns"."wr_returned_date_sk" = "wr_date_dim_date_dim"."d_date_sk"
  WHERE
      "wr_date_dim_date_dim"."d_date" BETWEEN date '2000-08-23' AND date '2000-09-22'

  GROUP BY
      1),
  trite as (
  SELECT
      "elated"."web_ret_agg_r_amt" as "web_ret_agg_r_amt",
      "elated"."web_ret_agg_r_loss" as "web_ret_agg_r_loss",
      "flashy"."_virt_presence_4118804935211818" as "_virt_presence_4118804935211818",
      "flashy"."web_sales_agg_profit" as "web_sales_agg_profit",
      "flashy"."web_sales_agg_sales" as "web_sales_agg_sales",
      coalesce("elated"."web_ret_agg_sk","flashy"."web_sales_agg_sk") as "web_ret_agg_sk",
      coalesce("elated"."web_ret_agg_sk","flashy"."web_sales_agg_sk") as "web_sales_agg_sk"
  FROM
      "flashy"
      LEFT OUTER JOIN "elated" on "flashy"."web_sales_agg_sk" is not distinct from "elated"."web_ret_agg_sk"
  WHERE
      "flashy"."_virt_presence_4118804935211818" is not null
  ),
  quick as (
  SELECT
      "trite"."_virt_presence_4118804935211818" as "_virt_presence_4118804935211818",
      "trite"."web_ret_agg_sk" as "web_ret_agg_sk",
      "trite"."web_sales_agg_profit" - coalesce("trite"."web_ret_agg_r_loss",0) as "___tvf_arm_2_profit",
      "trite"."web_sales_agg_sales" as "___tvf_arm_2_sales",
      "trite"."web_sales_agg_sk" as "___tvf_arm_2_outlet",
      "trite"."web_sales_agg_sk" as "web_sales_agg_sk",
      coalesce("trite"."web_ret_agg_r_amt",0) as "___tvf_arm_2_returns"
  FROM
      "trite"),
  abhorrent as (
  SELECT
      :___tvf_arm_0_channel as "___tvf_arm_0_channel",
      :___tvf_arm_1_channel as "___tvf_arm_1_channel",
      :___tvf_arm_2_channel as "___tvf_arm_2_channel",
      INVALID_REFERENCE_BUG<Missing source reference to cs.call_center.call_center_sk> as "catalog_sales_agg_sk",
      INVALID_REFERENCE_BUG<Missing source reference to ss.store.store_sk> as "store_sales_agg_sk",
      INVALID_REFERENCE_BUG<Missing source reference to ws.web_page.web_page_sk> as "web_sales_agg_sk"
  ),
  nondescript as (
  SELECT
      "abhorrent"."___tvf_arm_2_channel" as "___tvf_arm_2_channel",
      "quick"."___tvf_arm_2_profit" as "___tvf_arm_2_profit",
      "quick"."___tvf_arm_2_returns" as "___tvf_arm_2_returns",
      "quick"."___tvf_arm_2_sales" as "___tvf_arm_2_sales",
      coalesce("abhorrent"."web_sales_agg_sk","quick"."___tvf_arm_2_outlet","quick"."web_sales_agg_sk") as "___tvf_arm_2_outlet"
  FROM
      "quick"
      FULL JOIN "abhorrent" on "quick"."___tvf_arm_2_outlet" = "abhorrent"."web_sales_agg_sk"
  WHERE
      "quick"."_virt_presence_4118804935211818" is not null
  ),
  waggish as (
  SELECT
      coalesce("ss_store_sales"."ss_store_sk","ss_store_store"."s_store_sk") as "_store_sales_agg_sk",
      sum("ss_store_sales"."ss_ext_sales_price") as "_store_sales_agg_sales",
      sum("ss_store_sales"."ss_net_profit") as "_store_sales_agg_profit"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "store" as "ss_store_store" on "ss_store_sales"."ss_store_sk" = "ss_store_store"."s_store_sk"
      INNER JOIN "date_dim" as "ss_date_dim_date_dim" on "ss_store_sales"."ss_sold_date_sk" = "ss_date_dim_date_dim"."d_date_sk"
  WHERE
      "ss_date_dim_date_dim"."d_date" BETWEEN date '2000-08-23' AND date '2000-09-22' and coalesce("ss_store_sales"."ss_store_sk","ss_store_store"."s_store_sk") is not null

  GROUP BY
      1),
  hard as (
  SELECT
      "waggish"."_store_sales_agg_profit" as "store_sales_agg_profit",
      "waggish"."_store_sales_agg_sales" as "store_sales_agg_sales",
      "waggish"."_store_sales_agg_sk" as "store_sales_agg_sk",
      coalesce("waggish"."_store_sales_agg_sk") as "_virt_presence_9158607131677881"
  FROM
      "waggish"),
  kaput as (
  SELECT
      coalesce("sr_store_returns"."sr_store_sk","sr_store_store"."s_store_sk") as "store_ret_agg_sk",
      sum("sr_store_returns"."sr_net_loss") as "store_ret_agg_r_loss",
      sum("sr_store_returns"."sr_return_amt") as "store_ret_agg_r_amt"
  FROM
      "store_returns" as "sr_store_returns"
      FULL JOIN "store" as "sr_store_store" on "sr_store_returns"."sr_store_sk" = "sr_store_store"."s_store_sk"
      RIGHT OUTER JOIN "date_dim" as "sr_date_dim_date_dim" on "sr_store_returns"."sr_returned_date_sk" = "sr_date_dim_date_dim"."d_date_sk"
  WHERE
      "sr_date_dim_date_dim"."d_date" BETWEEN date '2000-08-23' AND date '2000-09-22'

  GROUP BY
      1),
  sedate as (
  SELECT
      "hard"."_virt_presence_9158607131677881" as "_virt_presence_9158607131677881",
      "hard"."store_sales_agg_profit" as "store_sales_agg_profit",
      "hard"."store_sales_agg_sales" as "store_sales_agg_sales",
      "kaput"."store_ret_agg_r_amt" as "store_ret_agg_r_amt",
      "kaput"."store_ret_agg_r_loss" as "store_ret_agg_r_loss",
      coalesce("hard"."store_sales_agg_sk","kaput"."store_ret_agg_sk") as "store_ret_agg_sk",
      coalesce("hard"."store_sales_agg_sk","kaput"."store_ret_agg_sk") as "store_sales_agg_sk"
  FROM
      "hard"
      LEFT OUTER JOIN "kaput" on "hard"."store_sales_agg_sk" is not distinct from "kaput"."store_ret_agg_sk"
  WHERE
      "hard"."_virt_presence_9158607131677881" is not null
  ),
  yellow as (
  SELECT
      "sedate"."_virt_presence_9158607131677881" as "_virt_presence_9158607131677881",
      "sedate"."store_ret_agg_sk" as "store_ret_agg_sk",
      "sedate"."store_sales_agg_profit" - coalesce("sedate"."store_ret_agg_r_loss",0) as "___tvf_arm_0_profit",
      "sedate"."store_sales_agg_sales" as "___tvf_arm_0_sales",
      "sedate"."store_sales_agg_sk" as "___tvf_arm_0_outlet",
      "sedate"."store_sales_agg_sk" as "store_sales_agg_sk",
      coalesce("sedate"."store_ret_agg_r_amt",0) as "___tvf_arm_0_returns"
  FROM
      "sedate"),
  resonant as (
  SELECT
      "abhorrent"."___tvf_arm_0_channel" as "___tvf_arm_0_channel",
      "yellow"."___tvf_arm_0_profit" as "___tvf_arm_0_profit",
      "yellow"."___tvf_arm_0_returns" as "___tvf_arm_0_returns",
      "yellow"."___tvf_arm_0_sales" as "___tvf_arm_0_sales",
      coalesce("abhorrent"."store_sales_agg_sk","yellow"."___tvf_arm_0_outlet","yellow"."store_sales_agg_sk") as "___tvf_arm_0_outlet"
  FROM
      "yellow"
      FULL JOIN "abhorrent" on "yellow"."___tvf_arm_0_outlet" = "abhorrent"."store_sales_agg_sk"
  WHERE
      "yellow"."_virt_presence_9158607131677881" is not null
  ),
  yummy as (
  SELECT
      "cs_catalog_sales"."cs_call_center_sk" as "_catalog_sales_agg_sk",
      sum("cs_catalog_sales"."cs_ext_sales_price") as "_catalog_sales_agg_sales",
      sum("cs_catalog_sales"."cs_net_profit") as "_catalog_sales_agg_profit"
  FROM
      "catalog_sales" as "cs_catalog_sales"
      INNER JOIN "date_dim" as "cs_sold_date_date_dim" on "cs_catalog_sales"."cs_sold_date_sk" = "cs_sold_date_date_dim"."d_date_sk"
  WHERE
      "cs_sold_date_date_dim"."d_date" BETWEEN date '2000-08-23' AND date '2000-09-22' and "cs_catalog_sales"."cs_call_center_sk" is not null

  GROUP BY
      1),
  concerned as (
  SELECT
      "yummy"."_catalog_sales_agg_profit" as "catalog_sales_agg_profit",
      "yummy"."_catalog_sales_agg_sales" as "catalog_sales_agg_sales",
      "yummy"."_catalog_sales_agg_sk" as "catalog_sales_agg_sk",
      coalesce("yummy"."_catalog_sales_agg_sk") as "_virt_presence_9447600895504718"
  FROM
      "yummy"),
  cheerful as (
  SELECT
      "cr_catalog_returns"."cr_call_center_sk" as "catalog_ret_agg_sk",
      sum("cr_catalog_returns"."cr_net_loss") as "catalog_ret_agg_r_loss",
      sum("cr_catalog_returns"."cr_return_amount") as "catalog_ret_agg_r_amt"
  FROM
      "item" as "cr_item_item"
      LEFT OUTER JOIN "catalog_returns" as "cr_catalog_returns" on "cr_item_item"."i_item_sk" = "cr_catalog_returns"."cr_item_sk"
      RIGHT OUTER JOIN "date_dim" as "cr_date_dim_date_dim" on "cr_catalog_returns"."cr_returned_date_sk" = "cr_date_dim_date_dim"."d_date_sk"
  WHERE
      "cr_date_dim_date_dim"."d_date" BETWEEN date '2000-08-23' AND date '2000-09-22'

  GROUP BY
      1),
  young as (
  SELECT
      "cheerful"."catalog_ret_agg_r_amt" as "catalog_ret_agg_r_amt",
      "cheerful"."catalog_ret_agg_r_loss" as "catalog_ret_agg_r_loss",
      "concerned"."_virt_presence_9447600895504718" as "_virt_presence_9447600895504718",
      "concerned"."catalog_sales_agg_profit" as "catalog_sales_agg_profit",
      "concerned"."catalog_sales_agg_sales" as "catalog_sales_agg_sales",
      coalesce("cheerful"."catalog_ret_agg_sk","concerned"."catalog_sales_agg_sk") as "catalog_ret_agg_sk",
      coalesce("cheerful"."catalog_ret_agg_sk","concerned"."catalog_sales_agg_sk") as "catalog_sales_agg_sk"
  FROM
      "concerned"
      LEFT OUTER JOIN "cheerful" on "concerned"."catalog_sales_agg_sk" is not distinct from "cheerful"."catalog_ret_agg_sk"
  WHERE
      "concerned"."_virt_presence_9447600895504718" is not null
  ),
  sparkling as (
  SELECT
      "young"."_virt_presence_9447600895504718" as "_virt_presence_9447600895504718",
      "young"."catalog_ret_agg_sk" as "catalog_ret_agg_sk",
      "young"."catalog_sales_agg_profit" - coalesce("young"."catalog_ret_agg_r_loss",0) as "___tvf_arm_1_profit",
      "young"."catalog_sales_agg_sales" as "___tvf_arm_1_sales",
      "young"."catalog_sales_agg_sk" as "___tvf_arm_1_outlet",
      "young"."catalog_sales_agg_sk" as "catalog_sales_agg_sk",
      coalesce("young"."catalog_ret_agg_r_amt",0) as "___tvf_arm_1_returns"
  FROM
      "young"),
  sweltering as (
  SELECT
      "abhorrent"."___tvf_arm_1_channel" as "___tvf_arm_1_channel",
      "sparkling"."___tvf_arm_1_outlet" as "___tvf_arm_1_outlet",
      "sparkling"."___tvf_arm_1_profit" as "___tvf_arm_1_profit",
      "sparkling"."___tvf_arm_1_returns" as "___tvf_arm_1_returns",
      "sparkling"."___tvf_arm_1_sales" as "___tvf_arm_1_sales"
  FROM
      "sparkling"
      FULL JOIN "abhorrent" on "sparkling"."catalog_sales_agg_sk" = "abhorrent"."catalog_sales_agg_sk"
  WHERE
      "sparkling"."_virt_presence_9447600895504718" is not null
  ),
  slow as (
  SELECT
      "sweltering"."___tvf_arm_1_channel" as "_all_channels_channel",
      "sweltering"."___tvf_arm_1_outlet" as "_all_channels_outlet",
      "sweltering"."___tvf_arm_1_sales" as "_all_channels_sales",
      "sweltering"."___tvf_arm_1_returns" as "_all_channels_returns",
      "sweltering"."___tvf_arm_1_profit" as "_all_channels_profit"
  FROM
      "sweltering"
  UNION ALL
  SELECT
      "resonant"."___tvf_arm_0_channel" as "_all_channels_channel",
      "resonant"."___tvf_arm_0_outlet" as "_all_channels_outlet",
      "resonant"."___tvf_arm_0_sales" as "_all_channels_sales",
      "resonant"."___tvf_arm_0_returns" as "_all_channels_returns",
      "resonant"."___tvf_arm_0_profit" as "_all_channels_profit"
  FROM
      "resonant"
  UNION ALL
  SELECT
      "nondescript"."___tvf_arm_2_channel" as "_all_channels_channel",
      "nondescript"."___tvf_arm_2_outlet" as "_all_channels_outlet",
      "nondescript"."___tvf_arm_2_sales" as "_all_channels_sales",
      "nondescript"."___tvf_arm_2_returns" as "_all_channels_returns",
      "nondescript"."___tvf_arm_2_profit" as "_all_channels_profit"
  FROM
      "nondescript"),
  spiritual as (
  SELECT
      "slow"."_all_channels_channel" as "all_channels_channel",
      "slow"."_all_channels_outlet" as "all_channels_outlet",
      "slow"."_all_channels_profit" as "all_channels_profit",
      "slow"."_all_channels_returns" as "all_channels_returns",
      "slow"."_all_channels_sales" as "all_channels_sales"
  FROM
      "slow")
  SELECT
      "spiritual"."all_channels_channel" as "all_channels_channel",
      "spiritual"."all_channels_outlet" as "all_channels_outlet",
      sum("spiritual"."all_channels_sales") as "sales",
      sum("spiritual"."all_channels_returns") as "returns",
      sum("spiritual"."all_channels_profit") as "profit"
  FROM
      "spiritual"
  GROUP BY
      ROLLUP (1, 2)
  ORDER BY
      "spiritual"."all_channels_channel" asc nulls first,
      "spiritual"."all_channels_outlet" asc nulls first,
      "returns" desc nulls last
  LIMIT (100)
  ```
- `trilogy run answer_3210116865.preql`

  ```text
  Unexpected error in answer_3210116865.preql: Could not render the query: Missing source reference to cs.call_center.call_center_sk; Missing source reference to ss.store.store_sk; Missing source reference to ws.web_page.web_page_sk. A planned reference has no backing source CTE -- typically an unsupported cross-rowset or membership shape the planner could not wire. Review the rowset/join structure (or file an issue if the query looks valid).

  Full SQL with sentinel(s):

  WITH
  trite as (
  SELECT
      "ws_web_sales"."ws_web_page_sk" as "_web_sales_agg_sk",
      sum("ws_web_sales"."ws_ext_sales_price") as "_web_sales_agg_sales",
      sum("ws_web_sales"."ws_net_profit") as "_web_sales_agg_profit"
  FROM
      "web_sales" as "ws_web_sales"
      INNER JOIN "date_dim" as "ws_sold_date_date_dim" on "ws_web_sales"."ws_sold_date_sk" = "ws_sold_date_date_dim"."d_date_sk"
  WHERE
      "ws_sold_date_date_dim"."d_date" BETWEEN date '2000-08-23' AND date '2000-09-22' and "ws_web_sales"."ws_web_page_sk" is not null

  GROUP BY
      1),
  round as (
  SELECT
      "trite"."_web_sales_agg_profit" as "web_sales_agg_profit",
      "trite"."_web_sales_agg_sales" as "web_sales_agg_sales",
      "trite"."_web_sales_agg_sk" as "web_sales_agg_sk",
      coalesce("trite"."_web_sales_agg_sk") as "_virt_presence_4118804935211818"
  FROM
      "trite"),
  wooden as (
  SELECT
      "wr_web_returns"."wr_web_page_sk" as "web_ret_agg_sk",
      sum("wr_web_returns"."wr_net_loss") as "web_ret_agg_r_loss",
      sum("wr_web_returns"."wr_return_amt") as "web_ret_agg_r_amt"
  FROM
      "item" as "wr_item_item"
      LEFT OUTER JOIN "web_returns" as "wr_web_returns" on "wr_item_item"."i_item_sk" = "wr_web_returns"."wr_item_sk"
      RIGHT OUTER JOIN "date_dim" as "wr_date_dim_date_dim" on "wr_web_returns"."wr_returned_date_sk" = "wr_date_dim_date_dim"."d_date_sk"
  WHERE
      "wr_date_dim_date_dim"."d_date" BETWEEN date '2000-08-23' AND date '2000-09-22'

  GROUP BY
      1),
  slow as (
  SELECT
      "round"."_virt_presence_4118804935211818" as "_virt_presence_4118804935211818",
      "round"."web_sales_agg_profit" as "web_sales_agg_profit",
      "round"."web_sales_agg_sales" as "web_sales_agg_sales",
      "wooden"."web_ret_agg_r_amt" as "web_ret_agg_r_amt",
      "wooden"."web_ret_agg_r_loss" as "web_ret_agg_r_loss",
      coalesce("round"."web_sales_agg_sk","wooden"."web_ret_agg_sk") as "web_ret_agg_sk",
      coalesce("round"."web_sales_agg_sk","wooden"."web_ret_agg_sk") as "web_sales_agg_sk"
  FROM
      "round"
      LEFT OUTER JOIN "wooden" on "round"."web_sales_agg_sk" is not distinct from "wooden"."web_ret_agg_sk"
  WHERE
      "round"."_virt_presence_4118804935211818" is not null
  ),
  spiritual as (
  SELECT
      "slow"."_virt_presence_4118804935211818" as "_virt_presence_4118804935211818",
      "slow"."web_ret_agg_sk" as "web_ret_agg_sk",
      "slow"."web_sales_agg_profit" - coalesce("slow"."web_ret_agg_r_loss",0) as "_web_res_profit",
      "slow"."web_sales_agg_sales" as "_web_res_sales",
      "slow"."web_sales_agg_sk" as "_web_res_outlet",
      "slow"."web_sales_agg_sk" as "web_sales_agg_sk",
      coalesce("slow"."web_ret_agg_r_amt",0) as "_web_res_returns"
  FROM
      "slow"),
  abhorrent as (
  SELECT
      :_catalog_res_channel as "_catalog_res_channel",
      :_store_res_channel as "_store_res_channel",
      :_web_res_channel as "_web_res_channel",
      INVALID_REFERENCE_BUG<Missing source reference to cs.call_center.call_center_sk> as "catalog_sales_agg_sk",
      INVALID_REFERENCE_BUG<Missing source reference to ss.store.store_sk> as "store_sales_agg_sk",
      INVALID_REFERENCE_BUG<Missing source reference to ws.web_page.web_page_sk> as "web_sales_agg_sk"
  ),
  brave as (
  SELECT
      "abhorrent"."_web_res_channel" as "___tvf_arm_2_channel",
      "spiritual"."_web_res_profit" as "___tvf_arm_2_profit",
      "spiritual"."_web_res_returns" as "___tvf_arm_2_returns",
      "spiritual"."_web_res_sales" as "___tvf_arm_2_sales",
      coalesce("abhorrent"."web_sales_agg_sk","spiritual"."_web_res_outlet","spiritual"."web_sales_agg_sk") as "___tvf_arm_2_outlet"
  FROM
      "spiritual"
      FULL JOIN "abhorrent" on "spiritual"."_web_res_outlet" = "abhorrent"."web_sales_agg_sk"
  WHERE
      "spiritual"."_virt_presence_4118804935211818" is not null
  ),
  puffy as (
  SELECT
      coalesce("ss_store_sales"."ss_store_sk","ss_store_store"."s_store_sk") as "_store_sales_agg_sk",
      sum("ss_store_sales"."ss_ext_sales_price") as "_store_sales_agg_sales",
      sum("ss_store_sales"."ss_net_profit") as "_store_sales_agg_profit"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "store" as "ss_store_store" on "ss_store_sales"."ss_store_sk" = "ss_store_store"."s_store_sk"
      INNER JOIN "date_dim" as "ss_date_dim_date_dim" on "ss_store_sales"."ss_sold_date_sk" = "ss_date_dim_date_dim"."d_date_sk"
  WHERE
      "ss_date_dim_date_dim"."d_date" BETWEEN date '2000-08-23' AND date '2000-09-22' and coalesce("ss_store_sales"."ss_store_sk","ss_store_store"."s_store_sk") is not null

  GROUP BY
      1),
  yellow as (
  SELECT
      "puffy"."_store_sales_agg_profit" as "store_sales_agg_profit",
      "puffy"."_store_sales_agg_sales" as "store_sales_agg_sales",
      "puffy"."_store_sales_agg_sk" as "store_sales_agg_sk",
      coalesce("puffy"."_store_sales_agg_sk") as "_virt_presence_9158607131677881"
  FROM
      "puffy"),
  busy as (
  SELECT
      coalesce("sr_store_returns"."sr_store_sk","sr_store_store"."s_store_sk") as "store_ret_agg_sk",
      sum("sr_store_returns"."sr_net_loss") as "store_ret_agg_r_loss",
      sum("sr_store_returns"."sr_return_amt") as "store_ret_agg_r_amt"
  FROM
      "store_returns" as "sr_store_returns"
      FULL JOIN "store" as "sr_store_store" on "sr_store_returns"."sr_store_sk" = "sr_store_store"."s_store_sk"
      RIGHT OUTER JOIN "date_dim" as "sr_date_dim_date_dim" on "sr_store_returns"."sr_returned_date_sk" = "sr_date_dim_date_dim"."d_date_sk"
  WHERE
      "sr_date_dim_date_dim"."d_date" BETWEEN date '2000-08-23' AND date '2000-09-22'

  GROUP BY
      1),
  resonant as (
  SELECT
      "busy"."store_ret_agg_r_amt" as "store_ret_agg_r_amt",
      "busy"."store_ret_agg_r_loss" as "store_ret_agg_r_loss",
      "yellow"."_virt_presence_9158607131677881" as "_virt_presence_9158607131677881",
      "yellow"."store_sales_agg_profit" as "store_sales_agg_profit",
      "yellow"."store_sales_agg_sales" as "store_sales_agg_sales",
      coalesce("busy"."store_ret_agg_sk","yellow"."store_sales_agg_sk") as "store_ret_agg_sk",
      coalesce("busy"."store_ret_agg_sk","yellow"."store_sales_agg_sk") as "store_sales_agg_sk"
  FROM
      "yellow"
      LEFT OUTER JOIN "busy" on "yellow"."store_sales_agg_sk" is not distinct from "busy"."store_ret_agg_sk"
  WHERE
      "yellow"."_virt_presence_9158607131677881" is not null
  ),
  dapper as (
  SELECT
      "resonant"."_virt_presence_9158607131677881" as "_virt_presence_9158607131677881",
      "resonant"."store_ret_agg_sk" as "store_ret_agg_sk",
      "resonant"."store_sales_agg_profit" - coalesce("resonant"."store_ret_agg_r_loss",0) as "_store_res_profit",
      "resonant"."store_sales_agg_sales" as "_store_res_sales",
      "resonant"."store_sales_agg_sk" as "_store_res_outlet",
      "resonant"."store_sales_agg_sk" as "store_sales_agg_sk",
      coalesce("resonant"."store_ret_agg_r_amt",0) as "_store_res_returns"
  FROM
      "resonant"),
  courageous as (
  SELECT
      "abhorrent"."_store_res_channel" as "___tvf_arm_0_channel",
      "dapper"."_store_res_profit" as "___tvf_arm_0_profit",
      "dapper"."_store_res_returns" as "___tvf_arm_0_returns",
      "dapper"."_store_res_sales" as "___tvf_arm_0_sales",
      coalesce("abhorrent"."store_sales_agg_sk","dapper"."_store_res_outlet","dapper"."store_sales_agg_sk") as "___tvf_arm_0_outlet"
  FROM
      "dapper"
      FULL JOIN "abhorrent" on "dapper"."_store_res_outlet" = "abhorrent"."store_sales_agg_sk"
  WHERE
      "dapper"."_virt_presence_9158607131677881" is not null
  ),
  yummy as (
  SELECT
      "cs_catalog_sales"."cs_call_center_sk" as "_catalog_sales_agg_sk",
      sum("cs_catalog_sales"."cs_ext_sales_price") as "_catalog_sales_agg_sales",
      sum("cs_catalog_sales"."cs_net_profit") as "_catalog_sales_agg_profit"
  FROM
      "catalog_sales" as "cs_catalog_sales"
      INNER JOIN "date_dim" as "cs_sold_date_date_dim" on "cs_catalog_sales"."cs_sold_date_sk" = "cs_sold_date_date_dim"."d_date_sk"
  WHERE
      "cs_sold_date_date_dim"."d_date" BETWEEN date '2000-08-23' AND date '2000-09-22' and "cs_catalog_sales"."cs_call_center_sk" is not null

  GROUP BY
      1),
  concerned as (
  SELECT
      "yummy"."_catalog_sales_agg_profit" as "catalog_sales_agg_profit",
      "yummy"."_catalog_sales_agg_sales" as "catalog_sales_agg_sales",
      "yummy"."_catalog_sales_agg_sk" as "catalog_sales_agg_sk",
      coalesce("yummy"."_catalog_sales_agg_sk") as "_virt_presence_9447600895504718"
  FROM
      "yummy"),
  cheerful as (
  SELECT
      "cr_catalog_returns"."cr_call_center_sk" as "catalog_ret_agg_sk",
      sum("cr_catalog_returns"."cr_net_loss") as "catalog_ret_agg_r_loss",
      sum("cr_catalog_returns"."cr_return_amount") as "catalog_ret_agg_r_amt"
  FROM
      "item" as "cr_item_item"
      LEFT OUTER JOIN "catalog_returns" as "cr_catalog_returns" on "cr_item_item"."i_item_sk" = "cr_catalog_returns"."cr_item_sk"
      RIGHT OUTER JOIN "date_dim" as "cr_date_dim_date_dim" on "cr_catalog_returns"."cr_returned_date_sk" = "cr_date_dim_date_dim"."d_date_sk"
  WHERE
      "cr_date_dim_date_dim"."d_date" BETWEEN date '2000-08-23' AND date '2000-09-22'

  GROUP BY
      1),
  young as (
  SELECT
      "cheerful"."catalog_ret_agg_r_amt" as "catalog_ret_agg_r_amt",
      "cheerful"."catalog_ret_agg_r_loss" as "catalog_ret_agg_r_loss",
      "concerned"."_virt_presence_9447600895504718" as "_virt_presence_9447600895504718",
      "concerned"."catalog_sales_agg_profit" as "catalog_sales_agg_profit",
      "concerned"."catalog_sales_agg_sales" as "catalog_sales_agg_sales",
      coalesce("cheerful"."catalog_ret_agg_sk","concerned"."catalog_sales_agg_sk") as "catalog_ret_agg_sk",
      coalesce("cheerful"."catalog_ret_agg_sk","concerned"."catalog_sales_agg_sk") as "catalog_sales_agg_sk"
  FROM
      "concerned"
      LEFT OUTER JOIN "cheerful" on "concerned"."catalog_sales_agg_sk" is not distinct from "cheerful"."catalog_ret_agg_sk"
  WHERE
      "concerned"."_virt_presence_9447600895504718" is not null
  ),
  sparkling as (
  SELECT
      "young"."_virt_presence_9447600895504718" as "_virt_presence_9447600895504718",
      "young"."catalog_ret_agg_sk" as "catalog_ret_agg_sk",
      "young"."catalog_sales_agg_profit" - coalesce("young"."catalog_ret_agg_r_loss",0) as "_catalog_res_profit",
      "young"."catalog_sales_agg_sales" as "_catalog_res_sales",
      "young"."catalog_sales_agg_sk" as "_catalog_res_outlet",
      "young"."catalog_sales_agg_sk" as "catalog_sales_agg_sk",
      coalesce("young"."catalog_ret_agg_r_amt",0) as "_catalog_res_returns"
  FROM
      "young"),
  sweltering as (
  SELECT
      "abhorrent"."_catalog_res_channel" as "___tvf_arm_1_channel",
      "sparkling"."_catalog_res_outlet" as "___tvf_arm_1_outlet",
      "sparkling"."_catalog_res_profit" as "___tvf_arm_1_profit",
      "sparkling"."_catalog_res_returns" as "___tvf_arm_1_returns",
      "sparkling"."_catalog_res_sales" as "___tvf_arm_1_sales"
  FROM
      "sparkling"
      FULL JOIN "abhorrent" on "sparkling"."catalog_sales_agg_sk" = "abhorrent"."catalog_sales_agg_sk"
  WHERE
      "sparkling"."_virt_presence_9447600895504718" is not null
  ),
  sloppy as (
  SELECT
      "sweltering"."___tvf_arm_1_channel" as "_all_channels_channel",
      "sweltering"."___tvf_arm_1_outlet" as "_all_channels_outlet",
      "sweltering"."___tvf_arm_1_sales" as "_all_channels_sales",
      "sweltering"."___tvf_arm_1_returns" as "_all_channels_returns",
      "sweltering"."___tvf_arm_1_profit" as "_all_channels_profit"
  FROM
      "sweltering"
  UNION ALL
  SELECT
      "courageous"."___tvf_arm_0_channel" as "_all_channels_channel",
      "courageous"."___tvf_arm_0_outlet" as "_all_channels_outlet",
      "courageous"."___tvf_arm_0_sales" as "_all_channels_sales",
      "courageous"."___tvf_arm_0_returns" as "_all_channels_returns",
      "courageous"."___tvf_arm_0_profit" as "_all_channels_profit"
  FROM
      "courageous"
  UNION ALL
  SELECT
      "brave"."___tvf_arm_2_channel" as "_all_channels_channel",
      "brave"."___tvf_arm_2_outlet" as "_all_channels_outlet",
      "brave"."___tvf_arm_2_sales" as "_all_channels_sales",
      "brave"."___tvf_arm_2_returns" as "_all_channels_returns",
      "brave"."___tvf_arm_2_profit" as "_all_channels_profit"
  FROM
      "brave"),
  obsolete as (
  SELECT
      "sloppy"."_all_channels_channel" as "all_channels_channel",
      "sloppy"."_all_channels_outlet" as "all_channels_outlet",
      "sloppy"."_all_channels_profit" as "all_channels_profit",
      "sloppy"."_all_channels_returns" as "all_channels_returns",
      "sloppy"."_all_channels_sales" as "all_channels_sales"
  FROM
      "sloppy")
  SELECT
      "obsolete"."all_channels_channel" as "all_channels_channel",
      "obsolete"."all_channels_outlet" as "all_channels_outlet",
      sum("obsolete"."all_channels_sales") as "sales",
      sum("obsolete"."all_channels_returns") as "returns",
      sum("obsolete"."all_channels_profit") as "profit"
  FROM
      "obsolete"
  GROUP BY
      ROLLUP (1, 2)
  ORDER BY
      "obsolete"."all_channels_channel" asc nulls first,
      "obsolete"."all_channels_outlet" asc nulls first,
      "returns" desc nulls last
  LIMIT (100)
  ```
- `trilogy run answer_3210116865.preql`

  ```text
  Unexpected error in answer_3210116865.preql: Could not render the query: Missing source reference to cs.call_center.call_center_sk; Missing source reference to ss.store.store_sk; Missing source reference to ws.web_page.web_page_sk. A planned reference has no backing source CTE -- typically an unsupported cross-rowset or membership shape the planner could not wire. Review the rowset/join structure (or file an issue if the query looks valid).

  Full SQL with sentinel(s):

  WITH
  trite as (
  SELECT
      "ws_web_sales"."ws_web_page_sk" as "_web_sales_agg_sk",
      sum("ws_web_sales"."ws_ext_sales_price") as "_web_sales_agg_sales",
      sum("ws_web_sales"."ws_net_profit") as "_web_sales_agg_profit"
  FROM
      "web_sales" as "ws_web_sales"
      INNER JOIN "date_dim" as "ws_sold_date_date_dim" on "ws_web_sales"."ws_sold_date_sk" = "ws_sold_date_date_dim"."d_date_sk"
  WHERE
      "ws_sold_date_date_dim"."d_date" BETWEEN date '2000-08-23' AND date '2000-09-22' and "ws_web_sales"."ws_web_page_sk" is not null

  GROUP BY
      1),
  round as (
  SELECT
      "trite"."_web_sales_agg_profit" as "web_sales_agg_profit",
      "trite"."_web_sales_agg_sales" as "web_sales_agg_sales",
      "trite"."_web_sales_agg_sk" as "web_sales_agg_sk",
      coalesce("trite"."_web_sales_agg_sk") as "_virt_presence_4118804935211818"
  FROM
      "trite"),
  wooden as (
  SELECT
      "wr_web_returns"."wr_web_page_sk" as "web_ret_agg_sk",
      sum("wr_web_returns"."wr_net_loss") as "web_ret_agg_r_loss",
      sum("wr_web_returns"."wr_return_amt") as "web_ret_agg_r_amt"
  FROM
      "item" as "wr_item_item"
      LEFT OUTER JOIN "web_returns" as "wr_web_returns" on "wr_item_item"."i_item_sk" = "wr_web_returns"."wr_item_sk"
      RIGHT OUTER JOIN "date_dim" as "wr_date_dim_date_dim" on "wr_web_returns"."wr_returned_date_sk" = "wr_date_dim_date_dim"."d_date_sk"
  WHERE
      "wr_date_dim_date_dim"."d_date" BETWEEN date '2000-08-23' AND date '2000-09-22'

  GROUP BY
      1),
  slow as (
  SELECT
      "round"."_virt_presence_4118804935211818" as "_virt_presence_4118804935211818",
      "round"."web_sales_agg_profit" as "web_sales_agg_profit",
      "round"."web_sales_agg_sales" as "web_sales_agg_sales",
      "wooden"."web_ret_agg_r_amt" as "web_ret_agg_r_amt",
      "wooden"."web_ret_agg_r_loss" as "web_ret_agg_r_loss",
      coalesce("round"."web_sales_agg_sk","wooden"."web_ret_agg_sk") as "web_ret_agg_sk",
      coalesce("round"."web_sales_agg_sk","wooden"."web_ret_agg_sk") as "web_sales_agg_sk"
  FROM
      "round"
      LEFT OUTER JOIN "wooden" on "round"."web_sales_agg_sk" is not distinct from "wooden"."web_ret_agg_sk"
  WHERE
      "round"."_virt_presence_4118804935211818" is not null
  ),
  spiritual as (
  SELECT
      "slow"."_virt_presence_4118804935211818" as "_virt_presence_4118804935211818",
      "slow"."web_sales_agg_profit" - coalesce("slow"."web_ret_agg_r_loss",0) as "_web_res_profit",
      "slow"."web_sales_agg_sales" as "_web_res_sales",
      cast("slow"."web_sales_agg_sk" as bigint) as "_web_res_outlet",
      coalesce("slow"."web_ret_agg_r_amt",0) as "_web_res_returns"
  FROM
      "slow"),
  abhorrent as (
  SELECT
      :_catalog_res_channel as "_catalog_res_channel",
      :_store_res_channel as "_store_res_channel",
      :_web_res_channel as "_web_res_channel",
      coalesce(INVALID_REFERENCE_BUG<Missing source reference to cs.call_center.call_center_sk>) as "_virt_presence_9447600895504718",
      coalesce(INVALID_REFERENCE_BUG<Missing source reference to ss.store.store_sk>) as "_virt_presence_9158607131677881",
      coalesce(INVALID_REFERENCE_BUG<Missing source reference to ws.web_page.web_page_sk>) as "_virt_presence_4118804935211818"
  ),
  brave as (
  SELECT
      "abhorrent"."_web_res_channel" as "___tvf_arm_2_channel",
      "spiritual"."_web_res_outlet" as "___tvf_arm_2_outlet",
      "spiritual"."_web_res_profit" as "___tvf_arm_2_profit",
      "spiritual"."_web_res_returns" as "___tvf_arm_2_returns",
      "spiritual"."_web_res_sales" as "___tvf_arm_2_sales"
  FROM
      "spiritual"
      INNER JOIN "abhorrent" on "spiritual"."_virt_presence_4118804935211818" = "abhorrent"."_virt_presence_4118804935211818"
  WHERE
      "spiritual"."_virt_presence_4118804935211818" is not null

  GROUP BY
      1,
      2,
      3,
      4,
      5),
  puffy as (
  SELECT
      coalesce("ss_store_sales"."ss_store_sk","ss_store_store"."s_store_sk") as "_store_sales_agg_sk",
      sum("ss_store_sales"."ss_ext_sales_price") as "_store_sales_agg_sales",
      sum("ss_store_sales"."ss_net_profit") as "_store_sales_agg_profit"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "store" as "ss_store_store" on "ss_store_sales"."ss_store_sk" = "ss_store_store"."s_store_sk"
      INNER JOIN "date_dim" as "ss_date_dim_date_dim" on "ss_store_sales"."ss_sold_date_sk" = "ss_date_dim_date_dim"."d_date_sk"
  WHERE
      "ss_date_dim_date_dim"."d_date" BETWEEN date '2000-08-23' AND date '2000-09-22' and coalesce("ss_store_sales"."ss_store_sk","ss_store_store"."s_store_sk") is not null

  GROUP BY
      1),
  yellow as (
  SELECT
      "puffy"."_store_sales_agg_profit" as "store_sales_agg_profit",
      "puffy"."_store_sales_agg_sales" as "store_sales_agg_sales",
      "puffy"."_store_sales_agg_sk" as "store_sales_agg_sk",
      coalesce("puffy"."_store_sales_agg_sk") as "_virt_presence_9158607131677881"
  FROM
      "puffy"),
  busy as (
  SELECT
      coalesce("sr_store_returns"."sr_store_sk","sr_store_store"."s_store_sk") as "store_ret_agg_sk",
      sum("sr_store_returns"."sr_net_loss") as "store_ret_agg_r_loss",
      sum("sr_store_returns"."sr_return_amt") as "store_ret_agg_r_amt"
  FROM
      "store_returns" as "sr_store_returns"
      FULL JOIN "store" as "sr_store_store" on "sr_store_returns"."sr_store_sk" = "sr_store_store"."s_store_sk"
      RIGHT OUTER JOIN "date_dim" as "sr_date_dim_date_dim" on "sr_store_returns"."sr_returned_date_sk" = "sr_date_dim_date_dim"."d_date_sk"
  WHERE
      "sr_date_dim_date_dim"."d_date" BETWEEN date '2000-08-23' AND date '2000-09-22'

  GROUP BY
      1),
  resonant as (
  SELECT
      "busy"."store_ret_agg_r_amt" as "store_ret_agg_r_amt",
      "busy"."store_ret_agg_r_loss" as "store_ret_agg_r_loss",
      "yellow"."_virt_presence_9158607131677881" as "_virt_presence_9158607131677881",
      "yellow"."store_sales_agg_profit" as "store_sales_agg_profit",
      "yellow"."store_sales_agg_sales" as "store_sales_agg_sales",
      coalesce("busy"."store_ret_agg_sk","yellow"."store_sales_agg_sk") as "store_ret_agg_sk",
      coalesce("busy"."store_ret_agg_sk","yellow"."store_sales_agg_sk") as "store_sales_agg_sk"
  FROM
      "yellow"
      LEFT OUTER JOIN "busy" on "yellow"."store_sales_agg_sk" is not distinct from "busy"."store_ret_agg_sk"
  WHERE
      "yellow"."_virt_presence_9158607131677881" is not null
  ),
  dapper as (
  SELECT
      "resonant"."_virt_presence_9158607131677881" as "_virt_presence_9158607131677881",
      "resonant"."store_sales_agg_profit" - coalesce("resonant"."store_ret_agg_r_loss",0) as "_store_res_profit",
      "resonant"."store_sales_agg_sales" as "_store_res_sales",
      cast("resonant"."store_sales_agg_sk" as bigint) as "_store_res_outlet",
      coalesce("resonant"."store_ret_agg_r_amt",0) as "_store_res_returns"
  FROM
      "resonant"),
  courageous as (
  SELECT
      "abhorrent"."_store_res_channel" as "___tvf_arm_0_channel",
      "dapper"."_store_res_outlet" as "___tvf_arm_0_outlet",
      "dapper"."_store_res_profit" as "___tvf_arm_0_profit",
      "dapper"."_store_res_returns" as "___tvf_arm_0_returns",
      "dapper"."_store_res_sales" as "___tvf_arm_0_sales"
  FROM
      "dapper"
      INNER JOIN "abhorrent" on "dapper"."_virt_presence_9158607131677881" = "abhorrent"."_virt_presence_9158607131677881"
  WHERE
      "dapper"."_virt_presence_9158607131677881" is not null

  GROUP BY
      1,
      2,
      3,
      4,
      5),
  yummy as (
  SELECT
      "cs_catalog_sales"."cs_call_center_sk" as "_catalog_sales_agg_sk",
      sum("cs_catalog_sales"."cs_ext_sales_price") as "_catalog_sales_agg_sales",
      sum("cs_catalog_sales"."cs_net_profit") as "_catalog_sales_agg_profit"
  FROM
      "catalog_sales" as "cs_catalog_sales"
      INNER JOIN "date_dim" as "cs_sold_date_date_dim" on "cs_catalog_sales"."cs_sold_date_sk" = "cs_sold_date_date_dim"."d_date_sk"
  WHERE
      "cs_sold_date_date_dim"."d_date" BETWEEN date '2000-08-23' AND date '2000-09-22' and "cs_catalog_sales"."cs_call_center_sk" is not null

  GROUP BY
      1),
  concerned as (
  SELECT
      "yummy"."_catalog_sales_agg_profit" as "catalog_sales_agg_profit",
      "yummy"."_catalog_sales_agg_sales" as "catalog_sales_agg_sales",
      "yummy"."_catalog_sales_agg_sk" as "catalog_sales_agg_sk",
      coalesce("yummy"."_catalog_sales_agg_sk") as "_virt_presence_9447600895504718"
  FROM
      "yummy"),
  cheerful as (
  SELECT
      "cr_catalog_returns"."cr_call_center_sk" as "catalog_ret_agg_sk",
      sum("cr_catalog_returns"."cr_net_loss") as "catalog_ret_agg_r_loss",
      sum("cr_catalog_returns"."cr_return_amount") as "catalog_ret_agg_r_amt"
  FROM
      "item" as "cr_item_item"
      LEFT OUTER JOIN "catalog_returns" as "cr_catalog_returns" on "cr_item_item"."i_item_sk" = "cr_catalog_returns"."cr_item_sk"
      RIGHT OUTER JOIN "date_dim" as "cr_date_dim_date_dim" on "cr_catalog_returns"."cr_returned_date_sk" = "cr_date_dim_date_dim"."d_date_sk"
  WHERE
      "cr_date_dim_date_dim"."d_date" BETWEEN date '2000-08-23' AND date '2000-09-22'

  GROUP BY
      1),
  young as (
  SELECT
      "cheerful"."catalog_ret_agg_r_amt" as "catalog_ret_agg_r_amt",
      "cheerful"."catalog_ret_agg_r_loss" as "catalog_ret_agg_r_loss",
      "concerned"."_virt_presence_9447600895504718" as "_virt_presence_9447600895504718",
      "concerned"."catalog_sales_agg_profit" as "catalog_sales_agg_profit",
      "concerned"."catalog_sales_agg_sales" as "catalog_sales_agg_sales",
      coalesce("cheerful"."catalog_ret_agg_sk","concerned"."catalog_sales_agg_sk") as "catalog_ret_agg_sk",
      coalesce("cheerful"."catalog_ret_agg_sk","concerned"."catalog_sales_agg_sk") as "catalog_sales_agg_sk"
  FROM
      "concerned"
      LEFT OUTER JOIN "cheerful" on "concerned"."catalog_sales_agg_sk" is not distinct from "cheerful"."catalog_ret_agg_sk"
  WHERE
      "concerned"."_virt_presence_9447600895504718" is not null
  ),
  sparkling as (
  SELECT
      "young"."_virt_presence_9447600895504718" as "_virt_presence_9447600895504718",
      "young"."catalog_sales_agg_profit" - coalesce("young"."catalog_ret_agg_r_loss",0) as "_catalog_res_profit",
      "young"."catalog_sales_agg_sales" as "_catalog_res_sales",
      cast("young"."catalog_sales_agg_sk" as bigint) as "_catalog_res_outlet",
      coalesce("young"."catalog_ret_agg_r_amt",0) as "_catalog_res_returns"
  FROM
      "young"),
  sweltering as (
  SELECT
      "abhorrent"."_catalog_res_channel" as "___tvf_arm_1_channel",
      "sparkling"."_catalog_res_outlet" as "___tvf_arm_1_outlet",
      "sparkling"."_catalog_res_profit" as "___tvf_arm_1_profit",
      "sparkling"."_catalog_res_returns" as "___tvf_arm_1_returns",
      "sparkling"."_catalog_res_sales" as "___tvf_arm_1_sales"
  FROM
      "sparkling"
      INNER JOIN "abhorrent" on "sparkling"."_virt_presence_9447600895504718" = "abhorrent"."_virt_presence_9447600895504718"
  WHERE
      "sparkling"."_virt_presence_9447600895504718" is not null

  GROUP BY
      1,
      2,
      3,
      4,
      5),
  sloppy as (
  SELECT
      "sweltering"."___tvf_arm_1_channel" as "_all_channels_channel",
      "sweltering"."___tvf_arm_1_outlet" as "_all_channels_outlet",
      "sweltering"."___tvf_arm_1_sales" as "_all_channels_sales",
      "sweltering"."___tvf_arm_1_returns" as "_all_channels_returns",
      "sweltering"."___tvf_arm_1_profit" as "_all_channels_profit"
  FROM
      "sweltering"
  UNION ALL
  SELECT
      "courageous"."___tvf_arm_0_channel" as "_all_channels_channel",
      "courageous"."___tvf_arm_0_outlet" as "_all_channels_outlet",
      "courageous"."___tvf_arm_0_sales" as "_all_channels_sales",
      "courageous"."___tvf_arm_0_returns" as "_all_channels_returns",
      "courageous"."___tvf_arm_0_profit" as "_all_channels_profit"
  FROM
      "courageous"
  UNION ALL
  SELECT
      "brave"."___tvf_arm_2_channel" as "_all_channels_channel",
      "brave"."___tvf_arm_2_outlet" as "_all_channels_outlet",
      "brave"."___tvf_arm_2_sales" as "_all_channels_sales",
      "brave"."___tvf_arm_2_returns" as "_all_channels_returns",
      "brave"."___tvf_arm_2_profit" as "_all_channels_profit"
  FROM
      "brave"),
  obsolete as (
  SELECT
      "sloppy"."_all_channels_channel" as "all_channels_channel",
      "sloppy"."_all_channels_outlet" as "all_channels_outlet",
      "sloppy"."_all_channels_profit" as "all_channels_profit",
      "sloppy"."_all_channels_returns" as "all_channels_returns",
      "sloppy"."_all_channels_sales" as "all_channels_sales"
  FROM
      "sloppy")
  SELECT
      "obsolete"."all_channels_channel" as "all_channels_channel",
      "obsolete"."all_channels_outlet" as "all_channels_outlet",
      sum("obsolete"."all_channels_sales") as "sales",
      sum("obsolete"."all_channels_returns") as "returns",
      sum("obsolete"."all_channels_profit") as "profit"
  FROM
      "obsolete"
  GROUP BY
      ROLLUP (1, 2)
  ORDER BY
      "obsolete"."all_channels_channel" asc nulls first,
      "obsolete"."all_channels_outlet" asc nulls first,
      "returns" desc nulls last
  LIMIT (100)
  ```
- `trilogy run t03.preql`

  ```text
  Unexpected error in t03.preql: Could not render the query: Missing source reference to cs.call_center.call_center_sk; Missing source reference to ss.store.store_sk. A planned reference has no backing source CTE -- typically an unsupported cross-rowset or membership shape the planner could not wire. Review the rowset/join structure (or file an issue if the query looks valid).

  Full SQL with sentinel(s):

  WITH
  waggish as (
  SELECT
      coalesce("ss_store_sales"."ss_store_sk","ss_store_store"."s_store_sk") as "_ss_agg_sk",
      sum("ss_store_sales"."ss_ext_sales_price") as "_ss_agg_sales",
      sum("ss_store_sales"."ss_net_profit") as "_ss_agg_profit"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "store" as "ss_store_store" on "ss_store_sales"."ss_store_sk" = "ss_store_store"."s_store_sk"
      INNER JOIN "date_dim" as "ss_date_dim_date_dim" on "ss_store_sales"."ss_sold_date_sk" = "ss_date_dim_date_dim"."d_date_sk"
  WHERE
      "ss_date_dim_date_dim"."d_date" BETWEEN date '2000-08-23' AND date '2000-09-22' and coalesce("ss_store_sales"."ss_store_sk","ss_store_store"."s_store_sk") is not null

  GROUP BY
      1),
  hard as (
  SELECT
      "waggish"."_ss_agg_profit" as "ss_agg_profit",
      "waggish"."_ss_agg_sales" as "ss_agg_sales",
      "waggish"."_ss_agg_sk" as "ss_agg_sk",
      coalesce("waggish"."_ss_agg_sk") as "_virt_presence_5572074822870325"
  FROM
      "waggish"),
  kaput as (
  SELECT
      coalesce("sr_store_returns"."sr_store_sk","sr_store_store"."s_store_sk") as "sr_agg_sk",
      sum("sr_store_returns"."sr_net_loss") as "sr_agg_r_loss",
      sum("sr_store_returns"."sr_return_amt") as "sr_agg_r_amt"
  FROM
      "store_returns" as "sr_store_returns"
      FULL JOIN "store" as "sr_store_store" on "sr_store_returns"."sr_store_sk" = "sr_store_store"."s_store_sk"
      RIGHT OUTER JOIN "date_dim" as "sr_date_dim_date_dim" on "sr_store_returns"."sr_returned_date_sk" = "sr_date_dim_date_dim"."d_date_sk"
  WHERE
      "sr_date_dim_date_dim"."d_date" BETWEEN date '2000-08-23' AND date '2000-09-22'

  GROUP BY
      1),
  sedate as (
  SELECT
      "hard"."_virt_presence_5572074822870325" as "_virt_presence_5572074822870325",
      "hard"."ss_agg_profit" as "ss_agg_profit",
      "hard"."ss_agg_sales" as "ss_agg_sales",
      "kaput"."sr_agg_r_amt" as "sr_agg_r_amt",
      "kaput"."sr_agg_r_loss" as "sr_agg_r_loss",
      coalesce("hard"."ss_agg_sk","kaput"."sr_agg_sk") as "sr_agg_sk",
      coalesce("hard"."ss_agg_sk","kaput"."sr_agg_sk") as "ss_agg_sk"
  FROM
      "hard"
      LEFT OUTER JOIN "kaput" on "hard"."ss_agg_sk" is not distinct from "kaput"."sr_agg_sk"
  WHERE
      "hard"."_virt_presence_5572074822870325" is not null
  ),
  yellow as (
  SELECT
      "sedate"."_virt_presence_5572074822870325" as "_virt_presence_5572074822870325",
      "sedate"."sr_agg_sk" as "sr_agg_sk",
      "sedate"."ss_agg_profit" - coalesce("sedate"."sr_agg_r_loss",0) as "___tvf_arm_0_p",
      "sedate"."ss_agg_sales" as "___tvf_arm_0_s",
      "sedate"."ss_agg_sk" as "___tvf_arm_0_o",
      "sedate"."ss_agg_sk" as "ss_agg_sk",
      coalesce("sedate"."sr_agg_r_amt",0) as "___tvf_arm_0_r"
  FROM
      "sedate"),
  abhorrent as (
  SELECT
      :___tvf_arm_0_ch as "___tvf_arm_0_ch",
      :___tvf_arm_1_ch as "___tvf_arm_1_ch",
      INVALID_REFERENCE_BUG<Missing source reference to cs.call_center.call_center_sk> as "cs_agg_sk",
      INVALID_REFERENCE_BUG<Missing source reference to ss.store.store_sk> as "ss_agg_sk"
  ),
  resonant as (
  SELECT
      "abhorrent"."___tvf_arm_0_ch" as "___tvf_arm_0_ch",
      "yellow"."___tvf_arm_0_p" as "___tvf_arm_0_p",
      "yellow"."___tvf_arm_0_r" as "___tvf_arm_0_r",
      "yellow"."___tvf_arm_0_s" as "___tvf_arm_0_s",
      coalesce("abhorrent"."ss_agg_sk","yellow"."___tvf_arm_0_o","yellow"."ss_agg_sk") as "___tvf_arm_0_o"
  FROM
      "yellow"
      FULL JOIN "abhorrent" on "yellow"."___tvf_arm_0_o" = "abhorrent"."ss_agg_sk"
  WHERE
      "yellow"."_virt_presence_5572074822870325" is not null
  ),
  yummy as (
  SELECT
      "cs_catalog_sales"."cs_call_center_sk" as "_cs_agg_sk",
      sum("cs_catalog_sales"."cs_ext_sales_price") as "_cs_agg_sales",
      sum("cs_catalog_sales"."cs_net_profit") as "_cs_agg_profit"
  FROM
      "catalog_sales" as "cs_catalog_sales"
      INNER JOIN "date_dim" as "cs_sold_date_date_dim" on "cs_catalog_sales"."cs_sold_date_sk" = "cs_sold_date_date_dim"."d_date_sk"
  WHERE
      "cs_sold_date_date_dim"."d_date" BETWEEN date '2000-08-23' AND date '2000-09-22' and "cs_catalog_sales"."cs_call_center_sk" is not null

  GROUP BY
      1),
  concerned as (
  SELECT
      "yummy"."_cs_agg_profit" as "cs_agg_profit",
      "yummy"."_cs_agg_sales" as "cs_agg_sales",
      "yummy"."_cs_agg_sk" as "cs_agg_sk",
      coalesce("yummy"."_cs_agg_sk") as "_virt_presence_573701124787251"
  FROM
      "yummy"),
  cheerful as (
  SELECT
      "cr_catalog_returns"."cr_call_center_sk" as "cr_agg_sk",
      sum("cr_catalog_returns"."cr_net_loss") as "cr_agg_r_loss",
      sum("cr_catalog_returns"."cr_return_amount") as "cr_agg_r_amt"
  FROM
      "item" as "cr_item_item"
      LEFT OUTER JOIN "catalog_returns" as "cr_catalog_returns" on "cr_item_item"."i_item_sk" = "cr_catalog_returns"."cr_item_sk"
      RIGHT OUTER JOIN "date_dim" as "cr_date_dim_date_dim" on "cr_catalog_returns"."cr_returned_date_sk" = "cr_date_dim_date_dim"."d_date_sk"
  WHERE
      "cr_date_dim_date_dim"."d_date" BETWEEN date '2000-08-23' AND date '2000-09-22'

  GROUP BY
      1),
  young as (
  SELECT
      "cheerful"."cr_agg_r_amt" as "cr_agg_r_amt",
      "cheerful"."cr_agg_r_loss" as "cr_agg_r_loss",
      "concerned"."_virt_presence_573701124787251" as "_virt_presence_573701124787251",
      "concerned"."cs_agg_profit" as "cs_agg_profit",
      "concerned"."cs_agg_sales" as "cs_agg_sales",
      coalesce("cheerful"."cr_agg_sk","concerned"."cs_agg_sk") as "cr_agg_sk",
      coalesce("cheerful"."cr_agg_sk","concerned"."cs_agg_sk") as "cs_agg_sk"
  FROM
      "concerned"
      LEFT OUTER JOIN "cheerful" on "concerned"."cs_agg_sk" is not distinct from "cheerful"."cr_agg_sk"
  WHERE
      "concerned"."_virt_presence_573701124787251" is not null
  ),
  sparkling as (
  SELECT
      "young"."_virt_presence_573701124787251" as "_virt_presence_573701124787251",
      "young"."cr_agg_sk" as "cr_agg_sk",
      "young"."cs_agg_profit" - coalesce("young"."cr_agg_r_loss",0) as "___tvf_arm_1_p",
      "young"."cs_agg_sales" as "___tvf_arm_1_s",
      "young"."cs_agg_sk" as "___tvf_arm_1_o",
      "young"."cs_agg_sk" as "cs_agg_sk",
      coalesce("young"."cr_agg_r_amt",0) as "___tvf_arm_1_r"
  FROM
      "young"),
  sweltering as (
  SELECT
      "abhorrent"."___tvf_arm_1_ch" as "___tvf_arm_1_ch",
      "sparkling"."___tvf_arm_1_o" as "___tvf_arm_1_o",
      "sparkling"."___tvf_arm_1_p" as "___tvf_arm_1_p",
      "sparkling"."___tvf_arm_1_r" as "___tvf_arm_1_r",
      "sparkling"."___tvf_arm_1_s" as "___tvf_arm_1_s"
  FROM
      "sparkling"
      FULL JOIN "abhorrent" on "sparkling"."cs_agg_sk" = "abhorrent"."cs_agg_sk"
  WHERE
      "sparkling"."_virt_presence_573701124787251" is not null
  ),
  courageous as (
  SELECT
      "sweltering"."___tvf_arm_1_ch" as "_allx_ch",
      "sweltering"."___tvf_arm_1_o" as "_allx_o",
      "sweltering"."___tvf_arm_1_s" as "_allx_s",
      "sweltering"."___tvf_arm_1_r" as "_allx_r",
      "sweltering"."___tvf_arm_1_p" as "_allx_p"
  FROM
      "sweltering"
  UNION ALL
  SELECT
      "resonant"."___tvf_arm_0_ch" as "_allx_ch",
      "resonant"."___tvf_arm_0_o" as "_allx_o",
      "resonant"."___tvf_arm_0_s" as "_allx_s",
      "resonant"."___tvf_arm_0_r" as "_allx_r",
      "resonant"."___tvf_arm_0_p" as "_allx_p"
  FROM
      "resonant")
  SELECT
      "courageous"."_allx_ch" as "allx_ch",
      "courageous"."_allx_o" as "allx_o",
      "courageous"."_allx_s" as "allx_s",
      "courageous"."_allx_r" as "allx_r",
      "courageous"."_allx_p" as "allx_p"
  FROM
      "courageous"
  ```
- `trilogy run probe.preql`

  ```text
  Unexpected error in probe.preql: name 'nonstandard_grouping_lineage' is not defined
  ```
- `trilogy run answer_1226264875.preql`

  ```text
  Unexpected error in answer_1226264875.preql: name 'nonstandard_grouping_lineage' is not defined
  ```
- `trilogy run probe4.preql`

  ```text
  Unexpected error in probe4.preql: name 'nonstandard_grouping_lineage' is not defined
  ```
- `trilogy run explore_tmp2.preql`

  ```text
  trilogy error: subprocess timed out after 600s.
  ```
- `trilogy run answer_2910545909.preql`

  ```text
  Unexpected error in answer_2910545909.preql: Could not render the query: Missing source reference to ss.customer.last_name; Missing source reference to ss.customer.first_name. A planned reference has no backing source CTE -- typically an unsupported cross-rowset or membership shape the planner could not wire. Review the rowset/join structure (or file an issue if the query looks valid).

  Full SQL with sentinel(s):

  WITH
  abundant as (
  SELECT
      "ss_store_sales"."ss_coupon_amt" as "ss_coupon_amt",
      "ss_store_sales"."ss_net_profit" as "ss_net_profit",
      "ss_store_sales"."ss_ticket_number" as "ss_ticket_number",
      "ss_store_store"."s_city" as "ss_store_city",
      coalesce("ss_customer_customer"."c_customer_sk","ss_store_sales"."ss_customer_sk") as "ss_customer_customer_sk"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "store" as "ss_store_store" on "ss_store_sales"."ss_store_sk" = "ss_store_store"."s_store_sk"
      INNER JOIN "date_dim" as "ss_date_dim_date_dim" on "ss_store_sales"."ss_sold_date_sk" = "ss_date_dim_date_dim"."d_date_sk"
      INNER JOIN "customer" as "ss_customer_customer" on "ss_store_sales"."ss_customer_sk" = "ss_customer_customer"."c_customer_sk"
      INNER JOIN "household_demographics" as "ss_household_demographics_household_demographics" on "ss_store_sales"."ss_hdemo_sk" = "ss_household_demographics_household_demographics"."hd_demo_sk"
  WHERE
      "ss_date_dim_date_dim"."d_day_name" = 'Monday' and "ss_date_dim_date_dim"."d_year" BETWEEN 1999 AND 2001 and "ss_store_store"."s_number_employees" BETWEEN 200 AND 295 and coalesce("ss_customer_customer"."c_customer_sk","ss_store_sales"."ss_customer_sk") is not null and coalesce("ss_household_demographics_household_demographics"."hd_demo_sk","ss_store_sales"."ss_hdemo_sk") is not null and ( "ss_household_demographics_household_demographics"."hd_dep_count" = 6 or "ss_household_demographics_household_demographics"."hd_vehicle_count" > 2 )
  ),
  vacuous as (
  SELECT
      "abundant"."ss_customer_customer_sk" as "ss_customer_customer_sk",
      "abundant"."ss_store_city" as "ss_store_city",
      "abundant"."ss_ticket_number" as "ss_ticket_number",
      sum("abundant"."ss_coupon_amt") as "total_coupon_amt",
      sum("abundant"."ss_net_profit") as "total_net_profit"
  FROM
      "abundant"
  GROUP BY
      1,
      2,
      3),
  wakeful as (
  SELECT
      "ss_customer_customer"."c_customer_sk" as "ss_customer_customer_sk",
      coalesce("ss_customer_customer"."c_current_addr_sk","ss_customer_customer_address_customer_address"."ca_address_sk") as "ss_customer_customer_address_address_sk"
  FROM
      "customer_address" as "ss_customer_customer_address_customer_address"
      INNER JOIN "customer" as "ss_customer_customer" on "ss_customer_customer_address_customer_address"."ca_address_sk" = "ss_customer_customer"."c_current_addr_sk"
  WHERE
      "ss_customer_customer"."c_customer_sk" is not null
  ),
  uneven as (
  SELECT
      "abundant"."ss_store_city" as "ss_store_city",
      "abundant"."ss_ticket_number" as "ss_ticket_number",
      "wakeful"."ss_customer_customer_sk" as "ss_customer_customer_sk"
  FROM
      "wakeful"
      INNER JOIN "abundant" on "wakeful"."ss_customer_customer_sk" = "abundant"."ss_customer_customer_sk"
  GROUP BY
      1,
      2,
      3,
      "wakeful"."ss_customer_customer_address_address_sk")
  SELECT
      "vacuous"."total_coupon_amt" as "total_coupon_amt",
      "vacuous"."total_net_profit" as "total_net_profit"
  FROM
      "vacuous"
      INNER JOIN "uneven" on "vacuous"."ss_customer_customer_sk" = "uneven"."ss_customer_customer_sk" AND "vacuous"."ss_store_city" is not distinct from "uneven"."ss_store_city" AND "vacuous"."ss_ticket_number" is not distinct from "uneven"."ss_ticket_number"
  ORDER BY
      INVALID_REFERENCE_BUG<Missing source reference to ss.customer.last_name> asc nulls first,
      INVALID_REFERENCE_BUG<Missing source reference to ss.customer.first_name> asc nulls first,
      SUBSTRING("uneven"."ss_store_city",1,30) asc nulls first,
      "vacuous"."total_net_profit" asc nulls first,
      "uneven"."ss_ticket_number" asc nulls last
  LIMIT (100)
  ```
- `trilogy run explore_tmp6.preql`

  ```text
  Unexpected error in explore_tmp6.preql: Could not render the query: Missing source reference to ss.customer.last_name; Missing source reference to ss.customer.first_name. A planned reference has no backing source CTE -- typically an unsupported cross-rowset or membership shape the planner could not wire. Review the rowset/join structure (or file an issue if the query looks valid).

  Full SQL with sentinel(s):

  WITH
  abundant as (
  SELECT
      "ss_store_sales"."ss_coupon_amt" as "ss_coupon_amt",
      "ss_store_sales"."ss_net_profit" as "ss_net_profit",
      "ss_store_sales"."ss_ticket_number" as "ss_ticket_number",
      "ss_store_store"."s_city" as "ss_store_city",
      coalesce("ss_customer_customer"."c_customer_sk","ss_store_sales"."ss_customer_sk") as "ss_customer_customer_sk"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "store" as "ss_store_store" on "ss_store_sales"."ss_store_sk" = "ss_store_store"."s_store_sk"
      INNER JOIN "date_dim" as "ss_date_dim_date_dim" on "ss_store_sales"."ss_sold_date_sk" = "ss_date_dim_date_dim"."d_date_sk"
      INNER JOIN "customer" as "ss_customer_customer" on "ss_store_sales"."ss_customer_sk" = "ss_customer_customer"."c_customer_sk"
      INNER JOIN "household_demographics" as "ss_household_demographics_household_demographics" on "ss_store_sales"."ss_hdemo_sk" = "ss_household_demographics_household_demographics"."hd_demo_sk"
  WHERE
      "ss_date_dim_date_dim"."d_day_name" = 'Monday' and "ss_date_dim_date_dim"."d_year" BETWEEN 1999 AND 2001 and "ss_store_store"."s_number_employees" BETWEEN 200 AND 295 and coalesce("ss_customer_customer"."c_customer_sk","ss_store_sales"."ss_customer_sk") is not null and coalesce("ss_household_demographics_household_demographics"."hd_demo_sk","ss_store_sales"."ss_hdemo_sk") is not null and ( "ss_household_demographics_household_demographics"."hd_dep_count" = 6 or "ss_household_demographics_household_demographics"."hd_vehicle_count" > 2 )
  ),
  vacuous as (
  SELECT
      "abundant"."ss_customer_customer_sk" as "ss_customer_customer_sk",
      "abundant"."ss_store_city" as "ss_store_city",
      "abundant"."ss_ticket_number" as "ss_ticket_number",
      sum("abundant"."ss_coupon_amt") as "total_coupon_amt",
      sum("abundant"."ss_net_profit") as "total_net_profit"
  FROM
      "abundant"
  GROUP BY
      1,
      2,
      3),
  wakeful as (
  SELECT
      "ss_customer_customer"."c_customer_sk" as "ss_customer_customer_sk",
      coalesce("ss_customer_customer"."c_current_addr_sk","ss_customer_customer_address_customer_address"."ca_address_sk") as "ss_customer_customer_address_address_sk"
  FROM
      "customer_address" as "ss_customer_customer_address_customer_address"
      INNER JOIN "customer" as "ss_customer_customer" on "ss_customer_customer_address_customer_address"."ca_address_sk" = "ss_customer_customer"."c_current_addr_sk"
  WHERE
      "ss_customer_customer"."c_customer_sk" is not null
  ),
  uneven as (
  SELECT
      "abundant"."ss_store_city" as "ss_store_city",
      "abundant"."ss_ticket_number" as "ss_ticket_number",
      "wakeful"."ss_customer_customer_sk" as "ss_customer_customer_sk"
  FROM
      "wakeful"
      INNER JOIN "abundant" on "wakeful"."ss_customer_customer_sk" = "abundant"."ss_customer_customer_sk"
  GROUP BY
      1,
      2,
      3,
      "wakeful"."ss_customer_customer_address_address_sk")
  SELECT
      "vacuous"."total_coupon_amt" as "total_coupon_amt",
      "vacuous"."total_net_profit" as "total_net_profit"
  FROM
      "vacuous"
      INNER JOIN "uneven" on "vacuous"."ss_customer_customer_sk" = "uneven"."ss_customer_customer_sk" AND "vacuous"."ss_store_city" is not distinct from "uneven"."ss_store_city" AND "vacuous"."ss_ticket_number" is not distinct from "uneven"."ss_ticket_number"
  ORDER BY
      INVALID_REFERENCE_BUG<Missing source reference to ss.customer.last_name> asc nulls first,
      INVALID_REFERENCE_BUG<Missing source reference to ss.customer.first_name> asc nulls first,
      SUBSTRING("uneven"."ss_store_city",1,30) asc nulls first,
      "vacuous"."total_net_profit" asc nulls first,
      "uneven"."ss_ticket_number" asc nulls last
  LIMIT (100)
  ```
- `trilogy run probe12.preql`

  ```text
  trilogy error: subprocess timed out after 600s.
  ```
- `trilogy `

  ```text
  trilogy error: 'args' must be a list of strings.
  ```
- `trilogy run answer_426809370.preql`

  ```text
  Resolution error in answer_426809370.preql: WHERE input(s) ['cr.returning_customer.customer_address.state'] cannot be related to the query outputs ['local._qual_cust_sk', 'local._qual_rstate', 'local._qual_total']: no join or merge connects the filter's source to any output-producing source. Add a join/merge relating them, or select a concept from the filter's model.
  ```
- `trilogy run probe_union5.preql`

  ```text
  Syntax error in probe_union5.preql: Impossible comparison in divide(ref:y2002.q,ref:y2001.q) < 0.9: declared domain bigint[1..100] has no value < 0.9
  ```
- `trilogy run probe12.preql`

  ```text
  trilogy error: subprocess timed out after 600s.
  ```
- `trilogy run answer_2852230229.preql`

  ```text
  Syntax error in answer_2852230229.preql: ORDER BY references 'local.parent', which is not in the SELECT projection (line 11). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --local.parent order by local.parent asc`.
  ```
- `trilogy run probe12.preql`

  ```text
  trilogy error: subprocess timed out after 600s.
  ```

### `import-path`

- `trilogy run answer_1858999935.preql`

  ```text
  Import error in answer_1858999935.preql: Unable to import '.\web_sales.preql': [Errno 2] No such file or directory: '.\\web_sales.preql'. Did you mean: root.web_sales?
  ```
- `trilogy run answer_3863442186.preql`

  ```text
  Import error in answer_3863442186.preql: Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: root.store_sales?
  ```
- `trilogy run answer_883027685.preql`

  ```text
  Import error in answer_883027685.preql: Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: root.store_sales?
  ```
- `trilogy run answer_3697706765.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Import error in answer_3697706765.preql: Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: root.store_sales?
  ```
- `trilogy run answer_1297355556.preql`

  ```text
  Import error in answer_1297355556.preql: Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: root.store_sales?
  ```
- `trilogy run answer_1835050598.preql`

  ```text
  Import error in answer_1835050598.preql: Unable to import '.\catalog_sales.preql': [Errno 2] No such file or directory: '.\\catalog_sales.preql'. Did you mean: root.catalog_sales?
  ```
- `trilogy run answer_2133330107.preql`

  ```text
  Import error in answer_2133330107.preql: Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: root.store_sales?
  ```
- `trilogy run answer_2604809012.preql`

  ```text
  Import error in answer_2604809012.preql: Unable to import '.\inventory.preql': [Errno 2] No such file or directory: '.\\inventory.preql'. Did you mean: root.inventory?
  ```
- `trilogy run answer_3849221871.preql`

  ```text
  Import error in answer_3849221871.preql: Unable to import '.\inventory.preql': [Errno 2] No such file or directory: '.\\inventory.preql'. Did you mean: root.inventory?
  ```
- `trilogy run answer_3530408544.preql`

  ```text
  Import error in answer_3530408544.preql: Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: root.store_sales?
  ```
- `trilogy run answer_630074061.preql`

  ```text
  Import error in answer_630074061.preql: Unable to import '.\catalog_sales.preql': [Errno 2] No such file or directory: '.\\catalog_sales.preql'. Did you mean: root.catalog_sales?
  ```
- `trilogy run answer_1072197500.preql`

  ```text
  Import error in answer_1072197500.preql: Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: root.store_sales?
  ```
- `trilogy run answer_968172492.preql`

  ```text
  Import error in answer_968172492.preql: Unable to import '.\web_sales.preql': [Errno 2] No such file or directory: '.\\web_sales.preql'. Did you mean: root.web_sales?
  ```
- `trilogy run answer_1965638525.preql`

  ```text
  Import error in answer_1965638525.preql: Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: root.store_sales?
  ```
- `trilogy run answer_1821211265.preql`

  ```text
  Import error in answer_1821211265.preql: Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: root.store_sales?
  ```
- `trilogy run probe.preql`

  ```text
  Import error in probe.preql: Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: root.store_sales?
  ```
- `trilogy run answer_71623752.preql`

  ```text
  Import error in answer_71623752.preql: Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: root.store_sales?
  ```
- `trilogy run answer_609701806.preql`

  ```text
  Import error in answer_609701806.preql: Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: root.store_sales?
  ```
- `trilogy run answer_3273495117.preql`

  ```text
  Import error in answer_3273495117.preql: Unable to import '.\web_sales.preql': [Errno 2] No such file or directory: '.\\web_sales.preql'. Did you mean: root.web_sales?
  ```
- `trilogy run answer_4037365554.preql`

  ```text
  Import error in answer_4037365554.preql: Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: root.store_sales?
  ```
- `trilogy run answer_1256642802.preql`

  ```text
  Import error in answer_1256642802.preql: Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: root.store_sales?
  ```
- `trilogy run answer_3036656719.preql`

  ```text
  Import error in answer_3036656719.preql: Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: root.store_sales?
  ```
- `trilogy run answer_3063407983.preql`

  ```text
  Import error in answer_3063407983.preql: Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: root.store_sales?
  ```
- `trilogy run answer_755724379.preql`

  ```text
  Import error in answer_755724379.preql: Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: root.store_sales?
  ```
- `trilogy run answer_2091535883.preql`

  ```text
  Import error in answer_2091535883.preql: Unable to import '.\catalog_sales.preql': [Errno 2] No such file or directory: '.\\catalog_sales.preql'. Did you mean: root.catalog_sales?
  ```
- `trilogy run answer_1772060640.preql`

  ```text
  Import error in answer_1772060640.preql: Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: root.store_sales?
  ```
- `trilogy run probe_sr_date.preql`

  ```text
  Import error in probe_sr_date.preql: Unable to import '.\store_returns.preql': [Errno 2] No such file or directory: '.\\store_returns.preql'. Did you mean: root.store_returns?
  ```
- `trilogy run answer_1827331030.preql`

  ```text
  Import error in answer_1827331030.preql: Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: root.store_sales?
  ```
- `trilogy run answer_2333598260.preql`

  ```text
  Import error in answer_2333598260.preql: Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: root.store_sales?
  ```
- `trilogy run answer_3498056762.preql`

  ```text
  Import error in answer_3498056762.preql: Unable to import '.\catalog_sales.preql': [Errno 2] No such file or directory: '.\\catalog_sales.preql'. Did you mean: root.catalog_sales?
  ```

### `syntax-parse`

- `trilogy run --stdin`

  ```text
  Syntax error in stdin:  --> 1:3
    |
  1 | --stdin;
    |   ^---
    |
    = expected PURPOSE, PROPERTY, UNIQUE, or AUTO
  Location:
  -- ??? stdin;
  ```
- `trilogy file write test_web.preql --content import root.web_returns as wr;
select 
    wr.web_page.web_page_id as pid,
    count(wr.item.item_sk) as cnt
where wr.date_dim.date between '2000-08-23'::date and '2000-09-06'::date
group by wr.web_page.web_page_id
limit 5;`

  ```text
  refused to write 'test_web.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...::date and '2000-09-06'::date ??? group by wr.web_page.web_page_...
  ```
- `trilogy file write test_web4.preql --content import root.web_returns as wr;
import root.web_sales as ws;
select 
    ws.web_site.site_id as sid,
    count(wr… between '2000-08-23'::date and '2000-09-06'::date
union join wr.order_number = ws.order_number
union join wr.item.item_sk = ws.item.item_sk
limit 5;`

  ```text
  refused to write 'test_web4.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:1
    |
  7 | union join wr.order_number = ws.order_number
    | ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...::date and '2000-09-06'::date ??? union join wr.order_number = w...
  ```
- `trilogy file write answer_219203908.preql`

  ```text
  refused to write 'answer_219203908.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...limit 3;
   select t1.bid, t1.c ??? from t1;
  ```
- `trilogy file write answer_2844519538.preql`

  ```text
  refused to write 'answer_2844519538.preql': not syntactically valid Trilogy.

  Parse error:
    --> 21:43
     |
  21 |     and lower(ss.customer.birth_country) <> lower(ss.customer.customer_address.country)
     |                                           ^---
     |
     = expected sum_operator
  Location:
  ...r(ss.customer.birth_country) < ??? > lower(ss.customer.customer_a...
  ```
- `trilogy file write probe.preql`

  ```text
  refused to write 'probe.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...r = 1999 and ss.date.moy = 9
   ??? group by 1,2,3,4,5,6,7
   order
  ```
- `trilogy run -`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...where ss.date_dim.year = 2000 ??? group by county, yr order by c...
  ```
- `trilogy run -`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...here ws.sold_date.year = 2000 ??? group by county, yr order by c...
  ```
- `trilogy file write diag.preql`

  ```text
  refused to write 'diag.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a NON-NULL GRAIN KEY: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). It MUST be a key, and one that is not nullable: `count(x)` skips rows where `x` is NULL, so counting a nullable property (a name, a date, any optional field) silently undercounts. When the grain takes SEVERAL keys, name them with `grain(...)`: `count(grain(order_id, item.id))` counts order+item combinations, and `count_distinct(grain(first_name, last_name, sale_date))` counts distinct combinations - `grain()` is never NULL, so combinations with a missing member still count. For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...q is not null;

   select
       ??? count(*) as total
   with joined...
  ```
- `trilogy file write probe_years.preql --content import root.store_sales as ss;
select ss.date_dim.year as yr, count(ss.ticket_number) as c by ss.date_dim.year order by yr;
`

  ```text
  refused to write 'probe_years.preql': not syntactically valid Trilogy.

  Parse error:
   --> 2:61
    |
  2 | select ss.date_dim.year as yr, count(ss.ticket_number) as c by ss.date_dim.year order by yr;
    |                                                             ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
   count(ss.ticket_number) as c ??? by ss.date_dim.year order by y...
  ```
- `trilogy file write probe_years.preql --content import root.store_sales as ss;
select ss.date_dim.year as yr, count(ss.ticket_number) as c
by ss.date_dim.year
order by yr;
`

  ```text
  refused to write 'probe_years.preql': not syntactically valid Trilogy.

  Parse error:
   --> 3:1
    |
  3 | by ss.date_dim.year
    | ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
   count(ss.ticket_number) as c ??? by ss.date_dim.year order by y...
  ```
- `trilogy file write probe_sr_years.preql --content import root.store_returns as sr;
select sr.date_dim.year as yr, sr.item.item_sk as it, count(sr.ticket_number) as c
ext_sales
`

  ```text
  refused to write 'probe_sr_years.preql': not syntactically valid Trilogy.

  Parse error:
   --> 3:1
    |
  3 | ext_sales
    | ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
   count(sr.ticket_number) as c ??? ext_sales
  ```
- `trilogy file write probe_sr_years.preql --content import root.store_returns as sr;
select sr.date_dim.year as yr, sr.item.item_sk as it, count(sr.ticket_number) as c
`

  ```text
  refused to write 'probe_sr_years.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [202]: Missing closing semicolon? Statements must be terminated with a semicolon `;`.
  Location:
   count(sr.ticket_number) as c ???
  ```
- `trilogy file write probe_cat_net.preql --content import root.catalog_sales as cs;
import root.catalog_returns as cr;

where cs.item.category = 'Books'
select…e,0) - coalesce(cr.return_amount,0)) as net_amt,
union join cs.item.item_sk = cr.item.item_sk
union join cs.order_number = cr.order_number
limit 10;
`

  ```text
  refused to write 'probe_cat_net.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [226]: Misplaced `subset|union join`. The key looks fine - the join is in the wrong PLACE. A query-scoped join is part of a `select` statement, not a standalone statement and not a pre-`where` clause. Put it right after the select list (preferred, SQL-like): `where <filters> select <cols> subset join a.key = b.key`. The clause order is `where` -> `select` <cols> -> join(s) -> `having` -> `order by` -> `limit`; a join may also sit between `where` and `select`, but never before `where` and never on its own. Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...tem.item_sk = cr.item.item_sk ??? union join cs.order_number = c...
  ```
- `trilogy file write answer_3046445280.preql --content import root.store_sales as ss;
import root.store_returns as sr;
import root.catalog_sales as cs;
import …ot null
   and y2002.net_qty is not null
   and y2001.net_qty <> 0
   and y2002.net_qty / y2001.net_qty < 0.9
order by qty_diff, amt_diff
limit 100;
`

  ```text
  refused to write 'answer_3046445280.preql': not syntactically valid Trilogy.

  Parse error:
    --> 10:5
     |
  10 |     where ss.item.category = 'Books'
     |     ^---
     |
     = expected tvf_rel_arg
  Location:
  ...nion(     # store channel     ??? where ss.item.category = 'Book...
  ```
- `trilogy file write answer_3046445280.preql --content import root.store_sales as ss;
import root.store_returns as sr;
import root.catalog_sales as cs;
import …ot null
   and y2002.net_qty is not null
   and y2001.net_qty <> 0
   and y2002.net_qty / y2001.net_qty < 0.9
order by qty_diff, amt_diff
limit 100;
`

  ```text
  refused to write 'answer_3046445280.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
   - y2001.net_amt as amt_diff, ??? union join y2001.brand_id = y2...
  ```
- `trilogy file write probe_union_j.preql --content import root.store_sales as ss;
import root.store_returns as sr;
import root.catalog_sales as cs;
import root…ot null
   and y2002.net_qty is not null
   and y2001.net_qty <> 0
   and y2002.net_qty / y2001.net_qty < 0.9
order by qty_diff, amt_diff
limit 100;
`

  ```text
  refused to write 'probe_union_j.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
   - y2001.net_amt as amt_diff, ??? union join y2001.brand_id = y2...
  ```
- `trilogy file write probe_union8.preql --content import root.store_sales as ss;
import root.store_returns as sr;
import root.catalog_sales as cs;
import root.…ot null
   and y2002.net_qty is not null
   and y2001.net_qty <> 0
   and y2002.net_qty / y2001.net_qty < 0.9
order by qty_diff, amt_diff
limit 100;
`

  ```text
  refused to write 'probe_union8.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
   - y2001.net_amt as amt_diff, ??? union join y2001.brand_id = y2...
  ```
- `trilogy file write answer_3210116865.preql`

  ```text
  refused to write 'answer_3210116865.preql': not syntactically valid Trilogy.

  Parse error:
    --> 57:1
     |
  57 | rowset all_channels as union(
     | ^---
     |
     = expected EOI, block, or show_statement
  Location:

   # ---- Union channels ----
   ??? rowset all_channels as union(
  ```
- `trilogy file write probe12.preql --content import root.store_sales as ss;
import root.store_returns as sr;

rowset returned_keys <-
select
    concat(cast(sr…n join concat(cast(ss.ticket_number as string), '-', cast(ss.item.item_sk as string)) = returned_keys.rk
where returned_keys.rk is not null
limit 5;
`

  ```text
  refused to write 'probe12.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a NON-NULL GRAIN KEY: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). It MUST be a key, and one that is not nullable: `count(x)` skips rows where `x` is NULL, so counting a nullable property (a name, a date, any optional field) silently undercounts. When the grain takes SEVERAL keys, name them with `grain(...)`: `count(grain(order_id, item.id))` counts order+item combinations, and `count_distinct(grain(first_name, last_name, sale_date))` counts distinct combinations - `grain()` is never NULL, so combinations with a missing member still count. For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...stomer_sk as store_qty,     -- ??? count(*) by ss.ticket_number u...
  ```
- `trilogy file write probe_mine.preql`

  ```text
  refused to write 'probe_mine.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a NON-NULL GRAIN KEY: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). It MUST be a key, and one that is not nullable: `count(x)` skips rows where `x` is NULL, so counting a nullable property (a name, a date, any optional field) silently undercounts. When the grain takes SEVERAL keys, name them with `grain(...)`: `count(grain(order_id, item.id))` counts order+item combinations, and `count_distinct(grain(first_name, last_name, sale_date))` counts distinct combinations - `grain()` is never NULL, so combinations with a missing member still count. For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...cr;
   select cr.date_dim.year, ??? count(*) as c;
  ```
- `trilogy file write answer_426809370.preql`

  ```text
  refused to write 'answer_426809370.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [105]: A `rowset`/`auto`/`metric`/`property` definition connects its name to its expression with `<-`, not `as` - write `rowset base <- select ...;` (a `rowset` may also use the `with base as select ...;` form). For `auto`/`metric`/`property` only `<-` is valid, e.g. `auto total <- sum(sales.amount);`.
  Location:
  ...ng_addr.state;

   rowset qual ??? as
   where cust_state_total > 1...
  ```
- `trilogy file write answer_2374450308.preql`

  ```text
  refused to write 'answer_2374450308.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:38
    |
  7 | where inv.date_dim.date between date '2000-05-25' and date '2000-07-24'
    |                                      ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...nv.date_dim.date between date ??? '2000-05-25' and date '2000-07...
  ```
- `trilogy file write answer_2869182220.preql`

  ```text
  refused to write 'answer_2869182220.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...mber,
       --sr.item.item_sk
   ??? subset join sr.customer_demogr...
  ```
- `trilogy file write answer_2869182220.preql`

  ```text
  refused to write 'answer_2869182220.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...mber,
       --sr.item.item_sk
   ??? subset join sr.customer_demogr...
  ```
- `trilogy file write _verify3.preql`

  ```text
  refused to write '_verify3.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...ct count(s.d) as store_combos ??? from ...;
  ```
- `trilogy file write _probe_date.preql`

  ```text
  refused to write '_probe_date.preql': not syntactically valid Trilogy.

  Parse error:
   --> 6:2
    |
  6 | ;limit 5;
    |  ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...ss.item.item_sk as item_sk,
   ; ??? limit 5;
  ```

### `cli-misuse`

- `trilogy explore run root`

  ```text
  Invalid value for 'PATH': File 'run' does not exist.
  ```
- `trilogy file cat root/web_returns.preql`

  ```text
  No such command 'cat'.
  ```
- `trilogy file cat root/customer_address.preql`

  ```text
  No such command 'cat'.
  ```
- `trilogy file list . --regex probe|test_cat`

  ```text
  No such option '--regex'.
  ```
- `trilogy file list root --regex catalog_sales`

  ```text
  No such option '--regex'.
  ```
- `trilogy syntax example query-structure`

  ```text
  No such command 'syntax'.
  ```
- `trilogy explore store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'store_sales.preql' does not exist.
  ```
- `trilogy cat root/store_sales.preql`

  ```text
  No such command 'cat'.
  ```
- `trilogy file cat test_ws2.preql`

  ```text
  No such command 'cat'.
  ```
- `trilogy file remove probe_d98.preql probe_scalar.preql debug1.preql debug2.preql debug3.preql debug4.preql debug5.preql`

  ```text
  No such command 'remove'. Did you mean 'move'?
  ```
- `trilogy explore catalog_sales.preql`

  ```text
  Invalid value for 'PATH': File 'catalog_sales.preql' does not exist.
  ```
- `trilogy explore item.preql`

  ```text
  Invalid value for 'PATH': File 'item.preql' does not exist.
  ```
- `trilogy explore call_center.preql`

  ```text
  Invalid value for 'PATH': File 'call_center.preql' does not exist.
  ```
- `trilogy explore date_dim.preql`

  ```text
  Invalid value for 'PATH': File 'date_dim.preql' does not exist.
  ```
- `trilogy explore root`

  ```text
  Invalid value for 'PATH': File 'root' is a directory.
  ```
- `trilogy explore root/store_sales.preql --show sold_date --regex date`

  ```text
  Invalid value for '--show': 'sold_date' is not one of 'all', 'concepts', 'datasources', 'imports', 'groups'.
  ```
- `trilogy cat trilogy.toml`

  ```text
  No such command 'cat'.
  ```
- `trilogy file show root/store_sales.preql`

  ```text
  No such command 'show'.
  ```
- `trilogy cat trilogy.toml`

  ```text
  No such command 'cat'.
  ```
- `trilogy cat probe11.preql`

  ```text
  No such command 'cat'.
  ```
- `trilogy explore root/store_returns.preql --show roles`

  ```text
  Invalid value for '--show': 'roles' is not one of 'all', 'concepts', 'datasources', 'imports', 'groups'.
  ```
- `trilogy file cat probe.preql`

  ```text
  No such command 'cat'.
  ```
- `trilogy file cat probe11.preql`

  ```text
  No such command 'cat'.
  ```
- `trilogy file cat root/web_sales.preql`

  ```text
  No such command 'cat'.
  ```

### `undefined-concept`

- `trilogy run test_web2.preql`

  ```text
  Syntax error in test_web2.preql: Undefined concept: wr.web_site.site_id. Suggestions: ['wr.item.item_id', 'wr.item.item_desc', 'wr.item.item_sk']
  ```
- `trilogy run test_web3.preql`

  ```text
  Syntax error in test_web3.preql: Undefined concept: wr.web_site.site_id. Suggestions: ['wr.item.item_id', 'wr.item.item_desc', 'wr.item.item_sk', 'ws.web_site.site_id', 'ws.web_site.site_sk', 'ws.web_site.mkt_id']
  ```
- `trilogy run test_store.preql`

  ```text
  Syntax error in test_store.preql: Undefined concept: ss.sold_date.date (line 10, col 7, in WHERE). Suggestions: ['ss.promotion.end_date.date', 'ss.promotion.end_date.date_sk', 'ss.promotion.end_date.date_id', 'ss.customer.first_sales_date.date', 'ss.customer.last_review_date.date', 'ss.promotion.start_date.date']
  ```
- `trilogy run answer_3705756794.preql`

  ```text
  Syntax error in answer_3705756794.preql: Undefined concept: s.channel. Suggestions: ['all_sales.channel', 'all_returns.channel']
  ```
- `trilogy run probe.preql`

  ```text
  Syntax error in probe.preql: Undefined concept: ss.sold_date.year (line 2, col 8, in SELECT). Suggestions: ['ss.date_dim.year', 'ss.store.date_dim.year', 'ss.promotion.end_date.year', 'ss.customer.first_sales_date.year', 'ss.customer.last_review_date.year', 'ss.promotion.start_date.year']
  ```
- `trilogy run test_web3.preql`

  ```text
  Syntax error in test_web3.preql: Undefined concept: wr.web_site.site_id. Suggestions: ['wr.item.item_id', 'wr.item.item_desc', 'wr.item.item_sk', 'ws.web_site.site_id', 'ws.web_site.site_sk', 'ws.web_site.mkt_id']
  ```
- `trilogy run -`

  ```text
  Syntax error in stdin: Undefined concept: ss.sold_date.year (line 7, col 7, in WHERE). Suggestions: ['ss.sold_date.qoy', 'ss.date_dim.year', 'ss.store.date_dim.year', 'ss.promotion.end_date.year', 'ss.customer.first_sales_date.year', 'ss.customer.last_review_date.year']
  ```
- `trilogy run answer_374591292.preql`

  ```text
  Syntax error in answer_374591292.preql: 2 undefined concept references; fix all before re-running:
    - best_name (line 41, col 30, in ORDER BY); did you mean: best10.best_name?
    - worst_name (line 41, col 57, in ORDER BY); did you mean: worst10.worst_name, ss.store.street_name?
  ```
- `trilogy run probe_scalar.preql`

  ```text
  Syntax error in probe_scalar.preql: 3 undefined concept references; fix all before re-running:
    - dd.month_seq (line 2, in SELECT); did you mean: ss.date_dim.month_seq, ss.store.date_dim.month_seq, ss.promotion.end_date.month_seq, ss.promotion.start_date.month_seq, ss.customer.first_sales_date.month_seq, ss.customer.last_review_date.month_seq?
    - dd.year (line 2, col 69, in WHERE); did you mean: dd.moy, ss.date_dim.year, ss.store.date_dim.year, ss.promotion.end_date.year, ss.promotion.start_date.year, ss.customer.first_sales_date.year?
    - dd.moy (line 2, col 88, in WHERE); did you mean: dd.year, ss.date_dim.moy, ss.store.date_dim.moy, ss.promotion.end_date.moy, ss.promotion.start_date.moy, ss.customer.first_sales_date.moy?
  ```
- `trilogy run answer_3247656127.preql`

  ```text
  Syntax error in answer_3247656127.preql: Undefined concept: ss.sold_date.year. Suggestions: ['ss.date_dim.year', 'ss.store.date_dim.year', 'ss.promotion.end_date.year', 'cs.sold_date.year', 'ws.sold_date.year', 'cs.ship_date.year']
  ```
- `trilogy run -`

  ```text
  Syntax error in stdin: 4 undefined concept references; fix all before re-running:
    - ss.sold_date.date (line 2, col 8, in SELECT); did you mean: ss.sold_date.week_seq, ss.promotion.end_date.date, ss.promotion.end_date.date_sk, ss.customer.first_sales_date.date, ss.customer.last_review_date.date, ss.promotion.start_date.date?
    - ss.sold_date.week_seq (line 2, col 27, in SELECT); did you mean: ss.date_dim.week_seq, ss.store.date_dim.week_seq, ss.promotion.end_date.week_seq, ss.customer.first_sales_date.week_seq, ss.customer.last_review_date.week_seq, ss.promotion.start_date.week_seq?
    - ss.sold_date.date (line 3, col 7, in WHERE); did you mean: ss.sold_date.week_seq, ss.promotion.end_date.date, ss.promotion.end_date.date_sk, ss.customer.first_sales_date.date, ss.customer.last_review_date.date, ss.promotion.start_date.date?
    - ss.sold_date.date (line 4, col 10, in ORDER BY); did you mean: ss.sold_date.week_seq, ss.promotion.end_date.date, ss.promotion.end_date.date_sk, ss.customer.first_sales_date.date, ss.customer.last_review_date.date, ss.promotion.start_date.date?
  ```
- `trilogy run probe_ss.preql`

  ```text
  Syntax error in probe_ss.preql: 4 undefined concept references; fix all before re-running:
    - ss.sold_date.date (line 7, col 5, in SELECT); did you mean: ss.sold_date.dow, ss.sold_date.year, ss.sold_date.week_seq, ss.promotion.end_date.date, ss.customer.first_sales_date.date, ss.customer.last_review_date.date?
    - ss.sold_date.dow (line 8, col 5, in SELECT); did you mean: ss.sold_date.date, ss.sold_date.year, ss.sold_date.week_seq, ss.date_dim.dow, ss.store.date_dim.dow, ss.promotion.end_date.dow?
    - ss.sold_date.week_seq (line 9, col 5, in SELECT); did you mean: ss.date_dim.week_seq, ss.store.date_dim.week_seq, ss.sold_date.dow, ss.promotion.end_date.week_seq, ss.customer.first_sales_date.week_seq, ss.customer.last_review_date.week_seq?
    - ss.sold_date.year (line 3, col 7, in WHERE); did you mean: ss.sold_date.date, ss.sold_date.dow, ss.sold_date.week_seq, ss.date_dim.year, ss.store.date_dim.year, ss.promotion.end_date.year?
  ```
- `trilogy run t_catalog.preql`

  ```text
  Syntax error in t_catalog.preql: Undefined concept: local.it (line 21, col 64, in ORDER BY). Suggestions: ['cat_list.it', 'cat_refund.it', 'qual_items.it']
  ```
- `trilogy run answer_755724379.preql`

  ```text
  Syntax error in answer_755724379.preql: 2 undefined concept references; fix all before re-running:
    - ss.sold_date.year (line 14, col 3, in WHERE); did you mean: ss.sold_date.moy, ss.date_dim.year, ss.store.date_dim.year, ws.sold_date.year, cs.sold_date.year, ws.ship_date.year?
    - ss.sold_date.moy (line 14, col 32, in WHERE); did you mean: ss.sold_date.year, ss.date_dim.moy, ss.store.date_dim.moy, ws.sold_date.moy, cs.sold_date.moy, ws.ship_date.moy?
  ```
- `trilogy run test_web2.preql`

  ```text
  Syntax error in test_web2.preql: Undefined concept: wr.web_site.site_id. Suggestions: ['wr.item.item_id', 'wr.item.item_desc', 'wr.item.item_sk']
  ```
- `trilogy run test_web3.preql`

  ```text
  Syntax error in test_web3.preql: Undefined concept: wr.web_site.site_id. Suggestions: ['wr.item.item_id', 'wr.item.item_desc', 'wr.item.item_sk', 'ws.web_site.site_id', 'ws.web_site.site_sk', 'ws.web_site.mkt_id']
  ```
- `trilogy run answer_1226264875.preql`

  ```text
  Syntax error in answer_1226264875.preql: 6 undefined concept references; fix all before re-running:
    - store_qty (line 97, col 5, in ORDER BY); did you mean: store_sum.store_qty, store_sum.yr, ss.store.city?
    - store_ws (line 98, col 5, in ORDER BY); did you mean: store_sum.store_ws, store_sum.yr, customer_sk?
    - store_sp (line 99, col 5, in ORDER BY); did you mean: store_sum.store_sp, store_sum.yr, customer_sk?
    - comb_qty (line 100, col 5, in ORDER BY); did you mean: combined.comb_qty?
    - comb_ws (line 101, col 5, in ORDER BY); did you mean: combined.comb_ws?
    - comb_sp (line 102, col 5, in ORDER BY); did you mean: combined.comb_sp?
  ```
- `trilogy run answer_4111870542.preql`

  ```text
  Syntax error in answer_4111870542.preql: Undefined concept: store_returns.date_dim.week_seq.
  ```
- `trilogy run probe_sr_date.preql`

  ```text
  Syntax error in probe_sr_date.preql: 2 undefined concept references; fix all before re-running:
    - root.date_dim.week_seq (line 7, col 12, in SELECT); did you mean: root.date_dim.date, sr.date_dim.week_seq, sr.store.date_dim.week_seq, sr.customer.first_sales_date.week_seq, sr.customer.last_review_date.week_seq, sr.customer.first_shipto_date.week_seq?
    - root.date_dim.date (line 7, col 41, in WHERE); did you mean: root.date_dim.week_seq, sr.date_dim.date, sr.store.date_dim.date, sr.customer.first_shipto_date.date, sr.customer.first_sales_date.date, sr.customer.last_review_date.date?
  ```

### `file-not-found`

- `trilogy run /tmp/test1.preql`

  ```text
  Input '/tmp/test1.preql' does not exist.
  ```
- `trilogy run test_web.preql`

  ```text
  Input 'test_web.preql' does not exist.
  ```
- `trilogy run answer_219203908.preql`

  ```text
  Input 'answer_219203908.preql' does not exist.
  ```
- `trilogy run test_ws3.preql`

  ```text
  Input 'test_ws3.preql' does not exist.
  ```
- `trilogy run test_ws6.preql`

  ```text
  Input 'test_ws6.preql' does not exist.
  ```
- `trilogy run web_sales_tmp.preql`

  ```text
  Input 'web_sales_tmp.preql' does not exist.
  ```
- `trilogy run debug6.preql`

  ```text
  Input 'debug6.preql' does not exist.
  ```
- `trilogy run debug7.preql`

  ```text
  Input 'debug7.preql' does not exist.
  ```
- `trilogy run debug8.preql`

  ```text
  Input 'debug8.preql' does not exist.
  ```
- `trilogy run debug6.preql`

  ```text
  Input 'debug6.preql' does not exist.
  ```
- `trilogy run debug7.preql`

  ```text
  Input 'debug7.preql' does not exist.
  ```
- `trilogy run debug8.preql`

  ```text
  Input 'debug8.preql' does not exist.
  ```
- `trilogy run answer_1827331030.preql`

  ```text
  Input 'answer_1827331030.preql' does not exist.
  ```

### `join-resolution`

- `trilogy run test_cr2.preql`

  ```text
  Resolution error in test_cr2.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {cr.date_dim.date, rets}; {cs.catalog_page.catalog_page_id, cpid}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run test_cr2.preql`

  ```text
  Resolution error in test_cr2.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {cr.date_dim.date, rets}; {cs.catalog_page.catalog_page_id, cpid}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run t_catalog.preql`

  ```text
  Resolution error in t_catalog.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 21). The requested concepts split into 2 disconnected subgraphs: {qual_items.cat_list.lst, qual_items.it}; {qual_items.cat_refund.rfnd}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run t_catalog.preql`

  ```text
  Resolution error in t_catalog.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 14). The requested concepts split into 2 disconnected subgraphs: {cat_metrics.cat_list.lst, cat_metrics.it}; {cat_metrics.cat_refund.rfnd}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run t_catalog.preql`

  ```text
  Resolution error in t_catalog.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 14). The requested concepts split into 2 disconnected subgraphs: {cat_metrics.it, cat_metrics.lst}; {cat_metrics.rfnd}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run test_cr2.preql`

  ```text
  Resolution error in test_cr2.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {cr.date_dim.date, rets}; {cs.catalog_page.catalog_page_id, cpid}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run test_cr2.preql`

  ```text
  Resolution error in test_cr2.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {cr.date_dim.date, rets}; {cs.catalog_page.catalog_page_id, cpid}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_3562094594.preql`

  ```text
  Resolution error in answer_3562094594.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {_catalog_only_cust}; {_catalog_only_item_sk}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_3562094594.preql`

  ```text
  Resolution error in answer_3562094594.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {_catalog_only_cust}; {_catalog_only_item_sk}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run _test_set.preql`

  ```text
  Resolution error in _test_set.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {_catalog_only_cust}; {_catalog_only_item_sk}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_3562094594.preql`

  ```text
  Resolution error in answer_3562094594.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {_catalog_only_cust}; {_catalog_only_item_sk}. Are you missing a join or merge statement to relate them?
  ```

### `no-output`

- `trilogy run root/store_sales.preql`

  ```text
  Nothing was executed: parsed 12 definition statement(s) (9 imports, 1 concept, 1 datasource, 1 property) but none produce output. Did you mean to include a SELECT statement, or run a refresh on datasources instead?
  ```
- `trilogy run probe.preql`

  ```text
  Nothing was executed: the script contains no statements. Did you mean to include a SELECT statement, or run a refresh on datasources instead?
  ```
- `trilogy run probe.preql`

  ```text
  Nothing was executed: parsed 3 definition statement(s) (2 rowsets, 1 import) but none produce output. Did you mean to include a SELECT statement, or run a refresh on datasources instead?
  ```
- `trilogy run root/store_returns.preql`

  ```text
  Nothing was executed: parsed 12 definition statement(s) (9 imports, 1 concept, 1 datasource, 1 property) but none produce output. Did you mean to include a SELECT statement, or run a refresh on datasources instead?
  ```
- `trilogy run root/catalog_returns.preql`

  ```text
  Nothing was executed: parsed 19 definition statement(s) (16 imports, 1 concept, 1 datasource, 1 property) but none produce output. Did you mean to include a SELECT statement, or run a refresh on datasources instead?
  ```

### `syntax-missing-alias`

- `trilogy file write answer_374591292.preql`

  ```text
  refused to write 'answer_374591292.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `join best10.rank = worst10.position_rank as join_best10_rank_worst10_position_rank`
  Location:
      worst10.worst_name,
   join ??? best10.rank = worst10.position...
  ```
- `trilogy file write answer_374591292.preql`

  ```text
  refused to write 'answer_374591292.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `join best10.rank = worst10.position_rank as join_best10_rank_worst10_position_rank`
  Location:
      worst10.worst_name,
   join ??? best10.rank = worst10.position...
  ```
- `trilogy file write answer_374591292.preql`

  ```text
  refused to write 'answer_374591292.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `join best10.pos = worst10.pos as join_best10_pos_worst10_pos`
  Location:
      worst10.worst_name,
   join ??? best10.pos = worst10.pos
   orde...
  ```

### `type-error`

- `trilogy run answer_525180526.preql`

  ```text
  Syntax error in answer_525180526.preql: Cannot use BETWEEN with incompatible types DATE and STRING (low)
  ```
