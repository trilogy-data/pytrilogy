# Trilogy failure analysis — 20260806-224551

- Run `20260806-224551` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 1404 | failed: 188 (13%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 140 | 74% |
| `cli-misuse` | 25 | 13% |
| `syntax-parse` | 22 | 12% |
| `file-not-found` | 1 | 1% |

## Detail

### `other`

- `trilogy run answer_507046194.preql`

  ```text
  Import error in answer_507046194.preql: Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: raw.store_sales?
  ```
- `trilogy run answer_1858999935.preql`

  ```text
  Import error in answer_1858999935.preql: Unable to import '.\all_sales.preql': [Errno 2] No such file or directory: '.\\all_sales.preql'. Did you mean: raw.all_sales?
  ```
- `trilogy file read trilogy.toml`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3863442186.preql`

  ```text
  Import error in answer_3863442186.preql: Unable to import '.\all_sales.preql': [Errno 2] No such file or directory: '.\\all_sales.preql'. Did you mean: raw.all_sales?
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3705756794.preql`

  ```text
  Import error in answer_3705756794.preql: Unable to import '.\all_sales.preql': [Errno 2] No such file or directory: '.\\all_sales.preql'. Did you mean: raw.all_sales?
  ```
- `trilogy run answer_883027685.preql`

  ```text
  Resolution error in answer_883027685.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 6). The requested concepts split into 2 disconnected subgraphs: {item_cat_avg}; {line_item_count, state, ss.customer.current_address.sk, ss.item.category, ss.item.current_price, ss.sale_date.month_of_year, ss.sale_date.year}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_689898357.preql`

  ```text
  Import error in answer_689898357.preql: Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: raw.store_sales?
  ```
- `trilogy run answer_3697706765.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Import error in answer_3697706765.preql: Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: raw.store_sales?
  ```
- `trilogy run answer_3263796749.preql`

  ```text
  Import error in answer_3263796749.preql: Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: raw.store_sales?
  ```
- `trilogy file read raw/_q04_agent_rowset_union_join.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3697440276.preql`

  ```text
  Import error in answer_3697440276.preql: Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: raw.store_sales?
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_1297355556.preql`

  ```text
  Import error in answer_1297355556.preql: Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: raw.store_sales?
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_1455459008.preql`

  ```text
  Import error in answer_1455459008.preql: Unable to import '.\catalog_sales.preql': [Errno 2] No such file or directory: '.\\catalog_sales.preql'. Did you mean: raw.catalog_sales?
  ```
- `trilogy run answer_2524943990.preql`

  ```text
  Import error in answer_2524943990.preql: Unable to import '.\catalog_sales.preql': [Errno 2] No such file or directory: '.\\catalog_sales.preql'. Did you mean: raw.catalog_sales?
  ```
- `trilogy run raw/catalog_store_returns.preql`

  ```text
  Nothing was executed: parsed 15 definition statement(s) (8 imports, 3 datasources, 2 concepts, 2 propertys) but none produce output. Did you mean to include a SELECT statement, or run a refresh on datasources instead?
  ```
- `trilogy file read raw/catalog_store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under root/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run test2.preql`

  ```text
  Syntax error in test2.preql: Undefined concept: cm.store_line_cnt (line 13, col 10, in ORDER BY). Suggestions: ['cm.store.county', 'cm.store_sale_date.sk', 'cm.store_sale_date.id', 'store_line_cnt', 'cat_line_cnt']
  ```
- `trilogy run test9.preql`

  ```text
  Nothing was executed: parsed 4 definition statement(s) (2 imports, 2 rowsets) but none produce output. Did you mean to include a SELECT statement, or run a refresh on datasources instead?
  ```
- `trilogy file read raw/_q04_agent_rowset_union_join.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read trilogy.toml`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run raw/catalog_sales.preql`

  ```text
  Nothing was executed: parsed 40 definition statement(s) (31 imports, 6 concepts, 2 datasources, 1 property) but none produce output. Did you mean to include a SELECT statement, or run a refresh on datasources instead?
  ```
- `trilogy run answer_1835050598.preql`

  ```text
  Import error in answer_1835050598.preql: Unable to import '.\catalog_sales.preql': [Errno 2] No such file or directory: '.\\catalog_sales.preql'. Did you mean: raw.catalog_sales?
  ```
- `trilogy run answer_2604809012.preql`

  ```text
  Import error in answer_2604809012.preql: Unable to import '.\inventory.preql': [Errno 2] No such file or directory: '.\\inventory.preql'. Did you mean: raw.inventory?
  ```
- `trilogy run answer_2928586490.preql`

  ```text
  Import error in answer_2928586490.preql: Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: raw.store_sales?
  ```
- `trilogy run answer_2844519538.preql`

  ```text
  Import error in answer_2844519538.preql: Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: raw.store_sales?
  ```
- `trilogy file read raw/catalog_store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_4254732537.preql`

  ```text
  Import error in answer_4254732537.preql: Unable to import '.\catalog_sales.preql': [Errno 2] No such file or directory: '.\\catalog_sales.preql'. Did you mean: raw.catalog_sales?
  ```
- `trilogy file list ./root --recursive`

  ```text
  No such path: ./root
  ```
- `trilogy run answer_2802535988.preql`

  ```text
  Import error in answer_2802535988.preql: Unable to import '.\web_sales.preql': [Errno 2] No such file or directory: '.\\web_sales.preql'. Did you mean: raw.web_sales?
  ```
- `trilogy run answer_525180526.preql`

  ```text
  Import error in answer_525180526.preql: Unable to import '.\catalog_sales.preql': [Errno 2] No such file or directory: '.\\catalog_sales.preql'. Did you mean: raw.catalog_sales?
  ```
- `trilogy run answer_525180526.preql`

  ```text
  Syntax error in answer_525180526.preql: Undefined concept: item.sk. Suggestions: ['cs.item.sk', 'cs.ship_date.sk', 'cs.ship_customer.sk', 'cs.sale_date.sk', 'cs.sale_time.sk', 'cs.ship_mode.sk']
  ```
- `trilogy run answer_3347758002.preql`

  ```text
  Import error in answer_3347758002.preql: Unable to import '.\all_sales.preql': [Errno 2] No such file or directory: '.\\all_sales.preql'. Did you mean: raw.all_sales?
  ```
- `trilogy run answer_3347758002.preql`

  ```text
  Syntax error in answer_3347758002.preql: Undefined concept: item.category. Suggestions: ['sales.item.category', 'sales.item.category_id']
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read trilogy.toml`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3809267817.preql`

  ```text
  Import error in answer_3809267817.preql: Unable to import '.\customer.preql': [Errno 2] No such file or directory: '.\\customer.preql'. Did you mean: raw.customer?
  ```
- `trilogy run answer_4140546834.preql`

  ```text
  Import error in answer_4140546834.preql: Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: raw.store_sales?
  ```
- `trilogy file read raw/inventory.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3770074305.preql`

  ```text
  Import error in answer_3770074305.preql: Unable to import '.\item.preql': [Errno 2] No such file or directory: '.\\item.preql'. Did you mean: raw.item?
  ```
- `trilogy file read raw/_q04_agent_rowset_union_join.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read trilogy.toml`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run raw/catalog_sales.preql`

  ```text
  Nothing was executed: parsed 40 definition statement(s) (31 imports, 6 concepts, 2 datasources, 1 property) but none produce output. Did you mean to include a SELECT statement, or run a refresh on datasources instead?
  ```
- `trilogy file read trilogy.toml`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_630074061.preql`

  ```text
  Import error in answer_630074061.preql: Unable to import '.\catalog_sales.preql': [Errno 2] No such file or directory: '.\\catalog_sales.preql'. Did you mean: raw.catalog_sales?
  ```
- `trilogy run answer_1663418777.preql`

  ```text
  Syntax error in answer_1663418777.preql: Undefined concept: item.category.
  ```
- `trilogy file read raw/item.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_1663418777.preql`

  ```text
  Import error in answer_1663418777.preql: Unable to import '.\item.preql': [Errno 2] No such file or directory: '.\\item.preql'. Did you mean: raw.item?
  ```
- `trilogy run answer_1663418777.preql`

  ```text
  Syntax error in answer_1663418777.preql: Undefined concept: item.category. Suggestions: ['category', 'category_id']
  ```
- `trilogy run answer_1663418777.preql`

  ```text
  Syntax error in answer_1663418777.preql: Output column 'product_name' renames 'local.product_name' back to the name of an existing concept 'product_name' (defined at line 9) that 'local.product_name' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'product_name_out').
  ```
- `trilogy file read raw/item.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run`

  ```text
  Unexpected error: No .preql files found in .
  ```
- `trilogy file read explore_item.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run test_import.preql`

  ```text
  Import error in test_import.preql: Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: raw.store_sales?
  ```
- `trilogy run test_import.preql`

  ```text
  Syntax error in test_import.preql: 2 undefined concept references; fix all before re-running:
    - store_sales.item.category (line 3, col 5, in SELECT); did you mean: store_sales.store.name, item.category, item.category_id?
    - store_sales.store.name (line 4, col 5, in SELECT); did you mean: store_sales.item.category, return_store.name, store.name?
  ```
- `trilogy file read explore_item.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_2940558602.preql`

  ```text
  Import error in answer_2940558602.preql: Unable to import '.\all_sales.preql': [Errno 2] No such file or directory: '.\\all_sales.preql'. Did you mean: raw.all_sales?
  ```
- `trilogy run answer_1809796058.preql`

  ```text
  Syntax error in answer_1809796058.preql: Undefined concept: ss.return_date (line 4, col 3, in WHERE). Suggestions: ['ss.return_date.sk', 'ss.return_date.id', 'ss.return_date.date', 'ss.return_date.year', 'ss.return_date.day_of_week', 'ss.return_date.day_of_month']
  ```
- `trilogy file read explore_item.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/repro.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3560698360.preql`

  ```text
  Import error in answer_3560698360.preql: Unable to import '.\web_sales.preql': [Errno 2] No such file or directory: '.\\web_sales.preql'. Did you mean: raw.web_sales?
  ```
- `trilogy run answer_3247656127.preql`

  ```text
  Import error in answer_3247656127.preql: Unable to import '.\all_sales.preql': [Errno 2] No such file or directory: '.\\all_sales.preql'. Did you mean: raw.all_sales?
  ```
- `trilogy file read explore_item.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_2986518257.preql`

  ```text
  Import error in answer_2986518257.preql: Unable to import '.\catalog_sales.preql': [Errno 2] No such file or directory: '.\\catalog_sales.preql'. Did you mean: raw.catalog_sales?
  ```
- `trilogy run answer_145690531.preql`

  ```text
  Nothing was executed: parsed 3 definition statement(s) (2 imports, 1 rowset) but none produce output. Did you mean to include a SELECT statement, or run a refresh on datasources instead?
  ```
- `trilogy run answer_145690531.preql`

  ```text
  Resolution error in answer_145690531.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 5). The requested concepts split into 2 disconnected subgraphs: {_subquery_5_34.dref.week_seq}; {_ch_catalog_total, _ch_item_id, _ch_store_total, _ch_web_total, sales.sale_date.week_seq}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_1623435181.preql`

  ```text
  Import error in answer_1623435181.preql: Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: raw.store_sales?
  ```
- `trilogy run --param`

  ```text
  Option '--param' requires an argument.
  ```
- `trilogy `

  ```text
  trilogy error: 'args' must be a list of strings.
  ```
- `trilogy file read explore_item.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_345752060.preql`

  ```text
  Import error in answer_345752060.preql: Unable to import '.\all_sales.preql': [Errno 2] No such file or directory: '.\\all_sales.preql'. Did you mean: raw.all_sales?
  ```
- `trilogy file read explore_item.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3273495117.preql`

  ```text
  Import error in answer_3273495117.preql: Unable to import '.\web_sales.preql': [Errno 2] No such file or directory: '.\\web_sales.preql'. Did you mean: raw.web_sales?
  ```
- `trilogy file read explore_item.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/_q04_agent_rowset_union_join.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3979964698.preql`

  ```text
  Import error in answer_3979964698.preql: Unable to import '.\all_sales.preql': [Errno 2] No such file or directory: '.\\all_sales.preql'. Did you mean: raw.all_sales?
  ```
- `trilogy run answer_3036656719.preql`

  ```text
  Import error in answer_3036656719.preql: Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: raw.store_sales?
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_755724379.preql`

  ```text
  Import error in answer_755724379.preql: Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: raw.store_sales?
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read explore_item.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read answer_2874833976.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read explore_item.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_2091535883.preql`

  ```text
  Import error in answer_2091535883.preql: Unable to import '.\catalog_sales.preql': [Errno 2] No such file or directory: '.\\catalog_sales.preql'. Did you mean: raw.catalog_sales?
  ```
- `trilogy run diag.probe.preql`

  ```text
  Syntax error in diag.probe.preql: Undefined concept: inv.item.sk. Suggestions: ['cs.item.sk', 'cs.ship_date.sk', 'cs.sale_date.sk', 'cs.return_date.sk', 'cs.call_center.sk', 'cs.billing_customer.sk']
  ```
- `trilogy run answer_1772060640.preql`

  ```text
  Import error in answer_1772060640.preql: Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: raw.store_sales?
  ```
- `trilogy run answer_1772060640.preql`

  ```text
  Syntax error in answer_1772060640.preql: 2 undefined concept references; fix all before re-running:
    - last_name (line 22, col 5, in ORDER BY); did you mean: ss.customer.last_name, ss.return_customer.last_name?
    - ticket_number (line 23, col 5, in ORDER BY); did you mean: ss.ticket_number?
  ```
- `trilogy run answer_1772060640.preql`

  ```text
  Syntax error in answer_1772060640.preql: ORDER BY references 'ss.customer.sk', which is not in the SELECT projection (line 3). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --ss.customer.sk order by ss.customer.sk asc`.
  ```
- `trilogy file read trilogy.toml`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read explore_item.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_747155668.preql`

  ```text
  Import error in answer_747155668.preql: Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: raw.store_sales?
  ```
- `trilogy file read explore_item.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/_q04_agent_rowset_union_join.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3210116865.preql`

  ```text
  Import error in answer_3210116865.preql: Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: raw.store_sales?
  ```
- `trilogy file read explore_item.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_1226264875.preql`

  ```text
  Import error in answer_1226264875.preql: Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: raw.store_sales?
  ```
- `trilogy file read trilogy.toml`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid \escape: line 1 column 81 (char 80). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file read explore_item.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/repro.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_426809370.preql`

  ```text
  Import error in answer_426809370.preql: Unable to import '.\catalog_sales.preql': [Errno 2] No such file or directory: '.\\catalog_sales.preql'. Did you mean: raw.catalog_sales?
  ```
- `trilogy run answer_426809370.preql`

  ```text
  Syntax error in answer_426809370.preql: Undefined concept: catalog_sales.is_returned. Suggestions: ['is_returned', 'catalog_page.id', 'catalog_page.sk']
  ```
- `trilogy file list root --recursive`

  ```text
  No such path: root
  ```
- `trilogy file read explore_item.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run tmp_dates.preql`

  ```text
  Syntax error in tmp_dates.preql: Undefined concept: d.date.week_seq (line 4, col 3, in SELECT). Suggestions: ['d.week_seq', 'd.date.year', 'd.date.date']
  ```
- `trilogy run answer_2869182220.preql`

  ```text
  Import error in answer_2869182220.preql: Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: raw.store_sales?
  ```
- `trilogy run answer_927797426.preql`

  ```text
  Import error in answer_927797426.preql: Unable to import '.\web_sales.preql': [Errno 2] No such file or directory: '.\\web_sales.preql'. Did you mean: raw.web_sales?
  ```
- `trilogy file read explore_item.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_2852230229.preql`

  ```text
  Unexpected error in answer_2852230229.preql: (_duckdb.BinderException) Binder Error: GROUPING statement cannot be used without groups

  LINE 69: 	WHEN grouping("questionable"."category") + grouping("questionable...
                ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "ws_item_items"."I_CATEGORY" as "ws_item_category",
      "ws_item_items"."I_CLASS" as "ws_item_class",
      "ws_web_sales"."WS_NET_PAID" as "ws_net_paid"
  FROM
      "web_sales" as "ws_web_sales"
      INNER JOIN "item" as "ws_item_items" on "ws_web_sales"."WS_ITEM_SK" = "ws_item_items"."I_ITEM_SK"
      INNER JOIN "date_dim" as "ws_sale_date_date" on "ws_web_sales"."WS_SOLD_DATE_SK" = "ws_sale_date_date"."D_DATE_SK"
  WHERE
      "ws_sale_date_date"."D_YEAR" = 2000
  ),
  thoughtful as (
  SELECT
      "cheerful"."ws_item_category" as "ws_item_category",
      "cheerful"."ws_item_class" as "ws_item_class",
      CASE
  	WHEN grouping("cheerful"."ws_item_class") = 0 THEN "cheerful"."ws_item_category"
  	ELSE null
  	END as "_virt_func_case_9780469026048542",
      grouping("cheerful"."ws_item_category") + grouping("cheerful"."ws_item_class") as "_virt_func_add_7020667660037447",
      grouping("cheerful"."ws_item_category") as "_virt_agg_grouping_164218295708188",
      grouping("cheerful"."ws_item_category") as "g_cat",
      grouping("cheerful"."ws_item_class") as "_virt_agg_grouping_5549377253046357",
      grouping("cheerful"."ws_item_class") as "g_class",
      sum("cheerful"."ws_net_paid") as "total"
  FROM
      "cheerful"
  GROUP BY
      ROLLUP (1, 2)),
  cooperative as (
  SELECT
      "thoughtful"."_virt_agg_grouping_164218295708188" as "_virt_agg_grouping_164218295708188",
      "thoughtful"."_virt_agg_grouping_5549377253046357" as "_virt_agg_grouping_5549377253046357",
      "thoughtful"."g_cat" as "g_cat",
      "thoughtful"."g_class" as "g_class",
      "thoughtful"."total" as "total",
      "thoughtful"."ws_item_category" as "ws_item_category",
      "thoughtful"."ws_item_class" as "ws_item_class",
      rank() over (partition by "thoughtful"."_virt_func_add_7020667660037447","thoughtful"."_virt_func_case_9780469026048542" order by "thoughtful"."total" desc ) as "rnk"
  FROM
      "thoughtful"),
  questionable as (
  SELECT
      "cooperative"."_virt_agg_grouping_164218295708188" + "cooperative"."_virt_agg_grouping_5549377253046357" as "lvl",
      "cooperative"."g_cat" as "g_cat",
      "cooperative"."g_class" as "g_class",
      "cooperative"."rnk" as "rnk",
      "cooperative"."total" as "total_net_paid",
      "cooperative"."ws_item_category" as "category",
      "cooperative"."ws_item_class" as "class"
  FROM
      "cooperative")
  SELECT
      "questionable"."category" as "category",
      "questionable"."class" as "class",
      "questionable"."total_net_paid" as "total_net_paid",
      "questionable"."g_cat" as "g_cat",
      "questionable"."g_class" as "g_class",
      "questionable"."lvl" as "lvl",
      "questionable"."rnk" as "rnk"
  FROM
      "questionable"
  ORDER BY
      "questionable"."lvl" desc nulls first,
      CASE
  	WHEN grouping("questionable"."category") + grouping("questionable"."class") = 0 THEN "questionable"."category"
  	END asc nulls first,
      "questionable"."rnk" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run answer_2852230229.preql`

  ```text
  Syntax error in answer_2852230229.preql: Undefined concept: _virt_agg_grouping_5549377253046357.
  ```
- `trilogy file read explore_item.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read tmp_dates.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read explore_item.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_32504671.preql`

  ```text
  Import error in answer_32504671.preql: Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: raw.store_sales?
  ```
- `trilogy run answer_840315271.preql`

  ```text
  Import error in answer_840315271.preql: Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: raw.store_sales?
  ```
- `trilogy file read raw/reason.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run chk.preql`

  ```text
  Import error in chk.preql: Unable to import '.\reason.preql': [Errno 2] No such file or directory: '.\\reason.preql'. Did you mean: raw.reason?
  ```
- `trilogy run chk3.preql`

  ```text
  Syntax error in chk3.preql: ORDER BY references 'ss.ticket_number', which is not in the SELECT projection (line 3). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --ss.ticket_number order by ss.ticket_number asc`.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read explore_item.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/repro.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run test_preql.preql`

  ```text
  Import error in test_preql.preql: Unable to import '.\web_sales.preql': [Errno 2] No such file or directory: '.\\web_sales.preql'. Did you mean: raw.web_sales?
  ```
- `trilogy file read raw\web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read explore_item.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read repro.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read explore_item.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read repro.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3734509910.preql`

  ```text
  Import error in answer_3734509910.preql: Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: raw.store_sales?
  ```
- `trilogy file list root --recursive`

  ```text
  No such path: root
  ```
- `trilogy file read explore_item.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `cli-misuse`

- `trilogy syntax example query-structure`

  ```text
  No such command 'syntax'.
  ```
- `trilogy file list raw store_sales.preql`

  ```text
  Got unexpected extra argument (store_sales.preql)
  ```
- `trilogy file show raw/catalog_sales.preql`

  ```text
  No such command 'show'.
  ```
- `trilogy explore raw/catalog_store_returns.preql --show datasource`

  ```text
  Invalid value for '--show': 'datasource' is not one of 'all', 'concepts', 'datasources', 'imports', 'groups'.
  ```
- `trilogy explore raw.store_sales`

  ```text
  Invalid value for 'PATH': File 'raw.store_sales' does not exist.
  ```
- `trilogy file cat trilogy.toml`

  ```text
  No such command 'cat'.
  ```
- `trilogy explore raw`

  ```text
  Invalid value for 'PATH': File 'raw' is a directory.
  ```
- `trilogy file remove explore_item.preql`

  ```text
  No such command 'remove'. Did you mean 'move'?
  ```
- `trilogy explore`

  ```text
  Missing argument 'PATH'.
  ```
- `trilogy agent-info query-syntax example scoped-join`

  ```text
  No such command 'query-syntax'. Did you mean 'syntax'?
  ```
- `trilogy file remove scratch_date.preql`

  ```text
  No such command 'remove'. Did you mean 'move'?
  ```
- `trilogy cat trilogy.toml`

  ```text
  No such command 'cat'.
  ```
- `trilogy trilogy config`

  ```text
  No such command 'trilogy'.
  ```
- `trilogy agents explore`

  ```text
  No such command 'agents'.
  ```
- `trilogy file show explore_item.preql`

  ```text
  No such command 'show'.
  ```
- `trilogy file cat trilogy.toml`

  ```text
  No such command 'cat'.
  ```
- `trilogy cat explore_item.preql`

  ```text
  No such command 'cat'.
  ```
- `trilogy explore raw/all_sales.preql --regex store|catalog_page|web_site|outlet|channel -i`

  ```text
  No such option '-i'.
  ```
- `trilogy file rm tmp_dates.preql`

  ```text
  No such command 'rm'.
  ```
- `trilogy explore all_sales.preql`

  ```text
  Invalid value for 'PATH': File 'all_sales.preql' does not exist.
  ```
- `trilogy cat explore_item.preql`

  ```text
  No such command 'cat'.
  ```
- `trilogy file cat explore_item.preql`

  ```text
  No such command 'cat'.
  ```
- `trilogy file cat tmp_dates.preql`

  ```text
  No such command 'cat'.
  ```
- `trilogy file dump raw/web_sales.preql`

  ```text
  No such command 'dump'.
  ```
- `trilogy explore web_sales.preql`

  ```text
  Invalid value for 'PATH': File 'web_sales.preql' does not exist.
  ```

### `syntax-parse`

- `trilogy file write answer_883027685.preql`

  ```text
  refused to write 'answer_883027685.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/store_sales as ss;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/store_sales as ss;
   import...
  ```
- `trilogy file write answer_4199102535.preql`

  ```text
  refused to write 'answer_4199102535.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:8
    |
  7 | import raw/customer as cust;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  ...rrent demographics.

   import ??? raw/customer as cust;
   import
  ```
- `trilogy file write answer_2524943990.preql`

  ```text
  refused to write 'answer_2524943990.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/catalog_sales as cs;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/catalog_sales as cs;

   #
  ```
- `trilogy file write answer_2604809012.preql`

  ```text
  refused to write 'answer_2604809012.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/inventory as inv;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/inventory as inv;

   selec...
  ```
- `trilogy file write answer_2928586490.preql`

  ```text
  refused to write 'answer_2928586490.preql': not syntactically valid Trilogy.

  Parse error:
    --> 33:1
     |
  33 | rowset combined as union(
     | ^---
     |
     = expected EOI, block, or show_statement
  Location:
  ...uent items + best customers.
   ??? rowset combined as union(
  ```
- `trilogy file write answer_4254732537.preql`

  ```text
  refused to write 'answer_4254732537.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/catalog_sales as cs;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/catalog_sales as cs;

   wh...
  ```
- `trilogy file write answer_3770074305.preql`

  ```text
  refused to write 'answer_3770074305.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/item as item;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/item as item;
   import raw/...
  ```
- `trilogy file write check.preql --content import raw.item as i;
select distinct i.brand_name order by i.brand_name;`

  ```text
  refused to write 'check.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  import raw.item as i; select ??? distinct i.brand_name order by...
  ```
- `trilogy file write answer_3247656127.preql`

  ```text
  refused to write 'answer_3247656127.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/all_sales as all_sales;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/all_sales as all_sales;
  ```
- `trilogy file write answer_2986518257.preql`

  ```text
  refused to write 'answer_2986518257.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/catalog_sales as catalog_sales;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/catalog_sales as catalog_s...
  ```
- `trilogy file write answer_3273495117.preql`

  ```text
  refused to write 'answer_3273495117.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/web_sales.preql as ws;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/web_sales.preql as ws;
  ```
- `trilogy file write answer_3544057080.preql`

  ```text
  refused to write 'answer_3544057080.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/catalog_sales as cs;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/catalog_sales as cs;

   ro...
  ```
- `trilogy file write answer_3979964698.preql`

  ```text
  refused to write 'answer_3979964698.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/all_sales as all_sales;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/all_sales as all_sales;
  ```
- `trilogy file write answer_3063407983.preql --content import raw/store_sales as ss;

select
    ss.customer.last_name,
    ss.customer.first_name,
    ss.cust…hicle_count = 3)
    and ss.customer.current_address.city != ss.pos_address.city
order by
    ss.customer.last_name,
    ss.ticket_number
limit 100;
`

  ```text
  refused to write 'answer_3063407983.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/store_sales as ss;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/store_sales as ss;  select...
  ```
- `trilogy file write diag.preql --content SELECT
`

  ```text
  refused to write 'diag.preql': not syntactically valid Trilogy.

  Parse error:
   --> 2:1
    |
  2 |
    | ^---
    |
    = expected select_item
  Location:
  SELECT ???
  ```
- `trilogy file write debug_store2.preql`

  ```text
  refused to write 'debug_store2.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a NON-NULL GRAIN KEY: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). It MUST be a key, and one that is not nullable: `count(x)` skips rows where `x` is NULL, so counting a nullable property (a name, a date, any optional field) silently undercounts. When the grain takes SEVERAL keys, name them with `grain(...)`: `count(grain(order_id, item.id))` counts order+item combinations, and `count_distinct(grain(first_name, last_name, sale_date))` counts distinct combinations - `grain()` is never NULL, so combinations with a missing member still count. For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...store_qty) as total_qty,
       ??? count(*) as cnt
   order by cnt
  ```
- `trilogy file write answer_2852230229.preql`

  ```text
  refused to write 'answer_2852230229.preql': not syntactically valid Trilogy.

  Parse error:
    --> 17:1
     |
  17 | where ws.sale_date.year = 2000
     | ^---
     |
     = expected limit, order_by, or having
  Location:
  ...tem.category, ws.item.class)
   ??? where ws.sale_date.year = 2000...
  ```
- `trilogy file write answer_210925266.preql`

  ```text
  refused to write 'answer_210925266.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/store_sales as ss;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/store_sales as ss;
   import...
  ```
- `trilogy file write answer_32504671.preql`

  ```text
  refused to write 'answer_32504671.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/store_sales as ss;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/store_sales as ss;

   def
  ```
- `trilogy file write answer_501431752.preql`

  ```text
  refused to write 'answer_501431752.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/catalog_sales as cs;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/catalog_sales as cs;

   wh...
  ```
- `trilogy file write test_preql.preql`

  ```text
  refused to write 'test_preql.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/web_sales as ws;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/web_sales as ws;
   select
  ```
- `trilogy file write answer_3281742578.preql`

  ```text
  refused to write 'answer_3281742578.preql': not syntactically valid Trilogy.

  Parse error:
    --> 11:33
     |
  11 | having wh_count >= 2 and (isnull(return_count) or return_count = 0)
     |                                 ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...ving wh_count >= 2 and (isnull ??? (return_count) or return_count...
  ```

### `file-not-found`

- `trilogy run n/a`

  ```text
  Input 'n/a' does not exist.
  ```
