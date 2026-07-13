# Trilogy failure analysis — 20260713-171519

- Run `scope_ab_20260713-123400_baseline_4` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 99 | failed: 13 (13%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 8 | 62% |
| `syntax-parse` | 3 | 23% |
| `type-error` | 2 | 15% |

## Detail

### `other`

- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_219203908.preql`

  ```text
  Syntax error in answer_219203908.preql: Nothing was executed: parsed 4 definition statement(s) (3 rowsets, 1 import) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy run --import raw/all_sales:all_sales select lower(all_sales.channel) as ch, all_sales.item.brand_id, all_sales.item.class_id, all_sales.item.category…) in (1001001, 1, 1) having ts > 4397.124366930134 order by ch, all_sales.item.brand_id, all_sales.item.class_id, all_sales.item.category_id limit 5;`

  ```text
  Unexpected error in stdin: composite membership right-hand operands must resolve to a single existence source, got none
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe web_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run --import raw.web_sales:web_sales select count(order_number) limit 5;`

  ```text
  Syntax error in stdin: Undefined concept: local.order_number (line 2, in SELECT). Suggestions: ['web_sales.order_number', 'order_number_count']
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_569612608.preql`

  ```text
  Syntax error in answer_569612608.preql: Undefined concept: warehouse.sk. Suggestions: ['web.warehouse.sk', 'web.ship_mode.sk', 'web.ship_customer.sk', 'web.return_date.sk', 'web.return_time.sk', 'web.return_customer.sk']
  ```

### `syntax-parse`

- `trilogy run --import raw/all_sales:all_sales select lower(all_sales.channel) as ch, all_sales.item.brand_id, all_sales.item.class_id, all_sales.item.category…) order by ch nulls first, all_sales.item.brand_id nulls first, all_sales.item.class_id nulls first, all_sales.item.category_id nulls first limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ....item.category_id is not null ??? group by all_sales.item.brand_...
  ```
- `trilogy run --import raw/all_sales:all_sales select lower(all_sales.channel) as ch, all_sales.item.brand_id, all_sales.item.class_id, all_sales.item.category…les.item.category_id having ts > 4397.124366930134 order by ch, all_sales.item.brand_id, all_sales.item.class_id, all_sales.item.category_id limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...tegory_id) in (1001001, 1, 1) ??? group by ch, all_sales.item.br...
  ```
- `trilogy run --import raw/catalog_sales:cs select cs.sold_date.year, count(cs.order_number) as cnt group by cs.sold_date.year order by cs.sold_date.year;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...count(cs.order_number) as cnt ??? group by cs.sold_date.year ord...
  ```

### `type-error`

- `trilogy run --import raw/all_sales:all_sales select all_sales.channel, all_sales.item.brand_id, all_sales.item.class_id, all_sales.item.category_id, sum(all_…s.item.sk || '-' || all_sales.order_id) as line_items where all_sales.date.year between 1999 and 2001 and all_sales.date.month_of_year = 11 limit 10;`

  ```text
  Type error in stdin: Invalid argument type 'INTEGER' passed into CONCAT_STRICT function in position 1 from concept: all_sales.item.sk. Valid: 'STRING'.
  ```
- `trilogy run answer_1197120511.preql`

  ```text
  Syntax error in answer_1197120511.preql: Cannot use BETWEEN with incompatible types DATE and STRING (low)
  ```
