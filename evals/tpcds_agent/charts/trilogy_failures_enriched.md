# Trilogy failure analysis — 20260531-010204

- Run `20260531-010204` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 23 | failed: 2 (9%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-missing-alias` | 1 | 50% |
| `syntax-parse` | 1 | 50% |

## Detail

### `syntax-missing-alias`

- `trilogy run --import raw.physical_sales:ps select ps.date.year, ps.date.month_of_year, count(ps.row_counter) limit 10; duckdb`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y` Here: `count(ps.row_counter) as
  row_counter_count`
  Location:
  ...f_year, count(ps.row_counter) ??? limit 10;
  ```

### `syntax-parse`

- `trilogy run --import raw.physical_sales:ps select ps.date.year, count(ps.row_counter) as cnt group by ps.date.year order by ps.date.year limit 20; duckdb`

  ```text
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP
  BY — remove it. Grouping is automatic by the non-aggregated fields in your
  SELECT. To aggregate at a different grain than the select, write `agg(x) by
  dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
   count(ps.row_counter) as cnt ??? group by ps.date.year order by...
  ```
