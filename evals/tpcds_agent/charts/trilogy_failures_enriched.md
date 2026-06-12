# Trilogy failure analysis — 20260612-123806

- Run `20260612-123802_enriched` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 286 | failed: 37 (13%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 19 | 51% |
| `undefined-concept` | 8 | 22% |
| `syntax-parse` | 7 | 19% |
| `cli-misuse` | 2 | 5% |
| `join-resolution` | 1 | 3% |

## Detail

### `other`

- `trilogy file read query40.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw/physical_sales:sales where sales.store.id = 1 select sales.item.product_name, avg(sales.net_profit) as avg_profit, rank(sales.item.p…er (order by avg(sales.net_profit) desc) as rank_desc having avg_profit > 0.9 * (avg(sales.net_profit ? sales.sale_address.id is null) by *) limit 5;`

  ```text

  ```
- `trilogy run --import raw/physical_sales:sales auto threshold_profit <- 0.9 * avg(sales.net_profit ? sales.sale_address.id is null) by *;

where sales.store.id = 1
select
  sales.item.product_name,
  avg(sales.net_profit) as avg_profit
having
  avg_profit > threshold_profit
order by avg_profit asc
limit 10;`

  ```text

  ```
- `trilogy run raw/date.preql duckdb --param tpcds.duckdb`

  ```text

  ```
- `trilogy file read trilogy.toml`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query54.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/physical_sales.preql`

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
- `trilogy run query72.preql`

  ```text

  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query75.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/warehouse.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query82.preql`

  ```text

  ```
- `trilogy file read query82.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query84.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `undefined-concept`

- `trilogy run query46.preql`

  ```text

  ```
- `trilogy run check_dates.preql`

  ```text

  ```
- `trilogy run query54.preql`

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
- `trilogy run query82.preql`

  ```text

  ```
- `trilogy run query82.preql`

  ```text

  ```

### `syntax-parse`

- `trilogy file write query54.preql --content import raw.catalog_sales as cs;
import raw.web_sales as ws;
import raw.physical_sales as ps;

# Dec 1998 month_seq…s.store_total / 50, 0) * 50 as segment_times_50
order by segment asc nulls first,
  customer_count asc nulls first,
  segment_times_50 asc
limit 100;`

  ```text

  ```
- `trilogy file write query54.preql --content import raw.catalog_sales as cs;
import raw.web_sales as ws;
import raw.physical_sales as ps;

auto dec1998_ms <- 1…ust_total / 50, 0) * 50 as segment_times_50
order by segment asc nulls first,
    customer_count asc nulls first,
    segment_times_50 asc
limit 100;`

  ```text

  ```
- `trilogy file write query54.preql --content import raw.catalog_sales as cs;
import raw.web_sales as ws;
import raw.physical_sales as ps;

auto dec1998_ms <- 1…ust_total / 50, 0) * 50 as segment_times_50
order by segment asc nulls first,
    customer_count asc nulls first,
    segment_times_50 asc
limit 100;`

  ```text

  ```
- `trilogy file write query64.preql --content import raw/physical_sales as ss;
import raw/catalog_sales as cs;
import raw/catalog_returns as cr;

# Item-level c….store_zip
where y2000.cnt <= y1999.cnt
order by y1999.product_name, y1999.store_name, y2000.cnt, y1999.wholesale_cost_sum, y2000.wholesale_cost_sum;`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.physical_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

# Compute item….store_zip
where y2000.cnt <= y1999.cnt
order by y1999.product_name, y1999.store_name, y2000.cnt, y1999.wholesale_cost_sum, y2000.wholesale_cost_sum;`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.physical_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

merge cs.item.….store_zip
where y2000.cnt <= y1999.cnt
order by y1999.product_name, y1999.store_name, y2000.cnt, y1999.wholesale_cost_sum, y2000.wholesale_cost_sum;`

  ```text

  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.item.brand_id, all_sales.item.class_id, all_sales.item.category_id, all_sales.item.manufacturer…sales.date.year = 2001 group by all_sales.item.brand_id, all_sales.item.class_id, all_sales.item.category_id, all_sales.item.manufacturer_id limit 5;`

  ```text

  ```

### `cli-misuse`

- `trilogy run --import raw/physical_sales:sales --format json auto threshold_profit <- 0.9 * avg(sales.net_profit ? sales.sale_address.id is null) by *;

where… = 1
select
  sales.item.product_name,
  avg(sales.net_profit) as avg_profit
having
  avg_profit > threshold_profit
order by avg_profit asc
limit 10;`

  ```text
  'json' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```

### `join-resolution`

- `trilogy run query40.preql`

  ```text

  ```
