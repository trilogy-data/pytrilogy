# Trilogy failure analysis — 20260607-160439

- Run `20260607-160439` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 114 | failed: 21 (18%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 14 | 67% |
| `syntax-parse` | 6 | 29% |
| `join-resolution` | 1 | 5% |

## Detail

### `other`

- `trilogy run query44.preql duckdb`

  ```text

  ```
- `trilogy run query44.preql duckdb`

  ```text

  ```
- `trilogy run query44.preql duckdb`

  ```text

  ```
- `trilogy run query44.preql duckdb`

  ```text

  ```
- `trilogy run query44.preql duckdb`

  ```text

  ```
- `trilogy run query44.preql duckdb`

  ```text

  ```
- `trilogy file read query44.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query64.preql`

  ```text

  ```
- `trilogy file read query64.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query78.preql`

  ```text

  ```
- `trilogy run query78.preql --import raw.all_sales:s select year(s.date.date) as yr, s.sales_channel as ch, count(s.order_id) as cnt limit 10;`

  ```text

  ```
- `trilogy file read query78.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy file write query44.preql -c import raw.physical_sales as ps;

# Global threshold: avg net profit at store 1 where address is null
auto null_addr_thre…rformer_name,
    coalesce(worst_pn, worst_pn) as worst_performer_name
order by r asc, best_performer_name desc, worst_performer_name desc
limit 100;`

  ```text

  ```
- `trilogy file write query64.preql --content # Query 64: Items sold in store sales with catalog price > 2x catalog refund,
# matched to store returns, self-pai…  sum(ps.ext_wholesale_cost) as wholesale_cost_sum,
    sum(ps.ext_list_price) as list_price_sum,
    sum(ps.coupon_amt) as coupon_amt_sum
limit 100;`

  ```text

  ```
- `trilogy file write query64.preql --content # Query 64: Items sold in store sales with catalog price > 2x catalog refund,
# matched to store returns, self-pai…(cnt_b, 0) as cnt_2
having cnt_1 is not null and cnt_2 is not null
    and cnt_2 <= cnt_1
order by pname, sname, cnt_2, wc_sum_1, wc_sum_2
limit 100;`

  ```text

  ```
- `trilogy file write query78.preql --content import raw.physical_sales as ps;
import raw.web_sales as ws;
import raw.catalog_sales as cs;

# Merge customer con…er by yr, itm, cust, store_qty desc, store_wcost desc, store_sprice desc,
         other_qty asc, other_wcost asc, other_sprice asc, ratio
limit 100;`

  ```text

  ```
- `trilogy file write query78.preql --content import raw.all_sales as s;

# Define filtered aggregates for each channel
# Store sales that were never returned i…er by yr, itm, cust, store_qty desc, store_wcost desc, store_sprice desc,
         other_qty asc, other_wcost asc, other_sprice asc, ratio
limit 100;`

  ```text

  ```
- `trilogy file write query78.preql --content import raw.all_sales as s;

# Filter conditions
auto store_cond <- s.sales_channel = 'STORE' and year(s.date.date)…er by yr, itm, cust, store_qty desc, store_wcost desc, store_sprice desc,
         other_qty asc, other_wcost asc, other_sprice asc, ratio
limit 100;`

  ```text

  ```

### `join-resolution`

- `trilogy run query44.preql duckdb`

  ```text

  ```
