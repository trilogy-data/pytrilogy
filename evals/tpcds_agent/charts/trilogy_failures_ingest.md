# Trilogy failure analysis — 20260611-133359

- Run `20260611-133358_ingest` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 375 | failed: 57 (15%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 33 | 58% |
| `syntax-parse` | 15 | 26% |
| `undefined-concept` | 5 | 9% |
| `join-resolution` | 2 | 4% |
| `syntax-missing-alias` | 1 | 2% |
| `file-not-found` | 1 | 2% |

## Detail

### `other`

- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query44.preql duckdb`

  ```text

  ```
- `trilogy file read query44.preql`

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
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query54.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query64.preql duckdb`

  ```text

  ```
- `trilogy run query64.preql duckdb`

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
- `trilogy file read query75.preql`

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
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 58 column 12 (char 2491). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query77.preql`

  ```text

  ```
- `trilogy run query77.preql`

  ```text
  trilogy error: subprocess timed out after 600s.
  ```
- `trilogy run query77.preql`

  ```text

  ```
- `trilogy run query77.preql`

  ```text

  ```
- `trilogy run query77.preql`

  ```text

  ```
- `trilogy run query77.preql`

  ```text

  ```
- `trilogy run query77.preql`

  ```text

  ```
- `trilogy run query77.preql`

  ```text

  ```
- `trilogy run query77.preql`

  ```text

  ```
- `trilogy run query77.preql`

  ```text

  ```
- `trilogy run query77.preql`

  ```text

  ```
- `trilogy file read query84.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy run --import raw/catalog_sales:cs --import raw/catalog_returns:cr merge cr.order_number into cs.order_number; merge cr.item.item_sk into cs.item.item…unt(cr.order_number) as return_count, count(cs.order_number) as sale_count group by cs.order_number, cs.item.item_sk having return_count > 1 limit 5;`

  ```text

  ```
- `trilogy file write query44.preql --content import raw.store_sales as store_sales;

# Compute average net profit by item for store_sk = 1
auto item_avg_profit…r by item_avg_profit asc) as rank_asc,
  rank(store_sales.item.item_sk) over (order by item_avg_profit desc) as rank_desc
order by rank_asc
limit 20;`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.item as item;

# Link catalog_sales a…ash + cr.reversed_charge + cr.store_credit) as cat_refund
    having cat_ext_list_price > 2 * cat_refund
;

select qualifying_items.item_sk
limit 10;`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

# Link catalog_returns to catalog_sales via ite…ash + cr.reversed_charge + cr.store_credit) as cat_refund
    having cat_ext_list_price > 2 * cat_refund
;

select qualifying_items.item_sk
limit 10;`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.store_sales as ss;
import raw.store_r…000.cnt as cnt_2000
order by
    y1999.product_name,
    y1999.store_name,
    y2000.cnt,
    y1999.sum_wholesale_cost,
    y2000.sum_wholesale_cost;`

  ```text

  ```
- `trilogy file write query72.preql --content import raw.catalog_sales as cs;
import raw.inventory as inv;

where cs.sold_date.year = 1999
and cs.bill_hdemo.buy… desc nulls first,
    cs.item.item_desc nulls first,
    inv.warehouse.warehouse_name nulls first,
    cs.sold_date.week_seq nulls first
limit 100;
`

  ```text

  ```
- `trilogy file write query75.preql --content import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.store_sales as ss;
import raw.store_r…  y2002.qty as curr_qty,
    y2002.qty - y2001.qty as qty_diff,
    y2002.amt - y2001.amt as amt_diff
order by qty_diff asc, amt_diff asc
limit 100;
`

  ```text

  ```
- `trilogy file write query75.preql --content import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.store_sales as ss;
import raw.store_r…  y2002.qty as curr_qty,
    y2002.qty - y2001.qty as qty_diff,
    y2002.amt - y2001.amt as amt_diff
order by qty_diff asc, amt_diff asc
limit 100;
`

  ```text

  ```
- `trilogy file write query76.preql --content with combined as union(
    (import raw.store_sales as ss
     where ss.store.store_sk is null
     select
       …irst,
    combined.missing_ref nulls first,
    combined.year nulls first,
    combined.qoy nulls first,
    combined.category nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query76.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sales as cs;

with combined as union(…irst,
    combined.missing_ref nulls first,
    combined.year nulls first,
    combined.qoy nulls first,
    combined.category nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query76.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sales as cs;

with combined as union(…irst,
    combined.missing_ref nulls first,
    combined.year nulls first,
    combined.qoy nulls first,
    combined.category nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query77.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;

merge ss.store.store_sk into ~sr.store.store_sk;

w…rns), 0) as returns,
  coalesce(sum(combined.profit), 0) as profit
order by channel nulls first, combined.outlet nulls first, returns desc
limit 100;`

  ```text

  ```
- `trilogy file write query77.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;

merge ss.store.store_sk into ~sr.store.store_sk;

w…annel, outlet as returns,
  sum(profit) by rollup channel, outlet as profit
order by channel nulls first, outlet nulls first, returns desc
limit 100;`

  ```text

  ```
- `trilogy file write query77.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;

where ss.date_dim.date between '2000-08-23'::date a… as returns,
  sum(ss.net_profit) - coalesce(sum(sr.net_loss), 0) as profit
order by channel nulls first, outlet nulls first, returns desc
limit 100;`

  ```text

  ```
- `trilogy run --import raw.store_returns:store_returns --import raw.customer:any_customer select any_customer.customer_demographics.demo_sk, store_returns.cust…tomer.customer_address.city = 'Edgewood' inner join any_customer.customer_demographics.demo_sk = store_returns.customer_demographics.demo_sk limit 5;`

  ```text

  ```

### `undefined-concept`

- `trilogy run query64.preql duckdb`

  ```text

  ```
- `trilogy run query68.preql`

  ```text

  ```
- `trilogy run query75.preql`

  ```text

  ```
- `trilogy run query75.preql`

  ```text

  ```
- `trilogy run query76.preql`

  ```text

  ```

### `join-resolution`

- `trilogy run --import raw/catalog_sales:cs --import raw/catalog_returns:cr select cs.order_number, cs.item.item_id, cr.refunded_cash limit 5;`

  ```text

  ```
- `trilogy run query64.preql duckdb`

  ```text

  ```

### `syntax-missing-alias`

- `trilogy run --import raw.store_sales:store_sales select store_sales.store.store_sk, avg(store_sales.net_profit) limit 10;`

  ```text

  ```

### `file-not-found`

- `trilogy run query44.preql duckdb`

  ```text

  ```
