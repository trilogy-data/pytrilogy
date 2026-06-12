# Trilogy failure analysis — 20260611-141606

- Run `20260611-141606_ingest` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 410 | failed: 77 (19%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 36 | 47% |
| `syntax-parse` | 20 | 26% |
| `syntax-missing-alias` | 7 | 9% |
| `undefined-concept` | 6 | 8% |
| `join-resolution` | 4 | 5% |
| `cli-misuse` | 2 | 3% |
| `file-not-found` | 2 | 3% |

## Detail

### `other`

- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.store_sales:store_sales auto threshold <- 0.9 * avg(store_sales.net_profit ? store_sales.store.store_sk = 1 and store_sales.customer…k = 1
select store_sales.item.product_name, avg(store_sales.net_profit) as avg_profit
having avg_profit > threshold
order by avg_profit asc
limit 20;`

  ```text

  ```
- `trilogy run --import raw.store_sales:store_sales auto threshold <- 0.9 * avg(store_sales.net_profit ? store_sales.store.store_sk = 1 and store_sales.customer…null) by *;
where store_sales.store.store_sk = 1
select count(store_sales.item.item_sk) as item_count
having avg(store_sales.net_profit) > threshold;`

  ```text

  ```
- `trilogy run --import raw.store_sales:store_sales auto threshold <- 0.9 * avg(store_sales.net_profit ? store_sales.store.store_sk = 1 and store_sales.customer…sk;

where store_sales.store.store_sk = 1
select count(store_sales.item.item_sk) as item_count, --item_avg_profit
having item_avg_profit > threshold;`

  ```text

  ```
- `trilogy run --import raw.store_sales:store_sales auto threshold <- 0.9 * avg(store_sales.net_profit ? store_sales.store.store_sk = 1 and store_sales.customer…est_performer, worst_items.prod_name as worst_performer
order by best_items.pos asc, best_items.prod_name desc, worst_items.prod_name desc
limit 100;`

  ```text

  ```
- `trilogy run --import raw.store_sales:store_sales auto threshold <- 0.9 * avg(store_sales.net_profit ? store_sales.store.store_sk = 1 and store_sales.customer…g_profit > threshold at store 1?
select count(store_sales.item.item_sk) as c
where store_sales.store.store_sk = 1
having item_avg_profit > threshold;`

  ```text

  ```
- `trilogy run --import raw.store_sales:store_sales auto threshold <- 0.9 * avg(store_sales.net_profit ? store_sales.store.store_sk = 1 and store_sales.customer…est_items.pos) by *;

select count(store_sales.item.item_sk) as c, --min_pos
where store_sales.store.store_sk = 1
having item_avg_profit > threshold;`

  ```text

  ```
- `trilogy file read query44.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query46.preql`

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
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query72.preql`

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
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe store_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
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
- `trilogy run query82.preql`

  ```text

  ```
- `trilogy file read query82.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe store_returns`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```

### `syntax-parse`

- `trilogy run --import raw.store_sales:store_sales select store_sales.date_dim.year, store_sales.date_dim.moy, count(store_sales.ticket_number) as cnt where store_sales.date_dim.year = 1999 group by 1,2 order by 1,2 limit 12;`

  ```text

  ```
- `trilogy run --import raw.store_sales:ss --import raw.store_returns:sr select ss.ticket_number, ss.item.item_id, ss.quantity as ss_qty, sr.return_quantity as ….item.item_sk = ss.item.item_sk inner join sr.ticket_number = ss.ticket_number inner join sr.customer.customer_sk = ss.customer.customer_sk limit 10;`

  ```text

  ```
- `trilogy run --import raw.store_sales:store_sales auto threshold <- 0.9 * avg(store_sales.net_profit ? store_sales.store.store_sk = 1 and store_sales.customer…key
select best_items.pos as best_pos, best_items.prod_name as best_name, worst_items.pos as worst_pos, worst_items.prod_name as worst_name
limit 10;`

  ```text

  ```
- `trilogy run --import raw.store_sales:store_sales auto threshold <- 0.9 * avg(store_sales.net_profit ? store_sales.store.store_sk = 1 and store_sales.customer…_name as best_performer, worst_items.prod_name as worst_performer
order by rank asc, best_items.prod_name desc, worst_items.prod_name desc
limit 100;`

  ```text

  ```
- `trilogy run --import raw.store_sales:store_sales auto threshold <- 0.9 * avg(store_sales.net_profit ? store_sales.store.store_sk = 1 and store_sales.customer….prod_name as worst_performer, worst_items.profit as worst_profit
order by rank asc, best_items.prod_name desc, worst_items.prod_name desc
limit 100;`

  ```text

  ```
- `trilogy file write query54.preql -e -c import raw.catalog_sales as cs;
import raw.web_sales as ws;
import raw.store_sales as ss;

# Get customers who bought …ss.store.state = ss.customer.customer_address.state
order by segment asc nulls first, customer_count asc nulls first, segment_times_50 asc
limit 100;`

  ```text

  ```
- `trilogy file write query54.preql -e -c import raw.catalog_sales as cs;
import raw.web_sales as ws;
import raw.store_sales as ss;

# Get customers who bought …rs
with all_qualifying as union(
    (select cat_cust.customer_sk from cat_cust),
    (select web_cust.customer_sk from web_cust)
) -> (customer_sk);`

  ```text

  ```
- `trilogy run --import raw/store_sales:ss select ss.customer.customer_sk, sum(ss.ext_sales_price) as total_spend where ss.date_dim.month_seq between 1188 and 1….customer_sk from qualifying_cust) and ss.store.county = ss.customer.customer_address.county and ss.store.state = ss.customer.customer_address.state;`

  ```text

  ```
- `trilogy run --import raw/store_sales:ss --import raw/catalog_sales:cs select count(ss.customer.customer_sk) as cnt where ss.date_dim.month_seq between 1188 a… = 1998 and cs.sold_date.moy = 12) and ss.store.county = ss.customer.customer_address.county and ss.store.state = ss.customer.customer_address.state;`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.catalog_returns as ca…sale_cost) as wholesale_cost_sum,
    sum(store_sales.ext_list_price) as list_price_sum,
    sum(store_sales.coupon_amt) as coupon_amt_sum
limit 100;`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.catalog_returns as ca…sale_cost) as wholesale_cost_sum,
    sum(store_sales.ext_list_price) as list_price_sum,
    sum(store_sales.coupon_amt) as coupon_amt_sum
limit 100;`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.catalog_returns as ca…sale_cost) as wholesale_cost_sum,
    sum(store_sales.ext_list_price) as list_price_sum,
    sum(store_sales.coupon_amt) as coupon_amt_sum
limit 100;`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.catalog_returns as ca…oduct_name asc,
    yr1999.store_name asc,
    yr2000.line_count asc,
    yr1999.wholesale_cost_sum asc,
    yr2000.wholesale_cost_sum asc
limit 100;`

  ```text

  ```
- `trilogy file write query76.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sales as cs;

with combined as union(…ed.missing_ref asc nulls first,
    combined.year asc nulls first,
    combined.qoy asc nulls first,
    combined.category asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query76.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sales as cs;

with combined as union(…ed.missing_ref asc nulls first,
    combined.year asc nulls first,
    combined.qoy asc nulls first,
    combined.category asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query76.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sales as cs;

with combined as union(…ed.missing_ref asc nulls first,
    combined.year asc nulls first,
    combined.qoy asc nulls first,
    combined.category asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query77.preql --content import raw.store_sales as store_sales;
import raw.store_returns as store_returns;
import raw.catalog_sales as cata…ns) as returns,
  sum(combined.profit) as profit
order by combined.channel nulls first, combined.outlet nulls first, combined.returns desc
limit 100;`

  ```text

  ```
- `trilogy file write query77.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r…ned.sales,
  combined.returns,
  combined.profit
order by combined.channel nulls first, combined.outlet nulls first, combined.returns desc
limit 100;`

  ```text

  ```
- `trilogy file write test_rollup2.preql --content select 1;`

  ```text

  ```
- `trilogy file write query82.preql --content import raw.item as item;
import raw.inventory as inv;
import raw.store_sales as sales;

where item.current_price b…sed)
inner join inv.item.item_sk = item.item_sk

select
    item.item_id,
    item.item_desc,
    item.current_price
order by item.item_id
limit 100;`

  ```text

  ```

### `syntax-missing-alias`

- `trilogy run --import raw.store_sales:store_sales select store_sales.date_dim.moy, count(store_sales.ticket_number) limit 5;`

  ```text

  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.store.store_sk, count(store_sales.item.item_sk) by store_sales.store.store_sk limit 10;`

  ```text

  ```
- `trilogy run --import raw.store_sales:store_sales auto threshold <- 0.9 * avg(store_sales.net_profit ? store_sales.store.store_sk = 1 and store_sales.customer…_items.pos = worst_items.pos
where best_items.pos <= 10
order by best_items.pos asc, best_items.prod_name desc, worst_items.prod_name desc
limit 100;`

  ```text

  ```
- `trilogy run --import raw/store_sales:ss select ss.date_dim.month_seq, ss.date_dim.year, ss.date_dim.moy, count(ss.item.item_sk) where ss.date_dim.year = 1999 and ss.date_dim.moy between 1 and 3 limit 10;`

  ```text

  ```
- `trilogy run --import raw/catalog_sales:cs select cs.bill_customer.customer_demographics.marital_status, count(cs.order_number) by cs.bill_customer.customer_demographics.marital_status limit 20;`

  ```text

  ```
- `trilogy file write query82.preql --content import raw.inventory as inv;
import raw.store_sales as store_sales;

where inv.item.current_price between 62 and 9…ore_sales.item.item_sk

select distinct
    inv.item.item_id,
    inv.item.item_desc,
    inv.item.current_price
order by inv.item.item_id
limit 100;`

  ```text

  ```
- `trilogy run --import raw.item:item --import raw.inventory:inv select distinct item.item_id where item.current_price between 62 and 92 and item.manufact_id in…sk in inv.item.item_sk ? (inv.date_dim.date between '2000-05-25'::date and '2000-07-24'::date and inv.quantity_on_hand between 100 and 500) limit 10;`

  ```text

  ```

### `undefined-concept`

- `trilogy run query64.preql`

  ```text

  ```
- `trilogy run query64.preql`

  ```text

  ```
- `trilogy run query64.preql`

  ```text

  ```
- `trilogy run query68.preql`

  ```text

  ```
- `trilogy run query75.preql`

  ```text

  ```
- `trilogy run query82.preql`

  ```text

  ```

### `join-resolution`

- `trilogy run query64.preql`

  ```text

  ```
- `trilogy run --import raw/catalog_sales:cs --import raw/inventory:inv select cs.item.item_desc, cs.sold_date.week_seq, inv.warehouse.warehouse_name, cs.quantity, inv.quantity_on_hand, cs.promotion.promo_sk limit 5;`

  ```text

  ```
- `trilogy run query82.preql`

  ```text

  ```
- `trilogy run query82.preql`

  ```text

  ```

### `cli-misuse`

- `trilogy run query54.preql --format json`

  ```text
  '--format' is not a valid dialect. The dialect argument comes after the input file and any options.
    Try: trilogy run <input> [<dialect>]
  ```
- `trilogy run query77.preql --format json`

  ```text
  '--format' is not a valid dialect. The dialect argument comes after the input file and any options.
    Try: trilogy run <input> [<dialect>]
  ```

### `file-not-found`

- `trilogy run query82.preql`

  ```text

  ```
- `trilogy run query82.preql`

  ```text

  ```
