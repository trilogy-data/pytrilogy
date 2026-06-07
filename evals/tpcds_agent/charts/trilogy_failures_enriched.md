# Trilogy failure analysis — 20260607-005408

- Run `20260607-005408` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 566 | failed: 79 (14%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 31 | 39% |
| `syntax-parse` | 22 | 28% |
| `join-resolution` | 14 | 18% |
| `undefined-concept` | 7 | 9% |
| `syntax-missing-alias` | 4 | 5% |
| `type-error` | 1 | 1% |

## Detail

### `other`

- `trilogy run query02.preql`

  ```text

  ```
- `trilogy file read query02.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query06.preql duckdb --import raw.physical_sales:store_sales select store_sales.customer.address.state as state, count(store_sales.line_item) as …r.address.id is not null and store_sales.date.year = 2001 and store_sales.date.month_of_year = 1 group by state order by cnt asc nulls first limit 5;`

  ```text

  ```
- `trilogy file read query06.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/date.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/physical_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query49.preql`

  ```text

  ```
- `trilogy run query49.preql`

  ```text

  ```
- `trilogy run query59.preql`

  ```text

  ```
- `trilogy file read raw/physical_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query59.preql`

  ```text

  ```
- `trilogy file read query59.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query64.preql`

  ```text

  ```
- `trilogy run query64.preql`

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
- `trilogy file read query75.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe catalog_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy database describe store_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy database describe web_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run test_merge2.preql`

  ```text

  ```
- `trilogy run test_simple4.preql`

  ```text

  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
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
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query86.preql`

  ```text

  ```

### `syntax-parse`

- `trilogy file write query02.preql --content import raw.all_sales as sales;

# Sum of extended sales price for web and catalog channels, by week_seq and day_na…io,
    @thu_ratio as thursday_ratio,
    @fri_ratio as friday_ratio,
    @sat_ratio as saturday_ratio
order by sales.date.week_seq asc nulls first;
`

  ```text

  ```
- `trilogy run --import raw.all_sales:sales select sales.date.year, min(sales.date.week_seq) as min_ws, max(sales.date.week_seq) as max_ws, count(sales.ext_sales_price ? sales.sales_channel in ('WEB', 'CATALOG')) as cnt group by sales.date.year order by sales.date.year;`

  ```text

  ```
- `trilogy file write query02.preql --content import raw.all_sales as sales;

select
    sales.date.week_seq,
    round(
        sum(sales.ext_sales_price ? sal… sum(sales.ext_sales_price ? sales.sales_channel in ('WEB', 'CATALOG') and sales.date.year = 2001) > 0
order by sales.date.week_seq asc nulls first;
`

  ```text

  ```
- `trilogy file write query06.preql --content import raw.physical_sales as store_sales;

# Per-category average of distinct item current prices
# First, get dis…ddress.state as state,
    count(line_item) as qualifying_line_items
order by qualifying_line_items asc nulls first, state asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query06.preql --content import raw.physical_sales as store_sales;

# Per-category average of distinct item current prices
# First, get dis…ddress.state as state,
    count(line_item) as qualifying_line_items
order by qualifying_line_items asc nulls first, state asc nulls first
limit 100;`

  ```text

  ```
- `trilogy run --import raw.physical_sales:store_sales select store_sales.customer.address.state as state, count(store_sales.line_item) as cnt where store_sales…ar = 2001 and store_sales.date.month_of_year = 1 and store_sales.customer.address.id is not null group by state order by cnt asc nulls first limit 5;`

  ```text

  ```
- `trilogy run --import raw.physical_sales:ss select ss.date.year, count(ss.ticket_number) as cnt group by 1 order by 1;`

  ```text

  ```
- `trilogy file write query75.preql --content import raw.all_sales as all_sales;
import raw.item as item;

# For items in the 'Books' category, compute per-line…s qty_diff,
  y2002.total_amt - y2001.amt_2001 as amt_diff
where y2002.qty_2002 / y2001.qty_2001 < 0.9
order by qty_diff asc, amt_diff asc
limit 100;`

  ```text

  ```
- `trilogy file write query75.preql --content import raw.all_sales as all_sales;
import raw.item as item;

# For items in the 'Books' category, compute net sale…_01, 0)) as qty_diff,
  coalesce(amt_02, 0) as curr_amt
having coalesce(qty_02, 0) * 1.0 / coalesce(qty_01, 0) < 0.9
order by qty_diff asc
limit 100;`

  ```text

  ```
- `trilogy file write query75.preql --content import raw.all_sales as all_sales;
import raw.item as item;

# For items in the 'Books' category, compute net sale…,
  (coalesce(amt_02, 0) - 0) as amt_diff
having coalesce(qty_02, 0) * 1.0 / coalesce(qty_01, 0) < 0.9
order by qty_diff asc, amt_diff asc
limit 100;`

  ```text

  ```
- `trilogy file write query75.preql --content import raw.all_sales as all_sales;
import raw.item as item;

merge all_sales.item.id into ~item.id;

# Deduplicate…01, 2002)
  and item.category = 'Books'
order by lines.brand_id, lines.class_id, lines.category_id, lines.manufacturer_id, lines.sale_year
limit 200;`

  ```text

  ```
- `trilogy run --import raw.all_sales:all_sales --import raw.item:item merge all_sales.item.id into ~item.id; rowset lines <- select item.brand_id as brand_id, …y) as qty where lines.sale_year = 2001 and lines.brand_id = 1001001 and lines.class_id = 3 and lines.category_id = 9 and lines.manufacturer_id = 332;`

  ```text

  ```
- `trilogy run --import raw.all_sales:all_sales --import raw.item:item merge all_sales.item.id into ~item.id; select all_sales.date.year as yr, sum(all_sales.qu…category = 'Books' and item.brand_id = 9005009 and item.class_id = 5 and item.category_id = 9 and item.manufacturer_id = 853 group by yr order by yr;`

  ```text

  ```
- `trilogy run --import raw.all_sales:all_sales --import raw.item:item merge all_sales.item.id into ~item.id; where all_sales.date.year = 2001 and item.category…ce(round(qty_02 * 1.0 / qty_01, 6), 0) as ratio where round(coalesce(qty_02, 0) * 1.0 / coalesce(qty_01, 0), 6) < 0.9 order by qty_diff asc limit 50;`

  ```text

  ```
- `trilogy file write query76.preql --content import raw.physical_sales as store_sales;
import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_s…nnel asc nulls first,
    missing_ref asc nulls first,
    year asc nulls first,
    quarter asc nulls first,
    category asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write multi_select.preql --content -- Store: missing store reference
import raw.physical_sales as ss;
where ss.store.id is null
select
    'stor…
    channel asc nulls first,
    ref asc nulls first,
    year asc nulls first,
    quarter asc nulls first,
    category asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query76.preql --content import raw.physical_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sales as cs;

parameter channel st…ice) as tot
order by channel asc nulls first, ref asc nulls first, year asc nulls first, quarter asc nulls first, category asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query77.preql --content import raw.physical_sales as store;
import raw.catalog_sales as catalog;
import raw.web_sales as web;

# Store arm…lesce(s_returns, 0) as returns,
    coalesce(s_profit, 0) as profit
order by channel asc nulls first, outlet asc nulls first, returns desc
limit 100;`

  ```text

  ```
- `trilogy file write query78.preql --content import raw.physical_sales as store_sales;
import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_s… > 0
order by yr, item_id, customer_id, store_qty desc, store_wholesale desc, store_sales_price desc, other_qty, other_sales_price, ratio
limit 100;
`

  ```text

  ```
- `trilogy run --import raw.physical_sales:store_sales --import raw.web_sales:web_sales select store_sales.item.id as item, store_sales.customer.id as cust wher…sales.billing_customer.id where year(web_sales.date.date)=2000 and web_sales.billing_customer.id is not null and not web_sales.is_returned) limit 10;`

  ```text

  ```
- `trilogy run --import raw.physical_sales:store_sales --import raw.web_sales:web_sales merge store_sales.customer.id into ~web_sales.billing_customer.id select…s.date.date)=2000 and web_sales.billing_customer.id is not null and not web_sales.is_returned and web_sales.billing_customer.id is not null limit 10;`

  ```text

  ```
- `trilogy file write query86.preql --content import raw.web_sales as web_sales;

# Filter to year 2000 sales
where year(web_sales.date.date) = 2000

# Use roll…  hierarchy_level desc nulls first,
    web_sales.item.category nulls first,
    web_sales.item.class nulls first,
    total_net_paid desc
limit 100;`

  ```text

  ```

### `join-resolution`

- `trilogy run - duckdb --config trilogy.toml`

  ```text

  ```
- `trilogy run query59.preql`

  ```text

  ```
- `trilogy run query64.preql`

  ```text

  ```
- `trilogy run query64.preql`

  ```text

  ```
- `trilogy run query75.preql`

  ```text

  ```
- `trilogy run --import raw.all_sales:all_sales --import raw.item:item select all_sales.date.year as yr, count(all_sales.order_id) as cnt where item.category = 'Books' and all_sales.date.year in (2001, 2002);`

  ```text

  ```
- `trilogy run --import raw.all_sales:all_sales --import raw.item:item --import raw.date:date select date.year as yr, sum(all_sales.quantity) as qty, sum(all_sales.ext_sales_price) as amt where item.category = 'Books' and date.year in (2001, 2002);`

  ```text

  ```
- `trilogy run --import raw.all_sales:all_sales --import raw.item:item select all_sales.date.year as yr, sum(all_sales.quantity) as qty, sum(all_sales.ext_sales_price) as amt where item.category = 'Books' and all_sales.date.year in (2001, 2002);`

  ```text

  ```
- `trilogy run test_simple5.preql`

  ```text

  ```
- `trilogy run multi_select.preql`

  ```text

  ```
- `trilogy run test_multi3.preql`

  ```text

  ```
- `trilogy run query76.preql`

  ```text

  ```
- `trilogy run -`

  ```text

  ```
- `trilogy run query76.preql`

  ```text

  ```

### `undefined-concept`

- `trilogy run query06.preql duckdb`

  ```text

  ```
- `trilogy run query76.preql`

  ```text

  ```
- `trilogy run test_merge.preql`

  ```text

  ```
- `trilogy run query76.preql`

  ```text

  ```
- `trilogy run test_simple.preql`

  ```text

  ```
- `trilogy run test_simple3.preql`

  ```text

  ```
- `trilogy run query77.preql`

  ```text

  ```

### `syntax-missing-alias`

- `trilogy run --import raw.catalog_store_returns:csr select count(csr.ticket_number), csr.store_sale_date.year, csr.store_sale_date.quarter;`

  ```text

  ```
- `trilogy run --import raw.item:item select count(item.id) where item.category = 'Books';`

  ```text

  ```
- `trilogy file write query77.preql --content import raw.all_sales as sales;

# Channel label based on sales_channel enum
auto channel_label <- case when sales.… total_profit by rollup channel_label, outlet_id as profit
order by channel_label asc nulls first, outlet_id asc nulls first, returns desc
limit 100;`

  ```text

  ```
- `trilogy file write query77.preql --content import raw.physical_sales as store;
import raw.catalog_sales as catalog;
import raw.web_sales as web;

# Store cha… coalesce(returns, 0) as returns,
    coalesce(profit, 0) as profit
order by channel asc nulls first, outlet asc nulls first, returns desc
limit 100;`

  ```text

  ```

### `type-error`

- `trilogy run - duckdb --config trilogy.toml`

  ```text

  ```
