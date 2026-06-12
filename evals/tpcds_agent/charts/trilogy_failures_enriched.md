# Trilogy failure analysis — 20260611-141606

- Run `20260611-141606_enriched` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 326 | failed: 68 (21%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 30 | 44% |
| `syntax-parse` | 27 | 40% |
| `undefined-concept` | 6 | 9% |
| `join-resolution` | 2 | 3% |
| `syntax-missing-alias` | 2 | 3% |
| `file-not-found` | 1 | 1% |

## Detail

### `other`

- `trilogy file read raw/catalog_store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/physical_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.physical_sales:sales auto ref_avg <- avg(sales.net_profit ? sales.store.id = 1 and sales.sale_address.id is null) by *; select count(sales.item.id) as num_items where sales.store.id = 1 and (avg(sales.net_profit ? sales.store.id = 1) by sales.item.id) > 0.9 * ref_avg;`

  ```text

  ```
- `trilogy run --import raw.physical_sales:sales auto ref_avg <- avg(sales.net_profit ? sales.store.id = 1 and sales.sale_address.id is null) by * ; auto item_a…vg(sales.net_profit ? sales.store.id = 1) by sales.item.id; select count(sales.item.id) as cnt where sales.store.id = 1 and item_avg > 0.9 * ref_avg;`

  ```text

  ```
- `trilogy run --import raw.physical_sales:sales auto ref_avg <- avg(sales.net_profit ? sales.store.id = 1 and sales.sale_address.id is null) by * ; auto item_a…avg asc) as rnk_asc, rank(sales.item.id) over (order by item_avg desc) as rnk_desc where sales.store.id = 1 having item_avg > 0.9 * ref_avg limit 20;`

  ```text

  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy file read query44.preql`

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
- `trilogy run query64.preql`

  ```text

  ```
- `trilogy run query64.preql`

  ```text

  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid \escape: line 1 column 91 (char 90). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file read query75.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/physical_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/customer.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy file write query44.preql -e -c import raw.physical_sales as sales;

# Reference average net profit at store 1 where sale's address key is null
auto r…best_rn.rank = worst_rn.rank
where best_rn.rank <= 10
order by rank asc,
         best_product_name desc,
         worst_product_name desc
limit 100;`

  ```text

  ```
- `trilogy file write query44.preql -e -c import raw.physical_sales as sales;

# Reference average net profit at store 1 where sale's address key is null
auto r…rn.rank <= 10
inner join best_rn.rank = worst_rn.rank
order by rank asc,
         best_product_name desc,
         worst_product_name desc
limit 100;`

  ```text

  ```
- `trilogy run --import raw.physical_sales:sales auto ref_avg <- avg(sales.net_profit ? sales.store.id = 1 and sales.sale_address.id is null) by *; auto item_av…item_avg where sales.store.id = 1 having item_avg > 0.9 * ref_avg; select best_rn.rank, best_rn.product_name from best_rn order by rank asc limit 15;`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.physical_sales as ps;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.phys…lesale_cost) as total_wholesale_cost,
  sum(ps.ext_list_price) as total_list_price,
  sum(ps.coupon_amt) as total_coupon_amt
order by 1, 2
limit 100;`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.physical_sales as ps;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.phys…lesale_cost) as total_wholesale_cost,
  sum(ps.ext_list_price) as total_list_price,
  sum(ps.coupon_amt) as total_coupon_amt
order by 1, 2
limit 100;`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.physical_sales as ps;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

# Build item-l…lesale_cost) as total_wholesale_cost,
  sum(ps.ext_list_price) as total_list_price,
  sum(ps.coupon_amt) as total_coupon_amt
order by 1, 2
limit 100;`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.physical_sales as ps;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

# Build item-l…lesale_cost) as total_wholesale_cost,
  sum(ps.ext_list_price) as total_list_price,
  sum(ps.coupon_amt) as total_coupon_amt
order by 1, 2
limit 100;`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.physical_sales as ps;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

# Build item-l…marital_status
  
inner join cat_sales_agg.item_id = cat_returns_agg.item_id

select
  year(ps.date.date) as sale_year,
  count(*) as cnt
order by 1;`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.physical_sales as ps;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

# Build item-l…on as coup_2000,
  2000 as yr_2000,
  agg_2000.line_cnt as cnt_2000
order by 1, 2, agg_2000.line_cnt, agg_1999.tot_wholesale, agg_2000.tot_wholesale;`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.physical_sales as ps;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

# Build item-l…on as coup_2000,
  2000 as yr_2000,
  agg_2000.line_cnt as cnt_2000
order by 1, 2, agg_2000.line_cnt, agg_1999.tot_wholesale, agg_2000.tot_wholesale;`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.physical_sales as ps;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

# Build item-l…on as coup_2000,
  2000 as yr_2000,
  agg_2000.line_cnt as cnt_2000
order by 1, 2, agg_2000.line_cnt, agg_1999.tot_wholesale, agg_2000.tot_wholesale;`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.physical_sales as ps;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

# Build item-l…on as coup_2000,
  2000 as yr_2000,
  agg_2000.line_cnt as cnt_2000
order by 1, 2, agg_2000.line_cnt, agg_1999.tot_wholesale, agg_2000.tot_wholesale;`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.physical_sales as ps;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

rowset cat_sal…agg_2000.tcoupon as coup_2000,
  2000 as yr_2000,
  agg_2000.lcnt as cnt_2000
order by 1, 2, agg_2000.lcnt, agg_1999.twholesale, agg_2000.twholesale;`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.physical_sales as ps;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

rowset cat_sal…agg_2000.tcoupon as coup_2000,
  2000 as yr_2000,
  agg_2000.lcnt as cnt_2000
order by 1, 2, agg_2000.lcnt, agg_1999.twholesale, agg_2000.twholesale;`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.physical_sales as ps;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

rowset cat_sal…agg_2000.tcoupon as coup_2000,
  2000 as yr_2000,
  agg_2000.lcnt as cnt_2000
order by 1, 2, agg_2000.lcnt, agg_1999.twholesale, agg_2000.twholesale;`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.physical_sales as ps;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

rowset cat_sal…000.tw as w00,
  agg_2000.tl as l00,
  agg_2000.tc as cp00,
  2000 as y00,
  agg_2000.lc as c00
order by 1, 2, agg_2000.lc, agg_1999.tw, agg_2000.tw;`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.physical_sales as ps;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

rowset cat_sal…000.tw as w00,
  agg_2000.tl as l00,
  agg_2000.tc as cp00,
  2000 as y00,
  agg_2000.lc as c00
order by 1, 2, agg_2000.lc, agg_1999.tw, agg_2000.tw;`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.physical_sales as ps;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

rowset cat_sal…w99,
  agg_1999.tl as l99,
  agg_1999.tc as cp99,
  agg_2000.tw as w00,
  agg_2000.tl as l00,
  agg_2000.tc as cp00,
  agg_2000.lc as c00
order by 1;`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.physical_sales as ps;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

rowset cat_sal…w99,
  agg_1999.tl as l99,
  agg_1999.tc as cp99,
  agg_2000.tw as w00,
  agg_2000.tl as l00,
  agg_2000.tc as cp00,
  agg_2000.lc as c00
order by 1;`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.physical_sales as ps;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

rowset cat_sal…;

# Items in both years
inner join items_1999.iid = items_2000.iid
inner join items_1999.sn = items_2000.sn
inner join items_1999.sz = items_2000.sz`

  ```text

  ```
- `trilogy file write query72.preql --content import raw.catalog_sales as catalog_sales;
import raw.inventory as inventory;

where catalog_sales.sold_date.year …otal_orders desc nulls first,
    item_description asc nulls first,
    warehouse_name asc nulls first,
    week_sequence asc nulls first

limit 100;`

  ```text

  ```
- `trilogy file write query75.preql --content import raw.all_sales as all_sales;

# Only Books category
where all_sales.item.category = 'Books'

# Per-line net …_qty,
    y2002.qty_2002 - y2001.qty_2001 as qty_diff,
    y2002.amt_2002 - y2001.amt_2001 as amt_diff
order by qty_diff asc, amt_diff asc
limit 100;`

  ```text

  ```
- `trilogy file write query75.preql --content import raw.all_sales as all_sales;

# Only Books category
where all_sales.item.category = 'Books'

# Per-line net …_qty,
    y2002.qty_2002 - y2001.qty_2001 as qty_diff,
    y2002.amt_2002 - y2001.amt_2001 as amt_diff
order by qty_diff asc, amt_diff asc
limit 100;`

  ```text

  ```
- `trilogy file write query75.preql --content import raw.all_sales as all_sales;

# Per-line net sales (row level before aggregation)
auto net_qty <- all_sales.…_qty,
    y2002.qty_2002 - y2001.qty_2001 as qty_diff,
    y2002.amt_2002 - y2001.amt_2001 as amt_diff
order by qty_diff asc, amt_diff asc
limit 100;`

  ```text

  ```
- `trilogy file write query75.preql --content import raw.all_sales as all_sales;

# Per-line net sales (concepts defined at row level, before aggregation)
auto …_qty,
    y2002.qty_2002 - y2001.qty_2001 as qty_diff,
    y2002.amt_2002 - y2001.amt_2001 as amt_diff
order by qty_diff asc, amt_diff asc
limit 100;`

  ```text

  ```
- `trilogy file write query75.preql --content import raw.all_sales as all_sales;

# Per-line net sales (concepts defined at row level, before aggregation)
auto …_qty,
    y2002.qty_2002 - y2001.qty_2001 as qty_diff,
    y2002.amt_2002 - y2001.amt_2001 as amt_diff
order by qty_diff asc, amt_diff asc
limit 100;`

  ```text

  ```
- `trilogy file write query75.preql --content import raw.all_sales as all_sales;

# Per-line net sales (concepts defined at row level, before aggregation)
auto …2001 as amt_diff
having cast(y2002.qty_2002 as float) / nullif(cast(y2001.qty_2001 as float), 0) < 0.9
order by qty_diff asc, amt_diff asc
limit 100;`

  ```text

  ```

### `undefined-concept`

- `trilogy run query68.preql`

  ```text

  ```
- `trilogy run --import raw/catalog_sales:catalog_sales select catalog_sales.item.id, catalog_sales.order_number, promotion.id limit 5;`

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

### `join-resolution`

- `trilogy run query40.preql`

  ```text

  ```
- `trilogy run query84.preql duckdb`

  ```text

  ```

### `syntax-missing-alias`

- `trilogy run --import raw.physical_sales:sales select sales.store.id, sales.store.name, count(sales.item.id) where sales.store.id = 1;`

  ```text

  ```
- `trilogy run --import raw/physical_sales:ps select ps.store.id, ps.date.year, ps.date.quarter, ps.item.category, count(ps.line_item), sum(ps.ext_sales_price) where ps.store.id is null limit 5;`

  ```text

  ```

### `file-not-found`

- `trilogy run query82.preql`

  ```text

  ```
