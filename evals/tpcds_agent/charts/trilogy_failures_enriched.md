# Trilogy failure analysis — 20260606-222624

- Run `20260606-222624` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 490 | failed: 91 (19%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 42 | 46% |
| `syntax-parse` | 23 | 25% |
| `undefined-concept` | 17 | 19% |
| `join-resolution` | 5 | 5% |
| `syntax-missing-alias` | 3 | 3% |
| `cli-misuse` | 1 | 1% |

## Detail

### `other`

- `trilogy run query02.preql`

  ```text

  ```
- `trilogy run query02.preql`

  ```text

  ```
- `trilogy file read query02.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query02.preql`

  ```text

  ```
- `trilogy file read query17.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy file read query44.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 59 column 12 (char 2181). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file read query49.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query59.preql`

  ```text

  ```
- `trilogy file read raw/physical_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file write --content import raw.catalog_sales as cat_sales;
import raw.catalog_returns as cat_returns;
import raw.item as item;

auto cat_ext_list_pr…d,
    item.product_name,
    item.color,
    item.current_price,
    cat_ext_list_price_by_item,
    cat_refund_by_item
limit 10; query64_test.preql`

  ```text
  trilogy file write: `--content` takes a SINGLE string argument. Your args list put 2 separate tokens after --content (treating it like a shell command). In a tool call, pass the entire file body as one string element after --content, with newlines embedded literally — e.g.
    {"args": ["file", "write", "query70.preql", "--content", "import raw.store_sales as store_sales;\n\nselect ..."]}
  Alternatively use `--escapes` with a single-line `\n`-escaped string.
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
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query75.preql`

  ```text

  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/physical_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run test1.preql`

  ```text

  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe catalog_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy database describe web_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy database describe store_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query78.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.web_sales:web_sales select count(web_sales.quantity) as cnt where year(web_sales.date.date)=2000 and web_sales.is_returned=0;`

  ```text

  ```

### `syntax-parse`

- `trilogy file write query02.preql --content import raw.all_sales as sales;

# Restrict to web and catalog sales only
where sales.sales_channel in ('WEB', 'CAT… / sum(next_dow_total ? sales.date.day_of_week = 6), 2) as sat_ratio
having sales.date.week_seq in ws_2001
order by sales.date.week_seq nulls first;
`

  ```text

  ```
- `trilogy file write query44.preql --content import raw.physical_sales as store_sales;

# Filter to store with surrogate id = 1
where store_sales.store.id = 1
…best_rank,
    --worst_rank,
    -- We need pairs by rank position
    store_sales.item.product_name as product_name
    --item_avg_profit
limit 10;
`

  ```text

  ```
- `trilogy file write query44.preql --content import raw.physical_sales as store_sales;

# Average net profit at store where sale's address is null
auto null_ad…roduct_name as worst_name,
    --item_avg_profit as worst_profit
merge

align
    rnk: best_rnk, worst_rnk
derive
    --
order by rnk asc
limit 100;
`

  ```text

  ```
- `trilogy file write query44.preql --content import raw.physical_sales as store_sales;

# Average net profit at store where sale's address is null
auto null_ad… --store_sales.item.product_name as worst_name
merge

align
    rnk: best_rnk, worst_rnk
derive
    rnk -> rank_pos
order by rank_pos asc
limit 100;
`

  ```text

  ```
- `trilogy file write query49.preql --content import raw.all_sales as s;

# Arm 1: Web channel
where s.sales_channel = 'WEB'
  and year(s.date.date) = 2001 and …= 10
order by channel asc nulls first, return_quantity_rank asc nulls first, return_currency_rank asc nulls first, item_id asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query59.preql --content import raw.physical_sales as ss;

# Filtered sums of unit price (sales_price) by day of week
def sun_sum -> sum(ss…sat_this / sat_next else null end as sat_ratio

order by store_name asc nulls first, store_code asc nulls first, week_seq asc nulls first
limit 100;
`

  ```text

  ```
- `trilogy file write query59.preql --content import raw.physical_sales as ss;

# Arm 1: 'this year' = 2001
where ss.store.date.year = 2001
select
    --ss.stor…sat_this / sat_next else null end as sat_ratio

order by store_name asc nulls first, store_code asc nulls first, week_seq asc nulls first
limit 100;
`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.physical_sales as store_sales;
import raw.catalog_returns as cat_returns;

merge cat_returns.sales.item…,
    coalesce(cnt_2, cnt) as cnt_2
having cnt_2 <= cnt_1
order by product_name asc, store_name asc, cnt_2 asc, sum_wc_1 asc, sum_wc_2 asc
limit 100;`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.physical_sales as store_sales;
import raw.catalog_returns as cat_returns;

merge cat_returns.sales.item…,
    coalesce(cnt_2, cnt) as cnt_2
having cnt_2 <= cnt_1
order by product_name asc, store_name asc, cnt_2 asc, sum_wc_1 asc, sum_wc_2 asc
limit 100;`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.physical_sales as store_sales;
import raw.catalog_returns as cat_returns;

merge cat_returns.sales.item…2000.item_id as item_id_2000
order by yr1999.product_name asc, yr1999.store_name asc, yr2000.cnt asc, yr1999.sum_wc asc, yr2000.sum_wc asc
limit 100;`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.physical_sales as store_sales;
import raw.catalog_returns as cat_returns;

merge cat_returns.sales.item…  sum(store_sales.ext_list_price) as sum_lp_1999,
    sum(store_sales.coupon_amt) as sum_ca_1999
order by product_name asc, store_name asc
limit 100;`

  ```text

  ```
- `trilogy file write query75.preql --content import raw.all_sales as s;

# Filter to Books category items only
where s.item.category = 'Books'

# Deduplicate p… yr2002.total_qty - yr2001.total_qty as qty_diff,
    yr2002.total_amt - yr2001.total_amt as amt_diff,
order by qty_diff asc, amt_diff asc
limit 100;`

  ```text

  ```
- `trilogy file write query76.preql --content import raw.physical_sales as store_sales;
import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_s…al_ext_price

order by channel asc nulls first, missing_ref asc nulls first, yr asc nulls first, qtr asc nulls first, cat asc nulls first

limit 100;`

  ```text

  ```
- `trilogy file write query76.preql --content import raw.physical_sales as store_sales;
import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_s…al_ext_price

order by channel asc nulls first, missing_ref asc nulls first, yr asc nulls first, qtr asc nulls first, cat asc nulls first

limit 100;`

  ```text

  ```
- `trilogy file write query76.preql --content import raw.physical_sales as store_sales;
import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_s…al_ext_price

order by channel asc nulls first, missing_ref asc nulls first, yr asc nulls first, qtr asc nulls first, cat asc nulls first

limit 100;`

  ```text

  ```
- `trilogy file write query76.preql --content import raw.physical_sales as store_sales;
import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_s…al_ext_price

order by channel asc nulls first, missing_ref asc nulls first, yr asc nulls first, qtr asc nulls first, cat asc nulls first

limit 100;`

  ```text

  ```
- `trilogy file write query76.preql --content import raw.physical_sales as store_sales;
import raw.web_sales as web_sales;

where store_sales.store.id is null
s…
derive
    coalesce(lc, 0) as line_count

order by channel asc nulls first, yr asc nulls first, qtr asc nulls first, cat asc nulls first

limit 100;`

  ```text

  ```
- `trilogy file write query78.preql --content import raw.physical_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_s… by year asc, item asc, customer asc, store_qty desc, store_wsc desc, store_sp desc, other_qty asc, other_wsc asc, other_sp asc, ratio asc
limit 100;`

  ```text

  ```
- `trilogy file write query78.preql --content import raw.physical_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_s…ar asc, sale_item asc, sale_cust asc, store_qty desc, store_wsc desc, store_sp desc, other_qty asc, other_wsc asc, other_sp asc, ratio asc
limit 100;`

  ```text

  ```
- `trilogy file write query78.preql --content import raw.physical_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_s… > 0
order by yr asc, it asc, cu asc, store_qty desc, store_wsc desc, store_sp desc, other_qty asc, other_wsc asc, other_sp asc, ratio asc
limit 100;`

  ```text

  ```
- `trilogy file write query78.preql --content import raw.physical_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_s… > 0
order by yr asc, it asc, cu asc, store_qty desc, store_wsc desc, store_sp desc, other_qty asc, other_wsc asc, other_sp asc, ratio asc
limit 100;`

  ```text

  ```
- `trilogy run --import raw.web_sales:web_sales select web_sales.is_returned, count(web_sales.quantity) as cnt where year(web_sales.date.date)=2000 group by web_sales.is_returned;`

  ```text

  ```
- `trilogy run --import raw.physical_sales:ps select not ps.is_returned, count(ps.quantity) as cnt where year(ps.date.date)=2000;`

  ```text

  ```

### `undefined-concept`

- `trilogy run query06.preql`

  ```text

  ```
- `trilogy run query06.preql`

  ```text

  ```
- `trilogy run query50.preql`

  ```text

  ```
- `trilogy run query64.preql`

  ```text

  ```
- `trilogy run query75.preql`

  ```text

  ```
- `trilogy run query76.preql`

  ```text

  ```
- `trilogy run query76.preql`

  ```text

  ```
- `trilogy run query76.preql`

  ```text

  ```
- `trilogy run query76.preql`

  ```text

  ```
- `trilogy run query76.preql`

  ```text

  ```
- `trilogy run query76.preql`

  ```text

  ```
- `trilogy run query76.preql`

  ```text

  ```
- `trilogy run query76.preql`

  ```text

  ```
- `trilogy run query76.preql`

  ```text

  ```
- `trilogy run test2.preql`

  ```text

  ```
- `trilogy run query76.preql`

  ```text

  ```
- `trilogy run test_cat_addr.preql`

  ```text

  ```

### `join-resolution`

- `trilogy run query64_test.preql`

  ```text

  ```
- `trilogy run query64_test.preql`

  ```text

  ```
- `trilogy run query64_test.preql`

  ```text

  ```
- `trilogy run test_simple.preql`

  ```text

  ```
- `trilogy run test_simple.preql`

  ```text

  ```

### `syntax-missing-alias`

- `trilogy run --import raw.date:date select distinct date.year order by date.year;`

  ```text

  ```
- `trilogy run --import raw.date:date select date.year, count(date.id) order by date.year;`

  ```text

  ```
- `trilogy run --import raw.catalog_store_returns:csr select distinct csr.store_sale_date.quarter_name as q order by q limit 20;`

  ```text

  ```

### `cli-misuse`

- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
