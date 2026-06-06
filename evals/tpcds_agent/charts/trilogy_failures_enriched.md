# Trilogy failure analysis — 20260606-200808

- Run `20260606-200808` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 443 | failed: 66 (15%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `undefined-concept` | 26 | 39% |
| `syntax-parse` | 19 | 29% |
| `other` | 13 | 20% |
| `syntax-missing-alias` | 3 | 5% |
| `join-resolution` | 3 | 5% |
| `type-error` | 1 | 2% |
| `cli-misuse` | 1 | 2% |

## Detail

### `undefined-concept`

- `trilogy run query06.preql`

  ```text

  ```
- `trilogy run query06.preql`

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
- `trilogy run query59.preql`

  ```text

  ```
- `trilogy run query75.preql`

  ```text

  ```
- `trilogy explore query75.preql`

  ```text

  ```
- `trilogy explore query75.preql --regex deduped`

  ```text

  ```
- `trilogy run query75.preql`

  ```text

  ```
- `trilogy run query75.preql`

  ```text

  ```
- `trilogy explore query75.preql`

  ```text

  ```
- `trilogy run query75.preql`

  ```text

  ```
- `trilogy explore query75.preql --regex deduped`

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
- `trilogy run query75_test3.preql`

  ```text

  ```
- `trilogy run query76.preql`

  ```text

  ```

### `syntax-parse`

- `trilogy file write query02.preql -e -c import raw.all_sales as sales;

# Filter to WEB and CATALOG channels only
where sales.sales_channel in ('WEB', 'CATALO…al / (lead(saturday_total, 53) over (order by sales.date.week_seq asc)), 2) as saturday_ratio
order by sales.date.week_seq asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query44.preql --content import raw.physical_sales as sales;

# Average net profit per item at store with surrogate id = 1
auto item_avg_pr…order by item_avg_profit desc) as worst_rank,
    sales.item.product_name
having best_rank <= 10 or worst_rank <= 10
order by best_rank asc
limit 20;`

  ```text

  ```
- `trilogy file write query59.preql`

  ```text

  ```
- `trilogy file write query59.preql`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.physical_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

# Connect cata…cost,
  sum(ss.ext_list_price) as total_list_price,
  sum(ss.coupon_amt) as total_coupon_amt
order by ss.item.product_name, ss.store.name
limit 100;
`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.physical_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.phys… coalesce(ca_2000, 0) as ca_2000,
  coalesce(cnt_2000, 0) as cnt_2000
order by product_name, store_name_final, cnt_2000, ws_1999, ws_2000
limit 100;
`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.physical_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.phys…ce(ca_1999, 0),
  coalesce(ws_2000, 0),
  coalesce(lp_2000, 0),
  coalesce(ca_2000, 0),
  coalesce(cnt_2000, 0)
order by 1, 2, 19, 12, 16
limit 100;
`

  ```text

  ```
- `trilogy file write query75.preql --content import raw.all_sales as sales;

# Only Books category
where sales.item.category = 'Books'
and sales.date.year in (…mt_2001 as amt_diff
where yr2001.qty_2001 > 0
and coalesce(yr2002.qty_2002, 0) / yr2001.qty_2001 < 0.9
order by qty_diff asc, amt_diff asc
limit 100;`

  ```text

  ```
- `trilogy file write query75.preql --content import raw.all_sales as sales;

# Per-line net values (deduplicated by the rowset's implicit distinct)
rowset per_…001 else 1.0 end -> ratio
where qty_2001 > 0 and (qty_2002 / qty_2001) < 0.9
order by (qty_2002 - qty_2001) asc, (amt_2002 - amt_2001) asc
limit 100;`

  ```text

  ```
- `trilogy file write query75.preql --content import raw.all_sales as sales;

# Per-line net values (deduplicated by the rowset's implicit distinct)
rowset per_…001 else 1.0 end -> ratio
where qty_2001 > 0 and (qty_2002 / qty_2001) < 0.9
order by (qty_2002 - qty_2001) asc, (amt_2002 - amt_2001) asc
limit 100;`

  ```text

  ```
- `trilogy file write query75.preql --content import raw.all_sales as sales;

# Per-line net values (deduplicated by the rowset's implicit distinct)
rowset per_…q, 0) -> qty_diff,
    coalesce(ca, 0) - coalesce(pa, 0) -> amt_diff
having pq > 0 and (cq / pq) < 0.9
order by qty_diff asc, amt_diff asc
limit 100;`

  ```text

  ```
- `trilogy file write query76.preql --content import physical_sales as store;
import web_sales as web;
import catalog_sales as catalog;

# Store channel: null s…asc nulls first, missing_ref_label asc nulls first, sale_year asc nulls first, sale_quarter asc nulls first, item_category asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query76.preql --content import physical_sales as store;
import web_sales as web;
import catalog_sales as catalog;

# Store channel: null s…asc nulls first, missing_ref_label asc nulls first, sale_year asc nulls first, sale_quarter asc nulls first, item_category asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query76.preql --content import physical_sales as store;
import web_sales as web;
import catalog_sales as catalog;

# Store channel: null s…asc nulls first, missing_ref_label asc nulls first, sale_year asc nulls first, sale_quarter asc nulls first, item_category asc nulls first
limit 100;`

  ```text

  ```
- `trilogy run --import raw.all_sales:s select s.return_date.date as rd, count(s.channel_dim_id) as cnt, sum(coalesce(s.return_amount,0)) as ret_amt where s.dat…tween '2000-08-23'::date and '2000-09-22'::date and s.channel_dim_id is not null and s.return_date.date is not null group by rd order by rd limit 10;`

  ```text

  ```
- `trilogy file write query78.preql --content import raw.all_sales as s;

# Store sales never returned in year 2000, with identified customer
where s.date.year …wholesale_cost desc,
    store_sales_price desc,
    other_qty asc,
    other_wholesale_cost asc,
    other_sales_price asc,
    ratio asc
limit 100;`

  ```text

  ```
- `trilogy file write query78.preql --content import raw.all_sales as s;

# Store sales never returned in year 2000, with identified customer
where s.date.year …wholesale_cost desc,
    store_sales_price desc,
    other_qty asc,
    other_wholesale_cost asc,
    other_sales_price asc,
    ratio asc
limit 100;`

  ```text

  ```
- `trilogy file write query78.preql --content import raw.all_sales as s;

# Store sales never returned in year 2000, with identified customer
where s.date.year …sale_cost,
    coalesce(cat_sales_price, 0) as cat_sales_price
having
    coalesce(web_qty, 0) + coalesce(cat_qty, 0) > 0
order by key asc
limit 100;`

  ```text

  ```
- `trilogy file write query78.preql --content import raw.all_sales as s;

# Store sales never returned in year 2000, with identified customer
where s.date.year …sale_cost,
    coalesce(cat_sales_price, 0) -> cat_sales_price
having
    coalesce(web_qty, 0) + coalesce(cat_qty, 0) > 0
order by key asc
limit 100;`

  ```text

  ```

### `other`

- `trilogy run query02.preql`

  ```text

  ```
- `trilogy run query02.preql`

  ```text

  ```
- `trilogy run query06.preql`

  ```text

  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy run query44.preql`

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
- `trilogy run query75.preql`

  ```text

  ```
- `trilogy explore raw/web_sales.preql --show imports`

  ```text
  name '_import_entry' is not defined
  ```
- `trilogy explore raw/catalog_sales.preql --show imports`

  ```text
  name '_import_entry' is not defined
  ```
- `trilogy run query76.preql`

  ```text

  ```
- `trilogy run query76.preql`

  ```text

  ```

### `syntax-missing-alias`

- `trilogy file write query02.preql -e -c import raw.all_sales as sales;

# Filtered sums for each day of the week - across ALL years per week_seq
# day_of_week…les.date.week_seq asc)), 2) as saturday_ratio
having
    sales.date.week_seq in weeks_in_2001
order by sales.date.week_seq asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query77.preql --content import raw.all_sales as s;

# Channel labels
auto channel_label <-
  case
    when s.sales_channel = 'STORE' then … channel_label, s.channel_dim_id

order by channel_label asc nulls first,
         outlet_id asc nulls first,
         total_returns desc
limit 100;
`

  ```text

  ```
- `trilogy run --import raw.all_sales:s select s.sales_channel, count(s.channel_dim_id) where s.date.date between '2000-08-23'::date and '2000-09-22'::date and s.channel_dim_id is not null group by s.sales_channel order by s.sales_channel limit 10;`

  ```text

  ```

### `join-resolution`

- `trilogy run query64.preql`

  ```text

  ```
- `trilogy run query64.preql`

  ```text

  ```
- `trilogy run query64.preql`

  ```text

  ```

### `type-error`

- `trilogy run query64.preql`

  ```text

  ```

### `cli-misuse`

- `trilogy run query77.preql --format json`

  ```text
  '--format' is not a valid dialect. The dialect argument comes after the input file and any options.
    Try: trilogy run <input> [<dialect>]
  ```
