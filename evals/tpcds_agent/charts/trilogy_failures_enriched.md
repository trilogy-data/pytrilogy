# Trilogy failure analysis — 20260613-041233

- Run `20260613-041229_enriched` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 1221 | failed: 191 (16%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 96 | 50% |
| `syntax-parse` | 50 | 26% |
| `undefined-concept` | 23 | 12% |
| `syntax-missing-alias` | 8 | 4% |
| `cli-misuse` | 6 | 3% |
| `join-resolution` | 6 | 3% |
| `file-not-found` | 2 | 1% |

## Detail

### `other`

- `trilogy run query01.preql`

  ```text

  ```
- `trilogy run query02.preql`

  ```text

  ```
- `trilogy run query02.preql`

  ```text

  ```
- `trilogy run query02.preql`

  ```text

  ```
- `trilogy run query02.preql`

  ```text

  ```
- `trilogy run query02.preql`

  ```text

  ```
- `trilogy run query02.preql`

  ```text

  ```
- `trilogy run query02.preql`

  ```text

  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy agent-info syntax example rollup`

  ```text

  ```
- `trilogy run query05.preql`

  ```text

  ```
- `trilogy run query05.preql`

  ```text

  ```
- `trilogy run query05.preql`

  ```text

  ```
- `trilogy run query05.preql`

  ```text

  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy agent-info syntax example rollup`

  ```text

  ```
- `trilogy run query14.preql`

  ```text

  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query15.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query18.preql`

  ```text

  ```
- `trilogy run query18.preql`

  ```text

  ```
- `trilogy file read query23.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query24.preql`

  ```text

  ```
- `trilogy run query30.preql`

  ```text

  ```
- `trilogy run query31.preql`

  ```text

  ```
- `trilogy run query31.preql`

  ```text

  ```
- `trilogy run query31.preql`

  ```text

  ```
- `trilogy file read query31.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 59 column 12 (char 2723). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query41.preql`

  ```text

  ```
- `trilogy file read raw/physical_sales.preql`

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
- `trilogy run --import raw/physical_sales:sales select count(sales.line_item) as passing_items where sales.store.id = 1 having avg(sales.net_profit) > 0.9 * avg(sales.net_profit ? sales.sale_address.id is null) by * limit 10;`

  ```text

  ```
- `trilogy run --import raw/physical_sales:sales auto item_avg <- avg(sales.net_profit ? sales.store.id = 1) by sales.item.product_name;
auto thresh <- 0.9 * av…uct_name, rank(sales.item.product_name) over (order by item_avg asc) as rnk where sales.store.id = 1 having item_avg > thresh and rnk <= 10 limit 10;`

  ```text

  ```
- `trilogy run --import raw/physical_sales:sales auto item_avg <- avg(sales.net_profit ? sales.store.id = 1) by sales.item.product_name;
auto thresh <- 0.9 * av….id = 1 and sales.sale_address.id is null) by *;
select sales.item.product_name, --thresh where sales.store.id = 1 having item_avg > thresh limit 10;`

  ```text

  ```
- `trilogy run --import raw/physical_sales:sales auto item_avg_profit <- avg(sales.net_profit ? sales.store.id = 1) by sales.item.product_name;
auto threshold <…_name, rank(sales.item.product_name) over (order by item_avg_profit asc) as rnk where sales.store.id = 1 having item_avg_profit > threshold limit 10;`

  ```text

  ```
- `trilogy run --import raw/physical_sales:sales auto item_avg <- avg(sales.net_profit ? sales.store.id = 1) by sales.item.product_name;
auto thresh <- 0.9 * av…name, row_number(sales.item.product_name) over (order by item_avg asc) as rn where sales.store.id = 1 having item_avg > thresh and rn <= 10 limit 20;`

  ```text

  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy run --import raw/physical_sales:sales auto item_avg <- avg(sales.net_profit ? sales.store.id = 1) by sales.item.product_name;
auto thresh <- 0.9 * av…_address.id is null) by *;
select sales.item.product_name, item_avg where sales.store.id = 1 having item_avg > thresh order by item_avg asc limit 10;`

  ```text

  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy file read query47.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query51.preql`

  ```text

  ```
- `trilogy run query51.preql`

  ```text

  ```
- `trilogy run query51.preql`

  ```text

  ```
- `trilogy run query51.preql`

  ```text

  ```
- `trilogy run query51.preql`

  ```text

  ```
- `trilogy run query51.preql`

  ```text

  ```
- `trilogy run query51.preql`

  ```text

  ```
- `trilogy run query51.preql`

  ```text

  ```
- `trilogy run query51.preql`

  ```text

  ```
- `trilogy run query51.preql`

  ```text

  ```
- `trilogy run query51.preql`

  ```text

  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 77 column 12 (char 2606). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file read query56.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw/date:date select date.id, date.date, date.week_seq where date.date = '2000-01-03';`

  ```text

  ```
- `trilogy file read raw/physical_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query58.preql`

  ```text

  ```
- `trilogy file read query59.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query64.preql`

  ```text

  ```
- `trilogy run query65.preql`

  ```text

  ```
- `trilogy run query65.preql`

  ```text

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
- `trilogy file read query70.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/physical_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query77.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query78.preql`

  ```text

  ```
- `trilogy run query78.preql`

  ```text

  ```
- `trilogy run query78.preql`

  ```text

  ```
- `trilogy run query78.preql`

  ```text

  ```
- `trilogy run query78.preql`

  ```text

  ```
- `trilogy run query78.preql`

  ```text

  ```
- `trilogy run query78.preql`

  ```text

  ```
- `trilogy file read query78.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query80.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query81.preql`

  ```text

  ```
- `trilogy run query83.preql`

  ```text

  ```
- `trilogy file read trilogy.toml`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query94_check.preql`

  ```text

  ```
- `trilogy run query94_check.preql`

  ```text

  ```
- `trilogy run query94_check.preql`

  ```text

  ```
- `trilogy file read query94.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query95.preql duckdb`

  ```text

  ```
- `trilogy run query95.preql duckdb`

  ```text

  ```
- `trilogy file read query96.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy run --import raw.all_sales:s select s.date.year, sum(1) as cnt group by s.date.year order by s.date.year;`

  ```text

  ```
- `trilogy file write query02.preql --content import raw.all_sales as s;

# Define a reusable macro for day-of-week filtered sales sums
def dow_sales(d) -> sum(…nd(all_weekly.sat_sales / nxt.sat_sales, 2) as sat_ratio
left join all_weekly.week_seq = nxt.week_seq + 53
order by all_weekly.week_seq nulls first;
`

  ```text

  ```
- `trilogy file write query02.preql --content import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Combined web + catalog sales as a union
with combin… 2) as sat_ratio
having ratios.week_seq in (
    where cs.sold_date.year = 2001 select cs.sold_date.week_seq
)
order by ratios.week_seq nulls first;
`

  ```text

  ```
- `trilogy file write query02.preql --content import raw.all_sales as s;

# Macro: sum of ext_sales_price for a specific day_of_week
def dow_sales(d) -> sum(s.e…    --s.date.year as yr,
    --s.date.week_seq in (select s.date.week_seq where s.date.year = 2001) as in_2001
order by s.date.week_seq nulls first;
`

  ```text

  ```
- `trilogy run --import raw.all_sales:all_sales with sales_data as (where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date and all_sales.ch…ofit, sum(all_sales.return_net_loss) as return_net_loss limit 5;) select sales_data.channel, sales_data.entity_id, sales_data.entity_text_id limit 5;`

  ```text

  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.channel, all_sales.return_channel_dim_id, all_sales.return_channel_dim_text_id, count(all_sales…sales.return_amount > 0 group by all_sales.channel, all_sales.return_channel_dim_id, all_sales.return_channel_dim_text_id order by cnt desc limit 10;`

  ```text

  ```
- `trilogy file write query05.preql -e -c import raw.all_sales as all_sales;

# Combine sales and returns using union, then aggregate with rollup for subtotals
… and entity_id is null) or (channel_label is null and entity_id is null)
order by channel_label asc nulls first, entity_id asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query05.preql -e -c import raw.all_sales as all_sales;

# Combine sales and returns using union, then aggregate with rollup for subtotals
…t null and entity_id = '') or (channel_label is null and entity_id = '')
order by channel_label asc nulls first, entity_id asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query05.preql -e -c import raw.all_sales as all_sales;

# Combine sales and returns using union, then aggregate with rollup for subtotals
…t null and entity_id = '') or (channel_label is null and entity_id = '')
order by channel_label asc nulls first, entity_id asc nulls first
limit 100;`

  ```text

  ```
- `trilogy run --import raw.all_sales:all_sales --import raw.store:store --import raw.catalog_page:catalog_page --import raw.web_site:web_site def by_geo(x) -> sum(x) by rollup store.state, store.city; select @by_geo(store.employees) as emp group by ...`

  ```text

  ```
- `trilogy file write query05.preql -e -c import raw.all_sales as all_sales;

-- Step 1: Define a rowset that aggregates with rollup from the union of sales and…gg_data.gross_sales,
    agg_data.total_returns,
    agg_data.net_profit
order by channel_label asc nulls first, entity_id asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query05.preql -e -c import raw.all_sales as all_sales;

# Step 1: Define a rowset that aggregates with rollup from the union

rowset agg_d…gg_data.gross_sales,
    agg_data.total_returns,
    agg_data.net_profit
order by channel_label asc nulls first, entity_id asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query05.preql -e -c import raw.all_sales as all_sales;

# Aggregate and rollup from the union of sales and returns

rowset agg_data <- sel… and entity_id is null) or (channel_label is null and entity_id is null)
order by channel_label asc nulls first, entity_id asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query05.preql -e -c import raw.all_sales as all_sales;

with combined as union(
    (
        where all_sales.date.date between '2000-08-2… net_profit
having gross_sales > 0 or total_returns > 0
order by combined.channel asc nulls first, combined.entity_text_id asc nulls first
limit 100;`

  ```text

  ```
- `trilogy run --import raw.all_sales:all_sales with combined as union( (where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date and all_sal…nl) by rollup combined.c, combined.tid, 0) as np having gs > 0 or tr > 0 order by combined.c asc nulls first, combined.tid asc nulls first limit 100;`

  ```text

  ```
- `trilogy file write query23.preql --content import raw.physical_sales as store;
import raw.all_sales as sales;

# Step 1: Dedup store sales to (item, date) pa…q_item_desc_prefix
  and sales.billing_customer.id in best_customer_id
order by last_name asc, first_name asc, total_sales asc nulls first
limit 100;`

  ```text

  ```
- `trilogy run --import raw.physical_sales:store select substring(store.item.desc, 1, 30) as prefix, count_distinct(store.item.id, store.date.id) as pair_count where store.date.year between 2000 and 2003 and store.item.desc is not null group by prefix order by pair_count desc limit 20;`

  ```text

  ```
- `trilogy file write query24.preql --content import raw.physical_sales as sales;
import raw.physical_returns as returns;

# Join sales to returns on ticket_num…eturns.item.id
having peach_second_stage_sum > 0.05 * avg_first_stage
order by sales.customer.last_name, sales.customer.first_name, sales.store.name;`

  ```text

  ```
- `trilogy run --import raw/physical_sales:physical_sales select physical_sales.store.id, physical_sales.sale_address.id, count(physical_sales.line_item) as cnt group by 1,2 limit 10;`

  ```text

  ```
- `trilogy run --import raw/physical_sales:physical_sales select physical_sales.item.product_name, avg(physical_sales.net_profit) as avg_profit where physical_sales.store.id = 1 group by 1 limit 10;`

  ```text

  ```
- `trilogy run --import raw/physical_sales:sales select count(*) as passing_items where sales.store.id = 1 having avg(sales.net_profit) > 0.9 * avg(sales.net_profit ? sales.sale_address.id is null) by * limit 10;`

  ```text

  ```
- `trilogy run --import raw/physical_sales:sales select sales.date.year, sales.date.month_of_year, sum(sales.sales_price) as monthly_total where sales.date.year = 1999 group by 1, 2 limit 5;`

  ```text

  ```
- `trilogy file write query49.preql --content import raw.all_sales as s;

# December 2001 sales that are profitable, positive-revenue, positive-unit
# AND tied …currency_rank
having
  return_rank <= 10 or currency_rank <= 10
order by
  channel asc,
  return_rank asc,
  currency_rank asc,
  item asc
limit 100;`

  ```text

  ```
- `trilogy file write query51.preql --content # Check full join count properly

import raw.web_sales as ws;
import raw.physical_sales as ss;

with w_daily as
wh…ss.sales_price) as s_price
;

select count(*) as combined_rows
full join s_daily.s_item = w_daily.w_item
full join s_daily.s_date = w_daily.w_date
;
`

  ```text

  ```
- `trilogy file write query54.preql --content import raw.catalog_sales as cs;
import raw.web_sales as ws;
import raw.physical_sales as ps;

# Customers who boug…ustomer.id) as customer_count,
    round(store_sales_amt / 50) * 50 as segment_times_50
order by segment, customer_count, segment_times_50
limit 100;`

  ```text

  ```
- `trilogy file write query56.preql --content import raw.all_sales as sales;
import raw.item as item;

# Items ever sold with color 'slate', 'blanched', or 'bur…al
left join sales.item.id = qualifying_items.item_code
order by
    total asc nulls first,
    qualifying_items.item_code asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query58.preql --content import raw/physical_sales as store;
import raw/catalog_sales as catalog;
import raw/web_sales as web;
import raw/i…_totals.web_total and 1.1 * web_totals.web_total
order by store_totals.item_code asc nulls first, store_totals.store_total asc nulls first
limit 100;`

  ```text

  ```
- `trilogy run --import raw/physical_sales:ps select ps.date.year, ps.date.week_seq, min(ps.date.date) as date_min, max(ps.date.date) as date_max group by ps.date.year, ps.date.week_seq order by ps.date.year, ps.date.week_seq limit 10;`

  ```text

  ```
- `trilogy file write query59.preql --content import raw.physical_sales as ps;

# Define the sum of unit prices per day-of-week for a given store+week+day
# For…
order by this_year.store_name asc nulls first,
         this_year.store_code asc nulls first,
         this_year.week_seq asc nulls first
limit 100;`

  ```text

  ```
- `trilogy run --import raw/date:d select d.year, d.week_seq, min(d.date) as date_min, max(d.date) as date_max where d.week_seq = 5322 or d.week_seq = 5323 group by d.year, d.week_seq order by d.week_seq, d.year limit 10;`

  ```text

  ```
- `trilogy run --import raw/physical_sales:ps select ps.store.name, ps.store.text_id, ps.store.id, ps.date.week_seq, count(ps.line_item) as cnt where ps.store.id is null and ps.date.year = 2001 group by ps.store.name, ps.store.text_id, ps.store.id, ps.date.week_seq order by ps.date.week_seq limit 10;`

  ```text

  ```
- `trilogy run --import raw/physical_sales:ps select ps.store.id, ps.date.week_seq, sum(ps.sales_price) as total where ps.store.id is null and ps.date.year = 2001 group by ps.store.id, ps.date.week_seq order by ps.date.week_seq limit 10;`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.physical_sales as ss;
import raw.catalog_returns as cr;
import raw.physical_returns as sr;

# Qualified…uct_name,
    store_sales_agg.y1999.store_name,
    store_sales_agg.y2000.cnt,
    store_sales_agg.y1999.ws_cost,
    store_sales_agg.y2000.ws_cost
;`

  ```text

  ```
- `trilogy file write query67.preql --content import raw.physical_sales as ss;

# Filter to year 2000
where ss.date.year = 2000

# Compute the sum of (per-line …t,
    ss.date.month_of_year asc nulls first,
    ss.store.text_id asc nulls first,
    sales_sum asc nulls first,
    rnk asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query70.preql --content import raw.physical_sales as sales;

# Total net profit by state for year 2000, to find top 5 states
auto state_pr…te,
    sales.store.county,
    total_profit,
    level,
    rnk
order by level desc nulls last, sales.store.state asc nulls last, rnk asc
limit 100;`

  ```text

  ```
- `trilogy run --import raw.store:s select s.state, s.county, count(s.id) as cnt where s.state is null group by s.state, s.county order by cnt desc limit 10;`

  ```text

  ```
- `trilogy run --import raw.physical_sales:sales select sales.store.state, sum(sales.net_profit) as profit where sales.date.year = 2000 group by sales.store.state order by profit desc limit 10;`

  ```text

  ```
- `trilogy file write query75.preql --content import raw.all_sales as s;

# Per-line net values, deduplicated by (item.id, order_id)
with lines as
where item.ca…urer_id
where cur.yr = 2002
    and prev.yr = 2001
    and cur.total_qty / prev.total_qty::float < 0.9
order by qty_diff asc, amt_diff asc
limit 100;`

  ```text

  ```
- `trilogy file write query77.preql --content import raw.all_sales as all_sales;

# Channel labels
auto channel_label <- case
    when all_sales.channel = 'STOR…e) as total_ret_loss
order by channel_label asc nulls first,
         all_sales.outlet_id asc nulls first,
         total_returns_raw desc
limit 100;`

  ```text

  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.channel, all_sales.channel_dim_text_id, all_sales.outlet_id, all_sales.date.date, all_sales.net_profit, all_sales.return_amount from all_sales limit 5;`

  ```text

  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.channel, all_sales.channel_dim_text_id, all_sales.date.date, all_sales.ext_sales_price, all_sal…s.date.date between '2000-08-23'::date and '2000-09-22'::date and all_sales.item.current_price > 50 and all_sales.promotion.channel_tv = 'N' limit 5;`

  ```text

  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.channel, count(all_sales.row_one) as cnt from all_sales group by all_sales.channel;`

  ```text

  ```
- `trilogy file write query81.preql -e -c import raw.catalog_returns as cr;

# Total return amount including tax per (returning customer, returning-address stat…y asc,
    cr.billing_customer.address.gmt_offset asc,
    cr.billing_customer.address.location_type asc,
    total_return_amt_inc_tax asc
limit 100;`

  ```text

  ```
- `trilogy file write query83.preql --content import raw/all_sales as all;

# Return records in the calendar weeks containing 2000-06-30, 2000-09-27, 2000-11-17… 'CATALOG') > 0
    and count(all.row_one ? all.channel = 'WEB') > 0

order by
    all.item.text_id nulls first,
    store_qty nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query87.preql --content import raw.all_sales as all_sales;

# Presence flags for each (last_name, first_name, date) tuple per channel in y…e but not in catalog and not in web)
select
    sum(case when in_store > 0 and in_catalog = 0 and in_web = 0 then 1 else 0 end) as store_only_count
;`

  ```text

  ```
- `trilogy file write query93.preql --content import raw/physical_sales as sales;

select
    sales.customer.id as customer_id,
    sum(
        case
          …   ) as adjusted_amount
where sales.return_reason.desc = 'reason 28'
order by adjusted_amount asc nulls first, customer_id asc nulls first
limit 100;`

  ```text

  ```
- `trilogy run --import raw.physical_sales:sales select sales.customer.id, sum((sales.quantity - coalesce(sales.return_quantity, 0)) * sales.sales_price) as adj where sales.return_reason.desc = 'reason 28' and sales.quantity is not null and sales.sales_price is not null group by sales.customer.id limit 10;`

  ```text

  ```
- `trilogy file write query95.preql --content import raw.web_sales as ws;

# Step 1: aggregate web sales lines to the order level
# Filter base row-level condit…sum(order_agg.ext_ship_cost_sum) as total_extended_shipping_cost,
  sum(order_agg.net_profit_sum) as total_net_profit
order by order_count
limit 100;`

  ```text

  ```
- `trilogy file write query99.preql --content import raw.catalog_sales as cs;

def bucket_30_or_fewer -> count(cs.line_item ? cs.days_to_ship <= 30);
def bucket…cket_over_120 as cnt_over_120
order by
    warehouse_name_20 nulls first,
    ship_mode_type nulls first,
    call_center_name nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query99.preql --content import raw.catalog_sales as cs;

def bucket_30_or_fewer() -> count(cs.line_item ? cs.days_to_ship <= 30);
def buck…et_over_120() as cnt_over_120
order by
    warehouse_name_20 nulls first,
    ship_mode_type nulls first,
    call_center_name nulls first
limit 100;`

  ```text

  ```

### `undefined-concept`

- `trilogy run query02.preql`

  ```text

  ```
- `trilogy explore query02.preql`

  ```text

  ```
- `trilogy run query02.preql`

  ```text

  ```
- `trilogy run query15.preql`

  ```text

  ```
- `trilogy run query18.preql`

  ```text

  ```
- `trilogy run query27.preql`

  ```text

  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy run query45.preql`

  ```text

  ```
- `trilogy run query64.preql`

  ```text

  ```
- `trilogy run query64.preql`

  ```text

  ```
- `trilogy run query67.preql`

  ```text

  ```
- `trilogy run query67.preql`

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
- `trilogy run query75.preql`

  ```text

  ```
- `trilogy run query94.preql`

  ```text

  ```
- `trilogy run query94.preql`

  ```text

  ```
- `trilogy run query95.preql duckdb`

  ```text

  ```
- `trilogy explore query95.preql`

  ```text

  ```
- `trilogy run query97.preql`

  ```text

  ```

### `syntax-missing-alias`

- `trilogy file write query41.preql --content import raw.item as item;

# Predefine the 8 attribute profiles as filters — items matching any profile
auto profil…acturer_id between 1 and 500
  and item.id in shared_manufact_item
select distinct item.product_name as product_name
order by product_name
limit 100;`

  ```text

  ```
- `trilogy file write query49.preql --content import raw.all_sales as s;

# December 2001 sales that are profitable, have positive revenue and unit sales,
# AND…ency_rank <= 10
order by
  channel asc nulls first,
  return_rank asc nulls first,
  currency_rank asc nulls first,
  item asc nulls first
limit 100;`

  ```text

  ```
- `trilogy run --import raw.date:date select distinct date.month_of_year, date.year, date.month_seq where date.year = 1998 and date.month_of_year between 11 and 12 order by date.month_seq; duckdb`

  ```text

  ```
- `trilogy run --import raw/physical_sales:ps select ps.date.year, ps.date.week_seq, min(ps.date.date), max(ps.date.date) where ps.date.year in (2001, 2002) group by ps.date.year, ps.date.week_seq order by ps.date.year, ps.date.week_seq limit 10;`

  ```text

  ```
- `trilogy file write query77.preql --content import raw.all_sales as all_sales;

# Channel labels
auto channel_label <- case
    when all_sales.channel = 'STOR…utlet_id as total_profit
order by channel_label asc nulls first,
         all_sales.outlet_id asc nulls first,
         total_returns desc
limit 100;`

  ```text

  ```
- `trilogy file write query80.preql -e -c import raw.all_sales as all_sales;

# Label the channel
auto channel_label <- case
    when all_sales.channel = 'STORE…
    profit_measure by rollup channel_label, outlet_identifier as profit
order by channel_label nulls first, outlet_identifier nulls first
limit 100;`

  ```text

  ```
- `trilogy run --import raw/all_sales:all select all.channel, count(all.return_quantity), sum(all.return_quantity) where all.return_date.week_seq in (5244, 5257, 5264) group by all.channel;`

  ```text

  ```
- `trilogy run --import raw.address:addr select distinct addr.city where lower(addr.city) = 'edgewood' limit 10;`

  ```text

  ```

### `cli-misuse`

- `trilogy explore raw/customer_address.preql`

  ```text
  Invalid value for 'PATH': File 'raw/customer_address.preql' does not exist.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy explore raw/ship_date.preql`

  ```text
  Invalid value for 'PATH': File 'raw/ship_date.preql' does not exist.
  ```

### `join-resolution`

- `trilogy run query35.preql`

  ```text

  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy run query64.preql`

  ```text

  ```
- `trilogy run query89.preql`

  ```text

  ```
- `trilogy run query99.preql`

  ```text

  ```
- `trilogy run --debug query99.preql`

  ```text

  ```

### `file-not-found`

- `trilogy run query75.preql`

  ```text

  ```
- `trilogy explore query75.preql`

  ```text

  ```
