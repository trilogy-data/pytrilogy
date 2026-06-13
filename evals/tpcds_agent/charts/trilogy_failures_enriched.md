# Trilogy failure analysis — 20260613-150755

- Run `20260613-150755_enriched` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 1486 | failed: 211 (14%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 102 | 48% |
| `undefined-concept` | 49 | 23% |
| `syntax-parse` | 37 | 18% |
| `cli-misuse` | 11 | 5% |
| `file-not-found` | 7 | 3% |
| `syntax-missing-alias` | 3 | 1% |
| `join-resolution` | 2 | 1% |

## Detail

### `other`

- `trilogy run query02.preql`

  ```text

  ```
- `trilogy file read query02.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query03.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query04.preql`

  ```text

  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query05.preql`

  ```text

  ```
- `trilogy run query05.preql`

  ```text

  ```
- `trilogy file read query07.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text

  ```
- `trilogy file read query08.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query10.preql`

  ```text

  ```
- `trilogy file read raw/physical_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query14.preql`

  ```text

  ```
- `trilogy run query14.preql`

  ```text

  ```
- `trilogy run query14.preql`

  ```text

  ```
- `trilogy run query14.preql`

  ```text
  trilogy error: subprocess timed out after 600s.
  ```
- `trilogy run query14.preql`

  ```text

  ```
- `trilogy file read query14.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query15.preql`

  ```text

  ```
- `trilogy run query15.preql`

  ```text

  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/physical_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query23.preql`

  ```text

  ```
- `trilogy run query23.preql`

  ```text

  ```
- `trilogy run query24.preql`

  ```text

  ```
- `trilogy run query30.preql`

  ```text

  ```
- `trilogy run query30.preql`

  ```text

  ```
- `trilogy run query30.preql`

  ```text

  ```
- `trilogy run query30.preql`

  ```text

  ```
- `trilogy run query30.preql`

  ```text

  ```
- `trilogy file read query30.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query31.preql`

  ```text

  ```
- `trilogy file read query36.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query37.preql`

  ```text

  ```
- `trilogy run query37.preql`

  ```text

  ```
- `trilogy run query37.preql`

  ```text

  ```
- `trilogy run query37.preql`

  ```text

  ```
- `trilogy run query38.preql`

  ```text

  ```
- `trilogy run query39.preql`

  ```text

  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query40.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query43.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy run --import raw.physical_sales:s select s.item.product_name as item_name, avg(s.net_profit) as avg_profit, rank(s.item.product_name) over (order by …here s.store.id = 1 having avg(s.net_profit) > avg(s.net_profit ? s.store.id = 1 and s.sale_address.id is null) by * * 0.9 order by rnk asc limit 10;`

  ```text

  ```
- `trilogy explore query44.preql --regex best_numbered|worst_numbered`

  ```text
  Cannot render type <class 'trilogy.core.models.author.RowsetItem'>
  ```
- `trilogy run --import raw.physical_sales:s select s.item.product_name as item_name, avg(s.net_profit) as avg_profit, rank(s.item.product_name) over (order by …here s.store.id = 1 having avg(s.net_profit) > avg(s.net_profit ? s.store.id = 1 and s.sale_address.id is null) by * * 0.9 order by rnk asc limit 10;`

  ```text

  ```
- `trilogy file read query44.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe item`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/item.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query49.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw/physical_sales:ps --import raw/customer:c --import raw/catalog_sales:cs select ps.customer.id as cust_id, sum(ps.ext_sales_price) as… ps.store.state = ps.customer.address.state and cs.sold_date.month_seq = 1187 and cs.item.category = 'Women' and cs.item.class = 'maternity' limit 5;`

  ```text

  ```
- `trilogy file read query56.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query62.preql`

  ```text

  ```
- `trilogy file list / --recursive`

  ```text
  trilogy error: subprocess timed out after 600s.
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
- `trilogy file read query64.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
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
- `trilogy run query66.preql`

  ```text

  ```
- `trilogy file read query69.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query70.preql duckdb`

  ```text

  ```
- `trilogy file read trilogy.toml`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query72.preql`

  ```text

  ```
- `trilogy run query74.preql`

  ```text

  ```
- `trilogy file read query75.preql`

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
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query76.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

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
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 56 column 12 (char 2620). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query81.preql`

  ```text

  ```
- `trilogy run query81.preql`

  ```text

  ```
- `trilogy file read query81.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query82.preql`

  ```text

  ```
- `trilogy run query82.preql`

  ```text

  ```
- `trilogy run query83.preql`

  ```text

  ```
- `trilogy run query83.preql`

  ```text

  ```
- `trilogy file read query83.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query84.preql`

  ```text

  ```
- `trilogy run query86.preql`

  ```text

  ```
- `trilogy file read raw/physical_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query87.preql`

  ```text

  ```
- `trilogy file read raw/time.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query89.preql`

  ```text

  ```
- `trilogy file read query89.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query90.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query93.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query94.preql`

  ```text

  ```
- `trilogy file read query94.preql`

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

### `undefined-concept`

- `trilogy run query02_debug.preql`

  ```text

  ```
- `trilogy run query02_debug.preql`

  ```text

  ```
- `trilogy run query04.preql`

  ```text

  ```
- `trilogy explore query04.preql`

  ```text

  ```
- `trilogy run query04.preql`

  ```text

  ```
- `trilogy run query06.preql`

  ```text

  ```
- `trilogy run query10.preql`

  ```text

  ```
- `trilogy run query10.preql`

  ```text

  ```
- `trilogy run query11.preql`

  ```text

  ```
- `trilogy run query17.preql`

  ```text

  ```
- `trilogy run query21.preql`

  ```text

  ```
- `trilogy run query24.preql`

  ```text

  ```
- `trilogy run query33.preql`

  ```text

  ```
- `trilogy run query34.preql`

  ```text

  ```
- `trilogy run query35.preql`

  ```text

  ```
- `trilogy run query39.preql`

  ```text

  ```
- `trilogy run query39.preql`

  ```text

  ```
- `trilogy explore query39.preql`

  ```text

  ```
- `trilogy run query39.preql`

  ```text

  ```
- `trilogy run query39.preql`

  ```text

  ```
- `trilogy run query39.preql`

  ```text

  ```
- `trilogy run query39.preql`

  ```text

  ```
- `trilogy run query41.preql`

  ```text

  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy explore query44.preql --regex best_items|worst_items|best_numbered|worst_numbered`

  ```text

  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy run query47.preql`

  ```text

  ```
- `trilogy run query53.preql`

  ```text

  ```
- `trilogy run --import raw/date select date.month_seq where date.year = 1998 and date.month_of_year = 12 limit 1;`

  ```text

  ```
- `trilogy run query60.preql`

  ```text

  ```
- `trilogy run query62.preql`

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
- `trilogy explore query64.preql`

  ```text

  ```
- `trilogy explore query64.preql --regex store_agg`

  ```text

  ```
- `trilogy run query65.preql`

  ```text

  ```
- `trilogy run query68.preql`

  ```text

  ```
- `trilogy run query69.preql`

  ```text

  ```
- `trilogy run query74.preql`

  ```text

  ```
- `trilogy run query75.preql`

  ```text

  ```
- `trilogy run query78.preql`

  ```text

  ```
- `trilogy run query83_check.preql`

  ```text

  ```
- `trilogy run query86.preql`

  ```text

  ```
- `trilogy run query94.preql`

  ```text

  ```
- `trilogy run query95.preql duckdb`

  ```text

  ```
- `trilogy explore query95.preql --show concepts`

  ```text

  ```

### `syntax-parse`

- `trilogy run --import raw/all_sales:all_sales select all_sales.date.year, count(all_sales.order_id) as cnt group by 1 order by 1;`

  ```text

  ```
- `trilogy file write query02.preql --content import raw.all_sales as all_sales;

# Find week_seqs that ever occur in 2001
auto ws_2001 <- all_sales.date.week_s…io,
    round(weekly_dow.sat / (lead(weekly_dow.sat, 53) over (order by weekly_dow.ws asc)), 2) as sat_ratio
order by weekly_dow.ws asc nulls first
;`

  ```text

  ```
- `trilogy file write query02.preql`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales select all_sales.date.week_seq, count(*) as cnt where all_sales.channel in ('WEB','CATALOG') and all_sales.date.week_seq >= 5320 and all_sales.date.week_seq <= 5330 order by 1;`

  ```text

  ```
- `trilogy file write query02.preql`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales select all_sales.date.week_seq where all_sales.date.year = 2001 group by 1;`

  ```text

  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

select
    all_sales.channel,
    sum(all_sales.ext_sales_price) as gross_sale…l_sales.date.date between '2000-08-23'::date and '2000-09-06'::date
    and all_sales.channel_dim_id is not null
group by all_sales.channel
limit 10;`

  ```text

  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

# Channel label helper
auto sale_channel_label <- case
    when all_sales.chan… as g_entity
order by
    g_channel + g_entity desc nulls first,
    combined.channel asc nulls first,
    combined.entity asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query30.preql -e -c import raw.web_returns as wr;

# Step 1: Compute per-customer per-origin-state total return amounts in 2002
# (for ALL…ogin asc nulls first,
    email_address asc nulls first,
    last_review_date asc nulls first,
    total_web_return_amount asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query30.preql -e -c import raw.web_returns as wr;

# Step 1: Compute per-customer per-origin-state total return amounts in 2002
# (for ALL…ogin asc nulls first,
    email_address asc nulls first,
    last_review_date asc nulls first,
    total_web_return_amount asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query30.preql -e -c import raw.web_returns as wr;

# Per-customer per-state totals in 2002
auto cust_state_2002 <- sum(wr.return_amount) b…ogin asc nulls first,
    email_address asc nulls first,
    last_review_date asc nulls first,
    total_web_return_amount asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query44.preql --content import raw.physical_sales as s;

# Items at store id=1, ranked by avg net profit (ascending = best, descending = w…ank,
    paired.best_item,
    paired.worst_item
order by
    paired.pair_rank asc,
    paired.best_item desc,
    paired.worst_item desc
limit 100;
`

  ```text

  ```
- `trilogy file write query44.preql --content import raw.physical_sales as s;

# Threshold: 90% of the average net profit at store 1 where sale_address is null
…as worst_performer
inner join best_ranked.rnk = worst_ranked.rnk
order by
    rank asc,
    best_performer desc,
    worst_performer desc
limit 100;
`

  ```text

  ```
- `trilogy file write query54.preql --content import raw.catalog_sales as cs;
import raw.web_sales as ws;
import raw.physical_sales as ps;
import raw.customer a…t,
    segment * 50 as segment_times_50
order by
    segment asc nulls first,
    customer_count asc nulls first,
    segment_times_50 asc
limit 100;`

  ```text

  ```
- `trilogy file write query58.preql --content import raw.all_sales as sales;

# Find the week containing 2000-01-03
where sales.date.week_seq = 5218

# Compute … and web_total between 0.9 * catalog_total and 1.1 * catalog_total
order by
    item_code asc nulls first,
    store_total asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query59.preql --content import raw.physical_sales as ss;

# Weekly aggregates for 2001: sum of unit price (sales_price) per store per week…rder by this_year.store_name asc nulls first, 
         this_year.store_code asc nulls first, 
         this_year.week_seq asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query63.preql --content import raw.physical_sales as store_sales;

# Filter to year 2000 store sales with a recorded store, qualifying ite…ly_totals_avg as avg_monthly_sales
order by
    monthly_totals.mgr_id asc,
    monthly_totals_avg asc,
    monthly_totals.month_total asc
limit 100;
`

  ```text

  ```
- `trilogy file write query64.preql --content # Query 64: Identify items sold in store sales with complex catalog/sales return conditions
import raw.physical_sa…ear,
  store_agg.first_shipto_year,
  store_agg.line_count,
  store_agg.wholesale_cost_sum,
  store_agg.list_price_sum,
  store_agg.coupon_amt_sum
;
`

  ```text

  ```
- `trilogy file write query64.preql --content # Query 64: Identify items sold in store sales with complex catalog/sales return conditions
import raw.physical_sa…e,
  yr1999.store_agg.store.store.name,
  yr2000.store_agg.line_count,
  yr1999.store_agg.wholesale_cost_sum,
  yr2000.store_agg.wholesale_cost_sum
;`

  ```text

  ```
- `trilogy file write query67.preql --content import raw.physical_sales as ps;

# Define the summed sales metric with the rollup
# sum of (per-line sales price …   ps.date.month_of_year nulls first,
    ps.store.text_id nulls first,
    summed_sales nulls first,
    within_category_rank nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query70.preql --content import raw.physical_sales as store_sales;

where store_sales.date.year = 2000;

# State-level total profit for ide…level = 2
    or (store_sales.store.state in top5_state)
order by
    level desc,
    store_sales.store.state asc nulls first,
    rnk asc
limit 100;`

  ```text

  ```
- `trilogy file write query70.preql --content import raw.physical_sales as store_sales;

# Exclude rows with null store state (no meaningful state assignment)
w…level = 2
    or (store_sales.store.state in top5_state)
order by
    level desc,
    store_sales.store.state asc nulls first,
    rnk asc
limit 100;`

  ```text

  ```
- `trilogy file write query74.preql --content import raw.physical_sales as store;
import raw.web_sales as web;

# Store sales total net_paid per customer per ye…ined.web_total > 0
having
    -- customer must have all 4 channel-year combos
    -- 2002 store exists
    -- 2002 web exists
    -- ratio condition
`

  ```text

  ```
- `trilogy file write query75.preql`

  ```text

  ```
- `trilogy file write query76.preql --content import raw.physical_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sales as cs;

with combined as uni…missing_ref nulls first,
    combined.sale_year nulls first,
    combined.sale_quarter nulls first,
    combined.item_category nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query78.preql --content import raw.physical_sales as store;
import raw.web_sales as web;
import raw.catalog_sales as catalog;

# Store cha… customer,
  store_agg.store_qty desc,
  store_agg.store_wholesale desc,
  store_agg.store_sales_price desc,
  other_qty asc,
  ratio asc
limit 100;
`

  ```text

  ```
- `trilogy file write query82.preql --content import raw.inventory as inv;
import raw.physical_sales as ss;
import raw.item as item;

# Use inventory as the bas…nd '2000-07-24'::date
select
    item.text_id as item_code,
    item.desc as description,
    item.current_price
order by
    item.text_id
limit 100;`

  ```text

  ```
- `trilogy file write query83.preql --content import raw.all_sales as sales;

# Filter to return rows in the specified weeks
where sales.return_date.week_seq in… 0
    and count(sales.order_id ? sales.channel = 'WEB') > 0
order by
    item_code asc nulls first,
    store_return_qty asc nulls first
limit 100;
`

  ```text

  ```
- `trilogy file write query83.preql -e -c import raw.all_sales as sales;

# Define reuseable channel-specific aggregates
def store_ret_qty -> coalesce(sum(sales… * 100 as web_pct,
    @three_sum / 3.0 as three_channel_avg
order by
    item_code asc nulls first,
    store_return_qty asc nulls first
limit 100;
`

  ```text

  ```
- `trilogy file write query86.preql --content import raw.web_sales as ws;

# Web sales in year 2000: total net paid rolled up by item category and item class
# …egory, ws.item.class) over (partition by hierarchy_level, ws.item.category order by total_net_paid desc nulls last) as within_parent_rank
limit 100;
`

  ```text

  ```
- `trilogy file write query92.preql --content import raw.web_sales as ws;

# Filter to the date window and manufacturer
where ws.date.date between '2000-01-27':…) by ws.item.id;

select
    sum(ws.ext_discount_amount ? ws.ext_discount_amount > 1.3 * avg_ext_discount_by_item) as total
order by total
limit 100;`

  ```text

  ```
- `trilogy file write query92.preql --content import raw.web_sales as ws;

# Per-item average extended discount (across all web sales of that item in the date w…ount > 1.3 * avg_ext_discount_by_item) as total
auto avg_ext_discount_by_item <- avg(ws.ext_discount_amount) by ws.item.id;
order by total
limit 100;`

  ```text

  ```
- `trilogy run --import raw.physical_sales:sales select sales.return_reason.desc, count(sales.line_item) as cnt where sales.return_reason.desc is not null group by sales.return_reason.desc order by cnt desc limit 20;`

  ```text

  ```
- `trilogy file write query97.preql --content import raw.physical_sales as store;
import raw.catalog_sales as catalog;

# Store sales: unique (customer, item) c…1 else 0 end) as both,
full join store_pairs.customer_id = catalog_pairs.customer_id
full join store_pairs.item_id = catalog_pairs.item_id
limit 100;`

  ```text

  ```
- `trilogy file write query97.preql --content import raw.physical_sales as store;
import raw.catalog_sales as catalog;

# Store sales: unique (customer, item) c…1 else 0 end) as both,
full join store_pairs.customer_id = catalog_pairs.customer_id
full join store_pairs.item_id = catalog_pairs.item_id
limit 100;`

  ```text

  ```
- `trilogy run --import raw.physical_sales:store --import raw.catalog_sales:catalog select count(*) as cnt from (select store.customer.id, store.item.id from store where store.date.year=2000 and store.customer.id is not null); duckdb ./tpcds.duckdb`

  ```text

  ```
- `trilogy run --import raw.physical_sales:store select count(customer) as unique_pairs from (select store.customer.id as customer, store.item.id as item where store.date.year=2000 and store.customer.id is not null); duckdb ./tpcds.duckdb`

  ```text

  ```

### `cli-misuse`

- `trilogy run query12.preql --format json`

  ```text
  '--format' is not a valid dialect. The dialect argument comes after the input file and any options.
    Try: trilogy run <input> [<dialect>]
  ```
- `trilogy explore raw/customer_address.preql`

  ```text
  Invalid value for 'PATH': File 'raw/customer_address.preql' does not exist.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy explore raw/sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/sales.preql' does not exist.
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
- `trilogy explore raw/sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/sales.preql' does not exist.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy run query77.preql --format json`

  ```text
  '--format' is not a valid dialect. The dialect argument comes after the input file and any options.
    Try: trilogy run <input> [<dialect>]
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```

### `file-not-found`

- `trilogy run query51.preql`

  ```text

  ```
- `trilogy explore query51.preql`

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
- `trilogy explore query75.preql`

  ```text

  ```
- `trilogy run query82.preql`

  ```text

  ```

### `syntax-missing-alias`

- `trilogy run --import raw/all_sales:all_sales select all_sales.channel, count(all_sales.order_id) limit 10;`

  ```text

  ```
- `trilogy run --import raw/physical_sales:ps --import raw/customer:c select ps.customer.id, count(ps.ticket_number), sum(ps.ext_sales_price) where ps.date.mont…lass = 'maternity') and ps.store.county = ps.customer.address.county and ps.store.state = ps.customer.address.state group by ps.customer.id limit 10;`

  ```text

  ```
- `trilogy file write query67.preql --content import raw.physical_sales as ps;

# Filter for store sales in year 2000
where ps.date.year = 2000

select
    ps.i…   ps.date.month_of_year nulls first,
    ps.store.text_id nulls first,
    summed_sales nulls first,
    within_category_rank nulls first
limit 100;`

  ```text

  ```

### `join-resolution`

- `trilogy run query30.preql`

  ```text

  ```
- `trilogy run query30.preql`

  ```text

  ```
