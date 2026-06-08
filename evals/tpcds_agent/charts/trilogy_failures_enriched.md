# Trilogy failure analysis — 20260608-031504

- Run `20260608-031455_enriched` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 1828 | failed: 252 (14%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 117 | 46% |
| `syntax-parse` | 77 | 31% |
| `undefined-concept` | 19 | 8% |
| `cli-misuse` | 15 | 6% |
| `syntax-missing-alias` | 12 | 5% |
| `join-resolution` | 7 | 3% |
| `type-error` | 3 | 1% |
| `file-not-found` | 2 | 1% |

## Detail

### `other`

- `trilogy run query01.preql`

  ```text

  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query02.preql`

  ```text

  ```
- `trilogy file read query02.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query04.preql duckdb`

  ```text

  ```
- `trilogy run query04.preql duckdb`

  ```text

  ```
- `trilogy run query04.preql duckdb`

  ```text

  ```
- `trilogy run query04.preql duckdb`

  ```text

  ```
- `trilogy run query04.preql duckdb`

  ```text

  ```
- `trilogy run query04.preql duckdb`

  ```text

  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query05.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/physical_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query06.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text

  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query13.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy agent-info syntax example rollup`

  ```text

  ```
- `trilogy run query14.preql`

  ```text

  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/date.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read query17.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/physical_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe store_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query25.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query27.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query31.preql`

  ```text

  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 50 column 12 (char 1972). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query38.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query39.preql`

  ```text

  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.physical_sales:ss --import raw.item:item select ss.item.id as item_id, item.product_name as name, avg(ss.net_profit ? ss.store.id = 1) by ss.item.id as avg_profit, count(ss.line_item ? ss.store.id = 1) by ss.item.id as cnt where ss.store.id = 1 order by avg_profit asc limit 10;`

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
- `trilogy run --import raw.physical_sales:ss --import raw.item:item select
    row_number() over (order by avg(ss.net_profit) by ss.item.id asc) as best_positi…s.store.id = 1
and avg(ss.net_profit) by ss.item.id > 0.9 * avg(ss.net_profit ? ss.sale_address.id is null) by *
order by best_position asc
limit 10;`

  ```text

  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy run --import raw.physical_sales:ss --import raw.item:item select ss.item.product_name as name, avg(ss.net_profit) by ss.item.id as avg_profit where ss.store.id = 1 and avg(ss.net_profit) by ss.item.id > 0.9 * avg(ss.net_profit ? ss.sale_address.id is null) by * order by avg_profit asc limit 5;`

  ```text

  ```
- `trilogy file read query44.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query48.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query49.preql`

  ```text

  ```
- `trilogy run query49.preql`

  ```text

  ```
- `trilogy run query49.preql`

  ```text

  ```
- `trilogy run query49.preql`

  ```text

  ```
- `trilogy run query49.preql`

  ```text

  ```
- `trilogy run query49.preql`

  ```text

  ```
- `trilogy run query49.preql`

  ```text

  ```
- `trilogy file read query51.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query53.preql`

  ```text

  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 23 column 12 (char 1081). Re-issue the call with valid JSON arguments.
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
- `trilogy file read query65.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query66.preql`

  ```text

  ```
- `trilogy run query66.preql`

  ```text

  ```
- `trilogy run query66.preql`

  ```text

  ```
- `trilogy file read query68.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query70.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query71.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query74.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
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
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run query76.preql`

  ```text

  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
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
- `trilogy run query80.preql`

  ```text

  ```
- `trilogy run query80.preql`

  ```text

  ```
- `trilogy run query80.preql`

  ```text

  ```
- `trilogy run query80.preql`

  ```text

  ```
- `trilogy run query80.preql`

  ```text

  ```
- `trilogy file read raw/catalog_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query81.preql`

  ```text

  ```
- `trilogy run query82.preql`

  ```text

  ```
- `trilogy file read raw/physical_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query83.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query83.preql`

  ```text

  ```
- `trilogy file read raw/physical_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/customer.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query85.preql`

  ```text

  ```
- `trilogy run query87.preql`

  ```text

  ```
- `trilogy file read query87.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy --debug run query87.preql`

  ```text

  ```
- `trilogy run query89.preql`

  ```text

  ```
- `trilogy run query90.preql`

  ```text

  ```
- `trilogy file read raw/physical_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query94.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query97.preql`

  ```text

  ```
- `trilogy file read query97.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/physical_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe store_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```

### `syntax-parse`

- `trilogy run --import raw/all_sales:all select all.date.week_seq, count(all.row_one) as cnt where all.sales_channel in ('WEB','CATALOG') group by all.date.week_seq order by all.date.week_seq;`

  ```text

  ```
- `trilogy run --import raw/date:d select d.week_seq, d.year, count(d.id) as days where d.week_seq between 5322 and 5325 group by ... order by d.week_seq;`

  ```text

  ```
- `trilogy file write query02.preql --content import raw/all_sales as all;

# Identify week_seqs that have a date in 2001
auto ws_2001 <- select all.date.week_s…i_ratio,
    round(coalesce(sat_curr, sat_curr) / coalesce(sat_next, sat_next), 2) as sat_ratio
having w in ws_2001
order by w nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query02.preql --content import raw.all_sales as all;

# Identify week_seqs that have a date in 2001
auto ws_2001 <- select all.date.week_s…i_ratio,
    round(coalesce(sat_curr, sat_curr) / coalesce(sat_next, sat_next), 2) as sat_ratio
having w in ws_2001
order by w nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query02.preql --content import raw.all_sales as all;

# Arm 1: current week sums (all years for week_seqs appearing in 2001)
where all.sal… fri_next), 2) as fri_ratio,
    round(coalesce(sat_curr, sat_curr) / coalesce(sat_next, sat_next), 2) as sat_ratio
order by w nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query04.preql --content import raw/physical_sales.preql as store;
import raw/catalog_sales.preql as catalog;
import raw/web_sales.preql as…
order by
    cust asc nulls first,
    first_name asc nulls first,
    last_name asc nulls first,
    preferred_cust_flag asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query04.preql --content import raw.physical_sales as store;
import raw.catalog_sales as catalog;
import raw.web_sales as web;
import raw.c…
order by
    cust asc nulls first,
    first_name asc nulls first,
    last_name asc nulls first,
    preferred_cust_flag asc nulls first
limit 100;`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales select all_sales.sales_channel, all_sales.channel_dim_text_id, left(all_sales.channel_dim_text_id, 1) as first_char, count(distinct all_sales.channel_dim_text_id) by all_sales.sales_channel as num_distinct;`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales select all_sales.sales_channel, substring(all_sales.channel_dim_text_id, 1, 1) as first_char, count(all_sales.channel_dim_text_id) as cnt by (all_sales.sales_channel, substring(all_sales.channel_dim_text_id, 1, 1)) limit 50;`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales select all_sales.sales_channel, count(all_sales.channel_dim_text_id) as cnt group by all_sales.sales_channel limit 50;`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales select all_sales.sales_channel, all_sales.return_channel_dim_text_id, count(all_sales.return_channel_dim_text_id) as cnt_by_chan_dim by (all_sales.sales_channel, all_sales.return_channel_dim_text_id) limit 20;`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales select case(all_sales.sales_channel, 'STORE','store channel','CATALOG','catalog channel','WEB','web channel') as…et_loss) as net_profit where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date and all_sales.channel_dim_id is not null limit 10;`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales select case(all_sales.sales_channel, 'STORE','store channel','CATALOG','catalog channel','WEB','web channel') as…et_loss) as net_profit where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date and all_sales.channel_dim_id is not null limit 10;`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales auto channel_label <- case(all_sales.sales_channel, 'STORE','store channel','CATALOG','catalog channel','WEB','web channel'); select channel_label limit 5;`

  ```text

  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

# Channel label from sales_channel enum
auto channel_label <- case
    when al…s total_returns,
    coalesce(s_profit, 0) - coalesce(r_loss, 0) as net_profit
order by channel asc nulls first, entity_id asc nulls first
limit 100;`

  ```text

  ```
- `trilogy run --import raw.all_sales:sales select count_distinct(sales.item.brand_id, sales.item.class_id, sales.item.category_id ? count_distinct(sales.sales_channel) by (sales.item.brand_id, sales.item.class_id, sales.item.category_id) = 3) as valid_bcc_count;`

  ```text

  ```
- `trilogy run --import raw.all_sales:sales select sales.sales_channel, sum(sales.ext_list_price ? sales.date.year = 2001 and sales.date.month_of_year = 11) as …nnel ? sales.date.year between 1999 and 2001) by (sales.item.brand_id, sales.item.class_id, sales.item.category_id) = 3 group by sales.sales_channel;`

  ```text

  ```
- `trilogy run --import raw.catalog_store_returns:csr select csr.catalog_date.quarter_name as q, count(csr.ticket_number) as cnt where csr.store_sale_date.quarter_name = '2001Q1' and csr.store_return_date.quarter_name in ('2001Q1','2001Q2','2001Q3') group by q order by q limit 20;`

  ```text

  ```
- `trilogy run --import raw.catalog_store_returns:csr select csr.store.state as st, count(csr.ticket_number) as cnt where csr.store_sale_date.quarter_name = '2001Q1' group by st order by st limit 100;`

  ```text

  ```
- `trilogy file write query18.preql --content import raw.catalog_sales as cs;

# Filter conditions
where year(cs.sold_date.date) = 1998
  and cs.bill_customer_d…s.county nulls first,
    cs.item.text_id nulls first,
    grp_country nulls first,
    grp_state nulls first,
    grp_county nulls first
limit 100;
`

  ```text

  ```
- `trilogy file write query23.preql -e -c import raw.physical_sales as store_sales;
import raw.all_sales as sales;

# Frequent items: items from store sales whe…* sales.list_price) as total_sales
order by
    last_name asc nulls first,
    first_name asc nulls first,
    total_sales asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query23.preql -e -c import raw.physical_sales as store_sales;
import raw.all_sales as sales;

# Frequent items: distinct (item, sold_date)…* sales.list_price) as total_sales
order by
    last_name asc nulls first,
    first_name asc nulls first,
    total_sales asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query27.preql --content import raw.physical_sales as store_sales;

where year(store_sales.date.date) = 2002
  and store_sales.store.state …nt,
    avg(store_sales.sales_price) as avg_unit_price
order by
    item_code asc nulls first,
    store_sales.store.state asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query27.preql --content import raw.physical_sales as store_sales;

where year(store_sales.date.date) = 2002
  and store_sales.store.state …_amount,
    @rollup_avg_sales_price as avg_unit_price
order by
    item_code asc nulls first,
    store_sales.store.state asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query27.preql --content import raw.physical_sales as store_sales;

def rollup_avg_quantity -> avg(store_sales.quantity) by rollup store_sa…_amount,
    @rollup_avg_sales_price as avg_unit_price
order by
    item_code asc nulls first,
    store_sales.store.state asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query36.preql --content import raw.physical_sales as store_sales;

# For store sales in year 2001 at stores in TN
# Gross-margin ratio = s…n_parent_rank
order by
    hierarchy_level desc nulls first,
    store_sales.item.category nulls first,
    within_parent_rank nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query36.preql -c import raw.physical_sales as store_sales;

where year(store_sales.date.date) = 2001
  and store_sales.store.state = 'TN'
…n_parent_rank
order by
    hierarchy_level desc nulls first,
    store_sales.item.category nulls first,
    within_parent_rank nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query37.preql --content import raw.catalog_sales as catalog_sales;
import raw.inventory as inventory;

inner join inventory.item.id = cata…ry.item.text_id as item_code,
    inventory.item.desc as description,
    inventory.item.current_price as current_price
order by item_code
limit 100;`

  ```text

  ```
- `trilogy file write query39.preql --content import raw.inventory as inv;

# January 2001: warehouse, item, mean, stddev, CV
where year(inv.date.date) = 2001 a…,
    coalesce(feb_month, feb_month) asc nulls first,
    coalesce(feb_mean, feb_mean) asc nulls first,
    coalesce(feb_cv, feb_cv) asc nulls first;`

  ```text

  ```
- `trilogy file write query41.preql --content import raw.item as item;

# Step 1: Build a rowset of manufacturers that have at least one item
# matching any of …item.product_name
where
  item.manufacturer_id between 1 and 500
  and item.manufact in profile_manufacts.mfr
order by
  item.product_name
limit 100;`

  ```text

  ```
- `trilogy run --import raw.physical_sales:ss select ss.sale_address.id is null as addr_null, count(ss.line_item) as cnt where ss.store.id = 1 group by addr_null limit 5;`

  ```text

  ```
- `trilogy file write query44.preql --content import raw.physical_sales as ss;
import raw.item as item;

auto null_addr_avg_profit <- avg(ss.net_profit ? ss.sto…e
    coalesce(best_profit, best_profit) as best_avg_profit,
    coalesce(worst_profit, worst_profit) as worst_avg_profit
order by pos asc
limit 100;`

  ```text

  ```
- `trilogy file write query44.preql -e -c import raw.physical_sales as ss;
import raw.item as item;

# Pre-compute qualifying items
rowset qualifying_items <-
 …product_name, qualifying_items.product_name) as worst_performer_name
order by pos asc, best_performer_name desc, worst_performer_name desc
limit 100;`

  ```text

  ```
- `trilogy file write query44.preql -e -c import raw.physical_sales as ss;
import raw.item as item;

# Pre-compute qualifying items
rowset qualifying_items <-
 …r_name,
    coalesce(worst_name, worst_name) as worst_performer_name
order by pos asc, best_performer_name desc, worst_performer_name desc
limit 100;`

  ```text

  ```
- `trilogy file write query49.preql --content import raw.all_sales as s;

# Items that have at least one sale line in Dec 2001 
# with a matching return where r…_currency_rank
having qty_rank <= 10 or curr_rank <= 10
order by channel asc, return_quantity_rank asc, return_currency_rank asc, item asc
limit 100;`

  ```text

  ```
- `trilogy file write query49.preql --content import raw.web_sales as ws;
import raw.web_returns as wr;

# Merge the two on the same grain (item.id, order_numbe…return_items)

select
    ws.item.id as item,
    return_qty_ratio as return_quantity_ratio,
    return_curr_ratio as return_currency_ratio
limit 10;`

  ```text

  ```
- `trilogy file write query51.preql --content import raw.all_sales as al;

# Filter to year 2000 and only WEB and STORE channels
where al.date.year = 2000 and a…eb_running_max,
    store_rt_max as store_running_max
having web_rt_max > store_rt_max
order by item asc nulls first, date asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query57.preql --content import raw.catalog_sales as cs;

# Monthly total: sum of per-unit sales price by (category, brand, call_center, ye…tegory, cs.item.brand_name, cs.call_center.name, cs.sold_date.year, cs.sold_date.month_of_year, cur_total, avg_ms, prev_total, next_total
limit 100;
`

  ```text

  ```
- `trilogy run --import raw.date select date.week_seq, date.date, date.year, date.month_of_year, date.day_of_month from raw.date where date.date = '2000-01-03'::date;`

  ```text

  ```
- `trilogy run --import raw.all_sales:sales select sales.sales_channel, sales.item.text_id, sum(sales.ext_sales_price) as total where sales.date.week_seq = 5218 group by sales.sales_channel, sales.item.text_id limit 10;`

  ```text

  ```
- `trilogy file write query59.preql --content # Query 59: Store unit price by day-of-week, paired across years
# For each (store, calendar week), sum the unit p…_a / sat_b else null end as sat_ratio
order by store_name asc nulls first, store_code asc nulls first, this_year_week_seq asc nulls first
limit 100;
`

  ```text

  ```
- `trilogy file write query65.preql --content import raw.physical_sales as ps;
import raw.store as store;
import raw.item as item;

# Per-item revenue: sum of u… item.brand_name
having item_revenue <= 0.10 * store_avg_rev
order by store_name asc, item_desc asc, store.id asc, item.id asc
limit 100
nulls first;`

  ```text

  ```
- `trilogy file write query65.preql --content import raw.physical_sales as ps;
import raw.store as store;
import raw.item as item;

# Per-item revenue: sum of u….current_price,
    item.wholesale_cost,
    item.brand_name
order by store_name asc, item_desc asc, store.id asc, item.id asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query65.preql --content import raw.physical_sales as ps;
import raw.store as store;
import raw.item as item;

# Per-item revenue: sum of u…esale_cost,
    item.brand_name,
    item_revenue as revenue
order by store_name asc, item_desc asc, store.id asc, item.id asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query66.preql --content import raw.all_sales as sales;

# Months 1-12 reference
auto month_1 <- 1;
auto month_2 <- 2;
auto month_3 <- 3;
a…thly_sales_per_sqft,
    coalesce(sales_data.tot_net, 0) as monthly_net
order by sales_data.wh_name nulls first, sales_data.yr nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query66.preql --content import raw.all_sales as sales;
import raw.warehouse as wh;

# Months 1-12 spine
rowset all_months <- select 1 as m…sce(sum(agg_data.tot_net ? agg_data.mo = all_months.mo) by all_months.mo, 0) as monthly_net
order by wh.name nulls first, year nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query66.preql --content import raw.all_sales as sales;

# All distinct warehouse combinations that have qualifying sales
rowset wh_data <-…s.net_paid
                 when sales.sales_channel = 'CATALOG' then sales.quantity * sales.net_paid_inc_tax
            end) as tot_net;

select 1;`

  ```text

  ```
- `trilogy file write query66.preql --content import raw.all_sales as sales;

# Arm 1: All months (1-12) x all warehouses (with qualifying sales)
rowset month_l…ehouse_list.co as wcounty,
        warehouse_list.st as wstate,
        warehouse_list.ct as wcountry,
        month_list.month_num as mo;

select 1;`

  ```text

  ```
- `trilogy file write query66.preql --content import raw.all_sales as sales;
import raw.warehouse as wh;

# Arms: first gets warehouse+month combinations, secon…as b_wn,
    wh.square_feet as b_sq,
    wh.city as b_ci,
    wh.county as b_co,
    wh.state as b_st,
    wh.country as b_ct,
    1 as b_mo
limit 0;`

  ```text

  ```
- `trilogy file write query70.preql --content import raw.physical_sales as store_sales;

# Top 5 states by total net profit from store sales in year 2000
auto t…    order by sum(store_sales.net_profit) desc
    ) as within_parent_rank
order by hierarchy_level desc, state asc, within_parent_rank asc
limit 100;`

  ```text

  ```
- `trilogy file write query73.preql --content import raw.physical_sales as store_sales;

# Filters: ticket years, day of month, store counties, household demogr…  line_item_count desc,
    store_sales.customer.last_name asc,
    store_sales.ticket_number asc,
    store_sales.customer.first_name asc
limit 100;`

  ```text

  ```
- `trilogy file write query75.preql --content import raw.all_sales as sales;

# Per-line net sales quantity and amount (returns treated as zero if missing)
auto… curr_qty - prev_qty as qty_diff,
    curr_amt - prev_amt as amt_diff
having curr_qty < 0.9 * prev_qty
order by qty_diff asc, amt_diff asc
limit 100;`

  ```text

  ```
- `trilogy file write query76.preql --content import raw/physical_sales as store_sales;
import raw/web_sales as web_sales;
import raw/catalog_sales as catalog_s…0) as total_extended_sales_price
order by ch asc nulls first, ref asc nulls first, y asc nulls first, q asc nulls first, c asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query76.preql --content import raw.physical_sales as store_sales;
import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_s…0) as total_extended_sales_price
order by ch asc nulls first, ref asc nulls first, y asc nulls first, q asc nulls first, c asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query77.preql --content import raw.all_sales as all_sales;

# Channel labels
auto channel_label <- case
    when all_sales.sales_channel =…@rollup_profit(profit_from_sales, loss_from_returns) as profit

order by channel asc nulls first, outlet_id asc nulls first, returns desc
limit 100;
`

  ```text

  ```
- `trilogy file write query78.preql --content import raw.physical_sales as stores;
import raw.web_sales as web;
import raw.catalog_sales as catalog;

# Arm 1: S…er by year asc, item asc, customer asc, store_qty desc, store_wc desc, store_sp desc, other_qty asc, other_wc asc, other_sp asc, ratio asc
limit 100;`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales select all_sales.sales_channel, sum(all_sales.ext_sales_price) as sales, sum(all_sales.return_amount) as returns… all_sales.promotion.channel_tv = 'N' and all_sales.outlet_id is not null by rollup all_sales.sales_channel, all_sales.channel_dim_text_id limit 100;`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales select all_sales.sales_channel, all_sales.channel_dim_text_id, sum(all_sales.ext_sales_price) as sales, sum(coal…v = 'N' and all_sales.outlet_id is not null order by all_sales.sales_channel asc nulls first, all_sales.channel_dim_text_id asc nulls first limit 30;`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales select all_sales.sales_channel, all_sales.channel_dim_text_id, count(*) as cnt where all_sales.channel_dim_text_id is null group by all_sales.sales_channel, all_sales.channel_dim_text_id;`

  ```text

  ```
- `trilogy file write query80.preql -e -c import raw.all_sales as all_sales;

def rollup_metrics(sales_metric, ret_metric, profit_metric, loss_metric) -> sum(sa…_price, all_sales.return_amount, all_sales.net_profit, all_sales.return_net_loss)
order by channel asc nulls first, outlet asc nulls first
limit 100;`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales select all_sales.sales_channel, all_sales.channel_dim_text_id, sum(all_sales.ext_sales_price) as sales, sum(coal…ales_channel, all_sales.channel_dim_text_id order by all_sales.sales_channel asc nulls first, all_sales.channel_dim_text_id asc nulls first limit 30;`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales select all_sales.sales_channel, count(all_sales.channel_dim_text_id) as cnt, sum(all_sales.ext_sales_price) as t… all_sales.promotion.channel_tv = 'N' and all_sales.outlet_id is not null and all_sales.channel_dim_text_id is null group by all_sales.sales_channel;`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales select all_sales.sales_channel, sum(all_sales.ext_sales_price) as sales, sum(all_sales.net_profit) as profit whe…50 and all_sales.promotion.channel_tv = 'N' and all_sales.outlet_id is not null and all_sales.channel_dim_text_id is null by all_sales.sales_channel;`

  ```text

  ```
- `trilogy file write query80.preql -e -c import raw.all_sales as all_sales;

where all_sales.date.date between '2000-08-23'::date and '2000-09-22'::date
  and …lesce(--net_profit_sum, 0) - coalesce(--return_loss_sum, 0) as profit
having true
order by channel asc nulls first, outlet asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query80.preql -e -c import raw.all_sales as all_sales;

where all_sales.date.date between '2000-08-23'::date and '2000-09-22'::date
  and …_loss, 0)) by rollup all_sales.sales_channel, all_sales.channel_dim_text_id as rl
order by channel asc nulls first, outlet asc nulls first
limit 100;`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales select simple_case(all_sales.sales_channel, 'STORE', 'store channel', 'CATALOG', 'catalog channel', 'WEB', 'web …te and '2000-09-22'::date and all_sales.item.current_price > 50 and all_sales.promotion.channel_tv = 'N' and all_sales.outlet_id is not null limit 5;`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales select case(all_sales.sales_channel = 'STORE', 'store channel', all_sales.sales_channel = 'CATALOG', 'catalog ch…te and '2000-09-22'::date and all_sales.item.current_price > 50 and all_sales.promotion.channel_tv = 'N' and all_sales.outlet_id is not null limit 5;`

  ```text

  ```
- `trilogy file write query81.preql -e -c import raw.catalog_returns as cr;

# For catalog returns in year 2000
where cr.date.year = 2000

# Also filter to cust…, cr.billing_customer.address.country, cr.billing_customer.address.gmt_offset, cr.billing_customer.address.location_type, cust_state_total
limit 100;`

  ```text

  ```
- `trilogy file write query83.preql -e -c import raw.all_sales as all_sales;

# Filter to the specific weeks containing the three dates
where all_sales.is_retur…turn_pct,
    three_channel_avg as three_channel_average
order by
    item_code asc nulls first,
    store_return_quantity asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query83.preql -e -c import raw.all_sales as all_sales;

# Define per-channel return quantities (sum of return_quantity)
auto store_return_…turn_pct,
    three_channel_avg as three_channel_average
order by
    item_code asc nulls first,
    store_return_quantity asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query83.preql -e -c import raw.all_sales as all_sales;

# Define per-channel return quantities (sum of return_quantity)
auto store_return_…turn_pct,
    three_channel_avg as three_channel_average
order by
    item_code asc nulls first,
    store_return_quantity asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query83.preql -e -c import raw.all_sales as all_sales;

# Define per-channel return quantities (sum of return_quantity)
auto store_return_…turn_pct,
    three_channel_avg as three_channel_average
order by
    item_code asc nulls first,
    store_return_quantity asc nulls first
limit 100;`

  ```text

  ```
- `trilogy run --import raw.all_sales:sales select sum(case when cs>0 and ws>0 then 1 else 0 end) as in_catalog_and_web, sum(case when cs>0 and ws=0 then 1 else…EB' and year(sales.date.date)=2000 then 1 else 0 end) as ws by sales.billing_customer.last_name, sales.billing_customer.first_name, sales.date.date);`

  ```text

  ```
- `trilogy file write query90.preql --content import raw.web_sales as web_sales;

# Count of web sale line items for morning hours (8-9)
auto morning_lines <- c…r_count <= 5200));

select
  case when evening_lines = 0 then null else morning_lines / evening_lines::numeric end as ratio
order by ratio
limit 100;`

  ```text

  ```
- `trilogy file write query90.preql --content import raw.web_sales as web_sales;

# Count of web sale line items for morning hours (8-9)
auto morning_lines <- c…r_count <= 5200));

select
  case when evening_lines = 0 then null else morning_lines / evening_lines::numeric end as ratio
order by ratio
limit 100;`

  ```text

  ```
- `trilogy file write query97.preql --content import raw.physical_sales as store;
import raw.catalog_sales as catalog;

# Merge customer concepts so customer ID…irs.cust_id not in catalog_pairs.cust_id and store_pairs.item_id is not null and store_pairs.item_id not in catalog_pairs.item_id then 1 else 0 end);`

  ```text

  ```
- `trilogy file write query97.preql --content import raw.physical_sales as store;
import raw.catalog_sales as catalog;

# Merge customer concepts so they're com… end) as catalog_only,
    -- count both -> both
    sum(case when st_cust is not null and ca_cust is not null then 1 else 0 end) as both
limit 100;
`

  ```text

  ```

### `undefined-concept`

- `trilogy run query02.preql`

  ```text

  ```
- `trilogy run query06.preql`

  ```text

  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text

  ```
- `trilogy run query11.preql duckdb`

  ```text

  ```
- `trilogy run query11.preql duckdb`

  ```text

  ```
- `trilogy run query23.preql`

  ```text

  ```
- `trilogy run query45.preql`

  ```text

  ```
- `trilogy run query48.preql`

  ```text

  ```
- `trilogy run query49.preql`

  ```text

  ```
- `trilogy run query49.preql`

  ```text

  ```
- `trilogy run query49.preql`

  ```text

  ```
- `trilogy run query57.preql`

  ```text

  ```
- `trilogy run --import raw.date select date.week_seq, date.date, date.year, date.month_of_year, date.day_of_month where date.date = '2000-01-03'::date;`

  ```text

  ```
- `trilogy run query64.preql`

  ```text

  ```
- `trilogy run query67.preql`

  ```text

  ```
- `trilogy run query71.preql`

  ```text

  ```
- `trilogy run query79.preql`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales select --help;`

  ```text

  ```
- `trilogy run --import raw/all_sales:as select as.item.text_id as item_code, --as.store_rows where as.is_returned = true limit 5;`

  ```text

  ```

### `cli-misuse`

- `trilogy run query14.preql --format json`

  ```text
  '--format' is not a valid dialect. The dialect argument comes after the input file and any options.
    Try: trilogy run <input> [<dialect>]
  ```
- `trilogy run duckdb --import raw.catalog_sales:cs select cs.warehouse_id limit 5;`

  ```text
  'select cs.warehouse_id limit 5;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy explore raw/date_dim.preql`

  ```text
  Invalid value for 'PATH': File 'raw/date_dim.preql' does not exist.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy file write --content import raw.physical_sales as store_sales;

where year(store_sales.date.date) = 2002
  and store_sales.store.state = 'TN'
  and s…ore_sales.sales_price) as avg_unit_price
order by
    item_code asc nulls first,
    store_sales.store.state asc nulls first
limit 100;
 --escapes -e`

  ```text
  Missing argument 'PATH'.
  ```
- `trilogy explore raw/`

  ```text
  Invalid value for 'PATH': File 'raw/' is a directory.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy explore raw/sale_address.preql`

  ```text
  Invalid value for 'PATH': File 'raw/sale_address.preql' does not exist.
  ```
- `trilogy run query51.preql --format json`

  ```text
  '--format' is not a valid dialect. The dialect argument comes after the input file and any options.
    Try: trilogy run <input> [<dialect>]
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
- `trilogy explore raw/`

  ```text
  Invalid value for 'PATH': File 'raw/' is a directory.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw/all_sales:all select all.date.week_seq, count(all.row_one) where all.sales_channel in ('WEB','CATALOG') order by all.date.week_seq limit 60;`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales select distinct all_sales.sales_channel;`

  ```text

  ```
- `trilogy run --import raw.catalog_store_returns:csr select csr.store_sale_date.quarter_name, count(csr.ticket_number) limit 10;`

  ```text

  ```
- `trilogy run --import raw.physical_sales:ss --import raw.item:item select ss.store.id, count(ss.line_item) limit 5;`

  ```text

  ```
- `trilogy file write query47.preql --content import raw.physical_sales as sales;

where sales.date.year = 1999 or (sales.date.year = 1998 and sales.date.month_…, sales.store.company_name, sales.date.year, sales.date.month_of_year, avg_monthly_sales, month_total, prev_month_total, next_month_total
limit 100;
`

  ```text

  ```
- `trilogy run --import raw/catalog_sales:cs select count(cs.bill_customer.id) where cs.item.category = 'Women' and cs.item.class = 'maternity' and cs.sold_date.month_seq = 1187;`

  ```text

  ```
- `trilogy run --import raw/web_sales:ws select count(ws.billing_customer.id) where ws.item.category = 'Women' and ws.item.class = 'maternity' and ws.date.month_seq = 1187;`

  ```text

  ```
- `trilogy file write query75_check.preql --content import raw.item as item;
select distinct item.category, item.category_id
where item.category = 'Books'
order by item.category asc
limit 10;`

  ```text

  ```
- `trilogy file write query77.preql --content import raw.all_sales as all_sales;

# Channel labels
auto channel_label <- case
    when all_sales.sales_channel =…ns), 0) by rollup channel_label, all_sales.outlet_id as profit

order by channel asc nulls first, outlet_id asc nulls first, returns desc
limit 100;
`

  ```text

  ```
- `trilogy file write query77.preql --content import raw.all_sales as all_sales;

# Channel labels
auto channel_label <- case
    when all_sales.sales_channel =…p channel_label, all_sales.outlet_id as profit

order by channel_label asc nulls first, all_sales.outlet_id asc nulls first, returns desc
limit 100;
`

  ```text

  ```
- `trilogy file write query77.preql --content import raw.all_sales as all_sales;

# Channel labels
auto channel_label <- case
    when all_sales.sales_channel =…ns), 0) by rollup channel_label, all_sales.outlet_id as profit

order by channel asc nulls first, outlet_id asc nulls first, returns desc
limit 100;
`

  ```text

  ```
- `trilogy run --import raw.web_returns:wr select distinct wr.returning_demographic.education_status limit 20;`

  ```text

  ```

### `join-resolution`

- `trilogy run query16.preql duckdb`

  ```text

  ```
- `trilogy run --import raw.catalog_sales:cs select cs.warehouse_id limit 5; duckdb`

  ```text

  ```
- `trilogy run query40.preql`

  ```text

  ```
- `trilogy run query47.preql`

  ```text

  ```
- `trilogy run query49.preql`

  ```text

  ```
- `trilogy run query56.preql`

  ```text

  ```
- `trilogy run query65.preql`

  ```text

  ```

### `type-error`

- `trilogy run query04.preql duckdb`

  ```text

  ```
- `trilogy run query04.preql duckdb`

  ```text

  ```
- `trilogy run query04.preql duckdb`

  ```text

  ```

### `file-not-found`

- `trilogy run query37.preql`

  ```text

  ```
- `trilogy run query82.preql`

  ```text

  ```
