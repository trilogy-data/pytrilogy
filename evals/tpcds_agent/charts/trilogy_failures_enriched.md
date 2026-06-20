# Trilogy failure analysis — 20260620-030133

- Run `20260620-030133` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 332 | failed: 66 (20%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 40 | 61% |
| `syntax-parse` | 18 | 27% |
| `cli-misuse` | 6 | 9% |
| `join-resolution` | 1 | 2% |
| `file-not-found` | 1 | 2% |

## Detail

### `other`

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
- `trilogy run _test4.preql`

  ```text

  ```
- `trilogy run _test7.preql`

  ```text

  ```
- `trilogy run _test7.preql`

  ```text

  ```
- `trilogy run query14.preql`

  ```text

  ```
- `trilogy run query30.preql`

  ```text

  ```
- `trilogy run query30.preql`

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
- `trilogy file read raw/physical_sales.preql`

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
- `trilogy run query78.preql --import raw.physical_sales:ss select count(ss.row_counter) as cnt, ss.date.year where ss.date.year = 2000 and ss.customer.id is not null;`

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
- `trilogy file read raw/physical_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 38 column 12 (char 1452). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query83.preql duckdb`

  ```text

  ```
- `trilogy run query83.preql duckdb`

  ```text

  ```
- `trilogy run query97.preql`

  ```text

  ```

### `syntax-parse`

- `trilogy file write query02.preql --content import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Aggregate catalog sales by week_seq and day_of_week…s wed_ratio,
    @ratio_thu() as thu_ratio,
    @ratio_fri() as fri_ratio,
    @ratio_sat() as sat_ratio
order by pivoted.week_seq asc nulls first
;
`

  ```text

  ```
- `trilogy file write query02.preql --content import raw.all_sales as s;

where s.channel in ('WEB', 'CATALOG')

# Build weekly aggregates per day-of-week, then…ed,
    sum(wk.total ? wk.dow = 4) by wk.ws as thu,
    sum(wk.total ? wk.dow = 5) by wk.ws as fri,
    sum(wk.total ? wk.dow = 6) by wk.ws as sat
;
`

  ```text

  ```
- `trilogy file write query02.preql --content import raw.all_sales as s;

# Web and catalog sales only
where s.channel in ('WEB', 'CATALOG')

# Get week sequenc…ios.thu_ratio as thu_ratio,
    with_ratios.fri_ratio as fri_ratio,
    with_ratios.sat_ratio as sat_ratio
order by with_ratios.ws asc nulls first
;
`

  ```text

  ```
- `trilogy file write query05.preql --content import raw.all_sales as sales;

# Channel labels
auto store_label <- 'store channel';
auto catalog_label <- 'catal…turns_amt), 0) as total_returns,
  coalesce(sum(combined.net_profit), 0) as net_profit
order by combined.channel_label, combined.entity_id
limit 100;`

  ```text

  ```
- `trilogy file write _test5.preql --content import raw.all_sales as sales;
select
  sales.channel,
  sales.channel_dim_text_id,
  sum(sales.ext_sales_price) as… between '2000-08-23'::date and '2000-09-06'::date
  and sales.channel_dim_id is not null
group by sales.channel, sales.channel_dim_text_id
limit 10;`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales select all_sales.channel, all_sales.date.year, count(all_sales.row_one) as cnt group by all_sales.channel, all_sales.date.year order by all_sales.channel, all_sales.date.year limit 30;`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales select all_sales.date.year, all_sales.date.month_of_year, count(all_sales.row_one) as cnt where all_sales.date.year between 1999 and 2001 group by all_sales.date.year, all_sales.date.month_of_year limit 40;`

  ```text

  ```
- `trilogy file write query14.preql --content import raw.all_sales as s;

# Global average sale value across all channels, 1999-2001
auto overall_avg <- avg(s.e…channel asc nulls first,
    s.item.brand_id asc nulls first,
    s.item.class_id asc nulls first,
    s.item.category_id asc nulls first
limit 100;
`

  ```text

  ```
- `trilogy run --import raw/all_sales:s select avg(s.ext_list_price) by () as overall_avg where s.date.year between 1999 and 2001 limit 10;`

  ```text

  ```
- `trilogy file write query14.preql --content import raw.all_sales as s;

# Global average sale value across all channels, 1999-2001
auto overall_avg <- avg(s.e…channel asc nulls first,
    s.item.brand_id asc nulls first,
    s.item.class_id asc nulls first,
    s.item.category_id asc nulls first
limit 100;
`

  ```text

  ```
- `trilogy run --import raw/all_sales:s select s.channel, count(s.row_one) as cnt, sum(s.ext_list_price) as total where s.date.year = 2001 and s.date.month_of_year = 11 group by s.channel limit 10;`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.physical_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.phys…    sum(ss.ext_list_price) as list_price_sum,
    sum(ss.coupon_amt) as coupon_amt_sum
order by
    ss.item.product_name asc,
    ss.store.name asc;
`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.physical_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.phys…    sum(ss.ext_list_price) as list_price_sum,
    sum(ss.coupon_amt) as coupon_amt_sum
order by
    ss.item.product_name asc,
    ss.store.name asc;
`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.physical_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.phys…ar,
    agg_sales.first_ship_year,
    agg_sales.cnt,
    agg_sales.wholesale_cost_sum,
    agg_sales.list_price_sum,
    agg_sales.coupon_amt_sum
;
`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.physical_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

# ---- rowset:…by
    yr1999.product_name asc,
    yr1999.store_name asc,
    yr2000.cnt asc,
    yr1999.wholesale_cost_sum asc,
    yr2000.wholesale_cost_sum asc;
`

  ```text

  ```
- `trilogy file write query78.preql --content import raw.physical_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Store sales: never…,
  store_agg.ss_qty desc,
  store_agg.ss_wholesale_cost desc,
  store_agg.ss_sales_price desc,
  other_qty,
  other_sales_price,
  ratio
limit 100;
`

  ```text

  ```
- `trilogy file write query78.preql --content import raw.physical_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

select
  2000 as yea…d ws.billing_customer.id is not null), 0) > 0
order by
  1 asc,
  2 asc,
  3 asc,
  5 desc,
  6 desc,
  7 desc,
  8 asc,
  9 asc,
  4 asc
limit 100;
`

  ```text

  ```
- `trilogy file write query78.preql --content import raw.physical_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

select
  2000 as yea…d ws.billing_customer.id is not null), 0) > 0
order by
  1 asc,
  2 asc,
  3 asc,
  5 desc,
  6 desc,
  7 desc,
  8 asc,
  9 asc,
  4 asc
limit 100;
`

  ```text

  ```

### `cli-misuse`

- `trilogy run - --import raw.all_sales:sales select distinct sales.channel, sales.channel_dim_text_id limit 10;`

  ```text
  'select distinct sales.channel, sales.channel_dim_text_id limit 10;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy run --import raw.all_sales:sales duckdb select distinct sales.channel, sales.channel_dim_text_id limit 10;`

  ```text
  'select distinct sales.channel, sales.channel_dim_text_id limit 10;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy run --import raw.all_sales:sales duck_db select sales.channel, sales.channel_dim_text_id limit 10;`

  ```text
  'select sales.channel, sales.channel_dim_text_id limit 10;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy syntax example scoped-join`

  ```text
  No such command 'syntax'.
  ```
- `trilogy run query80.preql --format json`

  ```text
  '--format' is not a valid dialect. The dialect argument comes after the input file and any options.
    Try: trilogy run <input> [<dialect>]
  ```

### `join-resolution`

- `trilogy run query64.preql`

  ```text

  ```

### `file-not-found`

- `trilogy run query64.preql`

  ```text

  ```
