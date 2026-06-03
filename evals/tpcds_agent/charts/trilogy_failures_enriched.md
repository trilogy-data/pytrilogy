# Trilogy failure analysis — 20260603-134731

- Run `20260603-134730_enriched` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 229 | failed: 43 (19%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 27 | 63% |
| `other` | 9 | 21% |
| `undefined-concept` | 3 | 7% |
| `cli-misuse` | 2 | 5% |
| `syntax-missing-alias` | 1 | 2% |
| `type-error` | 1 | 2% |

## Detail

### `syntax-parse`

- `trilogy file write query01.preql --content import raw.physical_returns as store_returns;

# Total returns per billing customer per store in 2000 at TN stores…billing_customer.text_id
where store_returns.billing_customer.id in qualifying.cust_id
order by store_returns.billing_customer.text_id asc
limit 100;`

  ```text

  ```
- `trilogy run --import raw.all_sales:s select s.sales_channel, count(s.order_id) as cnt group by 1;`

  ```text

  ```
- `trilogy run --import raw.all_sales:s select s.date.week_seq, s.date.year, count(s.order_id) as cnt where s.sales_channel in ('WEB','CATALOG') and s.date.year in (2001,2002) group by 1,2 order by 1;`

  ```text

  ```
- `trilogy run --import raw.all_sales:s select s.date.week_seq, min(s.date.year) as min_y, max(s.date.year) as max_y, count(s.order_id) as cnt where s.sales_channel in ('WEB','CATALOG') group by 1 order by 1;`

  ```text

  ```
- `trilogy file write query02.preql --content import raw.all_sales as s;

# Only WEB and CATALOG sales
where s.sales_channel in ('WEB', 'CATALOG')

# Get week s…ice ? (s.date.week_seq + 53) in year_2001_weeks and s.date.day_of_week = 0),
        2
    ) as Sunday
order by s.date.week_seq nulls first
limit 50;`

  ```text

  ```
- `trilogy file write query02.preql --content import raw.all_sales as s;

# Week sequences that occur in 2001
auto year_2001_weeks <- s.date.week_seq ? s.date.y…s.date.week_seq,
    round(cur_day_total / nullif(next_day_total, 0), 2) as Sunday
where s.date.day_of_week = 0
order by s.date.week_seq nulls first;`

  ```text

  ```
- `trilogy file write query02.preql --content import raw.all_sales as s;

# Week sequences that occur in 2001
auto year_2001_weeks <- s.date.week_seq ? s.date.y….day_of_week = 0

select
    s.date.week_seq,
    round(cur_day_total / nullif(next_day_total, 0), 2) as Sunday
order by s.date.week_seq nulls first;`

  ```text

  ```
- `trilogy file write query02.preql --content import raw.all_sales as s;

# Week sequences that occur in 2001
auto year_2001_weeks <- s.date.week_seq ? s.date.y….day_of_week = 0

select
    s.date.week_seq,
    round(cur_day_total / nullif(next_day_total, 0), 2) as Sunday
order by s.date.week_seq nulls first;`

  ```text

  ```
- `trilogy file write query02.preql --content import raw.all_sales as s;

auto year_2001_weeks <- s.date.week_seq ? s.date.year = 2001;

# First select: this ye…not null and nxt_day_sales is not null and nxt_day_sales > 0 then round(day_sales / nxt_day_sales, 2) end -> ratio
order by ws nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query02.preql --content import raw.all_sales as s;

auto year_2001_weeks <- s.date.week_seq ? s.date.year = 2001;

# First select: this ye…not null and nxt_day_sales is not null and nxt_day_sales > 0 then round(day_sales / nxt_day_sales, 2) end -> ratio
order by ws nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query02.preql --content import raw.all_sales as s;

# Week sequences that occur in 2001
auto year_2001_weeks <- s.date.week_seq ? s.date.y…  @ratio_col(3, Wednesday),
    @ratio_col(4, Thursday),
    @ratio_col(5, Friday),
    @ratio_col(6, Saturday)
order by s.date.week_seq nulls first;`

  ```text

  ```
- `trilogy file write query02.preql --content import raw.all_sales as s;

# Week sequences that occur in 2001
auto year_2001_weeks <- s.date.week_seq ? s.date.y…       all_wkdy_sales / nullif(nxt_yr_sales_concept, 0),
        2
    ) as Sunday
where s.date.day_of_week = 0
order by s.date.week_seq nulls first;`

  ```text

  ```
- `trilogy run --import raw.all_sales:s select s.date.day_of_week, count(s.date.week_seq) as num_weeks, min(s.date.week_seq) as min_ws, max(s.date.week_seq) as max_ws where s.sales_channel in ('WEB','CATALOG') group by 1 order by 1;`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales select --all_sales.sales_channel as s_chan, --coalesce(all_sales.channel_dim_id.channel_dim_text_id, '') as s_en…derive coalesce(s_gross, 0) as gross_sales, coalesce(r_returns, 0) as tot_returns, coalesce(s_gross, 0) - coalesce(r_loss, 0) as net_profit limit 10;`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales select --all_sales.sales_channel as s_chan, --all_sales.channel_dim_id.channel_dim_text_id as s_entity, sum(all_…derive coalesce(s_gross, 0) as gross_sales, coalesce(r_returns, 0) as tot_returns, coalesce(s_gross, 0) - coalesce(r_loss, 0) as net_profit limit 10;`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales select --all_sales.sales_channel as s_chan, --all_sales.channel_dim_id.channel_dim_text_id as s_entity, sum(all_…ign chan: s_chan, r_chan align entity: s_entity, r_entity derive coalesce(s_gross, 0) as gross_sales, coalesce(r_returns, 0) as tot_returns limit 10;`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales select --all_sales.sales_channel as s_chan, --all_sales.channel_dim_id.channel_dim_text_id as s_entity, sum(all_…ign chan: s_chan, r_chan align entity: s_entity, r_entity derive coalesce(s_gross, 0) as gross_sales, coalesce(r_returns, 0) as tot_returns limit 10;`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales select --all_sales.sales_channel as s_chan, --all_sales.channel_dim_id.channel_dim_text_id as s_entity, sum(all_…_returns align entity: s_chan, r_chan, s_entity, r_entity derive coalesce(s_gross, 0) as gross_sales, coalesce(r_returns, 0) as tot_returns limit 10;`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales select --all_sales.sales_channel as s_chan, --all_sales.channel_dim_id.channel_dim_text_id as s_entity, sum(all_…ign entity: s_chan, r_chan and entity: s_entity, r_entity derive coalesce(s_gross, 0) as gross_sales, coalesce(r_returns, 0) as tot_returns limit 10;`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales select --concat(all_sales.sales_channel, all_sales.channel_dim_id.channel_dim_text_id) as s_key, sum(all_sales.e…sales.return_amount) as r_returns align key: s_key, r_key derive coalesce(s_gross, 0) as gross_sales, coalesce(r_returns, 0) as tot_returns limit 10;`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales select --all_sales.sales_channel as s_chan, --all_sales.channel_dim_id.channel_dim_text_id as s_ent, sum(all_sal…s r_returns align key1: s_chan, r_chan key2: s_ent, r_ent derive coalesce(s_gross, 0) as gross_sales, coalesce(r_returns, 0) as tot_returns limit 10;`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales select --all_sales.sales_channel as s_chan, --all_sales.channel_dim_id.channel_dim_text_id as s_ent, sum(all_sal…returns align key1: s_chan, r_chan and key2: s_ent, r_ent derive coalesce(s_gross, 0) as gross_sales, coalesce(r_returns, 0) as tot_returns limit 10;`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales select --concat(all_sales.sales_channel, '|', coalesce(all_sales.channel_dim_id.channel_dim_text_id, '')) as s_k…sales.return_amount) as r_returns align key: s_key, r_key derive coalesce(s_gross, 0) as gross_sales, coalesce(r_returns, 0) as tot_returns limit 10;`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales select --all_sales.sales_channel || '|' || all_sales.channel_dim_id.channel_dim_text_id as s_key, sum(all_sales.…sales.return_amount) as r_returns align key: s_key, r_key derive coalesce(s_gross, 0) as gross_sales, coalesce(r_returns, 0) as tot_returns limit 10;`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales select --s_key, sum(all_sales.ext_sales_price) as s_gross merge select --concat(all_sales.sales_channel, all_sal…sales.return_amount) as r_returns align key: s_key, r_key derive coalesce(s_gross, 0) as gross_sales, coalesce(r_returns, 0) as tot_returns limit 10;`

  ```text

  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

# Sales aggregates by channel entity
auto s_key <- concat(all_sales.sales_chan…sce(r_loss, 0) -> net_profit,
    coalesce(r_amount, 0) -> tot_returns
having 
    gross_sales != 0 or tot_returns != 0 or net_profit != 0
limit 100;`

  ```text

  ```
- `trilogy file write query06.preql --content import raw/physical_sales as store_sales;

# Average current price per category (distinct items, unweighted)
auto …e,
    count(store_sales.line_item) as qualifying_line_items
order by
    qualifying_line_items asc nulls first,
    state asc nulls first
limit 100;`

  ```text

  ```

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
- `trilogy run --import raw.all_sales:s select s.date.week_seq, s.date.day_of_week, sum(s.ext_sales_price) as total where s.sales_channel in ('WEB','CATALOG') and (s.date.week_seq, s.date.day_of_week) in ((5270,0),(5270+53,0),(5324,0)) order by 1,2;`

  ```text

  ```
- `trilogy run query02.preql`

  ```text

  ```
- `trilogy run query02.preql`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales select --all_sales.sales_channel || all_sales.channel_dim_id.channel_dim_text_id as s_key, sum(all_sales.ext_sal…sales.return_amount) as r_returns align key: s_key, r_key derive coalesce(s_gross, 0) -> gross_sales, coalesce(r_returns, 0) -> tot_returns limit 10;`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales select case when 'STORE' = 'STORE' then 'store channel' else 'other' end as test limit 1;`

  ```text

  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text

  ```

### `undefined-concept`

- `trilogy run query04.preql`

  ```text

  ```
- `trilogy run query04.preql`

  ```text

  ```
- `trilogy run query09.preql`

  ```text

  ```

### `cli-misuse`

- `trilogy run --import raw/all_sales:all_sales --format json select all_sales.sales_channel, coalesce(all_sales.channel_dim_id.channel_dim_text_id, all_sales.r…-08-23'::date and '2000-09-06'::date and all_sales.return_channel_dim_id is not null) as net_profit where all_sales.sales_channel = 'STORE' limit 20;`

  ```text
  'json' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy run --import raw/all_sales:all_sales --format rich select --all_sales.sales_channel as s_chan, --all_sales.channel_dim_id.channel_dim_text_id as s_en…les.return_amount) as r_returns align key: s_chan, r_chan derive coalesce(s_gross, 0) as gross_sales, coalesce(r_returns, 0) as tot_returns limit 10;`

  ```text
  'rich' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw/all_sales:all_sales select all_sales.sales_channel, count(all_sales.order_id), sum(all_sales.net_profit) limit 10;`

  ```text

  ```

### `type-error`

- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text

  ```
