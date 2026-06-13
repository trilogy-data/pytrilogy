# Trilogy failure analysis — 20260613-141033

- Run `20260613-141033` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 880 | failed: 160 (18%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 79 | 49% |
| `syntax-parse` | 36 | 22% |
| `undefined-concept` | 30 | 19% |
| `cli-misuse` | 5 | 3% |
| `syntax-missing-alias` | 5 | 3% |
| `join-resolution` | 5 | 3% |

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

  ```
- `trilogy file read query14.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query18.preql`

  ```text

  ```
- `trilogy run query18.preql`

  ```text

  ```
- `trilogy run query23.preql duckdb`

  ```text

  ```
- `trilogy run query23.preql duckdb`

  ```text

  ```
- `trilogy run query23.preql duckdb`

  ```text

  ```
- `trilogy run test_freq.preql duckdb`

  ```text

  ```
- `trilogy run test_freq4.preql duckdb`

  ```text

  ```
- `trilogy run query23.preql duckdb`

  ```text

  ```
- `trilogy file read query23.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query24.preql`

  ```text

  ```
- `trilogy run query24.preql`

  ```text

  ```
- `trilogy explore query24.preql`

  ```text

  ```
- `trilogy run query24.preql`

  ```text

  ```
- `trilogy file read query24.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
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
- `trilogy file list / --recursive`

  ```text

  ```
- `trilogy file read raw/physical_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy explore query44.preql --show concepts --regex qualifying`

  ```text
  Cannot render type <class 'trilogy.core.models.author.RowsetItem'>
  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy file read query44.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

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
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run query56.preql`

  ```text

  ```
- `trilogy run query56.preql`

  ```text

  ```
- `trilogy run query56.preql`

  ```text

  ```
- `trilogy file read query56.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file delete query56_debug.preql`

  ```text

  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 58 column 3 (char 2127). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query64.preql`

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
- `trilogy run query66.preql`

  ```text

  ```
- `trilogy file read query69.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/physical_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
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
- `trilogy run query78.preql`

  ```text

  ```
- `trilogy file read query79.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file write -c import raw.catalog_returns as cr;

# Per (returning customer, returning-address state): total return amount including tax
auto cust_sta…billing_customer.address.gmt_offset asc,
    cr.billing_customer.address.location_type asc,
    total_return_amt_inc_tax asc
limit 100; query81.preql`

  ```text
  trilogy file write: `--content` takes a SINGLE string argument. Your args list put 2 separate tokens after --content (treating it like a shell command). In a tool call, pass the entire file body as one string element after --content, with newlines embedded literally — e.g.
    {"args": ["file", "write", "query70.preql", "--content", "import raw.store_sales as store_sales;\n\nselect ..."]}
  Alternatively use `--escapes` with a single-line `\n`-escaped string.
  ```
- `trilogy run query81.preql`

  ```text

  ```
- `trilogy file read raw/physical_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query83.preql`

  ```text

  ```
- `trilogy run query83.preql`

  ```text

  ```
- `trilogy file read query84.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query87.preql`

  ```text

  ```
- `trilogy file read query99.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy run --import raw.all_sales:all_sales select all_sales.date.week_seq, all_sales.date.year, all_sales.date.day_name, sum(all_sales.ext_sales_price) as …hannel in ('WEB','CATALOG') group by all_sales.date.week_seq, all_sales.date.year, all_sales.date.day_name order by all_sales.date.week_seq limit 20;`

  ```text

  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.date.week_seq, all_sales.date.year, sum(all_sales.ext_sales_price) as total where all_sales.cha…les.date.week_seq >= 5270 and all_sales.date.week_seq <= 5400 group by all_sales.date.week_seq, all_sales.date.year order by all_sales.date.week_seq;`

  ```text

  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.date.week_seq, count(*) as cnt where all_sales.channel in ('WEB','CATALOG') and all_sales.date.week_seq >= 5270 and all_sales.date.week_seq <= 5323 order by all_sales.date.week_seq;`

  ```text

  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.date.week_seq where all_sales.channel in ('WEB','CATALOG') and all_sales.date.week_seq in (select all_sales.date.week_seq where all_sales.date.year = 2001);`

  ```text

  ```
- `trilogy file write query02.preql --content import raw.all_sales as all_sales;

# Identify week_seqs that occur in 2001
rowset yr1_weeks <- select
    all_sal… round(yr1.sat_sales / nxt.nxt_sat_sales, 2) as sat_ratio
left join yr1.src_week_seq + 53 = nxt.nxt_week_seq
order by yr1.src_week_seq nulls first
;
`

  ```text

  ```
- `trilogy file write query02.preql --content import raw.all_sales as all_sales;

# Identify week_seqs that occur in 2001
rowset yr1_weeks <- select
    all_sal… round(yr1.sat_sales / nxt.nxt_sat_sales, 2) as sat_ratio
left join yr1.src_week_seq = nxt.nxt_week_seq - 53
order by yr1.src_week_seq nulls first
;
`

  ```text

  ```
- `trilogy file write query02.preql --content import raw.all_sales as all_sales;

# Rowset A: all weekly day-of-week sums for WEB/CATALOG
rowset wk_sums <- 
whe…t join wk_sums ws1
inner join yr1_map.src_ws = ws1.ws
left join wk_sums ws2
inner join yr1_map.tgt_ws = ws2.ws
order by yr1_map.src_ws nulls first
;
`

  ```text

  ```
- `trilogy file write query02.preql --content import raw.all_sales as all_sales;

# Week_seqs that occur in 2001
rowset yr1_weeks <- select
    all_sales.date.w… sums_nxt.sat, 2) as sat_ratio
inner join sums_yr1.ws = ws_map.tgt_ws - 53
inner join ws_map.tgt_ws = sums_nxt.ws
order by sums_yr1.ws nulls first
;
`

  ```text

  ```
- `trilogy file write query05.preql --content import raw/all_sales as sales;

# Stack sales and returns across all three channels
with combined as union(
    # …rns), 0) as total_returns,
    coalesce(sum(combined.net_prof), 0) as net_profit
order by combined.ch nulls last, combined.ent nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query05.preql --content import raw.all_sales as sales;

# Stack sales and returns across all three channels
with combined as union(
    # …rns), 0) as total_returns,
    coalesce(sum(combined.net_prof), 0) as net_profit
order by combined.ch nulls last, combined.ent nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query05.preql --content import raw.all_sales as sales;

# Stack sales and returns across all three channels
with combined as union(
    # …bined.total_returns) as total_returns,
    @rup(combined.net_prof) as net_profit
order by combined.ch nulls last, combined.ent nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query14.preql --content import raw.all_sales as sales;

# Step 1: Find (brand, class, category) combos appearing in ALL three channels dur…sales > overall_avg_sale
order by
    channel nulls first,
    brand_id nulls first,
    class_id nulls first,
    category_id nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query14.preql --content import raw.all_sales as sales;

# Step 1: Find (brand, class, category) combos appearing in ALL three channels dur…sales > overall_avg_sale
order by
    channel nulls first,
    brand_id nulls first,
    class_id nulls first,
    category_id nulls first
limit 100;`

  ```text

  ```
- `trilogy run --import raw.all_sales:sales select sales.channel, count(sales.item.id) as cnt where year(sales.date.date)=2001 and month(sales.date.date)=11 and….item.class_id::string,'|',sales.item.category_id::string) where year(sales.date.date) between 1999 and 2001 having count_distinct(sales.channel)=3);`

  ```text

  ```
- `trilogy run --import raw.all_sales:sales select sales.channel, count(sales.item.id) as cnt where year(sales.date.date)=2001 and month(sales.date.date)=11 and…les.item.class_id::string,'|',sales.item.category_id::string) in (select... well I can't subquery. Let me just check what I get at the channel level.`

  ```text

  ```
- `trilogy run --import raw.all_sales:sales select sales.channel, count(sales.item.id) as cnt where year(sales.date.date)=2001 and month(sales.date.date)=11 and….item.class_id::string,'|',sales.item.category_id::string) where year(sales.date.date) between 1999 and 2001 having count_distinct(sales.channel)=3);`

  ```text

  ```
- `trilogy file write query23.preql --content import raw.physical_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_s…   sum(combined.channel_total) as total_sales
order by last_name asc nulls first, first_name asc nulls first, total_sales asc nulls first
limit 100;
`

  ```text

  ```
- `trilogy file write query23.preql --content import raw.physical_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_s…   sum(combined.channel_total) as total_sales
order by last_name asc nulls first, first_name asc nulls first, total_sales asc nulls first
limit 100;
`

  ```text

  ```
- `trilogy file write query23.preql --content import raw.physical_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_s…   sum(combined.channel_total) as total_sales
order by last_name asc nulls first, first_name asc nulls first, total_sales asc nulls first
limit 100;
`

  ```text

  ```
- `trilogy file write query23.preql --content import raw.physical_sales as store_sales;
import raw.all_sales as all_sales;
import raw.customer as customer;
impo…antity * all_sales.list_price) as total_sales
order by last_name asc nulls first, first_name asc nulls first, total_sales asc nulls first
limit 100;
`

  ```text

  ```
- `trilogy file write query23.preql --content import raw.physical_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_s…   sum(combined.channel_total) as total_sales
order by last_name asc nulls first, first_name asc nulls first, total_sales asc nulls first
limit 100;
`

  ```text

  ```
- `trilogy file write query24.preql --content import raw.physical_sales as physical_sales;
import raw.physical_returns as physical_returns;

# Stage 1: matched …re.peach_total,
    --threshold,
having peach_store.peach_total > threshold
order by peach_store.last_name, peach_store.first_name, peach_store.name;`

  ```text

  ```
- `trilogy file write query24.preql --content import raw.physical_sales as physical_sales;
import raw.physical_returns as physical_returns;

# First stage: per-…each_by_name_store > threshold
order by
    physical_sales.customer.last_name,
    physical_sales.customer.first_name,
    physical_sales.store.name;`

  ```text

  ```
- `trilogy file write query31.preql --content import raw.physical_sales as store_sales;
import raw.web_sales as web_sales;

# Store sales aggregated by county a…y
inner join q1.county = q3.county
having
    web_q2_over_q1 > store_q2_over_q1
    and web_q3_over_q2 > store_q3_over_q2
order by county
limit 1000;`

  ```text

  ```
- `trilogy file write query38.preql --content import raw.physical_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Stack all three ch…os.first_name, all_combos.sale_date;

select
    count(*) as unique_combination_count
where in_store > 0 and in_catalog > 0 and in_web > 0
limit 100;`

  ```text

  ```
- `trilogy file write query51.preql --content import raw.all_sales as sales;

# Web sales daily sums (unit price per item per day)
rowset web_daily as
where sal…store_running_max
having web_running_max > store_running_max
order by combined.item_id asc nulls first, combined.sale_date asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query56_debug.preql --content import raw.all_sales as s;
import raw.item as item;

merge s.item.text_id into item.text_id;

with qualifyin…s total
left join s.item.text_id = qualifying_items.item_code
order by
    total asc nulls first,
    item_code asc nulls first
limit 100
offset 300;`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.physical_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.phys…_count
order by
    agg_1999.product_name,
    agg_1999.store_name,
    agg_2000.line_count,
    agg_1999.sum_wholesale,
    agg_2000.sum_wholesale
;`

  ```text

  ```
- `trilogy file write query66.preql --content import raw.all_sales as sales;

with all_months as
select
    1 as month
union all
select 2 union all select 3 uni…0
  end) as monthly_net
order by
  sales.warehouse.name asc nulls first,
  sales.date.year asc nulls first,
  sales.date.month_of_year asc
limit 100;`

  ```text

  ```
- `trilogy file write query66.preql --content import raw.all_sales as sales;

with all_months as union(
  (select 1 as m),
  (select 2 as m),
  (select 3 as m),…ly_data.monthly_net, 0) as monthly_net
order by
  groups.warehouse_name asc nulls first,
  groups.year asc nulls first,
  all_months.m asc
limit 100;`

  ```text

  ```
- `trilogy file write query76.preql --content import raw.physical_sales as store_sales;
import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_s…al_ext_sales_price
order by
    combined.channel,
    combined.missing_ref,
    combined.year,
    combined.quarter,
    combined.category
limit 100;`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales select all_sales.channel, all_sales.channel_dim_text_id, all_sales.channel_dim_id group by all_sales.channel, all_sales.channel_dim_text_id, all_sales.channel_dim_id having all_sales.channel = 'STORE' limit 10;`

  ```text

  ```
- `trilogy file write query80.preql --content import raw.all_sales as all_sales;

where all_sales.date.date between '2000-08-23'::date and '2000-09-22'::date
  …WEB' then concat('web_site', all_sales.channel_dim_text_id)
    end)
order by all_sales.channel nulls first, outlet_identifier nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query83.preql --content import raw/all_sales as sales;

select
    sales.item.text_id as item_code,
    sum(sales.return_quantity ? sales.…d sales.return_date.week_seq in (5244, 5257, 5264)) > 0
order by
    sales.item.text_id nulls first,
    store_return_quantity nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query83.preql --content import raw.all_sales as sales;

select
    sales.item.text_id as item_code,
    sum(sales.return_quantity ? sales.…d sales.return_date.week_seq in (5244, 5257, 5264)) > 0
order by
    sales.item.text_id nulls first,
    store_return_quantity nulls first
limit 100;`

  ```text

  ```
- `trilogy run --import raw/web_sales:ws with q as where ws.ship_date.date between '1999-02-01'::date and '1999-04-02'::date and ws.ship_address.state = 'IL' an…t, bool_or(ws.is_returned) by ws.order_number as has_return having distinct_warehouse_cnt > 1 and has_return = true select q.ws_order_number limit 5;`

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
- `trilogy run query02.preql`

  ```text

  ```
- `trilogy run query18.preql`

  ```text

  ```
- `trilogy run query23.preql duckdb`

  ```text

  ```
- `trilogy run query24.preql`

  ```text

  ```
- `trilogy run query24.preql`

  ```text

  ```
- `trilogy run query24.preql`

  ```text

  ```
- `trilogy run query24.preql`

  ```text

  ```
- `trilogy run query31.preql`

  ```text

  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy explore query44.preql`

  ```text

  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy run query51.preql`

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
- `trilogy run query66.preql`

  ```text

  ```
- `trilogy run query66.preql`

  ```text

  ```
- `trilogy run query67.preql`

  ```text

  ```
- `trilogy run query67.preql`

  ```text

  ```
- `trilogy run query67.preql`

  ```text

  ```
- `trilogy explore query67.preql`

  ```text

  ```
- `trilogy run query67.preql`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales select all_sales.channel, date.date, all_sales.channel_dim_text_id, all_sales.net_profit, all_sales.return_amount limit 5;`

  ```text

  ```
- `trilogy run query95.preql`

  ```text

  ```
- `trilogy run query95.preql`

  ```text

  ```

### `cli-misuse`

- `trilogy run --format json --import raw.all_sales:all_sales select all_sales.date.week_seq, all_sales.date.year, sum(all_sales.ext_sales_price) as total where all_sales.channel in ('WEB','CATALOG') and all_sales.date.week_seq >= 5270 and all_sales.date.week_seq <= 5400 order by all_sales.date.week_seq;`

  ```text
  'json' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy run query56.preql --format json`

  ```text
  '--format' is not a valid dialect. The dialect argument comes after the input file and any options.
    Try: trilogy run <input> [<dialect>]
  ```
- `trilogy run query66.preql --format json`

  ```text
  '--format' is not a valid dialect. The dialect argument comes after the input file and any options.
    Try: trilogy run <input> [<dialect>]
  ```
- `trilogy file write --content import raw.catalog_returns as cr;

# Per (returning customer, returning-address state): total return amount including tax
auto c…y asc,
    cr.billing_customer.address.gmt_offset asc,
    cr.billing_customer.address.location_type asc,
    total_return_amt_inc_tax asc
limit 100;`

  ```text
  Missing argument 'PATH'.
  ```

### `syntax-missing-alias`

- `trilogy file write query02.preql --content import raw.all_sales as all_sales;

# Identify week_seqs that occur in 2001 (for filtering later)
rowset yr1_weeks…er (order by all_sales.date.week_seq), 2) as sat_ratio
having all_sales.date.week_seq in yr1_weeks.ws
order by all_sales.date.week_seq nulls first
;
`

  ```text

  ```
- `trilogy run --import raw.all_sales:sales select count(sales.channel) by sales.channel where year(sales.date.date) between 1999 and 2001;`

  ```text

  ```
- `trilogy file write query41.preql`

  ```text

  ```
- `trilogy file write query44.preql --content import raw.physical_sales as sales;
import raw.item as item;

# Average net profit for store id=1 where address is… rank(qualifying_items.item_id) over (order by qualifying_items.avg_profit asc) as rank;

# Count best
select count(best_performers.item_id) limit 1;`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales select distinct all_sales.channel, all_sales.channel_dim_text_id limit 20;`

  ```text

  ```

### `join-resolution`

- `trilogy run --import raw.physical_sales:store_sales --import raw.item:item select count(store_sales.item.id) as cnt, store_sales.item.id, substring(item.desc,1,30) as dp having cnt > 4 limit 5; duckdb`

  ```text

  ```
- `trilogy run --import raw.physical_sales:store_sales --import raw.item:item --import raw.date:date where store_sales.date.year between 2000 and 2003 select substring(item.desc, 1, 30) as dp, item.id, count(store_sales.item.id) as pair_count having pair_count > 4 limit 10; duckdb`

  ```text

  ```
- `trilogy run query31.preql`

  ```text

  ```
- `trilogy run query51.preql`

  ```text

  ```
- `trilogy run query64.preql`

  ```text

  ```
