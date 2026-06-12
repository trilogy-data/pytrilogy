# Trilogy failure analysis — 20260612-180707

- Run `20260612-180707_enriched` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 2007 | failed: 335 (17%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 161 | 48% |
| `syntax-parse` | 66 | 20% |
| `undefined-concept` | 59 | 18% |
| `cli-misuse` | 19 | 6% |
| `syntax-missing-alias` | 11 | 3% |
| `join-resolution` | 11 | 3% |
| `file-not-found` | 8 | 2% |

## Detail

### `other`

- `trilogy run query01.preql`

  ```text

  ```
- `trilogy file read query01.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
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
- `trilogy file read query02.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query04.preql`

  ```text

  ```
- `trilogy run query04.preql`

  ```text

  ```
- `trilogy run query04.preql`

  ```text

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
- `trilogy run query05.preql`

  ```text

  ```
- `trilogy run query05.preql`

  ```text

  ```
- `trilogy run query05.preql`

  ```text

  ```
- `trilogy file read test_avg5.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run test_item_avg2.preql`

  ```text

  ```
- `trilogy run test_avg_merged2.preql`

  ```text

  ```
- `trilogy file read query09.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query10.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query11.preql`

  ```text

  ```
- `trilogy file read query11.preql`

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
- `trilogy file read query14.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/physical_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query20.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query23.preql`

  ```text

  ```
- `trilogy run --import raw/physical_sales:ps 
# Global max lifetime total, excluding null customers
auto global_max_sales <- sum((ps.quantity * ps.sales_price)… --max_of_max
having
  sum((ps.quantity * ps.sales_price) ? ps.customer.id is not null) > 0.5 * max_of_max
;

select best_custs.customer_id
limit 5;
`

  ```text

  ```
- `trilogy file read query23.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query24.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query25.preql`

  ```text

  ```
- `trilogy run --import raw.physical_sales:ps --import raw.catalog_sales:cs select count(ps.row_counter) as cnt where ps.date.year = 2001 and ps.date.month_of_y…m.id and ps.customer.id in cs.bill_customer.id and cs.sold_date.year = 2001 and cs.sold_date.month_of_year >= 4 and cs.sold_date.month_of_year <= 10;`

  ```text

  ```
- `trilogy run query27.preql`

  ```text

  ```
- `trilogy file read raw/physical_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe store_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run --import raw.physical_sales:sales --import raw.customer_demographic:custdemo select sales.ticket_number, custdemo.marital_status limit 5;`

  ```text

  ```
- `trilogy run query28.preql duckdb`

  ```text

  ```
- `trilogy run query29.preql`

  ```text

  ```
- `trilogy run query30.preql`

  ```text

  ```
- `trilogy file read test_avg.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query36.preql`

  ```text

  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 37 column 13 (char 1216). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 47 column 12 (char 1880). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file read query38.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query38_check2.preql`

  ```text

  ```
- `trilogy run query38_verify_count2.preql`

  ```text

  ```
- `trilogy run query38_verify_count2.preql`

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
- `trilogy file read query41.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe web_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run query47.preql`

  ```text

  ```
- `trilogy file read raw/customer_demographic.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query49.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 50 column 12 (char 2119). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query51.preql`

  ```text

  ```
- `trilogy run query51.preql`

  ```text

  ```
- `trilogy file read raw/physical_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query53.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 41 column 12 (char 1516). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query54.preql`

  ```text

  ```
- `trilogy run query54.preql`

  ```text

  ```
- `trilogy run query54.preql`

  ```text

  ```
- `trilogy run query54.preql`

  ```text

  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query57.preql`

  ```text

  ```
- `trilogy run query57.preql`

  ```text

  ```
- `trilogy file read query57.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query58.preql`

  ```text

  ```
- `trilogy run --import raw.date:d select d.year, d.week_seq, count(d.id) as days where d.week_seq = 5375;`

  ```text

  ```
- `trilogy run --import raw.date:d select d.year, d.week_seq, d.day_name where d.week_seq >= 5374 and d.week_seq <= 5376 and d.year in (2001, 2002);`

  ```text

  ```
- `trilogy run --import raw.date:d select d.year, d.week_seq, d.day_name where d.week_seq >= 5270 and d.week_seq <= 5330 limit 20;`

  ```text

  ```
- `trilogy file read query59.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query60.preql`

  ```text

  ```
- `trilogy run query60.preql`

  ```text

  ```
- `trilogy file read query61.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run -`

  ```text

  ```
- `trilogy run -`

  ```text

  ```
- `trilogy run -`

  ```text

  ```
- `trilogy run -`

  ```text

  ```
- `trilogy run -`

  ```text

  ```
- `trilogy run -`

  ```text

  ```
- `trilogy run -`

  ```text

  ```
- `trilogy run -`

  ```text

  ```
- `trilogy run -`

  ```text

  ```
- `trilogy run -`

  ```text

  ```
- `trilogy run -`

  ```text

  ```
- `trilogy run -`

  ```text

  ```
- `trilogy run -`

  ```text

  ```
- `trilogy run -`

  ```text

  ```
- `trilogy run -`

  ```text

  ```
- `trilogy run -`

  ```text

  ```
- `trilogy run -`

  ```text

  ```
- `trilogy run -`

  ```text

  ```
- `trilogy run -`

  ```text

  ```
- `trilogy run -`

  ```text

  ```
- `trilogy run -`

  ```text

  ```
- `trilogy run -`

  ```text

  ```
- `trilogy run -`

  ```text

  ```
- `trilogy run -`

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
- `trilogy file read raw/physical_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query70.preql`

  ```text

  ```
- `trilogy run query70.preql`

  ```text

  ```
- `trilogy run query70.preql`

  ```text

  ```
- `trilogy run query70.preql`

  ```text

  ```
- `trilogy run query70.preql`

  ```text

  ```
- `trilogy run query70.preql`

  ```text

  ```
- `trilogy run query70.preql`

  ```text

  ```
- `trilogy file read query70.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query73.preql`

  ```text

  ```
- `trilogy file read query73.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query75.preql`

  ```text

  ```
- `trilogy run query75.preql`

  ```text

  ```
- `trilogy file read raw/physical_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy agent-info syntax example rollup`

  ```text

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
- `trilogy run query78.preql --import raw.all_sales:s select s.date.year, count(s.row_one) as cnt where s.date.year = 2000 and s.sales_channel = 'STORE' and s.is_returned = false and s.billing_customer.id is not null;`

  ```text

  ```
- `trilogy file read query78.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query79.preql`

  ```text

  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query81.preql`

  ```text

  ```
- `trilogy run /tmp/find_weeks.preql`

  ```text

  ```
- `trilogy run /tmp/find_weeks.preql duckdb`

  ```text

  ```
- `trilogy file read query83.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.physical_returns:r --import raw.customer:c select r.customer_demographic.id, r.ticket_number, c.text_id, c.first_name, c.last_name, …hics.id, c.household_demographic.income_band.lower_bound, c.household_demographic.income_band.upper_bound where c.address.city = 'Edgewood' limit 10;`

  ```text

  ```
- `trilogy file read query84.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query85.preql`

  ```text

  ```
- `trilogy run query87v2.preql`

  ```text

  ```
- `trilogy run query87v3.preql`

  ```text

  ```
- `trilogy file read query87.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query89.preql`

  ```text

  ```
- `trilogy run query89.preql`

  ```text

  ```
- `trilogy file read query89.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query91.preql`

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
- `trilogy file read query95.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query99.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy file write query01.preql --content import raw.physical_returns as r;

# Total returns per billing customer per store
auto cust_store_returns <- sum(r…
    cust_store_returns > 1.2 * store_avg_returns
;

select
    qualified.cid as r_billing_customer_text_id
order by
    qualified.cid asc
limit 100;`

  ```text

  ```
- `trilogy run --import raw.all_sales:all_sales --import raw.date:date select all_sales.sales_channel, date.year, date.week_seq, date.day_of_week, sum(all_sales.ext_sales_price) as total where all_sales.sales_channel in ('WEB','CATALOG') group by 1,2,3,4 order by 3,4 limit 20;`

  ```text

  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.date.week_seq, all_sales.date.year, sum(all_sales.ext_sales_price) as total where all_sales.sal…sales.date.year in (2001, 2002) group by all_sales.date.week_seq, all_sales.date.year order by all_sales.date.week_seq, all_sales.date.year limit 20;`

  ```text

  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.date.week_seq, all_sales.date.year, count(all_sales.date.id) as cnt where all_sales.sales_channel in ('WEB','CATALOG') and all_sales.date.year = 2001 group by all_sales.date.week_seq, all_sales.date.year order by all_sales.date.week_seq;`

  ```text

  ```
- `trilogy file write query02.preql --content import raw.all_sales as all_sales;

# Filter to web and catalog only
where all_sales.sales_channel in ('WEB', 'CAT… same-day value (53 weeks ahead)
select
    all_sales.date.week_seq,
    all_sales.date.day_of_week,
    sum(all_sales.ext_sales_price) as dow_sales
`

  ```text

  ```
- `trilogy file write query02.preql --content import raw.all_sales as all_sales;

# Step 1: Compute sum of ext_sales_price by week_seq and day_of_week
with week…tio_for_dow(weekly_dow.dow_sales ? weekly_dow.day_of_week = 6, weekly_dow.week_seq) as saturday_ratio
order by weekly_dow.week_seq asc nulls first
;
`

  ```text

  ```
- `trilogy file write query02.preql --content import raw.all_sales as all_sales;

# Step 1: Compute sum of ext_sales_price by week_seq and day_of_week
with week….date.week_seq ? all_sales.date.year = 2001
)
select
    weekly_dow.week_seq,
    @ratio_for_dow(weekly_dow.dow_sales, weekly_dow.week_seq) as ratio
`

  ```text

  ```
- `trilogy file write query02.preql -e -c import raw.all_sales as all_sales;

# Week sequences that touch 2001
auto ws_2001 <- all_sales.date.week_seq ? all_sal…ith_next.next_year_sales, 2) as ratio,
    with_next.day_of_week
order by with_next.week_seq asc nulls first, with_next.day_of_week asc nulls first
;`

  ```text

  ```
- `trilogy file write query02.preql -e -c import raw.all_sales as all_sales;

# Week sequences that touch 2001
auto ws_2001 <- all_sales.date.week_seq ? all_sal…   round(all_sat / lead(all_sat, 53) over (order by all_sales.date.week_seq), 2) as saturday_ratio
order by all_sales.date.week_seq asc nulls first
;`

  ```text

  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.date.week_seq, sum(all_sales.ext_sales_price) as total where all_sales.sales_channel in ('WEB',…LOG') and all_sales.date.week_seq in (5285, 5285+53, 5300, 5300+53, 5307, 5307+53) group by all_sales.date.week_seq order by all_sales.date.week_seq;`

  ```text

  ```
- `trilogy file write query04.preql --content # Query 4: Customers with positive value in all 3 channels (store, catalog, web) in 2001 and 2002,
# where catalog…b.item.wholesale_cost * web.quantity - web.ext_discount_amount + web.ext_sales_price) as annual_val
;

# Simple test: just explore
select * limit 1;
`

  ```text

  ```
- `trilogy file write query04.preql --content # Query 4: Customers with positive value in all 3 channels (store, catalog, web)
# in 2001 and 2002, where catalog…ag
where combined.s_yr = 2001
  and combined.c_yr = 2001
  and combined.w_yr = 2001
  -- and also have 2002 versions (need to join again)
limit 100;
`

  ```text

  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

# Sales data: aggregate by channel and entity
with sales_data as
where all_sal…um(combined.net_profit), 0) - coalesce(sum(combined.return_net_loss), 0) as net_profit
order by combined.channel, combined.entity_text_id
limit 100;
`

  ```text

  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

# Sales: aggregate gross sales and net profit by channel and entity
with sales…(sum(combined.net_profit), 0) - coalesce(sum(combined.ret_net_loss), 0) as net_profit
order by combined.sort_ch asc, combined.sort_id asc
limit 100;
`

  ```text

  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

with combined as union(
  (where all_sales.date.date between '2000-08-23'::dat…  @rollup_metrics(combined.net_profit) - @rollup_metrics(combined.ret_net_loss) as net_profit
order by combined.sort_ch, combined.sort_id
limit 100;
`

  ```text

  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

with combined as union(
  (where all_sales.date.date between '2000-08-23'::dat…  @rollup_metrics(combined.net_profit) - @rollup_metrics(combined.ret_net_loss) as net_profit
order by combined.sort_ch, combined.sort_id
limit 100;
`

  ```text

  ```
- `trilogy file write query06.preql --content import raw.physical_sales as sales;
import raw.item as it;

# Per-category average of distinct item prices (each i…
  qualifying_line_items asc nulls first,
  state asc nulls first
limit 100
# Scoped join to bridge the two imports
inner join sales.item.id = it.id;`

  ```text

  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.sales_channel, all_sales.item.brand_id, all_sales.item.class_id, all_sales.item.category_id, count(all_sales.order_id) as cnt where all_sales.date.year between 1999 and 2001 group by 1,2,3,4 limit 10;`

  ```text

  ```
- `trilogy file write query14.preql --content import raw.all_sales as all_sales;

# Overall average sale value across all three channels in 1999-2001
# This is …
    all_sales.item.brand_id asc nulls first,
    all_sales.item.class_id asc nulls first,
    all_sales.item.category_id asc nulls first
limit 100;
`

  ```text

  ```
- `trilogy file write query14.preql --content import raw.all_sales as all_sales;

# Overall average sale value across all three channels in 1999-2001
# This is …
    all_sales.item.brand_id asc nulls first,
    all_sales.item.class_id asc nulls first,
    all_sales.item.category_id asc nulls first
limit 100;
`

  ```text

  ```
- `trilogy run --import raw.catalog_store_returns:csr select csr.store_sale_date.quarter_name, count(csr.ticket_number) as cnt group by 1 order by 1 limit 20;`

  ```text

  ```
- `trilogy file write query18.preql --content import raw.catalog_sales as cs;

auto avg_qty <- avg(cs.quantity::numeric(12,2)) by rollup item.text_id, cs.bill_c…ls first,
    item.text_id asc nulls first,
    grp_country asc nulls first,
    grp_state asc nulls first,
    grp_county asc nulls first
limit 100;`

  ```text

  ```
- `trilogy run --import raw/physical_sales:ps --import raw/catalog_sales:cs --import raw/web_sales:ws --import raw/all_sales:as 
# Step 1: Frequent items - item…
select
  substring(ps.item.desc, 1, 30) as desc_prefix,
  count(ps.item.id, ps.date.date) as pair_count
having
  pair_count > 4
;
select *
limit 5;
`

  ```text

  ```
- `trilogy run --import raw/all_sales:al 
select
  al.billing_customer.last_name as last_name,
  al.billing_customer.first_name as first_name,
  count(al.billin…
where
  year(al.date.date) = 2000
  and month(al.date.date) = 2
  and al.sales_channel in ('CATALOG', 'WEB')
group by 1, 2
having cnt > 1
limit 10;
`

  ```text

  ```
- `trilogy file write query25.preql --content import raw.physical_sales as ps;
import raw.catalog_sales as cs;

# Rowset: store sales in April 2001 with store r…e_data.item_sid = catalog_data.item_sid
order by store_data.item_code, store_data.item_desc, store_data.store_code, store_data.store_name
limit 100;
`

  ```text

  ```
- `trilogy run --import raw.physical_sales:ps --import raw.catalog_sales:cs select count(ps.row_counter) as cnt where ps.date.year = 2001 and ps.date.month_of_y….customer.id in (select cs.bill_customer.id where cs.sold_date.year = 2001 and cs.sold_date.month_of_year >= 4 and cs.sold_date.month_of_year <= 10);`

  ```text

  ```
- `trilogy run --import raw.catalog_store_returns:csr select store_sale_date.year, store_sale_date.month_of_year, count(csr.ticket_number) as cnt where store_sa…store_return_date.month_of_year >= 4 and store_return_date.month_of_year <= 10 group by store_sale_date.year, store_sale_date.month_of_year limit 10;`

  ```text

  ```
- `trilogy file write query25.preql --content import raw.physical_sales as ps;
import raw.catalog_sales as cs;

with store_data as
where ps.date.year = 2001
  a…ct
    count(*) as match_count
inner join store_data.cust_id = catalog_data.cust_id
inner join store_data.item_sid = catalog_data.item_sid
limit 10;
`

  ```text

  ```
- `trilogy run --import raw.physical_sales:ps --import raw.catalog_sales:cs select count(*) as cnt inner join ps.customer.id = cs.bill_customer.id inner join ps…<= 10 and cs.sold_date.year = 2001 and cs.sold_date.month_of_year >= 4 and cs.sold_date.month_of_year <= 10 and ps.item.text_id = 'AAAAAAAANMADAAAA';`

  ```text

  ```
- `trilogy file write query30.preql --content import raw.web_returns as wr;

# Per-customer per-return-origin-state total return amount in 2002
auto cust_state_…billing_customer.last_review_date asc nulls first,
    -- wr.return_address.state,  # selection field, not output
    -- state_avg_ret_amt
limit 100;`

  ```text

  ```
- `trilogy file write query35.preql --content import raw.customer as customer;

# Store sale customers in Q1-Q3 2002
with store_cust as
import raw.physical_sale…st,
    customer.demographics.employed_dependent_count asc nulls first,
    customer.demographics.college_dependent_count asc nulls first
limit 100;
`

  ```text

  ```
- `trilogy file write query38.preql`

  ```text

  ```
- `trilogy run --import raw.physical_sales:ss --import raw.catalog_sales:cs --import raw.web_sales:ws select ss.customer.last_name, ss.customer.first_name, ss.date.date, 'store' as ch limit 5 where ss.date.year = 2000;`

  ```text

  ```
- `trilogy file write query38_verify_simple.preql`

  ```text

  ```
- `trilogy file write query38_verify_count2.preql`

  ```text

  ```
- `trilogy file write query51.preql --content import raw.all_sales as s;

web_daily <- sum(s.sales_price ? s.sales_channel = 'WEB') by s.item.text_id, s.date.da…sc)) over (partition by s.item.text_id order by s.date.date asc) as store_running_max
order by item asc nulls first, date asc nulls first
limit 100;
`

  ```text

  ```
- `trilogy file write query54.preql --content import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sales;
import raw.physical_sales as physica…nd(total_price / 50) * 50 as segment_times_50
order by segment nulls first,
         customer_count nulls first,
         segment_times_50
limit 100;`

  ```text

  ```
- `trilogy file write query54.preql --content import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sales;
import raw.physical_sales as physica…t_sales_price) / 50) * 50 as segment_times_50
order by segment nulls first,
         customer_count nulls first,
         segment_times_50
limit 100;`

  ```text

  ```
- `trilogy file write query56.preql --content import raw.all_sales as sales;

# Distinct qualifying item codes (items with the right colors)
rowset qualifying_i…join qualifying_items.item_code = feb_sales.item_code
order by feb_sales.total asc nulls first, qualifying_items.item_code asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query57.preql --content import raw.catalog_sales as cs;

# Monthly totals per (category, brand, call_center, year, month_of_year)
with mon…y_sales asc nulls first,
    this_month_total asc nulls first,
    prior_month_total asc nulls first,
    next_month_total asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query59.preql --content import raw.physical_sales as sales;
import raw.store as store;

# Day-of-week names mapping
# date.day_of_week: 0=…
order by this_year.store_name asc nulls first,
         this_year.store_code asc nulls first,
         this_year.week_seq asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query59.preql --content import raw.physical_sales as sales;
import raw.store as store;

# Filtered aggregate per day-of-week: sum of unit …
order by this_year.store_name asc nulls first,
         this_year.store_code asc nulls first,
         this_year.week_seq asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query59.preql --content import raw.physical_sales as sales;
import raw.store as store;

# Filtered aggregate per day-of-week: sum of unit …
order by this_year.store_name asc nulls first,
         this_year.store_code asc nulls first,
         this_year.week_seq asc nulls first
limit 100;`

  ```text

  ```
- `trilogy run --import raw.date:d select d.year, d.week_seq, count(d.id) as days where d.week_seq = 5375 group by d.year, d.week_seq;`

  ```text

  ```
- `trilogy run -`

  ```text

  ```
- `trilogy run --import raw/catalog_sales:cs select cs.item.id, sum(cs.ext_list_price) as total_list group by cs.item.id limit 5;`

  ```text

  ```
- `trilogy run -`

  ```text

  ```
- `trilogy run -`

  ```text

  ```
- `trilogy run -`

  ```text

  ```
- `trilogy run -`

  ```text

  ```
- `trilogy run -`

  ```text

  ```
- `trilogy file write query69.preql --content import raw/physical_sales as store;
import raw/catalog_sales as catalog;
import raw/web_sales as web;

# Qualifyin…  store_customers.marital_status,
  store_customers.education_status,
  store_customers.purchase_estimate,
  store_customers.credit_rating
limit 100;`

  ```text

  ```
- `trilogy file write query70.preql -c import raw.physical_sales as ss;

# Filter to year 2000 and compute net profit rollup
where ss.date.year = 2000

# Rollup…= 2  # only top 5 states + grand total
order by
    level desc,
    case when level = 0 then ss.store.state else null end asc,
    rnk asc
limit 100;`

  ```text

  ```
- `trilogy file write query71.preql --content import raw.all_sales as sales;

where
  sales.item.manager_id = 1
  and sales.date.month_of_year = 11
  and sales.…les.ext_sales_price) as total_extended_sales_price
order by
  total_extended_sales_price desc,
  brand_id asc,
  hour asc,
  minute asc
  nulls first`

  ```text

  ```
- `trilogy run --import raw.all_sales:s select s.date.year, s.item.brand_id, s.item.class_id, s.item.category_id, s.item.manufacturer_id, sum(s.quantity - coale…xt_sales_price - coalesce(s.return_amount, 0)) as net_amt where s.item.category = 'Books' and s.date.year in (2001, 2002) group by 1,2,3,4,5 limit 5;`

  ```text

  ```
- `trilogy file write query77.preql --content import raw.all_sales as all_sales;

# Channel label as specified
auto channel_label <- case(
    all_sales.sales_c…alesce(all_sales.return_net_loss, 0.0)) as profit
order by channel asc nulls first,
         outlet asc nulls first,
         returns desc
limit 100;`

  ```text

  ```
- `trilogy file write query77.preql --content import raw.all_sales as all_sales;

# Compute channel label
auto channel_label <- case
    when all_sales.sales_ch…llup channel_label, all_sales.outlet_id as profit
order by channel asc nulls first,
         outlet asc nulls first,
         returns desc
limit 100;`

  ```text

  ```
- `trilogy run --import raw.all_sales:s select s.date.year, s.sales_channel, count(s.row_one) as cnt where s.date.year = 2000 and s.is_returned = false and s.billing_customer.id is not null group by s.date.year, s.sales_channel;`

  ```text

  ```
- `trilogy file write query82.preql --content import raw.inventory as inv;
import raw.physical_sales as store;

# Items in store sales that also meet the price/…date and '2000-07-24'::date
    and inv.quantity_on_hand between 100 and 500
inner join inv.item.id = store.item.id
order by
    item_code
limit 100;`

  ```text

  ```
- `trilogy file write query87.preql --content import raw.physical_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_s…me
left join store_combos.s_sale_date = web_combos.w_sale_date
having
    catalog_combos.c_last_name is null
    and web_combos.w_last_name is null
;`

  ```text

  ```
- `trilogy file write query87.preql --content import raw.physical_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_s…me
left join store_combos.s_sale_date = web_combos.w_sale_date
having
    catalog_combos.c_last_name is null
    and web_combos.w_last_name is null
;`

  ```text

  ```
- `trilogy file write query89.preql --content import raw.physical_sales as ss;
import raw.item as item;
import raw.store as store;
import raw.date as date;

# F…   store.name as store_name,
    store.company_name as store_company_name,
    date.month_of_year as month_of_year,
    sum(ss.sales_price) as total
`

  ```text

  ```
- `trilogy run --import raw/web_sales:ws select ws.order_number, ws.warehouse.id, count(ws.row_counter) from ws limit 5`

  ```text

  ```
- `trilogy run --import raw/web_sales:ws select ws.order_number, count(ws.row_counter) as cnt where ship_date.date between '1999-02-01'::date and '1999-04-02'::date and ws.ship_address.state = 'IL' and ws.web_site.company_name = 'pri' group by ws.order_number limit 5`

  ```text

  ```
- `trilogy run --import raw.physical_sales:store select count_distinct(store.customer.id, store.item.id) as store_tuples where store.date.year=2000 and store.customer.id is not null;`

  ```text

  ```
- `trilogy file write query97_check.preql -e -c import raw.physical_sales as store;
import raw.catalog_sales as catalog;

with store_tuples as
where store.date.…item) pairs from each side
select count(store_tuples.c) as cnt1 from (
    select distinct store_tuples.c, store_tuples.i from store_tuples
) as t1;
`

  ```text

  ```

### `undefined-concept`

- `trilogy run --import raw.all_sales:all_sales select date.week_seq, date.year, sum(all_sales.ext_sales_price) as total where all_sales.sales_channel in ('WEB','CATALOG') and date.year = 2001 order by date.week_seq limit 10;`

  ```text

  ```
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
- `trilogy run query02.preql`

  ```text

  ```
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
- `trilogy run query04.preql`

  ```text

  ```
- `trilogy run query04.preql`

  ```text

  ```
- `trilogy run query04.preql`

  ```text

  ```
- `trilogy run query04.preql`

  ```text

  ```
- `trilogy run query04.preql`

  ```text

  ```
- `trilogy run query06.preql`

  ```text

  ```
- `trilogy run query06.preql`

  ```text

  ```
- `trilogy run test_avg5.preql`

  ```text

  ```
- `trilogy run test_avg5.preql`

  ```text

  ```
- `trilogy run test_avg5.preql`

  ```text

  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text

  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text

  ```
- `trilogy run query11.preql`

  ```text

  ```
- `trilogy run query13.preql`

  ```text

  ```
- `trilogy run query16.preql`

  ```text

  ```
- `trilogy run query16.preql`

  ```text

  ```
- `trilogy run query18.preql`

  ```text

  ```
- `trilogy run --import raw/physical_sales:ps 
# Step 1: Get distinct (item.id, date.date) pairs for store sales 2000-2003
with distinct_item_date as
where year…item_date.item_id) as pair_count
having
  pair_count > 4
;

# Test: how many groups?
select count(frequent_groups.desc_prefix) as grp_cnt
limit 100;
`

  ```text

  ```
- `trilogy run --import raw/physical_sales:ps 
# Step 1: Get distinct (item.id, date.date) pairs for store sales 2000-2003
with distinct_item_date as
where year…item_date.item_id) as pair_count
having
  pair_count > 4
;

# Test: how many groups?
select count(frequent_groups.desc_prefix) as grp_cnt
limit 100;
`

  ```text

  ```
- `trilogy run --import raw/physical_sales:ps 
# Get distinct (item.id, date.date) pairs for store sales 2000-2003
with distinct_item_date as
where year(ps.date…item_id,
  substring(ps.item.desc, 1, 30) as desc_prefix
;

select distinct_item_date.desc_prefix, count(distinct_item_date.item_id) as cnt
limit 5;
`

  ```text

  ```
- `trilogy run query23.preql`

  ```text

  ```
- `trilogy run query23.preql`

  ```text

  ```
- `trilogy run --import raw/physical_sales:ps 
with cust_totals as
select
  ps.customer.id as customer_id,
  sum(ps.quantity * ps.sales_price) as lifetime_sales…er_id,
  --cust_totals.lifetime_sales,
  --max_total
having
  cust_totals.lifetime_sales > 0.5 * max_total
;

select best_custs.customer_id
limit 5;
`

  ```text

  ```
- `trilogy run --import raw/physical_sales:ps 
# Compute customer totals (excluding null customers)
with cust_totals as
where ps.customer.id is not null
select
…_id,
  --cust_totals.lifetime_sales,
  --global_max
having
  cust_totals.lifetime_sales > 0.5 * global_max
;

select best_custs.customer_id
limit 5;
`

  ```text

  ```
- `trilogy run --import raw/all_sales:al --import raw/physical_sales:ps 
auto global_max_sales <- max(
  sum((ps.quantity * ps.sales_price) ? ps.customer.id is …d al.billing_customer.first_name is null
order by
  last_name asc nulls first,
  first_name asc nulls first,
  total_sales asc nulls first
limit 10;
`

  ```text

  ```
- `trilogy run --import raw.catalog_store_returns:csr select count(csr.ticket_number) as cnt where store_sale_date.year = 2001 and store_sale_date.month_of_year = 4 and store_return_date.year = 2001 and store_return_date.month_of_year >= 4 and store_return_date.month_of_year <= 10;`

  ```text

  ```
- `trilogy run query31.preql`

  ```text

  ```
- `trilogy run query41.preql`

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
- `trilogy run query51.preql`

  ```text

  ```
- `trilogy run query57.preql`

  ```text

  ```
- `trilogy run query60.preql`

  ```text

  ```
- `trilogy run query69.preql`

  ```text

  ```
- `trilogy run query70.preql`

  ```text

  ```
- `trilogy run query71.preql`

  ```text

  ```
- `trilogy run query75.preql`

  ```text

  ```
- `trilogy run query89.preql`

  ```text

  ```
- `trilogy explore query89.preql`

  ```text

  ```
- `trilogy run _check.preql`

  ```text

  ```
- `trilogy run query94.preql`

  ```text

  ```
- `trilogy explore query94.preql`

  ```text

  ```
- `trilogy run query94.preql`

  ```text

  ```
- `trilogy run --import raw/web_sales:ws select ws.order_number, ws.ext_ship_cost, ws.net_profit where ship_date.date between '1999-02-01'::date and '1999-04-02'::date and ws.ship_address.state = 'IL' and ws.web_site.company_name = 'pri' limit 5`

  ```text

  ```
- `trilogy run query95.preql`

  ```text

  ```
- `trilogy run query95.preql`

  ```text

  ```
- `trilogy run query96.preql`

  ```text

  ```

### `cli-misuse`

- `trilogy file write query02.preql --escapes true --content import raw.all_sales as all_sales;

# Identify week sequences that touch 2001
auto ws_2001 <- all_s…ow.dow_sales, 53) over (partition by weekly_dow.day_of_week order by weekly_dow.week_seq), 2) as ratio
order by weekly_dow.week_seq asc nulls first
;`

  ```text
  Got unexpected extra argument (true)
  ```
- `trilogy run query14.preql --format json`

  ```text
  '--format' is not a valid dialect. The dialect argument comes after the input file and any options.
    Try: trilogy run <input> [<dialect>]
  ```
- `trilogy run query23.preql --format json`

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
- `trilogy explore raw/`

  ```text
  Invalid value for 'PATH': File 'raw/' is a directory.
  ```
- `trilogy file run query38_validate5.preql`

  ```text
  No such command 'run'.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy run - --import raw/physical_sales:ps --import raw/physical_returns:pr --import raw/catalog_sales:cs --import raw/catalog_returns:cr select ps.item.id… inner join ps.item.id = pr.item.id inner join ps.item.id = cs.item.id inner join cs.item.id = cr.item.id order by item_id, sname, szip, yr limit 20;`

  ```text
  'select ps.item.id as item_id, ps.store.name as sname, ps.store.zip as szip, ps.date.year as yr, count(ps.line_item) as cnt, sum(ps.wholesale_cost) as swc, sum(ps.list_price) as slp, sum(ps.coupon_amt) as sca where ps.date.year in (1999,2000) and ps.item.color in ('purple','burlywood','indian','spring','floral','medium') and ps.item.current_price between 65 and 74 and ps.customer_demographic.marital_status != ps.customer.demographics.marital_status and sum(cs.ext_list_price) by cs.item.id > 2 * sum(cr.refunded_cash + cr.reversed_charge + cr.store_credit) by cr.item.id inner join ps.ticket_number = pr.ticket_number inner join ps.item.id = pr.item.id inner join ps.item.id = cs.item.id inner join cs.item.id = cr.item.id order by item_id, sname, szip, yr limit 20;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy explore raw/sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/sales.preql' does not exist.
  ```
- `trilogy explore raw/raw/customer.preql --regex text_id|id|code`

  ```text
  Invalid value for 'PATH': File 'raw/raw/customer.preql' does not exist.
  ```
- `trilogy run query81.preql --format json`

  ```text
  '--format' is not a valid dialect. The dialect argument comes after the input file and any options.
    Try: trilogy run <input> [<dialect>]
  ```
- `trilogy explore raw/catalog_return.preql`

  ```text
  Invalid value for 'PATH': File 'raw/catalog_return.preql' does not exist.
  ```
- `trilogy explore raw/store_sales.preql --regex customer --regex date`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy run --import raw/physical_sales:s where s.return_reason.desc = 'reason 28' and s.customer.id is not null --select s.customer.id select count(s.custom…re s.return_reason.desc = 'reason 28' and s.customer.id is not null and s.quantity is null or s.sales_price is null --nope, let me check differently;`

  ```text
  'run' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy run --import raw/physical_sales:s with good_customers as where s.return_reason.desc = 'reason 28' and s.customer.id is not null and s.quantity is not…d is not null select s.customer.id; select count(all_customers.customer.id) as cnt where all_customers.customer.id not in good_customers.customer.id;`

  ```text
  'with all_customers as where s.return_reason.desc = 'reason 28' and s.customer.id is not null select s.customer.id;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy run --import raw/physical_sales:s with null_cust as where s.return_reason.desc = 'reason 28' and s.customer.id is not null and (s.quantity is null or…sales_price is not null select s.customer.id; select count(null_cust.customer.id) as overlap where null_cust.customer.id in nonnull_cust.customer.id;`

  ```text
  'with nonnull_cust as where s.return_reason.desc = 'reason 28' and s.customer.id is not null and s.quantity is not null and s.sales_price is not null select s.customer.id;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw/all_sales:all_sales select distinct all_sales.sales_channel, all_sales.channel_dim_text_id limit 20;`

  ```text

  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.sales_channel, count(all_sales.order_id), sum(all_sales.ext_list_price) where all_sales.date.year between 1999 and 2001 limit 5;`

  ```text

  ```
- `trilogy run --import raw/physical_sales:ps 
# First, get distinct (item.id, date.date year) pairs for store sales 2000-2003
# and compute desc_prefix
with st…ps.item.id as item_id,
  substring(ps.item.desc, 1, 30) as desc_prefix
;

select frequent_items.desc_prefix, count(frequent_items.item_id)
limit 10;
`

  ```text

  ```
- `trilogy run --import raw.physical_sales:ss select count(ss.line_item) where ss.date.year = 2000;`

  ```text

  ```
- `trilogy run --import raw.catalog_sales:cs select count(cs.line_item) where cs.sold_date.year = 2000;`

  ```text

  ```
- `trilogy run --import raw.web_sales:ws select count(ws.line_item) where ws.date.year = 2000;`

  ```text

  ```
- `trilogy file write query41.preql --content import raw.item as item;

# Eight attribute profiles: (category, color, units, size)
# 1: Books / tan / Oz / N/A
#…n 1 and 500
    and item.manufact in shared_manus.manufact
select
    distinct item.product_name as product_name
order by
    product_name
limit 100;`

  ```text

  ```
- `trilogy file write query47.preql --content import raw.physical_sales as ss;

# Monthly totals of unit price per (category, brand, store, company)
with monthl…ar asc,
  avg_monthly_sales asc,
  monthly_totals.monthly_total asc,
  prior_month_total asc nulls last,
  next_month_total asc nulls last
limit 100;`

  ```text

  ```
- `trilogy file write query77.preql --content import raw.all_sales as all_sales;

# Compute channel label
auto channel_label <- case
    when all_sales.sales_ch…llup channel_label, all_sales.outlet_id as profit
order by channel asc nulls first,
         outlet asc nulls first,
         returns desc
limit 100;`

  ```text

  ```
- `trilogy file write query77.preql --content import raw.all_sales as all_sales;

# Compute channel label
auto channel_label <- case
    when all_sales.sales_ch…llup channel_label, all_sales.outlet_id as profit
order by channel asc nulls first,
         outlet asc nulls first,
         returns desc
limit 100;`

  ```text

  ```
- `trilogy file write query89.preql --content import raw.physical_sales as ss;

where ss.date.year = 1999
  and (
    (ss.item.category in ('Books', 'Electronic…e_company_name)
    avg(sum(ss.sales_price)) by (ss.item.category, ss.item.brand_name, ss.store.name, ss.store.company_name) as avg_total
limit 100
;`

  ```text

  ```

### `join-resolution`

- `trilogy run query06.preql`

  ```text

  ```
- `trilogy run query06.preql`

  ```text

  ```
- `trilogy run test_item_avg2.preql`

  ```text

  ```
- `trilogy run query06.preql`

  ```text

  ```
- `trilogy run query16.preql`

  ```text

  ```
- `trilogy run --import raw.physical_sales:ss --import raw.catalog_sales:cs --import raw.web_sales:ws where ss.date.year = 2000 select ss.customer.last_name, ss.customer.first_name, ss.date.date, 'store' as ch limit 5;`

  ```text

  ```
- `trilogy run query40.preql`

  ```text

  ```
- `trilogy run query52.preql`

  ```text

  ```
- `trilogy run query53.preql`

  ```text

  ```
- `trilogy run query89.preql`

  ```text

  ```
- `trilogy run query89.preql`

  ```text

  ```

### `file-not-found`

- `trilogy run query04.preql`

  ```text

  ```
- `trilogy explore query04.preql --regex s01 --show concepts`

  ```text

  ```
- `trilogy run query04.preql`

  ```text

  ```
- `trilogy run query04.preql`

  ```text

  ```
- `trilogy run query74.preql duckdb`

  ```text

  ```
- `trilogy explore query74.preql`

  ```text

  ```
- `trilogy run query82.preql`

  ```text

  ```
- `trilogy run --import raw.physical_sales:store --import raw.catalog_sales:catalog select count(store.customer.id) as cnt where store.date.year=2000 and store.customer.id is not null and store.customer.id not in catalog.bill_customer.id;`

  ```text

  ```
