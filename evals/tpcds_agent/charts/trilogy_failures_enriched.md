# Trilogy failure analysis — 20260612-203221

- Run `20260612-203218_enriched` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 1438 | failed: 223 (16%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 115 | 52% |
| `syntax-parse` | 50 | 22% |
| `undefined-concept` | 26 | 12% |
| `join-resolution` | 12 | 5% |
| `syntax-missing-alias` | 8 | 4% |
| `cli-misuse` | 7 | 3% |
| `file-not-found` | 4 | 2% |
| `type-error` | 1 | 0% |

## Detail

### `other`

- `trilogy run query01.preql duckdb`

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
- `trilogy run query04.preql`

  ```text

  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

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
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query16.preql`

  ```text

  ```
- `trilogy run query18.preql`

  ```text

  ```
- `trilogy file read query18.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query23.preql`

  ```text

  ```
- `trilogy run query23.preql`

  ```text

  ```
- `trilogy run query23.preql`

  ```text

  ```
- `trilogy run query23.preql`

  ```text

  ```
- `trilogy run query23.preql`

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
- `trilogy run query24.preql`

  ```text
  [guidance] You have issued this identical call 3 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy run query24.preql`

  ```text
  [guidance] You have issued this identical call 4 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy file read trilogy.toml`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query24.preql duckdb ./tpcds.duckdb`

  ```text

  ```
- `trilogy run query24.preql duckdb ./tpcds.duckdb?mode=ro`

  ```text

  ```
- `trilogy run query24.preql`

  ```text

  ```
- `trilogy file read raw/catalog_store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query29.preql`

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
- `trilogy file delete test_join.preql`

  ```text

  ```
- `trilogy file delete test_join2.preql`

  ```text

  ```
- `trilogy file read query38.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 1 column 2304 (char 2303). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run test_rowset.preql`

  ```text

  ```
- `trilogy run test_rowset2.preql`

  ```text

  ```
- `trilogy run test_rowset3.preql`

  ```text

  ```
- `trilogy run test_rowset4.preql`

  ```text

  ```
- `trilogy run test_rowset5.preql`

  ```text

  ```
- `trilogy run test_rowset6.preql`

  ```text

  ```
- `trilogy file read raw/inventory.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query39.preql`

  ```text

  ```
- `trilogy file read query39.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw\physical_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query53.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query54.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
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
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query59.preql`

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
- `trilogy file read raw/warehouse.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query66.preql`

  ```text

  ```
- `trilogy run query66.preql`

  ```text

  ```
- `trilogy file read query70.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query72.preql`

  ```text

  ```
- `trilogy run query74.preql`

  ```text

  ```
- `trilogy run query74.preql`

  ```text

  ```
- `trilogy file read query74.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 72 column 12 (char 2424). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file read raw/repro.preql`

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
- `trilogy file read raw/physical_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query84.preql`

  ```text

  ```
- `trilogy run query84.preql`

  ```text

  ```
- `trilogy run query87.preql`

  ```text

  ```
- `trilogy run query87.preql`

  ```text

  ```
- `trilogy file read query87.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 20 column 3 (char 1469). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file read query88.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query89.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw/web_sales:ws with q as where ws.ship_date.date between '1999-02-01'::date and '1999-04-02'::date and ws.ship_address.state = 'IL' an…r) as order_count, sum(o.tsc) as total_shipping_cost, sum(o.tnp) as total_net_profit having o.wc > 1 and o.ar = false order by order_count limit 100;`

  ```text

  ```
- `trilogy file read raw/physical_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query97.preql`

  ```text

  ```
- `trilogy run query97.preql`

  ```text

  ```
- `trilogy run query97.preql`

  ```text

  ```

### `syntax-parse`

- `trilogy run --import raw.all_sales:sales select sales.date.week_seq, sales.date.year, sales.date.day_of_week, sum(sales.ext_sales_price) as total where sales.sales_channel in ('WEB', 'CATALOG') group by sales.date.week_seq, sales.date.year, sales.date.day_of_week order by sales.date.week_seq limit 20;`

  ```text

  ```
- `trilogy file write query02.preql --content import raw.all_sales as sales;

# Filter to web and catalog only
where sales.sales_channel in ('WEB', 'CATALOG')

…sales(sales.date.week_seq + 53), 2) as sat_ratio
where
    sales.date.week_seq in ws_2001
order by
    sales.date.week_seq asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query02.preql --content import raw.all_sales as sales;

# Sum of ext_sales_price for each day-of-week
def sun_sales -> sum(sales.ext_sales…   round(@sat_sales / lead(@sat_sales, 53) over (order by sales.date.week_seq asc), 2) as sat_ratio
order by
    sales.date.week_seq asc nulls first;`

  ```text

  ```
- `trilogy file write query04.preql --content import raw.all_sales as s;

# Annual value per customer per channel: sum of (ext_list_price - ext_wholesale_cost -…vals.catalog_value as catalog_value,
    web_vals.web_value as web_value
;

select
    cust_id,
    first_name,
    last_name
from combined
limit 10;`

  ```text

  ```
- `trilogy run --import raw/all_sales:sales select sales.sales_channel, sales.channel_dim_id, sales.channel_dim_text_id, sum(sales.net_profit) as total_net_prof…s), 0) as net, count(*) as cnt where sales.date.date between '2000-08-23'::date and '2000-09-06'::date and sales.channel_dim_id is not null limit 20;`

  ```text

  ```
- `trilogy run --import raw/all_sales:sales select sales.sales_channel, sales.channel_dim_text_id as entity_id, sum(sales.net_profit) as gross_sales where sales…ween '2000-08-23'::date and '2000-09-06'::date and sales.channel_dim_id is not null group by sales.sales_channel, sales.channel_dim_text_id limit 10;`

  ```text

  ```
- `trilogy file write query05.preql --content import raw/all_sales as sales;

# Sales data: per-channel, per-entity gross sales
rowset sales_agg <-
    select
 …id, gross_sales, total_returns);

select
    combined.channel,
    combined.entity_id,
    combined.gross_sales,
    combined.total_returns
limit 20;`

  ```text

  ```
- `trilogy file write query14.preql --content import raw.all_sales as all_sales;

# Overall average sale value (ext_list_price) across all three channels in 199…sales > overall_avg_sale
order by
    channel nulls first,
    brand_id nulls first,
    class_id nulls first,
    category_id nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query14.preql --content import raw.all_sales as all_sales;

# Overall average sale value across all three channels in 1999-2001
auto overa…sales > overall_avg_sale
order by
    channel nulls first,
    brand_id nulls first,
    class_id nulls first,
    category_id nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query14.preql --content import raw.all_sales as all_sales;

# First, find (brand_id, class_id, category_id) combos that appeared in ALL TH…select overall_avg_sale)
order by
    channel nulls first,
    brand_id nulls first,
    class_id nulls first,
    category_id nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query14.preql --content import raw.all_sales as all_sales;

# Define combo ID as reusable concept
auto combo_id <- concat(all_sales.item.b…ategory_id as sale_count
order by
    channel nulls first,
    brand_id nulls first,
    class_id nulls first,
    category_id nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query14.preql --content import raw.all_sales as all_sales;

# Define combo ID as reusable concept
auto combo_id <- concat(all_sales.item.b… --count_distinct(all_sales.sales_channel) as cc
limit 20;

select 
    channel_counts.cid as combo,
    channel_counts.cc as channel_count
limit 20;`

  ```text

  ```
- `trilogy file write query31.preql --content import raw.physical_sales as store_sales;
import raw.web_sales as web;

# Total extended store sales price by coun…), 0) as store_q3_over_q2
having
    web_q2_over_q1 > store_q2_over_q1
    and web_q3_over_q2 > store_q3_over_q2
order by
    county_q_sales.county
;`

  ```text

  ```
- `trilogy file write query31.preql --content import raw.physical_sales as ss;
import raw.web_sales as ws;

# Per-county, per-quarter totals
rowset store_sales … 2), 0) as store_q3_over_q2
having
    web_q2_over_q1 > store_q2_over_q1
    and web_q3_over_q2 > store_q3_over_q2
order by
    all_counties.county
;`

  ```text

  ```
- `trilogy file write query31.preql --content import raw.physical_sales as ss;
import raw.web_sales as ws;

# Store sales by county and quarter in 2000
rowset s…s1.county,
    s1.ext_sales as q1,
    s2.ext_sales as q2,
    s3.ext_sales as q3
inner join s1.county = s2.county
inner join s1.county = s3.county
;`

  ```text

  ```
- `trilogy file write query31.preql --content import raw.physical_sales as ss;
import raw.web_sales as ws;

# Get all counties from both channels
rowset store_c…y)), 0) as store_q3_over_q2
having
    web_q2_over_q1 > store_q2_over_q1
    and web_q3_over_q2 > store_q3_over_q2
order by
    all_counties.county
;`

  ```text

  ```
- `trilogy file write query31.preql --content import raw.physical_sales as ss;
import raw.web_sales as ws;

# All counties with store or web sales in 2000 Q1-Q3… = all_cty.cty)), 0) as store_q3_over_q2
having
    web_q2_over_q1 > store_q2_over_q1
    and web_q3_over_q2 > store_q3_over_q2
order by
    county
;`

  ```text

  ```
- `trilogy file write query31.preql --content import raw.physical_sales as ss;
import raw.web_sales as ws;

merge ss.sale_address.county into ws.bill_address.co…unty,
    coalesce(s_tot.q, w_tot.q) as q,
    s_tot.val as s_val,
    w_tot.val as w_val
full join s_tot.cty = w_tot.cty
    and s_tot.q = w_tot.q
;`

  ```text

  ```
- `trilogy file write query32.preql`

  ```text

  ```
- `trilogy file write query35.preql --content import raw.physical_sales as store_sales;
import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_s…s first,
         dependent_count nulls first,
         employed_dependent_count nulls first,
         college_dependent_count nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query35.preql --content import raw.physical_sales as store_sales;
import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_s…s first,
         dependent_count nulls first,
         employed_dependent_count nulls first,
         college_dependent_count nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query38.preql --content import raw.physical_sales as store;
import raw.catalog_sales as catalog;
import raw.web_sales as web;

# Build uni…ll three
select count(store_keys.key) as unique_combinations
where store_keys.key in catalog_keys.key
  and store_keys.key in web_keys.key
limit 100;`

  ```text

  ```
- `trilogy file write query44.preql --content import raw.physical_sales as store_sales;

# Average net profit on sales at store 1 where sale_address is null (th…by
    avg_profit desc
limit 10;

# Pair them by rank position
select
    best.product_name as best_product,
    worst.product_name as worst_product
`

  ```text

  ```
- `trilogy file write query47.preql -e -c import raw.physical_sales as ss;

# Filter to the 14-month window: Dec 1998 + Jan 2000 + all months of 1999
where (ss.…_of_year asc,
    final.avg_monthly_sales asc,
    final.month_total asc,
    final.prior_month_total asc,
    final.next_month_total asc
limit 100
;`

  ```text

  ```
- `trilogy file write query51.preql --content import raw.web_sales as web;
import raw.physical_sales as store;

# Web sales daily aggregates (year 2000)
rowset …eb_daily.sale_date = store_daily.sale_date
having web_running_max > store_running_max
order by item asc nulls first, date asc nulls first
limit 100;
`

  ```text

  ```
- `trilogy file write query53.preql --content import raw.physical_sales as ss;

# Per-item per-quarter totals of unit price (sales_price) for items matching pro…> 0
   and abs(mfr_quarter_total - avg_mfr_sales) / avg_mfr_sales > 0.1
order by avg_mfr_sales, mfr_quarter_total, ss.item.manufacturer_id
limit 100;`

  ```text

  ```
- `trilogy file write query54.preql -e -c import raw/catalog_sales as catalog;
import raw/web_sales as web;
import raw/physical_sales as store;

# Find the mont…mer.id as customer_id,
  sum(store.ext_sales_price) as total_store_ext_price
having customer_id is not null
  and total_store_ext_price is not null;
`

  ```text

  ```
- `trilogy file write query54.preql -e -c import raw.catalog_sales as catalog;
import raw.web_sales as web;
import raw.physical_sales as store;

# Find the mont…omer.id as customer_id,
  sum(store.ext_sales_price) as total_store_ext_price
having customer_id is not null
  and total_store_ext_price is not null;`

  ```text

  ```
- `trilogy file write query56.preql -e -c import raw.all_sales as sales;

# Use a filtered concept to define qualifying items
auto qual_items <- sales.item.text… as total
having --sales.item.text_id in qual_items
    sales.item.text_id in qual_items
order by total nulls first, item_code nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query56.preql -e -c import raw.item as item;
import raw.all_sales as sales;

# Distinct item text_ids with qualifying colors
rowset qual_i…es_by_item.total) as total
left join qual_item_codes.item_code = sales_by_item.item_code
order by total nulls first, item_code nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query59.preql --content import raw.physical_sales as ps;

# Define macros for day-of-week filtered aggregates
def sun_price -> sum(ps.sale…year.week_seq - 53
order by this_year.store_name asc nulls first, this_year.store_code asc nulls first, this_year.week_seq asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query59.preql --content import raw.physical_sales as ps;

# Define macros for day-of-week filtered aggregates - no params, just inline con…year.week_seq - 53
order by this_year.store_name asc nulls first, this_year.store_code asc nulls first, this_year.week_seq asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query59.preql --content import raw.physical_sales as ps;

# This year (2001) weekly store sales by day of week
with this_year as
where ps.…year.week_seq - 53
order by this_year.store_name asc nulls first, this_year.store_code asc nulls first, this_year.week_seq asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.physical_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

# Per-item cum…ount,
    sum(ws_cost) as total_ws_cost,
    sum(list_price) as total_list_price,
    sum(coupon_amt) as total_coupon_amt
;

select count(*) as cnt
;`

  ```text

  ```
- `trilogy file write query65.preql --content import raw.physical_sales as store_sales;

# Filter to 1998 store sales
where store_sales.date.year = 1998

# Comp…ore_name asc nulls first,
    item_desc asc nulls first,
    store_sales.store.id asc nulls first,
    store_sales.item.id asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query66.preql --content import raw.all_sales as s;

# Define the 12 month numbers
auto all_months <- unnest(generate_array(1, 12, 1));

# …s_data.monthly_net ? sales_data.month = all_months, 0) as monthly_net
order by
  warehouses.warehouse_name nulls first,
  year nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query66.preql --content import raw.all_sales as s;

# Aggregate sales per warehouse+month
with sales_agg as
where
  s.sales_channel in ('W…(sales_agg.monthly_net ? sales_agg.month = all_months, 0) as monthly_net
order by
  wh_list.warehouse_name nulls first,
  year nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query74.preql --content import raw.physical_sales as store;
import raw.web_sales as web;

# Store sales net_paid by customer and year
rows…0
    and web_2002 is not null
    and (web_2002 / web_2001) > (store_2002 / store_2001)
order by store_totals.customer_id asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query75.preql --content import raw/all_sales as sales;

# Deduplicate per-line records: one row per (item.id, order_id, sales_channel, dat… yr2002.manufacturer_id = yr2001.manufacturer_id
having yr2002.qty_2002 * 1.0 / yr2001.qty_2001 < 0.9
order by qty_diff asc, amt_diff asc
limit 100;
`

  ```text

  ```
- `trilogy run --import raw.all_sales:sales select sales.item.category as cat, sales.ext_sales_price as price, sales.quantity as qty, sales.return_quantity as rqty, sales.return_amount as ramt, sales.date.year as yr limit 3 where sales.item.category = 'Books';`

  ```text

  ```
- `trilogy run --import raw.all_sales:sales where sales.item.category = 'Books' select sales.date.year as yr, count(sales.order_id) as cnt group by yr order by yr;`

  ```text

  ```
- `trilogy file write query75.preql --content import raw.all_sales as sales;

# Deduplicate per-line records: one row per (item.id, order_id, sales_channel, yea… yr2002.manufacturer_id = yr2001.manufacturer_id
having yr2002.qty_2002 * 1.0 / yr2001.qty_2001 < 0.9
order by qty_diff asc, amt_diff asc
limit 100;
`

  ```text

  ```
- `trilogy file write query76.preql --content import raw.physical_sales as store_sales;
import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_s…hannel asc,
  combined.missing_ref asc,
  combined.yr asc nulls first,
  combined.qtr asc nulls first,
  combined.category asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query78.preql --content import raw.physical_sales as store;
import raw.web_sales as web;
import raw.catalog_sales as catalog;

# Store sal…tore_agg.web_sp, 0) + coalesce(store_agg.catalog_sp, 0) as other_sp
left join store_agg.web_qty != store_agg.catalog_qty  # dummy, will be replaced
;`

  ```text

  ```
- `trilogy file write query78.preql --content import raw.physical_sales as store;
import raw.web_sales as web;
import raw.catalog_sales as catalog;

# Store sal…item_id, store_agg.customer_id, store_agg.qty desc, store_agg.wc desc, store_agg.sp desc, other_qty asc, other_wc asc, other_sp asc, ratio
limit 100;`

  ```text

  ```
- `trilogy file write query78.preql --content import raw.all_sales as sales;

# For each channel, compute aggregates for never-returned, year=2000, identified b… store_agg.customer_id asc, store_agg.qty desc, store_agg.wc desc, store_agg.sp desc, other_qty asc, other_wc asc, other_sp asc, ratio asc
limit 100;`

  ```text

  ```
- `trilogy file write query78.preql --content import raw.all_sales as sales;

# Compute store aggregates per (year, item, purchasing_customer)
with store_part a…r_id asc, store_part.qty desc, store_part.wc desc, store_part.sp desc, other_part.qty asc, other_part.wc asc, other_part.sp asc, ratio asc
limit 100;`

  ```text

  ```
- `trilogy file write query79.preql --content import raw/physical_sales as sales;

where sales.date.day_name = 'Monday'
  and sales.date.year in (1999, 2000, 20…les.customer.first_name nulls first,
    store_city_30 nulls first,
    total_net_profit nulls first,
    sales.ticket_number nulls first
limit 100;
`

  ```text

  ```
- `trilogy file write query83.preql -e -c import raw.catalog_returns as catalog_returns;
import raw.physical_sales as physical_sales;
import raw.web_returns as …r join catalog_returns.item.text_id = web_returns.item.text_id
where
    store_row_count > 0
    and catalog_row_count > 0
    and web_row_count > 0
`

  ```text

  ```
- `trilogy file write query95.preql --content import raw.web_sales as web_sales;

# Identify orders that satisfy all conditions
rowset qualifying_orders as
wher…,
    sum(web_sales.ext_ship_cost) as total_extended_shipping_cost,
    sum(web_sales.net_profit) as total_net_profit
order by order_count
limit 100;`

  ```text

  ```

### `undefined-concept`

- `trilogy run query01.preql duckdb`

  ```text

  ```
- `trilogy run query02.preql`

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
- `trilogy run query24.preql`

  ```text

  ```
- `trilogy run query47.preql`

  ```text

  ```
- `trilogy explore query47.preql --show all`

  ```text

  ```
- `trilogy run query47.preql`

  ```text

  ```
- `trilogy explore query47.preql --show all`

  ```text

  ```
- `trilogy run query47.preql`

  ```text

  ```
- `trilogy explore query47.preql --show all --regex monthly_totals`

  ```text

  ```
- `trilogy run query56.preql`

  ```text

  ```
- `trilogy run query56.preql`

  ```text

  ```
- `trilogy run query60.preql`

  ```text

  ```
- `trilogy run query64.preql`

  ```text

  ```
- `trilogy explore query64.preql`

  ```text

  ```
- `trilogy run query64.preql`

  ```text

  ```
- `trilogy run query66.preql`

  ```text

  ```
- `trilogy explore query66.preql`

  ```text

  ```
- `trilogy run query83.preql`

  ```text

  ```
- `trilogy run query94.preql`

  ```text

  ```
- `trilogy explore query94.preql`

  ```text

  ```
- `trilogy run --import raw/web_sales:ws with q as where ws.ship_date.date between '1999-02-01'::date and '1999-04-02'::date and ws.ship_address.state = 'IL' an…o as select q.ws.order_number, count_distinct(q.warehouse_id) as wc, bool_or(q.ws.is_returned) as ar; select o.q.ws.order_number, o.wc, o.ar limit 5;`

  ```text

  ```
- `trilogy run query97.preql`

  ```text

  ```
- `trilogy run query97.preql`

  ```text

  ```

### `join-resolution`

- `trilogy run query10.preql`

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
- `trilogy run test_auto.preql`

  ```text

  ```
- `trilogy run test_simple.preql`

  ```text

  ```
- `trilogy run test_simple2.preql`

  ```text

  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy run query56.preql`

  ```text

  ```
- `trilogy run query56.preql`

  ```text

  ```
- `trilogy run --import raw/all_sales:sales --import raw/item:item --import raw/date:date select item.text_id, sum(sales.ext_sales_price ? sales.sales_channel =…ate.week_seq = 5218) as catalog_total, sum(sales.ext_sales_price ? sales.sales_channel = 'WEB' and sales.date.week_seq = 5218) as web_total limit 20;`

  ```text

  ```
- `trilogy run query97.preql`

  ```text

  ```

### `syntax-missing-alias`

- `trilogy run --import raw/all_sales:sales select distinct sales.sales_channel, sales.return_channel_dim_text_id where sales.return_channel_dim_text_id is not null limit 20;`

  ```text

  ```
- `trilogy file write query05.preql --content import raw.all_sales as sales;

# Sales activity: per-channel, per-entity sales and returns
rowset sales_agg <-
  … by rollup combined.channel_type, combined.entity_text_id as total_returns
order by combined.channel_type asc, combined.entity_text_id asc
limit 100;`

  ```text

  ```
- `trilogy file write query10.preql --content import raw.physical_sales as store_sales;
import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_s…ducation_status,
    purchase_estimate,
    credit_rating,
    dependent_count,
    employed_dependent_count,
    college_dependent_count
limit 100;
`

  ```text

  ```
- `trilogy file write query41.preql --content import raw.item as item;

# Items matching any of the 8 attribute profiles
auto profile_items <- item.text_id ? (
….product_name as product_name
where item.manufacturer_id between 1 and 500
  and item.manufact in manuf_with_profile
order by product_name
limit 100;`

  ```text

  ```
- `trilogy file write query56.preql -e -c import raw.address;

select
    distinct address.gmt_offset as gmt_offset
order by gmt_offset
limit 50;`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.physical_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

# Per-item cum…mt as coupon_amt
    inner join qualifying_items.item_id = ss.item.id
;

select distinct filtered_sales.sale_year
order by filtered_sales.sale_year
;`

  ```text

  ```
- `trilogy run --import raw.all_sales:sales select sales.item.category, sales.ext_sales_price, sales.quantity, sales.return_quantity, sales.return_amount, sales.date.year limit 3 where sales.item.category = 'Books';`

  ```text

  ```
- `trilogy run --import raw.reason:reason select distinct reason.desc limit 50;`

  ```text

  ```

### `cli-misuse`

- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy run query66.preql --format json`

  ```text
  '--format' is not a valid dialect. The dialect argument comes after the input file and any options.
    Try: trilogy run <input> [<dialect>]
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy explore raw/store_returns.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_returns.preql' does not exist.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy syntax example nested-aggregate-group-average`

  ```text
  No such command 'syntax'.
  ```

### `file-not-found`

- `trilogy run query39.preql`

  ```text

  ```
- `trilogy run query39.preql`

  ```text

  ```
- `trilogy run query78.preql`

  ```text

  ```
- `trilogy run query78.preql`

  ```text

  ```

### `type-error`

- `trilogy run query90.preql duckdb`

  ```text

  ```
