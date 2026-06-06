# Trilogy failure analysis — 20260606-032823

- Run `20260606-032822_enriched` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 1896 | failed: 228 (12%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 83 | 36% |
| `syntax-parse` | 82 | 36% |
| `undefined-concept` | 29 | 13% |
| `syntax-missing-alias` | 18 | 8% |
| `join-resolution` | 12 | 5% |
| `cli-misuse` | 3 | 1% |
| `type-error` | 1 | 0% |

## Detail

### `other`

- `trilogy run query01.preql`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales rowset wk_2001 <- select all_sales.date.week_seq where all_sales.sales_channel != 'STORE' and all_sales.date.yea…elect all_sales.date.week_seq as ws, wk_sun as sun where all_sales.sales_channel != 'STORE' having ws in wk_2001.ws order by ws nulls first limit 10;`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales rowset ws_2001 <- select all_sales.date.week_seq where all_sales.sales_channel != 'STORE' and all_sales.date.yea…eq or all_sales.date.week_seq - 53 in ws_2001.all_sales.date.week_seq) having ws in ws_2001.all_sales.date.week_seq order by ws nulls first limit 55;`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales rowset ws_2001 <- select all_sales.date.week_seq where all_sales.sales_channel != 'STORE' and all_sales.date.yea…eq or all_sales.date.week_seq - 53 in ws_2001.all_sales.date.week_seq) having ws in ws_2001.all_sales.date.week_seq order by ws nulls first limit 55;`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales auto sun_next <- sum(all_sales.ext_sales_price ? all_sales.date.day_of_week = 0) by (all_sales.date.week_seq - 5…week_seq as ws, sun_next as sn where all_sales.sales_channel != 'STORE' and all_sales.date.week_seq in (5270, 5271, 5323, 5324) order by ws limit 10;`

  ```text

  ```
- `trilogy run --import raw/web_sales:web_sales --import raw/catalog_sales:catalog_sales select web_sales.date.week_seq as ws, sum(web_sales.ext_sales_price) + sum(catalog_sales.ext_sales_price) as total where web_sales.date.week_seq in (5270, 5323) order by ws limit 5;`

  ```text

  ```
- `trilogy run query02.preql`

  ```text

  ```
- `trilogy run query02.preql`

  ```text

  ```
- `trilogy file write --content import raw.all_sales as all_sales;

# Channel label mapping
auto channel_label <- case
    when all_sales.sales_channel = 'STORE…, 0) as total_returns,
    coalesce(net_profit_val, 0) as net_profit
order by
    channel_label asc,
    sales_entity_id asc
limit 100; query05.preql`

  ```text
  trilogy file write: `--content` takes a SINGLE string argument. Your args list put 2 separate tokens after --content (treating it like a shell command). In a tool call, pass the entire file body as one string element after --content, with newlines embedded literally — e.g.
    {"args": ["file", "write", "query70.preql", "--content", "import raw.store_sales as store_sales;\n\nselect ..."]}
  Alternatively use `--escapes` with a single-line `\n`-escaped string.
  ```
- `trilogy run query14.preql`

  ```text

  ```
- `trilogy run query14.preql`

  ```text

  ```
- `trilogy run --import raw.catalog_store_returns:csr where csr.store_sale_date.quarter_name = '2001Q1' and csr.store_return_date.quarter_name in ('2001Q1','200…, csr.catalog_date.quarter_name as cq, count(csr.store_quantity) as cnt where csr.catalog_date.quarter_name in ('2001Q1','2001Q2','2001Q3') limit 10;`

  ```text

  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 49 column 38 (char 2068). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file write query23.preql -e -c`

  ```text
  Option '-c' requires an argument.
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
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 1 column 2571 (char 2570). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query37.preql`

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
- `trilogy run query44.preql`

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
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file write query46.preql -e -c`

  ```text
  Option '-c' requires an argument.
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
- `trilogy run query53.preql`

  ```text

  ```
- `trilogy run query58.preql`

  ```text

  ```
- `trilogy run query58.preql`

  ```text

  ```
- `trilogy run query59.preql`

  ```text

  ```
- `trilogy run query59.preql`

  ```text

  ```
- `trilogy run query59.preql`

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
- `trilogy run query65.preql`

  ```text

  ```
- `trilogy run query65.preql`

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
- `trilogy run query72.preql`

  ```text

  ```
- `trilogy run query73.preql`

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
- `trilogy run query76.preql`

  ```text

  ```
- `trilogy run query76_two.preql`

  ```text

  ```
- `trilogy run query76.preql`

  ```text

  ```
- `trilogy run query76.preql`

  ```text

  ```
- `trilogy run query77_chk.preql`

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
- `trilogy run query78.preql`

  ```text

  ```
- `trilogy run query78.preql`

  ```text

  ```
- `trilogy run query82.preql`

  ```text

  ```
- `trilogy run query85.preql`

  ```text

  ```
- `trilogy run -e -c import raw.physical_sales as s;
where s.return_reason.desc = 'reason 28'
select s.customer.id, s.quantity, s.sales_price, s.return_quantity, (s.quantity - s.return_quantity) * s.sales_price as alt1, s.quantity * s.sales_price as alt2 limit 20;`

  ```text

  ```
- `trilogy run -e -c import raw.physical_sales as s;
auto adjusted_amount <- case when s.return_quantity is not null then (s.quantity - s.return_quantity) * s.s…ustomer.id as customer_id, sum(adjusted_amount) as total_adjusted_amount
order by total_adjusted_amount asc nulls first, customer_id asc nulls first;`

  ```text

  ```
- `trilogy run query94.preql`

  ```text

  ```
- `trilogy run query95.preql`

  ```text

  ```

### `syntax-parse`

- `trilogy run --import raw/all_sales:all_sales select all_sales.date.year as year, min(all_sales.date.week_seq) as wk_min, max(all_sales.date.week_seq) as wk_m…ales_channel != 'STORE' and all_sales.date.year >= 2000 and all_sales.date.year <= 2003 by all_sales.date.year order by all_sales.date.year limit 10;`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales select sum(all_sales.ext_sales_price) as total_sales, all_sales.date.week_seq as ws where all_sales.sales_channel != 'STORE' and all_sales.date.week_seq in (5270, 5323) by all_sales.date.week_seq order by ws limit 10;`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales select all_sales.date.week_seq as ws, min(all_sales.date.year) as yr_min, max(all_sales.date.year) as yr_max where all_sales.sales_channel != 'STORE' and all_sales.date.year = 2001 by all_sales.date.week_seq order by ws limit 5;`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales rowset all_ws <- select all_sales.date.week_seq as WS, sum(all_sales.ext_sales_price ? all_sales.date.day_of_week = 0) as SUN where all_sales.sales_channel != 'STORE'; select ws from all_ws limit 5;`

  ```text

  ```
- `trilogy run --import raw.all_sales:sales select sales.billing_customer.text_id, sales.date.year, sales.sales_channel, sum(((sales.ext_list_price - sales.ext_…ales.ext_discount_amount) + sales.ext_sales_price) / 2) as yr_total by sales.billing_customer.text_id, sales.date.year, sales.sales_channel limit 30;`

  ```text

  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

# Channel label
auto channel_label <- case
    when all_sales.sales_channel = …annel_label is null then 0 else 1 end asc,
    case when entity_id is null then 0 else 1 end asc,
    channel_label asc,
    entity_id asc
limit 100;`

  ```text

  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

# Channel label
auto channel_label <- case
    when all_sales.sales_channel = … 0 else 1 end asc,
    case when entity_id is null then 0 else 1 end asc,
    channel_label asc nulls first,
    entity_id asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query10.preql --content import raw.customer as customer;
import raw.physical_sales as store;
import raw.web_sales as web;
import raw.catal…  customer.demographics.dependent_count,
  customer.demographics.employed_dependent_count,
  customer.demographics.college_dependent_count
limit 100;`

  ```text

  ```
- `trilogy run --import raw.physical_sales:store_sales select store_sales.sale_address.state as st, max(store_sales.net_profit) as max_profit where store_sales.…re_sales.household_demographic.dependent_count = 1)) and store_sales.sale_address.state in ('TX','OH','OR','NM','KY','VA','MS') group by st limit 10;`

  ```text

  ```
- `trilogy file write query14.preql --content import raw.all_sales as sales;

# Overall average sale value (ext_list_price) across all 3 channels 1999-2001
auto…r_sales
order by
    channel asc nulls first,
    brand_id asc nulls first,
    class_id asc nulls first,
    category_id asc nulls first
limit 100;
`

  ```text

  ```
- `trilogy run --import raw.catalog_store_returns:csr where csr.store_sale_date.quarter_name = '2001Q1' and csr.store_return_date.quarter_name in ('2001Q1','200…og_date.quarter_name is not null select csr.catalog_date.quarter_name as cq, count(csr.store_quantity) as cnt group by cq order by cnt desc limit 10;`

  ```text

  ```
- `trilogy file write query17.preql --content import raw.physical_sales as ss;
import raw.catalog_sales as cs;

# Inner join: catalog sales match on customer + …(cs.quantity), 0) as catalog_qty_cv
order by ss.item.text_id asc nulls first, ss.item.desc asc nulls first, ss.store.state asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query17.preql --content import raw.physical_sales as ss;
import raw.catalog_sales as cs;

# Inner join: catalog sales match on customer + …(cs.quantity), 0) as catalog_qty_cv
order by ss.item.text_id asc nulls first, ss.item.desc asc nulls first, ss.store.state asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query23.preql --content # Query23: Frequent items + best customers, Feb 2000 catalog/web sales
# Step 1: Define frequent items from store …simplicity, let's build the set of item.id values
# that have >4 (item, sold_date) pairs within the store 2000-2003 data
# We need a rowset approach
`

  ```text

  ```
- `trilogy file write query23.preql --content import raw.physical_sales as store;
import raw.catalog_sales as catalog;
import raw.web_sales as web;

# Step 1: F… catalog.bill_customer.first_name,
  sum(catalog.quantity * catalog.list_price) as total_sales
order by last_name, first_name, total_sales
limit 100;`

  ```text

  ```
- `trilogy file write query24.preql --content import raw.physical_sales as ss;
import raw.physical_returns as sr;

# Match store sales to store returns by ticke…e,
  peach_total as total
having total > 0.05 * avg_all
order by ss.customer.last_name asc, ss.customer.first_name asc, ss.store.name asc
limit 100;
`

  ```text

  ```
- `trilogy file write query24.preql --content import raw.physical_sales as ss;
import raw.physical_returns as sr;

# Match store sales to store returns by ticke…e,
  peach_total as total
having total > 0.05 * avg_all
order by ss.customer.last_name asc, ss.customer.first_name asc, ss.store.name asc
limit 100;
`

  ```text

  ```
- `trilogy file write query27.preql --content import raw.physical_sales as store_sales;
import raw.customer_demographic as demo;

# Filter: store sales in 2002,…ales.item.text_id asc nulls first,
    store_sales.store.state asc nulls first
limit 100
by rollup store_sales.item.text_id, store_sales.store.state;`

  ```text

  ```
- `trilogy file write query27.preql --content import raw.physical_sales as store_sales;
import raw.customer_demographic as demo;

# Filter: store sales in 2002,…re_sales.sales_price) as avg_unit_price
order by
    store_sales.item.text_id asc nulls first,
    store_sales.store.state asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query27.preql --content import raw.physical_sales as store_sales;
import raw.customer_demographic as demo;

# Filter: store sales in 2002,…ales.item.text_id asc nulls first,
    store_sales.store.state asc nulls first
limit 150
by rollup store_sales.item.text_id, store_sales.store.state;`

  ```text

  ```
- `trilogy file write query27.preql --content import raw.physical_sales as store_sales;
import raw.customer_demographic as demo;

def rollup_avg_qty -> avg(stor…
    @rollup_avg_unit as avg_unit_price
order by
    store_sales.item.text_id asc nulls first,
    store_sales.store.state asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query34.preql --content import raw.physical_sales as ss;

# Restrict to years 1999, 2001, 2001 and day_of_month between 1-3 or 25-28
# at …ulls first,
    ss.customer.preferred_cust_flag desc nulls first,
    ss.ticket_number asc nulls first

having
    line_item_count between 15 and 20;`

  ```text

  ```
- `trilogy file write query36.preql --content import raw.physical_sales as sales;

# Filter: store sales in year 2001 at stores in TN
where sales.date.year = 20…tion by grouping(sales.item.category) + grouping(sales.item.class)
        order by @gm(sales.net_profit) asc
    ) as within_parent_rank
limit 100;
`

  ```text

  ```
- `trilogy file write query36.preql --content import raw.physical_sales as sales;

# Define the gross margin at three rollup levels
def gm(metric) -> sum(sales.…les.item.category) + grouping(sales.item.class) desc nulls first,
    sales.item.category nulls first,
    within_parent_rank nulls first
limit 100;
`

  ```text

  ```
- `trilogy file write query36.preql --content import raw.physical_sales as sales;

# Define the gross margin at three rollup levels
def gm(metric) -> sum(sales.…   ) as within_parent_rank
order by
    h_level desc nulls first,
    sales.item.category nulls first,
    within_parent_rank nulls first
limit 100;
`

  ```text

  ```
- `trilogy file write query38.preql --content import raw.physical_sales as store;
import raw.catalog_sales as catalog;
import raw.web_sales as web;

# Build pre…ustomer.last_name, store.customer.first_name, store.date.date) as count_in_all_three
where in_store > 0 and in_catalog > 0 and in_web > 0
limit 100;
`

  ```text

  ```
- `trilogy file write query38.preql --content import raw.physical_sales as store;
import raw.catalog_sales as catalog;
import raw.web_sales as web;

# Distinct …uples.last_name in web_tuples.last_name
  and store_tuples.first_name in web_tuples.first_name
  and store_tuples.date in web_tuples.date
limit 100;
`

  ```text

  ```
- `trilogy file write query39.preql`

  ```text

  ```
- `trilogy file write query39.preql`

  ```text

  ```
- `trilogy file write query39.preql`

  ```text

  ```
- `trilogy file write query44.preql`

  ```text

  ```
- `trilogy file write query44.preql`

  ```text

  ```
- `trilogy file write query49.preql --content import raw.all_sales as s;

# Define per-channel, per-item aggregate concepts
auto item_return_qty <- sum(s.return… 10
order by
    channel asc,
    return_quantity_rank asc nulls first,
    return_currency_rank asc nulls first,
    item asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query50.preql --content import raw/catalog_store_returns as csr;

where csr.is_returned = true
and csr.store_return_date.year = 2001
and c…e,
    csr.store.street_type,
    csr.store.suite_number,
    csr.store.city,
    csr.store.county,
    csr.store.state,
    csr.store.zip
limit 100;`

  ```text

  ```
- `trilogy run --import raw/catalog_store_returns:csr select csr.store.name as store_name, count(*) limit 5;`

  ```text

  ```
- `trilogy run --import raw/catalog_store_returns:csr select csr.store_return_date.year, csr.store_return_date.month_of_year, count(csr.ticket_number) as cnt where csr.is_returned = true group by 1,2 order by 1,2 limit 20;`

  ```text

  ```
- `trilogy file write query51.preql --content import raw.all_sales as sales;

# Filter to year 2000, web and store channels ONLY
where year(sales.date.date) = 2…  web_rt,
    store_rt,
    web_rmax,
    store_rmax
having web_rmax > store_rmax
order by item asc nulls first, sale_date asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query54.preql -e -c import raw/catalog_sales as cat_sales;
import raw/web_sales as web_sales;
import raw/physical_sales as store_sales;

#…ric) / 50, 0) * 50 as segment_times_50
order by
    segment asc nulls first,
    customer_count asc nulls first,
    segment_times_50 asc
limit 100;
`

  ```text

  ```
- `trilogy file write query54.preql -e -c import raw.catalog_sales as cat_sales;
import raw.web_sales as web_sales;
import raw.physical_sales as store_sales;

#…ric) / 50, 0) * 50 as segment_times_50
order by
    segment asc nulls first,
    customer_count asc nulls first,
    segment_times_50 asc
limit 100;
`

  ```text

  ```
- `trilogy file write query57.preql --content import raw.catalog_sales as cs;

# Filter to the relevant months: Dec 1998, all months of 1999, Jan 2000
where (cs…e,
    cs.sold_date.year,
    cs.sold_date.month_of_year,
    avg_monthly,
    monthly_total,
    prior_month_total,
    next_month_total
limit 100;
`

  ```text

  ```
- `trilogy file write query59.preql`

  ```text

  ```
- `trilogy file write query59.preql`

  ```text

  ```
- `trilogy file write query59.preql`

  ```text

  ```
- `trilogy run --import raw.item:item select item.text_id, item.category where item.category='Music' and item.text_id in (select item2.text_id from raw.item as item2 where item2.category!='Music') limit 10;`

  ```text

  ```
- `trilogy run --import raw.item:item select item.text_id, count(item.id) as cnt, count_distinct(item.category) as cats where item.category = 'Music' group by item.text_id having count_distinct(item.category)>1 limit 10;`

  ```text

  ```
- `trilogy run --import raw.item:item select substring(item.text_id,1,8) as prefix, count_distinct(item.category) as cats, count(item.id) as cnt by (substring(item.text_id,1,8)) order by cnt desc limit 20;`

  ```text

  ```
- `trilogy run --import raw.item:item select count(item.id) as cnt by (substring(item.text_id,1,8)) order by cnt desc limit 20;`

  ```text

  ```
- `trilogy file write query64.preql`

  ```text

  ```
- `trilogy file write query64.preql`

  ```text

  ```
- `trilogy file write query64.preql`

  ```text

  ```
- `trilogy file write query64.preql`

  ```text

  ```
- `trilogy file write query64.preql`

  ```text

  ```
- `trilogy file write query67.preql`

  ```text

  ```
- `trilogy file write query70.preql`

  ```text

  ```
- `trilogy file write query70.preql`

  ```text

  ```
- `trilogy file write query75.preql -c import raw.all_sales as all_sales;

# Deduplicate at line-item grain (item, order_id, sales_channel) using row_one
# and …  ), 0) as amt_diff
having yearly_agg.yr = 2002
    and prev_qty is not null
    and curr_qty / prev_qty < 0.9
order by qty_diff, amt_diff
limit 100;`

  ```text

  ```
- `trilogy file write query76.preql --content import raw.physical_sales as store_sales;
import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_s…nnel asc nulls first,
    missing_ref asc nulls first,
    year asc nulls first,
    quarter asc nulls first,
    category asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query76.preql --content import raw.physical_sales as store_sales;
import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_s…rder by channel asc nulls first,
    missing_ref asc nulls first,
    yr asc nulls first,
    qtr asc nulls first,
    cat asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query76.preql --content import raw.physical_sales as store_sales;
import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_s…sales_price_sum
order by ch asc nulls first,
    ref asc nulls first,
    y asc nulls first,
    q asc nulls first,
    ct asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query76.preql --content import raw.physical_sales as store_sales;
import raw.web_sales as web_sales;

# Store channel
select
    'store' a…sales_price_sum
order by ch asc nulls first,
    ref asc nulls first,
    y asc nulls first,
    q asc nulls first,
    ct asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query76.preql --content import raw.physical_sales as store_sales;
import raw.web_sales as web_sales;

# Store channel
select
    'store' a…sales_price_sum
order by ch asc nulls first,
    ref asc nulls first,
    y asc nulls first,
    q asc nulls first,
    ct asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query76.preql --content import raw.physical_sales as store_sales;
import raw.web_sales as web_sales;

# Store channel
select
    'store' a…sales_price_sum
order by ch asc nulls first,
    ref asc nulls first,
    y asc nulls first,
    q asc nulls first,
    ct asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query76.preql --content import raw.physical_sales as store_sales;

select
    'store' as channel,
    'store reference' as missing_ref,
  …rder by channel asc nulls first,
    missing_ref asc nulls first,
    yr asc nulls first,
    qtr asc nulls first,
    cat asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query76.preql --content import raw.physical_sales as store_sales;
import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_s…rder by channel asc nulls first,
    missing_ref asc nulls first,
    yr asc nulls first,
    qtr asc nulls first,
    cat asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query76.preql --content -- Store channel: lines where store reference is null
import raw.physical_sales as store_sales;

select
    'store…ore.id is null
order by channel asc nulls first, missing_ref asc nulls first, yr asc nulls first, qtr asc nulls first, cat asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query77.preql -e -c import raw.all_sales as all_sales;

# Channel labels
auto channel_label <-
    case
        when all_sales.sales_chann…ales.net_profit, period_return_loss) as profit
order by
    channel_label asc nulls first,
    outlet_id asc nulls first,
    returns desc
limit 100;`

  ```text

  ```
- `trilogy file write query77.preql -e -c import raw.all_sales as all_sales;

# Channel labels
auto channel_label <-
    case
        when all_sales.sales_chann…period_returns, period_return_loss) as metrics
order by
    channel_label asc nulls first,
    outlet_id asc nulls first,
    returns desc
limit 100;`

  ```text

  ```
- `trilogy file write query78.preql --content import raw.physical_sales as store;
import raw.web_sales as web;
import raw.catalog_sales as catalog;

# Merge cus…ustomer.id is not null and catalog.return_amount is null)
    --    by store.date.year, store.item.id, store.customer.id, 0) as _other_qty
limit 100;`

  ```text

  ```
- `trilogy file write query78.preql --content import raw.physical_sales as store;
import raw.web_sales as web;
import raw.catalog_sales as catalog;

merge store…ustomer.id is not null and catalog.return_amount is null)
    --    by store.date.year, store.item.id, store.customer.id, 0) as _other_qty
limit 100;`

  ```text

  ```
- `trilogy file write query80.preql --content import raw.all_sales as sales;

# Channel label
auto channel_label <- case(
    when sales.sales_channel = 'STORE'…
    sum(sales.net_profit) - coalesce(sum(sales.return_net_loss), 0) as profit
order by channel nulls first, outlet_identifier nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query80.preql --content import raw.all_sales as sales;

# Channel label
auto channel_label <- case
    when sales.sales_channel = 'STORE' …er,
    @rollup_metrics(sales.net_paid, sales.return_amount, sales.net_profit)
order by channel nulls first, outlet_identifier nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query80.preql --content import raw.all_sales as sales;

# Channel label
auto channel_label <- case
    when sales.sales_channel = 'STORE' …up_sales(sales.net_profit) - @rollup_net_loss(sales.return_net_loss) as profit
order by channel nulls first, outlet_identifier nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query80.preql --content import raw.all_sales as sales;

# Channel label
auto channel_label <- case
    when sales.sales_channel = 'STORE' …returns,
    @rollup_profit(sales.net_profit, sales.return_net_loss) as profit
order by channel nulls first, outlet_identifier nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query80.preql --content import raw.all_sales as sales;

# Channel label
auto channel_label <- case
    when sales.sales_channel = 'STORE' …@rollup_netp(sales.net_profit) - @rollup_netl(sales.return_net_loss) as profit
order by channel nulls first, outlet_identifier nulls first
limit 100;`

  ```text

  ```
- `trilogy run --import raw.date:date select date.date, date.week_seq, date.year from date where date.date in ('2000-06-30'::date, '2000-09-27'::date, '2000-11-17'::date);`

  ```text

  ```
- `trilogy file write query83.preql`

  ```text

  ```
- `trilogy file write query83.preql`

  ```text

  ```
- `trilogy file write query86.preql --content import raw.web_sales as ws;

where ws.date.year = 2000

# Rollup aggregates at (category, class), (category), and …al_net_paid,
    hierarchy_level,
    rnk
order by hierarchy_level desc nulls first, ws.item.category asc nulls first, rnk asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query91.preql --content import raw.catalog_returns as cr;
import raw.call_center as cc;
import raw.customer as cust;
import raw.customer_d… and cr.date.month_of_year = 11
select
  cr.call_center.text_id,
  cr.call_center.name,
  cr.call_center.manager,
  net_loss
order by net_loss desc;
`

  ```text

  ```
- `trilogy file write query95.preql --content import raw.web_sales as ws;

# Define orders that meet the basic criteria
# (ship date in range, IL ship address, …as order_count,
    sum(ws.ext_ship_cost) as total_extended_ship_cost,
    sum(ws.net_profit) as total_net_profit
order by order_count asc
limit 100;`

  ```text

  ```
- `trilogy file write query95.preql --content import raw.web_sales as ws;

# Define orders that meet the basic criteria
# (ship date in range, IL ship address, …as order_count,
    sum(ws.ext_ship_cost) as total_extended_ship_cost,
    sum(ws.net_profit) as total_net_profit
order by order_count asc
limit 100;`

  ```text

  ```
- `trilogy file write query95.preql --content import raw.web_sales as ws;

# Staged membership: orders meeting basic criteria
# (ship date in range, IL ship add…as order_count,
    sum(ws.ext_ship_cost) as total_extended_ship_cost,
    sum(ws.net_profit) as total_net_profit
order by order_count asc
limit 100;`

  ```text

  ```

### `undefined-concept`

- `trilogy run --import raw/all_sales:all_sales rowset wk_2001 <- select all_sales.date.week_seq where all_sales.sales_channel != 'STORE' and all_sales.date.yea…2001, all_sales.date.week_seq as ws, wk_sun as sun where all_sales.sales_channel != 'STORE' having ws in wk_2001.ws order by ws nulls first limit 10;`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales rowset ws_2001 <- select all_sales.date.week_seq where all_sales.sales_channel != 'STORE' and all_sales.date.yea…ales.date.week_seq in ws_2001.week_seq or all_sales.date.week_seq - 53 in ws_2001.week_seq) and all_sales.date.week_seq <= 5375 order by ws limit 10;`

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
- `trilogy run query11.preql`

  ```text

  ```
- `trilogy run query26.preql`

  ```text

  ```
- `trilogy run query41.preql`

  ```text

  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy run --import raw/catalog_store_returns:csr select store.name, store.company_id, store.street_number, store.street_name, store.street_type, store.suite_number, store.city, store.county, store.state, store.zip, min(store_sale_date.date) as sale_date_min limit 5;`

  ```text

  ```
- `trilogy run query52.preql`

  ```text

  ```
- `trilogy run query54.preql`

  ```text

  ```
- `trilogy run query59.preql`

  ```text

  ```
- `trilogy run --import raw.all_sales:as select sales.item.text_id where sales.item.text_id is null limit 5;`

  ```text

  ```
- `trilogy run query63.preql`

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
- `trilogy run query71.preql`

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
- `trilogy run query78.preql`

  ```text

  ```
- `trilogy run query82.preql`

  ```text

  ```
- `trilogy run query89.preql`

  ```text

  ```
- `trilogy run query96.preql`

  ```text

  ```

### `syntax-missing-alias`

- `trilogy run --import raw/all_sales:all_sales select min(date.year), max(date.year), min(date.week_seq), max(date.week_seq) limit 10;`

  ```text

  ```
- `trilogy run --import raw/all_sales:all_sales select distinct all_sales.date.day_of_week as dow, all_sales.date.day_name as dn order by dow limit 10;`

  ```text

  ```
- `trilogy run --import raw.physical_sales:store_sales select store_sales.date.year, count(store_sales.line_item) limit 10;`

  ```text

  ```
- `trilogy run --import raw.catalog_store_returns:csr select csr.store_sale_date.quarter_name, count(csr.store_quantity) limit 10;`

  ```text

  ```
- `trilogy file write query39.preql`

  ```text

  ```
- `trilogy file write query41.preql --content import raw.item as item;

# Define the 8 attribute profiles as a rowset (distinct manufact values from profile-mat…manufacturer_id between 1 and 500
  and item.manufact in profile_manus.manufact
select 
  distinct item.product_name
order by product_name
limit 100;`

  ```text

  ```
- `trilogy run --import raw/catalog_store_returns:csr select store.name, store.company_id, store.street_number, store.street_name, store.street_type, store.suite_number, store.city, store.county, store.state, store.zip, min(store_sale_date.date) limit 5;`

  ```text

  ```
- `trilogy file write query53.preql --content import raw.physical_sales as store_sales;

# Only store sales in year 2000
where store_sales.date.year = 2000
# Re…n for having filter
  --sum(store_sales.sales_price) by (store_sales.item.manufacturer_id, store_sales.date.quarter) as per_quarter_total
  limit 10;`

  ```text

  ```
- `trilogy run --import raw.all_sales:all_sales select count(all_sales.item.text_id) where all_sales.item.color in ('slate','blanched','burnished') limit 10;`

  ```text

  ```
- `trilogy file write query59.preql`

  ```text

  ```
- `trilogy run --import raw.physical_sales:ss select ss.store.name, ss.store.text_id, ss.date.week_seq, ss.date.year, sum(ss.sales_price) filter where ss.date.day_of_week = 0, sum(ss.sales_price) filter where ss.date.day_of_week = 1 where ss.store.name is null and ss.date.week_seq = 5322 limit 10;`

  ```text

  ```
- `trilogy run --import raw.date:d select min(d.week_seq), max(d.week_seq) where d.year = 2001;`

  ```text

  ```
- `trilogy run --import raw.date:d select min(d.week_seq), max(d.week_seq) where d.year = 2002;`

  ```text

  ```
- `trilogy run --import raw.item:item select distinct substring(item.text_id,1,8) as prefix;`

  ```text

  ```
- `trilogy file write query77.preql -e -c import raw.all_sales as all_sales;

# Channel labels
auto channel_label <-
    case
        when all_sales.sales_chann…nnel_label, all_sales.channel_dim_id as profit
order by
    channel_label asc nulls first,
    outlet_id asc nulls first,
    returns desc
limit 100;`

  ```text

  ```
- `trilogy file write query80.preql --content import raw.all_sales as sales;

# Channel label
auto channel_label <- case
    when sales.sales_channel = 'STORE' …ce(sum(sales.return_net_loss), 0) by rollup channel_label, outlet_id as profit
order by channel nulls first, outlet_identifier nulls first
limit 100;`

  ```text

  ```
- `trilogy run --import raw.physical_sales:sales select distinct sales.item.text_id, sales.item.current_price, sales.item.manufacturer_id where sales.item.current_price between 62 and 92 and sales.item.manufacturer_id in (129, 270, 821, 423) limit 20;`

  ```text

  ```
- `trilogy file write query82.preql -e -c import raw.physical_sales as sales;
import raw.inventory as inv;

# Items that had between 100 and 500 units on hand (…em.text_id as item_code,
    sales.item.desc as description,
    sales.item.current_price as current_price
order by
    sales.item.text_id
limit 100;`

  ```text

  ```

### `join-resolution`

- `trilogy run query06.preql`

  ```text

  ```
- `trilogy run query20.preql`

  ```text

  ```
- `trilogy run query42.preql`

  ```text

  ```
- `trilogy run query43.preql`

  ```text

  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy run query55.preql`

  ```text

  ```
- `trilogy run query70.preql`

  ```text

  ```
- `trilogy run query76.preql`

  ```text

  ```
- `trilogy run query76.preql`

  ```text

  ```
- `trilogy run query84.preql`

  ```text

  ```
- `trilogy run query99.preql`

  ```text

  ```

### `cli-misuse`

- `trilogy explore raw/store_sales.preql --show all`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy run query49.preql --format json`

  ```text
  '--format' is not a valid dialect. The dialect argument comes after the input file and any options.
    Try: trilogy run <input> [<dialect>]
  ```
- `trilogy run query77_preview.preql --format json`

  ```text
  '--format' is not a valid dialect. The dialect argument comes after the input file and any options.
    Try: trilogy run <input> [<dialect>]
  ```

### `type-error`

- `trilogy run query90.preql`

  ```text

  ```
