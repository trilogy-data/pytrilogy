# Trilogy failure analysis — 20260606-032823

- Run `20260606-032822_ingest` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 2464 | failed: 329 (13%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 107 | 33% |
| `other` | 102 | 31% |
| `undefined-concept` | 65 | 20% |
| `join-resolution` | 24 | 7% |
| `syntax-missing-alias` | 23 | 7% |
| `type-error` | 4 | 1% |
| `cli-misuse` | 2 | 1% |
| `file-not-found` | 2 | 1% |

## Detail

### `syntax-parse`

- `trilogy run --import raw.date_dim:dd select dd.week_seq, dd.year, dd.date where dd.year in (2001, 2002) order by dd.week_seq limit 30 offset 350;`

  ```text

  ```
- `trilogy file write query02.preql`

  ```text

  ```
- `trilogy run --import raw/store_sales:ss select ss.store.store_id, sum(ss.ext_sales_price) as total_sales, sum(ss.net_profit) as total_profit where ss.date_dim.date between '2000-08-23'::date and '2000-09-06'::date and ss.store.store_sk is not null group by ss.store.store_id limit 10;`

  ```text

  ```
- `trilogy run --import raw/store_sales:ss --import raw/store_returns:sr where ss.date_dim.date between '2000-08-23'::date and '2000-09-06'::date and ss.store.s…, coalesce(returns_amt,0)->total_returns, coalesce(gross_sales,0) - coalesce(return_loss,0) as net_profit order by store_id asc nulls first limit 20;`

  ```text

  ```
- `trilogy run --import raw/store_sales:ss --import raw/store_returns:sr where sr.date_dim.date between '2000-08-23'::date and '2000-09-06'::date and sr.store.s…ot in (select ss.store.store_id where ss.date_dim.date between '2000-08-23'::date and '2000-09-06'::date and ss.store.store_sk is not null) limit 10;`

  ```text

  ```
- `trilogy run --import raw/store_sales:ss --import raw/store_returns:sr --import raw/catalog_sales:cs --import raw/catalog_returns:cr where ss.date_dim.date be…al_gross, coalesce(sr_a,0)+coalesce(cra,0)->total_returns, (coalesce(sp,0)+coalesce(cp,0)) - (coalesce(sr_l,0)+coalesce(crl,0))->net_profit limit 20;`

  ```text

  ```
- `trilogy file write query06.preql --content import raw.store_sales as ss;

# Average current price of distinct items per category (each item counted once)
aut… as state,
    count(ss.item.item_sk, ss.ticket_number) as line_item_count
order by line_item_count asc nulls first, state asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query14.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Merge item dimensions…em.category_id) desc,
    channel nulls first, ss.item.brand_id nulls first, ss.item.class_id nulls first, ss.item.category_id nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query14.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Merge item dimensions…em.category_id) desc,
    channel nulls first, ss.item.brand_id nulls first, ss.item.class_id nulls first, ss.item.category_id nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query14.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Merge item dimensions…em.category_id) desc,
    channel nulls first, ss.item.brand_id nulls first, ss.item.class_id nulls first, ss.item.category_id nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query14.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Merge item dimensions…id) desc,
    channel nulls first,
    ss.item.brand_id nulls first,
    ss.item.class_id nulls first,
    ss.item.category_id nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query16.preql --content import raw/catalog_sales as cs;
import raw/catalog_returns as cr;

# Orders that have catalog returns
auto ret_ord…number) as order_count,
    sum(cs.ext_ship_cost) as total_ship_cost,
    sum(cs.net_profit) as total_net_profit
order by order_count desc
limit 100;`

  ```text

  ```
- `trilogy file write query18.preql --content import raw.catalog_sales as cs;

# Filter: catalog sales in year 1998
# billing customer demographics: gender = 'F…ill_customer.customer_address.state nulls first,
    cs.bill_customer.customer_address.county nulls first,
    cs.item.item_id nulls first
limit 100;`

  ```text

  ```
- `trilogy run --import raw.store_sales:ss select year(ss.date_dim.date) as yr, count(ss.ticket_number) as cnt group by yr order by yr limit 20;`

  ```text

  ```
- `trilogy run --import raw/store_sales:ss select --year(ss.date_dim.date) as yr limit 20 having yr is not null;`

  ```text

  ```
- `trilogy run --import raw/store_sales:ss select --substring(ss.item.item_desc,1,30) as prefix, count(ss.ticket_number ? year(ss.date_dim.date) between 2000 and 2003) as cnt by (substring(ss.item.item_desc,1,30), ss.date_dim.date_sk) having cnt > 4 order by cnt desc limit 20;`

  ```text

  ```
- `trilogy run --import raw/store_sales:ss auto ss_cust_total <- sum(ss.quantity * ss.sales_price ? year(ss.date_dim.date) between 2000 and 2003 and ss.customer…lect ss.customer.customer_sk as sk, ss_cust_total as total, max_total as m total having ss_cust_total > 0.5 * max_total order by total desc limit 50;`

  ```text

  ```
- `trilogy run --import raw/store_sales:ss --import raw/catalog_sales:cs auto item_date_pair_cnt <- count_distinct(ss.item.item_sk || '-' || ss.date_dim.date_sk…mber) as cnt where year(cs.sold_date.date) = 2000 and month(cs.sold_date.date) = 2 and substring(cs.item.item_desc,1,30) in frequent_prefix limit 20;`

  ```text

  ```
- `trilogy run --import raw/store_sales:ss --import raw/catalog_sales:cs auto pair_count <- count(ss.ticket_number ? year(ss.date_dim.date) between 2000 and 200…r.customer_sk as sk where year(cs.sold_date.date) = 2000 and month(cs.sold_date.date) = 2 and cs.bill_customer.customer_sk in (select ...) limit 100;`

  ```text

  ```
- `trilogy run --import raw/store_sales:ss import raw.item as item; auto pair_count <- count_distinct(ss.item.item_sk, ss.date_dim.date_sk ? year(ss.date_dim.da…ring(ss.item.item_desc,1,30); select substring(ss.item.item_desc,1,30) as prefix, pair_count having pair_count > 4 order by pair_count desc limit 20;`

  ```text

  ```
- `trilogy run --import raw/store_sales:ss select count(ss.ticket_number) as pair_count by (substring(ss.item.item_desc,1,30), ss.date_dim.date_sk) having pair_count > 4 order by pair_count desc limit 20;`

  ```text

  ```
- `trilogy run --import raw/store_sales:ss --import raw/catalog_sales:cs --import raw/web_sales:ws merge cs.bill_customer.customer_sk into ~ws.bill_customer.cus…ing(ws.item.item_desc,1,30) in frequent_item_prefix and ws.bill_customer.customer_sk in best_cust_sk group by cs.bill_customer.customer_sk limit 100;`

  ```text

  ```
- `trilogy file write query24.preql --content import raw.store_sales as sales;
import raw.store_returns as returns;

# Match store sales to store returns by tic…_subtotal > 0.05 * stage1_avg
order by
    sales.customer.last_name asc,
    sales.customer.first_name asc,
    sales.store.store_name asc
limit 100;`

  ```text

  ```
- `trilogy file write query24.preql --content import raw.store_sales as sales;
import raw.store_returns as returns;

# Match store sales to store returns by tic…subtotal > 0.05 * stage1_avg

order by
    sales.customer.last_name asc,
    sales.customer.first_name asc,
    sales.store.store_name asc
limit 100;`

  ```text

  ```
- `trilogy file write query24_debug5.preql --content import raw.store_sales as sales;
import raw.store_returns as returns;

merge sales.ticket_number into ~retu…nd sales.customer.customer_address.zip = sales.store.zip
  and returns.ticket_number is not null

select
    count(*) as cnt,
    --avg_all
limit 10;`

  ```text

  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.item.item_id, store_sales.store.state, count(store_sales.ticket_number) as cnt_by grouping sets ((store_sales.item.item_id, store_sales.store.state), (store_sales.item.item_id), ()) limit 20;`

  ```text

  ```
- `trilogy run --import raw.store_sales:store_sales where store_sales.date_dim.year = 2002 and store_sales.store.state = 'TN' and store_sales.customer_demograph…_sales.customer_demographics.education_status = 'College' select sum(1) as cnt_by rollup store_sales.item.item_id, store_sales.store.state limit 100;`

  ```text

  ```
- `trilogy file write query29.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;

where
  ss.date_dim…quantity,
  sum(cs.quantity) as total_catalog_sale_quantity

order by
  item_code asc,
  item_desc asc,
  store_code asc,
  store_name asc
limit 100;`

  ```text

  ```
- `trilogy file write query29.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;

inner join sr.ticke…quantity,
  sum(cs.quantity) as total_catalog_sale_quantity

order by
  item_code asc,
  item_desc asc,
  store_code asc,
  store_name asc
limit 100;`

  ```text

  ```
- `trilogy file write query31.preql`

  ```text

  ```
- `trilogy file write query31.preql`

  ```text

  ```
- `trilogy file write query31.preql`

  ```text

  ```
- `trilogy file write query31.preql`

  ```text

  ```
- `trilogy file write query31.preql`

  ```text

  ```
- `trilogy file write query35.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sales as cs;

# Merge customer concep…
  marital_status asc nulls first,
  dep_count asc nulls first,
  dep_employed_count asc nulls first,
  dep_college_count asc nulls first

limit 100;`

  ```text

  ```
- `trilogy file write query35.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sales as cs;

# Merge customer concep…
  marital_status asc nulls first,
  dep_count asc nulls first,
  dep_employed_count asc nulls first,
  dep_college_count asc nulls first

limit 100;`

  ```text

  ```
- `trilogy file write query35.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sales as cs;

# Store sales customers…sold_date.year = 2002
      and ws.sold_date.qoy in (1, 2, 3)
    select ws.bill_customer.customer_sk
  )

select
  ss.customer.customer_sk
limit 10;`

  ```text

  ```
- `trilogy file write query36.preql --content import raw.store_sales as store_sales;

# Filter condition: year 2001, stores in TN
auto filtered_sales <- store_s…thin_parent_rank
order by
  hierarchy_level desc nulls first,
  filtered_sales.item.category nulls first,
  within_parent_rank nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query39.preql --content import raw.inventory as inv;
import raw.warehouse as wh;
import raw.item as it;
import raw.date_dim as dd;

# Defi…rst,
    jan_mean asc nulls first,
    jan_cv asc nulls first,
    feb_moy asc nulls first,
    feb_mean asc nulls first,
    feb_cv asc nulls first;`

  ```text

  ```
- `trilogy file write query44.preql --content import raw.store_sales as store_sales;

# Global threshold: 90% of avg net profit where address is null at store 1…rmer_product_name,
    coalesce(worst_name, '') -> worst_performer_product_name
order by best_pos_num asc, best_name desc, worst_name desc
limit 100;`

  ```text

  ```
- `trilogy file write query44.preql --content import raw.store_sales as store_sales;

# Global threshold: 90% of avg net profit where address is null at store 1…_num, 0) -> rank,
    coalesce(best_name, '') -> best_performer_product_name,
    coalesce(worst_name, '') -> worst_performer_product_name
limit 100;`

  ```text

  ```
- `trilogy file write query49.preql --content import raw.web_sales as web_sales;
import raw.web_returns as web_returns;
import raw.catalog_sales as catalog_sale…001
    and web_sales.net_profit > 0
    and web_sales.net_paid > 0
    and web_sales.quantity > 0;

select 'web' as channel, web_sales.item.item_sk;`

  ```text

  ```
- `trilogy file write query49.preql --content import raw.web_sales as ws;
import raw.web_returns as wr;

# Web channel
select 
  'web' as channel,
  ws.item.ite…onth_seq = 13
  and ws.net_profit > 0
  and ws.net_paid > 0
  and ws.quantity > 0
  and wr.return_amt > 10000
  group by ws.item.item_sk
  limit 100;`

  ```text

  ```
- `trilogy file write query49.preql --content import raw.web_sales as ws;
import raw.web_returns as wr;

# Web channel - inner join web_sales and web_returns on… and ws.quantity > 0
  and wr.return_amt > 10000
inner join wr.order_number = ws.order_number
inner join wr.item.item_sk = ws.item.item_sk
limit 100;`

  ```text

  ```
- `trilogy file write query49.preql --content import raw.web_sales as ws;
import raw.web_returns as wr;
import raw.catalog_sales as cs;
import raw.catalog_retur…eturn_quantity_rank <= 10 or return_currency_rank <= 10
order by return_quantity_rank asc, return_currency_rank asc, ss.item.item_sk asc;

limit 100;`

  ```text

  ```
- `trilogy file write query51.preql --content import raw.web_sales as ws;
import raw.store_sales as ss;

merge ws.item.item_sk into ~ss.item.item_sk;

where ws.…c) as store_running_max
having
    web_running_max > store_running_max
order by
    item_id asc nulls first,
    sale_date asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query51.preql --content import raw.web_sales as ws;
import raw.store_sales as ss;

merge ws.item.item_sk into ~ss.item.item_sk;

where ws.… date asc) as store_running_max
having
    web_running_max > store_running_max
order by
    item asc nulls first,
    date asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query51.preql --content import raw.web_sales as ws;
import raw.store_sales as ss;

merge ws.item.item_sk into ~ss.item.item_sk;

where ws.… date asc) -> store_running_max
having
    web_running_max > store_running_max
order by
    item asc nulls first,
    date asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query51.preql --content import raw.web_sales as ws;
import raw.store_sales as ss;

merge ws.item.item_sk into ~ss.item.item_sk;

rowset co…re_running_max
where
    web_running_max > store_running_max
order by
    combined.item asc nulls first,
    combined.date asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query54.preql --content import raw.catalog_sales as cs;
import raw.web_sales as ws;
import raw.store_sales as ss;

merge cs.bill_customer.…   segment_val,
    count(ss.customer.customer_sk) as customer_count
order by segment_val asc nulls first, customer_count asc nulls first
limit 100;
`

  ```text

  ```
- `trilogy file write query54.preql --content import raw.catalog_sales as cs;
import raw.web_sales as ws;
import raw.store_sales as ss;

merge cs.bill_customer.…nd(customer_totals.total / 50) as segment_times_50
order by segment asc nulls first, customer_count asc nulls first, segment_times_50 asc
limit 100;
`

  ```text

  ```
- `trilogy file write query54.preql --content import raw.catalog_sales as cs;
import raw.web_sales as ws;
import raw.store_sales as ss;

merge cs.bill_customer.…lifying_customers_web.customer_sk
select
    segment_val,
    cnt_by_seg
order by segment_val asc nulls first, cnt_by_seg asc nulls first
limit 100;
`

  ```text

  ```
- `trilogy file write query58.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;
import raw.date_dim as d…c
    and s between 0.9 * w and 1.1 * w
    and c between 0.9 * w and 1.1 * w
order by ss.item.item_id asc nulls first, s asc nulls first
limit 100;
`

  ```text

  ```
- `trilogy run --import raw/date_dim:date_dim select date_dim.year, min(date_dim.week_seq) as min_ws, max(date_dim.week_seq) as max_ws where date_dim.year between 2000 and 2003 group by date_dim.year order by date_dim.year;`

  ```text

  ```
- `trilogy run --import raw/store_sales:ss select ss.store.store_name as sn, ss.store.store_id as si, ss.date_dim.year as yr, ss.date_dim.week_seq as ws, ss.dat…s dow, sum(ss.sales_price) as sp where ss.date_dim.year in (2001, 2002) group by sn, si, yr, ws, dow having sp > 0 order by sn, si, ws, dow limit 10;`

  ```text

  ```
- `trilogy run --import raw/date_dim:dd select dd.year, dd.week_seq, min(dd.date) as first_day where dd.week_seq in (5322, 5323) group by dd.year, dd.week_seq order by dd.week_seq limit 5;`

  ```text

  ```
- `trilogy run --import raw/store_sales:ss select ss.store.store_name as sn, ss.store.store_id as si, count(ss.ticket_number) as total_sales, min(ss.date_dim.we…ek_seq) as max_ws, count(distinct ss.date_dim.week_seq) as num_weeks where ss.date_dim.year in (2001, 2002) group by sn, si order by sn, si limit 20;`

  ```text

  ```
- `trilogy file write test59k.preql -e -c import raw.store_sales as ss;

# 2001 values at grain (store_sk, week_seq)
auto sun_01 <- sum(ss.sales_price ? ss.date…ame as sn,
    ss.store.store_id as si,
    ss.date_dim.week_seq as ws,
    sun_01 as sun_this,
    sun_02 as sun_next
order by sn, si, ws
limit 100;`

  ```text

  ```
- `trilogy file write test59k.preql -e -c import raw.store_sales as ss;

# 2001 values at grain (store_sk, week_seq)
auto sun_01 <- sum(ss.sales_price ? ss.date…ame as sn,
    ss.store.store_id as si,
    ss.date_dim.week_seq as ws,
    sun_01 as sun_this,
    sun_02 as sun_next
order by sn, si, ws
limit 100;`

  ```text

  ```
- `trilogy run --import raw.item:item select item.category, item.item_id, substr(item.item_id, 1, 8) as prefix limit 30;`

  ```text

  ```
- `trilogy run --import raw.item:item select item.class_id, item.class, count(item.item_sk) as cnt where item.category = 'Music' group by item.class_id, item.class;`

  ```text

  ```
- `trilogy run --import raw.item:item select item.category, count(item.item_sk) as cnt where substring(item.item_id, 1, 5) in (select distinct substring(item.item_id, 1, 5) where item.category = 'Music') group by item.category;`

  ```text

  ```
- `trilogy run --import raw.item:item auto music_item_grp <- substring(item.item_id, 1, 5) ? (item.category = 'Music'); select item.category, substring(item.ite…1, 5) as grp, count(item.item_sk) as cnt where substring(item.item_id, 1, 5) in music_item_grp group by item.category, substring(item.item_id, 1, 5);`

  ```text

  ```
- `trilogy file write query64.preql --content # TPC-DS Query 64
# Identify items sold in store sales where cumulative catalog ext_list_price > 2x catalog refund…ata.total_coupon_amt
order by
  agg_data.product_name,
  agg_data.store_name,
  agg_data.sale_line_count,
  agg_data.total_wholesale_cost
limit 100;
`

  ```text

  ```
- `trilogy file write query66.preql --content import raw.web_sales as web;
import raw.catalog_sales as cat;

# Merge warehouse dimension so both facts share it
…ales_per_sq_ft,
    coalesce(web_net_amt, 0) + coalesce(cat_net_amt, 0) as monthly_net
order by name asc nulls first, year asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query66.preql --content import raw.web_sales as web;
import raw.catalog_sales as cat;

# Merge warehouse dimension so both facts share it
…d as monthly_sales_per_sq_ft,
    coalesce(wnet, 0) + coalesce(cnet, 0) as monthly_net
order by name asc nulls first, year asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query66.preql --content import raw.web_sales as web;
import raw.catalog_sales as cat;

# Merge warehouse dimension so both facts share it
…d as monthly_sales_per_sq_ft,
    coalesce(wnet, 0) + coalesce(cnet, 0) as monthly_net
order by name asc nulls first, year asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query66.preql --content import raw.web_sales as web;
import raw.catalog_sales as cat;
import raw.date_dim as dd;

# Merge warehouse dimens…t -> monthly_sales_per_sq_ft,
    coalesce(wnet, 0) + coalesce(cnet, 0) -> monthly_net
order by name asc nulls first, year asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query70.preql --content import raw.store_sales as store_sales;

# For store sales in year 2000, report total net profit rolled up
# by sto…l_net_profit,
    hierarchy_level,
    rnk as rank
order by
    hierarchy_level desc,
    store_sales.store.state nulls first,
    rnk asc
limit 100;`

  ```text

  ```
- `trilogy run --import raw.catalog_sales:cs --import raw.inventory:inv merge cs.item.item_sk into ~inv.item.item_sk; select count(cs.order_number) as cnt_all, …nv.quantity_on_hand < cs.quantity and cs.sold_date.week_seq = inv.date_dim.week_seq group by cs.promotion.promo_sk, cs.promotion.promo_name limit 20;`

  ```text

  ```
- `trilogy file write query75.preql`

  ```text

  ```
- `trilogy file write query76.preql -c import raw.store_sales as store_sales;
import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_sales;

# P…xt_sales_price
order by channel asc nulls first, missing_ref asc nulls first, yr asc nulls first, qtr asc nulls first, cat asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query76.preql -c import raw.store_sales as store_sales;
import raw.web_sales as web_sales;

where store_sales.store.store_sk is null
selec…xt_sales_price
order by channel asc nulls first, missing_ref asc nulls first, yr asc nulls first, qtr asc nulls first, cat asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query76.preql -c import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sales as cs;

# Store sales: selling store r…tot -> total_ext_sales_price
order by ch asc nulls first, mr asc nulls first, yr asc nulls first, qtr asc nulls first, ctg asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query76.preql -c import raw.store_sales as ss;

# Store: null store reference
auto ss_lines <- count(ss.ticket_number ? ss.store.store_sk …xt_sales_price
order by channel asc nulls first, missing_ref asc nulls first, yr asc nulls first, qtr asc nulls first, cat asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query76.preql -c import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sales as cs;

rowset ss_data <- where ss.sto…xt_sales_price
order by channel asc nulls first, missing_ref asc nulls first, yr asc nulls first, qtr asc nulls first, cat asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query76.preql -c import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sales as cs;

# Compute store counts grouped…nnel,
    'store reference' as missing_ref,
    yr,
    qtr,
    cat,
    ss_cnt as line_count,
    ss_tot as total_ext_sales_price
from ss
limit 10;`

  ```text

  ```
- `trilogy file write query76.preql -c import raw.store_sales as ss;
auto ss_cnt <- count(ss.ticket_number ? ss.store.store_sk is null) by (year(ss.date_dim.dat…) as yr,
    (quarter(ss.date_dim.date)) as qtr,
    ss.item.category as cat,
    ss_cnt as line_count,
    ss_tot as total_ext_sales_price
limit 10;`

  ```text

  ```
- `trilogy file write query77.preql -e -c import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_retur…s, 0) as prof
    inner join store_returns_agg.st_sk = store_sales_agg.st_sk;

select 'test' as t, max(st_sales) as mx from store_sales_agg;
limit 5;`

  ```text

  ```
- `trilogy file write query77.preql -e -c import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_retur…lect channel, outlet_id, sum(sale_amt) as sales, sum(return_amt) as returns, sum(sale_profit) - sum(return_loss) as profit
from store_data
limit 100;`

  ```text

  ```
- `trilogy file write query77.preql -e -c import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_retur…:string as out, sum(ss.ext_sales_price) as sal, 
        0::numeric as ret, sum(ss.net_profit) as prof;

select out, sal, ret, prof from r1 limit 10;`

  ```text

  ```
- `trilogy file write query77.preql -e -c import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_retur…  ss.net_profit as sale_net
    ;

select ch, out_id, sum(sale_amt) as sales, sum(ret_amt) as returns, sum(sale_net) as profit
from st_chan
limit 10;`

  ```text

  ```
- `trilogy file write query78.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.web_sales as ws;
import raw.web_returns a… never returned
where ss.date_dim.year = 2000
and ss.customer.customer_sk is not null
and ss.ticket_number not in returned_store
select 1;

limit 10;`

  ```text

  ```
- `trilogy file write query78.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;

# Never-returned store sales for year 2000 with ide…dim.year, ss.item.item_sk, ss.customer.customer_sk;
  
select store_totals.yr, store_totals.item_sk, store_totals.cust_sk, store_totals.qty
limit 10;`

  ```text

  ```
- `trilogy file write query80.preql --content import raw.store_sales as store_sales;
import raw.store_returns as store_returns;
import raw.catalog_sales as cata…date_dim.date between '2000-08-23'::date and '2000-09-22'::date and store_returns.item.current_price > 50) by store_returns.ticket_number;

select 1;`

  ```text

  ```
- `trilogy file write query80.preql -e -c import raw.store_sales as store_sales;\nimport raw.store_returns as store_returns;\nimport raw.catalog_sales as catalo… -> outlet,\n    sales -> sales,\n    returns -> returns,\n    profit -> profit\norder by channel asc nulls first, outlet asc nulls first\nlimit 100;`

  ```text

  ```
- `trilogy file write query80.preql -e -c import raw.store_sales as store_sales;\nimport raw.store_returns as store_returns;\nimport raw.catalog_sales as catalo…_page_id) as b_outlet,\n    --sum(catalog_sales.ext_sales_price) as b_sales,\n    --coalesce(sum(catalog_sales.net_profit), 0) as b_profit\nselect 1;`

  ```text

  ```
- `trilogy file write query80.preql -e -c import raw.store_sales as store_sales;\nimport raw.store_returns as store_returns;\n\nmerge store_returns.item.item_sk…fit, b_profit\nderive\n    channel -> channel,\n    outlet -> outlet,\n    sales -> sales,\n    returns -> returns,\n    profit -> profit\nlimit 100;`

  ```text

  ```
- `trilogy file write query80.preql -e -c import raw.store_sales as store_sales;\nimport raw.store_returns as store_returns;\n\nmerge store_returns.item.item_sk…_profit, b_profit\nderive\n    channel -> channel\n    outlet -> outlet\n    sales -> sales\n    returns -> returns\n    profit -> profit\nlimit 100;`

  ```text

  ```
- `trilogy file write test_merge.preql -e -c import raw.store_sales as s;\nwhere s.date_dim.date between '2000-08-23'::date and '2000-08-25'::date\nand s.store.…t: a_out, b_out\n    sales: a_sales, b_sales\nderive\n    chan -> channel\n    out -> outlet\n    sales -> sales\norder by channel, outlet\nlimit 10;`

  ```text

  ```
- `trilogy file write test_merge.preql -e -c import raw.store_sales as s;\nimport raw.store_sales as s2;\n\nwhere s.date_dim.date between '2000-08-23'::date and…t: a_out, b_out\n    sales: a_sales, b_sales\nderive\n    chan -> channel\n    out -> outlet\n    sales -> sales\norder by channel, outlet\nlimit 10;`

  ```text

  ```
- `trilogy file write test_merge.preql -e -c import raw.store_sales as s;\nimport raw.store_sales as s2;\n\nwhere s.date_dim.date between '2000-08-23'::date and…alesce(a_chan, b_chan) -> channel\n    coalesce(a_out, b_out) -> outlet\n    coalesce(a_sales, b_sales) -> sales\norder by channel, outlet\nlimit 10;`

  ```text

  ```
- `trilogy file write test_merge.preql -e -c import raw.store_sales as s;\nimport raw.store_sales as s2;\n\nwhere s.date_dim.date between '2000-08-23'::date and…chan) -> channel\n    coalesce(a_out, b_out) -> outlet\n    coalesce(a_sales, 0) + coalesce(b_sales, 0) -> sales\norder by channel, outlet\nlimit 10;`

  ```text

  ```
- `trilogy file write test_rollup6.preql -e -c import raw.store_sales as s;\nimport raw.catalog_sales as cs;\nimport raw.web_sales as ws;\n\nwhere s.date_dim.da…es\nalign\n    out: a_out, b_out\nderive\n    coalesce(a_out, b_out) -> outlet,\n    coalesce(a_sales, 0) + coalesce(b_sales, 0) -> sales\nlimit 100;`

  ```text

  ```
- `trilogy file write test_rollup9.preql -e -c import raw.store_sales as s;\n\nwhere s.date_dim.date between '2000-08-23'::date and '2000-08-25'::date\nand s.st…lect\n    concat('store', s.store.store_id) as outlet,\n    sum(s.ext_sales_price) by rollup (concat('store', s.store.store_id)) as sales\nlimit 100;`

  ```text

  ```
- `trilogy file write query83.preql --content import raw.store_returns as sr;
import raw.catalog_returns as cr;
import raw.web_returns as wr;
import raw.item as… web_return_pct,
    coalesce(avg_3ch, 0) as three_channel_avg

order by
    item.item_id nulls first,
    coalesce(sr_qty, 0) nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query83.preql --content import raw.store_returns as sr;
import raw.catalog_returns as cr;
import raw.web_returns as wr;
import raw.item as… 100.0 * wr_q / t3 / 3.0 as web_return_pct,
    t3 / 3.0 as three_channel_avg

order by
    item.item_id nulls first,
    sr_q nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query85.preql --content import raw.web_sales as ws;
import raw.web_returns as wr;

# Condition 1: both refunded and returning customer dem…nded_cash) as avg_refunded_cash,
  avg(wr.fee) as avg_return_fee

order by reason_desc_20, avg_quantity, avg_refunded_cash, avg_return_fee
limit 100;`

  ```text

  ```
- `trilogy file write query85.preql --content import raw.web_sales as ws;
import raw.web_returns as wr;

# Condition 1: both refunded and returning customer dem…nded_cash) as avg_refunded_cash,
  avg(wr.fee) as avg_return_fee

order by reason_desc_20, avg_quantity, avg_refunded_cash, avg_return_fee
limit 100;`

  ```text

  ```
- `trilogy file write query87.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…s.date
select
    count_distinct(store_sales.customer.last_name, store_sales.customer.first_name, store_sales.date_dim.date) as unique_count
limit 1;`

  ```text

  ```
- `trilogy file write query87.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…(concat(store_sales.customer.last_name, ':', store_sales.customer.first_name, ':', cast(store_sales.date_dim.date, string))) as unique_count
limit 1;`

  ```text

  ```
- `trilogy file write query91.preql --content import raw.catalog_returns as cr;

where cr.date_dim.moy = 11
  and cr.date_dim.year = 1998
  and cr.returning_cus…turning_customer.customer_demographics.marital_status,
  --cr.returning_customer.customer_demographics.education_status
order by total_net_loss desc;`

  ```text

  ```
- `trilogy file write query97.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;

# Build the set of (customer, item) combos from cat…and (ss.customer.customer_sk, ss.item.item_sk) not in (select cat_cust, cat_item from cat_combos)
        then 1 else 0 end) as store_only
limit 100;`

  ```text

  ```
- `trilogy file write query97.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;

# Merge customer across the two domains so they are…e_combos is null) then 1 else 0 end) as catalog_only,
    sum(case when store_combos > 0 and catalog_combos > 0 then 1 else 0 end) as both
limit 100;`

  ```text

  ```
- `trilogy file write query97.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;

merge ss.customer.customer_sk into ~cs.bill_custome…    cust: a_cust, b_cust,
    item: a_item, b_item
derive
    coalesce(store_flag, 0) -> in_store,
    coalesce(cat_flag, 0) -> in_catalog
limit 100;`

  ```text

  ```
- `trilogy run --import raw.catalog_sales:catalog_sales --import raw.warehouse:warehouse select catalog_sales.warehouse.warehouse_name, count(catalog_sales.orde…ship_mode.ship_mode_sk is not null and catalog_sales.call_center.call_center_sk is not null group by catalog_sales.warehouse.warehouse_name limit 10;`

  ```text

  ```
- `trilogy run --import raw.catalog_sales:catalog_sales select count(catalog_sales.order_number) as all_orders, count(catalog_sales.item.item_sk) as all_items, …house.warehouse_sk is not null and catalog_sales.ship_mode.ship_mode_sk is not null and catalog_sales.call_center.call_center_sk is not null limit 1;`

  ```text

  ```

### `other`

- `trilogy run query01.preql`

  ```text

  ```
- `trilogy run --import raw.catalog_sales:catalog_sales --import raw.web_sales:web_sales select catalog_sales.sold_date.week_seq, catalog_sales.sold_date.day_name, sum(catalog_sales.ext_sales_price) + sum(web_sales.ext_sales_price) as total_sales limit 5;`

  ```text

  ```
- `trilogy run query02.preql duckdb`

  ```text

  ```
- `trilogy run query02.preql duckdb`

  ```text

  ```
- `trilogy run --import raw.catalog_sales:cs --import raw.web_sales:ws merge cs.sold_date.date_sk into ~ws.sold_date.date_sk; select cs.sold_date.week_seq, count(cs.sold_date.week_seq) as cnt having cs.sold_date.year = 2001 order by cs.sold_date.week_seq;`

  ```text

  ```
- `trilogy run --import raw.catalog_sales:cs --import raw.web_sales:ws merge cs.sold_date.date_sk into ~ws.sold_date.date_sk; select cs.sold_date.week_seq, cs.sold_date.day_name, sum(cs.ext_sales_price) + sum(ws.ext_sales_price) as total where cs.sold_date.week_seq = 5271 order by cs.sold_date.dow;`

  ```text

  ```
- `trilogy run --import raw.date_dim:dd select dd.week_seq, dd.year where dd.week_seq = 5323 order by dd.date limit 1;`

  ```text

  ```
- `trilogy database describe web_returns`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run --import raw/store_sales:ss --import raw/store_returns:sr merge sr.store.store_sk into ~ss.store.store_sk; select ss.store.store_id, coalesce(sum…8-23'::date and '2000-09-06'::date or sr.date_dim.date between '2000-08-23'::date and '2000-09-06'::date) and ss.store.store_sk is not null limit 10;`

  ```text
  trilogy error: subprocess timed out after 600s.
  ```
- `trilogy run --import raw/store_sales:ss --import raw/store_returns:sr --import raw/catalog_sales:cs --import raw/catalog_returns:cr where ss.date_dim.date be…g_page_id as cr_loss align st_id: st_id, sr_id, cs_id, cr_id derive coalesce(st_gross,0)->total_sales, coalesce(sr_returns,0)->total_returns limit 5;`

  ```text

  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text

  ```
- `trilogy run query11.preql`

  ```text

  ```
- `trilogy run query11.preql`

  ```text

  ```
- `trilogy run query11.preql`

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
- `trilogy run --import raw/store_sales:ss select --substring(ss.item.item_desc,1,30) as prefix, --count(ss.ticket_number ? year(ss.date_dim.date) between 2000 and 2003) by (substring(ss.item.item_desc,1,30), ss.date_dim.date_sk) as pair_count having pair_count > 4 limit 20;`

  ```text

  ```
- `trilogy run --import raw/store_sales:ss auto pair_count <- count(ss.ticket_number ? year(ss.date_dim.date) between 2000 and 2003) by (substring(ss.item.item_desc,1,30), ss.date_dim.date_sk); select --substring(ss.item.item_desc,1,30) as prefix having pair_count > 4 limit 20;`

  ```text

  ```
- `trilogy run query24.preql`

  ```text

  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 1 column 1798 (char 1797). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query31.preql`

  ```text

  ```
- `trilogy run query31.preql`

  ```text

  ```
- `trilogy run query32.preql`

  ```text

  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 1 column 1594 (char 1593). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 1 column 1893 (char 1892). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query35.preql`

  ```text

  ```
- `trilogy run query37.preql`

  ```text

  ```
- `trilogy run query38.preql`

  ```text

  ```
- `trilogy run query38.preql`

  ```text

  ```
- `trilogy run query38.preql`

  ```text

  ```
- `trilogy run query38.preql`

  ```text

  ```
- `trilogy run query38.preql`

  ```text

  ```
- `trilogy run query38.preql`

  ```text

  ```
- `trilogy run query38.preql`

  ```text

  ```
- `trilogy run query39.preql`

  ```text

  ```
- `trilogy run query39.preql`

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
- `trilogy run query47.preql`

  ```text

  ```
- `trilogy run . duckdb --config trilogy.toml`

  ```text

  ```
- `trilogy run query49_test.preql`

  ```text

  ```
- `trilogy run query50.preql`

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
- `trilogy run query54.preql`

  ```text

  ```
- `trilogy run query54.preql`

  ```text

  ```
- `trilogy run query54.preql`

  ```text

  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 42 column 13 (char 1910). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query57.preql`

  ```text

  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 53 column 12 (char 3099). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run test59b.preql`

  ```text

  ```
- `trilogy run test59j.preql`

  ```text

  ```
- `trilogy run query61.preql`

  ```text

  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run query63.preql`

  ```text

  ```
- `trilogy run query63.preql`

  ```text

  ```
- `trilogy run query63.preql`

  ```text

  ```
- `trilogy run query63.preql`

  ```text

  ```
- `trilogy run query65.preql`

  ```text

  ```
- `trilogy run query66.preql`

  ```text

  ```
- `trilogy run query69.preql`

  ```text

  ```
- `trilogy agent-info syntax example rollup`

  ```text

  ```
- `trilogy run query70.preql`

  ```text

  ```
- `trilogy run query71.preql`

  ```text

  ```
- `trilogy run query72.preql`

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
- `trilogy run query76.preql`

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
- `trilogy run query76.preql`

  ```text

  ```
- `trilogy run query77.preql`

  ```text

  ```
- `trilogy run query77.preql`

  ```text

  ```
- `trilogy run query77.preql`

  ```text

  ```
- `trilogy run query77.preql`

  ```text

  ```
- `trilogy run query77.preql`

  ```text

  ```
- `trilogy run query77.preql`

  ```text

  ```
- `trilogy run query77.preql`

  ```text

  ```
- `trilogy run query77.preql`

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
- `trilogy run test_merge.preql`

  ```text

  ```
- `trilogy run query87.preql`

  ```text

  ```
- `trilogy run query87.preql`

  ```text

  ```
- `trilogy run query87.preql`

  ```text

  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 22 column 10 (char 2130). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query88.preql`

  ```text

  ```
- `trilogy run query97.preql`

  ```text

  ```
- `trilogy run query97_check5.preql`

  ```text

  ```
- `trilogy run --import raw.catalog_sales:catalog_sales select sum(1) as row_count where catalog_sales.ship_date.year = 2000 and catalog_sales.warehouse.warehouse_sk is not null and catalog_sales.ship_mode.ship_mode_sk is not null and catalog_sales.call_center.call_center_sk is not null limit 1;`

  ```text

  ```
- `trilogy database describe catalog_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```

### `undefined-concept`

- `trilogy run query07.preql`

  ```text

  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text

  ```
- `trilogy run query10.preql`

  ```text

  ```
- `trilogy run query13.preql`

  ```text

  ```
- `trilogy run query13.preql`

  ```text

  ```
- `trilogy run query13.preql`

  ```text

  ```
- `trilogy run query15.preql`

  ```text

  ```
- `trilogy run query21.preql`

  ```text

  ```
- `trilogy run query25.preql`

  ```text

  ```
- `trilogy run query27.preql`

  ```text

  ```
- `trilogy run query29.preql`

  ```text

  ```
- `trilogy run query29.preql`

  ```text

  ```
- `trilogy run query31.preql`

  ```text

  ```
- `trilogy run query31.preql`

  ```text

  ```
- `trilogy explore query31.preql`

  ```text

  ```
- `trilogy run query33.preql`

  ```text

  ```
- `trilogy run query33.preql`

  ```text

  ```
- `trilogy run query33.preql`

  ```text

  ```
- `trilogy run query35.preql`

  ```text

  ```
- `trilogy run query36.preql`

  ```text

  ```
- `trilogy run query38.preql`

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
- `trilogy run query42.preql`

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
- `trilogy run query51.preql`

  ```text

  ```
- `trilogy run query51.preql`

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
- `trilogy explore query54.preql`

  ```text

  ```
- `trilogy run test59i.preql`

  ```text

  ```
- `trilogy run --import raw.store_sales:ss select ss.item.item_id, sum(ss.ext_sales_price) as total where month(ss.sold_date.date) = 9 and year(ss.sold_date.date) = 1998 and ss.customer.gmt_offset = -5 limit 10;`

  ```text

  ```
- `trilogy run query64.preql`

  ```text

  ```
- `trilogy explore query64.preql`

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
- `trilogy run query76.preql`

  ```text

  ```
- `trilogy run query76.preql`

  ```text

  ```
- `trilogy run query76.preql`

  ```text

  ```
- `trilogy run query77.preql`

  ```text

  ```
- `trilogy run query77.preql`

  ```text

  ```
- `trilogy run query77.preql`

  ```text

  ```
- `trilogy run query77.preql`

  ```text

  ```
- `trilogy run query77.preql`

  ```text

  ```
- `trilogy run query77.preql`

  ```text

  ```
- `trilogy run query77.preql`

  ```text

  ```
- `trilogy run query77.preql`

  ```text

  ```
- `trilogy run query78.preql`

  ```text

  ```
- `trilogy run query78.preql`

  ```text

  ```
- `trilogy run query80.preql`

  ```text

  ```
- `trilogy run test_merge4.preql`

  ```text

  ```
- `trilogy run test_rollup5.preql`

  ```text

  ```
- `trilogy run query81.preql`

  ```text

  ```
- `trilogy run query81.preql`

  ```text

  ```
- `trilogy run query83.preql`

  ```text

  ```
- `trilogy run query89.preql`

  ```text

  ```

### `join-resolution`

- `trilogy run query14.preql`

  ```text

  ```
- `trilogy run query22.preql`

  ```text

  ```
- `trilogy run query37.preql`

  ```text

  ```
- `trilogy run query38.preql`

  ```text

  ```
- `trilogy run query38.preql`

  ```text

  ```
- `trilogy run query39.preql`

  ```text

  ```
- `trilogy run query40.preql`

  ```text

  ```
- `trilogy run query40.preql`

  ```text

  ```
- `trilogy run query40.preql`

  ```text

  ```
- `trilogy run query49.preql`

  ```text

  ```
- `trilogy run query49.preql`

  ```text

  ```
- `trilogy run query56.preql`

  ```text

  ```
- `trilogy run --import raw.store_sales:ss --import raw.date_dim:d select count(ss.ext_sales_price) as cnt where d.date=ss.date_dim.date and d.week_seq=5218;`

  ```text

  ```
- `trilogy run --import raw.catalog_sales:cs --import raw.date_dim:d select count(cs.ext_sales_price) as cnt where d.date=cs.sold_date.date and d.week_seq=5218;`

  ```text

  ```
- `trilogy run --import raw.web_sales:ws --import raw.date_dim:d select count(ws.ext_sales_price) as cnt where d.date=ws.sold_date.date and d.week_seq=5218;`

  ```text

  ```
- `trilogy run --import raw.item:item --import raw.store_sales:ss --import raw.catalog_sales:cs --import raw.web_sales:ws select item.item_id, sum(ss.ext_sales_price) as ss_total, sum(cs.ext_sales_price) as cs_total, sum(ws.ext_sales_price) as ws_total limit 10;`

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
- `trilogy run --import raw.catalog_returns:cr --import raw.item:item select item.item_id, cr.return_quantity, cr.date_dim.date where item.item_id = 'AAAAAAAAGJMBAAAA' limit 20;`

  ```text

  ```
- `trilogy run query97.preql`

  ```text

  ```
- `trilogy run query97_check.preql`

  ```text

  ```
- `trilogy run query97.preql`

  ```text

  ```

### `syntax-missing-alias`

- `trilogy run --import raw.date_dim:dd select dd.week_seq, dd.year, count(dd.date_sk) where dd.year = 2001 group by dd.week_seq order by dd.week_seq limit 5;`

  ```text

  ```
- `trilogy run --import raw/store_sales:ss --import raw/store_returns:sr auto store_sales_agg <- sum(ss.ext_sales_price) by ss.store.store_id ? (ss.date_dim.dat… sr.store.store_sk into ~ss.store.store_sk; select ss.store.store_id, store_sales_agg, store_returns_agg, store_profit_agg - store_loss_agg limit 10;`

  ```text

  ```
- `trilogy run --import raw/store_sales:ss --import raw/catalog_sales:cs auto pair_count <- count(ss.ticket_number ? year(ss.date_dim.date) between 2000 and 200…er_sk as sk where year(cs.sold_date.date) = 2000 and month(cs.sold_date.date) = 2 and substring(cs.item.item_desc,1,30) in frequent_prefix limit 200;`

  ```text

  ```
- `trilogy run --import raw/store_sales:ss select count(ss.ticket_number) by (substring(ss.item.item_desc,1,30), ss.date_dim.date_sk) having count > 4 order by count desc limit 20;`

  ```text

  ```
- `trilogy file write query24.preql --content import raw.store_sales as sales;
import raw.store_returns as returns;

# Match store sales to store returns by tic…ore_name > 0.05 * stage1_avg

order by
    sales.customer.last_name asc,
    sales.customer.first_name asc,
    sales.store.store_name asc
limit 100;`

  ```text

  ```
- `trilogy file write query54.preql --content import raw.catalog_sales as cs;
import raw.web_sales as ws;
import raw.store_sales as ss;

merge cs.bill_customer.…d(customer_totals.total / 50)) as segment_times_50
order by segment asc nulls first, customer_count asc nulls first, segment_times_50 asc
limit 100;
`

  ```text

  ```
- `trilogy run --import raw.store_sales:ss --import raw.date_dim:d select count(ss.ext_sales_price) where d.week_seq=5218 and d.date_sk=ss.date_dim.date_sk;`

  ```text

  ```
- `trilogy run --import raw.catalog_sales:cs --import raw.date_dim:d select count(cs.ext_sales_price) where d.week_seq=5218 and d.date_sk=cs.sold_date.date_sk;`

  ```text

  ```
- `trilogy run --import raw.web_sales:ws --import raw.date_dim:d select count(ws.ext_sales_price) where d.week_seq=5218 and d.date_sk=ws.sold_date.date_sk;`

  ```text

  ```
- `trilogy run --import raw/date_dim:date_dim select distinct date_dim.dow, date_dim.day_name where date_dim.year = 2001 order by date_dim.dow limit 10;`

  ```text

  ```
- `trilogy run --import raw.item:item select distinct item.category, substr(item.item_id, 1, 5) as grp, count(item.item_sk) limit 20;`

  ```text

  ```
- `trilogy run --import raw.item:item select item.category, item.class_id, count(item.item_sk) where item.class_id in (select distinct item.class_id where item.category = 'Music') and item.category != 'Music' group by item.category, item.class_id order by item.category limit 30;`

  ```text

  ```
- `trilogy run --import raw.item:item select distinct substring(item.item_id, 1, 5) as grp, count(item.item_sk) as cnt limit 20;`

  ```text

  ```
- `trilogy run --import raw.item:item select distinct substring(item.item_id, 1, 10) as grp, count(item.item_sk) as cnt where item.category = 'Music' limit 30;`

  ```text

  ```
- `trilogy file write query64.preql --content # TPC-DS Query 64
# Identify items sold in store sales where cumulative catalog ext_list_price > 2x catalog refund…al_list_price,
      sum(ss.coupon_amt) as total_coupon_amt;

# Explore the rowset
select agg_data.sale_year, count(agg_data.product_name) limit 10;
`

  ```text

  ```
- `trilogy file write query64.preql --content # TPC-DS Query 64
# Identify items sold in store sales where cumulative catalog ext_list_price > 2x catalog refund…cp_2000,
  agg_2000.sale_line_count as cnt_2000
order by
  agg_1999.product_name,
  agg_1999.store_name,
  cnt_2000,
  ws_1999,
  ws_2000
limit 100;
`

  ```text

  ```
- `trilogy run --import raw.ship_mode:ship_mode select distinct ship_mode.carrier limit 100;`

  ```text

  ```
- `trilogy run --import raw.time_dim:time_dim select distinct time_dim.second where time_dim.second between 30838 and 59638 limit 10;`

  ```text

  ```
- `trilogy run --import raw.time_dim:td select min(td.time), max(td.time) limit 10;`

  ```text

  ```
- `trilogy file write query70.preql --content import raw.store_sales as store_sales;

# For store sales in year 2000, report total net profit rolled up
# by sto….store.state in top5_states or hierarchy_level = 2
order by
    hierarchy_level desc,
    store_sales.store.state nulls first,
    rnk asc
limit 100;`

  ```text

  ```
- `trilogy file write query80.preql -e -c import raw.store_sales as store_sales;\nimport raw.store_returns as store_returns;\nimport raw.catalog_sales as catalo…e(b_returns, 0) + coalesce(c_returns, 0) -> returns,\n    coalesce(a_profit, 0) + coalesce(b_profit, 0) + coalesce(c_profit, 0) -> profit\nlimit 100;`

  ```text

  ```
- `trilogy file write query82.preql`

  ```text

  ```
- `trilogy file write query97_verify3.preql --content import raw.store_sales as ss;

select sum(1) by ss.customer.customer_sk, ss.item.item_sk
where ss.date_dim.year = 2000 and ss.customer.customer_sk is not null
limit 100;`

  ```text

  ```

### `type-error`

- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text

  ```
- `trilogy run query38.preql`

  ```text

  ```
- `trilogy run query95.preql`

  ```text

  ```
- `trilogy run query97_verify2.preql`

  ```text

  ```

### `cli-misuse`

- `trilogy file write -c import raw.store_sales as store; --content import raw.web_sales as web;

select 
  store.customer_address.county as county, 
  store.date_dim.qoy as qtr, 
  sum(store.ext_sales_price) as store_amt
where store.date_dim.year = 2000
limit 10;`

  ```text
  Missing argument 'PATH'.
  ```
- `trilogy explore raw/query54.preql`

  ```text
  Invalid value for 'PATH': File 'raw/query54.preql' does not exist.
  ```

### `file-not-found`

- `trilogy run query35.preql`

  ```text

  ```
- `trilogy run query37.preql`

  ```text

  ```
