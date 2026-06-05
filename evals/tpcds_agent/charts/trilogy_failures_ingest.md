# Trilogy failure analysis — 20260603-144034

- Run `20260603-144030_ingest` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 316 | failed: 55 (17%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 30 | 55% |
| `syntax-parse` | 15 | 27% |
| `undefined-concept` | 6 | 11% |
| `join-resolution` | 4 | 7% |

## Detail

### `other`

- `trilogy run query01.preql`

  ```text

  ```
- `trilogy run --import raw.catalog_sales:cs --import raw.web_sales:ws merge cs.sold_date.date_sk into ~ws.sold_date.date_sk; auto sun_this <- sum(cs.ext_sales_…s_price ? ws.sold_date.dow = 0) by ws.sold_date.week_seq; select sun_this where ws.sold_date.week_seq in (5270, 5323) order by ws.sold_date.week_seq;`

  ```text

  ```
- `trilogy run --import raw.catalog_sales:cs --import raw.web_sales:ws merge cs.sold_date.date_sk into ~ws.sold_date.date_sk; auto sales_by_ws_dow <- sum(cs.ext…year = 2002; select ws.sold_date.week_seq, ws.sold_date.dow, sales_by_ws_dow, next_yr.val where ws.sold_date.week_seq in next_yr.this_yr_ws limit 10;`

  ```text

  ```
- `trilogy run --import raw.catalog_sales:cs --import raw.web_sales:ws merge cs.sold_date.date_sk into ~ws.sold_date.date_sk; auto sun_c01 <- sum(cs.ext_sales_p….sold_date.year = 2001; rowset ws02 <- select ws.sold_date.week_seq as w, sun_c02 as s where ws.sold_date.year = 2002; select ws01.w, ws01.s, ws02.s;`

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
  The graph is not a DAG (contains cycles) and cannot be topologically sorted.
  ```
- `trilogy run query04.preql`

  ```text
  The graph is not a DAG (contains cycles) and cannot be topologically sorted.
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
- `trilogy run query10.preql`

  ```text

  ```

### `syntax-parse`

- `trilogy run --import raw.date_dim:date_dim select date_dim.min(week_seq) as min_ws, date_dim.max(week_seq) as max_ws, date_dim.count_distinct(week_seq) as dist_ws where date_dim.year = 2001;`

  ```text

  ```
- `trilogy run --import raw.catalog_sales:cs --import raw.web_sales:ws merge cs.sold_date.date_sk into ~ws.sold_date.date_sk; select count_distinct(ws.sold_date.dow) as days, ws.sold_date.week_seq where ws.sold_date.year = 2001 group by ws.sold_date.week_seq order by ws.sold_date.week_seq limit 10;`

  ```text

  ```
- `trilogy run --import raw.catalog_sales:cs --import raw.web_sales:ws merge cs.sold_date.date_sk into ~ws.sold_date.date_sk; auto ws_list <- select ws.sold_date.week_seq as w where ws.sold_date.year = 2001; select ws_list.w order by ws_list.w limit 10;`

  ```text

  ```
- `trilogy run --import raw.catalog_sales:cs --import raw.web_sales:ws merge cs.sold_date.date_sk into ~ws.sold_date.date_sk; auto total_dow <- sum(cs.ext_sales…eq, ws.sold_date.dow); select ws.sold_date.week_seq, ws.sold_date.dow, total_dow, next_dow order by ws.sold_date.week_seq, ws.sold_date.dow limit 50;`

  ```text

  ```
- `trilogy run --import raw.catalog_sales:cs --import raw.web_sales:ws merge cs.sold_date.date_sk into ~ws.sold_date.date_sk; auto sun_c01 <- sum(cs.ext_sales_p…date where ws.sold_date.year = 2001; rowset ws02 <- select ws.sold_date.week_seq as w, sun_c02 as s from ws.sold_date where ws.sold_date.year = 2002;`

  ```text

  ```
- `trilogy run --import raw.catalog_sales:cs --import raw.web_sales:ws merge cs.sold_date.date_sk into ~ws.sold_date.date_sk; auto sun_c01 <- sum(cs.ext_sales_p…old_date.week_seq as w, sun_c02 as s where ws.sold_date.year = 2002; merge ws01.w into ~(ws02.w - 53); select ws01.w, ws01.s, ws02.s order by ws01.w;`

  ```text

  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.date_dim.year, count(store_sales.ticket_number) as cnt group by store_sales.date_dim.year order by year;`

  ```text

  ```
- `trilogy run --import raw.catalog_sales:catalog_sales select catalog_sales.sold_date.year, count(catalog_sales.order_number) as cnt group by catalog_sales.sold_date.year order by year;`

  ```text

  ```
- `trilogy run --import raw.web_sales:web_sales select web_sales.sold_date.year, count(web_sales.order_number) as cnt group by web_sales.sold_date.year order by year;`

  ```text

  ```
- `trilogy file write query04.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…rst_name asc nulls first,
    store_sales.customer.last_name asc nulls first,
    store_sales.customer.preferred_cust_flag asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query05.preql --content import raw.store_sales as s;
import raw.store_returns as r;

# Store channel: combine sales and returns using mult…  coalesce(r_ret_amt, 0) -> returns,
    coalesce(s_profit, 0) - coalesce(r_ret_loss, 0) -> net_profit
    --sk
order by channel, entity_id
limit 20;`

  ```text

  ```
- `trilogy file write query05.preql --content import raw.store_sales as s;
import raw.store_returns as r;
import raw.catalog_sales as cs;
import raw.catalog_ret…oalesce(r_loss,0) as net_profit
where s.store.store_sk in (select s.store.store_sk where s_gross>0 or r_amt>0)
order by channel, entity_id
limit 100;`

  ```text

  ```
- `trilogy file write query05.preql --content import raw.store_sales as s;
import raw.store_returns as r;

# Merge store concepts so returns can be referenced v…ross, 0) - coalesce(total_loss, 0) as net_profit
where s.store.store_sk in active.sk or grp_level = 1
order by grp_level asc, entity_id asc
limit 20;`

  ```text

  ```
- `trilogy file write query05.preql --content import raw.store_sales as s;
import raw.store_returns as r;
import raw.catalog_sales as cs;
import raw.catalog_ret…m(cr.net_loss
        ? cr.date_dim.date between '2000-08-23'::date and '2000-09-06'::date), 0) as net_profit

order by channel, entity_id
limit 100;`

  ```text

  ```
- `trilogy file write query05.preql --content import raw.store_sales as s;
import raw.store_returns as r;
import raw.catalog_sales as cs;
import raw.catalog_ret…  coalesce(s_gross, 0) -> gross_sales,
    coalesce(s_ret, 0) -> returns,
    coalesce(s_net, 0) -> net_profit
order by channel, entity_id
limit 100;`

  ```text

  ```

### `undefined-concept`

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
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text

  ```

### `join-resolution`

- `trilogy run --import raw.catalog_sales:cs --import raw.web_sales:ws select cs.sold_date.week_seq, cs.sold_date.dow, sum(cs.ext_sales_price) + sum(ws.ext_sales_price) as total_sales where cs.sold_date.year in (2001,2002) limit 20;`

  ```text

  ```
- `trilogy run query05.preql`

  ```text

  ```
- `trilogy run query10.preql`

  ```text

  ```
- `trilogy run query10.preql`

  ```text

  ```
