# Trilogy failure analysis — 20260620-034651

- Run `20260620-034651` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 365 | failed: 84 (23%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 54 | 64% |
| `syntax-parse` | 13 | 15% |
| `join-resolution` | 11 | 13% |
| `syntax-missing-alias` | 5 | 6% |
| `cli-misuse` | 1 | 1% |

## Detail

### `other`

- `trilogy run query02.preql duckdb`

  ```text

  ```
- `trilogy run query02.preql duckdb`

  ```text

  ```
- `trilogy run query02.preql duckdb`

  ```text

  ```
- `trilogy run query02.preql duckdb`

  ```text

  ```
- `trilogy run query02.preql duckdb`

  ```text

  ```
- `trilogy run query02.preql duckdb`

  ```text

  ```
- `trilogy run query02.preql duckdb`

  ```text

  ```
- `trilogy run query02.preql duckdb`

  ```text

  ```
- `trilogy run query02.preql duckdb`

  ```text

  ```
- `trilogy run query02.preql duckdb`

  ```text

  ```
- `trilogy run query02.preql duckdb`

  ```text

  ```
- `trilogy run query02.preql duckdb`

  ```text

  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 1 column 3055 (char 3054). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales; 
 
# Sales union arm: sales filtered by sale date and non-null channel_dim_id 
…::float as gross_sales, 
    coalesce(sum(all_sales.return_amount), 0) as returns, 
    coalesce(-sum(all_sales.return_net_loss), 0) as net_profit 
;`

  ```text
  trilogy file write: `--content` takes a SINGLE string argument. Your args list put 41 separate tokens after --content (treating it like a shell command). In a tool call, pass the entire file body as one string element after --content, with newlines embedded literally — e.g.
    {"args": ["file", "write", "query70.preql", "--content", "import raw.store_sales as store_sales;\n\nselect ..."]}
  Alternatively use `--escapes` with a single-line `\n`-escaped string.
  ```
- `trilogy run query05.preql`

  ```text

  ```
- `trilogy run query05.preql`

  ```text

  ```
- `trilogy run query14.preql`

  ```text

  ```
- `trilogy run query14.preql`

  ```text

  ```
- `trilogy run --import raw/all_sales:as select count_distinct(asales.channel) as chans where asales.date.year = 2001 and asales.date.month_of_year = 11 limit 5;`

  ```text

  ```
- `trilogy file read query14.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
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
- `trilogy run --import raw.physical_sales:ps --import raw.item:item where ps.store.id = 1 select item.product_name as name, avg(ps.net_profit) as avg_profit order by avg_profit asc limit 5;`

  ```text

  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy run --import raw.physical_sales:ps where ps.store.id = 1 select avg(ps.net_profit) as thr where ps.sale_address.id is null;`

  ```text

  ```
- `trilogy run --import raw.physical_sales:ps select count(ps.item.product_name) as cnt where ps.store.id = 1 having avg(ps.net_profit) > -738.04;`

  ```text

  ```
- `trilogy run --import raw.physical_sales:ps select ps.item.product_name as name, --avg(ps.net_profit) as avg_profit having avg(ps.net_profit) > -738.04 and ps.store.id = 1;`

  ```text

  ```
- `trilogy run --import raw.physical_sales:ps select ps.item.product_name as name, avg(ps.net_profit) as avg_profit where ps.store.id = 1 having avg(ps.net_profit) > 0.9 * avg(ps.net_profit ? ps.sale_address.id is null and ps.store.id = 1) by * order by avg_profit asc limit 10;`

  ```text

  ```
- `trilogy file read query44.preql`

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
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 21 column 12 (char 1142). Re-issue the call with valid JSON arguments.
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

### `syntax-parse`

- `trilogy file write query02.preql --content import raw.web_sales as web;
import raw.catalog_sales as catalog;

# First check: what week_seq values exist in 20…select
    web.date.week_seq as ws,
    web.date.year as yr
where web.date.year = 2001
group by web.date.week_seq, web.date.year
order by ws
limit 5;`

  ```text

  ```
- `trilogy file write query02.preql --content import raw.web_sales as web;
import raw.catalog_sales as catalog;

# Combined sales per (week_seq, day_of_week) fr…um(s2001.total ? s2001.dow = 6) / sum(s2002.total ? s2002.dow = 6), 2) as Saturday
inner join s2001.ws = s2002.ws - 53
order by week_seq nulls first;`

  ```text

  ```
- `trilogy file write query02.preql --content import raw.web_sales as web;
import raw.catalog_sales as catalog;

# Combined web + catalog sales by (week_seq, da…? agg_2001.dow = 6) / sum(agg_2002.total ? agg_2002.dow = 6), 2) as Saturday
inner join agg_2001.ws = agg_2002.ws - 53
order by week_seq nulls first;`

  ```text

  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

# Sales union arm: sales filtered by sale date and non-null channel_dim_id
wit…  0::float as gross_sales,
    coalesce(sum(all_sales.return_amount), 0) as returns,
    coalesce(-sum(all_sales.return_net_loss), 0) as net_profit
;`

  ```text

  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

# Sales union arm: sales filtered by sale date and non-null channel_dim_id
wit…   @rollup_sum(combined.net_profit) as net_profit
order by
    combined.channel_name asc nulls last,
    combined.entity_id asc nulls last
limit 100;`

  ```text

  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

# Sales union arm: sales filtered by sale date and non-null channel_dim_id
wit…   @rollup_sum(combined.net_profit) as net_profit
order by
    combined.channel_name asc nulls last,
    combined.entity_id asc nulls last
limit 100;`

  ```text

  ```
- `trilogy run --import raw/all_sales:as select as.channel, sum(as.quantity * as.list_price) as total_sales, count(as.order_id) as num_sales where as.date.year = 2001 and as.date.month_of_year = 11 group by as.channel;`

  ```text

  ```
- `trilogy file write query44.preql --content import raw.physical_sales as ps;
import raw.item as item;

# Global threshold: avg net profit at store 1 where sal…_rank,
    rank(qualified_items.product_name) over (order by qualified_items.item_avg_profit desc) as worst_rank
order by
    best_rank asc
limit 10;`

  ```text

  ```
- `trilogy file write query64.preql -e -c import raw.physical_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.physical…,'floral','medium')
  and ss.item.current_price between 65 and 74
  and item_cat_ext_list_price > 2 * item_cat_refund
select ss.item.id
;

limit 10;
`

  ```text

  ```
- `trilogy file write query64.preql -e -c import raw.physical_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.physical…e_cnt,
  sum(ss.ext_wholesale_cost) as ws_cost,
  sum(ss.ext_list_price) as list_price_sum,
  sum(ss.coupon_amt) as coupon_sum
;

select *
limit 20;
`

  ```text

  ```
- `trilogy file write query64.preql -e -c import raw.catalog_returns as cr;
import raw.physical_sales as ss;
import raw.physical_returns as pr;

# Per-item aggr… list_price_sum,
  sum(ss.coupon_amt) as coupon_sum
;

rowset y1999 <- where sale_agg.sale_year = 1999 select *;

select y1999.product_name limit 5;
`

  ```text

  ```
- `trilogy file write query78.preql --content import raw.physical_sales as store;
import raw.catalog_sales as catalog;
import raw.web_sales as web;

merge store…b.ext_sales_price),0)+coalesce(sum(catalog.ext_sales_price),0)) asc, round(store_qty/nullif(coalesce(web_qty,0)+coalesce(cat_qty,0),0),2)
limit 100;
`

  ```text

  ```
- `trilogy file write query97.preql --content import raw.physical_sales as store;
import raw.catalog_sales as catalog;

# Store sales: unique (customer, item) c…then 1 else 0 end) as in_both
full join store_pairs.cust_id = catalog_pairs.cust_id
full join store_pairs.item_id = catalog_pairs.item_id
limit 100;
`

  ```text

  ```

### `join-resolution`

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
- `trilogy run --import raw.physical_sales:ps --import raw.item:item where ps.store.id = 1 select item.product_name as name, avg(ps.net_profit) as avg_profit, rank(item.product_name) over (order by avg(ps.net_profit) asc) as rnk order by rnk asc limit 5;`

  ```text

  ```
- `trilogy run query64.preql`

  ```text

  ```
- `trilogy run query64.preql`

  ```text

  ```

### `syntax-missing-alias`

- `trilogy file write query02.preql --content import raw.web_sales as web;
import raw.catalog_sales as catalog;

# Find all distinct week_seq values in 2001
select
    distinct web.date.week_seq as ws
where web.date.year = 2001
order by ws;`

  ```text

  ```
- `trilogy run --import raw.all_sales:all_sales select distinct all_sales.channel, all_sales.channel_dim_text_id limit 10;`

  ```text

  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

# Sales union arm: sales filtered by sale date and non-null channel_dim_id
wit…ed.channel_name, combined.entity_id as net_profit
order by
    combined.channel_name asc nulls last,
    combined.entity_id asc nulls last
limit 100;`

  ```text

  ```
- `trilogy run --import raw.physical_sales:ps select ps.store.id, count(ps.ticket_number) limit 5;`

  ```text

  ```
- `trilogy file write query80.preql --content import raw.all_sales as s;

# Filter conditions
where s.date.date between '2000-08-23'::date and '2000-09-22'::dat…net_loss, 0)) as profit,
    by rollup channel_label, outlet_identifier

order by channel_label nulls first, outlet_identifier nulls first
limit 100;`

  ```text

  ```

### `cli-misuse`

- `trilogy explore raw/store_sales.preql --show all`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
