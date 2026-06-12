# Trilogy failure analysis — 20260612-123806

- Run `20260612-123802_ingest` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 390 | failed: 78 (20%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 47 | 60% |
| `syntax-parse` | 16 | 21% |
| `undefined-concept` | 12 | 15% |
| `syntax-missing-alias` | 1 | 1% |
| `join-resolution` | 1 | 1% |
| `type-error` | 1 | 1% |

## Detail

### `other`

- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe store_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run --import raw.store_sales:ss select ss.item.item_sk, ss.item.product_name, avg(ss.net_profit) as avg_profit where ss.store.store_sk = 1 having avg_profit > 0.9 * avg(ss.net_profit ? ss.customer_address.address_sk is null) by ss.store.store_sk order by avg_profit asc limit 10;`

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
- `trilogy file read query44.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 45 column 12 (char 1890). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query54.preql`

  ```text

  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query54.preql`

  ```text

  ```
- `trilogy file read raw/store_sales.preql`

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
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 115 column 12 (char 4997). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query75.preql`

  ```text

  ```
- `trilogy file read query75.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

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
- `trilogy run query77.preql`

  ```text

  ```
- `trilogy file read query82.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/customer.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy file write query40.preql --content import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

where cs.item.current_price between 0.99 and 1.…11
left join cr.item.item_sk = cs.item.item_sk
  and cr.order_number = cs.order_number
order by
    warehouse_state asc,
    item_code asc
limit 100;`

  ```text

  ```
- `trilogy run --import raw.store_sales:ss select ss.item.item_sk, ss.item.product_name, avg(ss.net_profit) as avg_profit where ss.store.store_sk = 1 group by s… having avg(ss.net_profit) > 0.9 * avg(ss.net_profit ? ss.customer_address.address_sk is null) by ss.store.store_sk order by avg_profit asc limit 10;`

  ```text

  ```
- `trilogy run --import raw.store_sales:ss with test_set as union((select ss.item.item_sk as sk, ss.item.product_name as pn where ss.store.store_sk = 1 limit 2), (select ss.item.item_sk as sk, ss.item.product_name as pn where ss.store.store_sk = 1 limit 2)) -> (sk, pn) select test_set.sk, test_set.pn limit 5;`

  ```text

  ```
- `trilogy run --import raw.store_sales:ss with test_set as union((select ss.item.item_sk as sk, ss.item.product_name as pn where ss.store.store_sk = 1 limit 2)…em.item_sk as sk, ss.item.product_name as pn where ss.store.store_sk = 1 limit 2)) -> (sk bigint, pn string) select test_set.sk, test_set.pn limit 5;`

  ```text

  ```
- `trilogy file write query44.preql --content import raw.store_sales as ss;

# Items at store 1 that pass the threshold filter
rowset ranked_items <- select
   ….rank as rank_num,
    best_items.product_name as best_product,
    worst_items.product_name as worst_product
order by best_items.rank asc
limit 100;`

  ```text

  ```
- `trilogy file write query54.preql --content import raw.catalog_sales as catalog;
import raw.web_sales as web;
import raw.store_sales as store;
import raw.date…ustomer_count,
    segment * 50 as segment_times_50
order by segment asc nulls first, customer_count asc nulls first, segment_times_50 asc
limit 100;`

  ```text

  ```
- `trilogy file write query54.preql --content import raw.catalog_sales as catalog;
import raw.web_sales as web;
import raw.store_sales as store;
import raw.cust…ustomer_count,
    segment * 50 as segment_times_50
order by segment asc nulls first, customer_count asc nulls first, segment_times_50 asc
limit 100;`

  ```text

  ```
- `trilogy file write query54.preql --content import raw.catalog_sales as catalog;
import raw.web_sales as web;
import raw.store_sales as store;
import raw.cust…ustomer_count,
    segment * 50 as segment_times_50
order by segment asc nulls first, customer_count asc nulls first, segment_times_50 asc
limit 100;`

  ```text

  ```
- `trilogy run --import raw/store_sales:store select store.customer.customer_sk, sum(store.ext_sales_price) as total where store.date_dim.month_seq >= 1188 and store.date_dim.month_seq <= 1190 and store.customer.customer_sk = 460 group by store.customer.customer_sk;`

  ```text

  ```
- `trilogy run --import raw/store_sales:store select store.customer.customer_sk, sum(store.ext_sales_price) as total where store.date_dim.month_seq >= 1188 and …= store.customer.customer_address.county and store.store.state = store.customer.customer_address.state group by store.customer.customer_sk limit 100;`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r…unt(cs_by_item.item_sk) as cnt
inner join cs_by_item.item_sk = cr_by_item.item_sk
where cs_by_item.list_total > 2 * cr_by_item.refund_total
limit 5;
`

  ```text

  ```
- `trilogy run --import raw.catalog_sales:cs select cs.sold_date.year, cs.bill_hdemo.buy_potential, cs.bill_cdemo.marital_status, count(cs.order_number) as cnt where cs.sold_date.year = 1999 group by 1,2,3 limit 10;`

  ```text

  ```
- `trilogy file write query75.preql -e -c import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_retur…manufact_id = y2001.manufact_id
having y2001.net_qty > 0 and 1.0 * y2002.net_qty / y2001.net_qty < 0.9
order by qty_diff asc, amt_diff asc
limit 100;`

  ```text

  ```
- `trilogy file write query77.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r…ns) by rollup combined.channel, combined.outlet as profit
order by combined.channel nulls first, combined.outlet nulls first, returns desc
limit 100;`

  ```text

  ```
- `trilogy file write query77.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r…p combined.channel, combined.outlet as total_profit
order by combined.channel nulls first, combined.outlet nulls first, total_returns desc
limit 100;`

  ```text

  ```
- `trilogy file write query82.preql --content import raw.item as item;

where item.current_price between 62 and 92
  and item.manufact_id in (129, 270, 821, 423…ore_sales.item.item_sk
  )
select
  item.item_id as item_code,
  item.item_desc as description,
  item.current_price
order by item.item_id
limit 100;`

  ```text

  ```

### `undefined-concept`

- `trilogy run --import raw.store_sales:store_sales select store.store_sk, count(store_sales.ticket_number) as cnt where store.store_sk = 1 limit 5;`

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
- `trilogy run --import raw.store_sales:ss 
rowset ts <- select ss.item.item_sk as sk, ss.item.product_name as pn having 1=1 limit 5;

rowset ts2 <- select ts.sk, ts.pn having 1=1 limit 3;

select ts2.sk, ts2.pn limit 5;
`

  ```text

  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy run query68.preql`

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

### `syntax-missing-alias`

- `trilogy file write query64.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r…_sk
    inner join cs.order_number = cr.order_number
    having cat_list_total > 2 * cat_refund_total;

select count(cat_qualifying.item_sk) limit 5;`

  ```text

  ```

### `join-resolution`

- `trilogy run query64.preql`

  ```text

  ```

### `type-error`

- `trilogy run --import raw.catalog_sales:cs select cs.sold_date.year, count(cs.order_number) as cnt where cs.sold_date.year = 1999 and cs.bill_hdemo.buy_potential = '>10000' and cs.bill_cdemo.marital_status = 'D' and cs.sold_date.date - cs.ship_date.date > 5 limit 10;`

  ```text

  ```
