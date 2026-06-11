# Trilogy failure analysis — 20260611-124629

- Run `20260611-124628_ingest` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 437 | failed: 60 (14%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 30 | 50% |
| `syntax-parse` | 14 | 23% |
| `join-resolution` | 6 | 10% |
| `syntax-missing-alias` | 6 | 10% |
| `undefined-concept` | 3 | 5% |
| `cli-misuse` | 1 | 2% |

## Detail

### `other`

- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.store_sales:ss --import raw.store_returns:sr import raw.catalog_sales as cs; merge ss.ticket_number into sr.ticket_number; merge ss.….year = 1999 and cs.sold_date.year in (1999, 2000, 2001) select ss.ticket_number, ss.customer.customer_id, sr.ticket_number, cs.order_number limit 5;`

  ```text

  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 26 column 12 (char 1322). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run --import raw.store_sales:store_sales select count(store_sales.item.item_sk) as qualifying_item_count where store_sales.store.store_sk = 1 and avg…m_sk > 0.9 * avg(store_sales.net_profit ? store_sales.store.store_sk = 1 and store_sales.customer.customer_address.address_sk is null) by * limit 10;`

  ```text

  ```
- `trilogy run --import raw.store_sales:store_sales select count(store_sales.item.item_sk) as cnt where store_sales.store.store_sk = 1 and avg(store_sales.net_p…m_sk > 0.9 * avg(store_sales.net_profit ? store_sales.store.store_sk = 1 and store_sales.customer.customer_address.address_sk is null) by * limit 10;`

  ```text

  ```
- `trilogy file read query44.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query54.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query64.preql duckdb`

  ```text

  ```
- `trilogy run query64.preql duckdb`

  ```text

  ```
- `trilogy run query64.preql duckdb`

  ```text

  ```
- `trilogy database describe store_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run query64.preql duckdb`

  ```text

  ```
- `trilogy run query64.preql duckdb`

  ```text

  ```
- `trilogy run query64.preql duckdb`

  ```text

  ```
- `trilogy run query64.preql duckdb`

  ```text

  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query72.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query75.preql`

  ```text

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
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 50 column 12 (char 1984). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query76.preql`

  ```text

  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query77.preql`

  ```text

  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy run --import raw.store_sales:ss --import raw.store_returns:sr --import raw.catalog_sales:cs inner join sr.ticket_number = ss.ticket_number inner join… in (1999,2000,2001) select ss.ticket_number as tkt, ss.customer.customer_id as cust, sr.ticket_number as sr_tkt, cs.order_number as cs_ord limit 10;`

  ```text

  ```
- `trilogy run --import raw.store_sales:store_sales select store.store_sk, count(store_sales.ticket_number) as cnt by store.store_sk limit 20;`

  ```text

  ```
- `trilogy file write query64.preql -e -c import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.store_retur…rice_sum,
    sum(ss.coupon_amt) as coupon_amt_sum
    having sale_year in (1999, 2000)
order by ss.item.product_name, ss.store.store_name
limit 100;`

  ```text

  ```
- `trilogy run --import raw.catalog_sales:cs select date_diff(cs.ship_date.date, cs.sold_date.date, day) as ship_days, count(cs.order_number) as cnt where cs.sold_date.year=1999 group by (date_diff(cs.ship_date.date, cs.sold_date.date, day)) order by ship_days asc limit 50;`

  ```text

  ```
- `trilogy run --import raw.catalog_sales:cs select cs.promotion.promo_sk, count(cs.order_number) as cnt where cs.sold_date.year=1999 and cs.bill_customer.house…omer.customer_demographics.marital_status='D' and date_diff(cs.sold_date.date, cs.ship_date.date, day) > 5 group by (cs.promotion.promo_sk) limit 20;`

  ```text

  ```
- `trilogy file write query75.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r…(ss.quantity, 0) - coalesce(sr.return_quantity, 0) as net_qty,
    coalesce(ss.ext_sales_price, 0) - coalesce(sr.return_amt, 0) as net_amt
limit 100;`

  ```text

  ```
- `trilogy file write query75.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r…uantity, 0) - coalesce(sr.return_quantity, 0) as net_qty,
        coalesce(ss.ext_sales_price, 0) - coalesce(sr.return_amt, 0) as net_amt;

limit 10;`

  ```text

  ```
- `trilogy file write query76.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_sale…annel asc nulls first, missing_ref asc nulls first, sale_year asc nulls first, sale_quarter asc nulls first, item_category asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query76.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_sale…nnel asc nulls first, missing_ref asc nulls first, sale_year asc nulls first, sale_quarter asc nulls first, item_category asc nulls first
limit 100;
`

  ```text

  ```
- `trilogy file write query76.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_sale…nnel asc nulls first, missing_ref asc nulls first, sale_year asc nulls first, sale_quarter asc nulls first, item_category asc nulls first
limit 100;
`

  ```text

  ```
- `trilogy file write query76.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_sale…nnel asc nulls first, missing_ref asc nulls first, sale_year asc nulls first, sale_quarter asc nulls first, item_category asc nulls first
limit 100;
`

  ```text

  ```
- `trilogy file write query76.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_sale…nnel asc nulls first, missing_ref asc nulls first, sale_year asc nulls first, sale_quarter asc nulls first, item_category asc nulls first
limit 100;
`

  ```text

  ```
- `trilogy file write query77.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r…rofit, 0) + coalesce(profit_b, 0) + coalesce(profit_c, 0) as profit
order by channel asc nulls first, outlet asc nulls first, returns desc
limit 100;`

  ```text

  ```
- `trilogy file write query77.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r…0) - coalesce(ret_loss_b, 0)) + (coalesce(profit_c, 0) - coalesce(ret_loss_c, 0)) as profit
order by channel asc, outlet asc, returns desc
limit 100;`

  ```text

  ```

### `join-resolution`

- `trilogy run --import raw.store_sales:ss --import raw.store_returns:sr select ss.ticket_number, sr.ticket_number, ss.customer.customer_id, sr.customer.customer_id limit 5;`

  ```text

  ```
- `trilogy run query54.preql`

  ```text

  ```
- `trilogy run --import raw.catalog_sales:cs --import raw.inventory:inv select cs.item.item_sk as cs_item, cs.sold_date.week_seq as cs_week, cs.quantity as cs_q…s.marital_status='D' and date_diff(cs.sold_date.date, cs.ship_date.date, day) > 5 and cs.item.item_sk = 67 and cs.sold_date.week_seq = 5212 limit 10;`

  ```text

  ```
- `trilogy run --import raw.catalog_sales:cs --import raw.inventory:inv select cs.promotion.promo_sk is null as is_null_promo, count(cs.order_number) as cnt whe…v.quantity_on_hand < cs.quantity and cs.item.item_sk = inv.item.item_sk and cs.sold_date.week_seq = inv.date_dim.week_seq order by cnt desc limit 10;`

  ```text

  ```
- `trilogy run query75.preql`

  ```text

  ```
- `trilogy run --import raw.store_sales:ss --import raw.store_returns:sr select ss.item.brand_id, ss.item.class_id, ss.item.category_id, ss.item.manufact_id, ye…) as yr, sum(coalesce(ss.quantity,0)-coalesce(sr.return_quantity,0)) as qty where ss.item.category='Books' and year(ss.date_dim.date)=2001 limit 100;`

  ```text

  ```

### `syntax-missing-alias`

- `trilogy run --import raw.store_sales:ss --import raw.store_returns:sr import raw.catalog_sales as cs; merge ss.ticket_number into sr.ticket_number; merge ss.… = 1999 and cs.sold_date.year in (1999, 2000, 2001) and ss.ticket_number = sr.ticket_number select ss.item.item_id, count(ss.ticket_number) limit 10;`

  ```text

  ```
- `trilogy run --import raw.store_sales:store_sales select store.store_sk, count(store_sales.ticket_number) by store.store_sk limit 20;`

  ```text

  ```
- `trilogy run --import raw/catalog_sales:cs select count_distinct(cs.bill_customer.customer_sk) where cs.item.category = 'Women' and cs.item.class = 'maternity' and cs.sold_date.month_seq = 1187;`

  ```text

  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.date_dim.year, store_sales.date_dim.qoy, count(store_sales.ticket_number) limit 5;`

  ```text

  ```
- `trilogy file write query77.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r…rofit, 0) + coalesce(profit_b, 0) + coalesce(profit_c, 0) as profit
order by channel asc nulls first, outlet asc nulls first, returns desc
limit 100;`

  ```text

  ```
- `trilogy file write query77.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r…coalesce(ws_profit_c, 0) - coalesce(wr_net_loss_val_c, 0) as profit
order by channel asc nulls first, outlet asc nulls first, returns desc
limit 100;`

  ```text

  ```

### `undefined-concept`

- `trilogy run --import raw.store_sales:store_sales select store.store_sk, count(store_sales.ticket_number) as cnt order by store.store_sk limit 20;`

  ```text

  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy run query75.preql`

  ```text

  ```

### `cli-misuse`

- `trilogy run --import raw/catalog_sales:cs --import raw/web_sales:ws --import raw/customer:c merge cs.bill_customer.customer_sk into ~c.customer_sk; merge ws.…d ws.item.class = 'maternity' and ws.sold_date.month_seq = 1187; select count(c.customer_sk) as cnt where c.customer_sk in cc or c.customer_sk in wc;`

  ```text
  'merge ws.bill_customer.customer_sk into ~c.customer_sk;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
