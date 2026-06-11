# Trilogy failure analysis — 20260611-133359

- Run `20260611-133358_enriched` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 240 | failed: 35 (15%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 13 | 37% |
| `syntax-parse` | 12 | 34% |
| `undefined-concept` | 3 | 9% |
| `cli-misuse` | 3 | 9% |
| `syntax-missing-alias` | 2 | 6% |
| `join-resolution` | 1 | 3% |
| `type-error` | 1 | 3% |

## Detail

### `other`

- `trilogy file read raw/physical_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy file read query46.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query54.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 110 column 13 (char 3910). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query72.preql`

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
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query84.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy file write query44.preql --content import raw.physical_sales as physical_sales;

# Store 1 only
where physical_sales.store.id = 1

# Define the thres…_avg_profit > 0.9 * address_null_avg_profit

select
    physical_sales.item.product_name,
    item_avg_profit
order by item_avg_profit asc
limit 100;`

  ```text

  ```
- `trilogy run --import raw/item:item select item.category, item.class, count(item.id) as cnt by item.category, item.class where item.category='Women' and item.class='maternity' limit 10;`

  ```text

  ```
- `trilogy file write query54.preql --content import raw.catalog_sales as cat;
import raw.web_sales as web;
import raw.physical_sales as store;

# Customers who…t_qualifying.cust_id is not null
  union
  select web_qualifying.cust_id as cust_id
  from web_qualifying
  where web_qualifying.cust_id is not null;`

  ```text

  ```
- `trilogy run --import raw.physical_sales:store select store.customer.id, sum(store.ext_sales_price) as total where store.date.month_seq between 1188 and 1190 …atalog_sales.item.class='maternity') and store.store.county=store.customer.address.county and store.store.state=store.customer.address.state limit 5;`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.physical_sales as ps;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.phys…000,
    y2000.line_cnt as cnt2000
order by y1999.product_name, y1999.store_name, y2000.line_cnt, y1999.wholesale_sum, y2000.wholesale_sum
limit 100;`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.physical_sales as ps;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.phys…000,
    y2000.line_cnt as cnt2000
order by y1999.product_name, y1999.store_name, y2000.line_cnt, y1999.wholesale_sum, y2000.wholesale_sum
limit 100;`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.physical_sales as ps;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.phys…000,
    y2000.line_cnt as cnt2000
order by y1999.product_name, y1999.store_name, y2000.line_cnt, y1999.wholesale_sum, y2000.wholesale_sum
limit 100;`

  ```text

  ```
- `trilogy file write query77.preql --content import raw.all_sales as all;

# Filter sales sold in the period, only include recorded outlets
where all.date.date…te between '2000-08-23'::date and '2000-09-22'::date, 0)) as profit
order by channel asc nulls first, outlet asc nulls first, returns desc
limit 100;`

  ```text

  ```
- `trilogy file write query77.preql --content import raw.all_sales as all;

def rl(metric) -> sum(metric) by rollup
    (case
        when all.sales_channel = '…te between '2000-08-23'::date and '2000-09-22'::date, 0)) as profit
order by channel asc nulls first, outlet asc nulls first, returns desc
limit 100;`

  ```text

  ```
- `trilogy file write query77.preql --content import raw.all_sales as all;

def rl(metric) -> sum(metric) by rollup
    (case
        when all.sales_channel = '…te between '2000-08-23'::date and '2000-09-22'::date, 0)) as profit
order by channel asc nulls first, outlet asc nulls first, returns desc
limit 100;`

  ```text

  ```
- `trilogy file write query77.preql --content import raw.all_sales as all;

# Filter sales sold in the period, only include recorded outlets
where all.date.date…B' then 'web channel'
        end),
        all.outlet_id as profit
order by channel asc nulls first, outlet asc nulls first, returns desc
limit 100;`

  ```text

  ```
- `trilogy file write query84.preql --content import raw.customer as customer;
import raw.physical_returns as store_return;

# Match: customer's current demogra…stomer.text_id as customer_code,
    concat(customer.last_name, ', ', customer.first_name) as full_name
order by customer_code nulls first
limit 100;`

  ```text

  ```

### `undefined-concept`

- `trilogy run query29.preql`

  ```text

  ```
- `trilogy run query54.preql`

  ```text

  ```
- `trilogy run query75.preql`

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
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw/item:item select item.category, item.class, count(item.id) by item.category, item.class where item.category='Women' and item.class='maternity' limit 10;`

  ```text

  ```
- `trilogy run --import raw.catalog_sales:cs select cs.promotion.id, count(cs.line_item) where cs.promotion.id is null and cs.sold_date.year = 1999 limit 5;`

  ```text

  ```

### `join-resolution`

- `trilogy run query40.preql`

  ```text

  ```

### `type-error`

- `trilogy run query77.preql`

  ```text

  ```
