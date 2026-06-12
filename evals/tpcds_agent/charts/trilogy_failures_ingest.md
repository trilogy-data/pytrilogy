# Trilogy failure analysis — 20260612-133010

- Run `20260612-133004_ingest` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 389 | failed: 65 (17%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 41 | 63% |
| `syntax-parse` | 11 | 17% |
| `join-resolution` | 10 | 15% |
| `cli-misuse` | 2 | 3% |
| `undefined-concept` | 1 | 2% |

## Detail

### `other`

- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
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
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query54.preql`

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
- `trilogy file read raw/inventory.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query72.preql`

  ```text

  ```
- `trilogy run query72.preql`

  ```text

  ```
- `trilogy file read raw/store_sales.preql`

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
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 56 column 12 (char 1990). Re-issue the call with valid JSON arguments.
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
- `trilogy file read query82.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query84.preql`

  ```text

  ```

### `syntax-parse`

- `trilogy file write query40.preql --content import raw.catalog_sales as cs;

# Join catalog returns to get refunded cash per (order_number, item_sk)
inner joi…ded_cash, 0))
        ? cs.sold_date.date >= '2000-03-11'::date
    ), 0) as after_0311_total

order by warehouse_state asc, item_code asc
limit 100;`

  ```text

  ```
- `trilogy file write query44.preql -e -c import raw.store_sales as ss;

# Threshold: avg net profit at store 1 where customer address is null
auto threshold_av…d_avg
order by
    avg_profit desc
limit 10;

# Now pair them - but trilogy doesn't have row_number for enumeration...
# Let me rethink the approach
`

  ```text

  ```
- `trilogy file write query44.preql --content import raw.store_sales as ss;

# Threshold: avg net profit at store 1 where customer address is null (global)
auto…est.rnk as rank,
    best.product_name as best_product,
    worst.product_name as worst_product
inner join ranked_best.item_sk = worst_items.item_sk
`

  ```text

  ```
- `trilogy file write query44.preql --content import raw.store_sales as ss;

rowset threshold <-
where ss.store.store_sk = 1 and ss.customer.customer_address.ad…alified.product_name desc) as pos
    from qualified
    limit 10
) worst.pos
order by rank asc, best_performer desc, worst_performer desc
limit 100;`

  ```text

  ```
- `trilogy file write query44.preql --content import raw.store_sales as ss;

rowset threshold <-
where ss.store.store_sk = 1 and ss.customer.customer_address.ad…r (order by qualified.avg_profit asc, qualified.product_name asc) as pos
order by pos asc
limit 10;

select count(best10.item_sk) as cnt from best10;`

  ```text

  ```
- `trilogy file write query64.preql -c import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.store_returns …omer.first_shipto_date.year as first_shipto_yr
having 
  sale_year in (1999, 2000)
order by 
  ss.item.product_name,
  ss.store.store_name
limit 100;`

  ```text

  ```
- `trilogy file write query64.preql -e -c import raw.catalog_sales as cs;\nimport raw.catalog_returns as cr;\n\n# Join catalog sales and returns on item_sk and … cr.store_credit) by item.item_sk);\n\nimport raw.store_sales as ss;\n\nwhere\n  ss.item.item_sk in qual_sk\nlimit 5\nselect\n  ss.item.product_name;`

  ```text

  ```
- `trilogy file write query64.preql -e -c import raw.catalog_sales as cs;\nimport raw.catalog_returns as cr;\n\n# Stage 1: Items with catalog cumulative ext_lis…rital_status != ss.customer.customer_demographics.marital_status\nselect\n  ss.item.product_name,\n  count(ss.ticket_number) as sale_count\nlimit 10;`

  ```text

  ```
- `trilogy file write query64.preql -e -c import raw.catalog_sales as cs;\nimport raw.catalog_returns as cr;\n\nrowset qual_items <- select\n  cs.item.item_sk a…_sales.first_sales_yr,\n  agg_sales.first_shipto_yr\norder by\n  agg_sales.product_name,\n  agg_sales.store_name,\n  agg_sales.sale_count\nlimit 100;`

  ```text

  ```
- `trilogy file write query72.preql --content import raw.catalog_sales as cs;
import raw.inventory as inv;
import raw.date_dim as dd;

# Catalog sales in 1999 w…d < cs.quantity
order by total_count desc nulls first, item_description nulls first, warehouse_name nulls first, week_sequence nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query76.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_sale…issing_ref asc nulls first,
    combined.year asc nulls first,
    combined.quarter asc nulls first,
    combined.category asc nulls first
limit 100;`

  ```text

  ```

### `join-resolution`

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
- `trilogy run query82.preql`

  ```text

  ```

### `cli-misuse`

- `trilogy explore raw/sales/sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/sales/sales.preql' does not exist.
  ```
- `trilogy raw/catalog_sales.preql`

  ```text
  No such command 'raw/catalog_sales.preql'.
  ```

### `undefined-concept`

- `trilogy run query46.preql`

  ```text

  ```
