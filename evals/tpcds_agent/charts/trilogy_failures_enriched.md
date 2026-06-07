# Trilogy failure analysis — 20260607-025517

- Run `20260607-025517` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 326 | failed: 63 (19%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 33 | 52% |
| `syntax-parse` | 16 | 25% |
| `cli-misuse` | 6 | 10% |
| `syntax-missing-alias` | 3 | 5% |
| `type-error` | 2 | 3% |
| `undefined-concept` | 2 | 3% |
| `join-resolution` | 1 | 2% |

## Detail

### `other`

- `trilogy file read query02.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query06.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query06.preql`

  ```text

  ```
- `trilogy run query06.preql`

  ```text

  ```
- `trilogy run query06.preql`

  ```text

  ```
- `trilogy file read query06.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw\date.preql`

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
- `trilogy file read query44.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/physical_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query50.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/physical_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query59.preql`

  ```text

  ```
- `trilogy run query64.preql`

  ```text

  ```
- `trilogy file read query75.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
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
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 33 column 12 (char 1988). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 38 column 12 (char 1616). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 34 column 12 (char 1450). Re-issue the call with valid JSON arguments.

  [guidance] You have issued this identical call 3 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy file read query78.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy file write query02.preql --content import raw.all_sales as all_sales;

# Filter to web and catalog sales only
auto wc_sales <- all_sales.ext_sales_pr…s, 53) over (order by all_sales.date.week_seq), 2) as sat_ratio
where all_sales.date.week_seq in wk2001
order by all_sales.date.week_seq nulls first;`

  ```text

  ```
- `trilogy run --import raw.date:d select d.year, count(d.id) as id_count group by d.year order by d.year asc limit 10;`

  ```text

  ```
- `trilogy file write query49.preql --content import raw.all_sales as sales;

# Filter to December 2001, positive conditions, matching return > 10000
where sale…= 10
order by
  channel asc nulls first,
  return_qty_rank asc nulls first,
  return_currency_rank asc nulls first,
  item asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query64.preql -e -c import raw.physical_sales as ss;
import raw.catalog_returns as cat_ret;

# Items where cumulative catalog ext_list_pri…, 0) as count_2000
having count_2000 <= count_1999
order by item_product_name, item_store_name, count_2000, wholesale_1999, wholesale_2000
limit 100;`

  ```text

  ```
- `trilogy file write query64.preql -e -c import raw.physical_sales as ss;
import raw.catalog_returns as cat_ret;

# Items where cumulative catalog ext_list_pri…, 0) as count_2000
having count_2000 <= count_1999
order by item_product_name, item_store_name, count_2000, wholesale_1999, wholesale_2000
limit 100;`

  ```text

  ```
- `trilogy file write query64.preql -e -c import raw.physical_sales as ss;
import raw.catalog_returns as cat_ret;

# Items where cumulative catalog ext_list_pri…, 0) as count_2000
having count_2000 <= count_1999
order by item_product_name, item_store_name, count_2000, wholesale_1999, wholesale_2000
limit 100;`

  ```text

  ```
- `trilogy file write query64.preql -e -c import raw.physical_sales as ss;
import raw.catalog_returns as cat_ret;

# Items where cumulative catalog ext_list_pri…, 0) as count_2000
having count_2000 <= count_1999
order by item_product_name, item_store_name, count_2000, wholesale_1999, wholesale_2000
limit 100;`

  ```text

  ```
- `trilogy file write query75.preql --content import raw.all_sales as sales;

# Filter to Books category only
auto books_item <- sales.item.category = 'Books';
…qty,
  y2002_qty as curr_qty,
  y2002_qty - y2001_qty as qty_diff,
  y2002_amt - y2001_amt as amt_diff
order by qty_diff asc, amt_diff asc
limit 100;`

  ```text

  ```
- `trilogy file write query76.preql --content import raw.physical_sales as store_sales;
import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_s…ice
where catalog_sales.customer_address.id is null

order by channel, missing_ref_label, year, quarter_of_year, item_category nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query76.preql --content import raw.physical_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sales as cs;

auto store_lines <- …ales_price) as total_ext_sales_price
where cs.customer_address.id is null
group by channel, missing_ref_label, year, quarter_of_year, item_category;
`

  ```text

  ```
- `trilogy file write query78.preql --content import raw.physical_sales as store;
import raw.catalog_sales as cat;
import raw.web_sales as web;

# Arm 1: Store …y yr, item_id, customer_id, store_qty_all desc, store_wc_all desc, store_sp_all desc, other_qty asc, other_wc asc, other_sp asc, ratio asc
limit 100;`

  ```text

  ```
- `trilogy file write query78.preql --content import raw.physical_sales as store;
import raw.catalog_sales as cat;
import raw.web_sales as web;

# Arm 1: Store …y yr, item_id, customer_id, store_qty_all desc, store_wc_all desc, store_sp_all desc, other_qty asc, other_wc asc, other_sp asc, ratio asc
limit 100;`

  ```text

  ```
- `trilogy file write query86.preql -c import raw.web_sales as ws;

# Web sales in year 2000, net paid rolled up by item category and class
where year(ws.date.d…     else null
    end as item_class,
    total,
    level,
    rnk
order by level desc nulls first, category nulls first, rnk nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query86.preql -c import raw.web_sales as ws;

# Web sales in year 2000, net paid rolled up by item category and class

def rollup_metrics(…  order by @rollup_metrics(ws.net_paid) desc
    ) as rnk
order by hierarchy_level desc nulls first, category nulls first, rnk nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query86.preql -c import raw.web_sales as ws;

# Web sales in year 2000, net paid rolled up by item category and class

def rollup_net_paid…  end
        order by @rollup_net_paid desc
    ) as rnk
order by hierarchy_level desc nulls first, category nulls first, rnk nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query86.preql -c import raw.web_sales as ws;

# Web sales in year 2000, net paid rolled up by item category and class

def rollup_agg(metr…      order by @rollup_agg(ws.net_paid) desc
    ) as rnk
order by hierarchy_level desc nulls first, category nulls first, rnk nulls first
limit 100;`

  ```text

  ```

### `cli-misuse`

- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy run query49.preql --format json`

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
- `trilogy run - --import raw.physical_sales:sales select sales.date.year, min(sales.date.week_seq) as min_ws, max(sales.date.week_seq) as max_ws where sales.date.year in (2001, 2002) limit 10;`

  ```text
  'select sales.date.year, min(sales.date.week_seq) as min_ws, max(sales.date.week_seq) as max_ws where sales.date.year in (2001, 2002) limit 10;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy run query77.preql --format json --debug`

  ```text
  '--format' is not a valid dialect. The dialect argument comes after the input file and any options.
    Try: trilogy run <input> [<dialect>]
  ```

### `syntax-missing-alias`

- `trilogy run --import raw.date:d select d.quarter_name, count(d.id) limit 10;`

  ```text

  ```
- `trilogy file write query77.preql --content import raw.all_sales as sales;

# Channel label based on sales_channel enum
auto channel_label <- case
    when sa…lup channel_label, sales.outlet_id as profit_agg
order by channel_label asc nulls first, sales.outlet_id asc nulls first, returns_agg desc
limit 100;`

  ```text

  ```
- `trilogy file write query86.preql --content import raw.web_sales as ws;

# Web sales in year 2000, net paid rolled up by item category and class
where year(ws…c
    ) as within_parent_rank

order by
    hierarchy_level desc nulls first,
    category nulls first,
    within_parent_rank nulls first
limit 100;`

  ```text

  ```

### `type-error`

- `trilogy run --import raw.catalog_store_returns:csr select min(csr.store_sale_date.quarter_name) as min_ss_q, max(csr.store_sale_date.quarter_name) as max_ss_… max(csr.store_return_date.quarter_name) as max_sr_q, min(csr.catalog_date.quarter_name) as min_cs_q, max(csr.catalog_date.quarter_name) as max_cs_q;`

  ```text

  ```
- `trilogy run query44.preql`

  ```text

  ```

### `undefined-concept`

- `trilogy run query59.preql`

  ```text

  ```
- `trilogy run query86.preql`

  ```text

  ```

### `join-resolution`

- `trilogy run query06.preql`

  ```text

  ```
