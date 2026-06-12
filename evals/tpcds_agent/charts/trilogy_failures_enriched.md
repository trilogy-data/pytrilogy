# Trilogy failure analysis — 20260612-133010

- Run `20260612-133004_enriched` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 356 | failed: 85 (24%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 50 | 59% |
| `syntax-parse` | 16 | 19% |
| `undefined-concept` | 12 | 14% |
| `cli-misuse` | 4 | 5% |
| `join-resolution` | 2 | 2% |
| `syntax-missing-alias` | 1 | 1% |

## Detail

### `other`

- `trilogy file read raw/catalog_store_returns.preql`

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
- `trilogy file read raw/physical_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy run --import raw.physical_sales:ps --import raw.store:store select ps.item.id, ps.item.product_name, avg(ps.net_profit) as avg_profit where store.id = 1 order by avg_profit asc limit 5;`

  ```text

  ```
- `trilogy run --import raw.physical_sales:ps select ps.item.id, avg(ps.net_profit) as avg_profit where ps.store.id = 1 having avg_profit > 0.9 * avg(ps.net_profit ? ps.store.id = 1 and ps.sale_address.id is null) by * order by avg_profit asc limit 15;`

  ```text

  ```
- `trilogy run query54.preql`

  ```text

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
- `trilogy file read raw/catalog_sales.preql`

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
- `trilogy run query72.preql`

  ```text

  ```
- `trilogy run query72.preql`

  ```text

  ```
- `trilogy run query72.preql`

  ```text

  ```
- `trilogy run query72.preql`

  ```text

  ```
- `trilogy run query72.preql`

  ```text

  ```
- `trilogy run query72.preql`

  ```text

  ```
- `trilogy run query72.preql`

  ```text

  ```
- `trilogy run query72.preql`

  ```text

  ```
- `trilogy run query72.preql`

  ```text

  ```
- `trilogy run query72.preql`

  ```text

  ```
- `trilogy run query72.preql`

  ```text

  ```
- `trilogy run query72.preql`

  ```text

  ```
- `trilogy run query72.preql`

  ```text

  ```
- `trilogy run query72.preql`

  ```text

  ```
- `trilogy run query72.preql`

  ```text

  ```
- `trilogy run query75.preql`

  ```text

  ```
- `trilogy file read raw/physical_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query76.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query84.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy file write query40.preql --content import raw.catalog_sales as cs;

# Join with catalog_returns to get refunded_cash, matching on (item.id, order_num…rns.refunded_cash, 0)) ? cs.sold_date.date >= '2000-03-11'::date), 0) as total_on_or_after
order by
  warehouse_state asc,
  item_code asc
limit 100;`

  ```text

  ```
- `trilogy run --import raw.physical_sales:ps --import raw.store:store select ps.item.id, ps.item.product_name, avg(ps.net_profit) as avg_profit where store.id = 1 group by ps.item.id order by avg_profit asc limit 5;`

  ```text

  ```
- `trilogy run --import raw.physical_sales:ps select ps.item.id, ps.item.product_name, avg(ps.net_profit) as avg_profit, count(ps.ticket_number) as cnt where ps.store.id = 1 group by ps.item.id order by avg_profit asc limit 15;`

  ```text

  ```
- `trilogy run --import raw.physical_sales:ps with best as (where ps.store.id = 1 select ps.item.id, ps.item.product_name, avg(ps.net_profit) as avg_profit havi…t asc) select best.ps.item.id, best.avg_profit, rank(best.ps.item.id) over (order by best.avg_profit asc) as rnk from best order by rnk asc limit 10;`

  ```text

  ```
- `trilogy file write query44.preql --content import raw.physical_sales as ps;

# Average net profit at store 1 where address key is null, global
auto avg_null_… where all_worst.rn <= 10) as worst
inner join best.rn = worst.rn
order by
    rank asc,
    best_performer desc,
    worst_performer desc
limit 100;`

  ```text

  ```
- `trilogy file write query54.preql --content import raw.catalog_sales as cs;
import raw.web_sales as ws;
import raw.physical_sales as ps;
import raw.customer a…e customers who had store sales
  total_price is not null
order by
  segment nulls first,
  customer_count nulls first,
  segment_times_50
limit 100;`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.physical_sales as store_sales;
import raw.catalog_sales as cat_sales;
import raw.catalog_returns as cat…re_zip
having
    y2000.cnt <= y1999.cnt
order by
    y1999.product_name,
    y1999.store_name,
    y2000.cnt,
    y1999.ws_cost,
    y2000.ws_cost
;`

  ```text

  ```
- `trilogy file write query72.preql -e -c import raw.catalog_sales as cs;
import raw.inventory as inv;

where
  cs.sold_date.year = 1999
  and cs.bill_household…r by
  total_count desc nulls first,
  item_description asc nulls first,
  warehouse_name asc nulls first,
  week_sequence asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query72.preql -e -c import raw.catalog_sales as cs;
import raw.inventory as inv;

where
  cs.sold_date.year = 1999
  and cs.bill_household…r by
  total_count desc nulls first,
  item_description asc nulls first,
  warehouse_name asc nulls first,
  week_sequence asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query72.preql -e -c import raw.catalog_sales as cs;
import raw.inventory as inv;

rowset matched_sales <-
  where
    cs.sold_date.year = … matched_sales.inv_qty < (select cs.quantity where cs.order_number = matched_sales.order_number and cs.item.id = matched_sales.item_id)
select
  ...
`

  ```text

  ```
- `trilogy file write query72.preql -e -c import raw.catalog_sales as cs;
import raw.inventory as inv;

rowset inv_low <-
  where
    inv.quantity_on_hand < cs.…s.bill_household_demographic.buy_potential = '>10000'
  and cs.bill_customer_demographic.marital_status = 'D'
  and cs.days_to_ship > 5
select
  ...
`

  ```text

  ```
- `trilogy file write query72.preql -e -c import raw.catalog_sales as cs;
import raw.inventory as inv;

inner join inv.item.id = cs.item.id
inner join inv.date.…r by
  total_count desc nulls first,
  item_description asc nulls first,
  warehouse_name asc nulls first,
  week_sequence asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query75.preql --content import raw.all_sales as s;

# Deduplicate per-line records before aggregation
# Each line is unique per (order_id,…d cast(coalesce(yr2002.qty_2002, 0) as numeric) / cast(coalesce(yr2001.qty_2001, 0) as numeric) < 0.9
order by qty_diff asc, amt_diff asc
limit 100;
`

  ```text

  ```
- `trilogy file write query75.preql --content import raw.all_sales as s;

# Filter to Books category, years 2001-2002
where s.item.category = 'Books'
  and (s.d…y, 0) > 0
  and cast(coalesce(curr.qty, 0) as numeric) / cast(coalesce(prev.qty, 0) as numeric) < 0.9
order by qty_diff asc, amt_diff asc
limit 100;
`

  ```text

  ```
- `trilogy file write query75.preql --content import raw.all_sales as s;

# Net per-line metrics
auto net_qty <- s.quantity - coalesce(s.return_quantity, 0);
au…id, curr_row.class_id, curr_row.category_id, curr_row.manufacturer_id order by curr_row.yr asc) < 0.9
order by qty_diff asc, amt_diff asc
limit 100;
`

  ```text

  ```
- `trilogy file write query76.preql --content import raw.physical_sales as sales;
import raw.web_sales as ws;
import raw.catalog_sales as cs;

# Store: store re…y
    combined.channel,
    combined.missing_ref,
    combined.yr nulls first,
    combined.qtr nulls first,
    combined.cat nulls first
limit 100;
`

  ```text

  ```

### `undefined-concept`

- `trilogy run query44.preql`

  ```text

  ```
- `trilogy explore query44.preql --regex ranked_items`

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
- `trilogy run query64.preql`

  ```text

  ```
- `trilogy run test_rowset.preql`

  ```text

  ```
- `trilogy run test_rowset.preql`

  ```text

  ```
- `trilogy run query64.preql`

  ```text

  ```
- `trilogy run query72.preql`

  ```text

  ```
- `trilogy run query72.preql`

  ```text

  ```
- `trilogy run query72.preql`

  ```text

  ```

### `cli-misuse`

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
- `trilogy run query75_check.preql --content import raw.all_sales as s;
where s.item.category = 'Books'
select distinct s.item.category_id as cat_id
limit 20;
`

  ```text
  '--content' is not a valid dialect. The dialect argument comes after the input file and any options.
    Try: trilogy run <input> [<dialect>]
  ```

### `join-resolution`

- `trilogy run query44.preql`

  ```text

  ```
- `trilogy run query44.preql`

  ```text

  ```

### `syntax-missing-alias`

- `trilogy run --import raw.all_sales:s select distinct s.item.category_id as cat_id limit 20;`

  ```text

  ```
