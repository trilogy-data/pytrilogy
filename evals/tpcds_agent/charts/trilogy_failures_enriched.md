# Trilogy failure analysis — 20260607-211207

- Run `20260607-211207` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 114 | failed: 24 (21%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 15 | 62% |
| `syntax-parse` | 7 | 29% |
| `syntax-missing-alias` | 1 | 4% |
| `join-resolution` | 1 | 4% |

## Detail

### `other`

- `trilogy run --import raw.physical_sales:ps auto null_addr_avg <- avg(ps.net_profit ? ps.store.id = 1 and ps.sale_address.id is null) by *; auto threshold <- …select ps.item.product_name as prod, avg(ps.net_profit) as avg_prof where ps.store.id = 1 having avg_prof > threshold order by avg_prof asc limit 10;`

  ```text

  ```
- `trilogy run --import raw.physical_sales:ps auto null_addr_avg <- avg(ps.net_profit ? ps.store.id = 1 and ps.sale_address.id is null) by *; auto threshold <- …nk derive coalesce(a_prod, a_prod) as best_prod, coalesce(b_prod, b_prod) as worst_prod order by rank asc, best_prod desc, worst_prod desc limit 100;`

  ```text

  ```
- `trilogy run --import raw.physical_sales:ps auto null_addr_avg <- avg(ps.net_profit ? ps.store.id = 1 and ps.sale_address.id is null) by *; auto threshold <- …nk derive coalesce(a_prod, a_prod) as best_prod, coalesce(b_prod, b_prod) as worst_prod order by rank asc, best_prod desc, worst_prod desc limit 100;`

  ```text

  ```
- `trilogy run --import raw.physical_sales:ps auto null_addr_avg <- avg(ps.net_profit ? ps.store.id = 1 and ps.sale_address.id is null) by *; auto threshold <- …nk derive coalesce(a_prod, a_prod) as best_prod, coalesce(b_prod, b_prod) as worst_prod order by rank asc, best_prod desc, worst_prod desc limit 100;`

  ```text

  ```
- `trilogy run --import raw.physical_sales:ps auto null_addr_avg <- avg(ps.net_profit ? ps.store.id = 1 and ps.sale_address.id is null) by *; auto threshold <- …nk derive coalesce(a_prod, a_prod) as best_prod, coalesce(b_prod, b_prod) as worst_prod order by rank asc, best_prod desc, worst_prod desc limit 100;`

  ```text

  ```
- `trilogy run --import raw.physical_sales:ps auto null_addr_avg <- avg(ps.net_profit ? ps.store.id = 1 and ps.sale_address.id is null) by *; auto threshold <- 0.9 * null_addr_avg; where ps.store.id = 1 select count(ps.item.product_name) as cnt, --threshold having avg(ps.net_profit) > threshold;`

  ```text

  ```
- `trilogy run --import raw.physical_sales:ps auto null_addr_avg <- avg(ps.net_profit ? ps.store.id = 1 and ps.sale_address.id is null) by *; auto threshold <- 0.9 * null_addr_avg; where ps.store.id = 1 select count(ps.item.product_name) as cnt having avg(ps.net_profit) > threshold;`

  ```text

  ```
- `trilogy file read query44.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 1 column 4213 (char 4212). Re-issue the call with valid JSON arguments.
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
- `trilogy file read raw/physical_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query78.preql --import raw.physical_sales:s select year(s.date.date), sum(s.quantity), count(s.ticket_number) where year(s.date.date)=2000 and s.customer.id is not null and s.is_returned=false limit 10;`

  ```text

  ```

### `syntax-parse`

- `trilogy run --import raw.physical_sales:ps select ps.item.product_name, avg(ps.net_profit) as avg_prof where ps.store.id = 1 group by ps.item.product_name order by avg_prof asc limit 10;`

  ```text

  ```
- `trilogy run --import raw.physical_sales:ps auto null_addr_avg <- avg(ps.net_profit ? ps.store.id = 1 and ps.sale_address.id is null) by *; auto threshold <- …rod) over (order by avg_prof asc) as asc_rnk, rank(prod) over (order by avg_prof desc) as desc_rnk from filtered_items order by asc_rnk asc limit 15;`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.physical_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

merge cs.item.…  coalesce(cnt_00, cnt_00) as count_00
having count_00 <= count_99
order by product_name, store_name, count_00, wholesale_99, wholesale_00
limit 100;`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.physical_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

merge cs.item.…  coalesce(cnt_00, cnt_00) as count_00
having count_00 <= count_99
order by product_name, store_name, count_00, wholesale_99, wholesale_00
limit 100;`

  ```text

  ```
- `trilogy file write query78.preql --content import raw.physical_sales as store;
import raw.web_sales as web;
import raw.catalog_sales as cat;

# Store arm: ye…r asc, ss_qty desc, ss_wholesale_cost desc, ss_sales_price desc, other_qty asc, other_wholesale_cost asc, other_sales_price asc, ratio asc
limit 100;`

  ```text

  ```
- `trilogy run --import raw.physical_sales:s select year(s.date.date) as yr, s.customer.id as cust_id, sum(s.quantity) as qty where year(s.date.date)=2000 and s.customer.id is not null and s.is_returned=false group by yr, cust_id limit 10;`

  ```text

  ```
- `trilogy run --import raw.physical_sales:s --import raw.web_sales:w merge s.customer.id into w.billing_customer.id; select s.customer.id as cust_id, count_dis…s.date.date)=2000 and s.customer.id is not null and s.is_returned=false and year(w.date.date)=2000 and w.is_returned=false group by cust_id limit 10;`

  ```text

  ```

### `syntax-missing-alias`

- `trilogy run --import raw.physical_sales:s select year(s.date.date), sum(s.quantity), count(s.ticket_number) where year(s.date.date)=2000 and s.customer.id is not null and s.is_returned=false limit 10;`

  ```text

  ```

### `join-resolution`

- `trilogy run --import raw.physical_sales:s --import raw.web_sales:w select year(s.date.date) as yr, s.item.id as item_id, s.customer.id as cust_id, sum(s.quan… s.is_returned=false and year(w.date.date)=2000 and w.billing_customer.id = s.customer.id and w.item.id = s.item.id and w.is_returned=false limit 10;`

  ```text

  ```
