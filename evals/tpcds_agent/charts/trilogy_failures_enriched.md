# Trilogy failure analysis — 20260607-225157

- Run `20260607-225157` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 1908 | failed: 246 (13%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 103 | 42% |
| `undefined-concept` | 56 | 23% |
| `syntax-parse` | 50 | 20% |
| `syntax-missing-alias` | 13 | 5% |
| `cli-misuse` | 13 | 5% |
| `join-resolution` | 8 | 3% |
| `type-error` | 3 | 1% |

## Detail

### `other`

- `trilogy run query01.preql`

  ```text

  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query02.preql`

  ```text

  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query05.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/physical_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query06.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 38 column 13 (char 1455). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text

  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text

  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query13.preql`

  ```text

  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query16.preql`

  ```text

  ```
- `trilogy agent-info syntax example rollup`

  ```text

  ```
- `trilogy file read query19.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query20.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query21.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/inventory.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query24.preql`

  ```text

  ```
- `trilogy file read query27.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query30.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query31.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query37.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 32 column 12 (char 1413). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file read query39.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy run query44_debug5.preql`

  ```text

  ```
- `trilogy file read query44.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe item`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy database describe web_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy database describe customer`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy database describe customer_address`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/item.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/physical_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 68 column 12 (char 2716). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query51.preql`

  ```text

  ```
- `trilogy file read query54.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query56.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 38 column 12 (char 1570). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file write query59.preql -e -c`

  ```text
  Option '-c' requires an argument.
  ```
- `trilogy run query59.preql duckdb tpcds.duckdb`

  ```text
  cannot import name 'collect_clause_undefined' from 'trilogy.parsing.v2.select_finalize' (C:\Users\ethan\coding_projects\pytrilogy_two\trilogy\parsing\v2\select_finalize.py)
  ```
- `trilogy run query59.preql`

  ```text
  cannot import name 'raise_collected_undefined' from 'trilogy.parsing.v2.select_finalize' (C:\Users\ethan\coding_projects\pytrilogy_two\trilogy\parsing\v2\select_finalize.py)
  ```
- `trilogy file read trilogy.toml`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query60.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query61.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file write query62.preql --content import raw.web_sales as web;

where ship_date.year = 2000
  and warehouse.id is not null
  and ship_mode.id is not…ys_to_ship > 120)) as bucket_over_120

order by
  w_warehouse_name asc nulls first,
  sm_type asc nulls first,
  ws_name asc nulls first

limit 100;
`

  ```text
  cannot import name 'collect_clause_undefined' from 'trilogy.parsing.v2.select_finalize' (C:\Users\ethan\coding_projects\pytrilogy_two\trilogy\parsing\v2\select_finalize.py)
  ```
- `trilogy explore raw/store.preql`

  ```text
  cannot import name 'collect_clause_undefined' from 'trilogy.parsing.v2.select_finalize' (C:\Users\ethan\coding_projects\pytrilogy_two\trilogy\parsing\v2\select_finalize.py)
  ```
- `trilogy file read raw/store.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy explore raw/store.preql --show concepts`

  ```text
  cannot import name 'collect_clause_undefined' from 'trilogy.parsing.v2.select_finalize' (C:\Users\ethan\coding_projects\pytrilogy_two\trilogy\parsing\v2\select_finalize.py)
  ```
- `trilogy run query65.preql`

  ```text

  ```
- `trilogy file read query65.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query67.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query70.preql`

  ```text

  ```
- `trilogy run query70.preql`

  ```text

  ```
- `trilogy run query70.preql`

  ```text

  ```
- `trilogy run query74.preql`

  ```text

  ```
- `trilogy file read query74.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 50 column 12 (char 1813). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query75.preql`

  ```text

  ```
- `trilogy file read query75.preql`

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
- `trilogy run query76.preql`

  ```text

  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query81.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query82.preql`

  ```text

  ```
- `trilogy file read query82.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query83.preql`

  ```text

  ```
- `trilogy run query83_check.preql`

  ```text

  ```
- `trilogy file read query83.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query85.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query86.preql`

  ```text

  ```
- `trilogy run query89.preql`

  ```text

  ```
- `trilogy run query89.preql`

  ```text

  ```
- `trilogy file read query89.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query89.preql`

  ```text

  ```
- `trilogy file read raw/catalog_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/physical_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw/web_sales:ws select ws.order_number, count(ws.line_item) as line_count, count_distinct(ws.warehouse.id) as warehouse_count, bool_or(….web_site.company_name = 'pri' and count_distinct(ws.warehouse.id) > 1 and bool_or(ws.is_returned) is not true order by ws.order_number asc limit 20;`

  ```text

  ```
- `trilogy run --import raw/web_sales:ws select count(ws.order_number) as order_count, sum(ws.ext_ship_cost) as total_ship, sum(ws.net_profit) as total_profit w…dress.state = 'IL' and ws.web_site.company_name = 'pri' having count_distinct(ws.warehouse.id) > 1 and bool_or(ws.is_returned) is not true limit 100;`

  ```text

  ```
- `trilogy run --import raw/web_sales:ws # Rowset of qualifying orders: orders with >1 warehouse and no returns
rowset qualifying_orders <- 
  select ws.order_n…rder_number) as order_count,
  sum(ws.ext_ship_cost) as total_ship_cost,
  sum(ws.net_profit) as total_net_profit
order by order_count asc
limit 100;`

  ```text

  ```
- `trilogy run --import raw/web_sales:ws where ws.ship_date.date between '1999-02-01'::date and '1999-04-02'::date
  and ws.ship_address.state = 'IL'
  and ws.w…unt_distinct(ws.warehouse.id) by ws.order_number > 1
  and bool_or(ws.is_returned) by ws.order_number is not true
order by order_count asc
limit 100;`

  ```text

  ```
- `trilogy run --import raw/web_sales:ws rowset qualifying_orders <-
  select ws.order_number
  where ws.ship_date.date between '1999-02-01'::date and '1999-04-… by ws.order_number > 1
    and bool_or(ws.is_returned) by ws.order_number is not true;

select count(qualifying_orders.order_number) as order_count;`

  ```text

  ```
- `trilogy run --import raw/web_sales:ws auto applicable_orders <- ws.order_number
  ? ws.ship_date.date between '1999-02-01'::date and '1999-04-02'::date
    a…rder_number) as order_count,
  sum(ws.ext_ship_cost) as total_ship_cost,
  sum(ws.net_profit) as total_net_profit
order by order_count asc
limit 100;`

  ```text

  ```
- `trilogy run --import raw/web_sales:ws where ws.ship_date.date between '1999-02-01'::date and '1999-04-02'::date
  and ws.ship_address.state = 'IL'
  and ws.w…ouse.id) by ws.order_number as wc,
  --bool_or(ws.is_returned) by ws.order_number as hr
having wc > 1
  and hr is not true
order by wc asc
limit 100;`

  ```text

  ```
- `trilogy run --import raw/web_sales:ws # First, get the set of qualifying order numbers
auto q_orders <- ws.order_number
  ? count_distinct(ws.warehouse.id) b…count(ws.order_number) by * as order_count,
  sum(ws.ext_ship_cost) by * as total_ship_cost,
  sum(ws.net_profit) by * as total_net_profit
limit 100;`

  ```text

  ```
- `trilogy run --import raw/web_sales:ws where ws.ship_date.date between '1999-02-01'::date and '1999-04-02'::date
  and ws.ship_address.state = 'IL'
  and ws.w…otal_net_profit
having count_distinct(ws.warehouse.id) by ws.order_number > 1
  and bool_or(ws.is_returned) by ws.order_number is not true
limit 100;`

  ```text

  ```
- `trilogy run --import raw/web_sales:ws where ws.ship_date.date between '1999-02-01'::date and '1999-04-02'::date
  and ws.ship_address.state = 'IL'
  and ws.w…distinct(ws.warehouse.id) by ws.order_number > 1
  and bool_or(ws.is_returned) by ws.order_number is not true
order by ws.order_number asc
limit 100;`

  ```text

  ```
- `trilogy file read raw/physical_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query97.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query99.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
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
- `trilogy run query09.preql`

  ```text

  ```
- `trilogy run query10.preql`

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
- `trilogy run query15.preql`

  ```text

  ```
- `trilogy run query15.preql`

  ```text

  ```
- `trilogy run query16.preql`

  ```text

  ```
- `trilogy run query16.preql`

  ```text

  ```
- `trilogy run query18.preql`

  ```text

  ```
- `trilogy run query19.preql`

  ```text

  ```
- `trilogy run query30.preql`

  ```text

  ```
- `trilogy explore query30.preql`

  ```text

  ```
- `trilogy run query33.preql`

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
- `trilogy run --import raw/web_sales:web_sales select billing_customer.address.zip, billing_customer.address.city, sum(sales_price) as total_unit_price limit 5;`

  ```text

  ```
- `trilogy run query46.preql`

  ```text

  ```
- `trilogy run query59.preql`

  ```text

  ```
- `trilogy run query62.preql`

  ```text

  ```
- `trilogy run query70.preql`

  ```text

  ```
- `trilogy run query75.preql`

  ```text

  ```
- `trilogy run query85.preql`

  ```text

  ```
- `trilogy run query86.preql`

  ```text

  ```
- `trilogy run query88.preql`

  ```text

  ```
- `trilogy run --import raw/web_sales:ws # Rowset of qualifying orders: orders with >1 warehouse and no returns
rowset qualifying_orders <- 
  select ws.order_n…rder_number) as order_count,
  sum(ws.ext_ship_cost) as total_ship_cost,
  sum(ws.net_profit) as total_net_profit
order by order_count asc
limit 100;`

  ```text

  ```
- `trilogy run --import raw/web_sales:ws rowset qualifying_orders <-
  select ws.order_number, --count_distinct(ws.warehouse.id) by ws.order_number as wc, --boo…= 'IL'
    and ws.web_site.company_name = 'pri'
  having wc > 1
    and hr is not true;

select count(qualifying_orders.order_number) as order_count;`

  ```text

  ```
- `trilogy run --import raw/web_sales:ws rowset qualifying_orders <-
  select ws.order_number, --count_distinct(ws.warehouse.id) by ws.order_number as wc, --boo…s.state = 'IL'
    and ws.web_site.company_name = 'pri'
  having wc > 1
    and hr is not true;

select count(qualifying_orders.order_number) as cnt;`

  ```text

  ```
- `trilogy run query96.preql`

  ```text

  ```

### `syntax-parse`

- `trilogy run --import raw.all_sales:sales where sales.sales_channel in ('WEB','CATALOG') select sales.date.week_seq, min(sales.date.year) as min_yr, max(sales.date.year) as max_yr group by sales.date.week_seq order by sales.date.week_seq limit 10;`

  ```text

  ```
- `trilogy file write query04.preql --content import raw.all_sales as all_sales;

auto raw_total <- (all_sales.ext_list_price - all_sales.ext_wholesale_cost - a….billing_customer.id.text_id as billing_customer_code,
    all_sales.billing_customer.first_name,
    all_sales.billing_customer.last_name
limit 10;
`

  ```text

  ```
- `trilogy file write query05.preql --content import raw.all_sales as s;

# Sales arm: sales period, non-null sale entity
where s.date.date between '2000-08-23'…oalesce(sales_net_profit, 0) - coalesce(return_net_loss, 0) as net_profit
order by
    channel asc nulls first,
    entity asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query05.preql --content import raw.all_sales as s;

# Sales arm: sales data filtered by sale date and non-null sale entity
where s.date.da…    -- null entity_text means rollup - entity_id should be null for those rows
order by channel asc nulls first, entity_id asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query05.preql --content import raw.all_sales as s;

# Sales arm: sales data filtered by sale date and non-null sale entity
where s.date.da…returns,
    coalesce(net_profit_a, 0) - coalesce(net_loss_b, 0) as net_profit
order by channel asc nulls first, entity_id asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query05.preql --content import raw.all_sales as s;

# Sales arm: sales data filtered by sale date and non-null sale entity
where s.date.da…returns,
    coalesce(net_profit_a, 0) - coalesce(net_loss_b, 0) as net_profit
order by channel asc nulls first, entity_id asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query06.preql --content import raw/physical_sales as sales;

# Average current price per category across distinct items (each item counted…   count(sales.line_item) as qualifying_line_item_count
order by
    qualifying_line_item_count asc nulls first,
    state asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query10.preql -e -c import raw.customer as customer;
import raw.physical_sales as store_sales;
import raw.web_sales as web_sales;
import r…  customer.demographics.dependent_count,
  customer.demographics.employed_dependent_count,
  customer.demographics.college_dependent_count
limit 100;`

  ```text

  ```
- `trilogy file write query11.preql -e -c import raw.all_sales as s;

merge

# Arm 1: Store sales in 2001
where s.sales_channel = 'STORE' and year(s.date.date) … 0) as store_2002,
    coalesce(web_rev_2001, 0) as web_2001,
    coalesce(web_rev_2002, 0) as web_2002

order by
    cid asc nulls first
limit 100;
`

  ```text

  ```
- `trilogy file write query12.preql --content import raw.web_sales as ws;

# Filter to qualifying categories and date range
where ws.item.category in ('Sports',…s_total) * 100.0 as pct_of_class
order by
    ws.item.category,
    ws.item.class,
    ws.item.text_id,
    ws.item.desc,
    pct_of_class
limit 100;`

  ```text

  ```
- `trilogy file write query14.preql -e -c import raw.all_sales as sales;

# Count distinct channels this (brand, class, category) appears in, 1999-2001
auto com…t
having total_sales > overall_avg_sale
order by channel nulls first, brand_id nulls first, class_id nulls first, category_id nulls first
limit 100;
`

  ```text

  ```
- `trilogy file write query14.preql -e -c import raw.all_sales as sales;

auto combo_channels <- count_distinct(sales.sales_channel ? year(sales.date.date) betw…t
having total_sales > overall_avg_sale
order by channel nulls first, brand_id nulls first, class_id nulls first, category_id nulls first
limit 100;
`

  ```text

  ```
- `trilogy file write query14.preql -e -c import raw.all_sales as sales;

auto combo_channels <- count_distinct(sales.sales_channel ? year(sales.date.date) betw…t
having total_sales > overall_avg_sale
order by channel nulls first, brand_id nulls first, class_id nulls first, category_id nulls first
limit 100;
`

  ```text

  ```
- `trilogy file write query14.preql -e -c import raw.all_sales as sales;

auto combo_channels <- count_distinct(sales.sales_channel ? year(sales.date.date) betw…t
having total_sales > overall_avg_sale
order by channel nulls first, brand_id nulls first, class_id nulls first, category_id nulls first
limit 100;
`

  ```text

  ```
- `trilogy run --import raw.catalog_store_returns:csr select csr.store_sale_date.quarter_name as ss_q, csr.store_return_date.quarter_name as sr_q, csr.catalog_d…re csr.store_sale_date.quarter_name = '2001Q1' and csr.catalog_date.quarter_name in ('2001Q1', '2001Q2', '2001Q3') group by 1, 2, 3 order by 1, 2, 3;`

  ```text

  ```
- `trilogy run --import raw.catalog_store_returns:csr select csr.store_return_date.quarter_name as q, count(csr.ticket_number) as cnt where csr.store_return_date.quarter_name is not null and csr.store_sale_date.quarter_name = '2001Q1' group by q;`

  ```text

  ```
- `trilogy file write query23.preql --content import raw.physical_sales as store;
import raw.all_sales as sales;

# Frequent items: first 30 chars of desc that …null
order by sales.billing_customer.last_name asc, 
         sales.billing_customer.first_name asc, 
         total_sales asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query23.preql --content import raw.physical_sales as store;
import raw.all_sales as sales;

# Count per desc prefix - distinct dates per p…null
order by sales.billing_customer.last_name asc, 
         sales.billing_customer.first_name asc, 
         total_sales asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query23.preql --content import raw.physical_sales as store;
import raw.all_sales as sales;

# Frequent desc prefixes: count distinct sold …null
order by sales.billing_customer.last_name asc, 
         sales.billing_customer.first_name asc, 
         total_sales asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query23.preql --content import raw.physical_sales as store;
import raw.all_sales as sales;

# Frequent desc prefixes: count distinct sold …null
order by sales.billing_customer.last_name asc, 
         sales.billing_customer.first_name asc, 
         total_sales asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query24.preql --content import raw.physical_sales as ss;

where ss.store.market = 8
  and ss.is_returned = true
  and ss.customer.address.…as peach_sum
having
    peach_sum > 0.05 * global_avg
order by
    ss.customer.last_name asc,
    ss.customer.first_name asc,
    ss.store.name asc;
`

  ```text

  ```
- `trilogy run --import raw.catalog_store_returns:csr select csr.store_sale_date.year, csr.store_sale_date.month_of_year, count(*) from csr group by 1,2 order by 1,2 limit 20;`

  ```text

  ```
- `trilogy run --import raw.catalog_store_returns:csr select csr.catalog_date.year, csr.catalog_date.month_of_year, count(csr.ticket_number) as cnt where csr.catalog_date.year in (1999, 2000, 2001) group by 1,2 order by 1,2 limit 20;`

  ```text

  ```
- `trilogy file write query31.preql --content # Query 31: For each county and quarter in 2000, compute store and web extended sales price ratios
# Only counties…er: quarter, quarter
derive
    coalesce(store_sales, 0) as store_sales,
    coalesce(web_sales, 0) as web_sales
order by county, quarter
limit 1000;`

  ```text

  ```
- `trilogy file write query39.preql --content import raw.inventory as inv;

# Jan arm: year 2001, month_of_year = 1
where inv.date.year = 2001 and inv.date.mont… jan_cv_out asc nulls first,
         feb_month_of_year asc nulls first,
         feb_mean_out asc nulls first,
         feb_cv_out asc nulls first;
`

  ```text

  ```
- `trilogy file write query53.preql -e -c import raw.physical_sales as store_sales;

# Filter to year 2000 store sales with a recorded store
where store_sales.d…ly_sales) / avg_quarterly_sales > 0.1

order by
  avg_quarterly_sales asc,
  total_unit_price asc,
  store_sales.item.manufacturer_id asc

limit 100;`

  ```text

  ```
- `trilogy file write query54.preql --content import raw/physical_sales as ps;
import raw/catalog_sales as cs;
import raw/web_sales as ws;

# Customers who boug…s_price) / 50) * 50 as segment_times_50
order by
    segment asc nulls first,
    customer_count asc nulls first,
    segment_times_50 asc
limit 100;`

  ```text

  ```
- `trilogy file write query56.preql --content import raw.all_sales as all_sales;

# Arm 1: All qualifying items (colors slate, blanched, burnished)
where all_sa…   coalesce(b_total, b_total) as total_ext_sales_price

order by
    total_ext_sales_price asc nulls first,
    item_code asc nulls first

limit 100;`

  ```text

  ```
- `trilogy run --import raw.all_sales:all_sales --import raw.date:date select date.week_seq, date.date from raw.all_sales as all_sales left join all_sales.date.id = date.id where date.date = '2000-01-03'::date limit 5;`

  ```text

  ```
- `trilogy file write query63.preql --content import raw.physical_sales as store_sales;

# Profile A: category in Books, Children, Electronics with class in per…(monthly_total - avg_monthly) / avg_monthly > 0.1
order by
    store_sales.item.manager_id asc,
    avg_monthly asc,
    monthly_total asc
limit 100;`

  ```text

  ```
- `trilogy file write query63.preql --content import raw.physical_sales as store_sales;

# Profile A: category in Books, Children, Electronics with class in per…(monthly_total - avg_monthly) / avg_monthly > 0.1
order by
    store_sales.item.manager_id asc,
    avg_monthly asc,
    monthly_total asc
limit 100;`

  ```text

  ```
- `trilogy file write query63.preql --content import raw.physical_sales as store_sales;

# Profile A: category in Books, Children, Electronics with class in per…(monthly_total - avg_monthly) / avg_monthly > 0.1
order by
    store_sales.item.manager_id asc,
    avg_monthly asc,
    monthly_total asc
limit 100;`

  ```text

  ```
- `trilogy file write query63.preql --content import raw.physical_sales as store_sales;

# Items matching profile A or profile B
where
  (
    store_sales.item.…(monthly_total - avg_monthly) / avg_monthly > 0.1
order by
    store_sales.item.manager_id asc,
    avg_monthly asc,
    monthly_total asc
limit 100;`

  ```text

  ```
- `trilogy file write query63.preql --content import raw.physical_sales as store_sales;

# Per (manager_id, month_of_year): sum of unit price for store sales
au…(monthly_total - avg_monthly) / avg_monthly > 0.1
order by
    store_sales.item.manager_id asc,
    avg_monthly asc,
    monthly_total asc
limit 100;`

  ```text

  ```
- `trilogy file write query63.preql --content import raw.physical_sales as store_sales;

# Per (manager_id, month_of_year): sum of unit price for store sales
au…(monthly_total - avg_monthly) / avg_monthly > 0.1
order by
    store_sales.item.manager_id asc,
    avg_monthly asc,
    monthly_total asc
limit 100;`

  ```text

  ```
- `trilogy file write test.preql --content import raw.physical_sales as store_sales;
auto monthly_total <- sum(store_sales.sales_price) by (store_sales.item.man…store_sales.item.category in ('Books')
and year(store_sales.date.date) = 2000
select store_sales.item.manager_id, monthly_total, avg_monthly
limit 5;`

  ```text

  ```
- `trilogy file write query66.preql --content import raw.all_sales as s;

# Filtered sales
where s.sales_channel in ('WEB', 'CATALOG')
  and s.date.year = 2001
…) / nullif(wh_sqft, 0) as monthly_sales_per_sqft,
    coalesce(monthly_net, 0) as monthly_net
order by wh_name nulls first, yr nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query70.preql --content import raw.physical_sales as ss;

where ss.date.year = 2000

select
    ss.store.state,
    ss.store.county,
    s…rank(ss.store.state, ss.store.county) over (
        partition by _hierarchy_code, ss.store.state
        order by net_profit desc
    )

limit 100;
`

  ```text

  ```
- `trilogy file write query70.preql --content import raw.physical_sales as ss;

where ss.date.year = 2000

select
    ss.store.state,
    ss.store.county,
    s…k_hidden

having
    hierarchy_level != 1 or _state_rank_hidden <= 5

order by
    _gid desc,
    ss.store.state,
    within_parent_rank

limit 100;
`

  ```text

  ```
- `trilogy run --import raw.all_sales:sales select sales.date.year, count(sales.quantity) as cnt group by sales.date.year order by sales.date.year asc limit 10;`

  ```text

  ```
- `trilogy run --import raw.all_sales:sales select sales.sales_channel, count(sales.quantity) as cnt where sales.date.year = 2000 group by sales.sales_channel;`

  ```text

  ```
- `trilogy run --import raw.physical_sales:store select store.date.year, store.item.id, store.customer.id, sum(store.quantity) as qty where store.date.year = 20…tomer.id is not null and store.is_returned = false group by store.date.year, store.item.id, store.customer.id having sum(store.quantity) > 0 limit 5;`

  ```text

  ```
- `trilogy file write query84.preql --content import raw.physical_returns as returns;
import raw.customer as customer;

# Join customers to returns where the de…    concat(coalesce(customer.last_name, ''), ', ', coalesce(customer.first_name, '')) as full_name

order by customer_code asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query86.preql --content import raw.web_sales as ws;

def rollup_net_paid(metric) -> sum(metric) by rollup ws.item.category, ws.item.class;…archy_level >= 0
order by hierarchy_level desc nulls first,
         ws.item.category nulls first,
         rank_within_parent nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query87.preql --content import raw/physical_sales.preql as store;
import raw/catalog_sales.preql as catalog;
import raw/web_sales.preql as…t in web sales 2000
and store_key not in (web_key ? year(web.date.date) = 2000)
select 
    count_distinct(store_key) as unique_customer_date_combos;`

  ```text

  ```
- `trilogy file write query89.preql --content import raw.physical_sales as ss;

# Compute monthly total by full grain
# Then compute the cross-month average of …_of_year)) by (ss.item.category, ss.item.brand_name, ss.store.name, ss.store.company_name) as avg_monthly
truncate
order by
  ss.store.name
limit 10;`

  ```text

  ```
- `trilogy run --import raw/physical_sales:ss where ss.return_reason.desc = 'reason 28' select ss.customer.id, sum(case when ss.return_quantity is not null then….sales_price end) as total group by ss.customer.id having total is not null order by total asc nulls first, ss.customer.id asc nulls first limit 100;`

  ```text

  ```
- `trilogy run --import raw/web_sales:ws # Step 1: Get qualifying orders with their order-level aggregates
auto qualifying_orders <- select ws.order_number
  wh…r(ws.is_returned) by ws.order_number is not true;

# Step 2: Get totals for those orders
select count(qualifying_orders.order_number) as order_count;`

  ```text

  ```
- `trilogy run --import raw/web_sales:ws # Define order-level scoped metrics using inline filter syntax
# Use the pattern: aggregate filtered to the main WHERE …count(ws.order_number) by * as order_count,
  sum(ws.ext_ship_cost) by * as total_ship_cost,
  sum(ws.net_profit) by * as total_net_profit
limit 100;`

  ```text

  ```
- `trilogy file write query98.preql --content import raw/physical_sales as sales;

# Category filter
where sales.item.category in ('Sports', 'Books', 'Home')
# …y asc nulls first,
    class asc nulls first,
    item_code asc nulls first,
    description asc nulls first,
    pct_of_class_total asc nulls first;`

  ```text

  ```

### `syntax-missing-alias`

- `trilogy file write query05.preql --content import raw.all_sales as s;

# Gross sales: sum of extended sales price, filtered by sale date
# and non-null chann…alesce(s.channel_dim_text_id, s.return_channel_dim_text_id) as net_profit
order by channel_name asc nulls first, entity_id asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query14.preql -e -c import raw.all_sales as sales;

select sales.item.category_id, count(sales.order_id) limit 5;
`

  ```text

  ```
- `trilogy run --import raw.catalog_store_returns:csr select count(csr.ticket_number);`

  ```text

  ```
- `trilogy run --import raw.catalog_store_returns:csr select csr.store_sale_date.year, csr.store_sale_date.month_of_year, count(csr.ticket_number) order by 1,2 limit 20;`

  ```text

  ```
- `trilogy run --import raw.item:item select distinct item.size limit 10;`

  ```text

  ```
- `trilogy file write query47.preql --content import raw.physical_sales as store_sales;

# Filter: Dec 1998, all of 1999, Jan 2000
where (store_sales.date.year …e_sales.date.year as year,
    store_sales.date.month_of_year as month_of_year,
    --monthly_total will go here but we need to compute it
limit 100;`

  ```text

  ```
- `trilogy run --import raw.physical_sales:store_sales select distinct store_sales.item.brand_name limit 50;`

  ```text

  ```
- `trilogy run --import raw.store:store -`

  ```text

  ```
- `trilogy file write query70.preql --content import raw.physical_sales as ss;

# Total net profit by state and county
# Use rollup to get state-level, state+co…ank <= 5

order by
    hierarchy_level desc,
    case when hierarchy_level = 2 then null else ss.store.state end,
    within_parent_rank

limit 100;
`

  ```text

  ```
- `trilogy file write query70.preql --content import raw.physical_sales as ss;

# Rank states by net profit in year 2000
auto state_rank <- rank(ss.store.state)…hen grouping_id(ss.store.state, ss.store.county) in (0, 1)
        then ss.store.state
        else null
    end,
    within_parent_rank

limit 100;
`

  ```text

  ```
- `trilogy file write query77.preql --content import raw.all_sales as s;

# Computed channel label
auto channel_label <-
    case
        when s.sales_channel =… 0))) by rollup channel_label, s.outlet_id as profit
order by
    channel_label nulls first,
    s.outlet_id nulls first,
    returns desc
limit 100;`

  ```text

  ```
- `trilogy file write query80.preql --content import raw.all_sales as s;

auto channel_label <-
  case
    when s.sales_channel = 'STORE' then 'store channel'
 …eturn_net_loss, 0)) by rollup channel_label, outlet_identifier as profit
order by channel_label nulls first, outlet_identifier nulls first
limit 100;`

  ```text

  ```
- `trilogy run --import raw.all_sales:s select distinct s.sales_channel;`

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
- `trilogy explore raw/store_returns.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_returns.preql' does not exist.
  ```
- `trilogy run query27.preql --format json`

  ```text
  '--format' is not a valid dialect. The dialect argument comes after the input file and any options.
    Try: trilogy run <input> [<dialect>]
  ```
- `trilogy explore raw/store_sales.preql --regex list_price|coupon|wholesale|quantity`

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
- `trilogy explore raw/sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/sales.preql' does not exist.
  ```
- `trilogy explore raw/sales_1.preql`

  ```text
  Invalid value for 'PATH': File 'raw/sales_1.preql' does not exist.
  ```
- `trilogy run query66.preql --format json`

  ```text
  '--format' is not a valid dialect. The dialect argument comes after the input file and any options.
    Try: trilogy run <input> [<dialect>]
  ```
- `trilogy run query80.preql --format json`

  ```text
  '--format' is not a valid dialect. The dialect argument comes after the input file and any options.
    Try: trilogy run <input> [<dialect>]
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```

### `join-resolution`

- `trilogy run query16.preql`

  ```text

  ```
- `trilogy run query21.preql`

  ```text

  ```
- `trilogy run query22.preql`

  ```text

  ```
- `trilogy run --import raw.physical_sales:store --import raw.web_sales:web select store.sale_address.county, sum(store.ext_sales_price ? store.date.year=2000 a… and web.date.quarter=3) as wq3 where store.sale_address.county = web.bill_address.county and store.sale_address.county = 'Abbeville County' limit 5;`

  ```text

  ```
- `trilogy run query40.preql`

  ```text

  ```
- `trilogy run --import raw.catalog_sales:cs select cs.warehouse.state, cs.item.text_id, cs.sales_price, cs.sold_date.date, cs.warehouse_id limit 5;`

  ```text

  ```
- `trilogy run query52.preql`

  ```text

  ```
- `trilogy run --import raw.physical_sales:store_sales --import raw.store:store --import raw.date:date -`

  ```text

  ```

### `type-error`

- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text

  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text

  ```
- `trilogy run query90.preql`

  ```text

  ```
