# Trilogy failure analysis — 20260711-201848

- Run `20260711-201848_enriched` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 1104 | failed: 97 (9%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 55 | 57% |
| `syntax-parse` | 35 | 36% |
| `cli-misuse` | 2 | 2% |
| `type-error` | 2 | 2% |
| `syntax-missing-alias` | 2 | 2% |
| `planner-recursion` | 1 | 1% |

## Detail

### `other`

- `trilogy run answer_3863442186.preql`

  ```text
  Syntax error in answer_3863442186.preql: 9 undefined concept references; fix all before re-running:
    - local.s2001 (line 52, col 9, in WHERE); did you mean: combined.s2001, w2001, s2002, c2001?
    - local.c2001 (line 52, col 23, in WHERE); did you mean: combined.c2001, w2001, s2001, c2002?
    - local.w2001 (line 52, col 37, in WHERE); did you mean: combined.w2001, w2002, s2001, c2001?
    - local.c2002 (line 44, in WHERE); did you mean: combined.c2002, w2002, s2002, c2001?
    - local.c2001 (line 44, in WHERE); did you mean: combined.c2001, w2001, s2001, c2002?
    - local.s2002 (line 44, in WHERE); did you mean: combined.s2002, w2002, s2001, c2002?
    - local.s2001 (line 44, in WHERE); did you mean: combined.s2001, w2001, s2002, c2001?
    - local.w2002 (line 44, in WHERE); did you mean: combined.w2002, w2001, s2002, c2002?
    - local.w2001 (line 44, in WHERE); did you mean: combined.w2001, w2002, s2001, c2001?
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.all_sales:all_sales --format json 
# Test: entity with only returns
with sales_agg as
where sales.date.date between '2000-08-23'::da…eturns_agg.channel as r_channel,
union join sales_agg.channel = returns_agg.channel
union join sales_agg.entity_id = returns_agg.entity_id
limit 20;
`

  ```text
  Syntax error in stdin: Undefined concept: sales.date.date. Suggestions: ['all_sales.date.date', 'all_sales.return_date.date', 'all_sales.ship_customer.first_sales_date.date', 'all_sales.ship_customer.first_shipto_date.date', 'all_sales.billing_customer.first_sales_date.date', 'all_sales.billing_customer.first_shipto_date.date']
  ```
- `trilogy run answer_883027685.preql`

  ```text
  Syntax error in answer_883027685.preql: 5 undefined concept references; fix all before re-running:
    - date.year (line 7, col 5, in WHERE); did you mean: ss.date.year, ss.store.date.year, ss.return_store.date.year, date.month_of_year, ss.return_date.year, ss.customer.first_sales_date.year?
    - date.month_of_year (line 8, col 9, in WHERE); did you mean: ss.date.month_of_year, ss.store.date.month_of_year, ss.return_store.date.month_of_year, date.year, ss.return_date.month_of_year, ss.customer.first_sales_date.month_of_year?
    - item.category (line 9, col 9, in WHERE); did you mean: ss.item.category, ss.item.category_id, ss.item.color?
    - customer.address.sk (line 10, col 9, in WHERE); did you mean: ss.customer.address.sk, customer.address.state, ss.return_customer.address.sk, ss.customer.sk, ss.sale_address.sk, ss.return_address.sk?
    - item.current_price (line 11, col 9, in WHERE); did you mean: ss.item.current_price?
  ```
- `trilogy file read raw/customer.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_1297355556.preql`

  ```text
  Syntax error in answer_1297355556.preql: 6 undefined concept references; fix all before re-running:
    - ss.customer_demographics.marital_status (line 9, col 10, in WHERE); did you mean: ss.customer_demographic.marital_status, ss.customer.demographics.marital_status, ss.return_customer_demographic.marital_status, ss.return_customer.demographics.marital_status?
    - ss.customer_demographics.education_status (line 9, col 60, in WHERE); did you mean: ss.customer_demographic.education_status, ss.customer.demographics.education_status, ss.return_customer_demographic.education_status, ss.return_customer.demographics.education_status?
    - ss.customer_demographics.marital_status (line 10, col 13, in WHERE); did you mean: ss.customer_demographic.marital_status, ss.customer.demographics.marital_status, ss.return_customer_demographic.marital_status, ss.return_customer.demographics.marital_status?
    - ss.customer_demographics.education_status (line 10, col 63, in WHERE); did you mean: ss.customer_demographic.education_status, ss.customer.demographics.education_status, ss.return_customer_demographic.education_status, ss.return_customer.demographics.education_status?
    - ss.customer_demographics.marital_status (line 11, col 13, in WHERE); did you mean: ss.customer_demographic.marital_status, ss.customer.demographics.marital_status, ss.return_customer_demographic.marital_status, ss.return_customer.demographics.marital_status?
    - ss.customer_demographics.education_status (line 11, col 63, in WHERE); did you mean: ss.customer_demographic.education_status, ss.customer.demographics.education_status, ss.return_customer_demographic.education_status, ss.return_customer.demographics.education_status?
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.all_sales:all_sales select avg(s.quantity * s.list_price) as overall_avg where s.date.year between 1999 and 2001 limit 5;`

  ```text
  Syntax error in stdin: 3 undefined concept references; fix all before re-running:
    - s.quantity (line 2, in SELECT); did you mean: all_sales.quantity?
    - s.list_price (line 2, in SELECT); did you mean: all_sales.list_price, all_sales.ext_list_price?
    - s.date.year (line 2, col 60, in WHERE); did you mean: all_sales.date.year, all_sales.return_date.year, all_sales.ship_customer.first_sales_date.year, all_sales.ship_customer.first_shipto_date.year, all_sales.billing_customer.first_sales_date.year, all_sales.billing_customer.first_shipto_date.year?
  ```
- `trilogy run answer_219203908.preql`

  ```text
  Syntax error in answer_219203908.preql: `by rollup (…)` cannot re-aggregate the bare measure reference `channel_aggregates.channel_sales`. Alias it with an explicit aggregate (e.g. `sum(channel_aggregates.channel_sales) as channel_sales_total`).
  ```
- `trilogy run --import raw.all_sales:all_sales select s.sale_line_item_counter, count(s.sale_line_item_counter) as cnt where s.date.year = 2001 limit 10;`

  ```text
  Syntax error in stdin: 2 undefined concept references; fix all before re-running:
    - s.sale_line_item_counter (line 2, col 8, in SELECT); did you mean: all_sales.sale_line_item_counter?
    - s.date.year (line 2, col 79, in WHERE); did you mean: all_sales.date.year, all_sales.return_date.year, all_sales.ship_customer.first_sales_date.year, all_sales.ship_customer.first_shipto_date.year, all_sales.billing_customer.first_sales_date.year, all_sales.billing_customer.first_shipto_date.year?
  ```
- `trilogy file read answer_219203908.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_765177085.preql`

  ```text
  Resolution error in answer_765177085.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 30). The requested concepts split into 2 disconnected subgraphs: {cs_filtered.cs_cust_sk, cs_filtered.cs_item_sk, cs_filtered.cs_qty}; {ss_filtered.ss_cust_sk, ss_filtered.ss_item_desc, ss_filtered.ss_item_id, ss_filtered.ss_item_sk, ss_filtered.ss_store_state}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_1835050598.preql`

  ```text
  Syntax error in answer_1835050598.preql: ORDER BY contains aggregate `grouping(local.country)` (line 3), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(local.country) as g order by g desc`.
  ```
- `trilogy file read answer_1835050598.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_2604809012.preql`

  ```text
  Resolution error in answer_2604809012.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 12). The requested concepts split into 3 disconnected subgraphs: {item.current_price, item_code}; {after_qty, after_total, before_qty, before_total}; {warehouse_name}.
    - `item.current_price` is disconnected — did you mean `inv.item.current_price`? (connected to the other concepts)
  These look like separately-imported copies of models already reachable through a connected import; chain through that path (e.g. `inv.item.current_price`) instead of importing a second, disconnected copy.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 86 column 12 (char 2868). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run answer_2928586490.preql`

  ```text
  Syntax error in answer_2928586490.preql: Undefined concept: ss.sold_date.year. Suggestions: ['ss.date.year', 'ss.store.date.year', 'ss.return_date.year', 'cs.sold_date.year', 'cs.ship_date.year', 'ws.ship_date.year']
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_2928586490.preql`

  ```text
  Resolution error in answer_2928586490.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 42). The requested concepts split into 2 disconnected subgraphs: {cust_alltime.sk, cust_alltime.tot}; {cust_period.tot}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read answer_1798498862.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw\store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/inventory.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_142117636.preql`

  ```text
  Syntax error in answer_142117636.preql: Ambiguous reference 'jan_data.warehouse_sk': matches ['jan_data.both_months.warehouse_sk', 'jan_data.qualified.warehouse_sk']. Qualify the full path to disambiguate.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run answer_374591292.preql`

  ```text
  Resolution error in answer_374591292.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {best10.all_ranked.product_name, best10.rank, worst10.all_ranked.product_name, worst10.rank}
  ```
- `trilogy file read raw/item.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read answer_2118989494.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read answer_2940558602.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3553309440.preql`

  ```text
  Resolution error in answer_3553309440.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 39). The requested concepts split into 2 disconnected subgraphs: {_cust_totals_cust_sk, _cust_totals_total_spend, store.customer.address.county, store.customer.address.state, store.customer.sk, store.date.month_seq, store.store.county, store.store.state}; {dec1998_month_seq}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_1623435181.preql`

  ```text
  Resolution error in answer_1623435181.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {next_year.weekly.fri_total, next_year.weekly.mon_total, next_year.weekly.sat_total, next_year.weekly.store_code, next_year.weekly.sun_total, next_year.weekly.thu_total, next_year.weekly.tue_total, next_year.weekly.wed_total, next_year.weekly.wk_seq, this_year.weekly.fri_total, this_year.weekly.mon_total, this_year.weekly.sat_total, this_year.weekly.store_code, this_year.weekly.store_name, this_year.weekly.sun_total, this_year.weekly.thu_total, this_year.weekly.tue_total, this_year.weekly.wed_total, this_year.weekly.wk_seq}
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 29 column 3 (char 968). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run answer_3544057080.preql`

  ```text
  Syntax error in answer_3544057080.preql: Ambiguous reference 'store_agg.zip': matches ['store_agg.ss.sale_address.zip', 'store_agg.ss.store.zip']. Qualify the full path to disambiguate.
  ```
- `trilogy run answer_1256642802.preql`

  ```text
  Syntax error in answer_1256642802.preql: ORDER BY references 'per_item_revenue.item_sk', which is not in the SELECT projection (line 20). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --per_item_revenue.item_sk order by per_item_revenue.item_sk asc`.
  ```
- `trilogy file read answer_1256642802.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3979964698.preql`

  ```text
  Syntax error in answer_3979964698.preql: Undefined concept: WEB.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

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
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_2374450308.preql`

  ```text
  Resolution error in answer_2374450308.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 7). The requested concepts split into 2 disconnected subgraphs: {inv.date.date, inv.quantity_on_hand}; {item.current_price, item.desc, item.id, item.manufacturer_id, item.sk}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read answer_1827331030.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3281742578.preql`

  ```text
  Resolution error in answer_3281742578.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 33). The requested concepts split into 2 disconnected subgraphs: {qualifying_orders.order_line_info.ws.order_number}; {ws.order_number}.
    - `ws.order_number` is disconnected — did you mean `order_line_info.ws.order_number`? (connected to the other concepts)
  These look like separately-imported copies of models already reachable through a connected import; chain through that path (e.g. `order_line_info.ws.order_number`) instead of importing a second, disconnected copy.
  ```
- `trilogy run answer_3281742578.preql`

  ```text
  Syntax error in answer_3281742578.preql: 2 undefined concept references; fix all before re-running:
    - order_line_info.ws.ext_ship_cost (line 29, in SELECT); did you mean: order_line_info.ws.net_profit, order_line_info.return_count, order_line_info.warehouse_count, ws.ext_ship_cost?
    - order_line_info.ws.net_profit (line 30, in SELECT); did you mean: order_line_info.ws.ext_ship_cost, order_line_info.return_count, order_line_info.ws.order_number, ws.net_profit?
  ```
- `trilogy run --import raw/web_sales:ws select ws.order_number, count_distinct(ws.warehouse.sk) as wh_count, sum(case when ws.is_returned then 1 else 0 end) as…2.ship_date.date between '1999-02-01'::date and '1999-04-02'::date and ws2.ship_address.state = 'IL' and ws2.web_site.company_name = 'pri') limit 10;`

  ```text
  Syntax error in stdin: 4 undefined concept references; fix all before re-running:
    - ws2.order_number (line 2, col 162, in SELECT); did you mean: ws.order_number?
    - ws2.ship_date.date (line 2, col 185, in WHERE); did you mean: ws2.ship_address.state, ws.ship_date.date, ws.date.date, ws.return_date.date, ws.ship_customer.first_sales_date.date, ws.ship_customer.first_shipto_date.date?
    - ws2.ship_address.state (line 2, col 258, in WHERE); did you mean: ws2.ship_date.date, ws.ship_address.state, ws.bill_address.state, ws.ship_customer.address.state, ws.return_address.state, ws.return_refund_address.state?
    - ws2.web_site.company_name (line 2, col 292, in WHERE); did you mean: ws.web_site.company_name, ws.web_site.name, ws.ship_date.day_name?
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_569612608.preql`

  ```text
  Syntax error in answer_569612608.preql: 6 undefined concept references; fix all before re-running:
    - local.order_number (line 11, in SELECT); did you mean: ws.order_number, order_count?
    - local.ext_ship_cost (line 12, in SELECT); did you mean: ws.ext_ship_cost, total_ext_ship_cost, ws.return_ship_cost?
    - local.net_profit (line 13, in SELECT); did you mean: ws.net_profit, total_net_profit, ws.profit?
    - ship_date.date (line 7, col 3, in WHERE); did you mean: ws.ship_date.date, ws.date.date, ws.return_date.date, ws.ship_customer.first_sales_date.date, ws.ship_customer.first_shipto_date.date, ws.return_customer.first_shipto_date.date?
    - ship_address.state (line 8, col 7, in WHERE); did you mean: ws.ship_address.state, ws.bill_address.state, ws.ship_customer.address.state, ws.return_address.state, ws.billing_customer.address.state, ws.return_refund_address.state?
    - web_site.company_name (line 9, col 7, in WHERE); did you mean: ws.web_site.company_name, ws.web_site.name, ws.item.brand_name?
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy file write answer_1858999935.preql --content import raw.catalog_sales as cs;
import raw.web_sales as ws;
import raw.date as d;

# Aggregate sales by …ales ? with_next.day_of_week = 6) / max(with_next.next_year_sales ? with_next.day_of_week = 6), 2) as sat_ratio
order by
    with_next.week_seq asc
;`

  ```text
  refused to write 'answer_1858999935.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...001 with year_weeks as select ??? distinct d.week_seq where d.ye...

  Write stats: received 2597 chars / 2597 bytes; tail: …' = 6), 2) as sat_ratio\\norder by\\n    with_next.week_seq asc\\n;'.
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.channel, all_sales.return_date.date, sum(all_sales.return_amount) as ret_amt, sum(all_sales.ret…-08-23'::date and '2000-09-06'::date and all_sales.return_channel_dim_id is not null group by all_sales.channel, all_sales.return_date.date limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...rn_channel_dim_id is not null ??? group by all_sales.channel, al...
  ```
- `trilogy file write answer_3263796749.preql --content import raw.store_sales as ss;

auto bucket1_count <- count(ss.line_item ? ss.quantity between 1 and 20);…t_avg else bucket4_netpaid_avg end as col4,
    case when bucket5_count > 165306 then bucket5_discount_avg else bucket5_netpaid_avg end as col5
by *;`

  ```text
  refused to write 'answer_3263796749.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [213]: A `by <grain>` clause must follow an aggregate, but the expression before it has none. If the `by` sits inside an aggregate's parentheses (`max(x by *)`), move it outside the call: `max(x) by *`. To take each distinct value once per grain, wrap it in `group(...)` - e.g. `group(item.current_price) by item.id, item.category`. For a reduction, use an aggregate: `sum(x) by ...`, `avg(x) by ...`, `max(x) by ...`.
  Location:
  ...cket5_netpaid_avg end as col5 ??? by *;

  Write stats: received 1762 chars / 1762 bytes; tail: …'ket5_discount_avg else bucket5_netpaid_avg end as col5\\nby *;'.
  ```
- `trilogy file write answer_3263796749.preql --content import raw.store_sales as ss;

auto bucket1_count <- count(ss.line_item ? ss.quantity between 1 and 20);…t_avg else bucket4_netpaid_avg end as col4,
    case when bucket5_count > 165306 then bucket5_discount_avg else bucket5_netpaid_avg end as col5
by *;`

  ```text
  refused to write 'answer_3263796749.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [213]: A `by <grain>` clause must follow an aggregate, but the expression before it has none. If the `by` sits inside an aggregate's parentheses (`max(x by *)`), move it outside the call: `max(x) by *`. To take each distinct value once per grain, wrap it in `group(...)` - e.g. `group(item.current_price) by item.id, item.category`. For a reduction, use an aggregate: `sum(x) by ...`, `avg(x) by ...`, `max(x) by ...`.
  Location:
  ...cket5_netpaid_avg end as col5 ??? by *;

  Write stats: received 1762 chars / 1762 bytes; tail: …'ket5_discount_avg else bucket5_netpaid_avg end as col5\\nby *;'.
  ```
- `trilogy file write answer_3263796749.preql --content import raw.store_sales as ss;

auto bucket1_count <- count(ss.line_item ? ss.quantity between 1 and 20);…else bucket4_netpaid_avg end) as col4,
    max(case when bucket5_count > 165306 then bucket5_discount_avg else bucket5_netpaid_avg end) as col5
by *;`

  ```text
  refused to write 'answer_3263796749.preql': not syntactically valid Trilogy.

  Parse error:
    --> 27:1
     |
  27 | by *;
     | ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...ket5_netpaid_avg end) as col5 ??? by *;

  Write stats: received 1787 chars / 1787 bytes; tail: …'et5_discount_avg else bucket5_netpaid_avg end) as col5\\nby *;'.
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.channel, all_sales.item.brand_id, all_sales.item.class_id, all_sales.item.category_id, sum(all_…d all_sales.date.month_of_year = 11 group by all_sales.channel, all_sales.item.brand_id, all_sales.item.class_id, all_sales.item.category_id limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...sales.date.month_of_year = 11 ??? group by all_sales.channel, al...
  ```
- `trilogy file write answer_219203908.preql --content import raw.all_sales as s;

# Step 1: Find (brand_id, class_id, category_id) combos that appear in ALL TH…s.item.category_id)
order by channel nulls first, s.item.brand_id nulls first, s.item.class_id nulls first, s.item.category_id nulls first
limit 100;`

  ```text
  refused to write 'answer_219203908.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...es > (select overall_avg_sale ??? from overall_stats) by rollup

  Hint: the `by rollup/cube/grouping sets` clause must come *before* HAVING in Trilogy (same order as SQL's `GROUP BY ... HAVING`). Reorder to:
      select <cols> by rollup (<keys>) having <cond> order by <cols> limit <n>;

  Write stats: received 1603 chars / 1603 bytes; tail: …'ss_id nulls first, s.item.category_id nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.all_sales:all_sales select s.sale_line_item_counter, count(*) as cnt where s.date.year = 2001 group by s.sale_line_item_counter limit 10;`

  ```text
  Syntax error in stdin: Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...ect s.sale_line_item_counter, ??? count(*) as cnt where s.date.y...
  ```
- `trilogy run --import raw.all_sales:all_sales select s.sale_line_item_counter, count(s.sale_line_item_counter) as cnt where s.date.year = 2001 group by s.sale_line_item_counter limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
   cnt where s.date.year = 2001 ??? group by s.sale_line_item_coun...
  ```
- `trilogy run --import raw.all_sales:all_sales select count(*) as combo_count where all_sales.date.year between 1999 and 2001 limit 5;`

  ```text
  Syntax error in stdin: Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...ll_sales as all_sales; select ??? count(*) as combo_count where
  ```
- `trilogy file write answer_2524943990.preql --content import raw/catalog_sales as cs;

rowset order_info <-
where
  cs.ship_date.date between '2002-02-01'::da…(order_info.total_ship_cost) as total_extended_ship_cost,
  sum(order_info.total_net_profit) as total_net_profit
order by order_count desc
limit 100;`

  ```text
  refused to write 'answer_2524943990.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/catalog_sales as cs;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/catalog_sales as cs;  rows...

  Write stats: received 709 chars / 709 bytes; tail: …'it) as total_net_profit\\norder by order_count desc\\nlimit 100;'.
  ```
- `trilogy file write answer_751385098.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

with combined …omer.last_name,
     ws.billing_customer.first_name,
     ws.date.date
  )
) -> (last_name, first_name, sale_date)

select count(1) as cnt
limit 100;`

  ```text
  refused to write 'answer_751385098.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...t_name, first_name, sale_date) ???  select count(1) as cnt limit...

  Write stats: received 712 chars / 712 bytes; tail: …'e, first_name, sale_date)\\n\\nselect count(1) as cnt\\nlimit 100;'.
  ```
- `trilogy run --import raw.item:item select distinct item.category, item.color, item.units, item.size limit 50;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...port raw.item as item; select ??? distinct item.category, item.c...
  ```
- `trilogy run --import raw.item:item select item.manufact, count(item.id) as cnt where item.manufact is not null group by item.manufact order by cnt desc limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...ere item.manufact is not null ??? group by item.manufact order b...
  ```
- `trilogy file write answer_374591292.preql --content import raw.store_sales as ss;

# Filter to store.sk = 1
where ss.store.sk = 1

# Compute threshold: 90% o…r_product_name
order by
    best_performers.rank asc,
    best_performers.bp_product_name desc,
    worst_performers.wp_product_name desc
limit 100
;`

  ```text
  refused to write 'answer_374591292.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block - they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...need this as a constant value ??? auto threshold_avg <- 0.9 * av...

  Write stats: received 1925 chars / 1925 bytes; tail: …' desc,\\n    worst_performers.wp_product_name desc\\nlimit 100\\n;'.
  ```
- `trilogy file write answer_374591292.preql --content import raw.store_sales as ss;

# Compute threshold: 90% of average net profit where sale_address.sk is nu…der by item_avg_profit asc) as best_rank,
    rank(ss.item.sk) over (order by item_avg_profit desc) as worst_rank
limit 1000  # keep all candidates
;`

  ```text
  refused to write 'answer_374591292.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:82
    |
  4 | auto threshold_avg <- 0.9 * avg(ss.net_profit ? ss.sale_address.sk is null) by * where ss.store.sk = 1;
    |                                                                                  ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or window_sql_over
  Location:
  ...sale_address.sk is null) by * ??? where ss.store.sk = 1;  # Comp...

  Write stats: received 717 chars / 717 bytes; tail: …'ofit desc) as worst_rank\\nlimit 1000  # keep all candidates\\n;'.
  ```
- `trilogy run --import raw.web_sales:web --import raw.store_sales:store select web.item.sk, web.date.date, sum(web.sales_price) as daily_web_unit_price where web.date.year = 2000 group by web.item.sk, web.date.date order by web.item.sk, web.date.date limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...ce where web.date.year = 2000 ??? group by web.item.sk, web.date...
  ```
- `trilogy file write answer_4037365554.preql -e -c import raw.store_sales as ss;

# Filter to year 2000 and item profiles
where
  ss.date.year = 2000
  and (
 …nth.monthly_total,
  avg_per_mgr.avg_monthly
order by
  per_month.mgr_id asc,
  avg_per_mgr.avg_monthly asc,
  per_month.monthly_total asc
limit 100
`

  ```text
  refused to write 'answer_4037365554.preql': not syntactically valid Trilogy.

  Parse error:
    --> 21:1
     |
  21 | with per_month as
     | ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
  ...of_year) total of sales_price ??? with per_month as select   ss....

  Write stats: received 1372 chars / 1372 bytes; tail: …'gr.avg_monthly asc,\\n  per_month.monthly_total asc\\nlimit 100\\n'.
  ```
- `trilogy file write answer_4037365554.preql -e -c import raw.store_sales as ss;

# Rowset 1: per (manager_id, month_of_year) total sales_price, year 2000, fil… avg_per_mgr.avg_monthly > 0.1
order by
  per_month_totals.mgr_id asc,
  avg_per_mgr.avg_monthly asc,
  per_month_totals.monthly_total asc
limit 100
`

  ```text
  refused to write 'answer_4037365554.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...l,   avg_per_mgr.avg_monthly, ??? union join per_month_totals.mg...

  Write stats: received 1497 chars / 1497 bytes; tail: …'monthly asc,\\n  per_month_totals.monthly_total asc\\nlimit 100\\n'.
  ```
- `trilogy file write answer_4037365554.preql -e -c import raw.store_sales as ss;

# Rowset 1: per (manager_id, month_of_year) total sales_price, year 2000, fil… avg_per_mgr.avg_monthly > 0.1
order by
  per_month_totals.mgr_id asc,
  avg_per_mgr.avg_monthly asc,
  per_month_totals.monthly_total asc
limit 100
`

  ```text
  refused to write 'answer_4037365554.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...l,   avg_per_mgr.avg_monthly, ??? subset join per_month_totals.m...

  Write stats: received 1503 chars / 1503 bytes; tail: …'monthly asc,\\n  per_month_totals.monthly_total asc\\nlimit 100\\n'.
  ```
- `trilogy file write answer_4037365554.preql -e -c import raw.store_sales as ss;

# Per (manager_id, month_of_year): total sales_price
# Pin grain explicitly

…avg.avg_monthly) / row_avg.avg_monthly > 0.1
order by
  row_monthly.mgr_id asc,
  row_avg.avg_monthly asc,
  row_monthly.monthly_total asc
limit 100
`

  ```text
  refused to write 'answer_4037365554.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block - they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...per (manager_id, month) total ??? rowset row_monthly as select

  Write stats: received 1417 chars / 1417 bytes; tail: …'.avg_monthly asc,\\n  row_monthly.monthly_total asc\\nlimit 100\\n'.
  ```
- `trilogy file write answer_4037365554.preql -e -c import raw.store_sales as ss;

# Rowset 1: per (manager_id, month_of_year) total sales_price, year 2000, fil…avg.avg_monthly) / row_avg.avg_monthly > 0.1
order by
  row_monthly.mgr_id asc,
  row_avg.avg_monthly asc,
  row_monthly.monthly_total asc
limit 100
`

  ```text
  refused to write 'answer_4037365554.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...total,   row_avg.avg_monthly, ??? union join row_monthly.mgr_id

  Write stats: received 1415 chars / 1415 bytes; tail: …'.avg_monthly asc,\\n  row_monthly.monthly_total asc\\nlimit 100\\n'.
  ```
- `trilogy file write answer_4037365554.preql -e -c import raw.store_sales as ss;

where
  ss.date.year = 2000
  and (
    (
      ss.item.category in ('Books',…mporto #1', 'importoamalg #1')
    )
  )
  and ss.store.sk is not null
select
  ss.item.manager_id as mgr_id,
  ss.date.month_of_year as mo
limit 10
`

  ```text
  refused to write 'answer_4037365554.preql': not syntactically valid Trilogy.

  Parse error:
    --> 21:1
     |
  21 | limit 10
     | ^---
     |
     = expected metadata, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
    ss.date.month_of_year as mo ??? limit 10

  Write stats: received 721 chars / 721 bytes; tail: …'anager_id as mgr_id,\\n  ss.date.month_of_year as mo\\nlimit 10\\n'.
  ```
- `trilogy file write answer_4037365554.preql -e -c import raw.store_sales as ss;

where
  ss.date.year = 2000
  and (
    (
      ss.item.category in ('Books',…amalg #1')
    )
  )
  and ss.store.sk is not null
select
  ss.item.manager_id as mgr_id,
  ss.date.month_of_year as mo
order by mgr_id asc
limit 10
`

  ```text
  refused to write 'answer_4037365554.preql': not syntactically valid Trilogy.

  Parse error:
    --> 21:1
     |
  21 | order by mgr_id asc
     | ^---
     |
     = expected metadata, where, having, select_grouping, or JOIN_TYPE
  Location:
    ss.date.month_of_year as mo ??? order by mgr_id asc limit 10

  Write stats: received 741 chars / 741 bytes; tail: …'\\n  ss.date.month_of_year as mo\\norder by mgr_id asc\\nlimit 10\\n'.
  ```
- `trilogy file write answer_4037365554.preql -e -c import raw.store_sales as ss;

where
  ss.date.year = 2000
  and (
    (
      ss.item.category in ('Books',…   )
  )
  and ss.store.sk is not null
select
  ss.item.manager_id as mgr_id,
  ss.date.month_of_year as mo
order by ss.item.manager_id asc
limit 10
`

  ```text
  refused to write 'answer_4037365554.preql': not syntactically valid Trilogy.

  Parse error:
    --> 21:1
     |
  21 | order by ss.item.manager_id asc
     | ^---
     |
     = expected metadata, where, having, select_grouping, or JOIN_TYPE
  Location:
    ss.date.month_of_year as mo ??? order by ss.item.manager_id as...

  Write stats: received 753 chars / 753 bytes; tail: …'onth_of_year as mo\\norder by ss.item.manager_id asc\\nlimit 10\\n'.
  ```
- `trilogy file write answer_2091535883.preql --content import raw.catalog_sales as cs;
import raw.inventory as inv;

# Catalog sales in 1999 with filters
where…al_orders
order by
    total_orders desc nulls first,
    item_desc nulls first,
    warehouse_name nulls first,
    week_seq nulls first
limit 100;
`

  ```text
  refused to write 'answer_2091535883.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...join cs.item.sk = inv.item.sk ??? union join cs.sold_date.week_s...

  Write stats: received 1026 chars / 1026 bytes; tail: …'house_name nulls first,\\n    week_seq nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_3046445280.preql --content import raw.all_sales as all;

# Auto concepts for net values
# If quantity is null, net_qty is null; oth…r_id
having
    # Keep only where 2002_qty / 2001_qty < 0.9
    yr2002.qty_2002 / yr2001.qty_2001::float < 0.9
order by qty_diff, amt_diff
limit 100;`

  ```text
  refused to write 'answer_3046445280.preql': not syntactically valid Trilogy.

  Parse error:
    --> 13:1
     |
  13 | with yearly as
     | ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, or JOIN_TYPE
  Location:
   2002)  # Per-year aggregates ??? with yearly as select     all....

  Write stats: received 2141 chars / 2141 bytes; tail: …'qty_2001::float < 0.9\\norder by qty_diff, amt_diff\\nlimit 100;'.
  ```
- `trilogy file write answer_747155668.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sales as cs;

# Store sales:…ls first,
  combined.missing_ref nulls first,
  combined.year nulls first,
  combined.quarter nulls first,
  combined.category nulls first
limit 100;`

  ```text
  refused to write 'answer_747155668.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block - they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...l  with combined as union(   ( ??? import raw.store_sales as ss

  Write stats: received 1856 chars / 1856 bytes; tail: …'rter nulls first,\\n  combined.category nulls first\\nlimit 100;'.
  ```
- `trilogy file write answer_747155668.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sales as cs;

with combined …ls first,
  combined.missing_ref nulls first,
  combined.year nulls first,
  combined.quarter nulls first,
  combined.category nulls first
limit 100;`

  ```text
  refused to write 'answer_747155668.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...g, cnt int, esp numeric(15,2)) ???  select   combined.channel,

  Write stats: received 1482 chars / 1482 bytes; tail: …'rter nulls first,\\n  combined.category nulls first\\nlimit 100;'.
  ```
- `trilogy file write answer_3210116865.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Build three….channel_label, combined.outlet_sk)
order by combined.channel_label asc nulls first, combined.outlet_sk asc nulls first, return_total desc
limit 100;`

  ```text
  refused to write 'answer_3210116865.preql': not syntactically valid Trilogy.

  Parse error:
    --> 11:5
     |
  11 |     with store_sales_agg as
     |     ^---
     |
     = expected select_statement
  Location:
  ...gated by selling-store SK     ??? with store_sales_agg as     wh...

  Write stats: received 4089 chars / 4089 bytes; tail: …'ined.outlet_sk asc nulls first, return_total desc\\nlimit 100;'.
  ```
- `trilogy file write answer_3210116865.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# ---- Store ….channel_label, combined.outlet_sk)
order by combined.channel_label asc nulls first, combined.outlet_sk asc nulls first, return_total desc
limit 100;`

  ```text
  refused to write 'answer_3210116865.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...l, store_channel.return_total ??? from store_channel),   (select...

  Write stats: received 4731 chars / 4731 bytes; tail: …'ined.outlet_sk asc nulls first, return_total desc\\nlimit 100;'.
  ```
- `trilogy file write answer_943796012.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

with combined …ombined.profit) as profit
by rollup (combined.channel, combined.outlet)
order by combined.channel nulls first, combined.outlet nulls first
limit 100;`

  ```text
  refused to write 'answer_943796012.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...urns numeric, profit numeric?) ???  select   combined.channel,

  Write stats: received 1980 chars / 1980 bytes; tail: …'.channel nulls first, combined.outlet nulls first\\nlimit 100;'.
  ```
- `trilogy file write answer_210925266.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

with store_com…atalog_combos), (web_combos)) -> (last_name string?, first_name string?, sale_date date);

select count(only_store.last_name) as unique_combo_count;
`

  ```text
  refused to write 'answer_210925266.preql': not syntactically valid Trilogy.

  Parse error:
    --> 33:9
     |
  33 | except((store_combos), (catalog_combos), (web_combos)) -> (last_name string?, first_name string?, sale_date date);
     |         ^---
     |
     = expected select_statement
  Location:
  ...;  with only_store as except(( ??? store_combos), (catalog_combos...

  Write stats: received 828 chars / 828 bytes; tail: …'\\n\\nselect count(only_store.last_name) as unique_combo_count;\\n'.
  ```
- `trilogy run --import raw/web_sales:ws select ws.order_number, count_distinct(ws.warehouse.sk) as wh_count where ws.order_number in (51246) select ws.order_number, wh_count;`

  ```text
  Syntax error in stdin:  --> 2:102
    |
  2 | select ws.order_number, count_distinct(ws.warehouse.sk) as wh_count where ws.order_number in (51246) select ws.order_number, wh_count;
    |                                                                                                      ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...re ws.order_number in (51246) ??? select ws.order_number, wh_cou...
  ```
- `trilogy file write answer_3562094594.preql -e -c import raw.store_sales as ss;
import raw.catalog_sales as cs;

# Distinct (customer, item.sk) from store sal…nly_store.cust_sk) as only_store_count,
    count(only_catalog.cust_sk) as only_catalog_count,
    count(both_sets.cust_sk) as both_count
limit 100;
`

  ```text
  refused to write 'answer_3562094594.preql': not syntactically valid Trilogy.

  Parse error:
    --> 22:9
     |
  22 | except((store_combos), (catalog_combos)) -> (cust_sk, item_sk)
     |         ^---
     |
     = expected select_statement
  Location:
  ...re with only_store as except(( ??? store_combos), (catalog_combos...

  Write stats: received 908 chars / 908 bytes; tail: …'ount,\\n    count(both_sets.cust_sk) as both_count\\nlimit 100;\\n'.
  ```

### `cli-misuse`

- `trilogy trilogy agent-info syntax example query-structure`

  ```text
  No such command 'trilogy'.
  ```
- `trilogy explore raw/catalog_returns.preql`

  ```text
  Invalid value for 'PATH': File 'raw/catalog_returns.preql' does not exist.
  ```

### `type-error`

- `trilogy run answer_4080205480.preql duckdb`

  ```text
  Type error in answer_4080205480.preql: Invalid argument type 'INTEGER' passed into DATE_PART function in position 1 from concept: ss.date.sk. Valid: 'DATE', 'DATETIME', 'TIMESTAMP'.
  ```
- `trilogy run answer_3770074305.preql`

  ```text
  Syntax error in answer_3770074305.preql: Cannot use BETWEEN with incompatible types DATE and STRING (low)
  ```

### `syntax-missing-alias`

- `trilogy file write answer_4037365554.preql -e -c import raw.store_sales as ss;

# Auto concept: total sales_price per (manager_id, month_of_year)
# This is g…orto #1', 'importoamalg #1')
    )
  )
  and ss.store.sk is not null
select
  ss.item.manager_id,
  ss.date.month_of_year,
  monthly_total
limit 100
`

  ```text
  refused to write 'answer_4037365554.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `monthly_total as monthly_total`
  Location:
  ...onth_of_year,   monthly_total ??? limit 100

  Write stats: received 1141 chars / 1141 bytes; tail: …'ager_id,\\n  ss.date.month_of_year,\\n  monthly_total\\nlimit 100\\n'.
  ```
- `trilogy file write answer_4037365554.preql -e -c import raw.store_sales as ss;

where
  ss.date.year = 2000
  and (
    (
      ss.item.category in ('Books',…r #1', 'exportiimporto #1', 'importoamalg #1')
    )
  )
  and ss.store.sk is not null
select
  ss.item.manager_id,
  ss.date.month_of_year
limit 10
`

  ```text
  refused to write 'answer_4037365554.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `ss.date.month_of_year as ss_date_month_of_year`
  Location:
  ...r_id,   ss.date.month_of_year ??? limit 10

  Write stats: received 705 chars / 705 bytes; tail: …'lect\\n  ss.item.manager_id,\\n  ss.date.month_of_year\\nlimit 10\\n'.
  ```

### `planner-recursion`

- `trilogy run answer_630074061.preql`

  ```text
  Resolution error in answer_630074061.preql: query could not be planned; this is a bug.
  ```
