# Trilogy failure analysis — 20260713-173600

- Run `20260713-173600_ingest` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 1829 | failed: 220 (12%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 167 | 76% |
| `syntax-parse` | 48 | 22% |
| `cli-misuse` | 3 | 1% |
| `undefined-concept` | 1 | 0% |
| `type-error` | 1 | 0% |

## Detail

### `other`

- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe store_returns`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read answer_1858999935.preql`

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
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3705756794.preql`

  ```text
  Syntax error in answer_3705756794.preql: Undefined concept: local.sales_agg. Suggestions: ['sales_agg.entity_id', 'sales_agg.net_profit', 'sales_agg.total_sales']
  ```
- `trilogy run answer_883027685.preql`

  ```text
  Resolution error in answer_883027685.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 6). The requested concepts split into 2 disconnected subgraphs: {cat_avg_price}; {line_item_count, state, store_sales.date_dim.moy, store_sales.date_dim.year, store_sales.item.category, store_sales.item.current_price}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read answer_883027685.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run answer_689898357.preql`

  ```text
  Syntax error in answer_689898357.preql: Comparison `store_sales.promotion.channel_email = 'N'` matches every value of nullable enum field 'store_sales.promotion.channel_email', which contains only these values: 'N'. It only excludes nulls; simplify it to 'store_sales.promotion.channel_email is not null'.
  ```
- `trilogy run answer_3697706765.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Syntax error in answer_3697706765.preql: 3 undefined concept references; fix all before re-running:
    - local.z (line 23, in SELECT)
    - local.z (line 24, col 11, in WHERE)
    - local.z (line 25, col 11, in WHERE)
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/customer.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_4199102535.preql`

  ```text
  Syntax error in answer_4199102535.preql: Undefined concept: ss.sold_date.year. Suggestions: ['ss.date_dim.year', 'ss.store.date_dim.year', 'ss.promotion.end_date.year', 'ws.sold_date.year', 'cs.sold_date.year', 'ws.ship_date.year']
  ```
- `trilogy run answer_3697440276.preql`

  ```text
  Syntax error in answer_3697440276.preql: 3 undefined concept references; fix all before re-running:
    - first_name (line 69, col 5, in ORDER BY); did you mean: store.customer.first_name, web.bill_customer.first_name, web.ship_customer.first_name, web.web_page.customer.first_name?
    - last_name (line 70, col 5, in ORDER BY); did you mean: store.customer.last_name, web.bill_customer.last_name, web.ship_customer.last_name, web.web_page.customer.last_name?
    - preferred_cust_flag (line 71, col 5, in ORDER BY); did you mean: store.customer.preferred_cust_flag, web.bill_customer.preferred_cust_flag, web.ship_customer.preferred_cust_flag, web.web_page.customer.preferred_cust_flag?
  ```
- `trilogy run answer_3697440276.preql`

  ```text
  Resolution error in answer_3697440276.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 51). The requested concepts split into 2 disconnected subgraphs: {combined.c_sk, combined.s01, combined.s02, combined.w01, combined.w02}; {_result_customer_code, store.customer.customer_sk, store.customer.first_name, store.customer.last_name, store.customer.preferred_cust_flag, store.date_dim.year}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.store_sales:ss select count(ss.item.item_sk) where (ss.item.brand_id, ss.item.class_id) in ((1001001, 1), (1002001, 2));`

  ```text
  Syntax error in stdin: Cannot compare composite-membership elements ref:ss.item.brand_id (BIGINT) and (1001001, 1) (ArrayType<INTEGER>) of different types in SubselectComparison(left=row_tuple(ref:ss.item.brand_id,ref:ss.item.class_id), right=row_tuple((1001001, 1),(1002001, 2)), operator=<ComparisonOperator.IN: 'in'>)
  ```
- `trilogy run --import raw.store_sales:ss select count(ss.item.item_sk) where (ss.item.brand_id::bigint, ss.item.class_id::bigint) in ((1001001::bigint, 1::bigint), (1002001::bigint, 2::bigint));`

  ```text
  Syntax error in stdin: Cannot compare composite-membership elements cast(ref:ss.item.brand_id,BIGINT) (BIGINT) and (cast(1001001,BIGINT), cast(1,BIGINT)) (ArrayType<BIGINT>) of different types in SubselectComparison(left=row_tuple(cast(ref:ss.item.brand_id,BIGINT),cast(ref:ss.item.class_id,BIGINT)), right=row_tuple((cast(1001001,BIGINT), cast(1,BIGINT)),(cast(1002001,BIGINT), cast(2,BIGINT))), operator=<ComparisonOperator.IN: 'in'>)
  ```
- `trilogy run test_union_in.preql`

  ```text
  Syntax error in test_union_in.preql: union(...) requires at least two relational arms.
  ```
- `trilogy file read answer_219203908.preql`

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
- `trilogy run answer_2524943990.preql`

  ```text
  Syntax error in answer_2524943990.preql: Comparison `cs.call_center.county = 'Williamson County'` matches every value of enum field 'cs.call_center.county', which contains only these values: 'Williamson County'. It is always true and should be removed.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_765177085.preql`

  ```text
  Resolution error in answer_765177085.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 8). The requested concepts split into 2 disconnected subgraphs: {_sales_with_returns_sr_qty, sr.customer.customer_sk, sr.date_dim.year, sr.item.item_sk, sr.ticket_number}; {_sales_with_returns_ss_qty, ss.customer.customer_sk, ss.date_dim.year, ss.item.item_desc, ss.item.item_id, ss.item.item_sk, ss.store.state, ss.ticket_number}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_1835050598.preql duckdb`

  ```text
  Syntax error in answer_1835050598.preql: Undefined concept: bill_customer.birth_year. Suggestions: ['cs.bill_customer.birth_year', 'cs.ship_customer.birth_year', 'cs.bill_customer.birth_day']
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 18 column 47 (char 1293). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3825713089.preql`

  ```text
  Resolution error in answer_3825713089.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 6). The requested concepts split into 3 disconnected subgraphs: {cs.bill_customer.customer_sk, cs.item.item_sk, cs.sold_date.moy, cs.sold_date.year, catalog_sale_net_profit}; {item_code, item_desc, store_code, store_name, store_sale_net_profit, ss.customer.customer_sk, ss.date_dim.moy, ss.date_dim.year, ss.item.item_sk}; {store_return_net_loss, sr.customer.customer_sk, sr.date_dim.moy, sr.date_dim.year, sr.item.item_sk}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_3825713089.preql`

  ```text
  Resolution error in answer_3825713089.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 5). The requested concepts split into 3 disconnected subgraphs: {cs.bill_customer.customer_sk, cs.item.item_sk, cs.sold_date.moy, cs.sold_date.year, catalog_sale_net_profit}; {item_code, item_desc, store_code, store_name, store_sale_net_profit, ss.customer.customer_sk, ss.date_dim.moy, ss.date_dim.year, ss.item.item_sk, ss.ticket_number}; {store_return_net_loss, sr.customer.customer_sk, sr.date_dim.moy, sr.date_dim.year, sr.item.item_sk, sr.ticket_number}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read answer_3825713089.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_4254732537.preql duckdb`

  ```text
  Syntax error in answer_4254732537.preql: Comparison `cs.promotion.channel_email = 'N'` matches every value of nullable enum field 'cs.promotion.channel_email', which contains only these values: 'N'. It only excludes nulls; simplify it to 'cs.promotion.channel_email is not null'.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 52 column 11 (char 4801). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file read raw/web_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_4207382245.preql`

  ```text
  Syntax error in answer_4207382245.preql: Undefined concept: st.county. Suggestions: ['ss.store.county', 'ws.web_site.county', 'store_totals.county', 'web_totals.county', 'ws.bill_addr.county', 'ws.ship_addr.county']
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.store_sales:store_sales select ticket_number limit 5;`

  ```text
  Syntax error in stdin: Undefined concept: local.ticket_number (line 2, col 8, in SELECT). Suggestions: ['store_sales.ticket_number']
  ```
- `trilogy run answer_3530408544.preql`

  ```text
  Syntax error in answer_3530408544.preql: Comparison `store_sales.store.county = 'Williamson County'` matches every value of enum field 'store_sales.store.county', which contains only these values: 'Williamson County'. It is always true and should be removed.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.store_sales:ss --import raw.customer:c select c.customer_sk where ss.customer.customer_sk in c.customer_sk and ss.date_dim.year = 2002 and ss.date_dim.qoy in (1,2,3) limit 5;`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {c.customer_sk}; {ss.customer.customer_sk, ss.date_dim.qoy, ss.date_dim.year}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3770074305.preql`

  ```text
  Resolution error in answer_3770074305.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 9). The requested concepts split into 2 disconnected subgraphs: {inv.date_dim.date, inv.quantity_on_hand}; {item.current_price, item.item_desc, item.item_id, item.item_sk, item.manufact_id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 56 column 3 (char 1877). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run answer_142117636.preql`

  ```text
  Syntax error in answer_142117636.preql: 2 undefined concept references; fix all before re-running:
    - local.jan_cv (line 43, col 5, in WHERE); did you mean: combined.jan_cv?
    - local.feb_cv (line 44, col 9, in WHERE); did you mean: combined.feb_cv?
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read answer_1663418777.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_1072197500.preql`

  ```text
  Syntax error in answer_1072197500.preql: ORDER BY references 'store_sales.date_dim.year', which is not in the SELECT projection (line 3). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --store_sales.date_dim.year order by store_sales.date_dim.year asc`.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_374591292.preql`

  ```text
  Resolution error in answer_374591292.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 47). The requested concepts split into 2 disconnected subgraphs: {best_performers.best_product_name, best_performers.rank_pos}; {worst_performers.rank_pos, worst_performers.worst_product_name}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read answer_374591292.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe item`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_968172492.preql`

  ```text
  Syntax error in answer_968172492.preql: Undefined concept: item.item_id. Suggestions: ['ws.item.item_id', 'ws.promotion.item.item_id', 'ws.item.item_desc', 'ws.item.item_sk']
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_2118989494.preql`

  ```text
  Resolution error in answer_2118989494.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 31). The requested concepts split into 2 disconnected subgraphs: {_combined_next_month_total, _combined_prior_month_total, monthly_totals.brand, monthly_totals.category, monthly_totals.company_name, monthly_totals.month_of_year, monthly_totals.month_total, monthly_totals.store_name, monthly_totals.year}; {yearly_avg.avg_monthly_sales}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read trilogy.toml`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_2940558602.preql`

  ```text
  Syntax error in answer_2940558602.preql: Undefined concept: item.sk. Suggestions: ['item.size', 'item.item_sk', 'item', 'sr.reason.sk', 'cr.reason.sk', 'wr.reason.sk']
  ```
- `trilogy run answer_2940558602.preql`

  ```text
  Resolution error in answer_2940558602.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 103). The requested concepts split into 2 disconnected subgraphs: {channel, currency_rank, return_rank, return_ratio}; {item}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_2940558602.preql`

  ```text
  Syntax error in answer_2940558602.preql: None
  ```
- `trilogy --debug run answer_2940558602.preql`

  ```text
  Syntax error in answer_2940558602.preql: None
  ```
- `trilogy run answer_2940558602.preql`

  ```text
  Syntax error in answer_2940558602.preql: None
  ```
- `trilogy run answer_2940558602.preql`

  ```text
  Syntax error in answer_2940558602.preql: None
  ```
- `trilogy run answer_2940558602.preql`

  ```text
  Syntax error in answer_2940558602.preql: None
  ```
- `trilogy run answer_2940558602.preql`

  ```text
  Resolution error in answer_2940558602.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 5). The requested concepts split into 2 disconnected subgraphs: {currency_ratio, return_ratio, sr.return_amt, ss.date_dim.moy, ss.date_dim.year, ss.net_paid, ss.net_profit, ss.quantity}; {item}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_2940558602.preql`

  ```text
  Resolution error in answer_2940558602.preql: (AmbiguousRelationshipResolutionException(...), "Multiple possible concept additions (intermediate join keys) found to resolve ['sr.item.item_sk', 'sr.return_amt', 'sr.return_quantity', 'sr.ticket_number', 'ss.date_dim.moy', 'ss.date_dim.year', 'ss.net_paid', 'ss.net_profit', 'ss.quantity', 'ss.date_dim.date_sk'], have {'sr.item.item_sk'} or {'sr.ticket_number'}. Different paths are is: [{'sr.item.item_sk'}, {'sr.ticket_number'}]")
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw/store_sales:ss --import raw/store_returns:sr select ss.ticket_number, ss.item.item_sk, sr.ticket_number, sr.item.item_sk, ss.store.store_sk, ss.customer.customer_sk, sr.customer.customer_sk, sr.store.store_sk, ss.date_dim.date, sr.date_dim.date limit 5`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {sr.customer.customer_sk, sr.date_dim.date, sr.item.item_sk, sr.store.store_sk, sr.ticket_number}; {ss.customer.customer_sk, ss.date_dim.date, ss.item.item_sk, ss.store.store_sk, ss.ticket_number}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_3560698360.preql`

  ```text
  Syntax error in answer_3560698360.preql: None
  ```
- `trilogy file read answer_3560698360.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3560698360.preql`

  ```text
  Syntax error in answer_3560698360.preql: Output column 'store_running_total' renames 'local.store_running_total' back to the name of an existing concept 'store_running_total' (defined at line 25) that 'local.store_running_total' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'store_running_total_out').
  ```
- `trilogy run answer_3560698360.preql`

  ```text
  Unexpected error in answer_3560698360.preql: (_duckdb.BinderException) Binder Error: Values list "wakeful" does not have a column named "item_sk"

  LINE 97:     coalesce("wakeful"."item_sk","wakeful"."store_daily_item_sk","young...
                        ^
  [SQL:
  WITH
  abundant as (
  SELECT
      "ws_sold_date_date_dim"."d_date" as "web_daily_d",
      "ws_sold_date_date_dim"."d_date" as "ws_d",
      "ws_web_sales"."ws_item_sk" as "web_daily_item_sk",
      "ws_web_sales"."ws_item_sk" as "ws_item_sk",
      sum("ws_web_sales"."ws_sales_price") as "web_daily_day_total"
  FROM
      "web_sales" as "ws_web_sales"
      INNER JOIN "date_dim" as "ws_sold_date_date_dim" on "ws_web_sales"."ws_sold_date_sk" = "ws_sold_date_date_dim"."d_date_sk"
  WHERE
      "ws_sold_date_date_dim"."d_year" = 2000 and "ws_web_sales"."ws_item_sk" is not null

  GROUP BY
      2,
      4),
  wakeful as (
  SELECT
      "ss_date_dim_date_dim"."d_date" as "ss_d",
      "ss_date_dim_date_dim"."d_date" as "store_daily_d",
      "ss_store_sales"."ss_item_sk" as "ss_item_sk",
      "ss_store_sales"."ss_item_sk" as "store_daily_item_sk",
      sum("ss_store_sales"."ss_sales_price") as "store_daily_day_total"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "date_dim" as "ss_date_dim_date_dim" on "ss_store_sales"."ss_sold_date_sk" = "ss_date_dim_date_dim"."d_date_sk"
  WHERE
      "ss_date_dim_date_dim"."d_year" = 2000 and "ss_store_sales"."ss_item_sk" is not null

  GROUP BY
      1,
      3),
  juicy as (
  SELECT
      "abundant"."web_daily_d" as "web_daily_d",
      "abundant"."web_daily_day_total" as "web_daily_day_total",
      "abundant"."web_daily_item_sk" as "web_daily_item_sk",
      "abundant"."ws_d" as "ws_d",
      "abundant"."ws_item_sk" as "ws_item_sk"
  FROM
      "abundant"
  GROUP BY
      1,
      2,
      3,
      4,
      5),
  vacuous as (
  SELECT
      "juicy"."web_daily_day_total" as "web_daily_day_total",
      "wakeful"."store_daily_day_total" as "store_daily_day_total",
      coalesce("juicy"."web_daily_d","wakeful"."store_daily_d") as "store_daily_d",
      coalesce("juicy"."web_daily_d","wakeful"."store_daily_d") as "web_daily_d",
      coalesce("juicy"."web_daily_item_sk","wakeful"."store_daily_item_sk") as "store_daily_item_sk",
      coalesce("juicy"."web_daily_item_sk","wakeful"."store_daily_item_sk") as "web_daily_item_sk",
      coalesce("juicy"."ws_d","wakeful"."ss_d") as "ss_d",
      coalesce("juicy"."ws_d","wakeful"."ss_d") as "ws_d",
      coalesce("juicy"."ws_item_sk","wakeful"."ss_item_sk") as "ss_item_sk",
      coalesce("juicy"."ws_item_sk","wakeful"."ss_item_sk") as "ws_item_sk"
  FROM
      "juicy"
      FULL JOIN "wakeful" on "juicy"."web_daily_d" = "wakeful"."store_daily_d" AND "juicy"."web_daily_item_sk" = "wakeful"."store_daily_item_sk"),
  abhorrent as (
  SELECT
      "vacuous"."ss_d" as "ss_d",
      "vacuous"."ss_item_sk" as "ss_item_sk",
      "vacuous"."store_daily_item_sk" as "store_daily_item_sk",
      sum("vacuous"."store_daily_day_total") over (partition by "vacuous"."ss_item_sk" order by "vacuous"."ss_d" asc ) as "s_rt"
  FROM
      "vacuous"),
  concerned as (
  SELECT
      "vacuous"."store_daily_item_sk" as "store_daily_item_sk",
      "vacuous"."ws_d" as "ws_d",
      "vacuous"."ws_item_sk" as "ws_item_sk",
      sum("vacuous"."web_daily_day_total") over (partition by "vacuous"."ws_item_sk" order by "vacuous"."ws_d" asc ) as "w_rt"
  FROM
      "vacuous"),
  young as (
  SELECT
      "concerned"."w_rt" as "w_rt",
      "vacuous"."web_daily_item_sk" as "web_daily_item_sk",
      coalesce("concerned"."store_daily_item_sk","concerned"."ws_item_sk","vacuous"."store_daily_item_sk","vacuous"."ws_item_sk") as "ws_item_sk",
      coalesce("concerned"."store_daily_item_sk","vacuous"."ss_item_sk") as "ss_item_sk",
      coalesce("concerned"."ws_d","vacuous"."ss_d") as "ss_d",
      coalesce("concerned"."ws_d","vacuous"."web_daily_d","vacuous"."ws_d") as "ws_d"
  FROM
      "vacuous"
      FULL JOIN "concerned" on "vacuous"."store_daily_item_sk" is not distinct from "concerned"."ws_item_sk" AND "vacuous"."web_daily_d" is not distinct from "concerned"."ws_d"),
  sparkling as (
  SELECT
      "young"."ss_item_sk" as "ss_item_sk",
      "young"."w_rt" as "w_rt",
      "young"."ws_item_sk" as "ws_item_sk",
      coalesce("wakeful"."item_sk","wakeful"."store_daily_item_sk","young"."ss_item_sk","young"."web_daily_item_sk") as "store_daily_item_sk",
      coalesce("wakeful"."ss_d","wakeful"."store_daily_d","young"."ss_d","young"."ws_d") as "store_daily_d",
      coalesce("wakeful"."ss_d","wakeful"."store_daily_d","young"."ss_d","young"."ws_d") as "ws_d",
      coalesce("wakeful"."ss_d","young"."ss_d") as "ss_d"
  FROM
      "young"
      FULL JOIN "wakeful" on "young"."web_daily_item_sk" = "wakeful"."store_daily_item_sk" AND "young"."ws_d" = "wakeful"."store_daily_d"),
  sweltering as (
  SELECT
      "abhorrent"."s_rt" as "s_rt",
      "sparkling"."w_rt" as "w_rt",
      coalesce("abhorrent"."ss_d","sparkling"."ss_d","sparkling"."store_daily_d") as "ss_d",
      coalesce("abhorrent"."ss_d","sparkling"."ss_d","sparkling"."store_daily_d") as "store_daily_d",
      coalesce("abhorrent"."ss_d","sparkling"."ws_d") as "ws_d",
      coalesce("abhorrent"."ss_item_sk","abhorrent"."store_daily_item_sk","sparkling"."ss_item_sk","sparkling"."store_daily_item_sk") as "ss_item_sk",
      coalesce("abhorrent"."ss_item_sk","abhorrent"."store_daily_item_sk","sparkling"."ss_item_sk","sparkling"."store_daily_item_sk") as "store_daily_item_sk",
      coalesce(coalesce("abhorrent"."ss_item_sk","abhorrent"."store_daily_item_sk","sparkling"."ss_item_sk","sparkling"."store_daily_item_sk"),coalesce("abhorrent"."ss_item_sk","sparkling"."ws_item_sk")) as "_virt_func_coalesce_2513681567070847"
  FROM
      "sparkling"
      FULL JOIN "abhorrent" on "sparkling"."store_daily_d" is not distinct from "abhorrent"."ss_d" AND "sparkling"."store_daily_item_sk" is not distinct from "abhorrent"."ss_item_sk"),
  late as (
  SELECT
      "sweltering"."s_rt" as "s_rt",
      "sweltering"."ss_d" as "ss_d",
      "sweltering"."store_daily_d" as "store_daily_d",
      "sweltering"."store_daily_item_sk" as "store_daily_item_sk",
      "sweltering"."w_rt" as "w_rt",
      "sweltering"."ws_d" as "ws_d",
      max("sweltering"."s_rt") over (partition by "sweltering"."_virt_func_coalesce_2513681567070847" order by coalesce("sweltering"."ss_d","sweltering"."ws_d") asc ) as "store_running_max",
      max("sweltering"."w_rt") over (partition by "sweltering"."_virt_func_coalesce_2513681567070847" order by coalesce("sweltering"."ss_d","sweltering"."ws_d") asc ) as "web_running_max"
  FROM
      "sweltering")
  SELECT
      coalesce("vacuous"."ss_item_sk","vacuous"."ws_item_sk") as "item_sk",
      coalesce(coalesce("late"."ss_d","late"."store_daily_d"),"late"."ws_d") as "d",
      "late"."s_rt" as "store_running_total",
      "late"."w_rt" as "web_running_total",
      "late"."store_running_max" as "store_running_max",
      "late"."web_running_max" as "web_running_max"
  FROM
      "vacuous"
      RIGHT OUTER JOIN "late" on "vacuous"."store_daily_d" is not distinct from "late"."ss_d" AND "vacuous"."store_daily_item_sk" is not distinct from "late"."store_daily_item_sk"
  WHERE
      "late"."web_running_max" > "late"."store_running_max"

  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6
  ORDER BY
      "item_sk" asc nulls first,
      "d" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run answer_3560698360.preql`

  ```text
  Syntax error in answer_3560698360.preql: None
  ```
- `trilogy run answer_3560698360.preql`

  ```text
  Syntax error in answer_3560698360.preql: None
  ```
- `trilogy run answer_3560698360.preql`

  ```text
  Syntax error in answer_3560698360.preql: None
  ```
- `trilogy run answer_3560698360.preql`

  ```text
  Syntax error in answer_3560698360.preql: None
  ```
- `trilogy run test_win3.preql`

  ```text
  Syntax error in test_win3.preql: None
  ```
- `trilogy run answer_3560698360.preql`

  ```text
  Syntax error in answer_3560698360.preql: Output column 'store_running_max' renames 'local.store_running_max' back to the name of an existing concept 'store_running_max' (defined at line 29) that 'local.store_running_max' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'store_running_max_out').
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw/catalog_sales:cs select cs.date_dim.week_seq, cs.date_dim.date, cs.item.item_id, cs.ext_sales_price where cs.date_dim.date = '2000-01-03'::date limit 3;`

  ```text
  Syntax error in stdin: 3 undefined concept references; fix all before re-running:
    - cs.date_dim.week_seq (line 2, col 8, in SELECT); did you mean: cs.call_center.date_dim.week_seq, cs.sold_date.week_seq, cs.ship_date.week_seq, cs.promotion.end_date.week_seq, cs.promotion.start_date.week_seq, cs.catalog_page.end_date.week_seq?
    - cs.date_dim.date (line 2, col 30, in SELECT); did you mean: cs.call_center.date_dim.date, cs.time_dim.time, cs.date_dim.week_seq, cs.ship_date.date, cs.sold_date.date, cs.promotion.end_date.date?
    - cs.date_dim.date (line 2, col 90, in WHERE); did you mean: cs.call_center.date_dim.date, cs.time_dim.time, cs.date_dim.week_seq, cs.ship_date.date, cs.sold_date.date, cs.promotion.end_date.date?
  ```
- `trilogy run --import raw/web_sales:ws select ws.date_dim.week_seq, ws.date_dim.date, ws.item.item_id, ws.ext_sales_price where ws.date_dim.date = '2000-01-03'::date limit 3;`

  ```text
  Syntax error in stdin: 3 undefined concept references; fix all before re-running:
    - ws.date_dim.week_seq (line 2, col 8, in SELECT); did you mean: ws.sold_date.week_seq, ws.ship_date.week_seq, ws.sold_date.fy_week_seq, ws.promotion.end_date.week_seq, ws.web_site.open_date.week_seq, ws.web_site.close_date.week_seq?
    - ws.date_dim.date (line 2, col 30, in SELECT); did you mean: ws.time_dim.time, ws.date_dim.week_seq, ws.time_dim.minute, ws.ship_date.date, ws.sold_date.date, ws.promotion.end_date.date?
    - ws.date_dim.date (line 2, col 90, in WHERE); did you mean: ws.time_dim.time, ws.date_dim.week_seq, ws.time_dim.minute, ws.ship_date.date, ws.sold_date.date, ws.promotion.end_date.date?
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_1484301313.preql`

  ```text
  Syntax error in answer_1484301313.preql: Comparison `store_sales.promotion.channel_email = 'Y'` can never match enum field 'store_sales.promotion.channel_email', which contains only these values: 'N'. It is always false and should be removed.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.store_sales:ss --import raw.catalog_sales:cs --import raw.catalog_returns:cr --import raw.store_returns:sr select item.product_name, ss.item.item_sk limit 5;`

  ```text
  Syntax error in stdin: Undefined concept: item.product_name (line 5, col 8, in SELECT). Suggestions: ['ss.item.product_name', 'cs.item.product_name', 'cr.item.product_name', 'sr.item.product_name', 'ss.promotion.item.product_name', 'cs.promotion.item.product_name']
  ```
- `trilogy run answer_3544057080.preql`

  ```text
  Syntax error in answer_3544057080.preql: Nothing was executed: parsed 7 definition statement(s) (4 imports, 3 rowsets) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy run answer_3544057080.preql`

  ```text
  Resolution error in answer_3544057080.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 23). The requested concepts split into 3 disconnected subgraphs: {item_cat_ext_price.total_ext_list_price}; {item_cat_refund.total_refund}; {ss.item.item_sk}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_3544057080.preql`

  ```text
  Syntax error in answer_3544057080.preql: Nothing was executed: parsed 5 definition statement(s) (3 rowsets, 2 imports) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy run answer_3544057080.preql`

  ```text
  Syntax error in answer_3544057080.preql: Undefined concept: item.item_sk. Suggestions: ['ss.item.item_sk', 'sr.item.item_sk', 'cs.item.item_sk', 'cr.item.item_sk', 'ss.promotion.item.item_sk', 'cs.promotion.item.item_sk']
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
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read answer_3036656719.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run answer_2874833976.preql`

  ```text
  Syntax error in answer_2874833976.preql: Undefined concept: top_states.state_total.state (line 28, col 25, in WHERE). Suggestions: ['top_states.state_total', 'top_states.ss.store.state', 'ss.store.state', 'ss.customer.customer_address.state', 'ss.customer_address.state']
  ```
- `trilogy file read raw/catalog_sales.preql`

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
- `trilogy file read raw/store.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_1772060640.preql`

  ```text
  Syntax error in answer_1772060640.preql: Comparison `store_sales.store.county in ('Orange County', 'Bronx County', 'Franklin Parish', 'Williamson County')` matches every value of enum field 'store_sales.store.county', which contains only these values: 'Williamson County'. It is always true and should be removed.
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
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run --import raw/store_sales:ss select ss.ticket_number, ss.item.item_sk, ss.store.store_sk, ss.sold_date.date_sk limit 5;`

  ```text
  Syntax error in stdin: Undefined concept: ss.sold_date.date_sk (line 2, col 62, in SELECT). Suggestions: ['ss.date_dim.date_sk', 'ss.store.date_dim.date_sk', 'ss.promotion.end_date.date_sk', 'ss.customer.first_sales_date.date_sk', 'ss.customer.last_review_date.date_sk', 'ss.promotion.start_date.date_sk']
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3210116865.preql`

  ```text
  Syntax error in answer_3210116865.preql: ORDER BY contains aggregate `grouping(combined.channel)` (line 44), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(combined.channel) as g order by g desc`.
  ```
- `trilogy run answer_3210116865.preql`

  ```text
  Syntax error in answer_3210116865.preql: Missing source map entry for sr.store.store_sk with pseudonyms set(), have map: {'ss.date_dim.date_sk': {BuildDatasource(name='store_sales', columns=[BuildColumnAssignment(alias='ss_sold_date_sk', concept=ss.date_dim.date_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_sold_time_sk', concept=ss.time_dim.time_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_item_sk', concept=ss.item.item_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_customer_sk', concept=ss.customer.customer_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_cdemo_sk', concept=ss.customer_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_hdemo_sk', concept=ss.household_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_addr_sk', concept=ss.customer_address.address_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_store_sk', concept=ss.store.store_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_promo_sk', concept=ss.promotion.promo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ticket_number', concept=ss.ticket_number@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_quantity', concept=ss.quantity@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_wholesale_cost', concept=ss.wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_list_price', concept=ss.list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sales_price', concept=ss.sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_discount_amt', concept=ss.ext_discount_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_sales_price', concept=ss.ext_sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_wholesale_cost', concept=ss.ext_wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_list_price', concept=ss.ext_list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_tax', concept=ss.ext_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_coupon_amt', concept=ss.coupon_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid', concept=ss.net_paid@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid_inc_tax', concept=ss.net_paid_inc_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_profit', concept=ss.net_profit@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='store_sales', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'ss.ticket_number', 'ss.item.item_sk'}, where_clause=None, _str='Grain<ss.item.item_sk,ss.ticket_number>', _str_no_condition='Grain<ss.item.item_sk,ss.ticket_number>', abstract=False), namespace='ss', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ss.customer.customer_sk', 'ss.date_dim.date_sk', 'ss.time_dim.time_sk', 'ss.customer_demographics.demo_sk', 'ss.store.store_sk', 'ss.customer_address.address_sk'})}, 'ss.time_dim.time_sk': {BuildDatasource(name='store_sales', columns=[BuildColumnAssignment(alias='ss_sold_date_sk', concept=ss.date_dim.date_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_sold_time_sk', concept=ss.time_dim.time_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_item_sk', concept=ss.item.item_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_customer_sk', concept=ss.customer.customer_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_cdemo_sk', concept=ss.customer_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_hdemo_sk', concept=ss.household_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_addr_sk', concept=ss.customer_address.address_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_store_sk', concept=ss.store.store_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_promo_sk', concept=ss.promotion.promo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ticket_number', concept=ss.ticket_number@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_quantity', concept=ss.quantity@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_wholesale_cost', concept=ss.wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_list_price', concept=ss.list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sales_price', concept=ss.sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_discount_amt', concept=ss.ext_discount_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_sales_price', concept=ss.ext_sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_wholesale_cost', concept=ss.ext_wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_list_price', concept=ss.ext_list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_tax', concept=ss.ext_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_coupon_amt', concept=ss.coupon_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid', concept=ss.net_paid@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid_inc_tax', concept=ss.net_paid_inc_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_profit', concept=ss.net_profit@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='store_sales', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'ss.ticket_number', 'ss.item.item_sk'}, where_clause=None, _str='Grain<ss.item.item_sk,ss.ticket_number>', _str_no_condition='Grain<ss.item.item_sk,ss.ticket_number>', abstract=False), namespace='ss', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ss.customer.customer_sk', 'ss.date_dim.date_sk', 'ss.time_dim.time_sk', 'ss.customer_demographics.demo_sk', 'ss.store.store_sk', 'ss.customer_address.address_sk'})}, 'ss.item.item_sk': {BuildDatasource(name='store_sales', columns=[BuildColumnAssignment(alias='ss_sold_date_sk', concept=ss.date_dim.date_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_sold_time_sk', concept=ss.time_dim.time_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_item_sk', concept=ss.item.item_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_customer_sk', concept=ss.customer.customer_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_cdemo_sk', concept=ss.customer_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_hdemo_sk', concept=ss.household_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_addr_sk', concept=ss.customer_address.address_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_store_sk', concept=ss.store.store_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_promo_sk', concept=ss.promotion.promo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ticket_number', concept=ss.ticket_number@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_quantity', concept=ss.quantity@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_wholesale_cost', concept=ss.wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_list_price', concept=ss.list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sales_price', concept=ss.sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_discount_amt', concept=ss.ext_discount_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_sales_price', concept=ss.ext_sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_wholesale_cost', concept=ss.ext_wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_list_price', concept=ss.ext_list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_tax', concept=ss.ext_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_coupon_amt', concept=ss.coupon_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid', concept=ss.net_paid@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid_inc_tax', concept=ss.net_paid_inc_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_profit', concept=ss.net_profit@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='store_sales', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'ss.ticket_number', 'ss.item.item_sk'}, where_clause=None, _str='Grain<ss.item.item_sk,ss.ticket_number>', _str_no_condition='Grain<ss.item.item_sk,ss.ticket_number>', abstract=False), namespace='ss', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ss.customer.customer_sk', 'ss.date_dim.date_sk', 'ss.time_dim.time_sk', 'ss.customer_demographics.demo_sk', 'ss.store.store_sk', 'ss.customer_address.address_sk'})}, 'ss.customer.customer_sk': {BuildDatasource(name='store_sales', columns=[BuildColumnAssignment(alias='ss_sold_date_sk', concept=ss.date_dim.date_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_sold_time_sk', concept=ss.time_dim.time_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_item_sk', concept=ss.item.item_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_customer_sk', concept=ss.customer.customer_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_cdemo_sk', concept=ss.customer_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_hdemo_sk', concept=ss.household_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_addr_sk', concept=ss.customer_address.address_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_store_sk', concept=ss.store.store_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_promo_sk', concept=ss.promotion.promo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ticket_number', concept=ss.ticket_number@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_quantity', concept=ss.quantity@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_wholesale_cost', concept=ss.wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_list_price', concept=ss.list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sales_price', concept=ss.sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_discount_amt', concept=ss.ext_discount_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_sales_price', concept=ss.ext_sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_wholesale_cost', concept=ss.ext_wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_list_price', concept=ss.ext_list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_tax', concept=ss.ext_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_coupon_amt', concept=ss.coupon_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid', concept=ss.net_paid@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid_inc_tax', concept=ss.net_paid_inc_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_profit', concept=ss.net_profit@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='store_sales', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'ss.ticket_number', 'ss.item.item_sk'}, where_clause=None, _str='Grain<ss.item.item_sk,ss.ticket_number>', _str_no_condition='Grain<ss.item.item_sk,ss.ticket_number>', abstract=False), namespace='ss', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ss.customer.customer_sk', 'ss.date_dim.date_sk', 'ss.time_dim.time_sk', 'ss.customer_demographics.demo_sk', 'ss.store.store_sk', 'ss.customer_address.address_sk'})}, 'ss.customer_demographics.demo_sk': {BuildDatasource(name='store_sales', columns=[BuildColumnAssignment(alias='ss_sold_date_sk', concept=ss.date_dim.date_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_sold_time_sk', concept=ss.time_dim.time_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_item_sk', concept=ss.item.item_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_customer_sk', concept=ss.customer.customer_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_cdemo_sk', concept=ss.customer_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_hdemo_sk', concept=ss.household_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_addr_sk', concept=ss.customer_address.address_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_store_sk', concept=ss.store.store_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_promo_sk', concept=ss.promotion.promo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ticket_number', concept=ss.ticket_number@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_quantity', concept=ss.quantity@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_wholesale_cost', concept=ss.wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_list_price', concept=ss.list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sales_price', concept=ss.sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_discount_amt', concept=ss.ext_discount_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_sales_price', concept=ss.ext_sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_wholesale_cost', concept=ss.ext_wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_list_price', concept=ss.ext_list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_tax', concept=ss.ext_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_coupon_amt', concept=ss.coupon_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid', concept=ss.net_paid@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid_inc_tax', concept=ss.net_paid_inc_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_profit', concept=ss.net_profit@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='store_sales', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'ss.ticket_number', 'ss.item.item_sk'}, where_clause=None, _str='Grain<ss.item.item_sk,ss.ticket_number>', _str_no_condition='Grain<ss.item.item_sk,ss.ticket_number>', abstract=False), namespace='ss', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ss.customer.customer_sk', 'ss.date_dim.date_sk', 'ss.time_dim.time_sk', 'ss.customer_demographics.demo_sk', 'ss.store.store_sk', 'ss.customer_address.address_sk'})}, 'ss.household_demographics.demo_sk': {BuildDatasource(name='store_sales', columns=[BuildColumnAssignment(alias='ss_sold_date_sk', concept=ss.date_dim.date_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_sold_time_sk', concept=ss.time_dim.time_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_item_sk', concept=ss.item.item_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_customer_sk', concept=ss.customer.customer_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_cdemo_sk', concept=ss.customer_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_hdemo_sk', concept=ss.household_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_addr_sk', concept=ss.customer_address.address_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_store_sk', concept=ss.store.store_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_promo_sk', concept=ss.promotion.promo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ticket_number', concept=ss.ticket_number@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_quantity', concept=ss.quantity@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_wholesale_cost', concept=ss.wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_list_price', concept=ss.list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sales_price', concept=ss.sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_discount_amt', concept=ss.ext_discount_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_sales_price', concept=ss.ext_sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_wholesale_cost', concept=ss.ext_wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_list_price', concept=ss.ext_list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_tax', concept=ss.ext_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_coupon_amt', concept=ss.coupon_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid', concept=ss.net_paid@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid_inc_tax', concept=ss.net_paid_inc_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_profit', concept=ss.net_profit@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='store_sales', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'ss.ticket_number', 'ss.item.item_sk'}, where_clause=None, _str='Grain<ss.item.item_sk,ss.ticket_number>', _str_no_condition='Grain<ss.item.item_sk,ss.ticket_number>', abstract=False), namespace='ss', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ss.customer.customer_sk', 'ss.date_dim.date_sk', 'ss.time_dim.time_sk', 'ss.customer_demographics.demo_sk', 'ss.store.store_sk', 'ss.customer_address.address_sk'})}, 'ss.customer_address.address_sk': {BuildDatasource(name='store_sales', columns=[BuildColumnAssignment(alias='ss_sold_date_sk', concept=ss.date_dim.date_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_sold_time_sk', concept=ss.time_dim.time_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_item_sk', concept=ss.item.item_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_customer_sk', concept=ss.customer.customer_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_cdemo_sk', concept=ss.customer_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_hdemo_sk', concept=ss.household_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_addr_sk', concept=ss.customer_address.address_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_store_sk', concept=ss.store.store_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_promo_sk', concept=ss.promotion.promo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ticket_number', concept=ss.ticket_number@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_quantity', concept=ss.quantity@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_wholesale_cost', concept=ss.wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_list_price', concept=ss.list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sales_price', concept=ss.sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_discount_amt', concept=ss.ext_discount_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_sales_price', concept=ss.ext_sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_wholesale_cost', concept=ss.ext_wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_list_price', concept=ss.ext_list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_tax', concept=ss.ext_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_coupon_amt', concept=ss.coupon_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid', concept=ss.net_paid@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid_inc_tax', concept=ss.net_paid_inc_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_profit', concept=ss.net_profit@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='store_sales', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'ss.ticket_number', 'ss.item.item_sk'}, where_clause=None, _str='Grain<ss.item.item_sk,ss.ticket_number>', _str_no_condition='Grain<ss.item.item_sk,ss.ticket_number>', abstract=False), namespace='ss', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ss.customer.customer_sk', 'ss.date_dim.date_sk', 'ss.time_dim.time_sk', 'ss.customer_demographics.demo_sk', 'ss.store.store_sk', 'ss.customer_address.address_sk'})}, 'ss.store.store_sk': {BuildDatasource(name='store_sales', columns=[BuildColumnAssignment(alias='ss_sold_date_sk', concept=ss.date_dim.date_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_sold_time_sk', concept=ss.time_dim.time_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_item_sk', concept=ss.item.item_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_customer_sk', concept=ss.customer.customer_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_cdemo_sk', concept=ss.customer_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_hdemo_sk', concept=ss.household_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_addr_sk', concept=ss.customer_address.address_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_store_sk', concept=ss.store.store_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_promo_sk', concept=ss.promotion.promo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ticket_number', concept=ss.ticket_number@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_quantity', concept=ss.quantity@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_wholesale_cost', concept=ss.wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_list_price', concept=ss.list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sales_price', concept=ss.sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_discount_amt', concept=ss.ext_discount_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_sales_price', concept=ss.ext_sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_wholesale_cost', concept=ss.ext_wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_list_price', concept=ss.ext_list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_tax', concept=ss.ext_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_coupon_amt', concept=ss.coupon_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid', concept=ss.net_paid@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid_inc_tax', concept=ss.net_paid_inc_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_profit', concept=ss.net_profit@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='store_sales', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'ss.ticket_number', 'ss.item.item_sk'}, where_clause=None, _str='Grain<ss.item.item_sk,ss.ticket_number>', _str_no_condition='Grain<ss.item.item_sk,ss.ticket_number>', abstract=False), namespace='ss', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ss.customer.customer_sk', 'ss.date_dim.date_sk', 'ss.time_dim.time_sk', 'ss.customer_demographics.demo_sk', 'ss.store.store_sk', 'ss.customer_address.address_sk'})}, 'ss.promotion.promo_sk': {BuildDatasource(name='store_sales', columns=[BuildColumnAssignment(alias='ss_sold_date_sk', concept=ss.date_dim.date_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_sold_time_sk', concept=ss.time_dim.time_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_item_sk', concept=ss.item.item_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_customer_sk', concept=ss.customer.customer_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_cdemo_sk', concept=ss.customer_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_hdemo_sk', concept=ss.household_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_addr_sk', concept=ss.customer_address.address_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_store_sk', concept=ss.store.store_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_promo_sk', concept=ss.promotion.promo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ticket_number', concept=ss.ticket_number@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_quantity', concept=ss.quantity@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_wholesale_cost', concept=ss.wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_list_price', concept=ss.list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sales_price', concept=ss.sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_discount_amt', concept=ss.ext_discount_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_sales_price', concept=ss.ext_sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_wholesale_cost', concept=ss.ext_wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_list_price', concept=ss.ext_list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_tax', concept=ss.ext_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_coupon_amt', concept=ss.coupon_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid', concept=ss.net_paid@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid_inc_tax', concept=ss.net_paid_inc_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_profit', concept=ss.net_profit@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='store_sales', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'ss.ticket_number', 'ss.item.item_sk'}, where_clause=None, _str='Grain<ss.item.item_sk,ss.ticket_number>', _str_no_condition='Grain<ss.item.item_sk,ss.ticket_number>', abstract=False), namespace='ss', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ss.customer.customer_sk', 'ss.date_dim.date_sk', 'ss.time_dim.time_sk', 'ss.customer_demographics.demo_sk', 'ss.store.store_sk', 'ss.customer_address.address_sk'})}, 'ss.ticket_number': {BuildDatasource(name='store_sales', columns=[BuildColumnAssignment(alias='ss_sold_date_sk', concept=ss.date_dim.date_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_sold_time_sk', concept=ss.time_dim.time_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_item_sk', concept=ss.item.item_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_customer_sk', concept=ss.customer.customer_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_cdemo_sk', concept=ss.customer_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_hdemo_sk', concept=ss.household_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_addr_sk', concept=ss.customer_address.address_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_store_sk', concept=ss.store.store_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_promo_sk', concept=ss.promotion.promo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ticket_number', concept=ss.ticket_number@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_quantity', concept=ss.quantity@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_wholesale_cost', concept=ss.wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_list_price', concept=ss.list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sales_price', concept=ss.sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_discount_amt', concept=ss.ext_discount_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_sales_price', concept=ss.ext_sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_wholesale_cost', concept=ss.ext_wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_list_price', concept=ss.ext_list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_tax', concept=ss.ext_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_coupon_amt', concept=ss.coupon_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid', concept=ss.net_paid@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid_inc_tax', concept=ss.net_paid_inc_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_profit', concept=ss.net_profit@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='store_sales', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'ss.ticket_number', 'ss.item.item_sk'}, where_clause=None, _str='Grain<ss.item.item_sk,ss.ticket_number>', _str_no_condition='Grain<ss.item.item_sk,ss.ticket_number>', abstract=False), namespace='ss', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ss.customer.customer_sk', 'ss.date_dim.date_sk', 'ss.time_dim.time_sk', 'ss.customer_demographics.demo_sk', 'ss.store.store_sk', 'ss.customer_address.address_sk'})}, 'ss.quantity': {BuildDatasource(name='store_sales', columns=[BuildColumnAssignment(alias='ss_sold_date_sk', concept=ss.date_dim.date_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_sold_time_sk', concept=ss.time_dim.time_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_item_sk', concept=ss.item.item_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_customer_sk', concept=ss.customer.customer_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_cdemo_sk', concept=ss.customer_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_hdemo_sk', concept=ss.household_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_addr_sk', concept=ss.customer_address.address_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_store_sk', concept=ss.store.store_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_promo_sk', concept=ss.promotion.promo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ticket_number', concept=ss.ticket_number@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_quantity', concept=ss.quantity@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_wholesale_cost', concept=ss.wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_list_price', concept=ss.list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sales_price', concept=ss.sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_discount_amt', concept=ss.ext_discount_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_sales_price', concept=ss.ext_sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_wholesale_cost', concept=ss.ext_wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_list_price', concept=ss.ext_list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_tax', concept=ss.ext_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_coupon_amt', concept=ss.coupon_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid', concept=ss.net_paid@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid_inc_tax', concept=ss.net_paid_inc_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_profit', concept=ss.net_profit@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='store_sales', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'ss.ticket_number', 'ss.item.item_sk'}, where_clause=None, _str='Grain<ss.item.item_sk,ss.ticket_number>', _str_no_condition='Grain<ss.item.item_sk,ss.ticket_number>', abstract=False), namespace='ss', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ss.customer.customer_sk', 'ss.date_dim.date_sk', 'ss.time_dim.time_sk', 'ss.customer_demographics.demo_sk', 'ss.store.store_sk', 'ss.customer_address.address_sk'})}, 'ss.wholesale_cost': {BuildDatasource(name='store_sales', columns=[BuildColumnAssignment(alias='ss_sold_date_sk', concept=ss.date_dim.date_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_sold_time_sk', concept=ss.time_dim.time_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_item_sk', concept=ss.item.item_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_customer_sk', concept=ss.customer.customer_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_cdemo_sk', concept=ss.customer_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_hdemo_sk', concept=ss.household_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_addr_sk', concept=ss.customer_address.address_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_store_sk', concept=ss.store.store_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_promo_sk', concept=ss.promotion.promo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ticket_number', concept=ss.ticket_number@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_quantity', concept=ss.quantity@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_wholesale_cost', concept=ss.wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_list_price', concept=ss.list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sales_price', concept=ss.sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_discount_amt', concept=ss.ext_discount_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_sales_price', concept=ss.ext_sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_wholesale_cost', concept=ss.ext_wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_list_price', concept=ss.ext_list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_tax', concept=ss.ext_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_coupon_amt', concept=ss.coupon_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid', concept=ss.net_paid@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid_inc_tax', concept=ss.net_paid_inc_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_profit', concept=ss.net_profit@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='store_sales', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'ss.ticket_number', 'ss.item.item_sk'}, where_clause=None, _str='Grain<ss.item.item_sk,ss.ticket_number>', _str_no_condition='Grain<ss.item.item_sk,ss.ticket_number>', abstract=False), namespace='ss', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ss.customer.customer_sk', 'ss.date_dim.date_sk', 'ss.time_dim.time_sk', 'ss.customer_demographics.demo_sk', 'ss.store.store_sk', 'ss.customer_address.address_sk'})}, 'ss.list_price': {BuildDatasource(name='store_sales', columns=[BuildColumnAssignment(alias='ss_sold_date_sk', concept=ss.date_dim.date_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_sold_time_sk', concept=ss.time_dim.time_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_item_sk', concept=ss.item.item_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_customer_sk', concept=ss.customer.customer_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_cdemo_sk', concept=ss.customer_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_hdemo_sk', concept=ss.household_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_addr_sk', concept=ss.customer_address.address_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_store_sk', concept=ss.store.store_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_promo_sk', concept=ss.promotion.promo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ticket_number', concept=ss.ticket_number@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_quantity', concept=ss.quantity@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_wholesale_cost', concept=ss.wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_list_price', concept=ss.list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sales_price', concept=ss.sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_discount_amt', concept=ss.ext_discount_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_sales_price', concept=ss.ext_sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_wholesale_cost', concept=ss.ext_wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_list_price', concept=ss.ext_list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_tax', concept=ss.ext_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_coupon_amt', concept=ss.coupon_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid', concept=ss.net_paid@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid_inc_tax', concept=ss.net_paid_inc_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_profit', concept=ss.net_profit@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='store_sales', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'ss.ticket_number', 'ss.item.item_sk'}, where_clause=None, _str='Grain<ss.item.item_sk,ss.ticket_number>', _str_no_condition='Grain<ss.item.item_sk,ss.ticket_number>', abstract=False), namespace='ss', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ss.customer.customer_sk', 'ss.date_dim.date_sk', 'ss.time_dim.time_sk', 'ss.customer_demographics.demo_sk', 'ss.store.store_sk', 'ss.customer_address.address_sk'})}, 'ss.sales_price': {BuildDatasource(name='store_sales', columns=[BuildColumnAssignment(alias='ss_sold_date_sk', concept=ss.date_dim.date_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_sold_time_sk', concept=ss.time_dim.time_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_item_sk', concept=ss.item.item_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_customer_sk', concept=ss.customer.customer_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_cdemo_sk', concept=ss.customer_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_hdemo_sk', concept=ss.household_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_addr_sk', concept=ss.customer_address.address_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_store_sk', concept=ss.store.store_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_promo_sk', concept=ss.promotion.promo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ticket_number', concept=ss.ticket_number@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_quantity', concept=ss.quantity@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_wholesale_cost', concept=ss.wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_list_price', concept=ss.list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sales_price', concept=ss.sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_discount_amt', concept=ss.ext_discount_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_sales_price', concept=ss.ext_sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_wholesale_cost', concept=ss.ext_wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_list_price', concept=ss.ext_list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_tax', concept=ss.ext_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_coupon_amt', concept=ss.coupon_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid', concept=ss.net_paid@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid_inc_tax', concept=ss.net_paid_inc_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_profit', concept=ss.net_profit@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='store_sales', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'ss.ticket_number', 'ss.item.item_sk'}, where_clause=None, _str='Grain<ss.item.item_sk,ss.ticket_number>', _str_no_condition='Grain<ss.item.item_sk,ss.ticket_number>', abstract=False), namespace='ss', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ss.customer.customer_sk', 'ss.date_dim.date_sk', 'ss.time_dim.time_sk', 'ss.customer_demographics.demo_sk', 'ss.store.store_sk', 'ss.customer_address.address_sk'})}, 'ss.ext_discount_amt': {BuildDatasource(name='store_sales', columns=[BuildColumnAssignment(alias='ss_sold_date_sk', concept=ss.date_dim.date_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_sold_time_sk', concept=ss.time_dim.time_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_item_sk', concept=ss.item.item_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_customer_sk', concept=ss.customer.customer_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_cdemo_sk', concept=ss.customer_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_hdemo_sk', concept=ss.household_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_addr_sk', concept=ss.customer_address.address_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_store_sk', concept=ss.store.store_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_promo_sk', concept=ss.promotion.promo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ticket_number', concept=ss.ticket_number@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_quantity', concept=ss.quantity@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_wholesale_cost', concept=ss.wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_list_price', concept=ss.list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sales_price', concept=ss.sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_discount_amt', concept=ss.ext_discount_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_sales_price', concept=ss.ext_sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_wholesale_cost', concept=ss.ext_wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_list_price', concept=ss.ext_list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_tax', concept=ss.ext_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_coupon_amt', concept=ss.coupon_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid', concept=ss.net_paid@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid_inc_tax', concept=ss.net_paid_inc_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_profit', concept=ss.net_profit@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='store_sales', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'ss.ticket_number', 'ss.item.item_sk'}, where_clause=None, _str='Grain<ss.item.item_sk,ss.ticket_number>', _str_no_condition='Grain<ss.item.item_sk,ss.ticket_number>', abstract=False), namespace='ss', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ss.customer.customer_sk', 'ss.date_dim.date_sk', 'ss.time_dim.time_sk', 'ss.customer_demographics.demo_sk', 'ss.store.store_sk', 'ss.customer_address.address_sk'})}, 'ss.ext_sales_price': {BuildDatasource(name='store_sales', columns=[BuildColumnAssignment(alias='ss_sold_date_sk', concept=ss.date_dim.date_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_sold_time_sk', concept=ss.time_dim.time_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_item_sk', concept=ss.item.item_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_customer_sk', concept=ss.customer.customer_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_cdemo_sk', concept=ss.customer_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_hdemo_sk', concept=ss.household_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_addr_sk', concept=ss.customer_address.address_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_store_sk', concept=ss.store.store_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_promo_sk', concept=ss.promotion.promo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ticket_number', concept=ss.ticket_number@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_quantity', concept=ss.quantity@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_wholesale_cost', concept=ss.wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_list_price', concept=ss.list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sales_price', concept=ss.sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_discount_amt', concept=ss.ext_discount_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_sales_price', concept=ss.ext_sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_wholesale_cost', concept=ss.ext_wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_list_price', concept=ss.ext_list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_tax', concept=ss.ext_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_coupon_amt', concept=ss.coupon_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid', concept=ss.net_paid@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid_inc_tax', concept=ss.net_paid_inc_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_profit', concept=ss.net_profit@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='store_sales', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'ss.ticket_number', 'ss.item.item_sk'}, where_clause=None, _str='Grain<ss.item.item_sk,ss.ticket_number>', _str_no_condition='Grain<ss.item.item_sk,ss.ticket_number>', abstract=False), namespace='ss', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ss.customer.customer_sk', 'ss.date_dim.date_sk', 'ss.time_dim.time_sk', 'ss.customer_demographics.demo_sk', 'ss.store.store_sk', 'ss.customer_address.address_sk'})}, 'ss.ext_wholesale_cost': {BuildDatasource(name='store_sales', columns=[BuildColumnAssignment(alias='ss_sold_date_sk', concept=ss.date_dim.date_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_sold_time_sk', concept=ss.time_dim.time_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_item_sk', concept=ss.item.item_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_customer_sk', concept=ss.customer.customer_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_cdemo_sk', concept=ss.customer_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_hdemo_sk', concept=ss.household_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_addr_sk', concept=ss.customer_address.address_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_store_sk', concept=ss.store.store_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_promo_sk', concept=ss.promotion.promo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ticket_number', concept=ss.ticket_number@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_quantity', concept=ss.quantity@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_wholesale_cost', concept=ss.wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_list_price', concept=ss.list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sales_price', concept=ss.sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_discount_amt', concept=ss.ext_discount_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_sales_price', concept=ss.ext_sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_wholesale_cost', concept=ss.ext_wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_list_price', concept=ss.ext_list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_tax', concept=ss.ext_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_coupon_amt', concept=ss.coupon_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid', concept=ss.net_paid@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid_inc_tax', concept=ss.net_paid_inc_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_profit', concept=ss.net_profit@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='store_sales', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'ss.ticket_number', 'ss.item.item_sk'}, where_clause=None, _str='Grain<ss.item.item_sk,ss.ticket_number>', _str_no_condition='Grain<ss.item.item_sk,ss.ticket_number>', abstract=False), namespace='ss', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ss.customer.customer_sk', 'ss.date_dim.date_sk', 'ss.time_dim.time_sk', 'ss.customer_demographics.demo_sk', 'ss.store.store_sk', 'ss.customer_address.address_sk'})}, 'ss.ext_list_price': {BuildDatasource(name='store_sales', columns=[BuildColumnAssignment(alias='ss_sold_date_sk', concept=ss.date_dim.date_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_sold_time_sk', concept=ss.time_dim.time_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_item_sk', concept=ss.item.item_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_customer_sk', concept=ss.customer.customer_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_cdemo_sk', concept=ss.customer_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_hdemo_sk', concept=ss.household_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_addr_sk', concept=ss.customer_address.address_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_store_sk', concept=ss.store.store_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_promo_sk', concept=ss.promotion.promo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ticket_number', concept=ss.ticket_number@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_quantity', concept=ss.quantity@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_wholesale_cost', concept=ss.wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_list_price', concept=ss.list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sales_price', concept=ss.sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_discount_amt', concept=ss.ext_discount_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_sales_price', concept=ss.ext_sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_wholesale_cost', concept=ss.ext_wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_list_price', concept=ss.ext_list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_tax', concept=ss.ext_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_coupon_amt', concept=ss.coupon_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid', concept=ss.net_paid@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid_inc_tax', concept=ss.net_paid_inc_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_profit', concept=ss.net_profit@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='store_sales', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'ss.ticket_number', 'ss.item.item_sk'}, where_clause=None, _str='Grain<ss.item.item_sk,ss.ticket_number>', _str_no_condition='Grain<ss.item.item_sk,ss.ticket_number>', abstract=False), namespace='ss', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ss.customer.customer_sk', 'ss.date_dim.date_sk', 'ss.time_dim.time_sk', 'ss.customer_demographics.demo_sk', 'ss.store.store_sk', 'ss.customer_address.address_sk'})}, 'ss.ext_tax': {BuildDatasource(name='store_sales', columns=[BuildColumnAssignment(alias='ss_sold_date_sk', concept=ss.date_dim.date_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_sold_time_sk', concept=ss.time_dim.time_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_item_sk', concept=ss.item.item_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_customer_sk', concept=ss.customer.customer_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_cdemo_sk', concept=ss.customer_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_hdemo_sk', concept=ss.household_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_addr_sk', concept=ss.customer_address.address_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_store_sk', concept=ss.store.store_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_promo_sk', concept=ss.promotion.promo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ticket_number', concept=ss.ticket_number@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_quantity', concept=ss.quantity@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_wholesale_cost', concept=ss.wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_list_price', concept=ss.list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sales_price', concept=ss.sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_discount_amt', concept=ss.ext_discount_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_sales_price', concept=ss.ext_sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_wholesale_cost', concept=ss.ext_wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_list_price', concept=ss.ext_list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_tax', concept=ss.ext_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_coupon_amt', concept=ss.coupon_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid', concept=ss.net_paid@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid_inc_tax', concept=ss.net_paid_inc_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_profit', concept=ss.net_profit@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='store_sales', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'ss.ticket_number', 'ss.item.item_sk'}, where_clause=None, _str='Grain<ss.item.item_sk,ss.ticket_number>', _str_no_condition='Grain<ss.item.item_sk,ss.ticket_number>', abstract=False), namespace='ss', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ss.customer.customer_sk', 'ss.date_dim.date_sk', 'ss.time_dim.time_sk', 'ss.customer_demographics.demo_sk', 'ss.store.store_sk', 'ss.customer_address.address_sk'})}, 'ss.coupon_amt': {BuildDatasource(name='store_sales', columns=[BuildColumnAssignment(alias='ss_sold_date_sk', concept=ss.date_dim.date_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_sold_time_sk', concept=ss.time_dim.time_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_item_sk', concept=ss.item.item_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_customer_sk', concept=ss.customer.customer_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_cdemo_sk', concept=ss.customer_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_hdemo_sk', concept=ss.household_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_addr_sk', concept=ss.customer_address.address_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_store_sk', concept=ss.store.store_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_promo_sk', concept=ss.promotion.promo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ticket_number', concept=ss.ticket_number@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_quantity', concept=ss.quantity@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_wholesale_cost', concept=ss.wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_list_price', concept=ss.list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sales_price', concept=ss.sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_discount_amt', concept=ss.ext_discount_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_sales_price', concept=ss.ext_sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_wholesale_cost', concept=ss.ext_wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_list_price', concept=ss.ext_list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_tax', concept=ss.ext_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_coupon_amt', concept=ss.coupon_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid', concept=ss.net_paid@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid_inc_tax', concept=ss.net_paid_inc_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_profit', concept=ss.net_profit@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='store_sales', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'ss.ticket_number', 'ss.item.item_sk'}, where_clause=None, _str='Grain<ss.item.item_sk,ss.ticket_number>', _str_no_condition='Grain<ss.item.item_sk,ss.ticket_number>', abstract=False), namespace='ss', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ss.customer.customer_sk', 'ss.date_dim.date_sk', 'ss.time_dim.time_sk', 'ss.customer_demographics.demo_sk', 'ss.store.store_sk', 'ss.customer_address.address_sk'})}, 'ss.net_paid': {BuildDatasource(name='store_sales', columns=[BuildColumnAssignment(alias='ss_sold_date_sk', concept=ss.date_dim.date_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_sold_time_sk', concept=ss.time_dim.time_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_item_sk', concept=ss.item.item_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_customer_sk', concept=ss.customer.customer_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_cdemo_sk', concept=ss.customer_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_hdemo_sk', concept=ss.household_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_addr_sk', concept=ss.customer_address.address_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_store_sk', concept=ss.store.store_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_promo_sk', concept=ss.promotion.promo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ticket_number', concept=ss.ticket_number@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_quantity', concept=ss.quantity@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_wholesale_cost', concept=ss.wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_list_price', concept=ss.list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sales_price', concept=ss.sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_discount_amt', concept=ss.ext_discount_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_sales_price', concept=ss.ext_sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_wholesale_cost', concept=ss.ext_wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_list_price', concept=ss.ext_list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_tax', concept=ss.ext_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_coupon_amt', concept=ss.coupon_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid', concept=ss.net_paid@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid_inc_tax', concept=ss.net_paid_inc_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_profit', concept=ss.net_profit@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='store_sales', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'ss.ticket_number', 'ss.item.item_sk'}, where_clause=None, _str='Grain<ss.item.item_sk,ss.ticket_number>', _str_no_condition='Grain<ss.item.item_sk,ss.ticket_number>', abstract=False), namespace='ss', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ss.customer.customer_sk', 'ss.date_dim.date_sk', 'ss.time_dim.time_sk', 'ss.customer_demographics.demo_sk', 'ss.store.store_sk', 'ss.customer_address.address_sk'})}, 'ss.net_paid_inc_tax': {BuildDatasource(name='store_sales', columns=[BuildColumnAssignment(alias='ss_sold_date_sk', concept=ss.date_dim.date_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_sold_time_sk', concept=ss.time_dim.time_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_item_sk', concept=ss.item.item_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_customer_sk', concept=ss.customer.customer_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_cdemo_sk', concept=ss.customer_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_hdemo_sk', concept=ss.household_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_addr_sk', concept=ss.customer_address.address_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_store_sk', concept=ss.store.store_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_promo_sk', concept=ss.promotion.promo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ticket_number', concept=ss.ticket_number@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_quantity', concept=ss.quantity@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_wholesale_cost', concept=ss.wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_list_price', concept=ss.list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sales_price', concept=ss.sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_discount_amt', concept=ss.ext_discount_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_sales_price', concept=ss.ext_sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_wholesale_cost', concept=ss.ext_wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_list_price', concept=ss.ext_list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_tax', concept=ss.ext_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_coupon_amt', concept=ss.coupon_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid', concept=ss.net_paid@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid_inc_tax', concept=ss.net_paid_inc_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_profit', concept=ss.net_profit@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='store_sales', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'ss.ticket_number', 'ss.item.item_sk'}, where_clause=None, _str='Grain<ss.item.item_sk,ss.ticket_number>', _str_no_condition='Grain<ss.item.item_sk,ss.ticket_number>', abstract=False), namespace='ss', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ss.customer.customer_sk', 'ss.date_dim.date_sk', 'ss.time_dim.time_sk', 'ss.customer_demographics.demo_sk', 'ss.store.store_sk', 'ss.customer_address.address_sk'})}, 'ss.net_profit': {BuildDatasource(name='store_sales', columns=[BuildColumnAssignment(alias='ss_sold_date_sk', concept=ss.date_dim.date_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_sold_time_sk', concept=ss.time_dim.time_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_item_sk', concept=ss.item.item_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_customer_sk', concept=ss.customer.customer_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_cdemo_sk', concept=ss.customer_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_hdemo_sk', concept=ss.household_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_addr_sk', concept=ss.customer_address.address_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_store_sk', concept=ss.store.store_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='ss_promo_sk', concept=ss.promotion.promo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ticket_number', concept=ss.ticket_number@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_quantity', concept=ss.quantity@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_wholesale_cost', concept=ss.wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_list_price', concept=ss.list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sales_price', concept=ss.sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_discount_amt', concept=ss.ext_discount_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_sales_price', concept=ss.ext_sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_wholesale_cost', concept=ss.ext_wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_list_price', concept=ss.ext_list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_tax', concept=ss.ext_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_coupon_amt', concept=ss.coupon_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid', concept=ss.net_paid@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid_inc_tax', concept=ss.net_paid_inc_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_profit', concept=ss.net_profit@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='store_sales', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'ss.ticket_number', 'ss.item.item_sk'}, where_clause=None, _str='Grain<ss.item.item_sk,ss.ticket_number>', _str_no_condition='Grain<ss.item.item_sk,ss.ticket_number>', abstract=False), namespace='ss', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ss.customer.customer_sk', 'ss.date_dim.date_sk', 'ss.time_dim.time_sk', 'ss.customer_demographics.demo_sk', 'ss.store.store_sk', 'ss.customer_address.address_sk'})}, 'local._virt_presence_7721818722075568': set()}
  ```
- `trilogy run answer_3210116865.preql`

  ```text
  Syntax error in answer_3210116865.preql: Missing source map entry for sr.store.store_sk with pseudonyms set(), have map: {'ss.date_dim.date_sk': {BuildDatasource(name='store_sales', columns=[BuildColumnAssignment(alias='ss_sold_date_sk', concept=ss.date_dim.date_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sold_time_sk', concept=ss.time_dim.time_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_item_sk', concept=ss.item.item_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_customer_sk', concept=ss.customer.customer_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_cdemo_sk', concept=ss.customer_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_hdemo_sk', concept=ss.household_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_addr_sk', concept=ss.customer_address.address_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_store_sk', concept=ss.store.store_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_promo_sk', concept=ss.promotion.promo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ticket_number', concept=ss.ticket_number@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_quantity', concept=ss.quantity@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_wholesale_cost', concept=ss.wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_list_price', concept=ss.list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sales_price', concept=ss.sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_discount_amt', concept=ss.ext_discount_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_sales_price', concept=ss.ext_sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_wholesale_cost', concept=ss.ext_wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_list_price', concept=ss.ext_list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_tax', concept=ss.ext_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_coupon_amt', concept=ss.coupon_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid', concept=ss.net_paid@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid_inc_tax', concept=ss.net_paid_inc_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_profit', concept=ss.net_profit@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='store_sales', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'ss.item.item_sk', 'ss.ticket_number'}, where_clause=None, _str='Grain<ss.item.item_sk,ss.ticket_number>', _str_no_condition='Grain<ss.item.item_sk,ss.ticket_number>', abstract=False), namespace='ss', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ss.date_dim.date_sk', 'ss.time_dim.time_sk', 'ss.store.store_sk', 'ss.customer_address.address_sk', 'ss.customer.customer_sk', 'ss.customer_demographics.demo_sk'})}, 'ss.time_dim.time_sk': {BuildDatasource(name='store_sales', columns=[BuildColumnAssignment(alias='ss_sold_date_sk', concept=ss.date_dim.date_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sold_time_sk', concept=ss.time_dim.time_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_item_sk', concept=ss.item.item_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_customer_sk', concept=ss.customer.customer_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_cdemo_sk', concept=ss.customer_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_hdemo_sk', concept=ss.household_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_addr_sk', concept=ss.customer_address.address_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_store_sk', concept=ss.store.store_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_promo_sk', concept=ss.promotion.promo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ticket_number', concept=ss.ticket_number@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_quantity', concept=ss.quantity@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_wholesale_cost', concept=ss.wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_list_price', concept=ss.list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sales_price', concept=ss.sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_discount_amt', concept=ss.ext_discount_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_sales_price', concept=ss.ext_sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_wholesale_cost', concept=ss.ext_wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_list_price', concept=ss.ext_list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_tax', concept=ss.ext_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_coupon_amt', concept=ss.coupon_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid', concept=ss.net_paid@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid_inc_tax', concept=ss.net_paid_inc_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_profit', concept=ss.net_profit@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='store_sales', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'ss.item.item_sk', 'ss.ticket_number'}, where_clause=None, _str='Grain<ss.item.item_sk,ss.ticket_number>', _str_no_condition='Grain<ss.item.item_sk,ss.ticket_number>', abstract=False), namespace='ss', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ss.date_dim.date_sk', 'ss.time_dim.time_sk', 'ss.store.store_sk', 'ss.customer_address.address_sk', 'ss.customer.customer_sk', 'ss.customer_demographics.demo_sk'})}, 'ss.item.item_sk': {BuildDatasource(name='store_sales', columns=[BuildColumnAssignment(alias='ss_sold_date_sk', concept=ss.date_dim.date_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sold_time_sk', concept=ss.time_dim.time_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_item_sk', concept=ss.item.item_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_customer_sk', concept=ss.customer.customer_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_cdemo_sk', concept=ss.customer_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_hdemo_sk', concept=ss.household_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_addr_sk', concept=ss.customer_address.address_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_store_sk', concept=ss.store.store_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_promo_sk', concept=ss.promotion.promo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ticket_number', concept=ss.ticket_number@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_quantity', concept=ss.quantity@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_wholesale_cost', concept=ss.wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_list_price', concept=ss.list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sales_price', concept=ss.sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_discount_amt', concept=ss.ext_discount_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_sales_price', concept=ss.ext_sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_wholesale_cost', concept=ss.ext_wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_list_price', concept=ss.ext_list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_tax', concept=ss.ext_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_coupon_amt', concept=ss.coupon_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid', concept=ss.net_paid@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid_inc_tax', concept=ss.net_paid_inc_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_profit', concept=ss.net_profit@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='store_sales', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'ss.item.item_sk', 'ss.ticket_number'}, where_clause=None, _str='Grain<ss.item.item_sk,ss.ticket_number>', _str_no_condition='Grain<ss.item.item_sk,ss.ticket_number>', abstract=False), namespace='ss', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ss.date_dim.date_sk', 'ss.time_dim.time_sk', 'ss.store.store_sk', 'ss.customer_address.address_sk', 'ss.customer.customer_sk', 'ss.customer_demographics.demo_sk'})}, 'ss.customer.customer_sk': {BuildDatasource(name='store_sales', columns=[BuildColumnAssignment(alias='ss_sold_date_sk', concept=ss.date_dim.date_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sold_time_sk', concept=ss.time_dim.time_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_item_sk', concept=ss.item.item_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_customer_sk', concept=ss.customer.customer_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_cdemo_sk', concept=ss.customer_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_hdemo_sk', concept=ss.household_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_addr_sk', concept=ss.customer_address.address_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_store_sk', concept=ss.store.store_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_promo_sk', concept=ss.promotion.promo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ticket_number', concept=ss.ticket_number@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_quantity', concept=ss.quantity@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_wholesale_cost', concept=ss.wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_list_price', concept=ss.list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sales_price', concept=ss.sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_discount_amt', concept=ss.ext_discount_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_sales_price', concept=ss.ext_sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_wholesale_cost', concept=ss.ext_wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_list_price', concept=ss.ext_list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_tax', concept=ss.ext_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_coupon_amt', concept=ss.coupon_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid', concept=ss.net_paid@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid_inc_tax', concept=ss.net_paid_inc_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_profit', concept=ss.net_profit@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='store_sales', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'ss.item.item_sk', 'ss.ticket_number'}, where_clause=None, _str='Grain<ss.item.item_sk,ss.ticket_number>', _str_no_condition='Grain<ss.item.item_sk,ss.ticket_number>', abstract=False), namespace='ss', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ss.date_dim.date_sk', 'ss.time_dim.time_sk', 'ss.store.store_sk', 'ss.customer_address.address_sk', 'ss.customer.customer_sk', 'ss.customer_demographics.demo_sk'})}, 'ss.customer_demographics.demo_sk': {BuildDatasource(name='store_sales', columns=[BuildColumnAssignment(alias='ss_sold_date_sk', concept=ss.date_dim.date_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sold_time_sk', concept=ss.time_dim.time_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_item_sk', concept=ss.item.item_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_customer_sk', concept=ss.customer.customer_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_cdemo_sk', concept=ss.customer_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_hdemo_sk', concept=ss.household_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_addr_sk', concept=ss.customer_address.address_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_store_sk', concept=ss.store.store_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_promo_sk', concept=ss.promotion.promo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ticket_number', concept=ss.ticket_number@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_quantity', concept=ss.quantity@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_wholesale_cost', concept=ss.wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_list_price', concept=ss.list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sales_price', concept=ss.sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_discount_amt', concept=ss.ext_discount_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_sales_price', concept=ss.ext_sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_wholesale_cost', concept=ss.ext_wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_list_price', concept=ss.ext_list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_tax', concept=ss.ext_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_coupon_amt', concept=ss.coupon_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid', concept=ss.net_paid@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid_inc_tax', concept=ss.net_paid_inc_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_profit', concept=ss.net_profit@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='store_sales', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'ss.item.item_sk', 'ss.ticket_number'}, where_clause=None, _str='Grain<ss.item.item_sk,ss.ticket_number>', _str_no_condition='Grain<ss.item.item_sk,ss.ticket_number>', abstract=False), namespace='ss', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ss.date_dim.date_sk', 'ss.time_dim.time_sk', 'ss.store.store_sk', 'ss.customer_address.address_sk', 'ss.customer.customer_sk', 'ss.customer_demographics.demo_sk'})}, 'ss.household_demographics.demo_sk': {BuildDatasource(name='store_sales', columns=[BuildColumnAssignment(alias='ss_sold_date_sk', concept=ss.date_dim.date_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sold_time_sk', concept=ss.time_dim.time_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_item_sk', concept=ss.item.item_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_customer_sk', concept=ss.customer.customer_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_cdemo_sk', concept=ss.customer_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_hdemo_sk', concept=ss.household_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_addr_sk', concept=ss.customer_address.address_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_store_sk', concept=ss.store.store_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_promo_sk', concept=ss.promotion.promo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ticket_number', concept=ss.ticket_number@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_quantity', concept=ss.quantity@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_wholesale_cost', concept=ss.wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_list_price', concept=ss.list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sales_price', concept=ss.sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_discount_amt', concept=ss.ext_discount_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_sales_price', concept=ss.ext_sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_wholesale_cost', concept=ss.ext_wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_list_price', concept=ss.ext_list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_tax', concept=ss.ext_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_coupon_amt', concept=ss.coupon_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid', concept=ss.net_paid@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid_inc_tax', concept=ss.net_paid_inc_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_profit', concept=ss.net_profit@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='store_sales', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'ss.item.item_sk', 'ss.ticket_number'}, where_clause=None, _str='Grain<ss.item.item_sk,ss.ticket_number>', _str_no_condition='Grain<ss.item.item_sk,ss.ticket_number>', abstract=False), namespace='ss', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ss.date_dim.date_sk', 'ss.time_dim.time_sk', 'ss.store.store_sk', 'ss.customer_address.address_sk', 'ss.customer.customer_sk', 'ss.customer_demographics.demo_sk'})}, 'ss.customer_address.address_sk': {BuildDatasource(name='store_sales', columns=[BuildColumnAssignment(alias='ss_sold_date_sk', concept=ss.date_dim.date_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sold_time_sk', concept=ss.time_dim.time_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_item_sk', concept=ss.item.item_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_customer_sk', concept=ss.customer.customer_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_cdemo_sk', concept=ss.customer_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_hdemo_sk', concept=ss.household_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_addr_sk', concept=ss.customer_address.address_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_store_sk', concept=ss.store.store_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_promo_sk', concept=ss.promotion.promo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ticket_number', concept=ss.ticket_number@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_quantity', concept=ss.quantity@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_wholesale_cost', concept=ss.wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_list_price', concept=ss.list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sales_price', concept=ss.sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_discount_amt', concept=ss.ext_discount_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_sales_price', concept=ss.ext_sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_wholesale_cost', concept=ss.ext_wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_list_price', concept=ss.ext_list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_tax', concept=ss.ext_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_coupon_amt', concept=ss.coupon_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid', concept=ss.net_paid@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid_inc_tax', concept=ss.net_paid_inc_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_profit', concept=ss.net_profit@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='store_sales', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'ss.item.item_sk', 'ss.ticket_number'}, where_clause=None, _str='Grain<ss.item.item_sk,ss.ticket_number>', _str_no_condition='Grain<ss.item.item_sk,ss.ticket_number>', abstract=False), namespace='ss', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ss.date_dim.date_sk', 'ss.time_dim.time_sk', 'ss.store.store_sk', 'ss.customer_address.address_sk', 'ss.customer.customer_sk', 'ss.customer_demographics.demo_sk'})}, 'ss.store.store_sk': {BuildDatasource(name='store_sales', columns=[BuildColumnAssignment(alias='ss_sold_date_sk', concept=ss.date_dim.date_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sold_time_sk', concept=ss.time_dim.time_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_item_sk', concept=ss.item.item_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_customer_sk', concept=ss.customer.customer_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_cdemo_sk', concept=ss.customer_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_hdemo_sk', concept=ss.household_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_addr_sk', concept=ss.customer_address.address_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_store_sk', concept=ss.store.store_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_promo_sk', concept=ss.promotion.promo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ticket_number', concept=ss.ticket_number@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_quantity', concept=ss.quantity@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_wholesale_cost', concept=ss.wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_list_price', concept=ss.list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sales_price', concept=ss.sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_discount_amt', concept=ss.ext_discount_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_sales_price', concept=ss.ext_sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_wholesale_cost', concept=ss.ext_wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_list_price', concept=ss.ext_list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_tax', concept=ss.ext_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_coupon_amt', concept=ss.coupon_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid', concept=ss.net_paid@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid_inc_tax', concept=ss.net_paid_inc_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_profit', concept=ss.net_profit@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='store_sales', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'ss.item.item_sk', 'ss.ticket_number'}, where_clause=None, _str='Grain<ss.item.item_sk,ss.ticket_number>', _str_no_condition='Grain<ss.item.item_sk,ss.ticket_number>', abstract=False), namespace='ss', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ss.date_dim.date_sk', 'ss.time_dim.time_sk', 'ss.store.store_sk', 'ss.customer_address.address_sk', 'ss.customer.customer_sk', 'ss.customer_demographics.demo_sk'})}, 'ss.promotion.promo_sk': {BuildDatasource(name='store_sales', columns=[BuildColumnAssignment(alias='ss_sold_date_sk', concept=ss.date_dim.date_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sold_time_sk', concept=ss.time_dim.time_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_item_sk', concept=ss.item.item_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_customer_sk', concept=ss.customer.customer_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_cdemo_sk', concept=ss.customer_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_hdemo_sk', concept=ss.household_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_addr_sk', concept=ss.customer_address.address_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_store_sk', concept=ss.store.store_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_promo_sk', concept=ss.promotion.promo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ticket_number', concept=ss.ticket_number@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_quantity', concept=ss.quantity@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_wholesale_cost', concept=ss.wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_list_price', concept=ss.list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sales_price', concept=ss.sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_discount_amt', concept=ss.ext_discount_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_sales_price', concept=ss.ext_sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_wholesale_cost', concept=ss.ext_wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_list_price', concept=ss.ext_list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_tax', concept=ss.ext_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_coupon_amt', concept=ss.coupon_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid', concept=ss.net_paid@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid_inc_tax', concept=ss.net_paid_inc_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_profit', concept=ss.net_profit@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='store_sales', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'ss.item.item_sk', 'ss.ticket_number'}, where_clause=None, _str='Grain<ss.item.item_sk,ss.ticket_number>', _str_no_condition='Grain<ss.item.item_sk,ss.ticket_number>', abstract=False), namespace='ss', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ss.date_dim.date_sk', 'ss.time_dim.time_sk', 'ss.store.store_sk', 'ss.customer_address.address_sk', 'ss.customer.customer_sk', 'ss.customer_demographics.demo_sk'})}, 'ss.ticket_number': {BuildDatasource(name='store_sales', columns=[BuildColumnAssignment(alias='ss_sold_date_sk', concept=ss.date_dim.date_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sold_time_sk', concept=ss.time_dim.time_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_item_sk', concept=ss.item.item_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_customer_sk', concept=ss.customer.customer_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_cdemo_sk', concept=ss.customer_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_hdemo_sk', concept=ss.household_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_addr_sk', concept=ss.customer_address.address_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_store_sk', concept=ss.store.store_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_promo_sk', concept=ss.promotion.promo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ticket_number', concept=ss.ticket_number@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_quantity', concept=ss.quantity@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_wholesale_cost', concept=ss.wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_list_price', concept=ss.list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sales_price', concept=ss.sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_discount_amt', concept=ss.ext_discount_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_sales_price', concept=ss.ext_sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_wholesale_cost', concept=ss.ext_wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_list_price', concept=ss.ext_list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_tax', concept=ss.ext_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_coupon_amt', concept=ss.coupon_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid', concept=ss.net_paid@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid_inc_tax', concept=ss.net_paid_inc_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_profit', concept=ss.net_profit@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='store_sales', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'ss.item.item_sk', 'ss.ticket_number'}, where_clause=None, _str='Grain<ss.item.item_sk,ss.ticket_number>', _str_no_condition='Grain<ss.item.item_sk,ss.ticket_number>', abstract=False), namespace='ss', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ss.date_dim.date_sk', 'ss.time_dim.time_sk', 'ss.store.store_sk', 'ss.customer_address.address_sk', 'ss.customer.customer_sk', 'ss.customer_demographics.demo_sk'})}, 'ss.quantity': {BuildDatasource(name='store_sales', columns=[BuildColumnAssignment(alias='ss_sold_date_sk', concept=ss.date_dim.date_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sold_time_sk', concept=ss.time_dim.time_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_item_sk', concept=ss.item.item_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_customer_sk', concept=ss.customer.customer_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_cdemo_sk', concept=ss.customer_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_hdemo_sk', concept=ss.household_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_addr_sk', concept=ss.customer_address.address_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_store_sk', concept=ss.store.store_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_promo_sk', concept=ss.promotion.promo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ticket_number', concept=ss.ticket_number@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_quantity', concept=ss.quantity@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_wholesale_cost', concept=ss.wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_list_price', concept=ss.list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sales_price', concept=ss.sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_discount_amt', concept=ss.ext_discount_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_sales_price', concept=ss.ext_sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_wholesale_cost', concept=ss.ext_wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_list_price', concept=ss.ext_list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_tax', concept=ss.ext_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_coupon_amt', concept=ss.coupon_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid', concept=ss.net_paid@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid_inc_tax', concept=ss.net_paid_inc_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_profit', concept=ss.net_profit@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='store_sales', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'ss.item.item_sk', 'ss.ticket_number'}, where_clause=None, _str='Grain<ss.item.item_sk,ss.ticket_number>', _str_no_condition='Grain<ss.item.item_sk,ss.ticket_number>', abstract=False), namespace='ss', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ss.date_dim.date_sk', 'ss.time_dim.time_sk', 'ss.store.store_sk', 'ss.customer_address.address_sk', 'ss.customer.customer_sk', 'ss.customer_demographics.demo_sk'})}, 'ss.wholesale_cost': {BuildDatasource(name='store_sales', columns=[BuildColumnAssignment(alias='ss_sold_date_sk', concept=ss.date_dim.date_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sold_time_sk', concept=ss.time_dim.time_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_item_sk', concept=ss.item.item_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_customer_sk', concept=ss.customer.customer_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_cdemo_sk', concept=ss.customer_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_hdemo_sk', concept=ss.household_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_addr_sk', concept=ss.customer_address.address_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_store_sk', concept=ss.store.store_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_promo_sk', concept=ss.promotion.promo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ticket_number', concept=ss.ticket_number@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_quantity', concept=ss.quantity@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_wholesale_cost', concept=ss.wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_list_price', concept=ss.list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sales_price', concept=ss.sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_discount_amt', concept=ss.ext_discount_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_sales_price', concept=ss.ext_sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_wholesale_cost', concept=ss.ext_wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_list_price', concept=ss.ext_list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_tax', concept=ss.ext_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_coupon_amt', concept=ss.coupon_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid', concept=ss.net_paid@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid_inc_tax', concept=ss.net_paid_inc_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_profit', concept=ss.net_profit@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='store_sales', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'ss.item.item_sk', 'ss.ticket_number'}, where_clause=None, _str='Grain<ss.item.item_sk,ss.ticket_number>', _str_no_condition='Grain<ss.item.item_sk,ss.ticket_number>', abstract=False), namespace='ss', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ss.date_dim.date_sk', 'ss.time_dim.time_sk', 'ss.store.store_sk', 'ss.customer_address.address_sk', 'ss.customer.customer_sk', 'ss.customer_demographics.demo_sk'})}, 'ss.list_price': {BuildDatasource(name='store_sales', columns=[BuildColumnAssignment(alias='ss_sold_date_sk', concept=ss.date_dim.date_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sold_time_sk', concept=ss.time_dim.time_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_item_sk', concept=ss.item.item_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_customer_sk', concept=ss.customer.customer_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_cdemo_sk', concept=ss.customer_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_hdemo_sk', concept=ss.household_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_addr_sk', concept=ss.customer_address.address_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_store_sk', concept=ss.store.store_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_promo_sk', concept=ss.promotion.promo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ticket_number', concept=ss.ticket_number@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_quantity', concept=ss.quantity@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_wholesale_cost', concept=ss.wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_list_price', concept=ss.list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sales_price', concept=ss.sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_discount_amt', concept=ss.ext_discount_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_sales_price', concept=ss.ext_sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_wholesale_cost', concept=ss.ext_wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_list_price', concept=ss.ext_list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_tax', concept=ss.ext_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_coupon_amt', concept=ss.coupon_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid', concept=ss.net_paid@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid_inc_tax', concept=ss.net_paid_inc_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_profit', concept=ss.net_profit@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='store_sales', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'ss.item.item_sk', 'ss.ticket_number'}, where_clause=None, _str='Grain<ss.item.item_sk,ss.ticket_number>', _str_no_condition='Grain<ss.item.item_sk,ss.ticket_number>', abstract=False), namespace='ss', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ss.date_dim.date_sk', 'ss.time_dim.time_sk', 'ss.store.store_sk', 'ss.customer_address.address_sk', 'ss.customer.customer_sk', 'ss.customer_demographics.demo_sk'})}, 'ss.sales_price': {BuildDatasource(name='store_sales', columns=[BuildColumnAssignment(alias='ss_sold_date_sk', concept=ss.date_dim.date_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sold_time_sk', concept=ss.time_dim.time_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_item_sk', concept=ss.item.item_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_customer_sk', concept=ss.customer.customer_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_cdemo_sk', concept=ss.customer_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_hdemo_sk', concept=ss.household_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_addr_sk', concept=ss.customer_address.address_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_store_sk', concept=ss.store.store_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_promo_sk', concept=ss.promotion.promo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ticket_number', concept=ss.ticket_number@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_quantity', concept=ss.quantity@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_wholesale_cost', concept=ss.wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_list_price', concept=ss.list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sales_price', concept=ss.sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_discount_amt', concept=ss.ext_discount_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_sales_price', concept=ss.ext_sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_wholesale_cost', concept=ss.ext_wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_list_price', concept=ss.ext_list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_tax', concept=ss.ext_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_coupon_amt', concept=ss.coupon_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid', concept=ss.net_paid@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid_inc_tax', concept=ss.net_paid_inc_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_profit', concept=ss.net_profit@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='store_sales', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'ss.item.item_sk', 'ss.ticket_number'}, where_clause=None, _str='Grain<ss.item.item_sk,ss.ticket_number>', _str_no_condition='Grain<ss.item.item_sk,ss.ticket_number>', abstract=False), namespace='ss', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ss.date_dim.date_sk', 'ss.time_dim.time_sk', 'ss.store.store_sk', 'ss.customer_address.address_sk', 'ss.customer.customer_sk', 'ss.customer_demographics.demo_sk'})}, 'ss.ext_discount_amt': {BuildDatasource(name='store_sales', columns=[BuildColumnAssignment(alias='ss_sold_date_sk', concept=ss.date_dim.date_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sold_time_sk', concept=ss.time_dim.time_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_item_sk', concept=ss.item.item_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_customer_sk', concept=ss.customer.customer_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_cdemo_sk', concept=ss.customer_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_hdemo_sk', concept=ss.household_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_addr_sk', concept=ss.customer_address.address_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_store_sk', concept=ss.store.store_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_promo_sk', concept=ss.promotion.promo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ticket_number', concept=ss.ticket_number@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_quantity', concept=ss.quantity@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_wholesale_cost', concept=ss.wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_list_price', concept=ss.list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sales_price', concept=ss.sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_discount_amt', concept=ss.ext_discount_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_sales_price', concept=ss.ext_sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_wholesale_cost', concept=ss.ext_wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_list_price', concept=ss.ext_list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_tax', concept=ss.ext_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_coupon_amt', concept=ss.coupon_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid', concept=ss.net_paid@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid_inc_tax', concept=ss.net_paid_inc_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_profit', concept=ss.net_profit@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='store_sales', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'ss.item.item_sk', 'ss.ticket_number'}, where_clause=None, _str='Grain<ss.item.item_sk,ss.ticket_number>', _str_no_condition='Grain<ss.item.item_sk,ss.ticket_number>', abstract=False), namespace='ss', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ss.date_dim.date_sk', 'ss.time_dim.time_sk', 'ss.store.store_sk', 'ss.customer_address.address_sk', 'ss.customer.customer_sk', 'ss.customer_demographics.demo_sk'})}, 'ss.ext_sales_price': {BuildDatasource(name='store_sales', columns=[BuildColumnAssignment(alias='ss_sold_date_sk', concept=ss.date_dim.date_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sold_time_sk', concept=ss.time_dim.time_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_item_sk', concept=ss.item.item_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_customer_sk', concept=ss.customer.customer_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_cdemo_sk', concept=ss.customer_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_hdemo_sk', concept=ss.household_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_addr_sk', concept=ss.customer_address.address_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_store_sk', concept=ss.store.store_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_promo_sk', concept=ss.promotion.promo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ticket_number', concept=ss.ticket_number@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_quantity', concept=ss.quantity@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_wholesale_cost', concept=ss.wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_list_price', concept=ss.list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sales_price', concept=ss.sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_discount_amt', concept=ss.ext_discount_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_sales_price', concept=ss.ext_sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_wholesale_cost', concept=ss.ext_wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_list_price', concept=ss.ext_list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_tax', concept=ss.ext_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_coupon_amt', concept=ss.coupon_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid', concept=ss.net_paid@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid_inc_tax', concept=ss.net_paid_inc_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_profit', concept=ss.net_profit@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='store_sales', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'ss.item.item_sk', 'ss.ticket_number'}, where_clause=None, _str='Grain<ss.item.item_sk,ss.ticket_number>', _str_no_condition='Grain<ss.item.item_sk,ss.ticket_number>', abstract=False), namespace='ss', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ss.date_dim.date_sk', 'ss.time_dim.time_sk', 'ss.store.store_sk', 'ss.customer_address.address_sk', 'ss.customer.customer_sk', 'ss.customer_demographics.demo_sk'})}, 'ss.ext_wholesale_cost': {BuildDatasource(name='store_sales', columns=[BuildColumnAssignment(alias='ss_sold_date_sk', concept=ss.date_dim.date_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sold_time_sk', concept=ss.time_dim.time_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_item_sk', concept=ss.item.item_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_customer_sk', concept=ss.customer.customer_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_cdemo_sk', concept=ss.customer_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_hdemo_sk', concept=ss.household_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_addr_sk', concept=ss.customer_address.address_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_store_sk', concept=ss.store.store_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_promo_sk', concept=ss.promotion.promo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ticket_number', concept=ss.ticket_number@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_quantity', concept=ss.quantity@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_wholesale_cost', concept=ss.wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_list_price', concept=ss.list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sales_price', concept=ss.sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_discount_amt', concept=ss.ext_discount_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_sales_price', concept=ss.ext_sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_wholesale_cost', concept=ss.ext_wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_list_price', concept=ss.ext_list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_tax', concept=ss.ext_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_coupon_amt', concept=ss.coupon_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid', concept=ss.net_paid@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid_inc_tax', concept=ss.net_paid_inc_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_profit', concept=ss.net_profit@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='store_sales', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'ss.item.item_sk', 'ss.ticket_number'}, where_clause=None, _str='Grain<ss.item.item_sk,ss.ticket_number>', _str_no_condition='Grain<ss.item.item_sk,ss.ticket_number>', abstract=False), namespace='ss', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ss.date_dim.date_sk', 'ss.time_dim.time_sk', 'ss.store.store_sk', 'ss.customer_address.address_sk', 'ss.customer.customer_sk', 'ss.customer_demographics.demo_sk'})}, 'ss.ext_list_price': {BuildDatasource(name='store_sales', columns=[BuildColumnAssignment(alias='ss_sold_date_sk', concept=ss.date_dim.date_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sold_time_sk', concept=ss.time_dim.time_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_item_sk', concept=ss.item.item_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_customer_sk', concept=ss.customer.customer_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_cdemo_sk', concept=ss.customer_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_hdemo_sk', concept=ss.household_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_addr_sk', concept=ss.customer_address.address_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_store_sk', concept=ss.store.store_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_promo_sk', concept=ss.promotion.promo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ticket_number', concept=ss.ticket_number@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_quantity', concept=ss.quantity@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_wholesale_cost', concept=ss.wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_list_price', concept=ss.list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sales_price', concept=ss.sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_discount_amt', concept=ss.ext_discount_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_sales_price', concept=ss.ext_sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_wholesale_cost', concept=ss.ext_wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_list_price', concept=ss.ext_list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_tax', concept=ss.ext_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_coupon_amt', concept=ss.coupon_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid', concept=ss.net_paid@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid_inc_tax', concept=ss.net_paid_inc_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_profit', concept=ss.net_profit@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='store_sales', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'ss.item.item_sk', 'ss.ticket_number'}, where_clause=None, _str='Grain<ss.item.item_sk,ss.ticket_number>', _str_no_condition='Grain<ss.item.item_sk,ss.ticket_number>', abstract=False), namespace='ss', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ss.date_dim.date_sk', 'ss.time_dim.time_sk', 'ss.store.store_sk', 'ss.customer_address.address_sk', 'ss.customer.customer_sk', 'ss.customer_demographics.demo_sk'})}, 'ss.ext_tax': {BuildDatasource(name='store_sales', columns=[BuildColumnAssignment(alias='ss_sold_date_sk', concept=ss.date_dim.date_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sold_time_sk', concept=ss.time_dim.time_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_item_sk', concept=ss.item.item_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_customer_sk', concept=ss.customer.customer_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_cdemo_sk', concept=ss.customer_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_hdemo_sk', concept=ss.household_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_addr_sk', concept=ss.customer_address.address_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_store_sk', concept=ss.store.store_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_promo_sk', concept=ss.promotion.promo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ticket_number', concept=ss.ticket_number@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_quantity', concept=ss.quantity@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_wholesale_cost', concept=ss.wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_list_price', concept=ss.list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sales_price', concept=ss.sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_discount_amt', concept=ss.ext_discount_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_sales_price', concept=ss.ext_sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_wholesale_cost', concept=ss.ext_wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_list_price', concept=ss.ext_list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_tax', concept=ss.ext_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_coupon_amt', concept=ss.coupon_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid', concept=ss.net_paid@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid_inc_tax', concept=ss.net_paid_inc_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_profit', concept=ss.net_profit@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='store_sales', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'ss.item.item_sk', 'ss.ticket_number'}, where_clause=None, _str='Grain<ss.item.item_sk,ss.ticket_number>', _str_no_condition='Grain<ss.item.item_sk,ss.ticket_number>', abstract=False), namespace='ss', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ss.date_dim.date_sk', 'ss.time_dim.time_sk', 'ss.store.store_sk', 'ss.customer_address.address_sk', 'ss.customer.customer_sk', 'ss.customer_demographics.demo_sk'})}, 'ss.coupon_amt': {BuildDatasource(name='store_sales', columns=[BuildColumnAssignment(alias='ss_sold_date_sk', concept=ss.date_dim.date_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sold_time_sk', concept=ss.time_dim.time_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_item_sk', concept=ss.item.item_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_customer_sk', concept=ss.customer.customer_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_cdemo_sk', concept=ss.customer_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_hdemo_sk', concept=ss.household_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_addr_sk', concept=ss.customer_address.address_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_store_sk', concept=ss.store.store_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_promo_sk', concept=ss.promotion.promo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ticket_number', concept=ss.ticket_number@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_quantity', concept=ss.quantity@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_wholesale_cost', concept=ss.wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_list_price', concept=ss.list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sales_price', concept=ss.sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_discount_amt', concept=ss.ext_discount_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_sales_price', concept=ss.ext_sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_wholesale_cost', concept=ss.ext_wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_list_price', concept=ss.ext_list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_tax', concept=ss.ext_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_coupon_amt', concept=ss.coupon_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid', concept=ss.net_paid@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid_inc_tax', concept=ss.net_paid_inc_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_profit', concept=ss.net_profit@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='store_sales', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'ss.item.item_sk', 'ss.ticket_number'}, where_clause=None, _str='Grain<ss.item.item_sk,ss.ticket_number>', _str_no_condition='Grain<ss.item.item_sk,ss.ticket_number>', abstract=False), namespace='ss', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ss.date_dim.date_sk', 'ss.time_dim.time_sk', 'ss.store.store_sk', 'ss.customer_address.address_sk', 'ss.customer.customer_sk', 'ss.customer_demographics.demo_sk'})}, 'ss.net_paid': {BuildDatasource(name='store_sales', columns=[BuildColumnAssignment(alias='ss_sold_date_sk', concept=ss.date_dim.date_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sold_time_sk', concept=ss.time_dim.time_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_item_sk', concept=ss.item.item_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_customer_sk', concept=ss.customer.customer_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_cdemo_sk', concept=ss.customer_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_hdemo_sk', concept=ss.household_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_addr_sk', concept=ss.customer_address.address_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_store_sk', concept=ss.store.store_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_promo_sk', concept=ss.promotion.promo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ticket_number', concept=ss.ticket_number@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_quantity', concept=ss.quantity@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_wholesale_cost', concept=ss.wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_list_price', concept=ss.list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sales_price', concept=ss.sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_discount_amt', concept=ss.ext_discount_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_sales_price', concept=ss.ext_sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_wholesale_cost', concept=ss.ext_wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_list_price', concept=ss.ext_list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_tax', concept=ss.ext_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_coupon_amt', concept=ss.coupon_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid', concept=ss.net_paid@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid_inc_tax', concept=ss.net_paid_inc_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_profit', concept=ss.net_profit@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='store_sales', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'ss.item.item_sk', 'ss.ticket_number'}, where_clause=None, _str='Grain<ss.item.item_sk,ss.ticket_number>', _str_no_condition='Grain<ss.item.item_sk,ss.ticket_number>', abstract=False), namespace='ss', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ss.date_dim.date_sk', 'ss.time_dim.time_sk', 'ss.store.store_sk', 'ss.customer_address.address_sk', 'ss.customer.customer_sk', 'ss.customer_demographics.demo_sk'})}, 'ss.net_paid_inc_tax': {BuildDatasource(name='store_sales', columns=[BuildColumnAssignment(alias='ss_sold_date_sk', concept=ss.date_dim.date_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sold_time_sk', concept=ss.time_dim.time_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_item_sk', concept=ss.item.item_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_customer_sk', concept=ss.customer.customer_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_cdemo_sk', concept=ss.customer_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_hdemo_sk', concept=ss.household_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_addr_sk', concept=ss.customer_address.address_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_store_sk', concept=ss.store.store_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_promo_sk', concept=ss.promotion.promo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ticket_number', concept=ss.ticket_number@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_quantity', concept=ss.quantity@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_wholesale_cost', concept=ss.wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_list_price', concept=ss.list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sales_price', concept=ss.sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_discount_amt', concept=ss.ext_discount_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_sales_price', concept=ss.ext_sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_wholesale_cost', concept=ss.ext_wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_list_price', concept=ss.ext_list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_tax', concept=ss.ext_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_coupon_amt', concept=ss.coupon_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid', concept=ss.net_paid@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid_inc_tax', concept=ss.net_paid_inc_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_profit', concept=ss.net_profit@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='store_sales', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'ss.item.item_sk', 'ss.ticket_number'}, where_clause=None, _str='Grain<ss.item.item_sk,ss.ticket_number>', _str_no_condition='Grain<ss.item.item_sk,ss.ticket_number>', abstract=False), namespace='ss', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ss.date_dim.date_sk', 'ss.time_dim.time_sk', 'ss.store.store_sk', 'ss.customer_address.address_sk', 'ss.customer.customer_sk', 'ss.customer_demographics.demo_sk'})}, 'ss.net_profit': {BuildDatasource(name='store_sales', columns=[BuildColumnAssignment(alias='ss_sold_date_sk', concept=ss.date_dim.date_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sold_time_sk', concept=ss.time_dim.time_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_item_sk', concept=ss.item.item_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_customer_sk', concept=ss.customer.customer_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_cdemo_sk', concept=ss.customer_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_hdemo_sk', concept=ss.household_demographics.demo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_addr_sk', concept=ss.customer_address.address_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_store_sk', concept=ss.store.store_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.PARTIAL: 'Partial'>, <Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_promo_sk', concept=ss.promotion.promo_sk@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ticket_number', concept=ss.ticket_number@Grain<ss.item.item_sk,ss.ticket_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='ss_quantity', concept=ss.quantity@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_wholesale_cost', concept=ss.wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_list_price', concept=ss.list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_sales_price', concept=ss.sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_discount_amt', concept=ss.ext_discount_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_sales_price', concept=ss.ext_sales_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_wholesale_cost', concept=ss.ext_wholesale_cost@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_list_price', concept=ss.ext_list_price@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_ext_tax', concept=ss.ext_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_coupon_amt', concept=ss.coupon_amt@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid', concept=ss.net_paid@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_paid_inc_tax', concept=ss.net_paid_inc_tax@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='ss_net_profit', concept=ss.net_profit@Grain<ss.item.item_sk,ss.ticket_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='store_sales', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'ss.item.item_sk', 'ss.ticket_number'}, where_clause=None, _str='Grain<ss.item.item_sk,ss.ticket_number>', _str_no_condition='Grain<ss.item.item_sk,ss.ticket_number>', abstract=False), namespace='ss', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ss.date_dim.date_sk', 'ss.time_dim.time_sk', 'ss.store.store_sk', 'ss.customer_address.address_sk', 'ss.customer.customer_sk', 'ss.customer_demographics.demo_sk'})}, 'local._virt_presence_7721818722075568': set()}
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw/store_sales:ss select ss.net_profit, ss.ext_sales_price, ss.date_dim.date, ss.item.current_price, ss.promotion.channel_tv where ss.d…'2000-08-23'::date and '2000-09-22'::date and ss.item.current_price > 50 and ss.promotion.channel_tv = 'N' and ss.store.store_id is not null limit 5;`

  ```text
  Syntax error in stdin: Comparison `ss.promotion.channel_tv = 'N'` matches every value of nullable enum field 'ss.promotion.channel_tv', which contains only these values: 'N'. It only excludes nulls; simplify it to 'ss.promotion.channel_tv is not null'.
  ```
- `trilogy run answer_943796012.preql`

  ```text
  Unexpected error in answer_943796012.preql: Missing cs.order_number in {'cs.catalog_page.catalog_page_id': ['vacuous'], 'cs.catalog_page.catalog_page_sk': ['vacuous'], 'cs.item.current_price': ['vacuous'], 'cs.item.item_sk': ['thoughtful', 'vacuous'], 'cs.promotion.channel_tv': ['vacuous'], 'cs.promotion.promo_sk': ['vacuous'], 'cs.sold_date.date': ['vacuous'], 'cs.sold_date.date_sk': ['vacuous'], 'cr.item.item_sk': ['thoughtful', 'vacuous'], 'cr.order_number': ['thoughtful'], 'cr.return_amount': ['thoughtful'], 'local.___tvf_arm_1_outlet': []}, source map dict_keys(['cs.catalog_page.catalog_page_id', 'cs.catalog_page.catalog_page_sk', 'cs.item.current_price', 'cs.item.item_sk', 'cs.promotion.channel_tv', 'cs.promotion.promo_sk', 'cs.sold_date.date', 'cs.sold_date.date_sk', 'cr.item.item_sk', 'cr.order_number', 'cr.return_amount', 'local.___tvf_arm_1_outlet'])
  ```
- `trilogy file read answer_943796012.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe store_returns`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy database describe catalog_returns`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy database describe web_returns`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read answer_2869182220.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.web_sales:ws --import raw.web_returns:wr select count(ws.order_number) union join wr.item.item_sk = ws.item.item_sk union join wr.order_number = ws.order_number where ws.sold_date.year = 2000 and wr.order_number is not null limit 10;`

  ```text
  Unexpected error in stdin: (_duckdb.BinderException) Binder Error: Ambiguous reference to table "wr_web_returns" (duplicate alias "wr_web_returns", explicitly alias one of the tables using "AS my_alias")
  [SQL:
  WITH
  quizzical as (
  SELECT
      coalesce("wr_web_returns"."wr_order_number") as "_virt_presence_3838022047150424"
  FROM
      "web_returns" as "wr_web_returns"
  WHERE
      coalesce("wr_web_returns"."wr_order_number") is not null

  GROUP BY
      1,
      "wr_web_returns"."wr_order_number"),
  thoughtful as (
  SELECT
      coalesce("wr_web_returns"."wr_order_number","wr_web_returns"."wr_order_number","ws_web_sales"."ws_order_number") as "ws_order_number"
  FROM
      "web_sales" as "ws_web_sales"
      INNER JOIN "web_returns" as "wr_web_returns" on "ws_web_sales"."ws_item_sk" = "wr_web_returns"."wr_item_sk" AND "ws_web_sales"."ws_order_number" = "wr_web_returns"."wr_order_number"
      INNER JOIN "date_dim" as "ws_sold_date_date_dim" on "ws_web_sales"."ws_sold_date_sk" = "ws_sold_date_date_dim"."d_date_sk"
      INNER JOIN "web_returns" as "wr_web_returns" on "wr_web_returns"."wr_order_number" = "wr_web_returns"."wr_order_number"
  WHERE
      "ws_sold_date_date_dim"."d_year" = 2000

  GROUP BY
      1)
  SELECT
      count("thoughtful"."ws_order_number") as "order_number_count"
  FROM
      "thoughtful"
  LIMIT (10)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run --import raw.web_sales:ws --import raw.web_returns:wr select count(ws.order_number) where ws.sold_date.year = 2000 and wr.item.item_sk = ws.item.item_sk and wr.order_number = ws.order_number limit 10;`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {wr.item.item_sk, wr.order_number}; {ws.item.item_sk, ws.order_number, ws.sold_date.year}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.web_sales:ws --import raw.web_returns:wr select count(ws.order_number) subset join wr.item.item_sk = ws.item.item_sk subset join wr.…_number where ws.sold_date.year = 2000 and wr.order_number is not null and wr.returning_customer.customer_demographics.marital_status = 'M' limit 10;`

  ```text
  Unexpected error in stdin: (_duckdb.BinderException) Binder Error: Ambiguous reference to table "wr_web_returns" (duplicate alias "wr_web_returns", explicitly alias one of the tables using "AS my_alias")
  [SQL:
  WITH
  wakeful as (
  SELECT
      coalesce("wr_web_returns"."wr_order_number") as "_virt_presence_3838022047150424"
  FROM
      "web_returns" as "wr_web_returns"
  WHERE
      coalesce("wr_web_returns"."wr_order_number") is not null

  GROUP BY
      1,
      "wr_web_returns"."wr_order_number"),
  cooperative as (
  SELECT
      coalesce("wr_web_returns"."wr_order_number","wr_web_returns"."wr_order_number","ws_web_sales"."ws_order_number") as "ws_order_number"
  FROM
      "web_sales" as "ws_web_sales"
      INNER JOIN "web_returns" as "wr_web_returns" on "ws_web_sales"."ws_item_sk" = "wr_web_returns"."wr_item_sk" AND "ws_web_sales"."ws_order_number" = "wr_web_returns"."wr_order_number"
      INNER JOIN "date_dim" as "ws_sold_date_date_dim" on "ws_web_sales"."ws_sold_date_sk" = "ws_sold_date_date_dim"."d_date_sk"
      INNER JOIN "customer" as "wr_returning_customer_customer" on "wr_web_returns"."wr_returning_customer_sk" = "wr_returning_customer_customer"."c_customer_sk"
      INNER JOIN "customer_demographics" as "wr_returning_customer_customer_demographics_customer_demographics" on "wr_returning_customer_customer"."c_current_cdemo_sk" = "wr_returning_customer_customer_demographics_customer_demographics"."cd_demo_sk"
      INNER JOIN "web_returns" as "wr_web_returns" on "wr_web_returns"."wr_order_number" = "wr_web_returns"."wr_order_number"
  WHERE
      "ws_sold_date_date_dim"."d_year" = 2000 and "wr_returning_customer_customer_demographics_customer_demographics"."cd_marital_status" = 'M'

  GROUP BY
      1)
  SELECT
      count("cooperative"."ws_order_number") as "order_number_count"
  FROM
      "cooperative"
  LIMIT (10)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy file read answer_927797426.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.web_sales:ws --import raw.web_returns:wr select count(ws.order_number) subset join wr.item.item_sk = ws.item.item_sk subset join wr.…_number where ws.sold_date.year = 2000 and wr.order_number is not null and wr.returning_customer.customer_demographics.marital_status = 'M' limit 10;`

  ```text
  Unexpected error in stdin: (_duckdb.BinderException) Binder Error: Ambiguous reference to table "wr_web_returns" (duplicate alias "wr_web_returns", explicitly alias one of the tables using "AS my_alias")
  [SQL:
  WITH
  wakeful as (
  SELECT
      coalesce("wr_web_returns"."wr_order_number") as "_virt_presence_3838022047150424"
  FROM
      "web_returns" as "wr_web_returns"
  WHERE
      coalesce("wr_web_returns"."wr_order_number") is not null

  GROUP BY
      1,
      "wr_web_returns"."wr_order_number"),
  cooperative as (
  SELECT
      coalesce("wr_web_returns"."wr_order_number","wr_web_returns"."wr_order_number","ws_web_sales"."ws_order_number") as "ws_order_number"
  FROM
      "web_sales" as "ws_web_sales"
      INNER JOIN "web_returns" as "wr_web_returns" on "ws_web_sales"."ws_item_sk" = "wr_web_returns"."wr_item_sk" AND "ws_web_sales"."ws_order_number" = "wr_web_returns"."wr_order_number"
      INNER JOIN "date_dim" as "ws_sold_date_date_dim" on "ws_web_sales"."ws_sold_date_sk" = "ws_sold_date_date_dim"."d_date_sk"
      INNER JOIN "customer" as "wr_returning_customer_customer" on "wr_web_returns"."wr_returning_customer_sk" = "wr_returning_customer_customer"."c_customer_sk"
      INNER JOIN "customer_demographics" as "wr_returning_customer_customer_demographics_customer_demographics" on "wr_returning_customer_customer"."c_current_cdemo_sk" = "wr_returning_customer_customer_demographics_customer_demographics"."cd_demo_sk"
      INNER JOIN "web_returns" as "wr_web_returns" on "wr_web_returns"."wr_order_number" = "wr_web_returns"."wr_order_number"
  WHERE
      "ws_sold_date_date_dim"."d_year" = 2000 and "wr_returning_customer_customer_demographics_customer_demographics"."cd_marital_status" = 'M'

  GROUP BY
      1)
  SELECT
      count("cooperative"."ws_order_number") as "order_number_count"
  FROM
      "cooperative"
  LIMIT (10)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy file read raw/store_sales.preql`

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
- `trilogy run answer_32504671.preql`

  ```text
  Syntax error in answer_32504671.preql: Comparison `ss.household_demographics.vehicle_count <= 6` matches every value of enum field 'ss.household_demographics.vehicle_count', which contains only these values: -1, 0, 1, 2, 3, 4. It is always true and should be removed.
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
- `trilogy run --import raw.store_sales:ss --import raw.store_returns:sr select ss.customer.customer_sk, count(ss.ticket_number) as cnt union join ss.item.item_sk = sr.item.item_sk union join ss.ticket_number = sr.ticket_number where sr.reason.desc = 'reason 28' and sr.ticket_number is not null limit 5;`

  ```text
  Unexpected error in stdin: (_duckdb.BinderException) Binder Error: Ambiguous reference to table "sr_store_returns" (duplicate alias "sr_store_returns", explicitly alias one of the tables using "AS my_alias")
  [SQL:
  WITH
  highfalutin as (
  SELECT
      coalesce("sr_store_returns"."sr_ticket_number") as "_virt_presence_458502113429362"
  FROM
      "store_returns" as "sr_store_returns"
  WHERE
      coalesce("sr_store_returns"."sr_ticket_number") is not null

  GROUP BY
      1,
      "sr_store_returns"."sr_ticket_number"),
  thoughtful as (
  SELECT
      coalesce("sr_store_returns"."sr_ticket_number","sr_store_returns"."sr_ticket_number","ss_store_sales"."ss_ticket_number") as "sr_ticket_number",
      coalesce("ss_customer_customer"."c_customer_sk","ss_store_sales"."ss_customer_sk") as "ss_customer_customer_sk"
  FROM
      "store_sales" as "ss_store_sales"
      RIGHT OUTER JOIN "store_returns" as "sr_store_returns" on "ss_store_sales"."ss_item_sk" = "sr_store_returns"."sr_item_sk" AND "ss_store_sales"."ss_ticket_number" = "sr_store_returns"."sr_ticket_number"
      INNER JOIN "reason" as "sr_reason_reason" on "sr_store_returns"."sr_reason_sk" = "sr_reason_reason"."r_reason_sk"
      INNER JOIN "store_returns" as "sr_store_returns" on "sr_store_returns"."sr_ticket_number" = "sr_store_returns"."sr_ticket_number"
      LEFT OUTER JOIN "customer" as "ss_customer_customer" on "ss_store_sales"."ss_customer_sk" = "ss_customer_customer"."c_customer_sk"
  WHERE
      "sr_reason_reason"."r_reason_desc" = 'reason 28'

  GROUP BY
      1,
      2)
  SELECT
      "thoughtful"."ss_customer_customer_sk" as "ss_customer_customer_sk",
      count("thoughtful"."sr_ticket_number") as "cnt"
  FROM
      "thoughtful"
  GROUP BY
      1
  LIMIT (5)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy file read test_debug.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run test_debug25.preql`

  ```text
  Syntax error in test_debug25.preql: Composite membership requires both tuples to have the same number of elements; got 2 and 3.
  ```
- `trilogy run answer_3281742578.preql`

  ```text
  Syntax error in answer_3281742578.preql: Undefined concept: warehouse.warehouse_sk. Suggestions: ['ws.warehouse.warehouse_sk', 'ws.warehouse.warehouse_id', 'ws.warehouse.warehouse_sq_ft']
  ```
- `trilogy run answer_3281742578.preql`

  ```text
  Resolution error in answer_3281742578.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 8). The requested concepts split into 2 disconnected subgraphs: {distinct_wh_count, ws.ship_addr.state, ws.ship_date.date, ws.web_site.company_name}; {return_count}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_3281742578.preql`

  ```text
  Resolution error in answer_3281742578.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 10). The requested concepts split into 2 disconnected subgraphs: {ret_count}; {wh_count, ws.order_number}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_2333598260.preql`

  ```text
  Syntax error in answer_2333598260.preql: Undefined concept: local.store_sales (line 9, in SELECT). Suggestions: ['store_sales.ext_tax', 'store_sales.quantity', 'store_sales.net_paid']
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy file write answer_3863442186.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales a…ils.customer_id asc, customer_details.first_name asc, customer_details.last_name asc, customer_details.preferred_cust_flag asc nulls first
limit 100;`

  ```text
  refused to write 'answer_3863442186.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...tore_pivot.customer_sk select ??? distinct store_sales.customer....

  Write stats: received 3224 chars / 3224 bytes; tail: …'tomer_details.preferred_cust_flag asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write answer_3705756794.preql --content import raw.web_sales as ws;
import raw.web_returns as wr;

with ws_entity as
where ws.sold_date.date bet….net_loss) as net_loss
by *
;

select
    coalesce(ws_entity.site_id, 'none') as site_id
union join ws_entity.site_id = wr_entity.net_loss
limit 10;
`

  ```text
  refused to write 'answer_3705756794.preql': not syntactically valid Trilogy.

  Parse error:
    --> 20:1
     |
  20 | by *
     | ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
   sum(wr.net_loss) as net_loss ??? by * ;  select     coalesce(ws...

  Write stats: received 700 chars / 700 bytes; tail: …'union join ws_entity.site_id = wr_entity.net_loss\\nlimit 10;\\n'.
  ```
- `trilogy file write answer_3705756794.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw…t
    combined.ch,
    combined.eid,
    combined.sales,
    combined.ret,
    combined.net
order by combined.ch, combined.eid nulls first
limit 10;
`

  ```text
  refused to write 'answer_3705756794.preql': not syntactically valid Trilogy.

  Parse error:
   --> 9:6
    |
  9 |     (import raw.store_sales as ss;
    |      ^---
    |
    = expected select_statement
  Location:
       # Store channel arm     ( ??? import raw.store_sales as ss;

  Write stats: received 1457 chars / 1457 bytes; tail: …'et\\norder by combined.ch, combined.eid nulls first\\nlimit 10;\\n'.
  ```
- `trilogy file write answer_3705756794.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw…f.ch,
    store_perf.eid,
    store_perf.sales,
    store_perf.ret,
    store_perf.net
order by store_perf.ch, store_perf.eid nulls first
limit 100;
`

  ```text
  refused to write 'answer_3705756794.preql': not syntactically valid Trilogy.

  Parse error:
   --> 9:1
    |
  9 | with ss_agg as
    | ^---
    |
    = expected select_statement, tvf_union_invocation, tvf_except_invocation, or tvf_intersect_invocation
  Location:
  ...per entity with store_perf as ??? with ss_agg as where ss.date_d...

  Write stats: received 1118 chars / 1118 bytes; tail: …'der by store_perf.ch, store_perf.eid nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_3705756794.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw…s ch,
    web_perf.eid as eid,
    web_perf.sales as sales,
    web_perf.ret as ret,
    web_perf.net as net
order by ch, eid nulls first
limit 100;
`

  ```text
  refused to write 'answer_3705756794.preql': not syntactically valid Trilogy.

  Parse error:
    --> 96:1
     |
  96 | union all select
     | ^---
     |
     = expected join_clause, metadata, limit, order_by, where, having, or select_grouping
  Location:
  ...et,     store_perf.net as net ??? union all select     catalog_p...

  Write stats: received 2853 chars / 2853 bytes; tail: …'web_perf.net as net\\norder by ch, eid nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_3705756794.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw…, ret, net);

select combined.ch, combined.eid, combined.sales, combined.ret, combined.net
order by combined.ch, combined.eid nulls first
limit 100;
`

  ```text
  refused to write 'answer_3705756794.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style CTE (`(with ... as ...)`) inside a query? Trilogy supports inline `(select ...)` subqueries (single aliased column), but not parenthesized `with` CTEs. Define a named `rowset <name> <- ...;` (or `with <name> as ...;`) as a top-level statement above the query and reference its output, or - to filter on a related dimension - use its dot-path directly (e.g. `where ss.store.state = 'TN'`).
  Location:
  ...nion(     # Store channel     ??? (with ss_a as      where ss.da...

  Write stats: received 2390 chars / 2390 bytes; tail: …'t\\norder by combined.ch, combined.eid nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_3705756794.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw…ll select ch, eid, sales, ret, net from catalog_perf
union all select ch, eid, sales, ret, net from web_perf
order by ch, eid nulls first
limit 100;
`

  ```text
  refused to write 'answer_3705756794.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...lect ch, eid, sales, ret, net ??? from store_perf union all sele...

  Write stats: received 2540 chars / 2540 bytes; tail: …', net from web_perf\\norder by ch, eid nulls first\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.customer.customer_address.state, count(store_sales.ticket_number) as cnt, store_sales.customer.customer_address.address_sk is not null as has_addr where store_sales.date_dim.year = 2001 and store_sales.date_dim.moy = 1 group by 1,3 limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
   store_sales.date_dim.moy = 1 ??? group by 1,3 limit 5;
  ```
- `trilogy file write answer_3263796749.preql --content import raw.store_sales as ss;

# Bucket 1: 1-20
auto cnt_b1 <- sum(case when ss.quantity between 1 and 2…10097 then avg_disc_b4 else avg_net_b4 end as bucket_61_80,
    case when cnt_b5 > 165306 then avg_disc_b5 else avg_net_b5 end as bucket_81_100
by *;`

  ```text
  refused to write 'answer_3263796749.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [213]: A `by <grain>` clause must follow an aggregate, but the expression before it has none. If the `by` sits inside an aggregate's parentheses (`max(x by *)`), move it outside the call: `max(x) by *`. To take each distinct value once per grain, wrap it in `group(...)` - e.g. `group(item.current_price) by item.id, item.category`. For a reduction, use an aggregate: `sum(x) by ...`, `avg(x) by ...`, `max(x) by ...`.
  Location:
  ...g_net_b5 end as bucket_81_100 ??? by *;

  Write stats: received 1759 chars / 1759 bytes; tail: …' then avg_disc_b5 else avg_net_b5 end as bucket_81_100\\nby *;'.
  ```
- `trilogy file write answer_3263796749.preql --content import raw.store_sales as ss;

# Bucket 1: 1-20
auto col_b1 <- case when (sum(case when ss.quantity betw…lect
    col_b1 as bucket_1_20,
    col_b2 as bucket_21_40,
    col_b3 as bucket_41_60,
    col_b4 as bucket_61_80,
    col_b5 as bucket_81_100
by *;`

  ```text
  refused to write 'answer_3263796749.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:231
    |
  4 | auto col_b1 <- case when (sum(case when ss.quantity between 1 and 20 then 1 else 0 end) by *) > 74129 then avg(ss.ext_discount_amt ? ss.quantity between 1 and 20) by * else avg(ss.net_paid ? ss.quantity between 1 and 20) by * end by *;
    |                                                                                                                                                                                                                                       ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...ty between 1 and 20) by * end ??? by *;  # Bucket 2: 21-40 auto

  Write stats: received 1474 chars / 1474 bytes; tail: …'   col_b4 as bucket_61_80,\\n    col_b5 as bucket_81_100\\nby *;'.
  ```
- `trilogy file write answer_1297355556.preql --content import raw.store_sales as ss;

where ss.store.date_dim.year = 2001
select count(ss.ticket_number) as cnt, sum(ss.quantity) as qty
by *
limit 5;`

  ```text
  refused to write 'answer_1297355556.preql': not syntactically valid Trilogy.

  Parse error:
   --> 5:1
    |
  5 | by *
    | ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
   cnt, sum(ss.quantity) as qty ??? by * limit 5;

  Write stats: received 143 chars / 143 bytes; tail: …'ticket_number) as cnt, sum(ss.quantity) as qty\\nby *\\nlimit 5;'.
  ```
- `trilogy run --import raw.date_dim:dd select distinct dd.moy where dd.moy = 11 limit 5;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...rt raw.date_dim as dd; select ??? distinct dd.moy where dd.moy =...
  ```
- `trilogy file write answer_219203908.preql -e -c import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Items that appea…nnel_data.channel nulls first, channel_data.brand_id nulls first, channel_data.class_id nulls first, channel_data.category_id nulls first
limit 100;
`

  ```text
  refused to write 'answer_219203908.preql': not syntactically valid Trilogy.

  Parse error:
    --> 43:30
     |
  43 |         count(ss.item.item_sk, ss.ticket_number) as line_count),
     |                              ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
           count(ss.item.item_sk ??? , ss.ticket_number) as line_co...

  Write stats: received 3769 chars / 3769 bytes; tail: …'ulls first, channel_data.category_id nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_765177085.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;

where 
   …cs.quantity) as cs_stddev,
    stddev(cs.quantity) / avg(cs.quantity) as cs_cv
order by ss.item.item_id, ss.item.item_desc, ss.store.state
limit 100;`

  ```text
  refused to write 'answer_765177085.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
   cs.bill_customer.customer_sk ??? union join ss.item.item_sk = c...

  Write stats: received 1161 chars / 1161 bytes; tail: …'s.item.item_id, ss.item.item_desc, ss.store.state\\nlimit 100;'.
  ```
- `trilogy file write answer_765177085.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;

# First, g…,
    ss.quantity as ss_qty,
    sr.return_quantity as sr_qty
;

sales_with_returns.item_id, sales_with_returns.item_desc, sales_with_returns.state;
`

  ```text
  refused to write 'answer_765177085.preql': not syntactically valid Trilogy.

  Parse error:
    --> 24:1
     |
  24 | sales_with_returns.item_id, sales_with_returns.item_desc, sales_with_returns.state;
     | ^---
     |
     = expected EOI, block, or show_statement
  Location:
  ....return_quantity as sr_qty ;  ??? sales_with_returns.item_id, sa...

  Write stats: received 747 chars / 747 bytes; tail: …'id, sales_with_returns.item_desc, sales_with_returns.state;\\n'.
  ```
- `trilogy file write answer_2928586490.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Step 1: Fre…name asc nulls first,
    combined_sales.first_name asc nulls first,
    total_sales asc nulls first
having sum(combined_sales.total) > 0
limit 100
;`

  ```text
  refused to write 'answer_2928586490.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...iples.triple_count > 4 select ??? distinct     frequent_item_tri...

  Write stats: received 3018 chars / 3018 bytes; tail: …'nulls first\\nhaving sum(combined_sales.total) > 0\\nlimit 100\\n;'.
  ```
- `trilogy file write answer_2928586490.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Step 1: Fre…mbined.last_name asc nulls first,
    combined.first_name asc nulls first,
    total_sales asc nulls first
having sum(combined.total) > 0
limit 100
;`

  ```text
  refused to write 'answer_2928586490.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...te_dim.date as sold_date,     ??? count(*) as cnt having count(*...

  Write stats: received 2893 chars / 2893 bytes; tail: …'s asc nulls first\\nhaving sum(combined.total) > 0\\nlimit 100\\n;'.
  ```
- `trilogy file write answer_2928586490.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Step 1: Fre…mbined.last_name asc nulls first,
    combined.first_name asc nulls first,
    total_sales asc nulls first
having sum(combined.total) > 0
limit 100
;`

  ```text
  refused to write 'answer_2928586490.preql': not syntactically valid Trilogy.

  Parse error:
    --> 94:1
     |
  94 | having sum(combined.total) > 0
     | ^---
     |
     = expected limit
  Location:
    total_sales asc nulls first ??? having sum(combined.total) > 0...

  Write stats: received 2923 chars / 2923 bytes; tail: …'s asc nulls first\\nhaving sum(combined.total) > 0\\nlimit 100\\n;'.
  ```
- `trilogy file write answer_4207382245.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;

# Compute store sales totals per county, per …
    and (county_pivot.web_q3 / county_pivot.web_q2) > (county_pivot.store_q3 / county_pivot.store_q2)
order by county_pivot.county asc nulls first
;`

  ```text
  refused to write 'answer_4207382245.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...store_q subset join w = web_q ??? group by county ;  select

  Write stats: received 1941 chars / 1941 bytes; tail: …'vot.store_q2)\\norder by county_pivot.county asc nulls first\\n;'.
  ```
- `trilogy run --import raw.item:item select distinct item.manufact limit 100;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...port raw.item as item; select ??? distinct item.manufact limit 1...
  ```
- `trilogy run --import raw.item:item select item.manufact, item.manufact_id, item.product_name, count(item.item_sk) as cnt by item.manufact where ((item.catego…et' and item.size = 'N/A') or (item.category = 'Electronics' and item.color = 'mint' and item.units = 'Gross' and item.size = 'N/A')) having cnt > 1;`

  ```text
  Syntax error in stdin:  --> 2:87
    |
  2 | select item.manufact, item.manufact_id, item.product_name, count(item.item_sk) as cnt by item.manufact where ((item.category = 'Books' and item.color = 'tan' and item.units = 'Oz' and item.size = 'N/A') or (item.category = 'Electronics' and item.color = 'purple' and item.units = 'Ton' and item.size = 'N/A') or (item.category = 'Men' and item.color = 'misty' and item.units = 'Box' and item.size = 'medium') or (item.category = 'Books' and item.color = 'medium' and item.units = 'Tsp' and item.size = 'N/A') or (item.category = 'Books' and item.color = 'midnight' and item.units = 'Gram' and item.size = 'N/A') or (item.category = 'Books' and item.color = 'pale' and item.units = 'Pound' and item.size = 'N/A') or (item.category = 'Electronics' and item.color = 'khaki' and item.units = 'Pallet' and item.size = 'N/A') or (item.category = 'Electronics' and item.color = 'mint' and item.units = 'Gross' and item.size = 'N/A')) having cnt > 1;
    |                                                                                       ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...e, count(item.item_sk) as cnt ??? by item.manufact where ((item....
  ```
- `trilogy run --import raw.item:item select item.manufact, count(item.item_sk) as cnt by item.manufact where ((item.category = 'Books' and item.color = 'tan' a…et' and item.size = 'N/A') or (item.category = 'Electronics' and item.color = 'mint' and item.units = 'Gross' and item.size = 'N/A')) having cnt > 1;`

  ```text
  Syntax error in stdin:  --> 2:50
    |
  2 | select item.manufact, count(item.item_sk) as cnt by item.manufact where ((item.category = 'Books' and item.color = 'tan' and item.units = 'Oz' and item.size = 'N/A') or (item.category = 'Electronics' and item.color = 'purple' and item.units = 'Ton' and item.size = 'N/A') or (item.category = 'Men' and item.color = 'misty' and item.units = 'Box' and item.size = 'medium') or (item.category = 'Books' and item.color = 'medium' and item.units = 'Tsp' and item.size = 'N/A') or (item.category = 'Books' and item.color = 'midnight' and item.units = 'Gram' and item.size = 'N/A') or (item.category = 'Books' and item.color = 'pale' and item.units = 'Pound' and item.size = 'N/A') or (item.category = 'Electronics' and item.color = 'khaki' and item.units = 'Pallet' and item.size = 'N/A') or (item.category = 'Electronics' and item.color = 'mint' and item.units = 'Gross' and item.size = 'N/A')) having cnt > 1;
    |                                                  ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...t, count(item.item_sk) as cnt ??? by item.manufact where ((item....
  ```
- `trilogy run --import raw.item:item select item.manufact order by item.manufact where ((item.category = 'Books' and item.color = 'tan' and item.units = 'Oz' a…m.units = 'Pallet' and item.size = 'N/A') or (item.category = 'Electronics' and item.color = 'mint' and item.units = 'Gross' and item.size = 'N/A'));`

  ```text
  Syntax error in stdin:  --> 2:45
    |
  2 | select item.manufact order by item.manufact where ((item.category = 'Books' and item.color = 'tan' and item.units = 'Oz' and item.size = 'N/A') or (item.category = 'Electronics' and item.color = 'purple' and item.units = 'Ton' and item.size = 'N/A') or (item.category = 'Men' and item.color = 'misty' and item.units = 'Box' and item.size = 'medium') or (item.category = 'Books' and item.color = 'medium' and item.units = 'Tsp' and item.size = 'N/A') or (item.category = 'Books' and item.color = 'midnight' and item.units = 'Gram' and item.size = 'N/A') or (item.category = 'Books' and item.color = 'pale' and item.units = 'Pound' and item.size = 'N/A') or (item.category = 'Electronics' and item.color = 'khaki' and item.units = 'Pallet' and item.size = 'N/A') or (item.category = 'Electronics' and item.color = 'mint' and item.units = 'Gross' and item.size = 'N/A'));
    |                                             ^---
    |
    = expected limit, ORDER_DIRECTION_LA, ORDERING_DIRECTION, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...nufact order by item.manufact ??? where ((item.category = 'Books...
  ```
- `trilogy run --import raw.store_sales:ss select ss.item.product_name, avg(ss.net_profit) as avg_np where ss.store.store_sk=1 group by ss.item.product_name order by avg_np desc limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ..._np where ss.store.store_sk=1 ??? group by ss.item.product_name
  ```
- `trilogy run --import raw.customer_address:ca select ca.zip, length(ca.zip) as len where length(ca.zip) > 5 limit 10;`

  ```text
  Syntax error in stdin:  --> 2:22
    |
  2 | select ca.zip, length(ca.zip) as len where length(ca.zip) > 5 limit 10;
    |                      ^---
    |
    = expected limit, order_by, where, having, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, select_grouping, or JOIN_TYPE
  Location:
  ...s as ca; select ca.zip, length ??? (ca.zip) as len where length(c...
  ```
- `trilogy file write answer_2940558602.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw…nk
having
    return_rank <= 10 or currency_rank <= 10
order by
    channel asc,
    return_rank asc,
    currency_rank asc,
    item asc
limit 100;
`

  ```text
  refused to write 'answer_2940558602.preql': not syntactically valid Trilogy.

  Parse error:
    --> 65:6
     |
  65 |     (import raw.item as item
     |      ^---
     |
     = expected select_statement
  Location:
   with combined as union(     ( ??? import raw.item as item      w...

  Write stats: received 3474 chars / 3474 bytes; tail: …'rn_rank asc,\\n    currency_rank asc,\\n    item asc\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw/store_sales:ss --import raw/store_returns:sr select count(*) as cnt subset join ss.ticket_number = sr.ticket_number subset join ss.i…item_sk where sr.date_dim.moy = 8 and sr.date_dim.year = 2001 and ss.customer.customer_sk = sr.customer.customer_sk and ss.store.store_sk is not null`

  ```text
  Syntax error in stdin: Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...w.store_returns as sr; select ??? count(*) as cnt subset join ss...
  ```
- `trilogy run --import raw/store_sales:ss --import raw/store_returns:sr select case when date_diff(ss.date_dim.date, sr.date_dim.date, day) <= 30 then '30' whe….date_dim.moy = 8 and sr.date_dim.year = 2001 and ss.customer.customer_sk = sr.customer.customer_sk and ss.store.store_sk is not null group by bucket`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...ss.store.store_sk is not null ??? group by bucket;
  ```
- `trilogy run --import raw/store_sales:ss select ss.store.date_dim.week_seq, ss.store.date_dim.year, count(1) as cnt where ss.store.date_dim.week_seq between 5217 and 5220 group by ss.store.date_dim.week_seq, ss.store.date_dim.year order by ss.store.date_dim.week_seq limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...eek_seq between 5217 and 5220 ??? group by ss.store.date_dim.wee...
  ```
- `trilogy file write answer_345752060.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;
import raw.item…ect
    all_sales.item_code,
    sum(all_sales.channel_total) as total
order by all_sales.item_code asc nulls first, total asc nulls first
limit 100;`

  ```text
  refused to write 'answer_345752060.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...string, channel_total numeric) ???  select     all_sales.item_co...

  Write stats: received 1133 chars / 1133 bytes; tail: …'.item_code asc nulls first, total asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write answer_4037365554.preql --content import raw.store_sales as store_sales;

# First: compute per-manager, per-month total of unit price
# Fi…tal,
  per_manager_avg.avg_monthly
order by 
  per_month.manager_id asc,
  per_manager_avg.avg_monthly asc,
  per_month.monthly_total asc
limit 100
;`

  ```text
  refused to write 'answer_4037365554.preql': not syntactically valid Trilogy.

  Parse error:
    --> 26:1
     |
  26 | by per_month.manager_id
     | ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...monthly_total) as avg_monthly ??? by per_month.manager_id ;  # T...

  Write stats: received 1563 chars / 1563 bytes; tail: …'g.avg_monthly asc,\\n  per_month.monthly_total asc\\nlimit 100\\n;'.
  ```
- `trilogy file write answer_3544057080.preql -c import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalo…m_catalog_ext_list_price > 2 * cat_refund_by_item.total_refund
    and cat_refund_by_item.total_refund is not null;

select qualifying_items limit 5;`

  ```text
  refused to write 'answer_3544057080.preql': not syntactically valid Trilogy.

  Parse error:
    --> 22:5
     |
  22 |     where item_catalog_ext_list_price > 2 * cat_refund_by_item.total_refund
     |     ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...at_refund_by_item.item_sk     ??? where item_catalog_ext_list_pr...

  Write stats: received 912 chars / 912 bytes; tail: …'.total_refund is not null;\\n\\nselect qualifying_items limit 5;'.
  ```
- `trilogy run --import raw.web_sales:web select distinct web.sold_date.year order by 1 limit 20;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
   raw.web_sales as web; select ??? distinct web.sold_date.year or...
  ```
- `trilogy run --import raw.store_sales:ss select ss.store.state, sum(ss.net_profit) as total_net_profit where ss.date_dim.year = 2000 and ss.store.store_sk is not null group by ss.store.state order by total_net_profit desc limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...ss.store.store_sk is not null ??? group by ss.store.state order
  ```
- `trilogy file write answer_42596196.preql --content import raw.catalog_sales as cs;
import raw.web_sales as ws;
import raw.store_sales as ss;

with combined a…der by
    total_ext_sales_price desc nulls first,
    combined.brand_id nulls first,
    combined.hour nulls first,
    combined.minute nulls first;`

  ```text
  refused to write 'answer_42596196.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
   minute bigint, total numeric) ???  select     combined.brand_id...

  Write stats: received 1565 chars / 1565 bytes; tail: …' combined.hour nulls first,\\n    combined.minute nulls first;'.
  ```
- `trilogy file write answer_3840922834.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;

# Compute totals per customer per channel per…_2001)
select
    cust_info.customer_id,
    cust_info.first_name,
    cust_info.last_name
order by
    cust_info.customer_id nulls first
limit 100;
`

  ```text
  refused to write 'answer_3840922834.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...ng together and apply filters ??? union join cust_info.customer_...

  Write stats: received 1823 chars / 1823 bytes; tail: …'e\\norder by\\n    cust_info.customer_id nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_3046445280.preql --content import raw/catalog_sales as cs;
import raw/catalog_returns as cr;
import raw/store_sales as ss;
import r…manufact_id
where y2001.qty is not null
  and y2002.qty is not null
  and y2002.qty / y2001.qty < 0.9
order by qty_diff asc, amt_diff asc
limit 100;
`

  ```text
  refused to write 'answer_3046445280.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/catalog_sales as cs;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/catalog_sales as cs; impor...

  Write stats: received 4472 chars / 4472 bytes; tail: …'01.qty < 0.9\\norder by qty_diff asc, amt_diff asc\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw/catalog_sales:cs select cs.sold_date.year, count(*) as cnt where cs.sold_date.year = 2000 limit 5;`

  ```text
  Syntax error in stdin: Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...cs; select cs.sold_date.year, ??? count(*) as cnt where cs.sold_...
  ```
- `trilogy run --import raw/store_sales:ss --import raw/store_returns:sr select ss.net_profit, sr.return_amt, sr.net_loss where ss.date_dim.date between '2000-0…is not null and ss.store.store_id is not null subset join sr.item.item_sk = ss.item.item_sk subset join sr.ticket_number = ss.ticket_number limit 10;`

  ```text
  Syntax error in stdin:  --> 3:228
    |
  3 | select ss.net_profit, sr.return_amt, sr.net_loss where ss.date_dim.date between '2000-08-23'::date and '2000-09-22'::date and ss.item.current_price > 50 and ss.promotion.channel_tv is not null and ss.store.store_id is not null subset join sr.item.item_sk = ss.item.item_sk subset join sr.ticket_number = ss.ticket_number limit 10;
    |                                                                                                                                                                                                                                    ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...ss.store.store_id is not null ??? subset join sr.item.item_sk =
  ```
- `trilogy run --import raw.store_returns:sr --import raw.catalog_returns:cr --import raw.web_returns:wr select sr.item.item_id, sum(sr.return_quantity) as stor…in (5244, 5257, 5264) and cr.date_dim.week_seq in (5244, 5257, 5264) and wr.date_dim.week_seq in (5244, 5257, 5264) group by sr.item.item_id limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...eek_seq in (5244, 5257, 5264) ??? group by sr.item.item_id limit...
  ```
- `trilogy file write test_debug9.preql --content import raw.web_sales as ws;
import raw.web_returns as wr;

where
  ws.sold_date.year = 2000
  and wr.returning… wr.item.item_sk = ws.item.item_sk
  subset join wr.order_number = ws.order_number
group by ws.sales_price, ws.net_profit
order by cnt desc
limit 20;`

  ```text
  refused to write 'test_debug9.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...rder_number = ws.order_number ??? group by ws.sales_price, ws.ne...

  Write stats: received 784 chars / 784 bytes; tail: …'by ws.sales_price, ws.net_profit\\norder by cnt desc\\nlimit 20;'.
  ```
- `trilogy file write answer_32504671.preql --content import raw.store_sales as ss;

where ss.store.store_name = 'ese'
  and (
    (ss.household_demographics.de…nute >= 30) as bucket_1130_1200,
  count(ss.item.item_sk, ss.ticket_number ? ss.time_dim.hour = 12 and ss.time_dim.minute < 30) as bucket_1200_1230
;`

  ```text
  refused to write 'answer_32504671.preql': not syntactically valid Trilogy.

  Parse error:
    --> 10:24
     |
  10 |   count(ss.item.item_sk, ss.ticket_number ? ss.time_dim.hour = 8 and ss.time_dim.minute >= 30 and ss.time_dim.hour < 9) as bucket_0830_0900,
     |                        ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...select   count(ss.item.item_sk ??? , ss.ticket_number ? ss.time_d...

  Write stats: received 1329 chars / 1329 bytes; tail: …'hour = 12 and ss.time_dim.minute < 30) as bucket_1200_1230\\n;'.
  ```
- `trilogy file write answer_501431752.preql --content import raw.catalog_returns as cr;

# Catalog returns in the 11th month of year 1998
# Restrict to returni…status = 'W'
         and cr.returning_customer.customer_demographics.education_status = 'Advanced Degree')
    )
order by
    total_net_loss desc
;
`

  ```text
  refused to write 'answer_501431752.preql': not syntactically valid Trilogy.

  Parse error:
    --> 16:1
     |
  16 | by
     | ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...r.net_loss) as total_net_loss ??? by     cr.call_center.call_cen...

  Write stats: received 1433 chars / 1433 bytes; tail: …"'Advanced Degree')\\n    )\\norder by\\n    total_net_loss desc\\n;\\n".
  ```
- `trilogy file write answer_3281742578.preql --content import raw.web_sales as ws;
import raw.web_returns as wr;

# Rowset: orders where at least 2 different w…_number) as order_count,
    sum(ws.ext_ship_cost) as total_ext_ship_cost,
    sum(ws.net_profit) as total_net_profit
order by order_count
limit 100;`

  ```text
  refused to write 'answer_3281742578.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block - they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
   used with multi_wh_orders as ??? auto wh_count <- count_distinc...

  Write stats: received 1060 chars / 1060 bytes; tail: …'_profit) as total_net_profit\\norder by order_count\\nlimit 100;'.
  ```
- `trilogy file write answer_2333598260.preql --content import raw.store_sales as store_sales;

where
    store_sales.store.store_name = 'ese'
    and store_sal…= 30
    and store_sales.customer.household_demographics.dep_count = 7
select
    count(*) as line_item_count
order by
    line_item_count
limit 100;`

  ```text
  refused to write 'answer_2333598260.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...hics.dep_count = 7 select     ??? count(*) as line_item_count or...

  Write stats: received 312 chars / 312 bytes; tail: …') as line_item_count\\norder by\\n    line_item_count\\nlimit 100;'.
  ```
- `trilogy file write answer_2333598260.preql --content import raw.store_sales as store_sales;

with line_items as
where
    store_sales.store.store_name = 'ese…_sk
    --store_sales.coupon_amt
;

select
    count(line_items.store_sales.ticket_number) as line_item_count
order by
    line_item_count
limit 100;`

  ```text
  refused to write 'answer_2333598260.preql': not syntactically valid Trilogy.

  Parse error:
    --> 12:6
     |
  12 |     --store_sales.coupon_amt
     |      ^---
     |
     = expected access_chain
  Location:
  ...store_sales.item.item_sk     - ??? -store_sales.coupon_amt ;  sel...

  Write stats: received 467 chars / 467 bytes; tail: …') as line_item_count\\norder by\\n    line_item_count\\nlimit 100;'.
  ```
- `trilogy file write answer_3562094594.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;

# Store sales unique (customer, item) pai…nly_store.cust_sk) as store_only_count,
    count(only_catalog.cust_sk) as catalog_only_count,
    count(both_sets.cust_sk) as both_count
limit 100;
`

  ```text
  refused to write 'answer_3562094594.preql': not syntactically valid Trilogy.

  Parse error:
    --> 20:6
     |
  20 |     (store_set.cust_sk, store_set.item_sk),
     |      ^---
     |
     = expected select_statement
  Location:
  ...th only_store as except(     ( ??? store_set.cust_sk, store_set.i...

  Write stats: received 1073 chars / 1073 bytes; tail: …'ount,\\n    count(both_sets.cust_sk) as both_count\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_3498056762.preql --content import raw.catalog_sales as cs;

auto shipping_lag <- date_diff(cs.sold_date.date, cs.ship_date.date, da…en 1 else 0 end) as bucket_over_120, 0)
order by
    wh_name_20 nulls first,
    ship_mode_type nulls first,
    cc_name_lower nulls first
limit 100;`

  ```text
  refused to write 'answer_3498056762.preql': not syntactically valid Trilogy.

  Parse error:
    --> 14:66
     |
  14 |     coalesce(sum(case when shipping_lag <= 30 then 1 else 0 end) as bucket_30_or_fewer, 0),
     |                                                                  ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, aggregate_over, or window_sql_over
  Location:
  ..._lag <= 30 then 1 else 0 end) ??? as bucket_30_or_fewer, 0),

  Write stats: received 1042 chars / 1042 bytes; tail: …'e_type nulls first,\\n    cc_name_lower nulls first\\nlimit 100;'.
  ```

### `cli-misuse`

- `trilogy explore raw`

  ```text
  Invalid value for 'PATH': File 'raw' is a directory.
  ```
- `trilogy explore raw/`

  ```text
  Invalid value for 'PATH': File 'raw/' is a directory.
  ```
- `trilogy explore raw/sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/sales.preql' does not exist.
  ```

### `undefined-concept`

- `trilogy explore answer_2874833976.preql`

  ```text
  Failed to parse answer_2874833976.preql: (UndefinedConceptException(...), "Undefined concept: top_states.state_total.state (line 28, col 25, in WHERE). Suggestions: ['top_states.state_total', 'top_states.ss.store.state', 'ss.store.state', 'ss.customer.customer_address.state', 'ss.customer_address.state']")
  ```

### `type-error`

- `trilogy run answer_2333598260.preql`

  ```text
  Type error in answer_2333598260.preql: Invalid argument type 'BIGINT' passed into CONCAT_STRICT function in position 1 from concept: store_sales.ticket_number. Valid: 'STRING'.
  ```
