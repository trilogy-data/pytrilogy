# Trilogy failure analysis — 20260911-181006

- Run `20260911-181006_enriched_deepseek_deepseek-v4-flash` | `deepseek/deepseek-v4-flash` | sf=1
- `trilogy` calls: 1474 | failed: 47 (3%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 21 | 45% |
| `other` | 14 | 30% |
| `undefined-concept` | 6 | 13% |
| `no-output` | 2 | 4% |
| `join-resolution` | 2 | 4% |
| `disabled-tool` | 1 | 2% |
| `file-not-found` | 1 | 2% |

## Detail

### `syntax-parse`

- `trilogy file write probe5.preql --run-and-delete`

  ```text
  refused to write 'probe5.preql': not syntactically valid Trilogy.

  Parse error:
    --> 10:30
     |
  10 |   and a.channel_dim_text_id <> a.return_channel_dim_text_id
     |                              ^---
     |
     = expected sum_operator
  Location:
     and a.channel_dim_text_id < ??? > a.return_channel_dim_text_id...
  ```
- `trilogy file write probe_dup.preql --run-and-delete`

  ```text
  refused to write 'probe_dup.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...i.current_price) as n_prices  ??? group by i.id  having count(i....
  ```
- `trilogy file write probe_b.preql --run-and-delete`

  ```text
  refused to write 'probe_b.preql': not syntactically valid Trilogy.

  Parse error:
   --> 8:18
    |
  8 |   count(csr.grain(csr.item.sk, csr.ticket_number, csr.catalog_order_number)) as n_rows
    |                  ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...as cq_null,    count(csr.grain ??? (csr.item.sk, csr.ticket_numbe...
  ```
- `trilogy file write probe_weight.preql --run-and-delete`

  ```text
  refused to write 'probe_weight.preql': not syntactically valid Trilogy.

  Parse error:
    --> 21:1
     |
  21 | by *;
     | ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
     avg(line_dep_count) as dc  ??? by *;
  ```
- `trilogy file write answer_2133330107.preql --run`

  ```text
  refused to write 'answer_2133330107.preql': not syntactically valid Trilogy.

  Parse error:
    --> 12:57
     |
  12 |   and substring(ss.customer.current_address.zip, 1, 5) <> substring(ss.store.zip, 1, 5)
     |                                                         ^---
     |
     = expected sum_operator
  Location:
  ...r.current_address.zip, 1, 5) < ??? > substring(ss.store.zip, 1, 5...
  ```
- `trilogy file write probe_1078396760.preql --run-and-delete`

  ```text
  refused to write 'probe_1078396760.preql': not syntactically valid Trilogy.

  Parse error:
    --> 16:1
     |
  16 | by item_tot.item_class
     | ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...lass)) as window_class_total  ??? by item_tot.item_class  order
  ```
- `trilogy file write answer_2604809012.preql --run`

  ```text
  refused to write 'answer_2604809012.preql': not syntactically valid Trilogy.

  Parse error:
    --> 12:19
     |
  12 |     before_total <> 0
     |                   ^---
     |
     = expected sum_operator
  Location:
  ...49  having      before_total < ??? > 0      and after_total * 3 >...
  ```
- `trilogy file write probe1.preql --run-and-delete`

  ```text
  refused to write 'probe1.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:16
    |
  4 |   count(c.grain(c.item.sk, c.ticket_number)) as store_lines,
    |                ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
   c;    select    count(c.grain ??? (c.item.sk, c.ticket_number))
  ```
- `trilogy file write probe_c.preql --run-and-delete`

  ```text
  refused to write 'probe_c.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ....sale_date.month_of_year = 5  ??? group by gmt  order by gmt asc...
  ```
- `trilogy file write probe1.preql --run-and-delete`

  ```text
  refused to write 'probe1.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
   cs.warehouse.sk is not null  ??? group by cs.warehouse.state  o...
  ```
- `trilogy file write probe2.preql --run-and-delete`

  ```text
  refused to write 'probe2.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...as w_state, count(w.sk) as n  ??? group by w.state  order by w_s...
  ```
- `trilogy file write answer_1965638525.preql --run`

  ```text
  refused to write 'answer_1965638525.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:41
    |
  7 |   and ss.customer.current_address.city <> ss.pos_address.city
    |                                         ^---
    |
    = expected sum_operator
  Location:
  ...ustomer.current_address.city < ??? > ss.pos_address.city  select
  ```
- `trilogy file write probe_a.preql --run-and-delete`

  ```text
  refused to write 'probe_a.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...eturn_date.month_of_year = 8  ??? group by ry, rm  limit 5;
  ```
- `trilogy file write probe_tie.preql --run-and-delete`

  ```text
  refused to write 'probe_tie.preql': not syntactically valid Trilogy.

  Parse error:
    --> 16:18
     |
  16 | having rk_class <> rk_ts
     |                  ^---
     |
     = expected sum_operator
  Location:
  ...item.class)  having rk_class < ??? > rk_ts  order by ts desc  lim...
  ```
- `trilogy file write probe_dense.preql --run-and-delete`

  ```text
  refused to write 'probe_dense.preql': not syntactically valid Trilogy.

  Parse error:
    --> 16:12
     |
  16 | having rk <> dr
     |            ^---
     |
     = expected sum_operator
  Location:
  ...y, ss.item.class)  having rk < ??? > dr  order by ts desc  limit
  ```
- `trilogy file write answer_3063407983.preql --run`

  ```text
  refused to write 'answer_3063407983.preql': not syntactically valid Trilogy.

  Parse error:
   --> 8:41
    |
  8 |   and ss.customer.current_address.city <> ss.pos_address.city
    |                                         ^---
    |
    = expected sum_operator
  Location:
  ...ustomer.current_address.city < ??? > ss.pos_address.city  select
  ```
- `trilogy file write probe1.preql --run-and-delete`

  ```text
  refused to write 'probe1.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:16
    |
  4 |   count(s.grain(s.item.sk, s.channel, s.order_id)) as rows_total,
    |                ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
   s;    select    count(s.grain ??? (s.item.sk, s.channel, s.order...
  ```
- `trilogy file write probe1.preql --run-and-delete`

  ```text
  refused to write 'probe1.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...t null)) as n_retamt_notnull  ??? from s  where s.item.category
  ```
- `trilogy file write probe_b.preql --run-and-delete`

  ```text
  refused to write 'probe_b.preql': not syntactically valid Trilogy.

  Parse error:
    --> 22:1
     |
  22 | select
     | ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...channel = 'WEB') as web_rows  ??? select      count(totals.item_...
  ```
- `trilogy file write answer_840315271.preql --run`

  ```text
  refused to write 'answer_840315271.preql': not syntactically valid Trilogy.

  Parse error:
    --> 32:21
     |
  32 | having avg_monthly <> 0
     |                     ^---
     |
     = expected sum_operator
  Location:
  ..._monthly  having avg_monthly < ??? > 0     and abs(monthly_total
  ```
- `trilogy file write probe6.preql --run-and-delete`

  ```text
  refused to write 'probe6.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:1
    |
  7 | order by sum_adjusted_amount asc nulls first, customer_id asc nulls first
    | ^---
    |
    = expected THEN_LA, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...rn_reason.desc = 'reason 28'  ??? order by sum_adjusted_amount a...
  ```

### `other`

- `trilogy file write probe_a.preql --run-and-delete`

  ```text
  trilogy error: subprocess timed out after 600s.
  ```
- `trilogy file write probe5b.preql --run-and-delete`

  ```text
  trilogy error: subprocess timed out after 600s.
  ```
- `trilogy file write answer_2844519538.preql --run`

  ```text
  Syntax error in answer_2844519538.preql: Output column 'peach_sales' renames 'local.peach_sales' back to the name of an existing concept 'peach_sales' (defined at line 18) that 'local.peach_sales' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'peach_sales_out').
  ```
- `trilogy run probe5b.preql`

  ```text
  trilogy error: subprocess timed out after 600s.
  ```
- `trilogy file write probe_d.preql --run-and-delete`

  ```text
  Unexpected error in probe_d.preql: (_duckdb.ConversionException) Conversion Error: Could not convert string 'eseoughtable' to INT64 when casting from source column i_manufact

  LINE 18: ... from quizzical where quizzical."pi_psk" is not distinct from "item_items"."I_MANUFACT") and "item_items"."I_MANUFACT_ID...
                                                                            ^
  [SQL:
  WITH
  quizzical as (
  SELECT
      "item_items"."I_ITEM_SK" as "pi_psk"
  FROM
      "item" as "item_items"
  WHERE
      (( "item_items"."I_CATEGORY" = 'Books' and "item_items"."I_COLOR" = 'tan' and "item_items"."I_UNITS" = 'Oz' and "item_items"."I_SIZE" = 'N/A' ) or ( "item_items"."I_CATEGORY" = 'Electronics' and "item_items"."I_COLOR" = 'purple' and "item_items"."I_UNITS" = 'Ton' and "item_items"."I_SIZE" = 'N/A' ) or ( "item_items"."I_CATEGORY" = 'Men' and "item_items"."I_COLOR" = 'misty' and "item_items"."I_UNITS" = 'Box' and "item_items"."I_SIZE" = 'medium' ) or ( "item_items"."I_CATEGORY" = 'Books' and "item_items"."I_COLOR" = 'medium' and "item_items"."I_UNITS" = 'Tsp' and "item_items"."I_SIZE" = 'N/A' ) or ( "item_items"."I_CATEGORY" = 'Books' and "item_items"."I_COLOR" = 'midnight' and "item_items"."I_UNITS" = 'Gram' and "item_items"."I_SIZE" = 'N/A' ) or ( "item_items"."I_CATEGORY" = 'Books' and "item_items"."I_COLOR" = 'pale' and "item_items"."I_UNITS" = 'Pound' and "item_items"."I_SIZE" = 'N/A' ) or ( "item_items"."I_CATEGORY" = 'Electronics' and "item_items"."I_COLOR" = 'khaki' and "item_items"."I_UNITS" = 'Pallet' and "item_items"."I_SIZE" = 'N/A' ) or ( "item_items"."I_CATEGORY" = 'Electronics' and "item_items"."I_COLOR" = 'mint' and "item_items"."I_UNITS" = 'Gross' and "item_items"."I_SIZE" = 'N/A' )) = True

  GROUP BY
      1)
  SELECT
      count("item_items"."I_ITEM_SK") as "n_literal_textInSk"
  FROM
      "item" as "item_items"
  WHERE
      exists (select 1 from quizzical where quizzical."pi_psk" is not distinct from "item_items"."I_MANUFACT") and "item_items"."I_MANUFACT_ID" >= 1 and "item_items"."I_MANUFACT_ID" <= 500
  ]
  (Background on this error at: https://sqlalche.me/e/20/9h9h)
  ```
- `trilogy run ..\query08.preql --dry-run`

  ```text
  Syntax error in ..\query08.preql: This script requires parameter "zips" to be set in environment.
  ```
- `trilogy file write probe2.preql --run-and-delete`

  ```text
  Syntax error in probe2.preql: Output column 'benchmark' renames 'local.benchmark' back to the name of an existing concept 'benchmark' (defined at line 3) that 'local.benchmark' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'benchmark_out').
  ```
- `trilogy run probe5b.preql`

  ```text
  trilogy error: subprocess timed out after 600s.
  ```
- `trilogy file write probe7.preql --run-and-delete`

  ```text
  Syntax error in probe7.preql: HAVING filters on a dimension outside the SELECT projection, but the select has no grain key to anchor a post-aggregation semijoin (line 15). Move the filter to WHERE to filter before aggregation.
  ```
- `trilogy file write answer_3210116865.preql --run`

  ```text
  Resolution error in answer_3210116865.preql: Planner emitted a keyless join between row-bearing sources that share a join axis or one source relation: ss.sale_date.date_at_ss_sale_date_sk_join_ss.store_sales_at_ss_item_sk_ss_ticket_number_at_ss_item_sk_ss_ticket_number_filtered_by_7385886750277670_grouped_by_ss.store.sk_at_ss_store_sk_at_ss_store_sk_at_store_sales_per_outlet_outlet_sk_grouped_by_local._virt_presence_6095466435435125_at_local__virt_presence_6095466435435125 onto ss.return_date.date_at_ss_return_date_sk_join_ss.store_returns_at_ss_item_sk_ss_ticket_number_join_ss.store_sales_at_ss_item_sk_ss_ticket_number_at_ss_item_sk_ss_ticket_number_filtered_by_5496664543456184_grouped_by_ss.return_store.sk_at_ss_return_store_sk_at_ss_return_store_sk_at_store_returns_per_outlet_outlet_sk_join_ss.sale_date.date_at_ss_sale_date_sk_join_ss.store_sales_at_ss_item_sk_ss_ticket_number_at_ss_item_sk_ss_ticket_number_filtered_by_7385886750277670_grouped_by_ss.store.sk_at_ss_store_sk_at_ss_store_sk_at_store_sales_per_outlet_outlet_sk_at_store_returns_per_outlet_outlet_sk_at_store_returns_per_outlet_outlet_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_h.preql --run-and-delete`

  ```text
  Resolution error in probe_h.preql: Planner emitted a keyless join between row-bearing sources that share a join axis or one source relation: ss.store_returns_at_ss_item_sk_ss_ticket_number_grouped_by_ss._returned_ticket_at_ss__returned_ticket_at_ss__returned_ticket onto ss.store_returns_at_ss_item_sk_ss_ticket_number_join_ss.store_sales_at_ss_item_sk_ss_ticket_number_at_ss_item_sk_ss_ticket_number_filtered_by_5901485497079716_at_ss_item_sk_ss_ticket_number. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write answer_2852230229.preql --run`

  ```text
  Syntax error in answer_2852230229.preql: ORDER BY references 'local.parent', which is not in the SELECT projection (line 11). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --local.parent order by local.parent asc`.
  ```
- `trilogy file write probe_xcheck.preql --run-and-delete`

  ```text
  Syntax error in probe_xcheck.preql: HAVING filters on a dimension outside the SELECT projection, but the select has no grain key to anchor a post-aggregation semijoin (line 10). Move the filter to WHERE to filter before aggregation.
  ```
- `trilogy file write probe_verify.preql --run-and-delete`

  ```text
  Syntax error in probe_verify.preql: HAVING filters on a dimension outside the SELECT projection, but the select has no grain key to anchor a post-aggregation semijoin (line 8). Move the filter to WHERE to filter before aggregation.
  ```

### `undefined-concept`

- `trilogy file write answer_3347758002.preql --run`

  ```text
  Syntax error in answer_3347758002.preql: Undefined concept: item.category. Suggestions: ['sales.item.category', 'sales.item.category_id']
  ```
- `trilogy file write probe02.preql --run-and-delete`

  ```text
  Syntax error in probe02.preql: Undefined concept: monthly.year_key (line 28, col 7, in WHERE). Suggestions: ['monthly.yr', 'monthly.category', 'monthly.brand']
  ```
- `trilogy file write answer_42596196.preql --run`

  ```text
  Syntax error in answer_42596196.preql: Undefined concept: s.sale_date (line 3, in WHERE). Suggestions: ['s.sale_date.sk', 's.sale_date.id', 's.sale_date.date', 's.sale_date.year', 's.sale_date.day_of_week', 's.sale_date.day_of_month']
  ```
- `trilogy file write probe_b.preql --run-and-delete`

  ```text
  Syntax error in probe_b.preql: Undefined concept: s.coalesce_return_amount (line 9, in SELECT). Suggestions: ['s.return_amount', 's.ext_discount_amount']
  ```
- `trilogy file write answer_2869182220.preql --run`

  ```text
  Syntax error in answer_2869182220.preql: Undefined concept: customer_code (line 22, col 10, in ORDER BY). Suggestions: ['cand.customer_code', 'ss.customer.id', 'ss.customer.birth_date']
  ```
- `trilogy file write probe_cc.preql --run-and-delete`

  ```text
  Syntax error in probe_cc.preql: 9 undefined concept references; fix all before re-running:
    - local.return_net_loss (line 8, in SELECT); did you mean: cs.return_net_loss?
    - return_date.year (line 10, col 7, in WHERE); did you mean: cs.return_date.year, return_date.month_of_year, cs.sale_date.year, cs.ship_date.year, cs.return_customer.first_sales_date.year, cs.return_customer.first_shipto_date.year?
    - return_date.month_of_year (line 11, col 7, in WHERE); did you mean: cs.return_date.month_of_year, return_date.year, cs.sale_date.month_of_year, cs.ship_date.month_of_year, cs.return_customer.first_sales_date.month_of_year, cs.return_customer.first_shipto_date.month_of_year?
    - return_customer.current_address.gmt_offset (line 12, col 7, in WHERE); did you mean: cs.return_customer.current_address.gmt_offset, return_customer.current_demographics.marital_status, return_customer.current_demographics.education_status, cs.return_refund_customer.current_address.gmt_offset, cs.ship_customer.current_address.gmt_offset, cs.billing_customer.current_address.gmt_offset?
    - return_customer.current_household_demographics.buy_potential (line 13, col 7, in WHERE); did you mean: cs.return_customer.current_household_demographics.buy_potential, return_customer.current_demographics.education_status, return_customer.current_demographics.marital_status, cs.return_refund_customer.current_household_demographics.buy_potential, cs.ship_customer.current_household_demographics.buy_potential, cs.billing_customer.current_household_demographics.buy_potential?
    - return_customer.current_demographics.marital_status (line 14, col 9, in WHERE); did you mean: cs.return_customer.current_demographics.marital_status, return_customer.current_demographics.education_status, return_customer.current_household_demographics.buy_potential, return_customer.current_address.gmt_offset, cs.return_refund_customer.current_demographics.marital_status, cs.return_customer_demographic.marital_status?
    - return_customer.current_demographics.education_status (line 14, col 71, in WHERE); did you mean: cs.return_customer.current_demographics.education_status, return_customer.current_demographics.marital_status, return_customer.current_household_demographics.buy_potential, return_customer.current_address.gmt_offset, cs.return_refund_customer.current_demographics.education_status, cs.return_customer_demographic.education_status?
    - return_customer.current_demographics.marital_status (line 15, col 9, in WHERE); did you mean: cs.return_customer.current_demographics.marital_status, return_customer.current_demographics.education_status, return_customer.current_household_demographics.buy_potential, return_customer.current_address.gmt_offset, cs.return_refund_customer.current_demographics.marital_status, cs.return_customer_demographic.marital_status?
    - return_customer.current_demographics.education_status (line 15, col 71, in WHERE); did you mean: cs.return_customer.current_demographics.education_status, return_customer.current_demographics.marital_status, return_customer.current_household_demographics.buy_potential, return_customer.current_address.gmt_offset, cs.return_refund_customer.current_demographics.education_status, cs.return_customer_demographic.education_status?
  ```

### `no-output`

- `trilogy run raw/catalog_store_returns.preql --dry-run`

  ```text
  Nothing was executed: parsed 15 definition statement(s) (8 imports, 3 datasources, 2 concepts, 2 propertys) but none produce output. Did you mean to include a SELECT statement, or run a refresh on datasources instead?
  ```
- `trilogy file write probe1.preql --run-and-delete`

  ```text
  Nothing was executed: parsed 7 definition statement(s) (4 rowsets, 3 imports) but none produce output. Did you mean to include a SELECT statement, or run a refresh on datasources instead?
  ```

### `join-resolution`

- `trilogy file write probe_counts.preql --run-and-delete`

  ```text
  Resolution error in probe_counts.preql: Could not resolve connections for query with output ['local.qualifying_customers<Purpose.METRIC>Derivation.AGGREGATE>', 'local.with_web<Purpose.METRIC>Derivation.AGGREGATE>', 'local.with_catalog<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy file write probe_it2.preql --run-and-delete`

  ```text
  Resolution error in probe_it2.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {code (= it.id), isk (= it.sk), it.current_price, it.current_price, it.manufacturer_id, price (= it.current_price)}; {in_range_rows, inv_rows}.
    - `it.current_price` is disconnected, did you mean `inv.item.current_price`? (connected to the other concepts)
    - `it.current_price` is disconnected, did you mean `inv.item.current_price`? (connected to the other concepts)
    - `it.manufacturer_id` is disconnected, did you mean `inv.item.manufacturer_id`? (connected to the other concepts)
    - `it.id` (as `code`) is disconnected, did you mean `inv.item.id`? (connected to the other concepts)
    - `it.sk` (as `isk`) is disconnected, did you mean `inv.item.sk`? (connected to the other concepts)
    - `it.current_price` (as `price`) is disconnected, did you mean `inv.item.current_price`? (connected to the other concepts)
  These look like separately-imported copies of models already reachable through a connected import; chain through that path (e.g. `inv.item.current_price`) instead of importing a second, disconnected copy.
  ```

### `disabled-tool`

- `trilogy file read probe5b.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available. If a model declares no imports, relate models in the query instead: `select ... subset join a.key = b.key` or `where a.key in b.key` (`trilogy agent-info syntax example scoped-join`).
  ```

### `file-not-found`

- `trilogy file list .trilogy`

  ```text
  No such path: .trilogy
  ```
