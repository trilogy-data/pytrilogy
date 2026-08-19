# Trilogy failure analysis — 20260818-022329

- Run `20260818-022328_enriched_openai_gpt-5.6-luna_effort-max` | `openai/gpt-5.6-luna` | sf=1
- `trilogy` calls: 1213 | failed: 79 (7%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `cli-misuse` | 60 | 76% |
| `other` | 7 | 9% |
| `undefined-concept` | 6 | 8% |
| `syntax-parse` | 5 | 6% |
| `import-path` | 1 | 1% |

## Detail

### `cli-misuse`

- `trilogy explore raw/orders.preql`

  ```text
  Invalid value for 'PATH': File 'raw/orders.preql' does not exist.
  ```
- `trilogy explore raw/orders.preql`

  ```text
  Invalid value for 'PATH': File 'raw/orders.preql' does not exist.
  ```
- `trilogy explore raw/orders.preql`

  ```text
  Invalid value for 'PATH': File 'raw/orders.preql' does not exist.
  ```
- `trilogy explore raw/orders.preql`

  ```text
  Invalid value for 'PATH': File 'raw/orders.preql' does not exist.
  ```
- `trilogy explore raw/orders.preql`

  ```text
  Invalid value for 'PATH': File 'raw/orders.preql' does not exist.
  ```
- `trilogy explore raw/orders.preql`

  ```text
  Invalid value for 'PATH': File 'raw/orders.preql' does not exist.
  ```
- `trilogy explore raw\orders.preql`

  ```text
  Invalid value for 'PATH': File 'raw\\orders.preql' does not exist.
  ```
- `trilogy explore raw/orders.preql`

  ```text
  Invalid value for 'PATH': File 'raw/orders.preql' does not exist.
  ```
- `trilogy explore raw/orders.preql`

  ```text
  Invalid value for 'PATH': File 'raw/orders.preql' does not exist.
  ```
- `trilogy explore raw/orders.preql`

  ```text
  Invalid value for 'PATH': File 'raw/orders.preql' does not exist.
  ```
- `trilogy explore raw/orders.preql`

  ```text
  Invalid value for 'PATH': File 'raw/orders.preql' does not exist.
  ```
- `trilogy explore raw/orders.preql`

  ```text
  Invalid value for 'PATH': File 'raw/orders.preql' does not exist.
  ```
- `trilogy explore raw/orders`

  ```text
  Invalid value for 'PATH': File 'raw/orders' does not exist.
  ```
- `trilogy explore raw/all_sales`

  ```text
  Invalid value for 'PATH': File 'raw/all_sales' does not exist.
  ```
- `trilogy explore raw/orders.preql`

  ```text
  Invalid value for 'PATH': File 'raw/orders.preql' does not exist.
  ```
- `trilogy explore raw/orders.preql`

  ```text
  Invalid value for 'PATH': File 'raw/orders.preql' does not exist.
  ```
- `trilogy explore raw/orders`

  ```text
  Invalid value for 'PATH': File 'raw/orders' does not exist.
  ```
- `trilogy explore raw/item`

  ```text
  Invalid value for 'PATH': File 'raw/item' does not exist.
  ```
- `trilogy explore raw/orders.preql`

  ```text
  Invalid value for 'PATH': File 'raw/orders.preql' does not exist.
  ```
- `trilogy explore raw.orders`

  ```text
  Invalid value for 'PATH': File 'raw.orders' does not exist.
  ```
- `trilogy explore raw/orders.preql`

  ```text
  Invalid value for 'PATH': File 'raw/orders.preql' does not exist.
  ```
- `trilogy explore raw/orders.preql`

  ```text
  Invalid value for 'PATH': File 'raw/orders.preql' does not exist.
  ```
- `trilogy explore raw.web_sales`

  ```text
  Invalid value for 'PATH': File 'raw.web_sales' does not exist.
  ```
- `trilogy explore raw.store_sales`

  ```text
  Invalid value for 'PATH': File 'raw.store_sales' does not exist.
  ```
- `trilogy explore raw/orders.preql`

  ```text
  Invalid value for 'PATH': File 'raw/orders.preql' does not exist.
  ```
- `trilogy explore raw/orders.preql`

  ```text
  Invalid value for 'PATH': File 'raw/orders.preql' does not exist.
  ```
- `trilogy explore raw/orders.preql`

  ```text
  Invalid value for 'PATH': File 'raw/orders.preql' does not exist.
  ```
- `trilogy explore raw/orders.preql`

  ```text
  Invalid value for 'PATH': File 'raw/orders.preql' does not exist.
  ```
- `trilogy explore raw.catalog_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw.catalog_sales.preql' does not exist.
  ```
- `trilogy explore raw/orders.preql`

  ```text
  Invalid value for 'PATH': File 'raw/orders.preql' does not exist.
  ```
- `trilogy explore raw/orders.preql`

  ```text
  Invalid value for 'PATH': File 'raw/orders.preql' does not exist.
  ```
- `trilogy explore raw/orders.preql`

  ```text
  Invalid value for 'PATH': File 'raw/orders.preql' does not exist.
  ```
- `trilogy explore raw/orders`

  ```text
  Invalid value for 'PATH': File 'raw/orders' does not exist.
  ```
- `trilogy explore raw/orders.preql`

  ```text
  Invalid value for 'PATH': File 'raw/orders.preql' does not exist.
  ```
- `trilogy explore raw.store_sales`

  ```text
  Invalid value for 'PATH': File 'raw.store_sales' does not exist.
  ```
- `trilogy explore raw.web_sales`

  ```text
  Invalid value for 'PATH': File 'raw.web_sales' does not exist.
  ```
- `trilogy explore raw.store_sales`

  ```text
  Invalid value for 'PATH': File 'raw.store_sales' does not exist.
  ```
- `trilogy explore raw.catalog_sales`

  ```text
  Invalid value for 'PATH': File 'raw.catalog_sales' does not exist.
  ```
- `trilogy explore raw.web_sales`

  ```text
  Invalid value for 'PATH': File 'raw.web_sales' does not exist.
  ```
- `trilogy explore raw.store_returns`

  ```text
  Invalid value for 'PATH': File 'raw.store_returns' does not exist.
  ```
- `trilogy explore raw.catalog_returns`

  ```text
  Invalid value for 'PATH': File 'raw.catalog_returns' does not exist.
  ```
- `trilogy explore raw.web_returns`

  ```text
  Invalid value for 'PATH': File 'raw.web_returns' does not exist.
  ```
- `trilogy explore raw/orders.preql`

  ```text
  Invalid value for 'PATH': File 'raw/orders.preql' does not exist.
  ```
- `trilogy explore raw/orders.preql`

  ```text
  Invalid value for 'PATH': File 'raw/orders.preql' does not exist.
  ```
- `trilogy explore raw.orders`

  ```text
  Invalid value for 'PATH': File 'raw.orders' does not exist.
  ```
- `trilogy explore raw.store_sales`

  ```text
  Invalid value for 'PATH': File 'raw.store_sales' does not exist.
  ```
- `trilogy explore raw.customer`

  ```text
  Invalid value for 'PATH': File 'raw.customer' does not exist.
  ```
- `trilogy explore raw.customer_demographic`

  ```text
  Invalid value for 'PATH': File 'raw.customer_demographic' does not exist.
  ```
- `trilogy explore raw.household_demographic`

  ```text
  Invalid value for 'PATH': File 'raw.household_demographic' does not exist.
  ```
- `trilogy explore raw/orders.preql`

  ```text
  Invalid value for 'PATH': File 'raw/orders.preql' does not exist.
  ```
- `trilogy explore raw/orders.preql`

  ```text
  Invalid value for 'PATH': File 'raw/orders.preql' does not exist.
  ```
- `trilogy explore raw/orders.preql`

  ```text
  Invalid value for 'PATH': File 'raw/orders.preql' does not exist.
  ```
- `trilogy explore raw/orders.preql`

  ```text
  Invalid value for 'PATH': File 'raw/orders.preql' does not exist.
  ```
- `trilogy explore raw.orders`

  ```text
  Invalid value for 'PATH': File 'raw.orders' does not exist.
  ```
- `trilogy explore raw/web_sales`

  ```text
  Invalid value for 'PATH': File 'raw/web_sales' does not exist.
  ```
- `trilogy explore raw.orders`

  ```text
  Invalid value for 'PATH': File 'raw.orders' does not exist.
  ```
- `trilogy explore raw.store_sales`

  ```text
  Invalid value for 'PATH': File 'raw.store_sales' does not exist.
  ```
- `trilogy explore raw/orders.preql`

  ```text
  Invalid value for 'PATH': File 'raw/orders.preql' does not exist.
  ```
- `trilogy explore raw/orders.preql`

  ```text
  Invalid value for 'PATH': File 'raw/orders.preql' does not exist.
  ```
- `trilogy explore raw/orders`

  ```text
  Invalid value for 'PATH': File 'raw/orders' does not exist.
  ```

### `other`

- `trilogy file write probe_item_current_guess.preql --run-and-delete`

  ```text
  Resolution error in probe_item_current_guess.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: i.items_at_i_sk_grouped_by_i.id_at_i_id_at_i_id_at_item_current_current_sk_grouped_by__at_item_current_current_sk_join_o.customer.current_address.customer_address_at_o_customer_current_address_sk_join_o.customer.customers_at_o_customer_sk_join_o.item.items_at_o_item_sk_join_o.sale_date.date_at_o_sale_date_sk_join_o.store_sales_at_o_item_sk_o_ticket_number_at_o_item_sk_o_ticket_number_filtered_by_6930458303287640_at_o_customer_current_address_sk_o_item_sk_o_sale_date_month_of_year_o_sale_date_year_o_ticket_number_filtered_by_6038288118153548_at_o_customer_current_address_sk_o_item_sk_o_sale_date_month_of_year_o_sale_date_year_o_ticket_number onto i.items_at_i_sk_filtered_by_6988093325596498_grouped_by_i.category_at_i_category_at_i_category_at_category_avg_category. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_current_avg_price.preql --run-and-delete`

  ```text
  Resolution error in probe_current_avg_price.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: i.items_at_i_sk_filtered_by_6462675653923502_at__subquery_28_22_i_sk_join_o.customer.current_address.customer_address_at_o_customer_current_address_sk_join_o.customer.customers_at_o_customer_sk_join_o.item.items_at_o_item_sk_join_o.sale_date.date_at_o_sale_date_sk_join_o.store_sales_at_o_item_sk_o_ticket_number_at_o_item_sk_o_ticket_number_filtered_by_6930458303287640_at_o_customer_current_address_sk_o_item_sk_o_sale_date_month_of_year_o_sale_date_year_o_ticket_number_filtered_by_2067407202881733_at_o_customer_current_address_sk_o_item_sk_o_sale_date_month_of_year_o_sale_date_year_o_ticket_number onto i.items_at_i_sk_filtered_by_6988093325596498_grouped_by_i.category_i.current_price_i.id_at_i_category_i_current_price_i_id_at_local__item_price_rows_item_category_local__item_price_rows_item_code_local__item_price_rows_item_price_at_item_price_rows_item_category_item_price_rows_item_code_item_price_rows_item_price_at_local__unique_item_prices_item_category_local__unique_item_prices_item_code_local__unique_item_prices_item_price_at_unique_item_prices_item_category_unique_item_prices_item_code_unique_item_prices_item_price_grouped_by_unique_item_prices.item_category_at_unique_item_prices_item_category_at_unique_item_prices_item_category_at_category_avg_category. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write answer_4140546834.preql --run`

  ```text
  Syntax error in answer_4140546834.preql: Output column 'gross_margin' renames 'local.gross_margin' back to the name of an existing concept 'gross_margin' (defined at line 3) that 'local.gross_margin' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'gross_margin_out').
  ```
- `trilogy file write answer_71623752.preql --run`

  ```text
  Syntax error in answer_71623752.preql: Output column 'average_quarterly_sales' renames 'local.average_quarterly_sales' back to the name of an existing concept 'average_quarterly_sales' (defined at line 4) that 'local.average_quarterly_sales' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'average_quarterly_sales_out').
  ```
- `trilogy file write probe943f.preql --run-and-delete`

  ```text
  Syntax error in probe943f.preql: Aggregate concept local.scoped_profit cannot reference itself. If defining a new concept in a select, use a new name.
  ```
- `trilogy file write answer_840315271.preql --run`

  ```text
  Syntax error in answer_840315271.preql: Output column 'difference' renames 'local.difference' back to the name of an existing concept 'difference' (defined at line 15) that 'local.difference' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'difference_out').
  ```
- `trilogy file write answer_1827331030.preql --run`

  ```text
  Syntax error in answer_1827331030.preql: Aggregate concept local.adjusted_amount cannot reference itself. If defining a new concept in a select, use a new name.
  ```

### `undefined-concept`

- `trilogy file write probe_union_rollup.preql --run-and-delete`

  ```text
  Syntax error in probe_union_rollup.preql: 2 undefined concept references; fix all before re-running:
    - channel_label (line 41, col 10, in ORDER BY); did you mean: activity.channel_label, a.channel?
    - entity_id (line 41, col 41, in ORDER BY); did you mean: activity.entity_id?
  ```
- `trilogy file write probe_distinct_item_avg.preql --run-and-delete`

  ```text
  Syntax error in probe_distinct_item_avg.preql: Undefined concept: _item_prices.item_id (line 12, in SELECT). Suggestions: ['distinct_item_prices.item_id', '_distinct_item_prices_item_id', 'distinct_item_prices.category']
  ```
- `trilogy file write probe_distinct_item_avg.preql --run-and-delete`

  ```text
  Syntax error in probe_distinct_item_avg.preql: Undefined concept: _item_prices.item_id (line 12, in SELECT). Suggestions: ['distinct_item_prices.item_id', '_distinct_item_prices_item_id', 'distinct_item_prices.category']
  ```
- `trilogy file write answer_4140546834.preql --run`

  ```text
  Syntax error in answer_4140546834.preql: Undefined concept: _virt_agg_grouping_701977002600206.
  ```
- `trilogy file write probe_rank_auto.preql --run-and-delete`

  ```text
  Syntax error in probe_rank_auto.preql: Undefined concept: _virt_agg_grouping_701977002600206.
  ```
- `trilogy file write probe_3247656127_source.preql --run-and-delete`

  ```text
  Syntax error in probe_3247656127_source.preql: Undefined concept: ss.pos_bill_address.gmt_offset (line 7, col 9, in WHERE). Suggestions: ['ss.pos_address.gmt_offset', 'ss.return_address.gmt_offset', 'ss.store.gmt_offset', 'ss.customer.current_address.gmt_offset', 'ss.return_customer.current_address.gmt_offset', 'ss.return_store.gmt_offset']
  ```

### `syntax-parse`

- `trilogy file write probe_item_dupes.preql --run-and-delete`

  ```text
  refused to write 'probe_item_dupes.preql': not syntactically valid Trilogy.

  Parse error:
   --> 8:52
    |
  8 | having sk_count > 1 or categories > 1 or prices > 1,
    |                                                    ^---
    |
    = expected limit, order_by, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...r categories > 1 or prices > 1 ??? ,
   limit 20;
  ```
- `trilogy file write answer_219203908.preql --run`

  ```text
  refused to write 'answer_219203908.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [211]: Expression in `by` clause must be wrapped in parens - write `by (expr1, expr2, ...)`. Bare identifiers (`by a, b`) work without parens, but any function call, cast, or other expression needs them.
  Location:
  ...e_date.month_of_year = 11)
   ) ??? by lower(o.channel), o.item.br...
  ```
- `trilogy file write answer_219203908.preql --run`

  ```text
  refused to write 'answer_219203908.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [211]: Expression in `by` clause must be wrapped in parens - write `by (expr1, expr2, ...)`. Bare identifiers (`by a, b`) work without parens, but any function call, cast, or other expression needs them.
  Location:
  ...e_date.month_of_year = 11)
   ) ??? by (lower(o.channel)), o.item....
  ```
- `trilogy file write probe2844519538c.preql --run-and-delete`

  ```text
  refused to write 'probe2844519538c.preql': not syntactically valid Trilogy.

  Parse error:
    --> 35:1
     |
  35 | order by last_name asc, first_name asc, store_name asc;
     | ^---
     |
     = expected EOI, block, or show_statement
  Location:
  ...ving total_peach_sales > 0;
   ??? order by last_name asc, first_...
  ```
- `trilogy file write probe_final_shape.preql --run-and-delete`

  ```text
  refused to write 'probe_final_shape.preql': not syntactically valid Trilogy.

  Parse error:
    --> 19:6
     |
  19 |     --     then o.item.category else null end as leaf_category_sort,
     |      ^---
     |
     = expected access_chain
  Location:
  ...uping(o.item.class) = 0
       - ??? -     then o.item.category els...
  ```

### `import-path`

- `trilogy file write probe_orders_import.preql --run-and-delete`

  ```text
  Import error in probe_orders_import.preql: Unable to import '.\raw\orders.preql': [Errno 2] No such file or directory: '.\\raw\\orders.preql'.
  ```
