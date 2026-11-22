# Trilogy failure analysis — 20261122-145209

- Run `20261122-145208_enriched_deepseek_deepseek-v4-flash` | `deepseek/deepseek-v4-flash` | sf=1
- `trilogy` calls: 1286 | failed: 47 (4%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 19 | 40% |
| `other` | 15 | 32% |
| `undefined-concept` | 7 | 15% |
| `join-resolution` | 4 | 9% |
| `no-output` | 2 | 4% |

## Detail

### `syntax-parse`

- `trilogy file write answer_3863442186.preql --run`

  ```text
  refused to write 'answer_3863442186.preql': not syntactically valid Trilogy.

  Parse error:
   --> 5:11
    |
  5 | with v as (
    |           ^---
    |
    = expected select_statement, tvf_union_invocation, tvf_except_invocation, tvf_intersect_invocation, tvf_rollup_invocation, tvf_cube_invocation, or tvf_grouping_sets_invocation
  Location:
   + ext_sales_price  with v as ??? (      where s.sale_line_item_...
  ```
- `trilogy file write probe15.preql --run-and-delete`

  ```text
  refused to write 'probe15.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [231]: A `subset|union|equal join` cannot follow a trailing `where`. Put the join right after the select list, with the filter either before `select` or after the join: `where <filters> select <cols> subset join a.key = b.key` or `select <cols> subset join a.key = b.key where <filters>`. Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...ll and st.ticket is not null  ??? union join st.cust = cat.cust
  ```
- `trilogy file write answer_1798498862.preql --run`

  ```text
  refused to write 'answer_1798498862.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [231]: A `subset|union|equal join` cannot follow a trailing `where`. Put the join right after the select list, with the filter either before `select` or after the join: `where <filters> select <cols> subset join a.key = b.key` or `select <cols> subset join a.key = b.key where <filters>`. Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
   and cs.quantity is not null  ??? union join ss.item.sk = cs.ite...
  ```
- `trilogy file write probe_a.preql --run-and-delete`

  ```text
  refused to write 'probe_a.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [202]: Missing closing semicolon? Statements must be terminated with a semicolon `;`.
  Location:
  ...acturer_id)) as elec_manufs,  ???
  ```
- `trilogy file write probe_b.preql --run-and-delete`

  ```text
  refused to write 'probe_b.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [202]: Missing closing semicolon? Statements must be terminated with a semicolon `;`.
  Location:
  ...s.ext_sales_price) as total,  ???
  ```
- `trilogy file write answer_4140546834.preql --run`

  ```text
  refused to write 'answer_4140546834.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:69
    |
  4 | auto gross_margin <- (sum(ss.net_profit) / sum(ss.ext_sales_price)) as gross_margin;
    |                                                                     ^---
    |
    = expected LOGICAL_OR or LOGICAL_AND
  Location:
  ...t) / sum(ss.ext_sales_price)) ??? as gross_margin;  auto g_cat <...
  ```
- `trilogy file write probe_rows.preql --run-and-delete`

  ```text
  refused to write 'probe_rows.preql': not syntactically valid Trilogy.

  Parse error:
   --> 5:21
    |
  5 | with store_pairs as (
    |                     ^---
    |
    = expected select_statement, tvf_union_invocation, tvf_except_invocation, tvf_intersect_invocation, tvf_rollup_invocation, tvf_cube_invocation, or tvf_grouping_sets_invocation
  Location:
  ...as ws;    with store_pairs as ??? (      where ss.customer.sk is...
  ```
- `trilogy file write probe3.preql --run-and-delete`

  ```text
  refused to write 'probe3.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...unt(ws.line_item) as n_lines  ??? group by zip_code  order by n_...
  ```
- `trilogy file write probe13.preql --run-and-delete`

  ```text
  refused to write 'probe13.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [202]: Missing closing semicolon? Statements must be terminated with a semicolon `;`.
  Location:
  ...n target_item_id) as n_by_id  ???
  ```
- `trilogy file write probe16.preql --run-and-delete`

  ```text
  refused to write 'probe16.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...dress.state = ss.store.state  ??? group by () ;
  ```
- `trilogy file write probe_dup.preql --run-and-delete`

  ```text
  refused to write 'probe_dup.preql': not syntactically valid Trilogy.

  Parse error:
   --> 8:1
    |
  8 | from_placeholder();
    | ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...t_total) as max_ticket_total  ??? from_placeholder();
  ```
- `trilogy file write probe5.preql --run-and-delete`

  ```text
  refused to write 'probe5.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...(ss.ext_sales_price) as total ??? from raw.store_sales ss where
  ```
- `trilogy file write probe5.preql --run-and-delete`

  ```text
  refused to write 'probe5.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:46
    |
  7 |     (select sum(ss.ext_sales_price) as total from_marker where ss.item.manager_id = 1 and ss.sale_date.year = 1999 and ss.sale_date.month_of_year = 11 and (ss.sale_time.meal_time = 'breakfast' or ss.sale_time.meal_time = 'dinner')),
    |                                              ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...(ss.ext_sales_price) as total ??? from_marker where ss.item.mana...
  ```
- `trilogy file write probe4.preql --run-and-delete`

  ```text
  refused to write 'probe4.preql': not syntactically valid Trilogy.

  Parse error:
   --> 5:16
    |
  5 |   count(s.grain(s.item.sk, s.channel, s.order_id)) as grain_rows,
    |                ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...l as channel,    count(s.grain ??? (s.item.sk, s.channel, s.order...
  ```
- `trilogy file write probe5.preql --run-and-delete`

  ```text
  refused to write 'probe5.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:17
    |
  4 |   count(ws.grain(ws.item.sk, ws.order_number)) as grain_rows,
    |                 ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...s;    select    count(ws.grain ??? (ws.item.sk, ws.order_number))...
  ```
- `trilogy file write probe4.preql --run-and-delete`

  ```text
  refused to write 'probe4.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [233]: Trilogy has no select-level `by` clause - remove it. Grouping is automatic by the non-aggregated select columns, and a select of only aggregates already returns one row. To pin ONE aggregate's grain, put the `by` on that aggregate, before its alias: `sum(x) by * as total` or `sum(x) by store.id as total`. (For subtotals wrap the select in a call: `rollup((select ...), key1, key2);`.)
  Location:
  ...und <= 88128) as income_cust  ??? by *;
  ```
- `trilogy file write probe_b.preql --run-and-delete`

  ```text
  refused to write 'probe_b.preql': not syntactically valid Trilogy.

  Parse error:
   --> 3:11
    |
  3 | with s as (where ss.sale_date.year = 2000 and ss.customer.sk is not null
    |           ^---
    |
    = expected select_statement, tvf_union_invocation, tvf_except_invocation, tvf_intersect_invocation, tvf_rollup_invocation, tvf_cube_invocation, or tvf_grouping_sets_invocation
  Location:
  ...ore_sales as ss;    with s as ??? (where ss.sale_date.year = 200...
  ```
- `trilogy file write probe3.preql --run-and-delete`

  ```text
  refused to write 'probe3.preql': not syntactically valid Trilogy.

  Parse error:
    --> 10:68
     |
  10 | where ss.return_reason.desc = 'reason 28' and ss.customer.sk in 693, 893
     |                                                                    ^---
     |
     = expected limit, order_by, THEN_LA, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
   28' and ss.customer.sk in 693 ??? , 893  order by customer_sk;
  ```
- `trilogy file write probe2.preql --run-and-delete`

  ```text
  refused to write 'probe2.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...ar(cs.ship_date.date) = 2000  ??? group by cs.warehouse.sk, cs.w...
  ```

### `other`

- `trilogy file write probe_union.preql --run-and-delete`

  ```text
  Syntax error in probe_union.preql: union(...) requires at least two relational arms.
  ```
- `trilogy file write probe_u3.preql --run-and-delete`

  ```text
  Resolution error in probe_u3.preql: Planner emitted a keyless join between row-bearing sources that share a join axis or one source relation: (ss.customer.customers_at_ss_customer_sk_join_ss.sale_date.date_at_ss_sale_date_sk_join_ss.store_sales_at_ss_item_sk_ss_ticket_number_grouped_by_ss.customer.sk_ss.sale_date.sk_at_ss_customer_sk_ss_sale_date_sk)_at_ss_customer_sk_ss_sale_date_sk_filtered_by_969642762945098_at_ss_customer_sk_ss_sale_date_sk_grouped_by_local.___stacked_arm_0_sale_date_ss.sale_date.sk_at_ss_sale_date_sk onto (ss.customer.customers_at_ss_customer_sk_join_ss.sale_date.date_at_ss_sale_date_sk_join_ss.store_sales_at_ss_item_sk_ss_ticket_number_grouped_by_ss.customer.sk_ss.sale_date.sk_at_ss_customer_sk_ss_sale_date_sk)_at_ss_customer_sk_ss_sale_date_sk_filtered_by_969642762945098_at_ss_customer_sk_ss_sale_date_sk_grouped_by_local.___stacked_arm_0_channel_local.___stacked_arm_0_last_name_local.__stacked_arm_0__arm_ss.customer.sk_at_ss_customer_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write answer_142117636.preql --run`

  ```text
  Syntax error in answer_142117636.preql: Output column 'jan_mean' renames 'local.jan_mean' back to the name of an existing concept 'jan_mean' (defined at line 3) that 'local.jan_mean' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'jan_mean_out').
  ```
- `trilogy file write probe_a.preql --run-and-delete`

  ```text
  Syntax error in probe_a.preql: Output column 'benchmark' renames 'ranked.benchmark' back to the name of an existing concept 'benchmark' (defined at line 3) that 'ranked.benchmark' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'benchmark_out').
  ```
- `trilogy file write answer_4037365554.preql --run`

  ```text
  Syntax error in answer_4037365554.preql: Output column 'month_total' renames 'local.month_total' back to the name of an existing concept 'month_total' (defined at line 3) that 'local.month_total' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'month_total_out').
  ```
- `trilogy file write probe_a.preql --run-and-delete --run`

  ```text
  Pass at most one of --run or --run-and-delete.
  ```
- `trilogy file write probe2.preql --run-and-delete`

  ```text
  Unexpected error in probe2.preql: (_duckdb.BinderException) Binder Error: GROUP BY clause cannot contain aggregates!

  LINE 3:     rank() over (order by sum("ss_store_sales"."SS_NET_PROFIT") desc ) as "top_states...
                                    ^
  [SQL: SELECT
      "ss_store_store"."S_STATE" as "top_states_state",
      rank() over (order by sum("ss_store_sales"."SS_NET_PROFIT") desc ) as "top_states_rnk",
      sum("ss_store_sales"."SS_NET_PROFIT") as "top_states_p"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "date_dim" as "ss_sale_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_sale_date_date"."D_DATE_SK"
      INNER JOIN "store" as "ss_store_store" on "ss_store_sales"."SS_STORE_SK" = "ss_store_store"."S_STORE_SK"
  WHERE
      year(CASE WHEN regexp_full_match(CAST("ss_sale_date_date"."D_DATE" AS VARCHAR), '^[[:space:]]*[0-9][0-9][0-9][0-9](-[0-9][0-9]?-[0-9][0-9]?|/[0-9][0-9]?/[0-9][0-9]?)([ T]([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]([.][0-9]+)?)?[[:space:]]*$') THEN TRY_CAST(trim(CAST("ss_sale_date_date"."D_DATE" AS VARCHAR), ' ' || chr(9) || chr(10) || chr(13)) AS date) END) = 2000

  GROUP BY
      1,
      2
  ORDER BY
      "top_states_rnk" asc
  LIMIT (10)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy file write answer_1772060640.preql --run`

  ```text
  Syntax error in answer_1772060640.preql: ORDER BY references 'ss.customer.sk', which is not in the SELECT projection (line 3). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --ss.customer.sk order by ss.customer.sk asc`.
  ```
- `trilogy file write probe_bench.preql --run-and-delete`

  ```text
  Unexpected error in probe_bench.preql: Could not render the query: Missing source reference to cs.return_customer.id. A planned reference has no backing source CTE -- typically an unsupported cross-rowset or membership shape the planner could not wire. Review the rowset/join structure (or file an issue if the query looks valid).

  Full SQL with sentinel(s):

  WITH
  juicy as MATERIALIZED (
  SELECT
      "cs_catalog_returns"."CR_RETURNING_ADDR_SK" as "cs_return_address_sk",
      "cs_catalog_returns"."CR_RETURNING_CUSTOMER_SK" as "cs_return_customer_sk",
      "cs_return_address_customer_address"."CA_STATE" as "cs_return_address_state",
      "cs_return_customer_current_address_customer_address"."CA_STATE" as "cs_return_customer_current_address_state",
      "cs_return_customer_customers"."C_CUSTOMER_ID" as "cs_return_customer_id"
  FROM
      "catalog_returns" as "cs_catalog_returns"
      INNER JOIN "date_dim" as "cs_return_date_date" on "cs_catalog_returns"."CR_RETURNED_DATE_SK" = "cs_return_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "customer_address" as "cs_return_address_customer_address" on "cs_catalog_returns"."CR_RETURNING_ADDR_SK" = "cs_return_address_customer_address"."CA_ADDRESS_SK"
      LEFT OUTER JOIN "customer" as "cs_return_customer_customers" on "cs_catalog_returns"."CR_RETURNING_CUSTOMER_SK" = "cs_return_customer_customers"."C_CUSTOMER_SK"
      LEFT OUTER JOIN "customer_address" as "cs_return_customer_current_address_customer_address" on "cs_return_customer_customers"."C_CURRENT_ADDR_SK" = "cs_return_customer_current_address_customer_address"."CA_ADDRESS_SK"),
  questionable as (
  SELECT
      "cs_catalog_returns"."CR_ITEM_SK" as "cs_item_sk",
      "cs_catalog_returns"."CR_ORDER_NUMBER" as "cs_order_number",
      "cs_return_address_customer_address"."CA_STATE" as "cs_return_address_state",
      "cs_return_customer_customers"."C_CUSTOMER_ID" as "cs_return_customer_id",
      CASE WHEN regexp_full_match(CAST("cs_return_date_date"."D_DATE" AS VARCHAR), '^[[:space:]]*[0-9][0-9][0-9][0-9](-[0-9][0-9]?-[0-9][0-9]?|/[0-9][0-9]?/[0-9][0-9]?)([ T]([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]([.][0-9]+)?)?[[:space:]]*$') THEN TRY_CAST(trim(CAST("cs_return_date_date"."D_DATE" AS VARCHAR), ' ' || chr(9) || chr(10) || chr(13)) AS date) END as "cs_return_date_date"
  FROM
      "catalog_returns" as "cs_catalog_returns"
      INNER JOIN "date_dim" as "cs_return_date_date" on "cs_catalog_returns"."CR_RETURNED_DATE_SK" = "cs_return_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "customer_address" as "cs_return_address_customer_address" on "cs_catalog_returns"."CR_RETURNING_ADDR_SK" = "cs_return_address_customer_address"."CA_ADDRESS_SK"
      LEFT OUTER JOIN "customer" as "cs_return_customer_customers" on "cs_catalog_returns"."CR_RETURNING_CUSTOMER_SK" = "cs_return_customer_customers"."C_CUSTOMER_SK"),
  wakeful as (
  SELECT
      "cs_catalog_returns"."CR_ORDER_NUMBER" as "cs__returned_order_number",
      "cs_catalog_returns"."CR_RETURN_AMT_INC_TAX" as "cs_return_amount_inc_tax",
      "cs_catalog_sales"."CS_ITEM_SK" as "cs_item_sk",
      "cs_catalog_sales"."CS_ORDER_NUMBER" as "cs_order_number"
  FROM
      "catalog_sales" as "cs_catalog_sales"
      LEFT OUTER JOIN "catalog_returns" as "cs_catalog_returns" on "cs_catalog_sales"."CS_ITEM_SK" = "cs_catalog_returns"."CR_ITEM_SK" AND "cs_catalog_sales"."CS_ORDER_NUMBER" = "cs_catalog_returns"."CR_ORDER_NUMBER"),
  abundant as (
  SELECT
      "questionable"."cs_return_address_state" as "cs_return_address_state",
      "questionable"."cs_return_customer_id" as "cs_return_customer_id",
      sum(CASE WHEN ( ("wakeful"."cs__returned_order_number" is not null) and year("questionable"."cs_return_date_date") = 2000 and "questionable"."cs_return_address_state" is not null ) THEN "wakeful"."cs_return_amount_inc_tax" ELSE NULL END) as "ctr_total"
  FROM
      "wakeful"
      LEFT OUTER JOIN "questionable" on "wakeful"."cs_item_sk" = "questionable"."cs_item_sk" AND "wakeful"."cs_order_number" = "questionable"."cs_order_number"
  GROUP BY
      1,
      2),
  vacuous as (
  SELECT
      "abundant"."ctr_total" as "ctr_total",
      CASE WHEN "juicy"."cs_return_customer_current_address_state" = 'GA' THEN "abundant"."ctr_total" ELSE NULL END as "_virt_filter_ctr_total_5900667051553331",
      coalesce("abundant"."cs_return_address_state","juicy"."cs_return_address_state") as "cs_return_address_state"
  FROM
      "juicy"
      FULL JOIN "abundant" on "juicy"."cs_return_address_state" is not distinct from "abundant"."cs_return_address_state" AND "juicy"."cs_return_customer_id" is not distinct from "abundant"."cs_return_customer_id"
  GROUP BY
      1,
      2,
      3,
      coalesce("abundant"."cs_return_customer_id","juicy"."cs_return_customer_id")),
  sparkling as (
  SELECT
      "vacuous"."cs_return_address_state" as "cs_return_address_state",
      avg("vacuous"."_virt_filter_ctr_total_5900667051553331") as "avg_ga_only",
      avg("vacuous"."_virt_filter_ctr_total_5900667051553331") as "state_avg_gaonly",
      avg("vacuous"."ctr_total") as "avg_full",
      avg("vacuous"."ctr_total") as "state_avg_full"
  FROM
      "vacuous"
  GROUP BY
      1),
  abhorrent as (
  SELECT
      "juicy"."cs_return_address_sk" as "cs_return_address_sk",
      "juicy"."cs_return_address_state" as "cs_return_address_state",
      "juicy"."cs_return_customer_id" as "cs_return_customer_id",
      "juicy"."cs_return_customer_sk" as "cs_return_customer_sk",
      md5(CONCAT_WS('', coalesce(cast("juicy"."cs_return_customer_id" as string),'
  '), coalesce(cast("juicy"."cs_return_address_state" as string),'
  '))) as "_virt_func_hash_5481348069475797"
  FROM
      "juicy"),
  sweltering as (
  SELECT
      coalesce("abhorrent"."cs_return_address_state","abundant"."cs_return_address_state","sparkling"."cs_return_address_state") as "cs_return_address_state",
      max(CASE WHEN "abundant"."ctr_total" > 1.2 * "sparkling"."state_avg_full" THEN "abhorrent"."_virt_func_hash_5481348069475797" ELSE NULL END) as "_virt_filter_8271892138296656",
      max(CASE WHEN "abundant"."ctr_total" > 1.2 * "sparkling"."state_avg_gaonly" THEN "abhorrent"."_virt_func_hash_5481348069475797" ELSE NULL END) as "_virt_filter_7431417254935112"
  FROM
      "abhorrent"
      FULL JOIN "abundant" on "abhorrent"."cs_return_address_state" is not distinct from "abundant"."cs_return_address_state" AND "abhorrent"."cs_return_customer_id" is not distinct from "abundant"."cs_return_customer_id"
      FULL JOIN "sparkling" on coalesce("abhorrent"."cs_return_address_state", "abundant"."cs_return_address_state") is not distinct from "sparkling"."cs_return_address_state"
  GROUP BY
      1,
      "abhorrent"."cs_return_address_sk",
      "abhorrent"."cs_return_customer_sk"),
  scrawny as (
  SELECT
      "sweltering"."cs_return_address_state" as "cs_return_address_state",
      count("sweltering"."_virt_filter_7431417254935112") as "pass_ga_pop",
      count("sweltering"."_virt_filter_8271892138296656") as "pass_full_pop",
      count(md5(CONCAT_WS('', coalesce(cast(INVALID_REFERENCE_BUG<Missing source reference to cs.return_customer.id> as string),'
  '), coalesce(cast("sweltering"."cs_return_address_state" as string),'
  ')))) as "n_cust_groups"
  FROM
      "sweltering"
  GROUP BY
      1)
  SELECT
      "scrawny"."cs_return_address_state" as "cs_return_address_state",
      "scrawny"."n_cust_groups" as "n_cust_groups",
      "sparkling"."avg_full" as "avg_full",
      "sparkling"."avg_ga_only" as "avg_ga_only",
      "scrawny"."pass_full_pop" as "pass_full_pop",
      "scrawny"."pass_ga_pop" as "pass_ga_pop"
  FROM
      "scrawny"
      INNER JOIN "sparkling" on "scrawny"."cs_return_address_state" is not distinct from "sparkling"."cs_return_address_state"
  ORDER BY
      "scrawny"."cs_return_address_state" asc
  LIMIT (100)
  ```
- `trilogy file write probe2.preql --run-and-delete`

  ```text
  Syntax error in probe2.preql: HAVING filters on a dimension outside the SELECT projection, but the select has no grain key to anchor a post-aggregation semijoin (line 15). Move the filter to WHERE to filter before aggregation.
  ```
- `trilogy file write probe3.preql --run-and-delete`

  ```text
  Syntax error in probe3.preql: HAVING filters on a dimension outside the SELECT projection, but the select has no grain key to anchor a post-aggregation semijoin (line 15). Move the filter to WHERE to filter before aggregation.
  ```
- `trilogy file write probe8.preql --run-and-delete`

  ```text
  Resolution error in probe8.preql: WHERE atom BuildSubselectComparison(left=ss.return_customer_demographic.sk@Grain<ss.return_customer_demographic.sk>, right=edge_keys.cdemo@Grain<edge_keys.cdemo>, operator=<ComparisonOperator.IN: 'in'>, null_free_members=False) reads ['ss.return_customer_demographic.sk'] but could only be placed after the aggregates ['local.n_lines'] (disconnected_gate); they would never see it.
  ```
- `trilogy file write answer_2852230229.preql --run`

  ```text
  Syntax error in answer_2852230229.preql: Output column 'rnk' renames 'local.rnk' back to the name of an existing concept 'rnk' (defined at line 8) that 'local.rnk' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'rnk_out').
  ```
- `trilogy file write probe_a.preql --run-and-delete`

  ```text
  Resolution error in probe_a.preql: Planner emitted a keyless join between row-bearing sources that share a join axis or one source relation: (ss.customer.customers_at_ss_customer_sk_join_ss.sale_date.date_at_ss_sale_date_sk_join_ss.store_sales_at_ss_item_sk_ss_ticket_number_grouped_by_ss.customer.sk_ss.sale_date.sk_at_ss_customer_sk_ss_sale_date_sk)_at_ss_customer_sk_ss_sale_date_sk_filtered_by_2067127161725241_at_ss_customer_sk_ss_sale_date_sk_grouped_by_local.___per_channel_arm_0_sd_ss.sale_date.sk_at_ss_sale_date_sk onto (ss.customer.customers_at_ss_customer_sk_join_ss.sale_date.date_at_ss_sale_date_sk_join_ss.store_sales_at_ss_item_sk_ss_ticket_number_grouped_by_ss.customer.sk_ss.sale_date.sk_at_ss_customer_sk_ss_sale_date_sk)_at_ss_customer_sk_ss_sale_date_sk_filtered_by_2067127161725241_at_ss_customer_sk_ss_sale_date_sk_grouped_by_local.___per_channel_arm_0_channel_local.___per_channel_arm_0_fn_local.___per_channel_arm_0_ln_local.__per_channel_arm_0__arm_ss.customer.sk_at_ss_customer_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_null.preql --run-and-delete`

  ```text
  Syntax error in probe_null.preql: ORDER BY references 'local.monthly_total', which is not in the SELECT projection (line 6). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --local.monthly_total order by local.monthly_total asc`.
  ```

### `undefined-concept`

- `trilogy file write answer_3705756794.preql --run`

  ```text
  Syntax error in answer_3705756794.preql: 2 undefined concept references; fix all before re-running:
    - channel (line 45, col 10, in ORDER BY); did you mean: a.channel, stacked.channel, a.channel_dim_id?
    - entity_id (line 45, col 35, in ORDER BY); did you mean: stacked.entity_id?
  ```
- `trilogy file write probe_as.preql --run-and-delete`

  ```text
  Syntax error in probe_as.preql: Undefined concept: s.customer.last_name (line 8, in SELECT). Suggestions: ['s.ship_customer.last_name', 's.billing_customer.last_name', 's.purchasing_customer.last_name']
  ```
- `trilogy file write answer_609701806.preql --run`

  ```text
  Syntax error in answer_609701806.preql: Undefined concept: ss.sale_date.month (line 3, col 36, in WHERE). Suggestions: ['ss.sale_date.month_seq', 'ss.sale_date.day_of_month', 'ss.sale_date.month_of_year']
  ```
- `trilogy file write answer_145690531.preql --run`

  ```text
  Syntax error in answer_145690531.preql: Undefined concept: p.lines. Suggestions: ['per_ch.lines']
  ```
- `trilogy file write probe2.preql --run-and-delete`

  ```text
  Syntax error in probe2.preql: Undefined concept: ss.grain_line (line 8, in SELECT).
  ```
- `trilogy file write answer_2852230229.preql --run`

  ```text
  Syntax error in answer_2852230229.preql: Undefined concept: _virt_agg_grouping_5549377253046357.
  ```
- `trilogy file write probe1.preql --run-and-delete`

  ```text
  Syntax error in probe1.preql: Undefined concept: _virt_agg_grouping_164218295708188.
  ```

### `join-resolution`

- `trilogy file write probe2.preql --run-and-delete`

  ```text
  Resolution error in probe2.preql: Query is unresolvable: no complete sources found for output concepts {'local.n_nullretdate'}. These concepts could only be resolved from partial sources.
  ```
- `trilogy file write probe8.preql --run-and-delete`

  ```text
  Resolution error in probe8.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 16). The requested concepts split into 2 disconnected subgraphs: {busiest_date (= dt.d), busiest_lines (= dt.lines_per_date)}; {top_cnt (= iddt.c), top_item (= iddt.i)}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file write probe_check.preql --run-and-delete`

  ```text
  Resolution error in probe_check.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 7). The requested concepts split into 2 disconnected subgraphs: {cs_orders}; {ws_ext, ws_lines, ws_year (= ws.sale_date.year)}.
    - `ws.sale_date.year` (as `ws_year`) is disconnected, did you mean `cs.ship_customer.first_sales_date.year`? (connected to the other concepts)
  These look like separately-imported copies of models already reachable through a connected import; chain through that path (e.g. `cs.ship_customer.first_sales_date.year`) instead of importing a second, disconnected copy.
  ```
- `trilogy file write answer_2374450308.preql --run`

  ```text
  Resolution error in answer_2374450308.preql: Could not resolve connections for query with output ['local.item_code<Purpose.PROPERTY>Derivation.BASIC>', 'local.description<Purpose.PROPERTY>Derivation.BASIC>', 'local.current_price<Purpose.PROPERTY>Derivation.BASIC>'] from current model. Group grp:root:root_d1:∅ (Derivation.ROOT) could not be built, and nothing else delivers what it owes: WHERE atoms [], readers ['grp:[@condition]basic:d1:∅:sig:1940c2ff9524', 'grp:[@condition]filter:d1:∅:existence:local.inv_ok_items'], outputs ['inv.date.date', 'inv.item.sk', 'inv.quantity_on_hand', 'ss.item.sk', 'ss.ticket_number'].
  ```

### `no-output`

- `trilogy file write probe_g.preql --run-and-delete --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,1842…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Nothing was executed: parsed 3 definition statement(s) (1 concept, 1 import, 1 rowset) but none produce output. Did you mean to include a SELECT statement, or run a refresh on datasources instead?
  ```
- `trilogy file write probe9.preql --run-and-delete`

  ```text
  Nothing was executed: parsed 7 definition statement(s) (5 rowsets, 2 imports) but none produce output. Did you mean to include a SELECT statement, or run a refresh on datasources instead?
  ```
