# Trilogy failure analysis — 20261017-225410

- Run `20261017-225406_ingest_deepseek_deepseek-flash` | `deepseek/deepseek-flash` | sf=1
- `trilogy` calls: 1257 | failed: 89 (7%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 30 | 34% |
| `other` | 21 | 24% |
| `undefined-concept` | 19 | 21% |
| `join-resolution` | 17 | 19% |
| `type-error` | 1 | 1% |
| `planner-recursion` | 1 | 1% |

## Detail

### `syntax-parse`

- `trilogy file write probe1.preql --run-and-delete`

  ```text
  refused to write 'probe1.preql': not syntactically valid Trilogy.

  Parse error:
    --> 16:12
     |
  16 |     sum(sr return_dummy) as x;
     |            ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ..._pairs_per_store,      sum(sr ??? return_dummy) as x;
  ```
- `trilogy file write probe_3863442186.preql --run-and-delete`

  ```text
  refused to write 'probe_3863442186.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...01 > w02 / w01)) as qualify,  ??? from (      select          ch...
  ```
- `trilogy file write probe1.preql --run-and-delete`

  ```text
  refused to write 'probe1.preql': not syntactically valid Trilogy.

  Parse error:
    --> 63:5
     |
  63 | by *;
     |     ^---
     |
     = expected access_chain
  Location:
  ...id))) as n_three_tuples,  by * ??? ;
  ```
- `trilogy file write probe2.preql --run-and-delete`

  ```text
  refused to write 'probe2.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...ales.category_id is not null  ??? group by nothing_placeholder;
  ```
- `trilogy file write probe3.preql --run-and-delete`

  ```text
  refused to write 'probe3.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a NON-NULL GRAIN KEY: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). It MUST be a key, and one that is not nullable: `count(x)` skips rows where `x` is NULL, so counting a nullable property (a name, a date, any optional field) silently undercounts. When the grain takes SEVERAL keys, name them with `grain(...)`: `count(grain(order_id, item.id))` counts order+item combinations, and `count_distinct(grain(first_name, last_name, sale_date))` counts distinct combinations - `grain()` is never NULL, so combinations with a missing member still count. For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...g.zip) as groups_with_zip,    ??? count(*) as groups  from g;
  ```
- `trilogy file write probe_check.preql --run-and-delete`

  ```text
  refused to write 'probe_check.preql': not syntactically valid Trilogy.

  Parse error:
    --> 15:1
     |
  15 | union(
     | ^---
     |
     = expected limit, order_by, THEN_LA, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...county = 'Williamson County'  ??? union(   select 'multi_wh', co...
  ```
- `trilogy file write probe1.preql --run-and-delete`

  ```text
  refused to write 'probe1.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [231]: A `subset|union join` cannot follow a trailing `where`. Put the join right after the select list, with the filter either before `select` or after the join: `where <filters> select <cols> subset join a.key = b.key` or `select <cols> subset join a.key = b.key where <filters>`. Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...here ss.date_dim.year = 2001  ??? subset join ss.item.item_sk =
  ```
- `trilogy file write probe1.preql --run-and-delete`

  ```text
  refused to write 'probe1.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [231]: A `subset|union join` cannot follow a trailing `where`. Put the join right after the select list, with the filter either before `select` or after the join: `where <filters> select <cols> subset join a.key = b.key` or `select <cols> subset join a.key = b.key where <filters>`. Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...ld_date.year in (2001, 2002)  ??? subset join ss.item.item_sk =
  ```
- `trilogy file write probe1.preql --run-and-delete`

  ```text
  refused to write 'probe1.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:41
    |
  7 |   and lower(ss.customer.birth_country) <> lower(ss.customer.customer_address.country)
    |                                         ^---
    |
    = expected sum_operator
  Location:
  ...r(ss.customer.birth_country) < ??? > lower(ss.customer.customer_a...
  ```
- `trilogy file write answer_3825713089.preql --run`

  ```text
  refused to write 'answer_3825713089.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [231]: A `subset|union join` cannot follow a trailing `where`. Put the join right after the select list, with the filter either before `select` or after the join: `where <filters> select <cols> subset join a.key = b.key` or `select <cols> subset join a.key = b.key where <filters>`. Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
   cs.order_number is not null  ??? union join ss.item.item_sk = s...
  ```
- `trilogy file write answer_1798498862.preql --run`

  ```text
  refused to write 'answer_1798498862.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [231]: A `subset|union join` cannot follow a trailing `where`. Put the join right after the select list, with the filter either before `select` or after the join: `where <filters> select <cols> subset join a.key = b.key` or `select <cols> subset join a.key = b.key where <filters>`. Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...nd sr_agg.sr_qty is not null  ??? subset join ss_agg.item_sk = s...
  ```
- `trilogy file write probe3_2802535988.preql --run-and-delete`

  ```text
  refused to write 'probe3_2802535988.preql': not syntactically valid Trilogy.

  Parse error:
   --> 3:18
    |
  3 | with per_cust as (
    |                  ^---
    |
    = expected select_statement, tvf_union_invocation, tvf_except_invocation, or tvf_intersect_invocation
  Location:
  ...ns as wr;    with per_cust as ??? (      where wr.date_dim.year
  ```
- `trilogy file write probe_win.preql --run-and-delete`

  ```text
  refused to write 'probe_win.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [202]: Missing closing semicolon? Statements must be terminated with a semicolon `;`.
  Location:
  ...tem_sk)) as qualifying_lines  ???
  ```
- `trilogy file write probe_ref.preql --run-and-delete`

  ```text
  refused to write 'probe_ref.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [202]: Missing closing semicolon? Statements must be terminated with a semicolon `;`.
  Location:
  ...ext_discount_amt) as avg_all  ???
  ```
- `trilogy file write probe_check.preql --run-and-delete`

  ```text
  refused to write 'probe_check.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [202]: Missing closing semicolon? Statements must be terminated with a semicolon `;`.
  Location:
  ...ss.item.item_sk) as item_cnt  ???
  ```
- `trilogy file write answer_630074061.preql --run`

  ```text
  refused to write 'answer_630074061.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [231]: A `subset|union join` cannot follow a trailing `where`. Put the join right after the select list, with the filter either before `select` or after the join: `where <filters> select <cols> subset join a.key = b.key` or `select <cols> subset join a.key = b.key where <filters>`. Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...use.warehouse_sk is not null  ??? subset join cr.order_number =
  ```
- `trilogy file write probe_e.preql --run-and-delete`

  ```text
  refused to write 'probe_e.preql': not syntactically valid Trilogy.

  Parse error:
    --> 15:1
     |
  15 | by *;
     | ^---
     |
     = expected limit, order_by, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...size = 'N/A')         )) > 0  ??? by *;
  ```
- `trilogy file write probe_nulls.preql --run-and-delete`

  ```text
  refused to write 'probe_nulls.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:63
    |
  4 |        count(grain(ss.item.item_sk, ss.ticket_number) ? isnull(ss.store.store_sk)) as store_null_rows,
    |                                                               ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...sk, ss.ticket_number) ? isnull ??? (ss.store.store_sk)) as store_...
  ```
- `trilogy file write probe_store.preql --run-and-delete`

  ```text
  refused to write 'probe_store.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [231]: A `subset|union join` cannot follow a trailing `where`. Put the join right after the select list, with the filter either before `select` or after the join: `where <filters> select <cols> subset join a.key = b.key` or `select <cols> subset join a.key = b.key where <filters>`. Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
     and sr.return_amt > 10000  ??? union join ss.item.item_sk = s...
  ```
- `trilogy file write probe7.preql --run-and-delete`

  ```text
  refused to write 'probe7.preql': not syntactically valid Trilogy.

  Parse error:
    --> 11:1
     |
  11 | grouping sets ()
     | ^---
     |
     = expected limit, order_by, THEN_LA, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...r_address.county is not null  ??? grouping sets ()  order by row...
  ```
- `trilogy file write probe2.preql --run-and-delete`

  ```text
  refused to write 'probe2.preql': not syntactically valid Trilogy.

  Parse error:
   --> 8:1
    |
  8 | order by year, moy;
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...der_number)) as rows_cnt,  ;  ??? order by year, moy;
  ```
- `trilogy file write probe3.preql --run-and-delete`

  ```text
  refused to write 'probe3.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [213]: A `by <grain>` clause must follow an aggregate, but the expression before it has none. If the `by` sits inside an aggregate's parentheses (`max(x by *)`), move it outside the call: `max(x) by *`. To take each distinct value once per grain, wrap it in `group(...)` - e.g. `group(item.current_price) by item.id, item.category`. For a reduction, use an aggregate: `sum(x) by ...`, `avg(x) by ...`, `max(x) by ...`.
  Location:
  ...= 1), 0) as jan_sales,  group ??? by monthly.wh_sk  order by mon...
  ```
- `trilogy file write probe_arms.preql --run-and-delete`

  ```text
  refused to write 'probe_arms.preql': not syntactically valid Trilogy.

  Parse error:
    --> 13:1
     |
  13 | -> (cust_sk);
     | ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...tomer.customer_sk as cust_sk  ??? -> (cust_sk);    with web_cust...
  ```
- `trilogy file write answer_2091535883.preql --run`

  ```text
  refused to write 'answer_2091535883.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [231]: A `subset|union join` cannot follow a trailing `where`. Put the join right after the select list, with the filter either before `select` or after the join: `where <filters> select <cols> subset join a.key = b.key` or `select <cols> subset join a.key = b.key where <filters>`. Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...antity_on_hand < cs.quantity  ??? union join cs.item.item_sk = i...
  ```
- `trilogy file write probe1.preql --run-and-delete`

  ```text
  refused to write 'probe1.preql': not syntactically valid Trilogy.

  Parse error:
    --> 17:1
     |
  17 | by *;
     | ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...null)) as cs_sales_with_page  ??? by *;
  ```
- `trilogy file write probe_a.preql --run-and-delete`

  ```text
  refused to write 'probe_a.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [202]: Missing closing semicolon? Statements must be terminated with a semicolon `;`.
  Location:
  ...inv.item.item_sk) as n_items  ???
  ```
- `trilogy file write probe_b.preql --run-and-delete`

  ```text
  refused to write 'probe_b.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [202]: Missing closing semicolon? Statements must be terminated with a semicolon `;`.
  Location:
  ....ticket_number) as n_tickets  ???
  ```
- `trilogy file write answer_2869182220.preql --run`

  ```text
  refused to write 'answer_2869182220.preql': not syntactically valid Trilogy.

  Parse error:
    --> 15:1
     |
  15 | equal join c.customer_demographics.demo_sk = sr.customer_demographics.demo_sk
     | ^---
     |
     = expected limit, order_by, THEN_LA, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...graphics.demo_sk is not null  ??? equal join c.customer_demograp...
  ```
- `trilogy file write probe2.preql --run-and-delete`

  ```text
  refused to write 'probe2.preql': not syntactically valid Trilogy.

  Parse error:
    --> 11:1
     |
  11 | equal join wr.item.item_sk = ws.item.item_sk and wr.order_number = ws.order_number;
     | ^---
     |
     = expected limit, order_by, THEN_LA, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...ere ws.sold_date.year = 2000  ??? equal join wr.item.item_sk = w...
  ```
- `trilogy file write probe_cmp.preql --run-and-delete`

  ```text
  refused to write 'probe_cmp.preql': not syntactically valid Trilogy.

  Parse error:
    --> 15:66
     |
  15 | having abs(m_total - avg_total) / avg_total > 0.1 and avg_total <> 0
     |                                                                  ^---
     |
     = expected sum_operator
  Location:
  ...vg_total > 0.1 and avg_total < ??? > 0  order by (m_total - avg_t...
  ```

### `other`

- `trilogy file write probe3.preql --run-and-delete`

  ```text
  Unexpected error in probe3.preql: (_duckdb.BinderException) Binder Error: No function matches the given name and argument types 'sum(VARCHAR)'. You might need to add explicit type casts.
  	Candidate functions:
  	sum(DECIMAL) -> DECIMAL
  	sum(BOOLEAN) -> HUGEINT
  	sum(SMALLINT) -> HUGEINT
  	sum(INTEGER) -> HUGEINT
  	sum(BIGINT) -> HUGEINT
  	sum(HUGEINT) -> HUGEINT
  	sum(DOUBLE) -> DOUBLE
  	sum(BIGNUM) -> BIGNUM


  LINE 42:     sum(CASE WHEN "cooperative"."cust_store_total" > "questionable...
               ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "sr_store_returns"."sr_customer_sk" as "sr_customer_customer_sk",
      "sr_store_returns"."sr_store_sk" as "sr_store_store_sk",
      sum("sr_store_returns"."sr_return_amt") as "cust_store_total"
  FROM
      "store_returns" as "sr_store_returns"
      INNER JOIN "store" as "sr_store_store" on "sr_store_returns"."sr_store_sk" = "sr_store_store"."s_store_sk"
      INNER JOIN "date_dim" as "sr_date_dim_date_dim" on "sr_store_returns"."sr_returned_date_sk" = "sr_date_dim_date_dim"."d_date_sk"
  WHERE
      "sr_store_store"."s_state" = 'TN' and "sr_date_dim_date_dim"."d_year" = 2000 and "sr_store_returns"."sr_customer_sk" is not null

  GROUP BY
      1,
      2),
  questionable as (
  SELECT
      "cheerful"."sr_store_store_sk" as "sr_store_store_sk",
      avg("cheerful"."cust_store_total") as "store_cust_avg"
  FROM
      "cheerful"
  GROUP BY
      1),
  juicy as (
  SELECT
      avg("questionable"."store_cust_avg") as "grand_avg_cust_amt"
  FROM
      "questionable"),
  cooperative as (
  SELECT
      "cheerful"."cust_store_total" as "cust_store_total",
      "cheerful"."sr_customer_customer_sk" as "sr_customer_customer_sk",
      "cheerful"."sr_store_store_sk" as "sr_store_store_sk",
      md5(CONCAT_WS('', coalesce(cast("cheerful"."sr_customer_customer_sk" as string),'
  '), coalesce(cast("cheerful"."sr_store_store_sk" as string),'
  '))) as "_virt_func_hash_5334446093802053"
  FROM
      "cheerful"),
  abundant as (
  SELECT
      count(md5(CONCAT_WS('', coalesce(cast("cooperative"."sr_customer_customer_sk" as string),'
  '), coalesce(cast("cooperative"."sr_store_store_sk" as string),'
  ')))) as "pairs_total",
      sum(CASE WHEN "cooperative"."cust_store_total" > "questionable"."store_cust_avg" THEN "cooperative"."_virt_func_hash_5334446093802053" ELSE NULL END) as "above_avg",
      sum(CASE WHEN "cooperative"."cust_store_total" > 1.1 * "questionable"."store_cust_avg" THEN "cooperative"."_virt_func_hash_5334446093802053" ELSE NULL END) as "above_110",
      sum(CASE WHEN "cooperative"."cust_store_total" > 1.2 * "questionable"."store_cust_avg" THEN "cooperative"."_virt_func_hash_5334446093802053" ELSE NULL END) as "above_120"
  FROM
      "cooperative"
      INNER JOIN "questionable" on "cooperative"."sr_store_store_sk" = "questionable"."sr_store_store_sk")
  SELECT
      "abundant"."pairs_total" as "pairs_total",
      "juicy"."grand_avg_cust_amt" as "grand_avg_cust_amt",
      "abundant"."above_avg" as "above_avg",
      "abundant"."above_120" as "above_120",
      "abundant"."above_110" as "above_110"
  FROM
      "abundant"
      INNER JOIN "juicy" on 1=1]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy file write answer_142117636.preql --run`

  ```text
  Syntax error in answer_142117636.preql: Output column 'jan_cv' renames 'local.jan_cv' back to the name of an existing concept 'jan_cv' (defined at line 7) that 'local.jan_cv' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'jan_cv_out').
  ```
- `trilogy file write answer_1809796058.preql --run`

  ```text
  trilogy error: subprocess timed out after 600s.
  ```
- `trilogy file write probe_c.preql --run-and-delete`

  ```text
  Syntax error in probe_c.preql: HAVING filters on a dimension outside the SELECT projection, but the select has no grain key to anchor a post-aggregation semijoin (line 15). Move the filter to WHERE to filter before aggregation.
  ```
- `trilogy file write answer_1484301313.preql --run`

  ```text
  Syntax error in answer_1484301313.preql: Impossible comparison in ref:ss.promotion.channel_email = Y: 'Y' can never match a declared value of enum<'N'> — fix the constant, or update the enum declaration if the domain is stale
  ```
- `trilogy file write answer_3544057080.preql --run`

  ```text
  trilogy error: subprocess timed out after 600s.
  ```
- `trilogy file write probe1.preql --run-and-delete`

  ```text
  Syntax error in probe1.preql: Impossible comparison in ref:ws.time_dim.second between 30838 and 59638: declared domain bigint[0..59] has no value >= 30838
  ```
- `trilogy file write answer_3036656719.preql --run`

  ```text
  Syntax error in answer_3036656719.preql: Output column 'sales_rank' renames 'local.sales_rank' back to the name of an existing concept 'sales_rank' (defined at line 4) that 'local.sales_rank' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'sales_rank_out').
  ```
- `trilogy file write answer_3036656719.preql --run`

  ```text
  trilogy error: subprocess timed out after 600s.
  ```
- `trilogy file write answer_1772060640.preql --run`

  ```text
  Syntax error in answer_1772060640.preql: Impossible comparison in SubselectComparison(left=ref:ss.store.county, right=('Orange County', 'Bronx County', 'Franklin Parish', 'Williamson County'), operator=<ComparisonOperator.IN: 'in'>): 'Orange County' can never match a declared value of enum<'Williamson County'> — fix the constant, or update the enum declaration if the domain is stale
  ```
- `trilogy file write answer_1772060640.preql --run`

  ```text
  Syntax error in answer_1772060640.preql: ORDER BY references 'ss.customer.customer_sk', which is not in the SELECT projection (line 5). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --ss.customer.customer_sk order by ss.customer.customer_sk asc`.
  ```
- `trilogy file write answer_3210116865.preql --run`

  ```text
  Resolution error in answer_3210116865.preql: Planner emitted a keyless join between row-bearing sources that share a join axis or one source relation: ss.date_dim.date_dim_at_ss_date_dim_date_sk_join_ss.store_sales_at_ss_item_item_sk_ss_ticket_number_at_ss_date_dim_date_sk_ss_item_item_sk_ss_ticket_number_filtered_by_6498725783617218_preserving_3145037596524166_at_ss_date_dim_date_sk_ss_item_item_sk_ss_ticket_number_grouped_by_local._store_sales_agg_outlet_sk_at_local__store_sales_agg_outlet_sk_at_store_sales_agg_outlet_sk_grouped_by_local._virt_presence_1777910408744868_at_local__virt_presence_1777910408744868 onto sr.date_dim.date_dim_at_sr_date_dim_date_sk_join_sr.store_returns_at_sr_item_item_sk_sr_ticket_number_at_sr_date_dim_date_sk_sr_item_item_sk_sr_ticket_number_filtered_by_3030843720109086_preserving_3490468948963722_at_sr_date_dim_date_sk_sr_item_item_sk_sr_ticket_number_grouped_by_local._store_returns_agg_outlet_sk_at_local__store_returns_agg_outlet_sk_at_store_returns_agg_outlet_sk_join_ss.date_dim.date_dim_at_ss_date_dim_date_sk_join_ss.store_sales_at_ss_item_item_sk_ss_ticket_number_at_ss_date_dim_date_sk_ss_item_item_sk_ss_ticket_number_filtered_by_6498725783617218_preserving_3145037596524166_at_ss_date_dim_date_sk_ss_item_item_sk_ss_ticket_number_grouped_by_local._store_sales_agg_outlet_sk_at_local__store_sales_agg_outlet_sk_at_store_sales_agg_outlet_sk_at_store_returns_agg_outlet_sk_preserving_9461635271207500_at_store_returns_agg_outlet_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write answer_3210116865.preql --run`

  ```text
  Resolution error in answer_3210116865.preql: Planner emitted a keyless join between row-bearing sources that share a join axis or one source relation: ss.date_dim.date_dim_at_ss_date_dim_date_sk_join_ss.store_sales_at_ss_item_item_sk_ss_ticket_number_at_ss_date_dim_date_sk_ss_item_item_sk_ss_ticket_number_filtered_by_6498725783617218_preserving_3145037596524166_grouped_by_ss.store.store_sk_at_ss_store_store_sk_at_ss_store_store_sk_at_store_sales_agg_outlet_sk_grouped_by_local._virt_presence_1777910408744868_at_local__virt_presence_1777910408744868 onto sr.date_dim.date_dim_at_sr_date_dim_date_sk_join_sr.store_returns_at_sr_item_item_sk_sr_ticket_number_at_sr_date_dim_date_sk_sr_item_item_sk_sr_ticket_number_filtered_by_3030843720109086_preserving_3490468948963722_grouped_by_sr.store.store_sk_at_sr_store_store_sk_at_sr_store_store_sk_at_store_returns_agg_outlet_sk_join_ss.date_dim.date_dim_at_ss_date_dim_date_sk_join_ss.store_sales_at_ss_item_item_sk_ss_ticket_number_at_ss_date_dim_date_sk_ss_item_item_sk_ss_ticket_number_filtered_by_6498725783617218_preserving_3145037596524166_grouped_by_ss.store.store_sk_at_ss_store_store_sk_at_ss_store_store_sk_at_store_sales_agg_outlet_sk_at_store_returns_agg_outlet_sk_preserving_7466827448722469_at_store_returns_agg_outlet_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe3.preql --run-and-delete`

  ```text
  Resolution error in probe3.preql: Planner emitted a keyless join between row-bearing sources that share a join axis or one source relation: ss.date_dim.date_dim_at_ss_date_dim_date_sk_join_ss.store_sales_at_ss_item_item_sk_ss_ticket_number_at_ss_date_dim_date_sk_ss_item_item_sk_ss_ticket_number_filtered_by_6498725783617218_preserving_3145037596524166_grouped_by_ss.store.store_sk_at_ss_store_store_sk_at_ss_store_store_sk_at_store_sales_agg_outlet_sk_grouped_by_local._virt_presence_1777910408744868_at_local__virt_presence_1777910408744868 onto sr.date_dim.date_dim_at_sr_date_dim_date_sk_join_sr.store_returns_at_sr_item_item_sk_sr_ticket_number_at_sr_date_dim_date_sk_sr_item_item_sk_sr_ticket_number_filtered_by_3030843720109086_preserving_3490468948963722_grouped_by_sr.store.store_sk_at_sr_store_store_sk_at_sr_store_store_sk_at_store_returns_agg_outlet_sk_join_ss.date_dim.date_dim_at_ss_date_dim_date_sk_join_ss.store_sales_at_ss_item_item_sk_ss_ticket_number_at_ss_date_dim_date_sk_ss_item_item_sk_ss_ticket_number_filtered_by_6498725783617218_preserving_3145037596524166_grouped_by_ss.store.store_sk_at_ss_store_store_sk_at_ss_store_store_sk_at_store_sales_agg_outlet_sk_at_store_returns_agg_outlet_sk_preserving_7466827448722469_at_store_returns_agg_outlet_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write answer_3210116865.preql --run`

  ```text
  Resolution error in answer_3210116865.preql: Planner emitted a keyless join between row-bearing sources that share a join axis or one source relation: ss.date_dim.date_dim_at_ss_date_dim_date_sk_join_ss.store_sales_at_ss_item_item_sk_ss_ticket_number_at_ss_date_dim_date_sk_ss_item_item_sk_ss_ticket_number_filtered_by_6498725783617218_preserving_3145037596524166_grouped_by_ss.store.store_sk_at_ss_store_store_sk_at_ss_store_store_sk_at_store_sales_agg_outlet_sk_grouped_by_local._virt_presence_1777910408744868_at_local__virt_presence_1777910408744868 onto sr.date_dim.date_dim_at_sr_date_dim_date_sk_join_sr.store_returns_at_sr_item_item_sk_sr_ticket_number_at_sr_date_dim_date_sk_sr_item_item_sk_sr_ticket_number_filtered_by_3030843720109086_preserving_3490468948963722_grouped_by_sr.store.store_sk_at_sr_store_store_sk_at_sr_store_store_sk_at_store_returns_agg_outlet_sk_join_ss.date_dim.date_dim_at_ss_date_dim_date_sk_join_ss.store_sales_at_ss_item_item_sk_ss_ticket_number_at_ss_date_dim_date_sk_ss_item_item_sk_ss_ticket_number_filtered_by_6498725783617218_preserving_3145037596524166_grouped_by_ss.store.store_sk_at_ss_store_store_sk_at_ss_store_store_sk_at_store_sales_agg_outlet_sk_at_store_returns_agg_outlet_sk_preserving_7466827448722469_at_store_returns_agg_outlet_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_set.preql --run-and-delete`

  ```text
  Resolution error in probe_set.preql: WHERE atom BuildSubselectComparison(left=sr.date_dim.week_seq@Grain<sr.date_dim.date_sk>, right=local.target_weeks@Grain<sr.date_dim.date_sk>, operator=<ComparisonOperator.IN: 'in'>) reads ['sr.date_dim.week_seq'] but could only be placed after the aggregates ['local.n_rows'] (disconnected_gate); they would never see it.
  ```
- `trilogy file write answer_2852230229.preql --run`

  ```text
  Syntax error in answer_2852230229.preql: ORDER BY references 'local.parent', which is not in the SELECT projection (line 11). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --local.parent order by local.parent asc`.
  ```
- `trilogy file write probe_c.preql --run-and-delete`

  ```text
  Syntax error in probe_c.preql: except(...) requires at least two relational arms.
  ```
- `trilogy file write probe_nullsens.preql --run-and-delete`

  ```text
  Syntax error in probe_nullsens.preql: HAVING references 'local.stor_nm_placeholder', which is not defined (line 3). Check for a typo or import the relevant concept.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ':' delimiter: line 1 column 248 (char 247). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file write probe_2.preql --run-and-delete`

  ```text
  Syntax error in probe_2.preql: Aggregate concept local.class_ext cannot reference itself. If defining a new concept in a select, use a new name.
  ```

### `undefined-concept`

- `trilogy file write probe_d.preql --run-and-delete`

  ```text
  Syntax error in probe_d.preql: Undefined concept: wr.web_page.web_page_id. Suggestions: ['wr.web_page.web_page_sk', 'wr.web_page.type', 'wr.web_page.image_count']
  ```
- `trilogy file write probe_e.preql --run-and-delete`

  ```text
  Syntax error in probe_e.preql: Undefined concept: wr.web_site.site_id. Suggestions: ['wr.item.item_id', 'wr.item.item_desc', 'wr.item.item_sk']
  ```
- `trilogy file write answer_3263796749.preql --run`

  ```text
  Syntax error in answer_3263796749.preql: Undefined concept: ext_discount_amt. Suggestions: ['ss.ext_discount_amt', 'ss.coupon_amt']
  ```
- `trilogy file write answer_3263796749.preql --run`

  ```text
  Syntax error in answer_3263796749.preql: 10 undefined concept references; fix all before re-running:
    - item.item_sk (line 4, in SELECT); did you mean: ss.item.item_sk, ss.promotion.item.item_sk, ss.item.item_desc, ss.item.item_id?
    - local.ticket_number (line 4, in SELECT); did you mean: ss.ticket_number?
    - item.item_sk (line 7, in SELECT); did you mean: ss.item.item_sk, ss.promotion.item.item_sk, ss.item.item_desc, ss.item.item_id?
    - local.ticket_number (line 7, in SELECT); did you mean: ss.ticket_number?
    - item.item_sk (line 10, in SELECT); did you mean: ss.item.item_sk, ss.promotion.item.item_sk, ss.item.item_desc, ss.item.item_id?
    - local.ticket_number (line 10, in SELECT); did you mean: ss.ticket_number?
    - item.item_sk (line 13, in SELECT); did you mean: ss.item.item_sk, ss.promotion.item.item_sk, ss.item.item_desc, ss.item.item_id?
    - local.ticket_number (line 13, in SELECT); did you mean: ss.ticket_number?
    - item.item_sk (line 16, in SELECT); did you mean: ss.item.item_sk, ss.promotion.item.item_sk, ss.item.item_desc, ss.item.item_id?
    - local.ticket_number (line 16, in SELECT); did you mean: ss.ticket_number?
  ```
- `trilogy file write answer_1078396760.preql --run`

  ```text
  Syntax error in answer_1078396760.preql: 2 undefined concept references; fix all before re-running:
    - item.category (line 7, col 5, in WHERE); did you mean: cs.item.category, cs.promotion.item.category, item.class, item_category, cs.item.category_id?
    - item.item_id (line 10, col 9, in WHERE); did you mean: cs.item.item_id, cs.promotion.item.item_id, item.item_desc, cs.item.item_desc?
  ```
- `trilogy file write probe_freq.preql --run-and-delete`

  ```text
  Syntax error in probe_freq.preql: Undefined concept: ss.sold_date.year (line 8, col 7, in WHERE). Suggestions: ['ss.sold_date.date', 'ss.date_dim.year', 'ss.store.date_dim.year', 'ss.promotion.end_date.year', 'ss.customer.first_sales_date.year', 'ss.customer.last_review_date.year']
  ```
- `trilogy file write probe_best.preql --run-and-delete`

  ```text
  Syntax error in probe_best.preql: Undefined concept: ss.sold_date.date. Suggestions: ['ss.promotion.end_date.date', 'ss.promotion.end_date.date_sk', 'ss.promotion.end_date.date_id', 'ss.customer.first_sales_date.date', 'ss.customer.last_review_date.date', 'ss.promotion.start_date.date']
  ```
- `trilogy file write answer_3530408544.preql --run`

  ```text
  Syntax error in answer_3530408544.preql: 11 undefined concept references; fix all before re-running:
    - local.ticket_number (line 14, col 3, in SELECT); did you mean: ss.ticket_number?
    - item.item_sk (line 15, in SELECT); did you mean: ss.item.item_sk, ss.promotion.item.item_sk, ss.item.item_desc, ss.item.item_id?
    - local.ticket_number (line 15, in SELECT); did you mean: ss.ticket_number?
    - date_dim.year (line 3, col 7, in WHERE); did you mean: ss.date_dim.year, ss.store.date_dim.year, date_dim.dom, ss.promotion.end_date.year, ss.promotion.start_date.year, ss.customer.first_sales_date.year?
    - date_dim.dom (line 4, col 8, in WHERE); did you mean: ss.date_dim.dom, ss.store.date_dim.dom, date_dim.year, ss.promotion.end_date.dom, ss.promotion.start_date.dom, ss.customer.first_sales_date.dom?
    - store.county (line 5, col 7, in WHERE); did you mean: ss.store.county, ss.customer_address.county, ss.customer.customer_address.county, ss.store.country, ss.store.city?
    - household_demographics.buy_potential (line 6, col 7, in WHERE); did you mean: ss.household_demographics.buy_potential, ss.customer.household_demographics.buy_potential, household_demographics.dep_count, household_demographics.vehicle_count?
    - household_demographics.vehicle_count (line 7, col 7, in WHERE); did you mean: ss.household_demographics.vehicle_count, ss.customer.household_demographics.vehicle_count, household_demographics.dep_count, household_demographics.buy_potential?
    - household_demographics.dep_count (line 3, in WHERE); did you mean: ss.household_demographics.dep_count, ss.customer.household_demographics.dep_count, household_demographics.vehicle_count, household_demographics.buy_potential, ss.customer_demographics.dep_count, ss.customer.customer_demographics.dep_count?
    - household_demographics.vehicle_count (line 3, in WHERE); did you mean: ss.household_demographics.vehicle_count, ss.customer.household_demographics.vehicle_count, household_demographics.dep_count, household_demographics.buy_potential?
    - local.ticket_number (line 18, col 31, in ORDER BY); did you mean: ss.ticket_number?
  ```
- `trilogy file write answer_2118989494.preql --run`

  ```text
  Syntax error in answer_2118989494.preql: Undefined concept: mt.category. Suggestions: ['ss.item.category', 'month_totals.category', 'ss.promotion.item.category', 'ss.item.category_id']
  ```
- `trilogy file write answer_2940558602.preql --run`

  ```text
  Syntax error in answer_2940558602.preql: Undefined concept: cr.return_amt. Suggestions: ['cr.return_amount', 'cr.return_tax', 'cr.return_quantity', 'wr.return_amt', 'sr.return_amt']
  ```
- `trilogy file write probe_rank.preql --run-and-delete`

  ```text
  Syntax error in probe_rank.preql: Undefined concept: _virt_agg_grouping_2599847655796781.
  ```
- `trilogy file write probe_counties.preql --run-and-delete`

  ```text
  Syntax error in probe_counties.preql: 2 undefined concept references; fix all before re-running:
    - s.store.county (line 3, col 8, in SELECT); did you mean: s.county, s.country, s.store_name?
    - s.store.store_sk (line 3, in SELECT); did you mean: s.store_sk, s.store_id, s.store.county?
  ```
- `trilogy file write answer_3840922834.preql --run`

  ```text
  Syntax error in answer_3840922834.preql: Undefined concept: ss.sold_date.year. Suggestions: ['ss.date_dim.year', 'ss.store.date_dim.year', 'ss.promotion.end_date.year', 'ws.sold_date.year', 'ws.ship_date.year', 'ws.promotion.end_date.year']
  ```
- `trilogy file write answer_747155668.preql --run`

  ```text
  Syntax error in answer_747155668.preql: Undefined concept: ss.sold_date.date_sk. Suggestions: ['ss.date_dim.date_sk', 'ss.store.date_dim.date_sk', 'ss.promotion.end_date.date_sk', 'ws.sold_date.date_sk', 'cs.sold_date.date_sk', 'ws.ship_date.date_sk']
  ```
- `trilogy file write answer_1226264875.preql --run`

  ```text
  Syntax error in answer_1226264875.preql: Undefined concept: ss.sold_date.year. Suggestions: ['ss.date_dim.year', 'ss.store.date_dim.year', 'ss.promotion.end_date.year', 'ws.sold_date.year', 'cs.sold_date.year', 'ws.ship_date.year']
  ```
- `trilogy file write probe_dim.preql --run-and-delete`

  ```text
  Syntax error in probe_dim.preql: Undefined concept: c.customer.customer_sk (line 5, in SELECT). Suggestions: ['c.customer_sk', 'c.customer.last_name', 'c.customer_id']
  ```
- `trilogy file write probe_dim.preql --run-and-delete`

  ```text
  Syntax error in probe_dim.preql: Undefined concept: c.customer.last_name. Suggestions: ['c.customer_address.street_name', 'c.last_name', 'c.customer_address.state']
  ```
- `trilogy file write probe1.preql --run-and-delete`

  ```text
  Syntax error in probe1.preql: Undefined concept: cr.return_amt. Suggestions: ['cr.return_amount', 'cr.return_tax', 'cr.return_quantity', 'wr.return_amt', 'sr.return_amt']
  ```
- `trilogy file write answer_2822608529.preql --run`

  ```text
  Syntax error in answer_2822608529.preql: 4 undefined concept references; fix all before re-running:
    - item.item_sk (line 4, in SELECT); did you mean: web_sales.item.item_sk, web_sales.promotion.item.item_sk, web_sales.item.item_desc?
    - ship_hdemo.dep_count (line 6, col 7, in WHERE); did you mean: web_sales.ship_hdemo.dep_count, web_sales.ship_cdemo.dep_count, web_sales.bill_hdemo.dep_count, web_sales.bill_cdemo.dep_count, web_sales.ship_customer.household_demographics.dep_count, web_sales.ship_customer.customer_demographics.dep_count?
    - web_page.char_count (line 7, col 7, in WHERE); did you mean: web_sales.web_page.char_count, web_sales.web_page.image_count, web_sales.web_page.max_ad_count?
    - web_page.char_count (line 8, col 7, in WHERE); did you mean: web_sales.web_page.char_count, web_sales.web_page.image_count, web_sales.web_page.max_ad_count?
  ```

### `join-resolution`

- `trilogy file write probe3.preql --run-and-delete`

  ```text
  Resolution error in probe3.preql: Could not resolve connections for query with output ['local.pairs_total<Purpose.METRIC>Derivation.AGGREGATE>', 'local.rows_total<Purpose.METRIC>Derivation.AGGREGATE>', 'local.grand_avg_cust_amt<Purpose.METRIC>Derivation.AGGREGATE>', 'local.above_avg<Purpose.METRIC>Derivation.AGGREGATE>', 'local.above_120<Purpose.METRIC>Derivation.AGGREGATE>', 'local.above_110<Purpose.METRIC>Derivation.AGGREGATE>'] from current model. Group grp:aggregate:d0:∅ (Derivation.AGGREGATE) could not be built, and nothing else delivers what it owes: WHERE atoms [], readers [], outputs ['local.rows_total'].
  ```
- `trilogy file write probe_x.preql --run-and-delete`

  ```text
  Resolution error in probe_x.preql: Could not resolve connections for query with output ['local.dlines<Purpose.METRIC>Derivation.AGGREGATE>', 'local.nrows<Purpose.METRIC>Derivation.AGGREGATE>', 'local.plain<Purpose.METRIC>Derivation.AGGREGATE>', 'local.bymax<Purpose.METRIC>Derivation.AGGREGATE>', 'local.bygroup<Purpose.METRIC>Derivation.AGGREGATE>'] from current model. Group grp:aggregate:d0:∅ (Derivation.AGGREGATE) could not be built, and nothing else delivers what it owes: WHERE atoms [], readers [], outputs ['local.nrows'].
  ```
- `trilogy file write p7.preql --run-and-delete`

  ```text
  Resolution error in p7.preql: Could not resolve connections for query with output ['local.joined_rows<Purpose.METRIC>Derivation.AGGREGATE>', 'local.combos<Purpose.METRIC>Derivation.AGGREGATE>'] from current model. Group grp:aggregate:d0:∅ (Derivation.AGGREGATE) could not be built, and nothing else delivers what it owes: WHERE atoms [], readers [], outputs ['local.joined_rows'].
  ```
- `trilogy file write p8.preql --run-and-delete`

  ```text
  Resolution error in p8.preql: Could not resolve connections for query with output ['local.joined_rows<Purpose.METRIC>Derivation.AGGREGATE>', 'local.ss_grains<Purpose.METRIC>Derivation.AGGREGATE>', 'local.sr_grains<Purpose.METRIC>Derivation.AGGREGATE>'] from current model. Group grp:aggregate:d0:∅ (Derivation.AGGREGATE) could not be built, and nothing else delivers what it owes: WHERE atoms [], readers [], outputs ['local.joined_rows'].
  ```
- `trilogy file write probe_check.preql --run-and-delete`

  ```text
  Resolution error in probe_check.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {county (= ss.customer_address.county), ss.customer_address.county, store_q1, store_q2, store_q3}; {web_q1_atch, web_q2_atch}.
    - `ss.customer_address.county` (as `county`) is disconnected, did you mean `ws.bill_customer.customer_address.county`? (connected to the other concepts)
    - `ss.customer_address.county` is disconnected, did you mean `ws.bill_customer.customer_address.county`? (connected to the other concepts)
  These look like separately-imported copies of models already reachable through a connected import; chain through that path (e.g. `ws.bill_customer.customer_address.county`) instead of importing a second, disconnected copy.
  ```
- `trilogy file write probe_e.preql --run-and-delete`

  ```text
  Resolution error in probe_e.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 3 disconnected subgraphs: {cs.bill_addr.gmt_offset, cs.sold_date.moy, cs.sold_date.year}; {ss.customer_address.gmt_offset, ss.date_dim.moy, ss.date_dim.year}; {ws.bill_addr.gmt_offset, ws.sold_date.moy, ws.sold_date.year}.
    - `ss.customer_address.gmt_offset` is disconnected, did you mean `cs.bill_customer.customer_address.gmt_offset`? (connected to the other concepts)
    - `ss.date_dim.moy` is disconnected, did you mean `cs.promotion.end_date.moy`? (connected to the other concepts)
    - `ss.date_dim.year` is disconnected, did you mean `cs.promotion.end_date.year`? (connected to the other concepts)
    - `ws.bill_addr.gmt_offset` is disconnected, did you mean `cs.bill_customer.customer_address.gmt_offset`? (connected to the other concepts)
    - `ws.sold_date.moy` is disconnected, did you mean `cs.promotion.end_date.moy`? (connected to the other concepts)
    - `ws.sold_date.year` is disconnected, did you mean `cs.promotion.end_date.year`? (connected to the other concepts)
  These look like separately-imported copies of models already reachable through a connected import; chain through that path (e.g. `cs.bill_customer.customer_address.gmt_offset`) instead of importing a second, disconnected copy.
  ```
- `trilogy file write answer_3809267817.preql --run`

  ```text
  Resolution error in answer_3809267817.preql: Could not resolve connections for query with output ['local.ca_state<Purpose.PROPERTY>Derivation.BASIC>', 'local.cd_gender<Purpose.PROPERTY>Derivation.BASIC>', 'local.cd_marital_status<Purpose.PROPERTY>Derivation.BASIC>', 'local.cd_dep_count<Purpose.PROPERTY>Derivation.BASIC>', 'local.dep_count_cnt<Purpose.METRIC>Derivation.AGGREGATE>', 'local.dep_count_min<Purpose.METRIC>Derivation.AGGREGATE>', 'local.dep_count_max<Purpose.METRIC>Derivation.AGGREGATE>', 'local.dep_count_avg<Purpose.METRIC>Derivation.AGGREGATE>', 'local.cd_dep_employed_count<Purpose.PROPERTY>Derivation.BASIC>', 'local.dep_employed_cnt<Purpose.METRIC>Derivation.AGGREGATE>', 'local.dep_employed_min<Purpose.METRIC>Derivation.AGGREGATE>', 'local.dep_employed_max<Purpose.METRIC>Derivation.AGGREGATE>', 'local.dep_employed_avg<Purpose.METRIC>Derivation.AGGREGATE>', 'local.cd_dep_college_count<Purpose.PROPERTY>Derivation.BASIC>', 'local.dep_college_cnt<Purpose.METRIC>Derivation.AGGREGATE>', 'local.dep_college_min<Purpose.METRIC>Derivation.AGGREGATE>', 'local.dep_college_max<Purpose.METRIC>Derivation.AGGREGATE>', 'local.dep_college_avg<Purpose.METRIC>Derivation.AGGREGATE>'] from current model. Group grp:root:root_d1:∅ (Derivation.ROOT) could not be built, and nothing else delivers what it owes: WHERE atoms [], readers ['grp:[@condition]aggregate:d1:catalog_sales.ship_customer.customer_sk:input:catalog_sales.order_number|catalog_sales.ship_customer.customer_sk', 'grp:[@condition]aggregate:d1:store_sales.customer.customer_sk:input:store_sales.customer.customer_sk|store_sales.ticket_number', 'grp:[@condition]aggregate:d1:web_sales.bill_customer.customer_sk:input:web_sales.bill_customer.customer_sk|web_sales.order_number', 'grp:[@condition]filter:d1:∅:existence:local.cs_custs', 'grp:[@condition]filter:d1:∅:existence:local.ss_custs', 'grp:[@condition]filter:d1:∅:existence:local.ws_custs', 'grp:[@condition]filter:d1:∅:sig:7fbd695ea35e', 'grp:[@condition]filter:d1:∅:sig:9109394c5a56', 'grp:[@condition]filter:d1:∅:sig:db3852545d83'], outputs ['catalog_sales.order_number', 'catalog_sales.ship_customer.customer_sk', 'catalog_sales.sold_date.qoy', 'catalog_sales.sold_date.year', 'store_sales.customer.customer_sk', 'store_sales.date_dim.qoy', 'store_sales.date_dim.year', 'store_sales.ticket_number', 'web_sales.bill_customer.customer_sk', 'web_sales.order_number', 'web_sales.sold_date.qoy', 'web_sales.sold_date.year'].
  ```
- `trilogy file write answer_3809267817.preql --run`

  ```text
  Resolution error in answer_3809267817.preql: Could not resolve connections for query with output ['local.ca_state<Purpose.PROPERTY>Derivation.BASIC>', 'local.cd_gender<Purpose.PROPERTY>Derivation.BASIC>', 'local.cd_marital_status<Purpose.PROPERTY>Derivation.BASIC>', 'local.cd_dep_count<Purpose.PROPERTY>Derivation.BASIC>', 'local.dep_count_cnt<Purpose.METRIC>Derivation.AGGREGATE>', 'local.dep_count_min<Purpose.METRIC>Derivation.AGGREGATE>', 'local.dep_count_max<Purpose.METRIC>Derivation.AGGREGATE>', 'local.dep_count_avg<Purpose.METRIC>Derivation.AGGREGATE>', 'local.cd_dep_employed_count<Purpose.PROPERTY>Derivation.BASIC>', 'local.dep_employed_cnt<Purpose.METRIC>Derivation.AGGREGATE>', 'local.dep_employed_min<Purpose.METRIC>Derivation.AGGREGATE>', 'local.dep_employed_max<Purpose.METRIC>Derivation.AGGREGATE>', 'local.dep_employed_avg<Purpose.METRIC>Derivation.AGGREGATE>', 'local.cd_dep_college_count<Purpose.PROPERTY>Derivation.BASIC>', 'local.dep_college_cnt<Purpose.METRIC>Derivation.AGGREGATE>', 'local.dep_college_min<Purpose.METRIC>Derivation.AGGREGATE>', 'local.dep_college_max<Purpose.METRIC>Derivation.AGGREGATE>', 'local.dep_college_avg<Purpose.METRIC>Derivation.AGGREGATE>'] from current model. Group grp:root:root_d1:∅ (Derivation.ROOT) could not be built, and nothing else delivers what it owes: WHERE atoms [], readers ['grp:[@condition]filter:d1:∅:existence:local.cs_custs', 'grp:[@condition]filter:d1:∅:existence:local.ss_custs', 'grp:[@condition]filter:d1:∅:existence:local.ws_custs'], outputs ['catalog_sales.ship_customer.customer_sk', 'catalog_sales.sold_date.qoy', 'catalog_sales.sold_date.year', 'store_sales.customer.customer_sk', 'store_sales.date_dim.qoy', 'store_sales.date_dim.year', 'web_sales.bill_customer.customer_sk', 'web_sales.sold_date.qoy', 'web_sales.sold_date.year'].
  ```
- `trilogy file write probe2.preql --run-and-delete`

  ```text
  Resolution error in probe2.preql: Could not resolve connections for query with output ['local.st<Purpose.PROPERTY>Derivation.BASIC>', 'local.c<Purpose.METRIC>Derivation.AGGREGATE>'] from current model. Group grp:root:root_d1:∅ (Derivation.ROOT) could not be built, and nothing else delivers what it owes: WHERE atoms [], readers ['grp:[@condition]filter:d1:∅:existence:local.ss_custs', 'grp:[@condition]filter:d1:∅:existence:local.ws_custs'], outputs ['store_sales.customer.customer_sk', 'store_sales.date_dim.qoy', 'store_sales.date_dim.year', 'web_sales.bill_customer.customer_sk', 'web_sales.sold_date.qoy', 'web_sales.sold_date.year'].
  ```
- `trilogy file write probe_c.preql --run-and-delete`

  ```text
  Resolution error in probe_c.preql: Could not resolve connections for query with output ['local.n_cs_items<Purpose.METRIC>Derivation.AGGREGATE>', 'local.n_manuf<Purpose.METRIC>Derivation.AGGREGATE>', 'local.n_manuf_price<Purpose.METRIC>Derivation.AGGREGATE>', 'local.n_mpdq<Purpose.METRIC>Derivation.AGGREGATE>', 'local.n_all<Purpose.METRIC>Derivation.AGGREGATE>'] from current model. Group grp:root:root:∅ (Derivation.ROOT) could not be built, and nothing else delivers what it owes: WHERE atoms [], readers ['grp:aggregate:d0:∅:input:cs.item.item_sk', 'grp:aggregate:d0:∅:input:inv.item.item_sk', 'grp:basic:d*:∅:sig:1940c2ff9524', 'grp:filter:d*:∅:sig:1940c2ff9524', 'grp:filter:d*:∅:sig:4032100f7c30'], outputs ['inv.date_dim.date', 'inv.item.current_price', 'inv.item.manufact_id', 'inv.quantity_on_hand'].
  ```
- `trilogy file write probe_f.preql --run-and-delete`

  ```text
  Resolution error in probe_f.preql: Could not resolve connections for query with output ['local.item_code<Purpose.PROPERTY>Derivation.BASIC>', 'local.item_desc<Purpose.PROPERTY>Derivation.BASIC>', 'local.current_price<Purpose.PROPERTY>Derivation.BASIC>', 'local.manufact_id<Purpose.PROPERTY>Derivation.BASIC>'] from current model. Group grp:root:root_d1:∅ (Derivation.ROOT) could not be built, and nothing else delivers what it owes: WHERE atoms [], readers ['grp:[@condition]basic:d1:∅:sig:1940c2ff9524', 'grp:[@condition]filter:d1:∅:existence:local.inv_ok'], outputs ['inv.date_dim.date', 'inv.item.item_sk', 'inv.quantity_on_hand'].
  ```
- `trilogy file write probe2.preql --run-and-delete`

  ```text
  Resolution error in probe2.preql: Could not resolve connections for query with output ['local.rows_joined<Purpose.METRIC>Derivation.AGGREGATE>', 'local.pairs_joined<Purpose.METRIC>Derivation.AGGREGATE>', 'local.price_joined<Purpose.METRIC>Derivation.AGGREGATE>'] from current model. Group grp:aggregate:d0:∅ (Derivation.AGGREGATE) could not be built, and nothing else delivers what it owes: WHERE atoms [], readers [], outputs ['local.rows_joined'].
  ```
- `trilogy file write probe_check.preql --run-and-delete`

  ```text
  Resolution error in probe_check.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {cust_id (= ss.customer.customer_id), store_2001_direct, store_2002_direct}; {web_all_placeholder}.
    - `ss.customer.customer_id` (as `cust_id`) is disconnected, did you mean `ws.web_page.customer.customer_id`? (connected to the other concepts)
  These look like separately-imported copies of models already reachable through a connected import; chain through that path (e.g. `ws.web_page.customer.customer_id`) instead of importing a second, disconnected copy.
  ```
- `trilogy file write probe_h.preql --run-and-delete`

  ```text
  Resolution error in probe_h.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 23). The requested concepts split into 3 disconnected subgraphs: {cat_pairs.c, cat_pairs.i}; {store_pairs.c, store_pairs.i}; {web_pairs.c, web_pairs.i}. Are you missing a join or merge statement to relate them?
  Note: the membership predicate(s) `(store_pairs.i, store_pairs.c) in (web_pairs.i, web_pairs.c)`; `(store_pairs.i, store_pairs.c) in (cat_pairs.i, cat_pairs.c)` span these subgraphs, but membership only filters rows on its left side, it does not join the two sides, so it cannot relate them for outputs or grouping. To combine values from both sides, author a query-scoped join or a merge on shared keys.
  ```
- `trilogy file write probe_r.preql --run-and-delete`

  ```text
  Resolution error in probe_r.preql: Query is unresolvable: no complete sources found for output concepts {'local.with_demo'}. These concepts could only be resolved from partial sources.
  ```
- `trilogy file write probe_b.preql --run-and-delete`

  ```text
  Resolution error in probe_b.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 15). The requested concepts split into 2 disconnected subgraphs: {ss.customer.customer_sk, ss.date_dim.year}; {store_only_cat.first_name, store_only_cat.last_name, store_only_cat.sale_date}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file write probe_null2.preql --run-and-delete`

  ```text
  Resolution error in probe_null2.preql: Could not resolve connections for query with output ['local.rows_null_store<Purpose.METRIC>Derivation.AGGREGATE>', 'local.distinct_items<Purpose.METRIC>Derivation.AGGREGATE>'] from current model. Group grp:aggregate:d0:∅ (Derivation.AGGREGATE) could not be built, and nothing else delivers what it owes: WHERE atoms [], readers [], outputs ['local.rows_null_store'].
  ```

### `type-error`

- `trilogy file write answer_3705756794.preql --run`

  ```text
  Type error in answer_3705756794.preql: Invalid argument type 'BIGINT' passed into CONCAT function in position 2 from concept: wr.web_page.web_page_sk. Valid: 'STRING'.
  ```

### `planner-recursion`

- `trilogy file write probe4.preql --run-and-delete`

  ```text
  Resolution error in probe4.preql: query could not be planned; this is a bug.
  ```
