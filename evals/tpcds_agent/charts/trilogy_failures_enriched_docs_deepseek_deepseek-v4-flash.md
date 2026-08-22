# Trilogy failure analysis — 20260820-153008

- Run `20260820-153007_enriched_docs_deepseek_deepseek-v4-flash` | `deepseek/deepseek-v4-flash` | sf=1
- `trilogy` calls: 1149 | failed: 76 (7%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 20 | 26% |
| `other` | 16 | 21% |
| `undefined-concept` | 15 | 20% |
| `disabled-tool` | 11 | 14% |
| `cli-misuse` | 9 | 12% |
| `planner-recursion` | 3 | 4% |
| `join-resolution` | 1 | 1% |
| `file-not-found` | 1 | 1% |

## Detail

### `syntax-parse`

- `trilogy file write probe4.preql --run-and-delete`

  ```text
  refused to write 'probe4.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...s.sale_date.week_seq = 5375)
   ??? group by s.sale_date.week_seq,...
  ```
- `trilogy file write probe2.preql --run-and-delete`

  ```text
  refused to write 'probe2.preql': not syntactically valid Trilogy.

  Parse error:
   --> 6:16
    |
  6 |   count(s.grain(item.sk, channel, order_id)) as rows,
    |                ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...le_date.year,
     count(s.grain ??? (item.sk, channel, order_id))
  ```
- `trilogy file write probe1.preql --run-and-delete`

  ```text
  refused to write 'probe1.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [213]: A `by <grain>` clause must follow an aggregate, but the expression before it has none. If the `by` sits inside an aggregate's parentheses (`max(x by *)`), move it outside the call: `max(x) by *`. To take each distinct value once per grain, wrap it in `group(...)` - e.g. `group(item.current_price) by item.id, item.category`. For a reduction, use an aggregate: `sum(x) by ...`, `avg(x) by ...`, `max(x) by ...`.
  Location:
  ...re a.is_returned is not null
   ??? by a.channel, a.return_channel...
  ```
- `trilogy file write probe7.preql --run-and-delete`

  ```text
  refused to write 'probe7.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [213]: A `by <grain>` clause must follow an aggregate, but the expression before it has none. If the `by` sits inside an aggregate's parentheses (`max(x by *)`), move it outside the call: `max(x) by *`. To take each distinct value once per grain, wrap it in `group(...)` - e.g. `group(item.current_price) by item.id, item.category`. For a reduction, use an aggregate: `sum(x) by ...`, `avg(x) by ...`, `max(x) by ...`.
  Location:
  ...nnel_dim_text_id is not null
   ??? by a.channel;

   select
     sum...
  ```
- `trilogy file write probe3.preql --run-and-delete --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  refused to write 'probe3.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
   2) in qualifying_prefix_ss)
   ??? group by substring(ss.store.zi...
  ```
- `trilogy file write probe_buckets.preql --run-and-delete`

  ```text
  refused to write 'probe_buckets.preql': not syntactically valid Trilogy.

  Parse error:
    --> 11:1
     |
  11 | by *;
     | ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...and ss.quantity <= 20) as n1
   ??? by *;
  ```
- `trilogy file write probe_county.preql --run-and-delete`

  ```text
  refused to write 'probe_county.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ..._customers;
   select county, n ??? from bycounty order by n desc;...
  ```
- `trilogy file write probe1.preql --run-and-delete`

  ```text
  refused to write 'probe1.preql': not syntactically valid Trilogy.

  Parse error:
    --> 16:3
     |
  16 |   by *;
     |   ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...es.brand_id) as num_tuples
     ??? by *;
  ```
- `trilogy file write probe3.preql --run-and-delete`

  ```text
  refused to write 'probe3.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [213]: A `by <grain>` clause must follow an aggregate, but the expression before it has none. If the `by` sits inside an aggregate's parentheses (`max(x by *)`), move it outside the call: `max(x) by *`. To take each distinct value once per grain, wrap it in `group(...)` - e.g. `group(item.current_price) by item.id, item.category`. For a reduction, use an aggregate: `sum(x) by ...`, `avg(x) by ...`, `max(x) by ...`.
  Location:
  ...ry_id);

   select overall_avg ??? by * as ov_avg,
          count(...
  ```
- `trilogy file write probe7.preql --run-and-delete`

  ```text
  refused to write 'probe7.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...sale_date.month_of_year = 11
   ??? group by s.is_returned;
  ```
- `trilogy file write probe_ss.preql --run-and-delete`

  ```text
  refused to write 'probe_ss.preql': not syntactically valid Trilogy.

  Parse error:
   --> 5:115
    |
  5 |   sum(1 ? ss.is_returned and ss.customer.sk is not null and ss.return_customer.sk is not null and ss.customer.sk <> ss.return_customer.sk) as n_diff_cust,
    |                                                                                                                   ^---
    |
    = expected sum_operator
  Location:
   not null and ss.customer.sk < ??? > ss.return_customer.sk) as n_...
  ```
- `trilogy file write probe_x.preql --run-and-delete`

  ```text
  refused to write 'probe_x.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
   csr.customer.sk is not null
   ??? group by csr.customer.sk, csr....
  ```
- `trilogy file write answer_2133330107.preql --run`

  ```text
  refused to write 'answer_2133330107.preql': not syntactically valid Trilogy.

  Parse error:
   --> 6:57
    |
  6 |   and substring(ss.customer.current_address.zip, 1, 5) <> substring(ss.store.zip, 1, 5)
    |                                                         ^---
    |
    = expected sum_operator
  Location:
  ...r.current_address.zip, 1, 5) < ??? > substring(ss.store.zip, 1, 5...
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
  ...ustomer.current_address.city < ??? > ss.pos_address.city
   select
  ```
- `trilogy file write probe1.preql --run-and-delete --content import raw.all_sales as s;
select s.sale_date.week_seq, s.sale_date.date, s.channel, count(s.item.…te, '1999-12-31'::date, '2000-01-02'::date)
group by s.sale_date.week_seq, s.sale_date.date, s.channel
order by s.sale_date.date, s.channel
limit 10;`

  ```text
  refused to write 'probe1.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...1'::date, '2000-01-02'::date) ??? group by s.sale_date.week_seq,...
  ```
- `trilogy file write answer_747155668.preql --run`

  ```text
  refused to write 'answer_747155668.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ..._count, total_ext_sales_price) ???

   select
       combined.chann...
  ```
- `trilogy file write probe1.preql --run-and-delete`

  ```text
  refused to write 'probe1.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
      sum(ss.net_profit) as np
   ??? group by ss.is_returned;
  ```
- `trilogy file write probe2.preql --run-and-delete`

  ```text
  refused to write 'probe2.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...unt(ss.ticket_number) as cnt
   ??? group by ss.sale_date.day_name...
  ```
- `trilogy file write probe6.preql --run-and-delete`

  ```text
  refused to write 'probe6.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:22
    |
  4 | with sale_tickets as (select ss.ticket_number as tn from raw.store_sales where ss.is_returned = false),
    |                      ^---
    |
    = expected select_statement, tvf_union_invocation, tvf_except_invocation, or tvf_intersect_invocation
  Location:
  ...rn rows
   with sale_tickets as ??? (select ss.ticket_number as tn...
  ```
- `trilogy file write probe2_840315271.preql --run-and-delete`

  ```text
  refused to write 'probe2_840315271.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:9
    |
  4 |   isnull(ss.store.sk) as store_sk_null,
    |         ^---
    |
    = expected limit, order_by, where, having, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, select_grouping, or JOIN_TYPE
  Location:
  ...les as ss;

   select
     isnull ??? (ss.store.sk) as store_sk_null...
  ```

### `other`

- `trilogy file write probe3.preql --run-and-delete`

  ```text
  Syntax error in probe3.preql: HAVING references 'local.store_2001', 'local.catalog_2001', 'local.web_2001', 'local.act_store_2001', 'local.act_store_2002', 'local.act_catalog_2001', 'local.act_catalog_2002', 'local.act_web_2001', 'local.act_web_2002', 'local.catalog_2002', 'local.store_2002', 'local.web_2002', which are not defined (line 5). Check for a typo or import the relevant concept.
  ```
- `trilogy file write probe3.preql --run-and-delete --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  trilogy error: subprocess timed out after 600s.
  ```
- `trilogy file write probe_c.preql --run-and-delete`

  ```text
  Syntax error in probe_c.preql: Output column 'base_avg' renames 'local.base_avg' back to the name of an existing concept 'base_avg' (defined at line 3) that 'local.base_avg' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'base_avg_out').
  ```
- `trilogy file write probe4.preql --run-and-delete`

  ```text
  Syntax error in probe4.preql: Output column 'quarter_total' renames 'local.quarter_total' back to the name of an existing concept 'quarter_total' (defined at line 3) that 'local.quarter_total' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'quarter_total_out').
  ```
- `trilogy file write probe_check3.preql --run-and-delete`

  ```text
  Unexpected error in probe_check3.preql: Could not render the query: Missing source reference to ss.store.county; Missing source reference to ss.customer.current_address.county; Missing source reference to ss.store.state; Missing source reference to ss.customer.current_address.state. A planned reference has no backing source CTE -- typically an unsupported cross-rowset or membership shape the planner could not wire. Review the rowset/join structure (or file an issue if the query looks valid).

  Full SQL with sentinel(s):

  WITH
  cooperative as (
  SELECT
      "ss_store_sales"."SS_CUSTOMER_SK" as "ss_customer_sk",
      CASE WHEN "ss_store_store"."S_COUNTY" = "ss_customer_current_address_customer_address"."CA_COUNTY" and "ss_store_store"."S_STATE" = "ss_customer_current_address_customer_address"."CA_STATE" THEN "ss_store_sales"."SS_CUSTOMER_SK" ELSE NULL END as "_virt_filter_sk_9689870234368454",
      CONCAT(cast("ss_store_sales"."SS_TICKET_NUMBER" as string), '-', cast("ss_store_sales"."SS_ITEM_SK" as string)) as "ss_line_item"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "date_dim" as "ss_sale_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_sale_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "store" as "ss_store_store" on "ss_store_sales"."SS_STORE_SK" = "ss_store_store"."S_STORE_SK"
      LEFT OUTER JOIN "customer" as "ss_customer_customers" on "ss_store_sales"."SS_CUSTOMER_SK" = "ss_customer_customers"."C_CUSTOMER_SK"
      LEFT OUTER JOIN "customer_address" as "ss_customer_current_address_customer_address" on "ss_customer_customers"."C_CURRENT_ADDR_SK" = "ss_customer_current_address_customer_address"."CA_ADDRESS_SK"
  WHERE
      "ss_sale_date_date"."D_MONTH_SEQ" BETWEEN 1188 AND 1190
  ),
  abundant as (
  SELECT
      "cooperative"."_virt_filter_sk_9689870234368454" as "_virt_filter_sk_9689870234368454",
      "cooperative"."ss_customer_sk" as "ss_customer_sk"
  FROM
      "cooperative"
  GROUP BY
      1,
      2),
  uneven as (
  SELECT
      count(distinct "abundant"."_virt_filter_sk_9689870234368454") as "matched_customers",
      count(distinct "abundant"."ss_customer_sk") as "customers_in_window"
  FROM
      "abundant"),
  questionable as (
  SELECT
      count("cooperative"."ss_line_item") as "window_lines",
      count(CASE WHEN INVALID_REFERENCE_BUG<Missing source reference to ss.store.county> = INVALID_REFERENCE_BUG<Missing source reference to ss.customer.current_address.county> and INVALID_REFERENCE_BUG<Missing source reference to ss.store.state> = INVALID_REFERENCE_BUG<Missing source reference to ss.customer.current_address.state> THEN "cooperative"."ss_line_item" ELSE NULL END) as "matched_lines"
  FROM
      "cooperative")
  SELECT
      "questionable"."window_lines" as "window_lines",
      "uneven"."customers_in_window" as "customers_in_window",
      "questionable"."matched_lines" as "matched_lines",
      "uneven"."matched_customers" as "matched_customers"
  FROM
      "questionable"
      INNER JOIN "uneven" on 1=1
  ```
- `trilogy file write probe_scale.preql --run-and-delete`

  ```text
  Unexpected error in probe_scale.preql: Could not render the query: Missing source reference to ss.sale_date.month_seq. A planned reference has no backing source CTE -- typically an unsupported cross-rowset or membership shape the planner could not wire. Review the rowset/join structure (or file an issue if the query looks valid).

  Full SQL with sentinel(s):

  WITH
  wakeful as (
  SELECT
      "ss_store_sales"."SS_CUSTOMER_SK" as "ss_customer_sk",
      CASE WHEN "ss_sale_date_date"."D_MONTH_SEQ" BETWEEN 1188 AND 1190 THEN "ss_store_sales"."SS_CUSTOMER_SK" ELSE NULL END as "_virt_filter_sk_2960582277692406",
      CONCAT(cast("ss_store_sales"."SS_TICKET_NUMBER" as string), '-', cast("ss_store_sales"."SS_ITEM_SK" as string)) as "ss_line_item"
  FROM
      "store_sales" as "ss_store_sales"
      LEFT OUTER JOIN "date_dim" as "ss_sale_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_sale_date_date"."D_DATE_SK"),
  thoughtful as (
  SELECT
      "wakeful"."_virt_filter_sk_2960582277692406" as "_virt_filter_sk_2960582277692406",
      "wakeful"."ss_customer_sk" as "ss_customer_sk"
  FROM
      "wakeful"
  GROUP BY
      1,
      2),
  cooperative as (
  SELECT
      count(distinct "thoughtful"."_virt_filter_sk_2960582277692406") as "window_customers",
      count(distinct "thoughtful"."ss_customer_sk") as "total_customers"
  FROM
      "thoughtful"),
  cheerful as (
  SELECT
      count("wakeful"."ss_line_item") as "total_lines",
      count(CASE WHEN INVALID_REFERENCE_BUG<Missing source reference to ss.sale_date.month_seq> BETWEEN 1188 AND 1190 THEN "wakeful"."ss_line_item" ELSE NULL END) as "window_lines"
  FROM
      "wakeful")
  SELECT
      coalesce("cheerful"."total_lines",0) as "total_lines",
      coalesce("cheerful"."window_lines",0) as "window_lines",
      "cooperative"."total_customers" as "total_customers",
      "cooperative"."window_customers" as "window_customers"
  FROM
      "cheerful"
      INNER JOIN "cooperative" on 1=1
  ```
- `trilogy file write answer_2874833976.preql --run`

  ```text
  Syntax error in answer_2874833976.preql: Output column 'level' renames 'local.level' back to the name of an existing concept 'level' (defined at line 14) that 'local.level' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'level_out').
  ```
- `trilogy file write answer_426809370.preql --run`

  ```text
  Resolution error in answer_426809370.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: cs.return_customer.current_address.customer_address_at_cs_return_customer_current_address_sk_filtered_by_5987495888938297_grouped_by_cs.return_customer.current_address.city_cs.return_customer.current_address.country_cs.return_customer.current_address.county_cs.return_customer.current_address.gmt_offset_cs.return_customer.current_address.location_type_cs.return_customer.current_address.state_cs.return_customer.current_address.street_name_cs.return_customer.current_address.street_number_cs.return_customer.current_address.street_type_cs.return_customer.current_address.suite_number_cs.return_customer.current_address.zip_at_cs_return_customer_current_address_city_cs_return_customer_current_address_country_cs_return_customer_current_address_county_cs_return_customer_current_address_gmt_offset_cs_return_customer_current_address_location_type_cs_return_customer_current_address_state_cs_return_customer_current_address_street_name_cs_return_customer_current_address_street_number_cs_return_customer_current_address_street_type_cs_return_customer_current_address_suite_number_cs_return_customer_current_address_zip_at_cs_return_customer_current_address_city_cs_return_customer_current_address_country_cs_return_customer_current_address_county_cs_return_customer_current_address_gmt_offset_cs_return_customer_current_address_location_type_cs_return_customer_current_address_state_cs_return_customer_current_address_street_name_cs_return_customer_current_address_street_number_cs_return_customer_current_address_street_type_cs_return_customer_current_address_suite_number_cs_return_customer_current_address_zip onto cs.catalog_returns_at_cs_item_sk_cs_order_number_join_cs.catalog_sales_at_cs_item_sk_cs_order_number_join_cs.return_address.customer_address_at_cs_return_address_sk_join_cs.return_date.date_at_cs_return_date_sk_at_cs_item_sk_cs_order_number_grouped_by_cs.return_address.state_cs.return_customer.sk_at_cs_return_address_state_cs_return_customer_sk_join_cs.catalog_returns_at_cs_item_sk_cs_order_number_join_cs.catalog_sales_at_cs_item_sk_cs_order_number_join_cs.return_address.customer_address_at_cs_return_address_sk_join_cs.return_date.date_at_cs_return_date_sk_at_cs_item_sk_cs_order_number_grouped_by_cs.return_address.state_cs.return_customer.sk_at_cs_return_address_state_cs_return_customer_sk_grouped_by_cs.return_address.state_at_cs_return_address_state_at_cs_return_address_state_cs_return_customer_sk_at_cs_return_address_state_cs_return_customer_sk_at_cs_return_address_state_cs_return_customer_sk, cs.return_customer.customers_at_cs_return_customer_sk_at_cs_return_customer_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write answer_426809370.preql --run`

  ```text
  Resolution error in answer_426809370.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: cs.return_customer.current_address.customer_address_at_cs_return_customer_current_address_sk_join_cs.return_customer.customers_at_cs_return_customer_sk_at_cs_return_customer_sk_filtered_by_5987495888938297_grouped_by_cs.return_customer.current_address.city_cs.return_customer.current_address.country_cs.return_customer.current_address.county_cs.return_customer.current_address.gmt_offset_cs.return_customer.current_address.location_type_cs.return_customer.current_address.sk_cs.return_customer.current_address.state_cs.return_customer.current_address.street_name_cs.return_customer.current_address.street_number_cs.return_customer.current_address.street_type_cs.return_customer.current_address.suite_number_cs.return_customer.current_address.zip_at_cs_return_customer_current_address_sk onto cs.catalog_returns_at_cs_item_sk_cs_order_number_join_cs.catalog_sales_at_cs_item_sk_cs_order_number_join_cs.return_address.customer_address_at_cs_return_address_sk_join_cs.return_date.date_at_cs_return_date_sk_at_cs_item_sk_cs_order_number_filtered_by_2828915702879163_grouped_by_cs.return_address.state_cs.return_customer.sk_at_cs_return_address_state_cs_return_customer_sk_at_cs_return_address_state_cs_return_customer_sk_at_totals_customer_sk_totals_return_state_at_totals_customer_sk_totals_return_state. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write answer_426809370.preql --run`

  ```text
  Resolution error in answer_426809370.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: cs.return_customer.current_address.customer_address_at_cs_return_customer_current_address_sk_grouped_by_cs.return_customer.current_address.city_cs.return_customer.current_address.country_cs.return_customer.current_address.county_cs.return_customer.current_address.gmt_offset_cs.return_customer.current_address.location_type_cs.return_customer.current_address.state_cs.return_customer.current_address.street_name_cs.return_customer.current_address.street_number_cs.return_customer.current_address.street_type_cs.return_customer.current_address.suite_number_cs.return_customer.current_address.zip_at_cs_return_customer_current_address_city_cs_return_customer_current_address_country_cs_return_customer_current_address_county_cs_return_customer_current_address_gmt_offset_cs_return_customer_current_address_location_type_cs_return_customer_current_address_state_cs_return_customer_current_address_street_name_cs_return_customer_current_address_street_number_cs_return_customer_current_address_street_type_cs_return_customer_current_address_suite_number_cs_return_customer_current_address_zip_at_cs_return_customer_current_address_city_cs_return_customer_current_address_country_cs_return_customer_current_address_county_cs_return_customer_current_address_gmt_offset_cs_return_customer_current_address_location_type_cs_return_customer_current_address_state_cs_return_customer_current_address_street_name_cs_return_customer_current_address_street_number_cs_return_customer_current_address_street_type_cs_return_customer_current_address_suite_number_cs_return_customer_current_address_zip onto cs.catalog_returns_at_cs_item_sk_cs_order_number_join_cs.catalog_sales_at_cs_item_sk_cs_order_number_join_cs.return_address.customer_address_at_cs_return_address_sk_join_cs.return_date.date_at_cs_return_date_sk_at_cs_item_sk_cs_order_number_filtered_by_2828915702879163_grouped_by_cs.return_address.state_cs.return_customer.sk_at_cs_return_address_state_cs_return_customer_sk_at_cs_return_address_state_cs_return_customer_sk, cs.return_customer.customers_at_cs_return_customer_sk_at_cs_return_customer_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe4.preql --run-and-delete`

  ```text
  Resolution error in probe4.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: cs.return_customer.current_address.customer_address_at_cs_return_customer_current_address_sk_grouped_by_cs.return_customer.current_address.state_at_cs_return_customer_current_address_state_at_cs_return_customer_current_address_state onto cs.catalog_returns_at_cs_item_sk_cs_order_number_join_cs.catalog_sales_at_cs_item_sk_cs_order_number_join_cs.return_address.customer_address_at_cs_return_address_sk_join_cs.return_date.date_at_cs_return_date_sk_at_cs_item_sk_cs_order_number_filtered_by_2828915702879163_grouped_by_cs.return_address.state_cs.return_customer.sk_at_cs_return_address_state_cs_return_customer_sk_at_cs_return_address_state_cs_return_customer_sk, cs.return_customer.customers_at_cs_return_customer_sk_at_cs_return_customer_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe6.preql --run-and-delete`

  ```text
  Resolution error in probe6.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: cs.catalog_returns_at_cs_item_sk_cs_order_number_join_cs.catalog_sales_at_cs_item_sk_cs_order_number_join_cs.return_address.customer_address_at_cs_return_address_sk_join_cs.return_date.date_at_cs_return_date_sk_at_cs_item_sk_cs_order_number_filtered_by_2828915702879163_grouped_by_cs.return_address.state_cs.return_customer.sk_at_cs_return_address_state_cs_return_customer_sk_grouped_by_cs.return_address.state_at_cs_return_address_state_at_cs_return_address_state_at_state_avgs_return_state onto cs.catalog_returns_at_cs_item_sk_cs_order_number_join_cs.catalog_sales_at_cs_item_sk_cs_order_number_join_cs.return_customer.current_address.customer_address_at_cs_return_customer_current_address_sk_join_cs.return_customer.customers_at_cs_return_customer_sk_at_cs_item_sk_cs_order_number_filtered_by_5987495888938297. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe12.preql --run-and-delete`

  ```text
  Resolution error in probe12.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: ss.item.items_at_ss_item_sk_at_ss_item_sk onto ss.return_customer_demographic.customer_demographics_at_ss_return_customer_demographic_sk_at_ss_return_customer_demographic_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write verify3.preql --run-and-delete`

  ```text
  Resolution error in verify3.preql: WHERE input(s) ['ss.return_customer_demographic.sk'] cannot restrict output(s) ['demo_matched_lines'] (statement at line 4): no join or merge relates the filter's source to the source of those outputs, so the WHERE has no single row population to define -- the outputs would cross-join in unfiltered. Add a join/merge relating them, or scope the filter to the source it belongs to with an inline filtered aggregate (e.g. `sum(x ? <condition>)`).
  ```
- `trilogy file write answer_2852230229.preql --run`

  ```text
  Syntax error in answer_2852230229.preql: ORDER BY references 'local.cat_sort', which is not in the SELECT projection (line 18). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --local.cat_sort order by local.cat_sort asc`.
  ```
- `trilogy file write probe_diag1.preql --run-and-delete`

  ```text
  {
    "event": "write",
    "path": "probe_diag1.preql",
    "bytes": 1231
  }
  {
    "event": "output_truncated",
    "dropped_events": 0,
    "note": "Output exceeded the tool cap; intermediate events dropped (any error/summary events are preserved below). Narrow the call (--regex, --show, fewer rows) to see the rest."
  }
  {
    "event": "error",
    "message": "Syntax error in probe_diag1.preql: Missing source map entry for cs.billing_customer.first_name with pseudonyms set(), have map: {'ws.billing_customer.sk': {BuildDatasource(name='customers', columns=[BuildColumnAssignment(alias='C_CUSTOMER_SK', concept=w
  …
  'Nullable'>}, origin_address=None)], address=Address(location='customer', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[], env_label=None), grain=BuildGrain(components={'ws.billing_customer.sk'}, where_clause=None, _str='Grain<ws.billing_customer.sk>', _str_no_condition='Grain<ws.billing_customer.sk>', abstract=False), namespace='ws.billing_customer', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses=set())}}"
  }
  ```

### `undefined-concept`

- `trilogy file write probe_repro.preql --run-and-delete`

  ```text
  Syntax error in probe_repro.preql: Undefined concept: local.r (line 2, col 8, in SELECT).
  ```
- `trilogy file write probe3.preql --run-and-delete`

  ```text
  Syntax error in probe3.preql: 3 undefined concept references; fix all before re-running:
    - first_name (line 33, col 39, in ORDER BY); did you mean: s.ship_customer.first_name, s.billing_customer.first_name, s.purchasing_customer.first_name?
    - last_name (line 33, col 67, in ORDER BY); did you mean: s.ship_customer.last_name, s.billing_customer.last_name, s.purchasing_customer.last_name?
    - preferred_flag (line 33, col 94, in ORDER BY)
  ```
- `trilogy file write probe1.preql --run-and-delete --param zips=10001,20002`

  ```text
  Syntax error in probe1.preql: 3 undefined concept references; fix all before re-running:
    - cu.customer.sk (line 6, col 3, in SELECT); did you mean: cu.first_shipto_date.sk, cu.sk, cu.current_address.sk, cu.first_sales_date.sk, cu.current_household_demographics.sk, cu.current_demographics.sk?
    - cu.customer.current_address.zip (line 7, col 3, in SELECT); did you mean: cu.current_address.zip, cu.current_address.id, cu.current_address.city?
    - cu.customer.sk (line 8, col 7, in WHERE); did you mean: cu.first_shipto_date.sk, cu.sk, cu.current_address.sk, cu.first_sales_date.sk, cu.current_household_demographics.sk, cu.current_demographics.sk?
  ```
- `trilogy file write probe1.preql --run-and-delete`

  ```text
  Syntax error in probe1.preql: Undefined concept: electronics_mfr.item.manufacturer_id (line 9, col 33, in WHERE). Suggestions: ['electronics_mfr.it.manufacturer_id', 's.item.manufacturer_id', 'it.manufacturer_id', 'manufacturer_id']
  ```
- `trilogy file write probe2.preql --run-and-delete --content import raw.store_sales as ss;
import raw.all_sales as s;
select 'store_sales' as src, ss.date_dim.…em.sk) as n, sum(ss.ext_sales_price) as tot
where ss.date_dim.d_date in ('2000-01-03'::date, '1999-12-31'::date)
order by ss.date_dim.d_date
limit 6;`

  ```text
  Syntax error in probe2.preql: 4 undefined concept references; fix all before re-running:
    - ss.date_dim.d_week_seq (line 3, col 30, in SELECT); did you mean: ss.date_dim.d_date, ss.sale_date.week_seq, ss.return_date.week_seq, s.sale_date.week_seq?
    - ss.date_dim.d_date (line 3, col 54, in SELECT); did you mean: ss.date_dim.d_week_seq, ss.item.brand_name, ss.sale_date.sk?
    - ss.date_dim.d_date (line 4, col 7, in WHERE); did you mean: ss.date_dim.d_week_seq, ss.item.brand_name, ss.sale_date.sk?
    - ss.date_dim.d_date (line 5, col 10, in ORDER BY); did you mean: ss.date_dim.d_week_seq, ss.item.brand_name, ss.sale_date.sk?
  ```
- `trilogy file write answer_2874833976.preql --run`

  ```text
  Syntax error in answer_2874833976.preql: Undefined concept: _virt_agg_grouping_4453574046401692.
  ```
- `trilogy file write probe8.preql --run-and-delete`

  ```text
  Syntax error in probe8.preql: Undefined concept: _virt_agg_grouping_9769516219811737.
  ```
- `trilogy file write probe9.preql --run-and-delete`

  ```text
  Syntax error in probe9.preql: Undefined concept: _virt_agg_grouping_4453574046401692.
  ```
- `trilogy file write probe11.preql --run-and-delete`

  ```text
  Syntax error in probe11.preql: Undefined concept: _virt_agg_grouping_4453574046401692.
  ```
- `trilogy file write probe12.preql --run-and-delete`

  ```text
  Syntax error in probe12.preql: Undefined concept: _virt_agg_grouping_4453574046401692.
  ```
- `trilogy file write answer_2852230229.preql --run`

  ```text
  Syntax error in answer_2852230229.preql: Undefined concept: _virt_agg_grouping_164218295708188.
  ```
- `trilogy file write probe2.preql --run-and-delete`

  ```text
  Syntax error in probe2.preql: Undefined concept: _virt_agg_grouping_164218295708188.
  ```
- `trilogy file write probe4.preql --run-and-delete`

  ```text
  Syntax error in probe4.preql: Undefined concept: _virt_agg_grouping_164218295708188.
  ```
- `trilogy file write probe7.preql --run-and-delete`

  ```text
  Syntax error in probe7.preql: Undefined concept: _virt_agg_grouping_5549377253046357.
  ```
- `trilogy file write probe8.preql --run-and-delete`

  ```text
  Syntax error in probe8.preql: Undefined concept: _virt_agg_grouping_5549377253046357.
  ```

### `disabled-tool`

- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/inventory.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

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
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `cli-misuse`

- `trilogy explore raw/orders.preql`

  ```text
  Invalid value for 'PATH': File 'raw/orders.preql' does not exist.
  ```
- `trilogy explore raw/web_returns.preql`

  ```text
  Invalid value for 'PATH': File 'raw/web_returns.preql' does not exist.
  ```
- `trilogy explore raw/all_sales.preql --ns item --ns pos_bill_address sale_date --ns sale_date`

  ```text
  Got unexpected extra argument (sale_date)
  ```
- `trilogy explore raw/sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/sales.preql' does not exist.
  ```
- `trilogy explore raw/sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/sales.preql' does not exist.
  ```
- `trilogy explore raw.web_sales`

  ```text
  Invalid value for 'PATH': File 'raw.web_sales' does not exist.
  ```
- `trilogy explore raw.catalog_sales`

  ```text
  Invalid value for 'PATH': File 'raw.catalog_sales' does not exist.
  ```
- `trilogy explore raw/orders.preql`

  ```text
  Invalid value for 'PATH': File 'raw/orders.preql' does not exist.
  ```
- `trilogy explore raw/catalog_returns.preql`

  ```text
  Invalid value for 'PATH': File 'raw/catalog_returns.preql' does not exist.
  ```

### `planner-recursion`

- `trilogy file write answer_2802535988.preql --run`

  ```text
  Resolution error in answer_2802535988.preql: query could not be planned; this is a bug.
  ```
- `trilogy file write answer_71623752.preql --run`

  ```text
  Resolution error in answer_71623752.preql: query could not be planned; this is a bug.
  ```
- `trilogy file write probe3.preql --run-and-delete`

  ```text
  Resolution error in probe3.preql: query could not be planned; this is a bug.
  ```

### `join-resolution`

- `trilogy file write probe_verify.preql --run-and-delete`

  ```text
  Resolution error in probe_verify.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 3 disconnected subgraphs: {catalog_sales.call_center.sk, catalog_sales.sale_date.date}; {store_sales.sale_date.date, store_sales.store.sk}; {web_sales.sale_date.date, web_sales.web_page.sk}. Are you missing a join or merge statement to relate them?
  ```

### `file-not-found`

- `trilogy file write answer_4111870542.preql --run`

  ```text
  Unexpected error in answer_4111870542.preql: (_duckdb.CatalogException) Catalog Error: Type with name number does not exist!
  Did you mean "numeric"?

  LINE 109: ...rrent"."_virt_filter_return_quantity_2413472390232619") as number) / cast(( sum("abhorrent"."_virt_filter_return_quant...
                                                                          ^
  [SQL:
  WITH
  vacuous as (
  SELECT
      "d_date"."D_WEEK_SEQ" as "target_weeks_ws"
  FROM
      "date_dim" as "d_date"
  WHERE
      (date '2000-06-30' is not distinct from cast("d_date"."D_DATE" as date) or date '2000-09-27' is not distinct from cast("d_date"."D_DATE" as date) or date '2000-11-17' is not distinct from cast("d_date"."D_DATE" as date))

  GROUP BY
      1),
  abundant as (
  SELECT
       'CATALOG'  as "a_channel",
      "a_catalog_sales_unified"."CS_ITEM_SK" as "a_item_sk",
      "a_catalog_sales_unified"."CS_ORDER_NUMBER" as "a_order_id"
  FROM
      "catalog_sales" as "a_catalog_sales_unified"
  UNION ALL
  SELECT
       'STORE'  as "a_channel",
      "a_store_sales_unified"."SS_ITEM_SK" as "a_item_sk",
      "a_store_sales_unified"."SS_TICKET_NUMBER" as "a_order_id"
  FROM
      "store_sales" as "a_store_sales_unified"
  UNION ALL
  SELECT
       'WEB'  as "a_channel",
      "a_web_sales_unified"."WS_ITEM_SK" as "a_item_sk",
      "a_web_sales_unified"."WS_ORDER_NUMBER" as "a_order_id"
  FROM
      "web_sales" as "a_web_sales_unified"),
  cheerful as (
  SELECT
       'CATALOG'  as "a_channel",
       true  as "a_is_returned",
      "a_catalog_returns_unified"."CR_ITEM_SK" as "a_item_sk",
      "a_catalog_returns_unified"."CR_ORDER_NUMBER" as "a_order_id",
      "a_catalog_returns_unified"."CR_RETURNED_DATE_SK" as "a_return_date_sk",
      "a_catalog_returns_unified"."CR_RETURN_QUANTITY" as "a_return_quantity"
  FROM
      "catalog_returns" as "a_catalog_returns_unified"
  UNION ALL
  SELECT
       'STORE'  as "a_channel",
       true  as "a_is_returned",
      "a_store_returns_unified"."SR_ITEM_SK" as "a_item_sk",
      "a_store_returns_unified"."SR_TICKET_NUMBER" as "a_order_id",
      "a_store_returns_unified"."SR_RETURNED_DATE_SK" as "a_return_date_sk",
      "a_store_returns_unified"."SR_RETURN_QUANTITY" as "a_return_quantity"
  FROM
      "store_returns" as "a_store_returns_unified"
  UNION ALL
  SELECT
       'WEB'  as "a_channel",
       true  as "a_is_returned",
      "a_web_returns_unified"."WR_ITEM_SK" as "a_item_sk",
      "a_web_returns_unified"."WR_ORDER_NUMBER" as "a_order_id",
      "a_web_returns_unified"."WR_RETURNED_DATE_SK" as "a_return_date_sk",
      "a_web_returns_unified"."WR_RETURN_QUANTITY" as "a_return_quantity"
  FROM
      "web_returns" as "a_web_returns_unified"),
  juicy as (
  SELECT
      "a_item_items"."I_ITEM_ID" as "a_item_id",
      "cheerful"."a_is_returned" as "a_is_returned",
      "cheerful"."a_return_quantity" as "a_return_quantity",
      coalesce("a_item_items"."I_ITEM_SK","abundant"."a_item_sk","cheerful"."a_item_sk") as "a_item_sk",
      coalesce("abundant"."a_channel","cheerful"."a_channel") as "a_channel",
      coalesce("abundant"."a_order_id","cheerful"."a_order_id") as "a_order_id"
  FROM
      "abundant"
      FULL JOIN "cheerful" on "abundant"."a_channel" = "cheerful"."a_channel" AND "abundant"."a_item_sk" = "cheerful"."a_item_sk" AND "abundant"."a_order_id" = "cheerful"."a_order_id"
      RIGHT OUTER JOIN "date_dim" as "a_return_date_date" on "cheerful"."a_return_date_sk" = "a_return_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "item" as "a_item_items" on "abundant"."a_item_sk" = "a_item_items"."I_ITEM_SK"
  WHERE
      exists (select 1 from vacuous where vacuous."target_weeks_ws" is not distinct from "a_return_date_date"."D_WEEK_SEQ")
  ),
  abhorrent as (
  SELECT
      "juicy"."a_channel" as "a_channel",
      "juicy"."a_is_returned" as "a_is_returned",
      "juicy"."a_item_id" as "a_item_id",
      "juicy"."a_item_id" as "item_code",
      "juicy"."a_item_sk" as "a_item_sk",
      "juicy"."a_order_id" as "a_order_id",
      CASE WHEN "juicy"."a_is_returned" and "juicy"."a_channel" = 'CATALOG' THEN "juicy"."a_return_quantity" ELSE NULL END as "_virt_filter_return_quantity_3288225266003563",
      CASE WHEN "juicy"."a_is_returned" and "juicy"."a_channel" = 'STORE' THEN "juicy"."a_return_quantity" ELSE NULL END as "_virt_filter_return_quantity_5678499816464623",
      CASE WHEN "juicy"."a_is_returned" and "juicy"."a_channel" = 'WEB' THEN "juicy"."a_return_quantity" ELSE NULL END as "_virt_filter_return_quantity_2413472390232619",
      md5(CONCAT_WS('', coalesce(cast("juicy"."a_item_sk" as string),'
  '), coalesce(cast("juicy"."a_channel" as string),'
  '), coalesce(cast("juicy"."a_order_id" as string),'
  '))) as "_virt_func_hash_552524362417448"
  FROM
      "juicy"),
  late as (
  SELECT
      "abhorrent"."a_channel" as "a_channel",
      "abhorrent"."a_item_id" as "a_item_id",
      "abhorrent"."a_item_sk" as "a_item_sk",
      "abhorrent"."a_order_id" as "a_order_id",
      "abhorrent"."item_code" as "item_code",
      CASE WHEN "abhorrent"."a_is_returned" and "abhorrent"."a_channel" = 'CATALOG' THEN "abhorrent"."_virt_func_hash_552524362417448" ELSE NULL END as "_virt_filter_5974889622568974",
      CASE WHEN "abhorrent"."a_is_returned" and "abhorrent"."a_channel" = 'STORE' THEN "abhorrent"."_virt_func_hash_552524362417448" ELSE NULL END as "_virt_filter_6274118916034616",
      CASE WHEN "abhorrent"."a_is_returned" and "abhorrent"."a_channel" = 'WEB' THEN "abhorrent"."_virt_func_hash_552524362417448" ELSE NULL END as "_virt_filter_857702971099003"
  FROM
      "abhorrent"),
  macho as (
  SELECT
      "late"."a_item_id" as "a_item_id",
      ( ( cast(sum("abhorrent"."_virt_filter_return_quantity_2413472390232619") as number) / cast(( sum("abhorrent"."_virt_filter_return_quantity_5678499816464623") + sum("abhorrent"."_virt_filter_return_quantity_3288225266003563") ) + sum("abhorrent"."_virt_filter_return_quantity_2413472390232619") as number) ) / 3 ) * 100 as "web_pct",
      ( ( cast(sum("abhorrent"."_virt_filter_return_quantity_3288225266003563") as number) / cast(( sum("abhorrent"."_virt_filter_return_quantity_5678499816464623") + sum("abhorrent"."_virt_filter_return_quantity_3288225266003563") ) + sum("abhorrent"."_virt_filter_return_quantity_2413472390232619") as number) ) / 3 ) * 100 as "catalog_pct",
      ( ( cast(sum("abhorrent"."_virt_filter_return_quantity_5678499816464623") as number) / cast(( sum("abhorrent"."_virt_filter_return_quantity_5678499816464623") + sum("abhorrent"."_virt_filter_return_quantity_3288225266003563") ) + sum("abhorrent"."_virt_filter_return_quantity_2413472390232619") as number) ) / 3 ) * 100 as "store_pct",
      cast(( sum("abhorrent"."_virt_filter_return_quantity_5678499816464623") + sum("abhorrent"."_virt_filter_return_quantity_3288225266003563") ) + sum("abhorrent"."_virt_filter_return_quantity_2413472390232619") as number) / 3 as "three_channel_avg",
      count("late"."_virt_filter_5974889622568974") as "catalog_ret_rows",
      count("late"."_virt_filter_6274118916034616") as "store_ret_rows",
      count("late"."_virt_filter_857702971099003") as "web_ret_rows",
      sum("abhorrent"."_virt_filter_return_quantity_2413472390232619") as "web_qty",
      sum("abhorrent"."_virt_filter_return_quantity_3288225266003563") as "catalog_qty",
      sum("abhorrent"."_virt_filter_return_quantity_5678499816464623") as "store_qty"
  FROM
      "late"
      FULL JOIN "abhorrent" on "late"."a_channel" is not distinct from "abhorrent"."a_channel" AND "late"."a_item_sk" is not distinct from "abhorrent"."a_item_sk" AND "late"."a_order_id" is not distinct from "abhorrent"."a_order_id"
  GROUP BY
      1
  HAVING
      "store_ret_rows" > 0 and "catalog_ret_rows" > 0 and "web_ret_rows" > 0
  )
  SELECT
      "late"."item_code" as "item_code",
      "macho"."store_qty" as "store_qty",
      "macho"."store_pct" as "store_pct",
      "macho"."catalog_qty" as "catalog_qty",
      "macho"."catalog_pct" as "catalog_pct",
      "macho"."web_qty" as "web_qty",
      "macho"."web_pct" as "web_pct",
      "macho"."three_channel_avg" as "three_channel_avg"
  FROM
      "late"
      INNER JOIN "macho" on "late"."a_item_id" is not distinct from "macho"."a_item_id"
  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      "macho"."catalog_ret_rows",
      "macho"."store_ret_rows",
      "macho"."web_ret_rows"
  ORDER BY
      "late"."item_code" asc nulls first,
      "macho"."store_qty" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
