# Trilogy failure analysis — 20261017-225410

- Run `20261017-225406_enriched_deepseek_deepseek-flash` | `deepseek/deepseek-flash` | sf=1
- `trilogy` calls: 1325 | failed: 65 (5%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 37 | 57% |
| `other` | 15 | 23% |
| `join-resolution` | 7 | 11% |
| `undefined-concept` | 5 | 8% |
| `planner-recursion` | 1 | 2% |

## Detail

### `syntax-parse`

- `trilogy file write probe_e.preql --run-and-delete`

  ```text
  refused to write 'probe_e.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...EB' or s.channel = 'CATALOG'  ??? group by auto  order by channe...
  ```
- `trilogy file write probe_e.preql --run-and-delete`

  ```text
  refused to write 'probe_e.preql': not syntactically valid Trilogy.

  Parse error:
   --> 9:167
    |
  9 |     count(grain(s.item.sk, s.channel, s.order_id) ? (s.return_channel_dim_text_id is not null and s.channel_dim_text_id is not null and s.return_channel_dim_text_id <> s.channel_dim_text_id)) as n_ret_ent_differs
    |                                                                                                                                                                       ^---
    |
    = expected sum_operator
  Location:
  ...s.return_channel_dim_text_id < ??? > s.channel_dim_text_id)) as n...
  ```
- `trilogy file write probe_j.preql --run-and-delete`

  ```text
  refused to write 'probe_j.preql': not syntactically valid Trilogy.

  Parse error:
    --> 33:1
     |
  33 | by *;
     | ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...tacked.profit), 0) as profit  ??? by *;
  ```
- `trilogy file write probe_l.preql --run-and-delete`

  ```text
  refused to write 'probe_l.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:5
    |
  4 |     (where s.channel = 'WEB'
    |     ^---
    |
    = expected select_statement, tvf_union_invocation, tvf_except_invocation, or tvf_intersect_invocation
  Location:
     with web_sales_arm as      ??? (where s.channel = 'WEB'
  ```
- `trilogy file write probe_m.preql --run-and-delete`

  ```text
  refused to write 'probe_m.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:5
    |
  4 |     (where s.channel = 'WEB'
    |     ^---
    |
    = expected select_statement, tvf_union_invocation, tvf_except_invocation, or tvf_intersect_invocation
  Location:
  ...;    with web_ret_arm as      ??? (where s.channel = 'WEB'
  ```
- `trilogy file write probe_c.preql --run-and-delete`

  ```text
  refused to write 'probe_c.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...01, web_2002)) as same_count  ??? from (    select s.billing_cus...
  ```
- `trilogy file write probe_b.preql --run-and-delete`

  ```text
  refused to write 'probe_b.preql': not syntactically valid Trilogy.

  Parse error:
    --> 12:1
     |
  12 | select
     | ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...t_sales_price) as item_total  ??? select    item_totals.category...
  ```
- `trilogy file write probe_null.preql --run-and-delete`

  ```text
  refused to write 'probe_null.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:1
    |
  7 | select
    | ^---
    |
    = expected limit, order_by, THEN_LA, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...aw,  where s.channel is null  ??? select      count(ch.channel_r...
  ```
- `trilogy file write probe3.preql --run-and-delete`

  ```text
  refused to write 'probe3.preql': not syntactically valid Trilogy.

  Parse error:
    --> 36:1
     |
  36 | equal join store_side.item_sk = catalog_side.item_sk and store_side.customer_sk = catalog_side.customer_sk
     | ^---
     |
     = expected limit, order_by, THEN_LA, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...ide.order_number is not null  ??? equal join store_side.item_sk
  ```
- `trilogy file write probe3.preql --run-and-delete`

  ```text
  refused to write 'probe3.preql': not syntactically valid Trilogy.

  Parse error:
    --> 37:1
     |
  37 | equal join store_side.item_sk = catalog_side.item_sk and store_side.customer_sk = catalog_side.customer_sk;
     | ^---
     |
     = expected limit, order_by, THEN_LA, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...ide.order_number is not null  ??? equal join store_side.item_sk
  ```
- `trilogy file write probe_1835050598.preql --run-and-delete`

  ```text
  refused to write 'probe_1835050598.preql': not syntactically valid Trilogy.

  Parse error:
    --> 17:1
     |
  17 | by *
     | ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...cs.order_number) as n_orders  ??? by *  where cs.sale_date.year
  ```
- `trilogy file write answer_2844519538.preql --run`

  ```text
  refused to write 'answer_2844519538.preql': not syntactically valid Trilogy.

  Parse error:
    --> 23:43
     |
  23 |     and lower(ss.customer.birth_country) <> lower(ss.customer.current_address.country)
     |                                           ^---
     |
     = expected sum_operator
  Location:
  ...r(ss.customer.birth_country) < ??? > lower(ss.customer.current_ad...
  ```
- `trilogy file write probe_a.preql --run-and-delete`

  ```text
  refused to write 'probe_a.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:1
    |
  7 | by *;
    | ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ..._quantity) as n_catalog_rows  ??? by *;    where c.store_sale_da...
  ```
- `trilogy file write probe_nullq.preql --run-and-delete`

  ```text
  refused to write 'probe_nullq.preql': not syntactically valid Trilogy.

  Parse error:
    --> 14:1
     |
  14 | by *;
     | ^---
     |
     = expected limit, order_by, THEN_LA, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...education_status = 'College'  ??? by *;
  ```
- `trilogy file write p_a.preql --run-and-delete`

  ```text
  refused to write 'p_a.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [213]: A `by <grain>` clause must follow an aggregate, but the expression before it has none. If the `by` sits inside an aggregate's parentheses (`max(x by *)`), move it outside the call: `max(x) by *`. To take each distinct value once per grain, wrap it in `group(...)` - e.g. `group(item.current_price) by item.id, item.category`. For a reduction, use an aggregate: `sum(x) by ...`, `avg(x) by ...`, `max(x) by ...`.
  Location:
  ...nth_of_year between 9 and 12  ??? by *;
  ```
- `trilogy file write p_b.preql --run-and-delete`

  ```text
  refused to write 'p_b.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [213]: A `by <grain>` clause must follow an aggregate, but the expression before it has none. If the `by` sits inside an aggregate's parentheses (`max(x by *)`), move it outside the call: `max(x) by *`. To take each distinct value once per grain, wrap it in `group(...)` - e.g. `group(item.current_price) by item.id, item.category`. For a reduction, use an aggregate: `sum(x) by ...`, `avg(x) by ...`, `max(x) by ...`.
  Location:
  ...r.sk = ss.return_customer.sk  ??? by *;
  ```
- `trilogy file write probe_chk.preql --run-and-delete`

  ```text
  refused to write 'probe_chk.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [231]: A `subset|union join` cannot follow a trailing `where`. Put the join right after the select list, with the filter either before `select` or after the join: `where <filters> select <cols> subset join a.key = b.key` or `select <cols> subset join a.key = b.key where <filters>`. Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ..._demographics.sk is not null  ??? subset join store_custs.csk =
  ```
- `trilogy file write probe8_751385098.preql --run-and-delete`

  ```text
  refused to write 'probe8_751385098.preql': not syntactically valid Trilogy.

  Parse error:
    --> 10:33
     |
  10 |          or (all_sales.channel <> 'STORE' and all_sales.billing_customer.sk is not null))
     |                                 ^---
     |
     = expected sum_operator
  Location:
         or (all_sales.channel < ??? > 'STORE' and all_sales.billin...
  ```
- `trilogy file write probe_b.preql --run-and-delete`

  ```text
  refused to write 'probe_b.preql': not syntactically valid Trilogy.

  Parse error:
    --> 25:4
     |
  25 | -- the profile rows themselves
     |    ^---
     |
     = expected PURPOSE, PROPERTY, UNIQUE, or AUTO
  Location:
   item.size = 'N/A')  );    -- ??? the profile rows themselves  s...
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
  Syntax [202]: Missing closing semicolon? Statements must be terminated with a semicolon `;`.
  Location:
  ...sale_date.month_of_year = 12  ???
  ```
- `trilogy file write probe_b.preql --run-and-delete`

  ```text
  refused to write 'probe_b.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:44
    |
  7 |   count(ss.line_item ? is_not_distinct_from(ss.customer.sk, ss.return_customer.sk)) as same_cust_null_safe,
    |                                            ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...ne_item ? is_not_distinct_from ??? (ss.customer.sk, ss.return_cus...
  ```
- `trilogy file write probe_w6.preql --run-and-delete`

  ```text
  refused to write 'probe_w6.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [231]: A `subset|union join` cannot follow a trailing `where`. Put the join right after the select list, with the filter either before `select` or after the join: `where <filters> select <cols> subset join a.key = b.key` or `select <cols> subset join a.key = b.key where <filters>`. Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...eligible.cust_sk is not null  ??? union join ss.customer.sk = el...
  ```
- `trilogy file write probe_w8.preql --run-and-delete`

  ```text
  refused to write 'probe_w8.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [231]: A `subset|union join` cannot follow a trailing `where`. Put the join right after the select list, with the filter either before `select` or after the join: `where <filters> select <cols> subset join a.key = b.key` or `select <cols> subset join a.key = b.key where <filters>`. Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...eligible.cust_sk is not null  ??? union join ss.customer.sk = el...
  ```
- `trilogy file write probe2.preql --run-and-delete`

  ```text
  refused to write 'probe2.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...d s.sale_date.week_seq <= 62  ??? group by s.sale_date.week_seq
  ```
- `trilogy file write answer_3063407983.preql --run`

  ```text
  refused to write 'answer_3063407983.preql': not syntactically valid Trilogy.

  Parse error:
    --> 12:41
     |
  12 |   and ss.customer.current_address.city <> ss.pos_address.city
     |                                         ^---
     |
     = expected sum_operator
  Location:
  ...ustomer.current_address.city < ??? > ss.pos_address.city  select
  ```
- `trilogy file write probe05.preql --run-and-delete`

  ```text
  refused to write 'probe05.preql': not syntactically valid Trilogy.

  Parse error:
    --> 19:1
     |
  19 | order by cnt asc
     | ^---
     |
     = expected EOI, block, or show_statement
  Location:
  ....customer_sk)) as grp_count;  ??? order by cnt asc  limit 12;
  ```
- `trilogy file write probe_bvs.preql --run-and-delete`

  ```text
  refused to write 'probe_bvs.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:28
    |
  7 |     sum(1 ? is_not_distinct(ws.billing_customer.id, ws.ship_customer.id)) as same_customer
    |                            ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...,      sum(1 ? is_not_distinct ??? (ws.billing_customer.id, ws.sh...
  ```
- `trilogy file write probe3.preql --run-and-delete`

  ```text
  refused to write 'probe3.preql': not syntactically valid Trilogy.

  Parse error:
   --> 8:1
    |
  8 | grouping
    | ^---
    |
    = expected limit, order_by, THEN_LA, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...e_date.date) in (2001, 2002)  ??? grouping
  ```
- `trilogy file write answer_3046445280.preql --run`

  ```text
  refused to write 'answer_3046445280.preql': not syntactically valid Trilogy.

  Parse error:
    --> 28:17
     |
  28 |   and prev_qty <> 0
     |                 ^---
     |
     = expected sum_operator
  Location:
   = 2002) > 0    and prev_qty < ??? > 0    and (curr_qty * 1.0) /
  ```
- `trilogy file write probeB.preql --run-and-delete`

  ```text
  refused to write 'probeB.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [231]: A `subset|union join` cannot follow a trailing `where`. Put the join right after the select list, with the filter either before `select` or after the join: `where <filters> select <cols> subset join a.key = b.key` or `select <cols> subset join a.key = b.key where <filters>`. Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
   and ss.store.sk is not null  ??? subset join r.outlet = ss.stor...
  ```
- `trilogy file write probe_a.preql --run-and-delete`

  ```text
  refused to write 'probe_a.preql': not syntactically valid Trilogy.

  Parse error:
    --> 17:35
     |
  17 |     count_distinct(ss.customer.sk?) as x;
     |                                   ^---
     |
     = expected _and_conditional
  Location:
  ...count_distinct(ss.customer.sk? ??? ) as x;
  ```
- `trilogy file write probe_b.preql --run-and-delete`

  ```text
  refused to write 'probe_b.preql': not syntactically valid Trilogy.

  Parse error:
    --> 25:19
     |
  25 | having avg_total <> 0
     |                   ^---
     |
     = expected sum_operator
  Location:
   avg_total  having avg_total < ??? > 0     and (abs(total - avg_t...
  ```
- `trilogy file write probe10_1197120511.preql --run-and-delete`

  ```text
  refused to write 'probe10_1197120511.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [105]: A `rowset`/`auto`/`metric`/`property` definition connects its name to its expression with `<-`, not `as` - write `rowset base <- select ...;` (a `rowset` may also use the `with base as select ...;` form). For `auto`/`metric`/`property` only `<-` is valid, e.g. `auto total <- sum(sales.amount);`.
  Location:
  ...les as ws;    rowset item_avg ??? as      where ws.sale_date.dat...
  ```
- `trilogy file write answer_3734509910.preql --run`

  ```text
  refused to write 'answer_3734509910.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [202]: Missing closing semicolon? Statements must be terminated with a semicolon `;`.
  Location:
     class_pct asc nulls first  ???
  ```
- `trilogy file write probe_a.preql --run-and-delete`

  ```text
  refused to write 'probe_a.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:1
    |
  4 | order by i.class asc nulls first
    | ^---
    |
    = expected metadata, where, having, select_grouping, or JOIN_TYPE
  Location:
  ..._code, i.class as item_class  ??? order by i.class asc nulls fir...
  ```
- `trilogy file write probe_a.preql --run-and-delete`

  ```text
  refused to write 'probe_a.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:1
    |
  4 | order by i.class asc
    | ^---
    |
    = expected metadata, where, having, select_grouping, or JOIN_TYPE
  Location:
  ..._code, i.class as item_class  ??? order by i.class asc  limit 3
  ```

### `other`

- `trilogy file write probe_1835050598.preql --run-and-delete`

  ```text
  Syntax error in probe_1835050598.preql: Aggregate concept local.line_birth_year cannot reference itself. If defining a new concept in a select, use a new name.
  ```
- `trilogy file write p_c.preql --run-and-delete`

  ```text
  Unexpected error in p_c.preql: (_duckdb.BinderException) Binder Error: Referenced table "abundant" not found!
  Candidate tables: "wakeful"

  LINE 31:     "abundant"."pairs_cust_sk" as "pairs_cust_sk",
               ^
  [SQL:
  WITH
  abundant as (
  SELECT
      "ss_store_returns"."SR_ITEM_SK" as "pairs_item_sk",
      "ss_store_sales"."SS_CUSTOMER_SK" as "pairs_cust_sk"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "store_returns" as "ss_store_returns" on "ss_store_sales"."SS_ITEM_SK" = "ss_store_returns"."SR_ITEM_SK" AND "ss_store_sales"."SS_TICKET_NUMBER" = "ss_store_returns"."SR_TICKET_NUMBER"
      INNER JOIN "date_dim" as "ss_sale_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_sale_date_date"."D_DATE_SK"
      INNER JOIN "date_dim" as "ss_return_date_date" on "ss_store_returns"."SR_RETURNED_DATE_SK" = "ss_return_date_date"."D_DATE_SK"
  WHERE
      "ss_sale_date_date"."D_YEAR" = 1999 and "ss_sale_date_date"."D_MOY" = 9 and "ss_return_date_date"."D_YEAR" = 1999 and "ss_return_date_date"."D_MOY" BETWEEN 9 AND 12 and "ss_store_sales"."SS_CUSTOMER_SK" = "ss_store_returns"."SR_CUSTOMER_SK"

  GROUP BY
      1,
      2),
  wakeful as (
  SELECT
      "cs_catalog_sales"."CS_ITEM_SK" as "cs_item_sk",
      "cs_catalog_sales"."CS_ORDER_NUMBER" as "cs_order_number",
      "cs_catalog_sales"."CS_QUANTITY" as "cs_quantity"
  FROM
      "catalog_sales" as "cs_catalog_sales"
      INNER JOIN "date_dim" as "cs_sale_date_date" on "cs_catalog_sales"."CS_SOLD_DATE_SK" = "cs_sale_date_date"."D_DATE_SK"
  WHERE
      ("cs_sale_date_date"."D_YEAR" is not null and "cs_sale_date_date"."D_YEAR" in (1999,2000,2001)) and exists (select 1 from abundant where abundant."pairs_cust_sk" is not distinct from "cs_catalog_sales"."CS_BILL_CUSTOMER_SK" and abundant."pairs_item_sk" is not distinct from "cs_catalog_sales"."CS_ITEM_SK")
  ),
  concerned as (
  SELECT
      "abundant"."pairs_cust_sk" as "pairs_cust_sk",
      "abundant"."pairs_item_sk" as "pairs_item_sk",
      "wakeful"."cs_item_sk" as "cs_item_sk",
      "wakeful"."cs_order_number" as "cs_order_number",
      "wakeful"."cs_quantity" as "cs_quantity"
  FROM
      "wakeful"),
  sparkling as (
  SELECT
      sum("concerned"."cs_quantity") as "per_pair_qty"
  FROM
      "concerned"
  GROUP BY
      "concerned"."pairs_cust_sk",
      "concerned"."pairs_item_sk"),
  young as (
  SELECT
      count(md5(CONCAT_WS('', coalesce(cast("concerned"."cs_item_sk" as string),'
  '), coalesce(cast("concerned"."cs_order_number" as string),'
  ')))) as "n_cat_lines",
      sum("concerned"."cs_quantity") as "catalog_qty"
  FROM
      "concerned")
  SELECT
      "young"."catalog_qty" as "catalog_qty",
      "young"."n_cat_lines" as "n_cat_lines",
      "sparkling"."per_pair_qty" as "per_pair_qty"
  FROM
      "young"
      INNER JOIN "sparkling" on 1=1
  GROUP BY
      1,
      2,
      3]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy file write answer_2802535988.preql --run`

  ```text
  trilogy error: subprocess timed out after 600s.
  ```
- `trilogy file write probe_ctx.preql --run-and-delete`

  ```text
  Syntax error in probe_ctx.preql: Output column 'a_full' renames 'local.a_full' back to the name of an existing concept 'a_full' (defined at line 5) that 'local.a_full' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'a_full_out').
  ```
- `trilogy file write probe_a.preql --run-and-delete`

  ```text
  Syntax error in probe_a.preql: Output column 'total_sk' renames 'local.total_sk' back to the name of an existing concept 'total_sk' (defined at line 25) that 'local.total_sk' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'total_sk_out').
  ```
- `trilogy file write probe_h.preql --run-and-delete`

  ```text
  Unexpected error in probe_h.preql: Could not render the query: Missing source reference to ss.net_profit; global (by *) aggregate local._virt_agg_avg_1147483187926681_wscope rendered in CTE yummy at keyed grain Grain<ss.item.sk. A planned reference has no backing source CTE -- typically an unsupported cross-rowset or membership shape the planner could not wire. Review the rowset/join structure (or file an issue if the query looks valid).

  Full SQL with sentinel(s):

  WITH
  wakeful as (
  SELECT
      "ss_store_sales"."SS_ADDR_SK" as "ss_pos_address_sk",
      "ss_store_sales"."SS_ITEM_SK" as "ss_item_sk",
      "ss_store_sales"."SS_NET_PROFIT" as "ss_net_profit",
      "ss_store_sales"."SS_STORE_SK" as "ss_store_sk"
  FROM
      "store_sales" as "ss_store_sales"
  WHERE
      "ss_store_sales"."SS_STORE_SK" = 1
  ),
  thoughtful as (
  SELECT
      "wakeful"."ss_item_sk" as "ss_item_sk",
      avg("wakeful"."ss_net_profit") as "_virt_agg_avg_4106532836187985_wscope"
  FROM
      "wakeful"
  GROUP BY
      1),
  cheerful as (
  SELECT
      avg(CASE WHEN ( "wakeful"."ss_store_sk" = 1 and "wakeful"."ss_pos_address_sk" is null ) THEN "wakeful"."ss_net_profit" ELSE NULL END) as "_virt_agg_avg_1147483187926681_wscope"
  FROM
      "wakeful"),
  cooperative as (
  SELECT
      "wakeful"."ss_item_sk" as "ss_item_sk",
      avg("wakeful"."ss_net_profit") as "_best_perf_avg_np",
      avg("wakeful"."ss_net_profit") as "_virt_agg_avg_7425509928788153"
  FROM
      "wakeful"
      INNER JOIN "thoughtful" on "wakeful"."ss_item_sk" = "thoughtful"."ss_item_sk"
      INNER JOIN "cheerful" on 1=1
  GROUP BY
      1),
  abundant as (
  SELECT
      "cooperative"."_best_perf_avg_np" as "_best_perf_avg_np",
      "cooperative"."ss_item_sk" as "ss_item_sk",
      rank() over (order by "cooperative"."_virt_agg_avg_7425509928788153" asc ) as "_best_perf_rk"
  FROM
      "cooperative"),
  quizzical as (
  SELECT
      "ss_item_items"."I_ITEM_SK" as "_best_perf_item_sk",
      "ss_item_items"."I_PRODUCT_NAME" as "_best_perf_pname"
  FROM
      "item" as "ss_item_items"),
  uneven as (
  SELECT
      "abundant"."_best_perf_avg_np" as "_best_perf_avg_np",
      "abundant"."_best_perf_rk" as "_best_perf_rk",
      "quizzical"."_best_perf_pname" as "_best_perf_pname"
  FROM
      "abundant"
      INNER JOIN "thoughtful" on "abundant"."ss_item_sk" = "thoughtful"."ss_item_sk"
      INNER JOIN "quizzical" on "abundant"."ss_item_sk" = "quizzical"."_best_perf_item_sk"
      INNER JOIN "cheerful" on 1=1
  WHERE
      "thoughtful"."_virt_agg_avg_4106532836187985_wscope" > 0.9 * ("cheerful"."_virt_agg_avg_1147483187926681_wscope")
  ),
  yummy as (
  SELECT
      "uneven"."_best_perf_avg_np" as "_best_perf_avg_np",
      "uneven"."_best_perf_pname" as "_best_perf_pname",
      "uneven"."_best_perf_rk" as "_best_perf_rk"
  FROM
      "uneven"
  WHERE
      INVALID_REFERENCE_BUG<Missing source reference to ss.net_profit> > 0.9 * (INVALID_REFERENCE_BUG_AGG_GRAIN_MISMATCH<global (by *) aggregate local._virt_agg_avg_1147483187926681_wscope rendered in CTE yummy at keyed grain Grain<ss.item.sk>>) and "uneven"."_best_perf_rk" <= 10
  )
  SELECT
      "yummy"."_best_perf_rk" as "rk",
      "yummy"."_best_perf_pname" as "pname",
      "yummy"."_best_perf_avg_np" as "avg_np"
  FROM
      "yummy"
  ORDER BY
      "rk" asc
  LIMIT (100)
  ```
- `trilogy file write answer_2118989494.preql --run`

  ```text
  Syntax error in answer_2118989494.preql: Output column 'month_total' renames 'local.month_total' back to the name of an existing concept 'month_total' (defined at line 5) that 'local.month_total' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'month_total_out').
  ```
- `trilogy file write answer_2118989494.preql --run`

  ```text
  Syntax error in answer_2118989494.preql: ORDER BY references 'local.dev', which is not in the SELECT projection (line 24). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --local.dev order by local.dev asc`.
  ```
- `trilogy file write probe_d.preql --run-and-delete`

  ```text
  Syntax error in probe_d.preql: HAVING filters on a dimension outside the SELECT projection, but the select has no grain key to anchor a post-aggregation semijoin (line 13). Move the filter to WHERE to filter before aggregation.
  ```
- `trilogy file write answer_3544057080.preql --run`

  ```text
  trilogy error: subprocess timed out after 600s.
  ```
- `trilogy file write probe_q67d.preql --run-and-delete`

  ```text
  trilogy error: subprocess timed out after 600s.
  ```
- `trilogy file write probe_books.preql --run-and-delete`

  ```text
  Syntax error in probe_books.preql: Cannot use is not with non-null or boolean value <Filter: MagicConstants.NULL where bool(1) = True>
  ```
- `trilogy file write probe_e.preql --run-and-delete`

  ```text
  Syntax error in probe_e.preql: HAVING clause aggregate `avg(local.tot) by local.st` is not in the SELECT projection and would change the select grain if added (line 13). HAVING can only filter on off-grain or nested aggregates that are also computed in the SELECT. Fix one of: (a) add it to SELECT — prefix with `--` to keep it out of the output rows, e.g. `select ..., --avg(local.tot) by local.st`; (b) move the filter to WHERE — for an aggregate condition on a non-output grain, write the aggregate inline as `agg(x) by grain` directly in WHERE.
  ```
- `trilogy file write answer_2852230229.preql --run`

  ```text
  Syntax error in answer_2852230229.preql: Output column 'total_net_paid' renames 'local.total_net_paid' back to the name of an existing concept 'total_net_paid' (defined at line 3) that 'local.total_net_paid' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'total_net_paid_out').
  ```
- `trilogy file write answer_2852230229.preql --run`

  ```text
  Syntax error in answer_2852230229.preql: ORDER BY references 'local.sort_category', which is not in the SELECT projection (line 10). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --local.sort_category order by local.sort_category asc`.
  ```

### `join-resolution`

- `trilogy file write probe_a.preql --run-and-delete`

  ```text
  Resolution error in probe_a.preql: Could not resolve connections for query with output ['local.n_rows<Purpose.METRIC>Derivation.AGGREGATE>', 'local.n_store_rows<Purpose.METRIC>Derivation.AGGREGATE>', 'local.n_catalog_rows<Purpose.METRIC>Derivation.AGGREGATE>', 'local.ss_profit<Purpose.METRIC>Derivation.AGGREGATE>', 'local.sr_loss<Purpose.METRIC>Derivation.AGGREGATE>', 'local.cs_profit<Purpose.METRIC>Derivation.AGGREGATE>'] from current model. Group grp:aggregate:d0:∅ (Derivation.AGGREGATE) could not be built, and nothing else delivers what it owes: WHERE atoms [], readers [], outputs ['local.n_rows'].
  ```
- `trilogy file write probe_nulls.preql --run-and-delete`

  ```text
  Resolution error in probe_nulls.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 27). The requested concepts split into 2 disconnected subgraphs: {c.current_address.state, c.sk, n, no_demo}; {n2}. Are you missing a join or merge statement to relate them?
  Note: the membership predicate(s) `(c.sk) in (store_cust.cid)` span these subgraphs, but membership only filters rows on its left side, it does not join the two sides, so it cannot relate them for outputs or grouping. To combine values from both sides, author a query-scoped join or a merge on shared keys.
  ```
- `trilogy file write probe1.preql --run-and-delete`

  ```text
  Resolution error in probe1.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 3 disconnected subgraphs: {cs.sale_date.year}; {ss.sale_date.year}; {ws.sale_date.year}.
    - `ss.sale_date.year` is disconnected, did you mean `cs.ship_customer.first_sales_date.year`? (connected to the other concepts)
    - `ws.sale_date.year` is disconnected, did you mean `cs.ship_customer.first_sales_date.year`? (connected to the other concepts)
  These look like separately-imported copies of models already reachable through a connected import; chain through that path (e.g. `cs.ship_customer.first_sales_date.year`) instead of importing a second, disconnected copy.
  ```
- `trilogy file write probe_c.preql --run-and-delete`

  ```text
  Resolution error in probe_c.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {in_store_sales, inv.item.current_price, inv.item.manufacturer_id, inv.item.sk, item_code (= inv.item.id), item_desc (= inv.item.desc), item_sk (= inv.item.sk), manuf (= inv.item.manufacturer_id), price (= inv.item.current_price), qualifying_snaps}; {ss.item.sk}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file write probe_i.preql --run-and-delete`

  ```text
  Resolution error in probe_i.preql: Could not resolve connections for query with output ['local.inv_items<Purpose.METRIC>Derivation.AGGREGATE>', 'local.matched_by_membership<Purpose.METRIC>Derivation.AGGREGATE>', 'local.ss_items_direct<Purpose.METRIC>Derivation.AGGREGATE>'] from current model. Group grp:root:root:∅ (Derivation.ROOT) could not be built, and nothing else delivers what it owes: WHERE atoms [], readers ['grp:aggregate:d0:∅:input:inv.item.sk', 'grp:aggregate:d0:∅:input:ss.item.sk', 'grp:filter:d*:∅:sig:90c29f06380c'], outputs ['ss.item.sk'].
  ```
- `trilogy file write probe_c.preql --run-and-delete`

  ```text
  Resolution error in probe_c.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {dim_name (= st.name)}; {m_max, m_min, n, ss.sale_date.year, store_sk (= ss.store.sk)}.
    - `st.name` (as `dim_name`) is disconnected, did you mean `ss.store.name`? (connected to the other concepts)
  These look like separately-imported copies of models already reachable through a connected import; chain through that path (e.g. `ss.store.name`) instead of importing a second, disconnected copy.
  ```
- `trilogy file write probe_verify.preql --run-and-delete`

  ```text
  Resolution error in probe_verify.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {_catalog_only_cust_key}; {_catalog_only_item_key}. Are you missing a join or merge statement to relate them?
  ```

### `undefined-concept`

- `trilogy file write answer_2133330107.preql --run`

  ```text
  Syntax error in answer_2133330107.preql: 6 undefined concept references; fix all before re-running:
    - local.ext_sales_price (line 15, in SELECT); did you mean: ss.ext_sales_price, ss.sales_price, ss.ext_list_price?
    - item.manager_id (line 6, col 7, in WHERE); did you mean: ss.item.manager_id, item.manufacturer_id, item.brand_id, item.manufact, ss.item.manufacturer_id?
    - sale_date.year (line 7, col 7, in WHERE); did you mean: ss.sale_date.year, sale_date.month_of_year, ss.return_date.year, ss.customer.first_sales_date.year, ss.return_customer.first_sales_date.year, ss.customer.first_shipto_date.year?
    - sale_date.month_of_year (line 8, col 7, in WHERE); did you mean: ss.sale_date.month_of_year, sale_date.year, ss.return_date.month_of_year, ss.customer.first_sales_date.month_of_year, ss.return_customer.first_sales_date.month_of_year, ss.customer.first_shipto_date.month_of_year?
    - customer.current_address.zip (line 6, in WHERE); did you mean: ss.customer.current_address.zip, ss.return_customer.current_address.zip, ss.return_address.zip, ss.pos_address.zip, store.zip, ss.store.zip?
    - store.zip (line 6, in WHERE); did you mean: ss.store.zip, ss.return_store.zip, ss.pos_address.zip, customer.current_address.zip, ss.return_address.zip, ss.customer.current_address.zip?
  ```
- `trilogy file write answer_4207382245.preql --run`

  ```text
  Syntax error in answer_4207382245.preql: 14 undefined concept references; fix all before re-running:
    - cq.web_q2 (line 19, in SELECT); did you mean: cq.web_q3, cq.web_q1, cq.store_q2, county_quarter.web_q2?
    - cq.web_q1 (line 19, in SELECT); did you mean: cq.web_q3, cq.web_q2, cq.store_q1, county_quarter.web_q1?
    - cq.store_q2 (line 20, in SELECT); did you mean: cq.store_q3, cq.store_q1, cq.web_q2, county_quarter.store_q2?
    - cq.store_q1 (line 20, in SELECT); did you mean: cq.store_q3, cq.store_q2, cq.web_q1, county_quarter.store_q1?
    - cq.web_q3 (line 21, in SELECT); did you mean: cq.web_q2, cq.web_q1, cq.store_q3, county_quarter.web_q3?
    - cq.web_q2 (line 21, in SELECT); did you mean: cq.web_q3, cq.web_q1, cq.store_q2, county_quarter.web_q2?
    - cq.store_q3 (line 22, in SELECT); did you mean: cq.store_q2, cq.store_q1, cq.web_q3, county_quarter.store_q3?
    - cq.store_q2 (line 22, in SELECT); did you mean: cq.store_q3, cq.store_q1, cq.web_q2, county_quarter.store_q2?
    - cq.web_q2 (line 17, in WHERE); did you mean: cq.web_q3, cq.web_q1, cq.store_q2, county_quarter.web_q2?
    - cq.web_q1 (line 17, in WHERE); did you mean: cq.web_q3, cq.web_q2, cq.store_q1, county_quarter.web_q1?
    - cq.store_q2 (line 17, in WHERE); did you mean: cq.store_q3, cq.store_q1, cq.web_q2, county_quarter.store_q2?
    - cq.store_q1 (line 17, in WHERE); did you mean: cq.store_q3, cq.store_q2, cq.web_q1, county_quarter.store_q1?
    - cq.web_q3 (line 17, in WHERE); did you mean: cq.web_q2, cq.web_q1, cq.store_q3, county_quarter.web_q3?
    - cq.store_q3 (line 17, in WHERE); did you mean: cq.store_q2, cq.store_q1, cq.web_q3, county_quarter.store_q3?
  ```
- `trilogy file write probe_scale.preql --run-and-delete`

  ```text
  Syntax error in probe_scale.preql: Undefined concept: s.row_counter.
  ```
- `trilogy file write probe5.preql --run-and-delete`

  ```text
  Syntax error in probe5.preql: Undefined concept: sb.cust_code. Suggestions: ['cust_code', 'ss.customer.sk', 'ss.customer.id']
  ```
- `trilogy file write probe6.preql --run-and-delete`

  ```text
  Syntax error in probe6.preql: Undefined concept: STORE.
  ```

### `planner-recursion`

- `trilogy file write probe4_751385098.preql --run-and-delete`

  ```text
  Resolution error in probe4_751385098.preql: query could not be planned; this is a bug.
  ```
