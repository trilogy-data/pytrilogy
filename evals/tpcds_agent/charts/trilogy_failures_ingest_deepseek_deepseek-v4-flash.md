# Trilogy failure analysis — 20261122-145209

- Run `20261122-145208_ingest_deepseek_deepseek-v4-flash` | `deepseek/deepseek-v4-flash` | sf=1
- `trilogy` calls: 1189 | failed: 60 (5%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `undefined-concept` | 21 | 35% |
| `syntax-parse` | 16 | 27% |
| `other` | 16 | 27% |
| `join-resolution` | 5 | 8% |
| `no-output` | 1 | 2% |
| `type-error` | 1 | 2% |

## Detail

### `undefined-concept`

- `trilogy file write probe4.preql --run-and-delete`

  ```text
  Syntax error in probe4.preql: Undefined concept: customer.customer_sk. Suggestions: ['ss.customer.customer_sk', 'ss.customer.customer_id', 'ss.customer.customer_address.zip']
  ```
- `trilogy file write answer_3705756794.preql --run`

  ```text
  Syntax error in answer_3705756794.preql: Undefined concept: ss.sold_date.date. Suggestions: ['ss.promotion.end_date.date', 'ss.promotion.end_date.date_sk', 'ss.promotion.end_date.date_id', 'cs.sold_date.date', 'ws.sold_date.date', 'cs.ship_date.date']
  ```
- `trilogy file write probe1.preql --run-and-delete`

  ```text
  Syntax error in probe1.preql: 2 undefined concept references; fix all before re-running:
    - ss.item_sk (line 4, in SELECT); did you mean: ss.item.item_sk, ss.promotion.item.item_sk, ss.item.size, ss.item.units?
    - ss.item_sk (line 7, in SELECT); did you mean: ss.item.item_sk, ss.promotion.item.item_sk, ss.item.size, ss.item.units?
  ```
- `trilogy file write answer_4199102535.preql --run`

  ```text
  Syntax error in answer_4199102535.preql: 2 undefined concept references; fix all before re-running:
    - ss.sold_date.year (line 33, col 7, in WHERE); did you mean: ss.sold_date.moy, ss.date_dim.year, ss.store.date_dim.year, ws.sold_date.year, cs.sold_date.year, ws.ship_date.year?
    - ss.sold_date.moy (line 34, col 7, in WHERE); did you mean: ss.sold_date.year, ss.date_dim.moy, ss.store.date_dim.moy, ws.sold_date.moy, cs.sold_date.moy, ws.ship_date.moy?
  ```
- `trilogy file write answer_4077069387.preql --run`

  ```text
  Syntax error in answer_4077069387.preql: Undefined concept: it.total_ext_price (line 20, in SELECT). Suggestions: ['it.current_price', 'item_totals.total_ext_price', 'total_ext_sales_price', 'item_totals.current_price']
  ```
- `trilogy file write answer_219203908.preql --run`

  ```text
  Syntax error in answer_219203908.preql: 4 undefined concept references; fix all before re-running:
    - channel (line 120, col 10, in ORDER BY); did you mean: nov.channel, kept.channel?
    - brand_id (line 120, col 35, in ORDER BY); did you mean: nov.brand_id, kept.brand_id, tuples.brand_id, ss.item.brand_id, cs.item.brand_id, ws.item.brand_id?
    - class_id (line 120, col 61, in ORDER BY); did you mean: nov.class_id, kept.class_id, tuples.class_id, ss.item.class_id, cs.item.class_id, ws.item.class_id?
    - category_id (line 120, col 87, in ORDER BY); did you mean: nov.category_id, kept.category_id, tuples.category_id, ss.item.category_id, cs.item.category_id, ws.item.category_id?
  ```
- `trilogy file write answer_2133330107.preql --run`

  ```text
  Syntax error in answer_2133330107.preql: 5 undefined concept references; fix all before re-running:
    - item.manager_id (line 4, col 9, in WHERE); did you mean: ss.item.manager_id, ss.promotion.item.manager_id, item.manufact_id, item.brand_id, item.manufact?
    - date_dim.year (line 5, col 9, in WHERE); did you mean: ss.date_dim.year, ss.store.date_dim.year, date_dim.moy, ss.promotion.end_date.year, ss.promotion.start_date.year, ss.customer.first_sales_date.year?
    - date_dim.moy (line 6, col 9, in WHERE); did you mean: ss.date_dim.moy, ss.store.date_dim.moy, date_dim.year, ss.promotion.end_date.moy, ss.promotion.start_date.moy, ss.customer.first_sales_date.moy?
    - customer.customer_address.zip (line 3, in WHERE); did you mean: ss.customer.customer_address.zip, ss.customer_address.zip, store.zip, ss.store.zip, ss.customer.customer_address.city?
    - store.zip (line 3, in WHERE); did you mean: ss.store.zip, ss.customer_address.zip, customer.customer_address.zip, ss.customer.customer_address.zip, ss.store.city?
  ```
- `trilogy file write answer_4080205480.preql --run`

  ```text
  Syntax error in answer_4080205480.preql: 9 undefined concept references; fix all before re-running:
    - local.quantity (line 8, in SELECT); did you mean: ss.quantity?
    - local.list_price (line 9, in SELECT); did you mean: ss.list_price, ss.ext_list_price, avg_line_list_price?
    - local.coupon_amt (line 10, in SELECT); did you mean: ss.coupon_amt, avg_line_coupon_amt?
    - local.sales_price (line 11, in SELECT); did you mean: ss.sales_price, list_price, ss.ext_sales_price?
    - date_dim.year (line 12, col 11, in WHERE); did you mean: ss.date_dim.year, ss.store.date_dim.year, ss.promotion.end_date.year, ss.promotion.start_date.year, ss.customer.first_sales_date.year, ss.customer.last_review_date.year?
    - store.state (line 13, col 11, in WHERE); did you mean: ss.store.state, state, ss.customer_address.state, ss.customer.customer_address.state, ss.store.rec_start_date, ss.store.store_sk?
    - customer_demographics.gender (line 14, col 11, in WHERE); did you mean: ss.customer_demographics.gender, ss.customer.customer_demographics.gender, customer_demographics.education_status, customer_demographics.marital_status, ss.customer_demographics.demo_sk?
    - customer_demographics.marital_status (line 15, col 11, in WHERE); did you mean: ss.customer_demographics.marital_status, ss.customer.customer_demographics.marital_status, customer_demographics.education_status, customer_demographics.gender?
    - customer_demographics.education_status (line 16, col 11, in WHERE); did you mean: ss.customer_demographics.education_status, ss.customer.customer_demographics.education_status, customer_demographics.marital_status, customer_demographics.gender?
  ```
- `trilogy file write probe6.preql --run-and-delete`

  ```text
  Syntax error in probe6.preql: Undefined concept: cat_keys.tkt. Suggestions: ['cat_keys.item_sk', 'cat_keys.cust_sk', 'cat_keys.cs_qty', 'ret_keys.tkt']
  ```
- `trilogy file write probe9.preql --run-and-delete`

  ```text
  Syntax error in probe9.preql: Undefined concept: ss_qual.ret_keys.tkt. Suggestions: ['ss_qual.cust_sk', 'ss_qual.n_tickets', 'ss_qual.store_name', 'ret_keys.tkt', 'sr_store.tkt']
  ```
- `trilogy file write probe9.preql --run-and-delete`

  ```text
  Syntax error in probe9.preql: Undefined concept: local.grp_customer_id (line 10, col 5, in SELECT).
  ```
- `trilogy file write q3.preql --run-and-delete`

  ```text
  Syntax error in q3.preql: 2 undefined concept references; fix all before re-running:
    - item.manufact_id (line 8, col 38, in SELECT); did you mean: ss.item.manufact_id, ss.promotion.item.manufact_id, ss.item.manufact?
    - item.category (line 8, col 61, in WHERE); did you mean: ss.item.category, ss.promotion.item.category, ss.item.category_id?
  ```
- `trilogy file write answer_3560698360.preql --run`

  ```text
  Syntax error in answer_3560698360.preql: Undefined concept: ss.sold_date.year. Suggestions: ['ss.date_dim.year', 'ss.store.date_dim.year', 'ss.promotion.end_date.year', 'ws.sold_date.year', 'ws.ship_date.year', 'ws.promotion.end_date.year']
  ```
- `trilogy file write answer_145690531.preql --run`

  ```text
  Syntax error in answer_145690531.preql: Undefined concept: ss.sold_date.week_seq. Suggestions: ['ss.date_dim.week_seq', 'ss.store.date_dim.week_seq', 'ss.promotion.end_date.week_seq', 'cs.sold_date.week_seq', 'ws.sold_date.week_seq', 'cs.ship_date.week_seq']
  ```
- `trilogy file write probe_chk.preql --run-and-delete`

  ```text
  Syntax error in probe_chk.preql: Undefined concept: ss.grain_key_placeholder (line 9, in SELECT).
  ```
- `trilogy file write answer_3210116865.preql --run`

  ```text
  Syntax error in answer_3210116865.preql: Undefined concept: ss.sold_date.date. Suggestions: ['ss.promotion.end_date.date', 'ss.promotion.end_date.date_sk', 'ss.promotion.end_date.date_id', 'cs.sold_date.date', 'ws.sold_date.date', 'cs.ship_date.date']
  ```
- `trilogy file write answer_943796012.preql --run`

  ```text
  Syntax error in answer_943796012.preql: Undefined concept: cr.return_amt. Suggestions: ['cr.return_amount', 'cr.return_tax', 'cr.return_quantity', 'sr.return_amt', 'wr.return_amt']
  ```
- `trilogy file write answer_943796012.preql --run`

  ```text
  Syntax error in answer_943796012.preql: 2 undefined concept references; fix all before re-running:
    - channel (line 55, col 10, in ORDER BY); did you mean: combined.channel?
    - id (line 55, col 35, in ORDER BY); did you mean: combined.id, sr.reason.id, cr.reason.id, wr.reason.id?
  ```
- `trilogy file write probe_weeks.preql --run-and-delete`

  ```text
  Syntax error in probe_weeks.preql: Undefined concept: d.day_of_week. Suggestions: ['d.fy_week_seq', 'd.day_name']
  ```
- `trilogy file write probe1.preql --run-and-delete`

  ```text
  Syntax error in probe1.preql: Undefined concept: s_cnt. Suggestions: ['flags.s_cnt', 'flags.c_cnt']
  ```
- `trilogy file write answer_3498056762.preql --run`

  ```text
  Syntax error in answer_3498056762.preql: Undefined concept: sold_date.date. Suggestions: ['cs.sold_date.date', 'cs.ship_date.date', 'cs.promotion.end_date.date', 'cs.catalog_page.end_date.date', 'cs.promotion.start_date.date', 'cs.catalog_page.start_date.date']
  ```

### `syntax-parse`

- `trilogy file write probe_a.preql --run-and-delete`

  ```text
  refused to write 'probe_a.preql': not syntactically valid Trilogy.

  Parse error:
    --> 21:32
     |
  21 | where combined.week_seq in 5269, 5270, 5271, 5322, 5323, 5324, 5325
     |                                ^---
     |
     = expected limit, order_by, THEN_LA, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...here combined.week_seq in 5269 ??? , 5270, 5271, 5322, 5323, 5324...
  ```
- `trilogy file write answer_4199102535.preql --run`

  ```text
  refused to write 'answer_4199102535.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [231]: A `subset|union|equal join` cannot follow a trailing `where`. Put the join right after the select list, with the filter either before `select` or after the join: `where <filters> select <cols> subset join a.key = b.key` or `select <cols> subset join a.key = b.key where <filters>`. Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
   wc_customer.csk is not null  ??? subset join wc_customer.csk =
  ```
- `trilogy file write probe7.preql --run-and-delete`

  ```text
  refused to write 'probe7.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [211]: Expression in `by` clause must be wrapped in parens - write `by (expr1, expr2, ...)`. Bare identifiers (`by a, b`) work without parens, but any function call, cast, or other expression needs them.
  Location:
  ...nt <- count(ss.ticket_number) ??? by ss.item.item_sk, (substring...
  ```
- `trilogy file write probe6.preql --run-and-delete`

  ```text
  refused to write 'probe6.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...eys.cs_orders) as tot_orders  ??? from cat_keys;
  ```
- `trilogy file write q4.preql --run-and-delete`

  ```text
  refused to write 'q4.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
   ss.item.manufact_id in elec  ??? from (select item.manufact_id
  ```
- `trilogy file write answer_3809267817.preql --run`

  ```text
  refused to write 'answer_3809267817.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [231]: A `subset|union|equal join` cannot follow a trailing `where`. Put the join right after the select list, with the filter either before `select` or after the join: `where <filters> select <cols> subset join a.key = b.key` or `select <cols> subset join a.key = b.key where <filters>`. Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...and qual.cust_sk is not null  ??? subset join qual.cust_sk = ss....
  ```
- `trilogy file write p2.preql --run-and-delete`

  ```text
  refused to write 'p2.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [231]: A `subset|union|equal join` cannot follow a trailing `where`. Put the join right after the select list, with the filter either before `select` or after the join: `where <filters> select <cols> subset join a.key = b.key` or `select <cols> subset join a.key = b.key where <filters>`. Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ....warehouse.state is not null  ??? subset join cr.order_number =
  ```
- `trilogy file write answer_1809796058.preql --run`

  ```text
  refused to write 'answer_1809796058.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [231]: A `subset|union|equal join` cannot follow a trailing `where`. Put the join right after the select list, with the filter either before `select` or after the join: `where <filters> select <cols> subset join a.key = b.key` or `select <cols> subset join a.key = b.key where <filters>`. Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...sr.ticket_number is not null  ??? union join ss.ticket_number =
  ```
- `trilogy file write probe_c.preql --run-and-delete`

  ```text
  refused to write 'probe_c.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [231]: A `subset|union|equal join` cannot follow a trailing `where`. Put the join right after the select list, with the filter either before `select` or after the join: `where <filters> select <cols> subset join a.key = b.key` or `select <cols> subset join a.key = b.key where <filters>`. Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...sr.ticket_number is not null  ??? union join ss.ticket_number =
  ```
- `trilogy file write probe2.preql --run-and-delete`

  ```text
  refused to write 'probe2.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...item_qual.item_sk) as n_qual  ??? from item_qual;
  ```
- `trilogy file write probe_b.preql --run-and-delete`

  ```text
  refused to write 'probe_b.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...r, by_all.mn)) as groups_cnt  ??? from (      select          co...
  ```
- `trilogy file write probe_d.preql --run-and-delete`

  ```text
  refused to write 'probe_d.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [231]: A `subset|union|equal join` cannot follow a trailing `where`. Put the join right after the select list, with the filter either before `select` or after the join: `where <filters> select <cols> subset join a.key = b.key` or `select <cols> subset join a.key = b.key where <filters>`. Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...and sr.ticket_number is null  ??? union join ss.ticket_number =
  ```
- `trilogy file write probe_rowset.preql --run-and-delete`

  ```text
  refused to write 'probe_rowset.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [231]: A `subset|union|equal join` cannot follow a trailing `where`. Put the join right after the select list, with the filter either before `select` or after the join: `where <filters> select <cols> subset join a.key = b.key` or `select <cols> subset join a.key = b.key where <filters>`. Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...s.store.store_sk is not null  ??? subset join ss.item.item_sk =
  ```
- `trilogy file write probe_wk2.preql --run-and-delete`

  ```text
  refused to write 'probe_wk2.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
   = 5257 or d.week_seq = 5264  ??? group by d.week_seq  order by
  ```
- `trilogy file write probe4.preql --run-and-delete`

  ```text
  refused to write 'probe4.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [202]: Missing closing semicolon? Statements must be terminated with a semicolon `;`.
  Location:
  ...n_none_placeholder) as dummy  ???
  ```
- `trilogy file write probe_check.preql --run-and-delete`

  ```text
  refused to write 'probe_check.preql': not syntactically valid Trilogy.

  Parse error:
   --> 5:22
    |
  5 | with store_combos as (
    |                      ^---
    |
    = expected select_statement, tvf_union_invocation, tvf_except_invocation, tvf_intersect_invocation, tvf_rollup_invocation, tvf_cube_invocation, or tvf_grouping_sets_invocation
  Location:
  ...s ws;    with store_combos as ??? (      where ss.date_dim.year
  ```

### `other`

- `trilogy file write probe2.preql --run-and-delete`

  ```text
  Syntax error in probe2.preql: HAVING filters on a dimension outside the SELECT projection, but the select has no grain key to anchor a post-aggregation semijoin (line 26). Move the filter to WHERE to filter before aggregation.
  ```
- `trilogy file write probe2.preql --run-and-delete`

  ```text
  Unexpected error in probe2.preql: (_duckdb.BinderException) Binder Error: Referenced table "sedate" not found!
  Candidate tables: "waggish"

  LINE 184: ...."_virt_filter_cust_sk_7857131331149401" is not distinct from "sedate"."store_agg_cust_sk" and puffy."_virt_filter_v02_66...
                                                                             ^
  [SQL:
  WITH
  yummy as (
  SELECT
      "ws_web_sales"."ws_bill_customer_sk" as "web_agg_cust_sk",
      sum(CASE WHEN "ws_sold_date_date_dim"."d_year" = 2001 THEN ( ( "ws_web_sales"."ws_ext_list_price" - "ws_web_sales"."ws_ext_wholesale_cost" ) - "ws_web_sales"."ws_ext_discount_amt" ) + "ws_web_sales"."ws_ext_sales_price" ELSE NULL END) as "web_agg_v01",
      sum(CASE WHEN "ws_sold_date_date_dim"."d_year" = 2002 THEN ( ( "ws_web_sales"."ws_ext_list_price" - "ws_web_sales"."ws_ext_wholesale_cost" ) - "ws_web_sales"."ws_ext_discount_amt" ) + "ws_web_sales"."ws_ext_sales_price" ELSE NULL END) as "web_agg_v02"
  FROM
      "date_dim" as "ws_sold_date_date_dim"
      LEFT OUTER JOIN "web_sales" as "ws_web_sales" on "ws_sold_date_date_dim"."d_date_sk" = "ws_web_sales"."ws_sold_date_sk"
  WHERE
      ("ws_sold_date_date_dim"."d_year" is not null and "ws_sold_date_date_dim"."d_year" in (2001,2002))

  GROUP BY
      1),
  wakeful as (
  SELECT
      "cs_catalog_sales"."cs_bill_customer_sk" as "catalog_agg_cust_sk",
      sum(CASE WHEN "cs_sold_date_date_dim"."d_year" = 2001 THEN ( ( "cs_catalog_sales"."cs_ext_list_price" - "cs_catalog_sales"."cs_ext_wholesale_cost" ) - "cs_catalog_sales"."cs_ext_discount_amt" ) + "cs_catalog_sales"."cs_ext_sales_price" ELSE NULL END) as "catalog_agg_v01",
      sum(CASE WHEN "cs_sold_date_date_dim"."d_year" = 2002 THEN ( ( "cs_catalog_sales"."cs_ext_list_price" - "cs_catalog_sales"."cs_ext_wholesale_cost" ) - "cs_catalog_sales"."cs_ext_discount_amt" ) + "cs_catalog_sales"."cs_ext_sales_price" ELSE NULL END) as "catalog_agg_v02"
  FROM
      "date_dim" as "cs_sold_date_date_dim"
      LEFT OUTER JOIN "catalog_sales" as "cs_catalog_sales" on "cs_sold_date_date_dim"."d_date_sk" = "cs_catalog_sales"."cs_sold_date_sk"
  WHERE
      ("cs_sold_date_date_dim"."d_year" is not null and "cs_sold_date_date_dim"."d_year" in (2001,2002))

  GROUP BY
      1),
  sparkling as (
  SELECT
      "wakeful"."catalog_agg_cust_sk" as "catalog_agg_cust_sk",
      "wakeful"."catalog_agg_v01" as "catalog_agg_v01",
      "wakeful"."catalog_agg_v02" as "catalog_agg_v02",
      "yummy"."web_agg_cust_sk" as "web_agg_cust_sk",
      "yummy"."web_agg_v01" as "web_agg_v01",
      "yummy"."web_agg_v02" as "web_agg_v02"
  FROM
      "wakeful"
      INNER JOIN "yummy" on "wakeful"."catalog_agg_cust_sk" is not distinct from "yummy"."web_agg_cust_sk"),
  abhorrent as (
  SELECT
      "sparkling"."catalog_agg_cust_sk" as "catalog_agg_cust_sk",
      "sparkling"."catalog_agg_v01" as "catalog_agg_v01",
      "sparkling"."catalog_agg_v02" as "c02",
      "sparkling"."catalog_agg_v02" as "catalog_agg_v02",
      "sparkling"."web_agg_cust_sk" as "store_agg_cust_sk",
      "sparkling"."web_agg_cust_sk" as "web_agg_cust_sk",
      "sparkling"."web_agg_v01" as "web_agg_v01",
      "sparkling"."web_agg_v02" as "w02",
      "sparkling"."web_agg_v02" as "web_agg_v02"
  FROM
      "sparkling"),
  sedate as (
  SELECT
      "abhorrent"."catalog_agg_v02" as "_virt_filter_v02_2024169323765064",
      "abhorrent"."store_agg_cust_sk" as "_virt_filter_cust_sk_543728698283435",
      "abhorrent"."store_agg_cust_sk" as "store_agg_cust_sk",
      "abhorrent"."web_agg_cust_sk" as "web_agg_cust_sk",
      "abhorrent"."web_agg_v02" as "_virt_filter_v02_3414770203104890"
  FROM
      "abhorrent"
  WHERE
      "abhorrent"."c02" / nullif("abhorrent"."catalog_agg_v01", 0) > "abhorrent"."w02" / nullif("abhorrent"."web_agg_v01", 0)
  ),
  macho as (
  SELECT
      "ss_store_sales"."ss_customer_sk" as "store_agg_cust_sk",
      sum(CASE WHEN "ss_date_dim_date_dim"."d_year" = 2001 THEN ( ( "ss_store_sales"."ss_ext_list_price" - "ss_store_sales"."ss_ext_wholesale_cost" ) - "ss_store_sales"."ss_ext_discount_amt" ) + "ss_store_sales"."ss_ext_sales_price" ELSE NULL END) as "store_agg_v01",
      sum(CASE WHEN "ss_date_dim_date_dim"."d_year" = 2002 THEN ( ( "ss_store_sales"."ss_ext_list_price" - "ss_store_sales"."ss_ext_wholesale_cost" ) - "ss_store_sales"."ss_ext_discount_amt" ) + "ss_store_sales"."ss_ext_sales_price" ELSE NULL END) as "store_agg_v02"
  FROM
      "date_dim" as "ss_date_dim_date_dim"
      LEFT OUTER JOIN "store_sales" as "ss_store_sales" on "ss_date_dim_date_dim"."d_date_sk" = "ss_store_sales"."ss_sold_date_sk"
  WHERE
      ("ss_date_dim_date_dim"."d_year" is not null and "ss_date_dim_date_dim"."d_year" in (2001,2002))

  GROUP BY
      1),
  puzzled as (
  SELECT
      "macho"."store_agg_v01" as "store_agg_v01",
      "macho"."store_agg_v02" as "store_agg_v02",
      "wakeful"."catalog_agg_v01" as "catalog_agg_v01",
      "wakeful"."catalog_agg_v02" as "catalog_agg_v02",
      "yummy"."web_agg_v01" as "web_agg_v01",
      "yummy"."web_agg_v02" as "web_agg_v02",
      coalesce("macho"."store_agg_cust_sk","wakeful"."catalog_agg_cust_sk","yummy"."web_agg_cust_sk") as "catalog_agg_cust_sk",
      coalesce("macho"."store_agg_cust_sk","wakeful"."catalog_agg_cust_sk","yummy"."web_agg_cust_sk") as "store_agg_cust_sk",
      coalesce("macho"."store_agg_cust_sk","wakeful"."catalog_agg_cust_sk","yummy"."web_agg_cust_sk") as "web_agg_cust_sk"
  FROM
      "wakeful"
      INNER JOIN "yummy" on "wakeful"."catalog_agg_cust_sk" is not distinct from "yummy"."web_agg_cust_sk"
      LEFT OUTER JOIN "macho" on "wakeful"."catalog_agg_cust_sk" is not distinct from "macho"."store_agg_cust_sk"),
  hard as (
  SELECT
      "puzzled"."catalog_agg_v02" as "_virt_filter_v02_3600114377076422",
      "puzzled"."store_agg_cust_sk" as "_virt_filter_cust_sk_3323163999573781",
      "puzzled"."web_agg_v02" as "_virt_filter_v02_889703139307801"
  FROM
      "puzzled"
  WHERE
      "puzzled"."catalog_agg_v01" > 0
  ),
  puffy as (
  SELECT
      "puzzled"."catalog_agg_v02" as "_virt_filter_v02_9517016880004718",
      "puzzled"."store_agg_cust_sk" as "_virt_filter_cust_sk_7857131331149401",
      "puzzled"."web_agg_v02" as "_virt_filter_v02_6684305708213334"
  FROM
      "puzzled"
  WHERE
      "puzzled"."store_agg_v01" > 0
  ),
  rambunctious as (
  SELECT
      "puzzled"."catalog_agg_v02" as "_virt_filter_v02_3820539518660130",
      "puzzled"."store_agg_cust_sk" as "_virt_filter_cust_sk_9941685250820771",
      "puzzled"."web_agg_v02" as "_virt_filter_v02_1409703683511159"
  FROM
      "puzzled"
  WHERE
      "puzzled"."web_agg_v01" > 0
  ),
  busy as (
  SELECT
      "macho"."store_agg_cust_sk" as "store_agg_cust_sk",
      "macho"."store_agg_cust_sk" as "web_agg_cust_sk",
      "macho"."store_agg_v01" as "store_agg_v01",
      "macho"."store_agg_v02" as "store_agg_v02",
      "yummy"."web_agg_v02" as "web_agg_v02"
  FROM
      "yummy"
      INNER JOIN "macho" on "yummy"."web_agg_cust_sk" is not distinct from "macho"."store_agg_cust_sk"),
  charming as MATERIALIZED (
  SELECT
      "busy"."store_agg_cust_sk" as "store_agg_cust_sk",
      "busy"."store_agg_v01" as "store_agg_v01",
      "busy"."store_agg_v02" as "s02",
      "busy"."web_agg_cust_sk" as "web_agg_cust_sk",
      "busy"."web_agg_v02" as "web_agg_v02"
  FROM
      "busy"),
  protective as (
  SELECT
      "abhorrent"."c02" as "c02",
      "abhorrent"."catalog_agg_cust_sk" as "catalog_agg_cust_sk",
      "abhorrent"."catalog_agg_v02" as "catalog_agg_v02",
      "abhorrent"."store_agg_cust_sk" as "store_agg_cust_sk",
      "abhorrent"."web_agg_cust_sk" as "web_agg_cust_sk",
      "abhorrent"."web_agg_v02" as "web_agg_v02"
  FROM
      "abhorrent"
      INNER JOIN "charming" on "abhorrent"."web_agg_cust_sk" is not distinct from "charming"."store_agg_cust_sk" AND "abhorrent"."web_agg_v02" is not distinct from "charming"."web_agg_v02"
  WHERE
      "abhorrent"."c02" / nullif("abhorrent"."catalog_agg_v01", 0) > "charming"."s02" / nullif("charming"."store_agg_v01", 0)
  ),
  premium as (
  SELECT
      "protective"."catalog_agg_v02" as "_virt_filter_v02_3528037977026638",
      "protective"."store_agg_cust_sk" as "_virt_filter_cust_sk_4620714957581234",
      "protective"."web_agg_v02" as "_virt_filter_v02_8206546550688996"
  FROM
      "protective"),
  waggish as (
  SELECT
      "puzzled"."catalog_agg_v02" as "c02",
      "puzzled"."catalog_agg_v02" as "catalog_agg_v02",
      "puzzled"."store_agg_cust_sk" as "cust_sk",
      "puzzled"."store_agg_v02" as "s02",
      "puzzled"."web_agg_v02" as "w02",
      "puzzled"."web_agg_v02" as "web_agg_v02"
  FROM
      "puzzled"
  WHERE
      exists (select 1 from puffy where puffy."_virt_filter_v02_9517016880004718" is not distinct from "puzzled"."catalog_agg_v02" and puffy."_virt_filter_cust_sk_7857131331149401" is not distinct from "puzzled"."store_agg_cust_sk" and puffy."_virt_filter_v02_6684305708213334" is not distinct from "puzzled"."web_agg_v02") and exists (select 1 from hard where hard."_virt_filter_v02_3600114377076422" is not distinct from "puzzled"."catalog_agg_v02" and hard."_virt_filter_cust_sk_3323163999573781" is not distinct from "puzzled"."store_agg_cust_sk" and hard."_virt_filter_v02_889703139307801" is not distinct from "puzzled"."web_agg_v02") and exists (select 1 from rambunctious where rambunctious."_virt_filter_v02_3820539518660130" is not distinct from "puzzled"."catalog_agg_v02" and rambunctious."_virt_filter_cust_sk_9941685250820771" is not distinct from "puzzled"."store_agg_cust_sk" and rambunctious."_virt_filter_v02_1409703683511159" is not distinct from "puzzled"."web_agg_v02") and exists (select 1 from premium where premium."_virt_filter_v02_3528037977026638" is not distinct from "puzzled"."catalog_agg_v02" and premium."_virt_filter_cust_sk_4620714957581234" is not distinct from "puzzled"."store_agg_cust_sk" and premium."_virt_filter_v02_8206546550688996" is not distinct from "puzzled"."web_agg_v02") and exists (select 1 from sedate where sedate."_virt_filter_v02_2024169323765064" is not distinct from "puzzled"."catalog_agg_v02" and sedate."_virt_filter_cust_sk_543728698283435" is not distinct from "puzzled"."store_agg_cust_sk" and sedate."_virt_filter_v02_3414770203104890" is not distinct from "puzzled"."web_agg_v02") and (coalesce("puzzled"."store_agg_v02" > 0,False) = False or coalesce("puzzled"."catalog_agg_v02" > 0,False) = False or coalesce("puzzled"."web_agg_v02" > 0,False) = False)
  )
  SELECT
      "waggish"."cust_sk" as "cust_sk",
      "waggish"."s02" as "s02",
      "waggish"."c02" as "c02",
      "waggish"."w02" as "w02"
  FROM
      "waggish"
  WHERE
      exists (select 1 from puffy where puffy."_virt_filter_v02_9517016880004718" is not distinct from "waggish"."catalog_agg_v02" and puffy."_virt_filter_cust_sk_7857131331149401" is not distinct from "sedate"."store_agg_cust_sk" and puffy."_virt_filter_v02_6684305708213334" is not distinct from "waggish"."web_agg_v02") and exists (select 1 from hard where hard."_virt_filter_v02_3600114377076422" is not distinct from "waggish"."catalog_agg_v02" and hard."_virt_filter_cust_sk_3323163999573781" is not distinct from "sedate"."store_agg_cust_sk" and hard."_virt_filter_v02_889703139307801" is not distinct from "waggish"."web_agg_v02") and exists (select 1 from rambunctious where rambunctious."_virt_filter_v02_3820539518660130" is not distinct from "waggish"."catalog_agg_v02" and rambunctious."_virt_filter_cust_sk_9941685250820771" is not distinct from "sedate"."store_agg_cust_sk" and rambunctious."_virt_filter_v02_1409703683511159" is not distinct from "waggish"."web_agg_v02") and exists (select 1 from premium where premium."_virt_filter_v02_3528037977026638" is not distinct from "waggish"."catalog_agg_v02" and premium."_virt_filter_cust_sk_4620714957581234" is not distinct from "sedate"."store_agg_cust_sk" and premium."_virt_filter_v02_8206546550688996" is not distinct from "waggish"."web_agg_v02") and exists (select 1 from sedate where sedate."_virt_filter_v02_2024169323765064" is not distinct from "waggish"."catalog_agg_v02" and sedate."_virt_filter_cust_sk_543728698283435" is not distinct from "sedate"."store_agg_cust_sk" and sedate."_virt_filter_v02_3414770203104890" is not distinct from "waggish"."web_agg_v02")

  ORDER BY
      "waggish"."cust_sk" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy file write probeA.preql --run-and-delete`

  ```text
  Syntax error in probeA.preql: Output column 'n_items' renames 'local.n_items' back to the name of an existing concept 'n_items' (defined at line 5) that 'local.n_items' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'n_items_out').
  ```
- `trilogy file write probe1.preql --run-and-delete`

  ```text
  Syntax error in probe1.preql: HAVING filters on a dimension outside the SELECT projection, but the select has no grain key to anchor a post-aggregation semijoin (line 21). Move the filter to WHERE to filter before aggregation.
  ```
- `trilogy file write probe5.preql --run-and-delete`

  ```text
  Syntax error in probe5.preql: Output column 'full_avg' renames 'local.full_avg' back to the name of an existing concept 'full_avg' (defined at line 4) that 'local.full_avg' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'full_avg_out').
  ```
- `trilogy file write probe_e.preql --run-and-delete`

  ```text
  Syntax error in probe_e.preql: union arm 0 projects 2 column(s) but the output signature declares 3. Each arm must project exactly one column per output item, in order.
  ```
- `trilogy file write probe_verify.preql --run-and-delete`

  ```text
  Resolution error in probe_verify.preql: Planner emitted a keyless join between row-bearing sources that share a join axis or one source relation: (ss.customer.customer_at_ss_customer_customer_sk_join_ss.date_dim.date_dim_at_ss_date_dim_date_sk_join_ss.store_sales_at_ss_item_item_sk_ss_ticket_number_grouped_by_ss.customer.customer_sk_ss.date_dim.date_sk_at_ss_customer_customer_sk_ss_date_dim_date_sk)_at_ss_customer_customer_sk_ss_date_dim_date_sk_filtered_by_4113506896670384_at_ss_customer_customer_sk_ss_date_dim_date_sk_grouped_by_local.___all3_arm_0_dt_ss.date_dim.date_sk_at_ss_date_dim_date_sk onto (ss.customer.customer_at_ss_customer_customer_sk_join_ss.date_dim.date_dim_at_ss_date_dim_date_sk_join_ss.store_sales_at_ss_item_item_sk_ss_ticket_number_grouped_by_ss.customer.customer_sk_ss.date_dim.date_sk_at_ss_customer_customer_sk_ss_date_dim_date_sk)_at_ss_customer_customer_sk_ss_date_dim_date_sk_filtered_by_4113506896670384_at_ss_customer_customer_sk_ss_date_dim_date_sk_grouped_by_local.___all3_arm_0_chan_local.___all3_arm_0_fn_local.___all3_arm_0_ln_local.__all3_arm_0__arm_ss.customer.customer_sk_at_ss_customer_customer_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_top100.preql --run-and-delete`

  ```text
  Syntax error in probe_top100.preql: ORDER BY references 'stats.month_total', which is not in the SELECT projection (line 28). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --stats.month_total order by stats.month_total asc`.
  ```
- `trilogy file write answer_1484301313.preql --run`

  ```text
  Syntax error in answer_1484301313.preql: Impossible comparison in ref:ss.promotion.channel_email = Y: 'Y' can never match a declared value of enum<'N'> — fix the constant, or update the enum declaration if the domain is stale
  ```
- `trilogy file write probe4.preql --run-and-delete`

  ```text
  trilogy error: subprocess timed out after 600s.
  ```
- `trilogy file write answer_2874833976.preql --run`

  ```text
  Unexpected error in answer_2874833976.preql: (_duckdb.BinderException) Binder Error: GROUPING child "ss_store_county" must be a grouping column

  LINE 67:     grouping("juicy"."ss_store_county") as "g_county",
               ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "ss_store_store"."s_state" as "ss_store_state",
      sum("ss_store_sales"."ss_net_profit") as "_virt_agg_sum_2856259540536641"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "date_dim" as "ss_date_dim_date_dim" on "ss_store_sales"."ss_sold_date_sk" = "ss_date_dim_date_dim"."d_date_sk"
      INNER JOIN "store" as "ss_store_store" on "ss_store_sales"."ss_store_sk" = "ss_store_store"."s_store_sk"
  WHERE
      year("ss_date_dim_date_dim"."d_date") = 2000

  GROUP BY
      1),
  cooperative as (
  SELECT
      "cheerful"."ss_store_state" as "ss_store_state",
      rank() over (order by "cheerful"."_virt_agg_sum_2856259540536641" desc ) as "_state_profit_rnk"
  FROM
      "cheerful"),
  questionable as (
  SELECT
      "cooperative"."_state_profit_rnk" as "state_profit_rnk",
      "cooperative"."ss_store_state" as "state_profit_state"
  FROM
      "cooperative"),
  uneven as (
  SELECT
      "questionable"."state_profit_state" as "top_states"
  FROM
      "questionable"
  WHERE
      "questionable"."state_profit_rnk" <= 5
  ),
  juicy as (
  SELECT
      "ss_store_sales"."ss_net_profit" as "ss_net_profit",
      "ss_store_sales"."ss_store_sk" as "ss_store_store_sk",
      "ss_store_store"."s_county" as "ss_store_county",
      "ss_store_store"."s_county" as "store_county",
      "ss_store_store"."s_state" as "ss_store_state",
      "ss_store_store"."s_state" as "store_state"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "date_dim" as "ss_date_dim_date_dim" on "ss_store_sales"."ss_sold_date_sk" = "ss_date_dim_date_dim"."d_date_sk"
      INNER JOIN "store" as "ss_store_store" on "ss_store_sales"."ss_store_sk" = "ss_store_store"."s_store_sk"
  WHERE
      year("ss_date_dim_date_dim"."d_date") = 2000 and exists (select 1 from uneven where uneven."top_states" is not distinct from "ss_store_store"."s_state")
  ),
  young as (
  SELECT
      "juicy"."ss_store_county" as "ss_store_county",
      "juicy"."ss_store_county" as "store_county",
      "juicy"."ss_store_state" as "ss_store_state",
      "juicy"."ss_store_state" as "store_state"
  FROM
      "juicy"
  GROUP BY
      1,
      3,
      "juicy"."ss_store_store_sk"),
  concerned as (
  SELECT
      "juicy"."store_county" as "store_county",
      "juicy"."store_state" as "store_state",
      grouping("juicy"."ss_store_county") as "g_county",
      grouping("juicy"."ss_store_state") as "g_state",
      grouping("juicy"."store_county") as "_virt_agg_grouping_5529498757124878",
      grouping("juicy"."store_state") as "_virt_agg_grouping_8983744956360011",
      sum("juicy"."ss_net_profit") as "total"
  FROM
      "juicy"
  GROUP BY
      ROLLUP (2, 1)
  HAVING
      count(*) > 0
  ),
  abhorrent as (
  SELECT
      "concerned"."_virt_agg_grouping_5529498757124878" as "_virt_agg_grouping_5529498757124878",
      "concerned"."_virt_agg_grouping_8983744956360011" as "_virt_agg_grouping_8983744956360011",
      "concerned"."g_state" + "concerned"."g_county" as "hlevel",
      "concerned"."store_county" as "store_county",
      "concerned"."store_state" as "store_state",
      "concerned"."total" as "total",
      "young"."ss_store_county" as "ss_store_county",
      "young"."ss_store_state" as "ss_store_state",
      CASE
  	WHEN "concerned"."g_county" = 0 THEN "young"."ss_store_state"
  	ELSE null
  	END as "parent"
  FROM
      "young"
      INNER JOIN "concerned" on "young"."ss_store_county" = "concerned"."store_county" AND "young"."ss_store_state" = "concerned"."store_state"),
  late as (
  SELECT
      "abhorrent"."hlevel" as "hlevel",
      "abhorrent"."parent" as "parent",
      "abhorrent"."ss_store_county" as "ss_store_county",
      "abhorrent"."ss_store_state" as "ss_store_state",
      "abhorrent"."total" as "total"
  FROM
      "abhorrent"
  GROUP BY
      1,
      2,
      3,
      4,
      5,
      "abhorrent"."_virt_agg_grouping_5529498757124878",
      "abhorrent"."_virt_agg_grouping_8983744956360011"),
  macho as (
  SELECT
      "late"."hlevel" as "hlevel",
      "late"."ss_store_county" as "store_county",
      "late"."ss_store_state" as "store_state",
      "late"."total" as "total",
      rank() over (partition by "late"."hlevel","late"."parent" order by "late"."total" desc ) as "rnk"
  FROM
      "late")
  SELECT
      "macho"."store_state" as "store_state",
      "macho"."store_county" as "store_county",
      "macho"."total" as "total",
      "macho"."hlevel" as "hlevel",
      "macho"."rnk" as "rnk"
  FROM
      "macho"
  ORDER BY
      "macho"."hlevel" desc,
      "macho"."store_state" asc nulls first,
      "macho"."rnk" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy file write answer_1772060640.preql --run`

  ```text
  Syntax error in answer_1772060640.preql: Impossible comparison in SubselectComparison(left=ref:ss.store.county, right=('Orange County', 'Bronx County', 'Franklin Parish', 'Williamson County'), operator=<ComparisonOperator.IN: 'in'>): 'Orange County' can never match a declared value of enum<'Williamson County'> — fix the constant, or update the enum declaration if the domain is stale
  ```
- `trilogy file write answer_1772060640.preql --run`

  ```text
  Syntax error in answer_1772060640.preql: Impossible comparison in SubselectComparison(left=ref:ss.store.county, right=('Orange County', 'Bronx County', 'Franklin Parish', 'Williamson County'), operator=<ComparisonOperator.IN: 'in'>): 'Orange County' can never match a declared value of enum<'Williamson County'> — fix the constant, or update the enum declaration if the domain is stale
  ```
- `trilogy file write answer_943796012.preql --run`

  ```text
  Resolution error in answer_943796012.preql: Planner emitted a keyless join between row-bearing sources that share a join axis or one source relation: (ws.bill_customer.customer_at_ws_bill_customer_customer_sk_join_ws.bill_customer.household_demographics.household_demographics_at_ws_bill_customer_household_demographics_demo_sk_join_ws.bill_hdemo.household_demographics_at_ws_bill_hdemo_demo_sk_join_ws.item.item_at_wret_item_sk_grouped_by_ws.item.current_price_ws.item.item_sk_at_ws_item_item_sk_join_ws.promotion.promotion_at_ws_promotion_promo_sk_join_ws.ship_customer.customer_at_ws_ship_customer_customer_sk_join_ws.ship_customer.household_demographics.household_demographics_at_ws_ship_customer_household_demographics_demo_sk_join_ws.ship_hdemo.household_demographics_at_ws_ship_hdemo_demo_sk_join_ws.sold_date.date_dim_at_ws_sold_date_date_sk_join_ws.web_page.catalog_page.catalog_page_at_ws_web_page_catalog_page_catalog_page_sk_join_ws.web_page.customer.customer_at_ws_web_page_customer_customer_sk_join_ws.web_page.customer.household_demographics.household_demographics_at_ws_web_page_customer_household_demographics_demo_sk_join_ws.web_page.web_page_at_ws_web_page_web_page_sk_join_ws.web_sales_at_wret_item_sk_wret_order_number_grouped_by_ws.bill_customer.customer_sk_ws.bill_hdemo.demo_sk_ws.ext_sales_price_ws.item.item_sk_ws.net_profit_ws.order_number_ws.promotion.promo_sk_ws.ship_customer.customer_sk_ws.ship_hdemo.demo_sk_ws.sold_date.date_sk_ws.web_page.web_page_sk_ws.web_site.site_sk_at_ws_item_item_sk_ws_order_number_join_ws.web_site.web_site_at_ws_web_site_site_sk)_at_ws_bill_customer_customer_sk_ws_bill_hdemo_demo_sk_ws_item_item_sk_ws_order_number_ws_promotion_promo_sk_ws_ship_customer_customer_sk_ws_ship_hdemo_demo_sk_ws_sold_date_date_sk_ws_web_page_catalog_page_catalog_page_sk_ws_web_page_web_page_sk_ws_web_site_site_sk_filtered_by_7226545979919477_preserving_6409543489996546_grouped_by_local.___combined_arm_2_id_ws.web_site.site_sk_at_ws_web_site_site_sk onto (wr.item.item_at_wr_item_item_sk_at_wr_item_item_sk_join_wr.web_returns_at_wr_item_item_sk_wr_order_number_at_wr_item_item_sk_wr_order_number_at_wr_item_item_sk_wr_order_number)_at_wr_item_item_sk_wr_order_number_preserving_2664759800376642_grouped_by_local._wret_item_sk_local._wret_order_number_at_local__wret_item_sk_local__wret_order_number_at_wret_item_sk_wret_order_number. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_web.preql --run-and-delete`

  ```text
  Resolution error in probe_web.preql: Planner emitted a keyless join between row-bearing sources that share a join axis or one source relation: (ws.bill_customer.customer_at_ws_bill_customer_customer_sk_join_ws.bill_customer.household_demographics.household_demographics_at_ws_bill_customer_household_demographics_demo_sk_join_ws.bill_hdemo.household_demographics_at_ws_bill_hdemo_demo_sk_join_ws.item.item_at_wret_item_sk_grouped_by_ws.item.current_price_ws.item.item_sk_at_ws_item_item_sk_join_ws.promotion.promotion_at_ws_promotion_promo_sk_join_ws.ship_customer.customer_at_ws_ship_customer_customer_sk_join_ws.ship_customer.household_demographics.household_demographics_at_ws_ship_customer_household_demographics_demo_sk_join_ws.ship_hdemo.household_demographics_at_ws_ship_hdemo_demo_sk_join_ws.sold_date.date_dim_at_ws_sold_date_date_sk_join_ws.web_page.catalog_page.catalog_page_at_ws_web_page_catalog_page_catalog_page_sk_join_ws.web_page.customer.customer_at_ws_web_page_customer_customer_sk_join_ws.web_page.customer.household_demographics.household_demographics_at_ws_web_page_customer_household_demographics_demo_sk_join_ws.web_page.web_page_at_ws_web_page_web_page_sk_join_ws.web_sales_at_wret_item_sk_wret_order_number_grouped_by_ws.bill_customer.customer_sk_ws.bill_hdemo.demo_sk_ws.ext_sales_price_ws.item.item_sk_ws.net_profit_ws.order_number_ws.promotion.promo_sk_ws.ship_customer.customer_sk_ws.ship_hdemo.demo_sk_ws.sold_date.date_sk_ws.web_page.web_page_sk_ws.web_site.site_sk_at_ws_item_item_sk_ws_order_number_join_ws.web_site.web_site_at_ws_web_site_site_sk)_at_ws_bill_customer_customer_sk_ws_bill_hdemo_demo_sk_ws_item_item_sk_ws_order_number_ws_promotion_promo_sk_ws_ship_customer_customer_sk_ws_ship_hdemo_demo_sk_ws_sold_date_date_sk_ws_web_page_catalog_page_catalog_page_sk_ws_web_page_web_page_sk_ws_web_site_site_sk_filtered_by_7226545979919477_preserving_6409543489996546_grouped_by_local.id_ws.web_site.site_sk_at_ws_web_site_site_sk onto (wr.item.item_at_wr_item_item_sk_at_wr_item_item_sk_join_wr.web_returns_at_wr_item_item_sk_wr_order_number_at_wr_item_item_sk_wr_order_number_at_wr_item_item_sk_wr_order_number)_at_wr_item_item_sk_wr_order_number_preserving_2664759800376642_grouped_by_local._wret_item_sk_local._wret_order_number_at_local__wret_item_sk_local__wret_order_number_at_wret_item_sk_wret_order_number. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write answer_2374450308.preql --run`

  ```text
  Syntax error in answer_2374450308.preql: Cannot compare DATE (ref:inv.date_dim.date) and STRING (2000-07-24) of different types with operator <= in ref:inv.date_dim.date <= 2000-07-24
  ```

### `join-resolution`

- `trilogy file write probe_a.preql --run-and-delete --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,1842…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Resolution error in probe_a.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 15). The requested concepts split into 2 disconnected subgraphs: {net_profit_1998_q2_all}; {store_name (= s.store_name), zip (= s.zip)}.
    - `s.store_name` (as `store_name`) is disconnected, did you mean `ss.store.store_name`? (connected to the other concepts)
    - `s.zip` (as `zip`) is disconnected, did you mean `ss.store.zip`? (connected to the other concepts)
  These look like separately-imported copies of models already reachable through a connected import; chain through that path (e.g. `ss.store.store_name`) instead of importing a second, disconnected copy.
  ```
- `trilogy file write probe_rowset.preql --run-and-delete`

  ```text
  Resolution error in probe_rowset.preql: Query is unresolvable: no complete sources found for output concepts {'local.lines'}. These concepts could only be resolved from partial sources.
  ```
- `trilogy file write probe1.preql --run-and-delete`

  ```text
  Resolution error in probe1.preql: Could not resolve connections for query with output ['local.price_manuf_items<Purpose.METRIC>Derivation.AGGREGATE>', 'local.with_inv<Purpose.METRIC>Derivation.AGGREGATE>', 'local.with_sales<Purpose.METRIC>Derivation.AGGREGATE>'] from current model. Group grp:root:root:∅ (Derivation.ROOT) could not be built, and nothing else delivers what it owes: WHERE atoms ['i.current_price >= 62', 'i.current_price <= 92', '(i.manufact_id = 129 or i.manufact_id = 270 or i.manufact_id = 821 or i.manufact_id = 423)'], readers ['grp:aggregate:d0:inv.item.item_sk:input:inv.item.item_sk|inv.warehouse.warehouse_sk', 'grp:aggregate:d0:ss.item.item_sk:input:ss.item.item_sk|ss.ticket_number', 'grp:aggregate:d0:∅:input:i.item_sk', 'grp:filter:d*:∅:existence:local.inv_hits', 'grp:filter:d*:∅:existence:local.sold_items', 'grp:filter:d*:∅:sig:9b966de0e507', 'grp:filter:d*:∅:sig:a9cf02ac4cc5'], outputs ['i.item_sk', 'inv.date_dim.date', 'inv.item.item_sk', 'inv.quantity_on_hand', 'inv.warehouse.warehouse_sk', 'ss.item.item_sk', 'ss.ticket_number'].
  ```
- `trilogy file write probe1.preql --run-and-delete`

  ```text
  Resolution error in probe1.preql: Could not resolve connections for query with output ['local.manuf<Purpose.PROPERTY>Derivation.BASIC>', 'local.n_price<Purpose.METRIC>Derivation.AGGREGATE>', 'local.n_inv<Purpose.METRIC>Derivation.AGGREGATE>', 'local.n_sales<Purpose.METRIC>Derivation.AGGREGATE>'] from current model. Group grp:root:root:∅ (Derivation.ROOT) could not be built, and nothing else delivers what it owes: WHERE atoms ['i.current_price >= 62', 'i.current_price <= 92', '(i.manufact_id = 129 or i.manufact_id = 270 or i.manufact_id = 821 or i.manufact_id = 423)'], readers ['grp:aggregate:d0:i.manufact_id:input:i.item_sk', 'grp:aggregate:d0:inv.item.item_sk:input:inv.item.item_sk|inv.warehouse.warehouse_sk', 'grp:aggregate:d0:ss.item.item_sk:input:ss.item.item_sk|ss.ticket_number', 'grp:filter:d*:i.manufact_id:existence:local.inv_hits', 'grp:filter:d*:i.manufact_id:existence:local.sold_items', 'grp:filter:d*:i.manufact_id:sig:9b966de0e507', 'grp:filter:d*:i.manufact_id:sig:a9cf02ac4cc5'], outputs ['i.item_sk', 'i.manufact_id', 'inv.date_dim.date', 'inv.item.item_sk', 'inv.quantity_on_hand', 'inv.warehouse.warehouse_sk', 'ss.item.item_sk', 'ss.ticket_number'].
  ```
- `trilogy file write probeC.preql --run-and-delete`

  ```text
  Resolution error in probeC.preql: Could not resolve connections for query with output ['local.wr_pairs<Purpose.METRIC>Derivation.AGGREGATE>', 'local.overlap_all_years<Purpose.METRIC>Derivation.AGGREGATE>', 'local.overlap_y2000<Purpose.METRIC>Derivation.AGGREGATE>'] from current model. Group grp:root:root:∅ (Derivation.ROOT) could not be built, and nothing else delivers what it owes: WHERE atoms [], readers ['grp:aggregate:d0:∅:input:wr.item.item_sk|wr.order_number', 'grp:basic:d*:∅:sig:1940c2ff9524', 'grp:filter:d*:wr.item.item_sk|wr.order_number|ws.sold_date.date_sk:sig:8d411f30a7b8'], outputs ['wr.item.item_sk', 'wr.order_number', 'ws.sold_date.date_sk', 'ws.sold_date.year'].
  ```

### `no-output`

- `trilogy file write probe_seq.preql --run-and-delete`

  ```text
  Nothing was executed: parsed 2 definition statement(s) (1 import, 1 rowset) but none produce output. Did you mean to include a SELECT statement, or run a refresh on datasources instead?
  ```

### `type-error`

- `trilogy file write probe_join5.preql --run-and-delete`

  ```text
  Type error in probe_join5.preql: Invalid argument type 'BIGINT' passed into CONCAT function in position 3 from concept: ss.ticket_number. Valid: 'STRING'.
  ```
