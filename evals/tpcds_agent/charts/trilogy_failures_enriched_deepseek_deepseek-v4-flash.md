# Trilogy failure analysis — 20260820-153008

- Run `20260820-153007_enriched_deepseek_deepseek-v4-flash` | `deepseek/deepseek-v4-flash` | sf=1
- `trilogy` calls: 1357 | failed: 55 (4%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `disabled-tool` | 17 | 31% |
| `other` | 12 | 22% |
| `syntax-parse` | 12 | 22% |
| `undefined-concept` | 7 | 13% |
| `cli-misuse` | 3 | 5% |
| `join-resolution` | 2 | 4% |
| `no-output` | 2 | 4% |

## Detail

### `disabled-tool`

- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under root/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/_q04_agent_rowset_union_join.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/item.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read probe15.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under root/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under root/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under root/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/store_sales.preql`

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

### `other`

- `trilogy file write probe_profitvar.preql --run-and-delete`

  ```text
  Syntax error in probe_profitvar.preql: HAVING filters on a dimension outside the SELECT projection, but the select has no grain key to anchor a post-aggregation semijoin (line 21). Move the filter to WHERE to filter before aggregation.
  ```
- `trilogy file write probe_rows.preql --run-and-delete`

  ```text
  Syntax error in probe_rows.preql: ORDER BY references 'als.item.sk', which is not in the SELECT projection (line 3). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --als.item.sk order by als.item.sk asc`.
  ```
- `trilogy file write probe_zips.preql --run-and-delete --param zips=24128,76232`

  ```text
  Resolution error in probe_zips.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: ss.customer.current_address.customer_address_at_ss_customer_current_address_sk_join_ss.customer.customers_at_ss_customer_sk_at_ss_customer_sk_grouped_by_ss.customer.current_address.zip_at_ss_customer_current_address_zip_at_ss_customer_current_address_zip onto ss.customer.current_address.customer_address_at_ss_customer_current_address_sk_join_ss.customer.customers_at_ss_customer_sk_at_ss_customer_sk_at_ss_customer_sk_grouped_by_local._virt_func_substring_370089457884407_ss.customer.current_address.sk_at_ss_customer_current_address_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_verify.preql --run-and-delete`

  ```text
  Unexpected error in probe_verify.preql: (_duckdb.OutOfMemoryException) Out of Memory Error: Allocation failure
  [SQL:
  WITH
  vacuous as (
  SELECT
      "ws_web_sales"."WS_BILL_CUSTOMER_SK" as "ws_billing_customer_sk",
      "ws_web_sales"."WS_SOLD_DATE_SK" as "ws_sale_date_sk"
  FROM
      "web_sales" as "ws_web_sales"
  GROUP BY
      1,
      2),
  young as (
  SELECT
      "vacuous"."ws_billing_customer_sk" as "ws_billing_customer_sk",
      cast("ws_sale_date_date"."D_DATE" as date) as "ws_sale_date_date"
  FROM
      "vacuous"
      LEFT OUTER JOIN "date_dim" as "ws_sale_date_date" on "vacuous"."ws_sale_date_sk" = "ws_sale_date_date"."D_DATE_SK"),
  yummy as (
  SELECT
      "ss_customer_current_address_customer_address"."CA_COUNTY" as "ss_customer_current_address_county",
      "ss_customer_current_demographics_customer_demographics"."CD_DEMO_SK" as "ss_customer_current_demographics_sk",
      "ss_customer_current_demographics_customer_demographics"."CD_GENDER" as "ss_customer_current_demographics_gender",
      "ss_store_sales"."SS_CUSTOMER_SK" as "ss_customer_sk",
      "ss_store_sales"."SS_ITEM_SK" as "ss_item_sk",
      "ss_store_sales"."SS_TICKET_NUMBER" as "ss_ticket_number",
      cast("ss_sale_date_date"."D_DATE" as date) as "ss_sale_date_date"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "customer" as "ss_customer_customers" on "ss_store_sales"."SS_CUSTOMER_SK" = "ss_customer_customers"."C_CUSTOMER_SK"
      INNER JOIN "date_dim" as "ss_sale_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_sale_date_date"."D_DATE_SK"
      INNER JOIN "customer_address" as "ss_customer_current_address_customer_address" on "ss_customer_customers"."C_CURRENT_ADDR_SK" = "ss_customer_current_address_customer_address"."CA_ADDRESS_SK"
      INNER JOIN "customer_demographics" as "ss_customer_current_demographics_customer_demographics" on "ss_customer_customers"."C_CURRENT_CDEMO_SK" = "ss_customer_current_demographics_customer_demographics"."CD_DEMO_SK"
  WHERE
      ("ss_customer_current_address_customer_address"."CA_COUNTY" is not null and "ss_customer_current_address_customer_address"."CA_COUNTY" in ('Rush County','Toole County','Jefferson County','Dona Ana County','La Porte County')) and "ss_customer_current_demographics_customer_demographics"."CD_DEMO_SK" is not null and cast("ss_sale_date_date"."D_DATE" as date) BETWEEN date '2002-01-01' AND date '2002-04-30'
  ),
  quizzical as (
  SELECT
      "cs_catalog_sales"."CS_SHIP_CUSTOMER_SK" as "cs_ship_customer_sk",
      "cs_catalog_sales"."CS_SOLD_DATE_SK" as "cs_sale_date_sk"
  FROM
      "catalog_sales" as "cs_catalog_sales"
  GROUP BY
      1,
      2),
  cheerful as (
  SELECT
      "quizzical"."cs_ship_customer_sk" as "cs_ship_customer_sk",
      cast("cs_sale_date_date"."D_DATE" as date) as "cs_sale_date_date"
  FROM
      "quizzical"
      LEFT OUTER JOIN "date_dim" as "cs_sale_date_date" on "quizzical"."cs_sale_date_sk" = "cs_sale_date_date"."D_DATE_SK"),
  sparkling as (
  SELECT
      "cheerful"."cs_sale_date_date" as "cs_sale_date_date",
      "cheerful"."cs_ship_customer_sk" as "cs_ship_customer_sk",
      "young"."ws_billing_customer_sk" as "ws_billing_customer_sk",
      "young"."ws_sale_date_date" as "ws_sale_date_date",
      "yummy"."ss_customer_current_demographics_gender" as "ss_customer_current_demographics_gender",
      "yummy"."ss_customer_sk" as "ss_customer_sk",
      "yummy"."ss_item_sk" as "ss_item_sk",
      "yummy"."ss_ticket_number" as "ss_ticket_number"
  FROM
      "cheerful"
      RIGHT OUTER JOIN "yummy" on 1=1
      LEFT OUTER JOIN "young" on 1=1
  WHERE
      "yummy"."ss_sale_date_date" BETWEEN date '2002-01-01' AND date '2002-04-30' and ("yummy"."ss_customer_current_address_county" is not null and "yummy"."ss_customer_current_address_county" in ('Rush County','Toole County','Jefferson County','Dona Ana County','La Porte County')) and "yummy"."ss_customer_current_demographics_sk" is not null
  ),
  sweltering as (
  SELECT
      CASE WHEN ( "sparkling"."cs_sale_date_date" BETWEEN date '2002-01-01' AND date '2002-04-30' ) THEN "sparkling"."cs_ship_customer_sk" ELSE NULL END as "catalog_ship_customers",
      CASE WHEN ( "sparkling"."ws_sale_date_date" BETWEEN date '2002-01-01' AND date '2002-04-30' ) THEN "sparkling"."ws_billing_customer_sk" ELSE NULL END as "web_bill_customers"
  FROM
      "sparkling"),
  macho as (
  SELECT
      "sparkling"."ss_item_sk" as "ss_item_sk",
      "sparkling"."ss_ticket_number" as "ss_ticket_number",
      (exists (select 1 from sweltering where sweltering."catalog_ship_customers" is not distinct from "sparkling"."ss_customer_sk")) as "has_catalog_ship",
      (exists (select 1 from sweltering where sweltering."web_bill_customers" is not distinct from "sparkling"."ss_customer_sk")) as "has_web_bill",
      CONCAT(cast("sparkling"."ss_ticket_number" as string), '-', cast("sparkling"."ss_item_sk" as string)) as "ss_line_item"
  FROM
      "sparkling"
  GROUP BY
      1,
      2,
      3,
      4,
      5,
      "sparkling"."cs_ship_customer_sk",
      "sparkling"."ss_customer_current_demographics_gender",
      "sparkling"."ss_customer_sk",
      "sparkling"."ws_billing_customer_sk"),
  late as (
  SELECT
      "ss_customer_current_address_customer_address"."CA_COUNTY" as "ss_customer_current_address_county",
      "ss_customer_current_demographics_customer_demographics"."CD_GENDER" as "ss_customer_current_demographics_gender",
      "ss_store_sales"."SS_CUSTOMER_SK" as "ss_customer_sk",
      "ss_store_sales"."SS_ITEM_SK" as "ss_item_sk",
      "ss_store_sales"."SS_TICKET_NUMBER" as "ss_ticket_number",
      (exists (select 1 from sweltering where sweltering."catalog_ship_customers" is not distinct from "ss_store_sales"."SS_CUSTOMER_SK")) as "has_catalog_ship",
      (exists (select 1 from sweltering where sweltering."web_bill_customers" is not distinct from "ss_store_sales"."SS_CUSTOMER_SK")) as "has_web_bill"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "customer" as "ss_customer_customers" on "ss_store_sales"."SS_CUSTOMER_SK" = "ss_customer_customers"."C_CUSTOMER_SK"
      INNER JOIN "date_dim" as "ss_sale_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_sale_date_date"."D_DATE_SK"
      INNER JOIN "customer_address" as "ss_customer_current_address_customer_address" on "ss_customer_customers"."C_CURRENT_ADDR_SK" = "ss_customer_current_address_customer_address"."CA_ADDRESS_SK"
      INNER JOIN "customer_demographics" as "ss_customer_current_demographics_customer_demographics" on "ss_customer_customers"."C_CURRENT_CDEMO_SK" = "ss_customer_current_demographics_customer_demographics"."CD_DEMO_SK"
  WHERE
      cast("ss_sale_date_date"."D_DATE" as date) BETWEEN date '2002-01-01' AND date '2002-04-30' and ("ss_customer_current_address_customer_address"."CA_COUNTY" is not null and "ss_customer_current_address_customer_address"."CA_COUNTY" in ('Rush County','Toole County','Jefferson County','Dona Ana County','La Porte County')) and "ss_customer_current_demographics_customer_demographics"."CD_DEMO_SK" is not null

  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      "ss_customer_current_address_customer_address"."CA_ADDRESS_SK",
      "ss_customer_current_demographics_customer_demographics"."CD_DEMO_SK",
      "ss_store_sales"."SS_SOLD_DATE_SK",
      cast("ss_sale_date_date"."D_DATE" as date))
  SELECT
      "late"."ss_customer_sk" as "ss_customer_sk",
      "late"."ss_customer_current_address_county" as "ss_customer_current_address_county",
      "late"."ss_customer_current_demographics_gender" as "ss_customer_current_demographics_gender",
      "late"."has_web_bill" as "has_web_bill",
      "late"."has_catalog_ship" as "has_catalog_ship",
      count("macho"."ss_line_item") as "store_lines"
  FROM
      "macho"
      INNER JOIN "late" on "macho"."has_catalog_ship" is not distinct from "late"."has_catalog_ship" AND "macho"."has_web_bill" is not distinct from "late"."has_web_bill" AND "macho"."ss_item_sk" = "late"."ss_item_sk" AND "macho"."ss_ticket_number" = "late"."ss_ticket_number"
  GROUP BY
      1,
      2,
      3,
      4,
      5
  ORDER BY
      "late"."ss_customer_sk" asc]
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy file write probe_765177085.preql --run-and-delete`

  ```text
  trilogy error: subprocess timed out after 600s.
  ```
- `trilogy file write probe_check.preql --run-and-delete`

  ```text
  Syntax error in probe_check.preql: Output column 'peach_total' renames 'local.peach_total' back to the name of an existing concept 'peach_total' (defined at line 7) that 'local.peach_total' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'peach_total_out').
  ```
- `trilogy file write probe_scale.preql --run-and-delete`

  ```text
  Unexpected error in probe_scale.preql: Could not render the query: Missing source reference to ss.sale_date.year; Missing source reference to ss.sale_date.quarter. A planned reference has no backing source CTE -- typically an unsupported cross-rowset or membership shape the planner could not wire. Review the rowset/join structure (or file an issue if the query looks valid).

  Full SQL with sentinel(s):

  WITH
  wakeful as (
  SELECT
      "ss_store_sales"."SS_CUSTOMER_SK" as "ss_customer_sk",
      CASE WHEN ( "ss_sale_date_date"."D_YEAR" = 2002 and "ss_sale_date_date"."D_QOY" <= 3 ) THEN "ss_store_sales"."SS_CUSTOMER_SK" ELSE NULL END as "_virt_filter_sk_7697480711735839",
      CONCAT(cast("ss_store_sales"."SS_TICKET_NUMBER" as string), '-', cast("ss_store_sales"."SS_ITEM_SK" as string)) as "ss_line_item"
  FROM
      "store_sales" as "ss_store_sales"
      LEFT OUTER JOIN "date_dim" as "ss_sale_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_sale_date_date"."D_DATE_SK"),
  thoughtful as (
  SELECT
      "wakeful"."_virt_filter_sk_7697480711735839" as "_virt_filter_sk_7697480711735839",
      "wakeful"."ss_customer_sk" as "ss_customer_sk"
  FROM
      "wakeful"
  GROUP BY
      1,
      2),
  cooperative as (
  SELECT
      count(distinct "thoughtful"."_virt_filter_sk_7697480711735839") as "win_cust",
      count(distinct "thoughtful"."ss_customer_sk") as "all_cust"
  FROM
      "thoughtful"),
  cheerful as (
  SELECT
      count("wakeful"."ss_line_item") as "all_lines",
      count(CASE WHEN ( INVALID_REFERENCE_BUG<Missing source reference to ss.sale_date.year> = 2002 and INVALID_REFERENCE_BUG<Missing source reference to ss.sale_date.quarter> <= 3 ) THEN "wakeful"."ss_line_item" ELSE NULL END) as "win_lines"
  FROM
      "wakeful")
  SELECT
      coalesce("cheerful"."all_lines",0) as "all_lines",
      "cooperative"."all_cust" as "all_cust",
      coalesce("cheerful"."win_lines",0) as "win_lines",
      "cooperative"."win_cust" as "win_cust"
  FROM
      "cheerful"
      INNER JOIN "cooperative" on 1=1
  ```
- `trilogy file write answer_3553309440.preql --run`

  ```text
  Resolution error in answer_3553309440.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: ss.customer.current_address.customer_address_at_ss_customer_current_address_sk_grouped_by_ss.customer.current_address.county_ss.customer.current_address.state_at_ss_customer_current_address_county_ss_customer_current_address_state onto cs.catalog_sales_at_cs_item_sk_cs_order_number_grouped_by_cs.billing_customer.sk_cs.item.sk_cs.sale_date.sk_at_cs_billing_customer_sk_cs_item_sk_cs_sale_date_sk_join_cs.item.items_at_cs_item_sk_join_cs.sale_date.date_at_cs_sale_date_sk_at_cs_billing_customer_sk_cs_item_sk_cs_sale_date_sk_filtered_by_1846402161471008_at_cs_billing_customer_sk_cs_item_sk_cs_sale_date_sk_grouped_by_local.___tvf_arm_0_cust_sk_at_local____tvf_arm_0_cust_sk_at_local____tvf_arm_0_cust_sk_union_ws.item.items_at_ws_item_sk_join_ws.sale_date.date_at_ws_sale_date_sk_join_ws.web_sales_at_ws_item_sk_ws_order_number_grouped_by_ws.billing_customer.sk_ws.item.sk_ws.sale_date.sk_at_ws_billing_customer_sk_ws_item_sk_ws_sale_date_sk_at_ws_billing_customer_sk_ws_item_sk_ws_sale_date_sk_filtered_by_3152872554019764_at_ws_billing_customer_sk_ws_item_sk_ws_sale_date_sk_grouped_by_local.___tvf_arm_1_cust_sk_at_local____tvf_arm_1_cust_sk_at_local____tvf_arm_1_cust_sk_unioned_at_qualifying_customers_cust_sk_join_ss.sale_date.date_at_ss_sale_date_sk_join_ss.store.store_at_ss_store_sk_join_ss.store_sales_at_ss_item_sk_ss_ticket_number_at_ss_item_sk_ss_ticket_number_at_ss_item_sk_ss_ticket_number_filtered_by_2842634433621718, cs.catalog_sales_at_cs_item_sk_cs_order_number_grouped_by_cs.billing_customer.sk_cs.item.sk_cs.sale_date.sk_at_cs_billing_customer_sk_cs_item_sk_cs_sale_date_sk_join_cs.item.items_at_cs_item_sk_join_cs.sale_date.date_at_cs_sale_date_sk_at_cs_billing_customer_sk_cs_item_sk_cs_sale_date_sk_filtered_by_1846402161471008_at_cs_billing_customer_sk_cs_item_sk_cs_sale_date_sk_grouped_by_local.___tvf_arm_0_cust_sk_at_local____tvf_arm_0_cust_sk_at_local____tvf_arm_0_cust_sk_union_ws.item.items_at_ws_item_sk_join_ws.sale_date.date_at_ws_sale_date_sk_join_ws.web_sales_at_ws_item_sk_ws_order_number_grouped_by_ws.billing_customer.sk_ws.item.sk_ws.sale_date.sk_at_ws_billing_customer_sk_ws_item_sk_ws_sale_date_sk_at_ws_billing_customer_sk_ws_item_sk_ws_sale_date_sk_filtered_by_3152872554019764_at_ws_billing_customer_sk_ws_item_sk_ws_sale_date_sk_grouped_by_local.___tvf_arm_1_cust_sk_at_local____tvf_arm_1_cust_sk_at_local____tvf_arm_1_cust_sk_unioned_at_qualifying_customers_cust_sk_join_ss.sale_date.date_at_ss_sale_date_sk_join_ss.store_sales_at_ss_item_sk_ss_ticket_number_at_ss_item_sk_ss_ticket_number_at_ss_item_sk_ss_ticket_number_filtered_by_2842634433621718_join_d.date_at_d_sk_grouped_by__at_abstract_at_ss_item_sk_ss_ticket_number_at_ss_item_sk_ss_ticket_number_filtered_by_5989033797207142_grouped_by_ss.customer.sk_at_ss_customer_sk_at_ss_customer_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write answer_1772060640.preql --run`

  ```text
  Syntax error in answer_1772060640.preql: ORDER BY references 'ss.customer.id', which is not in the SELECT projection (line 3). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --ss.customer.id order by ss.customer.id asc`.
  ```
- `trilogy file write answer_3840922834.preql --run`

  ```text
  Syntax error in answer_3840922834.preql: Conflicting join types (full, left outer) on keys joined into one group: a FULL/UNION join cannot be mixed with another type on the same key (it is ambiguous whether the key is required or one-sided). Make the whole group one type (e.g. `UNION JOIN a = b = c`), or use a distinct key. (line 17, column 1)
  ```
- `trilogy file write answer_943796012.preql --run`

  ```text
  Syntax error in answer_943796012.preql: Cannot compare DATE (ref:a.sale_date.date) and STRING (2000-08-23) of different types with operator >= in ref:a.sale_date.date >= 2000-08-23
  ```
- `trilogy file write answer_2852230229.preql --run`

  ```text
  Syntax error in answer_2852230229.preql: ORDER BY references 'local.cat_ord', which is not in the SELECT projection (line 12). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --local.cat_ord order by local.cat_ord asc`.
  ```

### `syntax-parse`

- `trilogy file write probe_rows.preql --run-and-delete`

  ```text
  refused to write 'probe_rows.preql': not syntactically valid Trilogy.

  Parse error:
    --> 20:25
     |
  20 | order by als.order_id, --als.item.sk
     |                         ^---
     |
     = expected access_chain
  Location:
  ...date
   order by als.order_id, - ??? -als.item.sk
   limit 30;
  ```
- `trilogy file write probe_pref.preql --run-and-delete`

  ```text
  refused to write 'probe_pref.preql': not syntactically valid Trilogy.

  Parse error:
   --> 8:1
    |
  8 | group_by zip
    | ^---
    |
    = expected limit, order_by, THEN_LA, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ..., '65084', '87816', '83926')
   ??? group_by zip
  ```
- `trilogy file write answer_2133330107.preql --run`

  ```text
  refused to write 'answer_2133330107.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:59
    |
  7 |     and substring(ss.customer.current_address.zip, 1, 5) <> substring(ss.store.zip, 1, 5)
    |                                                           ^---
    |
    = expected sum_operator
  Location:
  ...r.current_address.zip, 1, 5) < ??? > substring(ss.store.zip, 1, 5...
  ```
- `trilogy file write probe7.preql --run-and-delete`

  ```text
  refused to write 'probe7.preql': not syntactically valid Trilogy.

  Parse error:
   --> 8:108
    |
  8 |   count(grain(ss.item.sk, ss.ticket_number) ? ss.return_customer.sk is not null and ss.return_customer.sk <> ss.customer.sk) as diff_cust_lines
    |                                                                                                            ^---
    |
    = expected sum_operator
  Location:
  ...ll and ss.return_customer.sk < ??? > ss.customer.sk) as diff_cust...
  ```
- `trilogy file write probe7.preql --run-and-delete`

  ```text
  refused to write 'probe7.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ..., '85460', '80348', '81792')
   ??? group by zip
   order by zip;
  ```
- `trilogy file write probe2.preql --run-and-delete`

  ```text
  refused to write 'probe2.preql': not syntactically valid Trilogy.

  Parse error:
    --> 15:1
     |
  15 | union(
     | ^---
     |
     = expected limit, order_by, THEN_LA, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...ll_address.gmt_offset = -5

   ??? union(

   select
       'sold_ve...
  ```
- `trilogy file write probe2.preql --run-and-delete`

  ```text
  refused to write 'probe2.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
   = -5)
   ) -> (approach, total) ???

   select
       combined.appro...
  ```
- `trilogy file write answer_3063407983.preql --run`

  ```text
  refused to write 'answer_3063407983.preql': not syntactically valid Trilogy.

  Parse error:
   --> 8:43
    |
  8 |     and ss.customer.current_address.city <> ss.pos_address.city
    |                                           ^---
    |
    = expected sum_operator
  Location:
  ...ustomer.current_address.city < ??? > ss.pos_address.city
   select
  ```
- `trilogy file write probe_2874833976.preql --run-and-delete`

  ```text
  refused to write 'probe_2874833976.preql': not syntactically valid Trilogy.

  Parse error:
    --> 13:1
     |
  13 | grouping sets (ss.store.state)
     | ^---
     |
     = expected limit, order_by, THEN_LA, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...re.state in top_states.state
   ??? grouping sets (ss.store.state)...
  ```
- `trilogy file write probe_2874833976.preql --run-and-delete`

  ```text
  refused to write 'probe_2874833976.preql': not syntactically valid Trilogy.

  Parse error:
    --> 13:19
     |
  13 | by grouping sets (ss.store.state)
     |                   ^---
     |
     = expected grouping_set
  Location:
  ...ates.state
   by grouping sets ( ??? ss.store.state)
   order by tota...
  ```
- `trilogy file write probe4.preql --run-and-delete`

  ```text
  refused to write 'probe4.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...el, a.order_id)) as rows_cnt
   ??? group by a.channel, dim_id_nul...
  ```
- `trilogy file write answer_840315271.preql --run`

  ```text
  refused to write 'answer_840315271.preql': not syntactically valid Trilogy.

  Parse error:
    --> 22:15
     |
  22 |   yearly_avg <> 0
     |               ^---
     |
     = expected sum_operator
  Location:
  ...total,
   having
     yearly_avg < ??? > 0
     and abs(monthly_total -...
  ```

### `undefined-concept`

- `trilogy file write probe_all.preql --run-and-delete`

  ```text
  Syntax error in probe_all.preql: Undefined concept: als.line_item (line 8, in SELECT). Suggestions: ['als.sale_line_item_counter', 'als.net_profit', 'als.item.sk']
  ```
- `trilogy file write probe3.preql --run-and-delete`

  ```text
  Syntax error in probe3.preql: Undefined concept: it.category. Suggestions: ['ct.category', 'ws.item.category', 'item_totals.category', 'class_totals.category', 'ws.item.category_id']
  ```
- `trilogy file write probe3.preql --run-and-delete`

  ```text
  Syntax error in probe3.preql: 4 undefined concept references; fix all before re-running:
    - local.item_code (line 41, col 10, in ORDER BY); did you mean: ssr.item_code, item_desc, store_code?
    - local.item_desc (line 41, col 21, in ORDER BY); did you mean: ssr.item_desc, item_code, ss.item.desc?
    - local.store_code (line 41, col 32, in ORDER BY); did you mean: ssr.store_code, item_code, store_name?
    - local.store_name (line 41, col 44, in ORDER BY); did you mean: ssr.store_name, ss.store.name, store_code?
  ```
- `trilogy file write answer_3544057080.preql --run`

  ```text
  Syntax error in answer_3544057080.preql: Undefined concept: a.item_sk. Suggestions: ['a.list_sum', 'b.item_sk', 'ss_agg.item_sk', 'y1999.item_sk', 'y2000.item_sk', 'catalog_qual.item_sk']
  ```
- `trilogy file write probe6.preql --run-and-delete`

  ```text
  Syntax error in probe6.preql: 2 undefined concept references; fix all before re-running:
    - y1999.product_name (line 36, col 5, in SELECT); did you mean: y1999.store_name, ss_agg.product_name, ss.item.product_name, cs.item.product_name?
    - y1999.product_name (line 51, col 10, in ORDER BY); did you mean: y1999.store_name, ss_agg.product_name, ss.item.product_name, cs.item.product_name?
  ```
- `trilogy file write probe_fields.preql --run-and-delete`

  ```text
  Syntax error in probe_fields.preql: Undefined concept: w.net_paid_inc_tax. Suggestions: ['w.net_paid', 'w.return_amount_inc_tax', 'w.return_tax', 'c.return_amount_inc_tax']
  ```
- `trilogy file write probe2_2874833976.preql --run-and-delete`

  ```text
  Syntax error in probe2_2874833976.preql: Undefined concept: local.year (line 5, col 10, in ORDER BY). Suggestions: ['ss.sale_date.year', 'ss.return_date.year', 'ss.customer.first_sales_date.year', 'ss.customer.first_shipto_date.year', 'ss.return_customer.first_sales_date.year', 'ss.return_customer.first_shipto_date.year']
  ```

### `cli-misuse`

- `trilogy explore raw/web_sales.preql --ns web_site web_page, return_web_page`

  ```text
  Got unexpected extra argument (web_page, return_web_page)
  ```
- `trilogy explore raw/orders.preql`

  ```text
  Invalid value for 'PATH': File 'raw/orders.preql' does not exist.
  ```
- `trilogy explore raw/store_sales.preql --ns store sale_date`

  ```text
  Got unexpected extra argument (sale_date)
  ```

### `join-resolution`

- `trilogy file write probe_883027685.preql --run-and-delete`

  ```text
  Resolution error in probe_883027685.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {cat_avg_price}; {line_item_count, state, ss.customer.current_address.sk, ss.item.category, ss.item.current_price, ss.sale_date.month_of_year, ss.sale_date.year}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file write probe_3562094594.preql --run-and-delete`

  ```text
  Resolution error in probe_3562094594.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {_catalog_only_ck}; {_catalog_only_ik}. Are you missing a join or merge statement to relate them?
  ```

### `no-output`

- `trilogy file write probe7.preql --run-and-delete`

  ```text
  Nothing was executed: parsed 2 definition statement(s) (1 import, 1 rowset) but none produce output. Did you mean to include a SELECT statement, or run a refresh on datasources instead?
  ```
- `trilogy run raw/all_sales.preql`

  ```text
  Nothing was executed: parsed 31 definition statement(s) (12 datasources, 11 imports, 5 concepts, 3 propertys) but none produce output. Did you mean to include a SELECT statement, or run a refresh on datasources instead?
  ```
