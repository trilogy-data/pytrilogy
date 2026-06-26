# Trilogy failure analysis — 20260626-135713

- Run `20260626-135713` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 289 | failed: 45 (16%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 27 | 60% |
| `syntax-parse` | 12 | 27% |
| `syntax-missing-alias` | 3 | 7% |
| `join-resolution` | 2 | 4% |
| `cli-misuse` | 1 | 2% |

## Detail

### `other`

- `trilogy run query02.preql`

  ```text
  Syntax error in query02.preql: Undefined concept: weekly_dow_sales.week_seq. Suggestions: ['weekly_dow_sales.sales.date.week_seq', 'sales.date.week_seq', 'sales.return_date.week_seq', 'sales.billing_customer.first_sales_date.week_seq', 'sales.billing_customer.first_shipto_date.week_seq', 'sales.ship_customer.first_sales_date.week_seq']
  ```
- `trilogy run query02.preql`

  ```text
  Syntax error in query02.preql: HAVING references 'local.has_2001_flag', which is not in the SELECT projection (line 16). To make them available, you may add it to  the SELECT. Prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.has_2001_flag
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy file read query02.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 62 column 13 (char 1846). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query11.preql duckdb`

  ```text
  Resolution error in query11.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 31). The requested concepts split into 2 disconnected subgraphs: {active_2001.customer_id}; {store.customer.first_name, store.customer.last_name, store.customer.preferred_cust_flag}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query11.preql duckdb`

  ```text
  Syntax error in query11.preql: Undefined concept: active.customer_id. Suggestions: ['active.pivoted.customer_id', 'store_rev.customer_id', 'web_rev.customer_id', 'pivoted.customer_id', 'store.customer.id']
  ```
- `trilogy run query11.preql duckdb`

  ```text
  Syntax error in query11.preql: HAVING references 'pivoted.web_2001', 'pivoted.web_2002', 'pivoted.store_2001', 'pivoted.store_2002', which are not in the SELECT projection (line 39). To make them available, you may add them to  the SELECT. Prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --pivoted.web_2001, --pivoted.web_2002, --pivoted.store_2001, --pivoted.store_2002
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: HAVING references 'local.overall_avg_value', which is not in the SELECT projection (line 120). To make them available, you may add it to  the SELECT. Prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.overall_avg_value
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query14.preql`

  ```text
  Resolution error in query14.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 120). The requested concepts split into 2 disconnected subgraphs: {all_sales_overall.sv}; {combined_nov.brand_id, combined_nov.category_id, combined_nov.channel, combined_nov.class_id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy database describe date_dim`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/physical_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/repro.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: HAVING references 'store_sales.item.id', 'store_sales.date.date', which are not in the SELECT projection (line 9). To make them available, you may add them to  the SELECT. Prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --store_sales.item.id, --store_sales.date.date
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: HAVING clause aggregate `count_distinct(concat(cast(ref:store_sales.item.id,STRING),-,cast(ref:store_sales.date.date,STRING))) by local._frequent_items_desc_prefix` is not in the SELECT projection (line 9). HAVING can only filter on off-grain or nested aggregates that are also computed in the SELECT. Fix one of: (a) add it to SELECT — prefix with `--` to keep it out of the output rows, e.g. `select ..., --count_distinct(concat(cast(ref:store_sales.item.id,STRING),-,cast(ref:store_sales.date.date,STRING))) by local._frequent_items_desc_prefix`; (b) move the filter to WHERE — for an aggregate condition on a non-output grain, write the aggregate inline as `agg(x) by grain` directly in WHERE.
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: HAVING references '__preql_internal.all_rows', which is not in the SELECT projection (line 30). To make them available, you may add it to  the SELECT. Prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --__preql_internal.all_rows
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: Undefined concept: best_customers.cust_id. Suggestions: ['best_customers.customer_totals.cust_id', 'customer_totals.cust_id', '_customer_totals_cust_id']
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: Undefined concept: best_customers.cust_id. Suggestions: ['best_customers.customer_totals.cust_id', 'customer_totals.cust_id', '_customer_totals_cust_id']
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: HAVING references 'customer_totals.total_spend', '__preql_internal.all_rows', which are not in the SELECT projection (line 24). To make them available, you may add them to  the SELECT. Prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --customer_totals.total_spend, --__preql_internal.all_rows
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: HAVING references '__preql_internal.all_rows', which is not in the SELECT projection (line 24). To make them available, you may add it to  the SELECT. Prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --__preql_internal.all_rows
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: Undefined concept: best_customers.cust_id. Suggestions: ['best_customers.customer_totals.cust_id', 'customer_totals.cust_id', '_customer_totals_cust_id']
  ```
- `trilogy file read query54.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 16). The requested concepts split into 2 disconnected subgraphs: {catalog_item_agg.cat_ext_list_price, catalog_item_agg.cat_refund}; {local.coupon_amt_sum, local.cust_city, local.cust_street_name, local.cust_street_number, local.cust_zip, local.first_sales_year, local.first_shipto_year, local.item_id, local.line_count, local.list_price_sum, local.sale_city, local.sale_street_name, local.sale_street_number, local.sale_year, local.sale_zip, local.store_name, local.store_zip, local.wholesale_cost_sum, ss.customer.demographics.marital_status, ss.customer_demographic.marital_status, ss.date.year, ss.item.color, ss.item.current_price, ss.item.product_name, ss.item.text_id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query64.preql`

  ```text
  Unexpected error in query64.preql: (_duckdb.BinderException) Binder Error: Referenced table "cooperative" not found!
  Candidate tables: "late"

  LINE 86:     "cooperative"."item_id" as "catalog_item_agg_item_id",
               ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "cs_item_items"."I_ITEM_ID" as "_catalog_item_agg_item_id",
      sum("cs_catalog_sales"."CS_EXT_LIST_PRICE") as "_catalog_item_agg_cat_ext_list_price",
      sum(( "cr_catalog_returns"."CR_REFUNDED_CASH" + "cr_catalog_returns"."CR_REVERSED_CHARGE" ) + "cr_catalog_returns"."CR_STORE_CREDIT") as "_catalog_item_agg_cat_refund"
  FROM
      "catalog_sales" as "cs_catalog_sales"
      LEFT OUTER JOIN "catalog_returns" as "cr_catalog_returns" on "cs_catalog_sales"."CS_ITEM_SK" = "cr_catalog_returns"."CR_ITEM_SK" AND "cs_catalog_sales"."CS_ORDER_NUMBER" = "cr_catalog_returns"."CR_ORDER_NUMBER"
      INNER JOIN "item" as "cs_item_items" on "cs_catalog_sales"."CS_ITEM_SK" = "cs_item_items"."I_ITEM_SK"
  GROUP BY
      1),
  cooperative as (
  SELECT
      "cheerful"."_catalog_item_agg_cat_ext_list_price" as "catalog_item_agg_cat_ext_list_price",
      "cheerful"."_catalog_item_agg_cat_refund" as "catalog_item_agg_cat_refund",
      "cheerful"."_catalog_item_agg_item_id" as "catalog_item_agg_item_id"
  FROM
      "cheerful"),
  late as (
  SELECT
      "ss_customer_address_customer_address"."CA_CITY" as "ss_customer_address_city",
      "ss_customer_address_customer_address"."CA_STREET_NAME" as "ss_customer_address_street_name",
      "ss_customer_address_customer_address"."CA_STREET_NUMBER" as "ss_customer_address_street_number",
      "ss_customer_address_customer_address"."CA_ZIP" as "ss_customer_address_zip",
      "ss_customer_first_sales_date_date"."D_YEAR" as "ss_customer_first_sales_date_year",
      "ss_customer_first_shipto_date_date"."D_YEAR" as "ss_customer_first_shipto_date_year",
      "ss_date_date"."D_YEAR" as "ss_date_year",
      "ss_item_items"."I_ITEM_ID" as "ss_item_text_id",
      "ss_item_items"."I_ITEM_SK" as "pr_item_id",
      "ss_item_items"."I_PRODUCT_NAME" as "ss_item_product_name",
      "ss_sale_address_customer_address"."CA_CITY" as "ss_sale_address_city",
      "ss_sale_address_customer_address"."CA_STREET_NAME" as "ss_sale_address_street_name",
      "ss_sale_address_customer_address"."CA_STREET_NUMBER" as "ss_sale_address_street_number",
      "ss_sale_address_customer_address"."CA_ZIP" as "ss_sale_address_zip",
      "ss_store_sales"."SS_COUPON_AMT" as "ss_coupon_amt",
      "ss_store_sales"."SS_EXT_LIST_PRICE" as "ss_ext_list_price",
      "ss_store_sales"."SS_EXT_WHOLESALE_COST" as "ss_ext_wholesale_cost",
      "ss_store_sales"."SS_TICKET_NUMBER" as "pr_ticket_number",
      "ss_store_store"."S_STORE_NAME" as "ss_store_name",
      "ss_store_store"."S_ZIP" as "ss_store_zip"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "item" as "ss_item_items" on "ss_store_sales"."SS_ITEM_SK" = "ss_item_items"."I_ITEM_SK"
      INNER JOIN "date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "store" as "ss_store_store" on "ss_store_sales"."SS_STORE_SK" = "ss_store_store"."S_STORE_SK"
      INNER JOIN "customer" as "ss_customer_customers" on "ss_store_sales"."SS_CUSTOMER_SK" = "ss_customer_customers"."C_CUSTOMER_SK"
      LEFT OUTER JOIN "customer_address" as "ss_sale_address_customer_address" on "ss_store_sales"."SS_ADDR_SK" = "ss_sale_address_customer_address"."CA_ADDRESS_SK"
      LEFT OUTER JOIN "customer_address" as "ss_customer_address_customer_address" on "ss_customer_customers"."C_CURRENT_ADDR_SK" = "ss_customer_address_customer_address"."CA_ADDRESS_SK"
      INNER JOIN "customer_demographics" as "ss_customer_demographic_customer_demographics" on "ss_store_sales"."SS_CDEMO_SK" = "ss_customer_demographic_customer_demographics"."CD_DEMO_SK"
      INNER JOIN "customer_demographics" as "ss_customer_demographics_customer_demographics" on "ss_customer_customers"."C_CURRENT_CDEMO_SK" = "ss_customer_demographics_customer_demographics"."CD_DEMO_SK"
      LEFT OUTER JOIN "date_dim" as "ss_customer_first_sales_date_date" on "ss_customer_customers"."C_FIRST_SALES_DATE_SK" = "ss_customer_first_sales_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "date_dim" as "ss_customer_first_shipto_date_date" on "ss_customer_customers"."C_FIRST_SHIPTO_DATE_SK" = "ss_customer_first_shipto_date_date"."D_DATE_SK"
      INNER JOIN "cooperative" on "ss_item_items"."I_ITEM_ID" = "cooperative"."catalog_item_agg_item_id"
  WHERE
      "cooperative"."catalog_item_agg_cat_ext_list_price" > 2 * "cooperative"."catalog_item_agg_cat_refund" and "ss_item_items"."I_ITEM_ID" in (select cooperative."catalog_item_agg_item_id" from cooperative where cooperative."catalog_item_agg_item_id" is not null) and "ss_item_items"."I_COLOR" in ('purple','burlywood','indian','spring','floral','medium') and "ss_item_items"."I_CURRENT_PRICE" BETWEEN 65 AND 74 and "ss_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" != "ss_customer_demographics_customer_demographics"."CD_MARITAL_STATUS" and "ss_date_date"."D_YEAR" in (1999,2000)

  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      10,
      11,
      12,
      13,
      14,
      15,
      16,
      17,
      18,
      19,
      20,
      "ss_customer_demographic_customer_demographics"."CD_MARITAL_STATUS",
      "ss_customer_demographics_customer_demographics"."CD_MARITAL_STATUS",
      "ss_item_items"."I_COLOR",
      "ss_item_items"."I_CURRENT_PRICE"),
  macho as (
  SELECT
      "cooperative"."item_id" as "catalog_item_agg_item_id",
      "late"."pr_item_id" as "pr_item_id",
      "late"."pr_ticket_number" as "pr_ticket_number",
      "late"."ss_coupon_amt" as "ss_coupon_amt",
      "late"."ss_customer_address_city" as "ss_customer_address_city",
      "late"."ss_customer_address_street_name" as "ss_customer_address_street_name",
      "late"."ss_customer_address_street_number" as "ss_customer_address_street_number",
      "late"."ss_customer_address_zip" as "ss_customer_address_zip",
      "late"."ss_customer_first_sales_date_year" as "ss_customer_first_sales_date_year",
      "late"."ss_customer_first_shipto_date_year" as "ss_customer_first_shipto_date_year",
      "late"."ss_date_year" as "ss_date_year",
      "late"."ss_ext_list_price" as "ss_ext_list_price",
      "late"."ss_ext_wholesale_cost" as "ss_ext_wholesale_cost",
      "late"."ss_item_product_name" as "ss_item_product_name",
      "late"."ss_item_text_id" as "ss_item_text_id",
      "late"."ss_sale_address_city" as "ss_sale_address_city",
      "late"."ss_sale_address_street_name" as "ss_sale_address_street_name",
      "late"."ss_sale_address_street_number" as "ss_sale_address_street_number",
      "late"."ss_sale_address_zip" as "ss_sale_address_zip",
      "late"."ss_store_name" as "ss_store_name",
      "late"."ss_store_zip" as "ss_store_zip"
  FROM
      "late")
  SELECT
      "macho"."ss_item_product_name" as "ss_item_product_name",
      "macho"."ss_item_text_id" as "item_id",
      "macho"."ss_store_name" as "store_name",
      "macho"."ss_store_zip" as "store_zip",
      "macho"."ss_sale_address_street_number" as "sale_street_number",
      "macho"."ss_sale_address_street_name" as "sale_street_name",
      "macho"."ss_sale_address_city" as "sale_city",
      "macho"."ss_sale_address_zip" as "sale_zip",
      "macho"."ss_customer_address_street_number" as "cust_street_number",
      "macho"."ss_customer_address_street_name" as "cust_street_name",
      "macho"."ss_customer_address_city" as "cust_city",
      "macho"."ss_customer_address_zip" as "cust_zip",
      "macho"."ss_date_year" as "sale_year",
      "macho"."ss_customer_first_sales_date_year" as "first_sales_year",
      "macho"."ss_customer_first_shipto_date_year" as "first_shipto_year",
      count((cast("macho"."pr_ticket_number" as string) || '-' || cast("macho"."pr_item_id" as string))) as "line_count",
      sum("macho"."ss_ext_wholesale_cost") as "wholesale_cost_sum",
      sum("macho"."ss_ext_list_price") as "list_price_sum",
      sum("macho"."ss_coupon_amt") as "coupon_amt_sum"
  FROM
      "macho"
      INNER JOIN "cooperative" on "macho"."catalog_item_agg_item_id" = "cooperative"."catalog_item_agg_item_id"
  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      10,
      11,
      12,
      13,
      14,
      15
  ORDER BY
      "macho"."ss_item_product_name" asc,
      "store_name" asc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Undefined concept: y1999.item_id. Suggestions: ['y1999.year_agg.item_id', 'catalog_qualifying_items.item_id', 'store_return_items.item_id', 'year_agg.item_id', 'y2000.year_agg.item_id', 'y2000.item_id']
  ```
- `trilogy run query75.preql`

  ```text
  Syntax error in query75.preql: 2 undefined concept references; fix all before re-running:
    - yr2002.total_amt (line 52, in SELECT); did you mean: aggregated.total_amt, yr2001.total_amt, yr2002.amt_2002, _yr2002_amt_2002?
    - yr2001.total_amt (line 52, in SELECT); did you mean: aggregated.total_amt, yr2002.total_amt, yr2001.amt_2001, _yr2001_amt_2001?
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy file write query02.preql --content import raw.all_sales as sales;

# Rowset: weekly sales by week_seq and day_of_week (WEB + CATALOG only)
rowset wee…es
      where sales.channel in ('WEB','CATALOG') and sales.date.year = 2001
    )) > 0
order by weekly_dow_sales.week_seq asc nulls first
limit 100;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
    --> 66:6
     |
  66 |     -- Filter to week_seqs that occur in year 2001
     |      ^---
     |
     = expected access_chain
  Location:
   , 2) as saturday having     - ??? - Filter to week_seqs that occ...

  Write stats: received 3163 chars / 3163 bytes; tail: …'rder by weekly_dow_sales.week_seq asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query02.preql --content import raw.all_sales as sales;

# Pre-compute which week_seqs have at least one 2001 date
# A week_seq can span ye…ences that occur in 2001
    wk.week_seq in (select sales.date.week_seq where sales.date.year = 2001)
order by wk.week_seq asc nulls first
limit 200;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
   --> 9:1
    |
  9 | def sun_sales -> sum(sales.ext_sales_price ? sales.date.day_of_week = 0);
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...m per day-of-week for clarity ??? def sun_sales -> sum(sales.ext...

  Write stats: received 2259 chars / 2259 bytes; tail: …'year = 2001)\\norder by wk.week_seq asc nulls first\\nlimit 200;'.
  ```
- `trilogy file write query02.preql --content import raw.all_sales as sales;

# Pre-compute which week_seqs have at least one 2001 date
auto has_2001 <- sum(sal…ences that occur in 2001
    wk.week_seq in (select sales.date.week_seq where sales.date.year = 2001)
order by wk.week_seq asc nulls first
limit 200;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
    --> 31:6
     |
  31 |     -- Only week sequences that occur in 2001
     |      ^---
     |
     = expected access_chain
  Location:
  ...), 2) as saturday having     - ??? - Only week sequences that occ...

  Write stats: received 1875 chars / 1875 bytes; tail: …'year = 2001)\\norder by wk.week_seq asc nulls first\\nlimit 200;'.
  ```
- `trilogy file write query02.preql --content import raw.all_sales as sales;

# Pre-compute which week_seqs have at least one 2001 date
auto has_2001 <- sum(sal…ences that occur in 2001
    wk.week_seq in (select sales.date.week_seq where sales.date.year = 2001)
order by wk.week_seq asc nulls first
limit 200;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy does not support subqueries — joins are auto-resolved from dotted paths. To filter on a value that lives on a related dimension, reference its dot-path directly. Example: instead of `where ss.store_id in (select store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...ur in 2001     wk.week_seq in ??? (select sales.date.week_seq wh...

  Write stats: received 1874 chars / 1874 bytes; tail: …'year = 2001)\\norder by wk.week_seq asc nulls first\\nlimit 200;'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all;

# Base data: compute entity-level sales and returns within the date range
rowset bas…sce(sum(base.gross_sales) - sum(base.return_net_loss), 0) as net_profit

order by
    channel_type nulls first,
    entity_id nulls first
limit 100;
`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:1
    |
  4 | rowset base as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...returns within the date range ??? rowset base as select     all....

  Write stats: received 2620 chars / 2620 bytes; tail: …'nnel_type nulls first,\\n    entity_id nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query14.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Step 1: Find (brand_i…first,
    combined_nov.brand_id asc nulls first,
    combined_nov.class_id asc nulls first,
    combined_nov.category_id asc nulls first
limit 100;
`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...<- avg(all_vals.val) by *     ??? from (         union(

  Write stats: received 5249 chars / 5249 bytes; tail: …'st,\\n    combined_nov.category_id asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query14.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Step 1: Find (brand_i…first,
    combined_nov.brand_id asc nulls first,
    combined_nov.class_id asc nulls first,
    combined_nov.category_id asc nulls first
limit 100;
`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy does not support subqueries — joins are auto-resolved from dotted paths. To filter on a value that lives on a related dimension, reference its dot-path directly. Example: instead of `where ss.store_id in (select store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...d, combined_nov.category_id > ??? (select avg(all_sales_overall....

  Write stats: received 5303 chars / 5303 bytes; tail: …'st,\\n    combined_nov.category_id asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query17.preql --content import raw.store_sales as ss;
import raw.physical_returns as sr;
import raw.catalog_sales as cs;

where 
  ss.date…er.id
inner join ss.item.id = cs.item.id
order by item_code asc nulls first, item_description asc nulls first, store_state asc nulls first
limit 100;`

  ```text
  refused to write 'query17.preql': not syntactically valid Trilogy.

  Parse error:
    --> 13:9
     |
  13 |   count(*) as cnt,
     |         ^---
     |
     = expected sum_operator
  Location:
  ...state as store_state,   count( ??? *) as cnt,   avg(ss.quantity)

  Write stats: received 1219 chars / 1219 bytes; tail: …'tion asc nulls first, store_state asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query17.preql --content import raw.store_sales as ss;
import raw.physical_returns as sr;
import raw.catalog_sales as cs;

# Rowset: store …y ss_with_returns.item_code asc nulls first, ss_with_returns.item_description asc nulls first, ss_with_returns.store_state asc nulls first
limit 100;`

  ```text
  refused to write 'query17.preql': not syntactically valid Trilogy.

  Parse error:
    --> 34:9
     |
  34 |   count(*) as cnt,
     |         ^---
     |
     = expected sum_operator
  Location:
  ..._returns.store_state,   count( ??? *) as cnt,   avg(ss_with_retur...

  Write stats: received 1923 chars / 1923 bytes; tail: …'irst, ss_with_returns.store_state asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…al as total_sales
order by
    combined.last_name nulls first,
    combined.first_name nulls first,
    combined.channel_total nulls first
limit 100;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` — a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...string, channel_total numeric) ???  select     combined.last_nam...

  Write stats: received 2969 chars / 2969 bytes; tail: …'lls first,\\n    combined.channel_total nulls first\\nlimit 100;'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…al as total_sales
order by
    combined.last_name nulls first,
    combined.first_name nulls first,
    combined.channel_total nulls first
limit 100;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
    --> 29:78
     |
  29 |     customer_totals.total_spend > 0.5 * max(customer_totals.total_spend) by ()
     |                                                                              ^---
     |
     = expected expr_over_list
  Location:
  ...tomer_totals.total_spend) by ( ??? ) ;  # Step 4: For Feb 2000, s...

  Write stats: received 2584 chars / 2584 bytes; tail: …'lls first,\\n    combined.channel_total nulls first\\nlimit 100;'.
  ```
- `trilogy file write query64.preql --content import raw.store_sales as ss;
import raw.physical_returns as pr;
import raw.catalog_sales as cs;
import raw.catalo…_cost_sum,
  sum(ss.ext_list_price) as list_price_sum,
  sum(ss.coupon_amt) as coupon_amt_sum
order by ss.item.product_name, ss.store.name
limit 100;`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [220]: Filter or stray clause after a `join`? A query-scoped join `inner|left|full join <a> = <b>` takes only key equalities — to join on multiple keys, chain `= c` (one equivalence group) or separate distinct groups with `and` (`a = b and c = d`); STACK another `join` clause for a different join type. Note `and` joins KEY EQUALITIES only, not filters. Joins go right after the `select` list (preferred, SQL-like) or before `select`; the order is `where` -> `select` <cols> -> join(s) -> `having` -> `order by` -> `limit`. Filter input rows in `where` (before `select`); filter a joined or aggregated RESULT in `having` (select the field, hide it with a leading `--`). Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...and marital status difference ??? inner join ss.ticket_number =

  Write stats: received 1890 chars / 1890 bytes; tail: …'_sum\\norder by ss.item.product_name, ss.store.name\\nlimit 100;'.
  ```

### `syntax-missing-alias`

- `trilogy file write query11.preql -e -c import raw.store_sales as store;
import raw.web_sales as web;

# Per-customer store revenue by year
with store_rev as
…voted.store_2001
        else 0 end
order by pivoted.customer_id asc
auto first_name asc,
auto last_name asc,
auto preferred_cust_flag asc
limit 100;`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `store growth rate (zero if store_2001 = 0)
      case when pivoted.store_2001 > 0
          then (coalesce(pivoted.store_2002, 0) - pivoted.store_2001) / pivoted.store_2001
          else 0 end as store_growth_rate_zero_if_store_2001_0_c`
  Location:
  ...ivoted.web_2002,     -- store ??? growth rate (zero if store_200...

  Write stats: received 2459 chars / 2459 bytes; tail: …'\\nauto last_name asc,\\nauto preferred_cust_flag asc\\nlimit 100;'.
  ```
- `trilogy run --import raw.catalog_sales:cs select count(cs.line_item), count_distinct(cs.bill_customer.id) where cs.item.category='Women' and cs.item.class='maternity' and cs.sold_date.month_seq=1187;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(cs.line_item) as line_item_count`
  Location:
  ...cs; select count(cs.line_item) ??? , count_distinct(cs.bill_custo...
  ```
- `trilogy run --import raw.web_sales:ws select count(ws.line_item), count_distinct(ws.billing_customer.id) where ws.item.category='Women' and ws.item.class='maternity' and ws.date.month_seq=1187;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(ws.line_item) as line_item_count`
  Location:
  ...ws; select count(ws.line_item) ??? , count_distinct(ws.billing_cu...
  ```

### `join-resolution`

- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Could not resolve connections for query with output ['local.qualifying_item<Purpose.PROPERTY>Derivation.FILTER>'] from current model.
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Could not resolve connections for query with output ['local.q_item<Purpose.PROPERTY>Derivation.FILTER>'] from current model.
  ```

### `cli-misuse`

- `trilogy explore raw/store_returns.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_returns.preql' does not exist.
  ```
