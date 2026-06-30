# Trilogy failure analysis — 20260630-022456

- Run `20260630-022456` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 237 | failed: 39 (16%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 21 | 54% |
| `syntax-parse` | 17 | 44% |
| `syntax-missing-alias` | 1 | 3% |

## Detail

### `other`

- `trilogy run query02.preql`

  ```text
  Resolution error in query02.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {cur_sales.sales.date.week_seq, nxt_sales.total_sales}
  ```
- `trilogy file read query02.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: ORDER BY contains aggregate `grouping(local.channel_type)` (line 21), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(local.channel_type) as g order by g desc`.
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: grouping() requires a concept (column) reference as its argument, not an inline expression like 'coalesce(ref:local.sale_entity,ref:local.return_entity)'. Assign the expression to a named concept and use that concept in both the grouping key and grouping() - e.g. `auto channel <- coalesce(a, b); select ..., grouping(channel) ... by rollup (channel)`.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 50 column 12 (char 1632). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 41 column 12 (char 2018). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query14.preql duckdb`

  ```text
  Syntax error in query14.preql: Undefined concept: local.sale_line_item_counter (line 20, in SELECT). Suggestions: ['sales.sale_line_item_counter']
  ```
- `trilogy run query14.preql duckdb`

  ```text
  Syntax error in query14.preql: ORDER BY contains aggregate `grouping(local.channel)` (line 20), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(local.channel) as g order by g desc`.
  ```
- `trilogy file read query14.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query23.preql duckdb`

  ```text
  Unexpected error in query23.preql: (_duckdb.BinderException) Binder Error: Cannot compare values of type BIGINT and VARCHAR in IN/ANY/ALL clause - an explicit cast is required

  LINE 101:     "catalog_catalog_sales"."CS_ITEM_SK" in (select juicy."frequent_item_prefixes_desc_prefix" from...
                                                     ^
  [SQL:
  WITH
  vacuous as (
  SELECT
      "store_store_sales"."SS_CUSTOMER_SK" as "store_customer_id",
      "store_store_sales"."SS_QUANTITY" as "store_quantity",
      "store_store_sales"."SS_SALES_PRICE" as "store_sales_price"
  FROM
      "store_sales" as "store_store_sales"
      INNER JOIN "date_dim" as "store_date_date" on "store_store_sales"."SS_SOLD_DATE_SK" = "store_date_date"."D_DATE_SK"
  WHERE
      "store_date_date"."D_YEAR" BETWEEN 2000 AND 2003 and "store_store_sales"."SS_CUSTOMER_SK" is not null
  ),
  abundant as (
  SELECT
      SUBSTRING("store_item_items"."I_ITEM_DESC",1,30) as "_frequent_item_prefixes_desc_prefix"
  FROM
      "store_sales" as "store_store_sales"
      INNER JOIN "date_dim" as "store_date_date" on "store_store_sales"."SS_SOLD_DATE_SK" = "store_date_date"."D_DATE_SK"
      INNER JOIN "item" as "store_item_items" on "store_store_sales"."SS_ITEM_SK" = "store_item_items"."I_ITEM_SK"
  WHERE
      "store_date_date"."D_YEAR" BETWEEN 2000 AND 2003

  GROUP BY
      1
  HAVING
      count((cast("store_store_sales"."SS_TICKET_NUMBER" as string) || '-' || cast("store_item_items"."I_ITEM_SK" as string))) > 4
  ),
  young as (
  SELECT
      sum("vacuous"."store_quantity" * "vacuous"."store_sales_price") as "_best_customers_cust_total"
  FROM
      "vacuous"
  GROUP BY
      "vacuous"."store_customer_id"),
  concerned as (
  SELECT
      "vacuous"."store_customer_id" as "_best_customers_cust_id",
      sum("vacuous"."store_quantity" * "vacuous"."store_sales_price") as "_best_customers_cust_total"
  FROM
      "vacuous"
  GROUP BY
      1),
  yummy as (
  SELECT
      "abundant"."_frequent_item_prefixes_desc_prefix" as "_frequent_item_prefixes_desc_prefix"
  FROM
      "abundant"),
  sparkling as (
  SELECT
      max("young"."_best_customers_cust_total") as "_virt_agg_max_4218174884127360"
  FROM
      "young"),
  juicy as (
  SELECT
      "yummy"."_frequent_item_prefixes_desc_prefix" as "frequent_item_prefixes_desc_prefix"
  FROM
      "yummy"),
  abhorrent as (
  SELECT
      "concerned"."_best_customers_cust_id" as "_best_customers_cust_id"
  FROM
      "concerned"
      INNER JOIN "sparkling" on 1=1
  WHERE
      "concerned"."_best_customers_cust_total" > 0.5 * "sparkling"."_virt_agg_max_4218174884127360"
  ),
  sweltering as (
  SELECT
      "abhorrent"."_best_customers_cust_id" as "best_customers_cust_id"
  FROM
      "abhorrent"),
  kaput as (
  SELECT
      "web_web_sales"."WS_BILL_CUSTOMER_SK" as "web_billing_customer_id",
      "web_web_sales"."WS_ITEM_SK" as "web_item_id",
      "web_web_sales"."WS_LIST_PRICE" as "web_list_price",
      "web_web_sales"."WS_QUANTITY" as "web_quantity",
      "web_web_sales"."WS_SOLD_DATE_SK" as "web_date_id"
  FROM
      "web_sales" as "web_web_sales"
  WHERE
      "web_web_sales"."WS_ITEM_SK" in (select juicy."frequent_item_prefixes_desc_prefix" from juicy where juicy."frequent_item_prefixes_desc_prefix" is not null) and "web_web_sales"."WS_BILL_CUSTOMER_SK" in (select sweltering."best_customers_cust_id" from sweltering where sweltering."best_customers_cust_id" is not null)

  GROUP BY
      1,
      2,
      3,
      4,
      5),
  highfalutin as (
  SELECT
      "catalog_catalog_sales"."CS_BILL_CUSTOMER_SK" as "catalog_bill_customer_id",
      "catalog_catalog_sales"."CS_ITEM_SK" as "catalog_item_id",
      "catalog_catalog_sales"."CS_LIST_PRICE" as "catalog_list_price",
      "catalog_catalog_sales"."CS_QUANTITY" as "catalog_quantity",
      "catalog_catalog_sales"."CS_SOLD_DATE_SK" as "catalog_date_id"
  FROM
      "catalog_sales" as "catalog_catalog_sales"
  WHERE
      "catalog_catalog_sales"."CS_ITEM_SK" in (select juicy."frequent_item_prefixes_desc_prefix" from juicy where juicy."frequent_item_prefixes_desc_prefix" is not null) and "catalog_catalog_sales"."CS_BILL_CUSTOMER_SK" in (select sweltering."best_customers_cust_id" from sweltering where sweltering."best_customers_cust_id" is not null)

  GROUP BY
      1,
      2,
      3,
      4,
      5),
  busy as (
  SELECT
      "kaput"."web_quantity" * "kaput"."web_list_price" as "___tvf_arm_1_line_total",
      "web_billing_customer_customers"."C_FIRST_NAME" as "___tvf_arm_1_first_name",
      "web_billing_customer_customers"."C_LAST_NAME" as "___tvf_arm_1_last_name"
  FROM
      "kaput"
      INNER JOIN "date_dim" as "web_date_date" on "kaput"."web_date_id" = "web_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "customer" as "web_billing_customer_customers" on "kaput"."web_billing_customer_id" = "web_billing_customer_customers"."C_CUSTOMER_SK"
  WHERE
      "web_date_date"."D_YEAR" = 2000 and "web_date_date"."D_MOY" = 2

  GROUP BY
      1,
      2,
      3,
      "kaput"."web_item_id"),
  late as (
  SELECT
      "catalog_bill_customer_customers"."C_FIRST_NAME" as "___tvf_arm_0_first_name",
      "catalog_bill_customer_customers"."C_LAST_NAME" as "___tvf_arm_0_last_name",
      "highfalutin"."catalog_quantity" * "highfalutin"."catalog_list_price" as "___tvf_arm_0_line_total"
  FROM
      "highfalutin"
      INNER JOIN "date_dim" as "catalog_date_date" on "highfalutin"."catalog_date_id" = "catalog_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "customer" as "catalog_bill_customer_customers" on "highfalutin"."catalog_bill_customer_id" = "catalog_bill_customer_customers"."C_CUSTOMER_SK"
  WHERE
      "catalog_date_date"."D_YEAR" = 2000 and "catalog_date_date"."D_MOY" = 2

  GROUP BY
      1,
      2,
      3,
      "highfalutin"."catalog_item_id"),
  protective as (
  SELECT
      "late"."___tvf_arm_0_last_name" as "_combined_sales_last_name",
      "late"."___tvf_arm_0_first_name" as "_combined_sales_first_name",
      "late"."___tvf_arm_0_line_total" as "_combined_sales_line_total"
  FROM
      "late"
  UNION ALL
  SELECT
      "busy"."___tvf_arm_1_last_name" as "_combined_sales_last_name",
      "busy"."___tvf_arm_1_first_name" as "_combined_sales_first_name",
      "busy"."___tvf_arm_1_line_total" as "_combined_sales_line_total"
  FROM
      "busy"),
  premium as (
  SELECT
      "protective"."_combined_sales_first_name" as "combined_sales_first_name",
      "protective"."_combined_sales_last_name" as "combined_sales_last_name",
      "protective"."_combined_sales_line_total" as "combined_sales_line_total"
  FROM
      "protective"
  GROUP BY
      1,
      2,
      3)
  SELECT
      "premium"."combined_sales_last_name" as "combined_sales_last_name",
      "premium"."combined_sales_first_name" as "combined_sales_first_name",
      sum("premium"."combined_sales_line_total") as "total_sales"
  FROM
      "premium"
  GROUP BY
      1,
      2
  ORDER BY
      "premium"."combined_sales_last_name" asc,
      "premium"."combined_sales_first_name" asc,
      "total_sales" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy file read query29.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 22). The requested concepts split into 2 disconnected subgraphs: {cat_ret_item.sum_refund_amount}; {cat_sales_item.item_id, cat_sales_item.sum_ext_list_price}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query77.preql`

  ```text
  Syntax error in query77.preql: ORDER BY contains aggregate `grouping(all_sales.channel)` (line 3), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(all_sales.channel) as g order by g desc`.
  ```
- `trilogy file read query77.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query80.preql`

  ```text
  Syntax error in query80.preql: ORDER BY contains aggregate `grouping(combined.channel)` (line 44), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(combined.channel) as g order by g desc`.
  ```
- `trilogy file read query80.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query83.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.reason select reason.desc, reason.id limit 10;`

  ```text
  Syntax error in stdin: 2 undefined concept references; fix all before re-running:
    - reason.desc (line 2, col 8, in SELECT); did you mean: desc, reason.id?
    - reason.id (line 2, col 21, in SELECT); did you mean: id, reason.desc?
  ```
- `trilogy file read query93.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy file write query02.preql --content import raw.all_sales as sales;

rowset cur_sales <-
    where sales.channel in ('WEB', 'CATALOG')
    select
     … distinct sales.date.week_seq
    where sales.channel in ('WEB', 'CATALOG') and sales.date.year = 2001
)
order by cur_sales.week_seq asc nulls first;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy does not support subqueries - joins are auto-resolved from dotted paths. To filter on a value that lives on a related dimension, reference its dot-path directly. Example: instead of `where ss.store_id in (select store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...k where cur_sales.week_seq in ??? (     select distinct sales.da...

  Write stats: received 2013 chars / 2013 bytes; tail: …'e.year = 2001\\n)\\norder by cur_sales.week_seq asc nulls first;'.
  ```
- `trilogy file write query02.preql --content import raw.catalog_sales as catalog;
import raw.web_sales as web;

# Current (2001) sales by week_seq and day_of_w…in cur_sales.week_seq + 53 = ftr_sales.week_seq
inner join cur_sales.day_of_week = ftr_sales.day_of_week
order by cur_sales.week_seq asc nulls first;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
    --> 11:5
     |
  11 |     union(
     |     ^---
     |
     = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
   catalog.date.year = 2001     ??? union(         select

  Write stats: received 2297 chars / 2297 bytes; tail: …'les.day_of_week\\norder by cur_sales.week_seq asc nulls first;'.
  ```
- `trilogy file write query02.preql --content import raw.all_sales as sales;

# Weekly sales by day_of_week for WEB and CATALOG across ALL years
where sales.cha… fri_ratio,
    round(sat_sales / nullif(ftr_sat, 0), 2) as sat_ratio
having
    sales.date.year = 2001
order by sales.date.week_seq asc nulls first;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block - they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
   sales aggregated by week_seq ??? auto sun_sales <- sum(sales.ex...

  Write stats: received 2102 chars / 2102 bytes; tail: …'te.year = 2001\\norder by sales.date.week_seq asc nulls first;'.
  ```
- `trilogy run -`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...w.all_sales as sales;  select ??? distinct     sales.date.week_s...
  ```
- `trilogy file write query10.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_sale…education_status,
    purchase_estimate,
    credit_rating,
    dependent_count,
    employed_dependent_count,
    college_dependent_count
limit 100;`

  ```text
  refused to write 'query10.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:26
    |
  7 | auto target_counties <- {'Rush County', 'Toole County', 'Jefferson County', 'Dona Ana County', 'La Porte County'};
    |                          ^---
    |
    = expected null_lit or MULTILINE_STRING
  Location:
  ...list auto target_counties <- { ??? 'Rush County', 'Toole County',...

  Write stats: received 2430 chars / 2430 bytes; tail: …'oyed_dependent_count,\\n    college_dependent_count\\nlimit 100;'.
  ```
- `trilogy file write query10.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_sale…education_status,
    purchase_estimate,
    credit_rating,
    dependent_count,
    employed_dependent_count,
    college_dependent_count
limit 100;`

  ```text
  refused to write 'query10.preql': not syntactically valid Trilogy.

  Parse error:
   --> 9:42
    |
  9 |   and store_sales.date.month_of_year in {1, 2, 3, 4}
    |                                          ^---
    |
    = expected FLOAT_LITERAL_PART, BOOL_KW, null_lit, or MULTILINE_STRING
  Location:
  ..._sales.date.month_of_year in { ??? 1, 2, 3, 4} select     store_s...

  Write stats: received 2416 chars / 2416 bytes; tail: …'oyed_dependent_count,\\n    college_dependent_count\\nlimit 100;'.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as sales;

# Step 1: Identify (brand, class, category) combos appearing in all 3 channels 199…ass_id) asc,
    sales.item.class_id asc nulls first,
    grouping(sales.item.category_id) asc,
    sales.item.category_id asc nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
    --> 27:20
     |
  27 |     in_all_channels(sales.item.brand_id, sales.item.class_id, sales.item.category_id)
     |                    ^---
     |
     = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...ount where     in_all_channels ??? (sales.item.brand_id, sales.it...

  Write stats: received 1713 chars / 1713 bytes; tail: …') asc,\\n    sales.item.category_id asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query64.preql -e -c import raw.store_sales as ss;
import raw.catalog_returns as cr;
import raw.catalog_sales as cs;
import raw.store_retur…ines,
    sum(ss.ext_wholesale_cost) as sum_wholesale_cost,
    sum(ss.ext_list_price) as sum_list_price,
    sum(ss.coupon_amt) as sum_coupon_amt
;
`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [220]: Filter or stray clause after a `join`? A query-scoped join `inner|left|full join <a> = <b>` takes only key equalities - to join on multiple keys, chain `= c` (one equivalence group) or separate distinct groups with `and` (`a = b and c = d`); STACK another `join` clause for a different join type. Note `and` joins KEY EQUALITIES only, not filters. Joins go right after the `select` list (preferred, SQL-like) or before `select`; the order is `where` -> `select` <cols> -> join(s) -> `having` -> `order by` -> `limit`. Filter input rows in `where` (before `select`); filter a joined or aggregated RESULT in `having` (select the field, hide it with a leading `--`). Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
   join ss.item.id = sr.item.id ??? inner join ss.ticket_number =

  Write stats: received 2099 chars / 2099 bytes; tail: …' sum_list_price,\\n    sum(ss.coupon_amt) as sum_coupon_amt\\n;\\n'.
  ```
- `trilogy file write query64.preql -e -c import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.store_retur…cnt
order by y1999.product_name asc, y1999.store_name asc, y2000.line_cnt asc, y1999.sum_wholesale_cost asc, y2000.sum_wholesale_cost asc
limit 100;
`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [220]: Filter or stray clause after a `join`? A query-scoped join `inner|left|full join <a> = <b>` takes only key equalities - to join on multiple keys, chain `= c` (one equivalence group) or separate distinct groups with `and` (`a = b and c = d`); STACK another `join` clause for a different join type. Note `and` joins KEY EQUALITIES only, not filters. Joins go right after the `select` list (preferred, SQL-like) or before `select`; the order is `where` -> `select` <cols> -> join(s) -> `having` -> `order by` -> `limit`. Filter input rows in `where` (before `select`); filter a joined or aggregated RESULT in `having` (select the field, hide it with a leading `--`). Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
   join ss.item.id = sr.item.id ??? inner join ss.ticket_number =

  Write stats: received 4961 chars / 4961 bytes; tail: …'wholesale_cost asc, y2000.sum_wholesale_cost asc\\nlimit 100;\\n'.
  ```
- `trilogy file write query64.preql -e -c import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.store_retur…cnt
order by y1999.product_name asc, y1999.store_name asc, y2000.line_cnt asc, y1999.sum_wholesale_cost asc, y2000.sum_wholesale_cost asc
limit 100;
`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [220]: Filter or stray clause after a `join`? A query-scoped join `inner|left|full join <a> = <b>` takes only key equalities - to join on multiple keys, chain `= c` (one equivalence group) or separate distinct groups with `and` (`a = b and c = d`); STACK another `join` clause for a different join type. Note `and` joins KEY EQUALITIES only, not filters. Joins go right after the `select` list (preferred, SQL-like) or before `select`; the order is `where` -> `select` <cols> -> join(s) -> `having` -> `order by` -> `limit`. Filter input rows in `where` (before `select`); filter a joined or aggregated RESULT in `having` (select the field, hide it with a leading `--`). Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...ket_number = sr.ticket_number ??? inner join ss.item.id = sr.ite...

  Write stats: received 4838 chars / 4838 bytes; tail: …'wholesale_cost asc, y2000.sum_wholesale_cost asc\\nlimit 100;\\n'.
  ```
- `trilogy file write query64.preql -e -c import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.store_retur…cnt
order by y1999.product_name asc, y1999.store_name asc, y2000.line_cnt asc, y1999.sum_wholesale_cost asc, y2000.sum_wholesale_cost asc
limit 100;
`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [220]: Filter or stray clause after a `join`? A query-scoped join `inner|left|full join <a> = <b>` takes only key equalities - to join on multiple keys, chain `= c` (one equivalence group) or separate distinct groups with `and` (`a = b and c = d`); STACK another `join` clause for a different join type. Note `and` joins KEY EQUALITIES only, not filters. Joins go right after the `select` list (preferred, SQL-like) or before `select`; the order is `where` -> `select` <cols> -> join(s) -> `having` -> `order by` -> `limit`. Filter input rows in `where` (before `select`); filter a joined or aggregated RESULT in `having` (select the field, hide it with a leading `--`). Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...filtered rowset store_base <- ??? inner join ss.ticket_number =

  Write stats: received 4824 chars / 4824 bytes; tail: …'wholesale_cost asc, y2000.sum_wholesale_cost asc\\nlimit 100;\\n'.
  ```
- `trilogy file write query64.preql -e -c import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

# Step 1: Compute cat…cnt
order by y1999.product_name asc, y1999.store_name asc, y2000.line_cnt asc, y1999.sum_wholesale_cost asc, y2000.sum_wholesale_cost asc
limit 100;
`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [220]: Filter or stray clause after a `join`? A query-scoped join `inner|left|full join <a> = <b>` takes only key equalities - to join on multiple keys, chain `= c` (one equivalence group) or separate distinct groups with `and` (`a = b and c = d`); STACK another `join` clause for a different join type. Note `and` joins KEY EQUALITIES only, not filters. Joins go right after the `select` list (preferred, SQL-like) or before `select`; the order is `where` -> `select` <cols> -> join(s) -> `having` -> `order by` -> `limit`. Filter input rows in `where` (before `select`); filter a joined or aggregated RESULT in `having` (select the field, hide it with a leading `--`). Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...tion rowset eligible_items <- ??? inner join cat_sales_item.item...

  Write stats: received 5123 chars / 5123 bytes; tail: …'wholesale_cost asc, y2000.sum_wholesale_cost asc\\nlimit 100;\\n'.
  ```
- `trilogy file write query64.preql -e -c import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

# Catalog sales ext_l…cnt
order by y1999.product_name asc, y1999.store_name asc, y2000.line_cnt asc, y1999.sum_wholesale_cost asc, y2000.sum_wholesale_cost asc
limit 100;
`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [220]: Filter or stray clause after a `join`? A query-scoped join `inner|left|full join <a> = <b>` takes only key equalities - to join on multiple keys, chain `= c` (one equivalence group) or separate distinct groups with `and` (`a = b and c = d`); STACK another `join` clause for a different join type. Note `and` joins KEY EQUALITIES only, not filters. Joins go right after the `select` list (preferred, SQL-like) or before `select`; the order is `where` -> `select` <cols> -> join(s) -> `having` -> `order by` -> `limit`. Filter input rows in `where` (before `select`); filter a joined or aggregated RESULT in `having` (select the field, hide it with a leading `--`). Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...dition with eligible_items as ??? inner join cat_sales_item.item...

  Write stats: received 4944 chars / 4944 bytes; tail: …'wholesale_cost asc, y2000.sum_wholesale_cost asc\\nlimit 100;\\n'.
  ```
- `trilogy file write query64.preql -e -c import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

# Catalog sales ext_l…cnt
order by y1999.product_name asc, y1999.store_name asc, y2000.line_cnt asc, y1999.sum_wholesale_cost asc, y2000.sum_wholesale_cost asc
limit 100;
`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy does not support subqueries - joins are auto-resolved from dotted paths. To filter on a value that lives on a related dimension, reference its dot-path directly. Example: instead of `where ss.store_id in (select store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...em.id, cr.order_number) in    ??? (select cs.item.id, cs.order_n...

  Write stats: received 4953 chars / 4953 bytes; tail: …'wholesale_cost asc, y2000.sum_wholesale_cost asc\\nlimit 100;\\n'.
  ```
- `trilogy file write query80.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

with combined as union(…nnel asc nulls first,
         grouping(combined.channel, combined.outlet_id) asc nulls first,
         combined.outlet_id asc nulls first
limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
    --> 47:40
     |
  47 |     case when grouping(combined.channel, combined.outlet_id) = 1
     |                                        ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...when grouping(combined.channel ??? , combined.outlet_id) = 1

  Write stats: received 2444 chars / 2444 bytes; tail: …'irst,\\n         combined.outlet_id asc nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.all_sales:s select s.item.text_id, s.channel, count(s.is_returned) as return_rows, sum(s.return_quantity) as return_qty where s.retu…(5244, 5257, 5264) and s.is_returned is not null group by s.item.text_id, s.channel having count(s.is_returned) > 0 order by s.item.text_id limit 30;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...and s.is_returned is not null ??? group by s.item.text_id, s.cha...
  ```
- `trilogy run --import raw.store_sales:ss select ss.return_reason.desc as rdesc, ss.return_reason.id as rid limit 10 where ss.return_reason.id is not null;`

  ```text
  Syntax error in stdin:  --> 2:67
    |
  2 | select ss.return_reason.desc as rdesc, ss.return_reason.id as rid limit 10 where ss.return_reason.id is not null;
    |                                                                   ^---
    |
    = expected metadata, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...c, ss.return_reason.id as rid ??? limit 10 where ss.return_reaso...
  ```

### `syntax-missing-alias`

- `trilogy run --import raw.store_sales:store_sales select store_sales.return_reason.desc, store_sales.return_reason.id limit 10 where store_sales.return_reason.id is not null;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `store_sales.return_reason.id as store_sales_return_reason_id`
  Location:
   store_sales.return_reason.id ??? limit 10 where store_sales.ret...
  ```
