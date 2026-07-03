# Trilogy failure analysis — 20260703-212501

- Run `20260703-212501` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 255 | failed: 36 (14%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 17 | 47% |
| `syntax-parse` | 12 | 33% |
| `planner-recursion` | 5 | 14% |
| `cli-misuse` | 2 | 6% |

## Detail

### `other`

- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: Undefined concept: local.sale_agg. Suggestions: ['sale_agg.ext_sales']
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: ORDER BY references 'sale_agg.all.channel', a measure at a finer grain (Grain<return_agg.all.channel>) than the select grain (Grain<local.channel_label,local.entity_id,local.net,local.total_ext_sales,local.total_returns>); it has no single value per output row. Project it (prefix with `--` to keep it out of the rows) and order by that alias instead.
  ```
- `trilogy run -e -c --import raw/all_sales:all -- select all.channel, all.channel_dim_text_id, sum(all.ext_sales_price) as ext_sales by rollup(all.channel, all.channel_dim_text_id) order by all.channel asc nulls first, all.channel_dim_text_id asc nulls first limit 10;`

  ```text
  Environment variable must be in KEY=VALUE format or be a path to an existing env file: -c
  ```
- `trilogy database describe store`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: 2 undefined concept references; fix all before re-running:
    - y1999.grouped_sales.sale_year (line 113, col 5, in SELECT); did you mean: grouped_sales.sale_year, y2000.grouped_sales.sale_year, _grouped_sales_sale_year?
    - y2000.grouped_sales.sale_year (line 123, col 5, in SELECT); did you mean: grouped_sales.sale_year, y1999.grouped_sales.sale_year, _grouped_sales_sale_year?
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 27). The requested concepts split into 2 disconnected subgraphs: {cs.item.id, cat_ext_list_by_item}; {cat_refund_by_item}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 38). The requested concepts split into 2 disconnected subgraphs: {cat_refund_by_item.refund_total}; {cs.item.id, cat_price_by_item}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 48). The requested concepts split into 2 disconnected subgraphs: {sr.item.id, sr.ticket_number}; {ss.item.id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 80 column 12 (char 6707). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query67.preql`

  ```text
  Unexpected error in query67.preql: (_duckdb.BinderException) Binder Error: GROUPING statement cannot be used without groups

  LINE 60:     grouping("questionable"."rollup_data_ss_item_category")...
               ^
  [SQL:
  WITH
  thoughtful as (
  SELECT
      "ss_date_date"."D_MOY" as "ss_date_month_of_year",
      "ss_date_date"."D_QOY" as "ss_date_quarter",
      "ss_date_date"."D_YEAR" as "ss_date_year",
      "ss_item_items"."I_BRAND" as "ss_item_brand_name",
      "ss_item_items"."I_CATEGORY" as "ss_item_category",
      "ss_item_items"."I_CLASS" as "ss_item_class",
      "ss_item_items"."I_PRODUCT_NAME" as "ss_item_product_name",
      "ss_store_sales"."SS_EXT_SALES_PRICE" as "ss_ext_sales_price",
      "ss_store_store"."S_STORE_ID" as "ss_store_text_id"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
      INNER JOIN "item" as "ss_item_items" on "ss_store_sales"."SS_ITEM_SK" = "ss_item_items"."I_ITEM_SK"
      LEFT OUTER JOIN "store" as "ss_store_store" on "ss_store_sales"."SS_STORE_SK" = "ss_store_store"."S_STORE_SK"
  WHERE
      "ss_date_date"."D_YEAR" = 2000
  ),
  cooperative as (
  SELECT
      "thoughtful"."ss_date_month_of_year" as "ss_date_month_of_year",
      "thoughtful"."ss_date_quarter" as "ss_date_quarter",
      "thoughtful"."ss_date_year" as "ss_date_year",
      "thoughtful"."ss_item_brand_name" as "ss_item_brand_name",
      "thoughtful"."ss_item_category" as "ss_item_category",
      "thoughtful"."ss_item_class" as "ss_item_class",
      "thoughtful"."ss_item_product_name" as "ss_item_product_name",
      "thoughtful"."ss_store_text_id" as "_rollup_data_store_code",
      sum(coalesce("thoughtful"."ss_ext_sales_price",0)) as "_rollup_data_summed_sales"
  FROM
      "thoughtful"
  GROUP BY
      ROLLUP (5, 6, 4, 7, 3, 2, 1, 8)),
  questionable as (
  SELECT
      "cooperative"."_rollup_data_store_code" as "rollup_data_store_code",
      "cooperative"."_rollup_data_summed_sales" as "rollup_data_summed_sales",
      "cooperative"."ss_date_month_of_year" as "rollup_data_ss_date_month_of_year",
      "cooperative"."ss_date_quarter" as "rollup_data_ss_date_quarter",
      "cooperative"."ss_date_year" as "rollup_data_ss_date_year",
      "cooperative"."ss_item_brand_name" as "rollup_data_ss_item_brand_name",
      "cooperative"."ss_item_category" as "rollup_data_ss_item_category",
      "cooperative"."ss_item_class" as "rollup_data_ss_item_class",
      "cooperative"."ss_item_product_name" as "rollup_data_ss_item_product_name"
  FROM
      "cooperative"),
  abundant as (
  SELECT
      "questionable"."rollup_data_ss_date_month_of_year" as "rollup_data_ss_date_month_of_year",
      "questionable"."rollup_data_ss_date_quarter" as "rollup_data_ss_date_quarter",
      "questionable"."rollup_data_ss_date_year" as "rollup_data_ss_date_year",
      "questionable"."rollup_data_ss_item_brand_name" as "rollup_data_ss_item_brand_name",
      "questionable"."rollup_data_ss_item_category" as "rollup_data_ss_item_category",
      "questionable"."rollup_data_ss_item_class" as "rollup_data_ss_item_class",
      "questionable"."rollup_data_ss_item_product_name" as "rollup_data_ss_item_product_name",
      "questionable"."rollup_data_store_code" as "rollup_data_store_code",
      grouping("questionable"."rollup_data_ss_item_category") as "g_cat"
  FROM
      "questionable"),
  uneven as (
  SELECT
      "questionable"."rollup_data_ss_date_month_of_year" as "rollup_data_ss_date_month_of_year",
      "questionable"."rollup_data_ss_date_quarter" as "rollup_data_ss_date_quarter",
      "questionable"."rollup_data_ss_date_year" as "rollup_data_ss_date_year",
      "questionable"."rollup_data_ss_item_brand_name" as "rollup_data_ss_item_brand_name",
      "questionable"."rollup_data_ss_item_category" as "rollup_data_ss_item_category",
      "questionable"."rollup_data_ss_item_class" as "rollup_data_ss_item_class",
      "questionable"."rollup_data_ss_item_product_name" as "rollup_data_ss_item_product_name",
      "questionable"."rollup_data_store_code" as "rollup_data_store_code",
      "questionable"."rollup_data_summed_sales" as "rollup_data_summed_sales",
      rank() over (partition by CASE
  	WHEN "abundant"."g_cat" = 1 THEN '~~GRAND_TOTAL~~'
  	ELSE coalesce("questionable"."rollup_data_ss_item_category",'~~NULL_CATEGORY~~')
  	END order by "questionable"."rollup_data_summed_sales" desc ) as "within_category_rank"
  FROM
      "abundant"
      INNER JOIN "questionable" on "abundant"."rollup_data_ss_date_month_of_year" = "questionable"."rollup_data_ss_date_month_of_year" AND "abundant"."rollup_data_ss_date_quarter" = "questionable"."rollup_data_ss_date_quarter" AND "abundant"."rollup_data_ss_date_year" = "questionable"."rollup_data_ss_date_year" AND "abundant"."rollup_data_ss_item_brand_name" = "questionable"."rollup_data_ss_item_brand_name" AND "abundant"."rollup_data_ss_item_category" is not distinct from "questionable"."rollup_data_ss_item_category" AND "abundant"."rollup_data_ss_item_class" is not distinct from "questionable"."rollup_data_ss_item_class" AND "abundant"."rollup_data_ss_item_product_name" = "questionable"."rollup_data_ss_item_product_name" AND "abundant"."rollup_data_store_code" = "questionable"."rollup_data_store_code")
  SELECT
      "uneven"."rollup_data_ss_item_category" as "rollup_data_ss_item_category",
      "uneven"."rollup_data_ss_item_class" as "rollup_data_ss_item_class",
      "uneven"."rollup_data_ss_item_brand_name" as "rollup_data_ss_item_brand_name",
      "uneven"."rollup_data_ss_item_product_name" as "rollup_data_ss_item_product_name",
      "uneven"."rollup_data_ss_date_year" as "rollup_data_ss_date_year",
      "uneven"."rollup_data_ss_date_quarter" as "rollup_data_ss_date_quarter",
      "uneven"."rollup_data_ss_date_month_of_year" as "rollup_data_ss_date_month_of_year",
      "uneven"."rollup_data_store_code" as "rollup_data_store_code",
      "uneven"."rollup_data_summed_sales" as "rollup_data_summed_sales",
      "uneven"."within_category_rank" as "within_category_rank"
  FROM
      "uneven"
  WHERE
      "uneven"."within_category_rank" <= 100

  ORDER BY
      "uneven"."rollup_data_ss_item_category" asc nulls first,
      "uneven"."rollup_data_ss_item_class" asc nulls first,
      "uneven"."rollup_data_ss_item_brand_name" asc nulls first,
      "uneven"."rollup_data_ss_item_product_name" asc nulls first,
      "uneven"."rollup_data_ss_date_year" asc nulls first,
      "uneven"."rollup_data_ss_date_quarter" asc nulls first,
      "uneven"."rollup_data_ss_date_month_of_year" asc nulls first,
      "uneven"."rollup_data_store_code" asc nulls first,
      "uneven"."rollup_data_summed_sales" asc nulls first,
      "uneven"."within_category_rank" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query67.preql`

  ```text
  Unexpected error in query67.preql: (_duckdb.BinderException) Binder Error: GROUPING statement cannot be used without groups

  LINE 60:     grouping("questionable"."rollup_data_ss_item_category")...
               ^
  [SQL:
  WITH
  thoughtful as (
  SELECT
      "ss_date_date"."D_MOY" as "ss_date_month_of_year",
      "ss_date_date"."D_QOY" as "ss_date_quarter",
      "ss_date_date"."D_YEAR" as "ss_date_year",
      "ss_item_items"."I_BRAND" as "ss_item_brand_name",
      "ss_item_items"."I_CATEGORY" as "ss_item_category",
      "ss_item_items"."I_CLASS" as "ss_item_class",
      "ss_item_items"."I_PRODUCT_NAME" as "ss_item_product_name",
      "ss_store_sales"."SS_EXT_SALES_PRICE" as "ss_ext_sales_price",
      "ss_store_store"."S_STORE_ID" as "ss_store_text_id"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
      INNER JOIN "item" as "ss_item_items" on "ss_store_sales"."SS_ITEM_SK" = "ss_item_items"."I_ITEM_SK"
      LEFT OUTER JOIN "store" as "ss_store_store" on "ss_store_sales"."SS_STORE_SK" = "ss_store_store"."S_STORE_SK"
  WHERE
      "ss_date_date"."D_YEAR" = 2000
  ),
  cooperative as (
  SELECT
      "thoughtful"."ss_date_month_of_year" as "ss_date_month_of_year",
      "thoughtful"."ss_date_quarter" as "ss_date_quarter",
      "thoughtful"."ss_date_year" as "ss_date_year",
      "thoughtful"."ss_item_brand_name" as "ss_item_brand_name",
      "thoughtful"."ss_item_category" as "ss_item_category",
      "thoughtful"."ss_item_class" as "ss_item_class",
      "thoughtful"."ss_item_product_name" as "ss_item_product_name",
      "thoughtful"."ss_store_text_id" as "_rollup_data_store_code",
      sum(coalesce("thoughtful"."ss_ext_sales_price",0)) as "_rollup_data_summed_sales"
  FROM
      "thoughtful"
  GROUP BY
      ROLLUP (5, 6, 4, 7, 3, 2, 1, 8)),
  questionable as (
  SELECT
      "cooperative"."_rollup_data_store_code" as "rollup_data_store_code",
      "cooperative"."_rollup_data_summed_sales" as "rollup_data_summed_sales",
      "cooperative"."ss_date_month_of_year" as "rollup_data_ss_date_month_of_year",
      "cooperative"."ss_date_quarter" as "rollup_data_ss_date_quarter",
      "cooperative"."ss_date_year" as "rollup_data_ss_date_year",
      "cooperative"."ss_item_brand_name" as "rollup_data_ss_item_brand_name",
      "cooperative"."ss_item_category" as "rollup_data_ss_item_category",
      "cooperative"."ss_item_class" as "rollup_data_ss_item_class",
      "cooperative"."ss_item_product_name" as "rollup_data_ss_item_product_name"
  FROM
      "cooperative"),
  abundant as (
  SELECT
      "questionable"."rollup_data_ss_date_month_of_year" as "rollup_data_ss_date_month_of_year",
      "questionable"."rollup_data_ss_date_quarter" as "rollup_data_ss_date_quarter",
      "questionable"."rollup_data_ss_date_year" as "rollup_data_ss_date_year",
      "questionable"."rollup_data_ss_item_brand_name" as "rollup_data_ss_item_brand_name",
      "questionable"."rollup_data_ss_item_category" as "rollup_data_ss_item_category",
      "questionable"."rollup_data_ss_item_class" as "rollup_data_ss_item_class",
      "questionable"."rollup_data_ss_item_product_name" as "rollup_data_ss_item_product_name",
      "questionable"."rollup_data_store_code" as "rollup_data_store_code",
      grouping("questionable"."rollup_data_ss_item_category") as "_virt_agg_grouping_9507444738240817"
  FROM
      "questionable")
  SELECT
      "questionable"."rollup_data_ss_item_category" as "rollup_data_ss_item_category",
      "questionable"."rollup_data_ss_item_class" as "rollup_data_ss_item_class",
      "questionable"."rollup_data_ss_item_brand_name" as "rollup_data_ss_item_brand_name",
      "questionable"."rollup_data_ss_item_product_name" as "rollup_data_ss_item_product_name",
      "questionable"."rollup_data_ss_date_year" as "rollup_data_ss_date_year",
      "questionable"."rollup_data_ss_date_quarter" as "rollup_data_ss_date_quarter",
      "questionable"."rollup_data_ss_date_month_of_year" as "rollup_data_ss_date_month_of_year",
      "questionable"."rollup_data_store_code" as "rollup_data_store_code",
      "questionable"."rollup_data_summed_sales" as "rollup_data_summed_sales",
      rank() over (partition by CASE
  	WHEN "abundant"."_virt_agg_grouping_9507444738240817" = 1 THEN '~~GRAND_TOTAL~~'
  	ELSE coalesce("questionable"."rollup_data_ss_item_category",'~~NULL_CATEGORY~~')
  	END order by "questionable"."rollup_data_summed_sales" desc ) as "within_category_rank"
  FROM
      "abundant"
      INNER JOIN "questionable" on "abundant"."rollup_data_ss_date_month_of_year" = "questionable"."rollup_data_ss_date_month_of_year" AND "abundant"."rollup_data_ss_date_quarter" = "questionable"."rollup_data_ss_date_quarter" AND "abundant"."rollup_data_ss_date_year" = "questionable"."rollup_data_ss_date_year" AND "abundant"."rollup_data_ss_item_brand_name" = "questionable"."rollup_data_ss_item_brand_name" AND "abundant"."rollup_data_ss_item_category" is not distinct from "questionable"."rollup_data_ss_item_category" AND "abundant"."rollup_data_ss_item_class" is not distinct from "questionable"."rollup_data_ss_item_class" AND "abundant"."rollup_data_ss_item_product_name" = "questionable"."rollup_data_ss_item_product_name" AND "abundant"."rollup_data_store_code" = "questionable"."rollup_data_store_code"
  ORDER BY
      "questionable"."rollup_data_ss_item_category" asc nulls first,
      "questionable"."rollup_data_ss_item_class" asc nulls first,
      "questionable"."rollup_data_ss_item_brand_name" asc nulls first,
      "questionable"."rollup_data_ss_item_product_name" asc nulls first,
      "questionable"."rollup_data_ss_date_year" asc nulls first,
      "questionable"."rollup_data_ss_date_quarter" asc nulls first,
      "questionable"."rollup_data_ss_date_month_of_year" asc nulls first,
      "questionable"."rollup_data_store_code" asc nulls first,
      "questionable"."rollup_data_summed_sales" asc nulls first,
      "within_category_rank" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query87.preql`

  ```text
  Resolution error in query87.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 18). The requested concepts split into 3 disconnected subgraphs: {in_catalog}; {in_store}; {in_web}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read query87.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy run --import raw/all_sales:all select distinct all.channel, all.channel_dim_text_id limit 10;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
   raw.all_sales as all; select ??? distinct all.channel, all.chan...
  ```
- `trilogy run --import raw/all_sales:all select count(all.channel_dim_id) as sales_rows, count(all.return_channel_dim_id) as return_rows, count(*) as total where all.return_channel_dim_id is not null and all.channel_dim_id is null;`

  ```text
  Syntax error in stdin: Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...annel_dim_id) as return_rows, ??? count(*) as total where all.re...
  ```
- `trilogy run --import raw/all_sales:all select distinct all.channel limit 10;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
   raw.all_sales as all; select ??? distinct all.channel limit 10;...
  ```
- `trilogy run --import raw/all_sales:all select count(*) cnt;`

  ```text
  Syntax error in stdin: Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
   raw.all_sales as all; select ??? count(*) cnt;
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…der by combined_sales.last_name asc nulls first,
         combined_sales.first_name asc nulls first,
         total_sales asc nulls first
limit 100;
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
    --> 29:37
     |
  29 | property max_lifetime_total numeric <- max(store_customer_totals.lifetime_total);
     |                                     ^---
     |
     = expected concept_nullable_modifier or metadata
  Location:
  ...ty max_lifetime_total numeric ??? <- max(store_customer_totals.l...

  Write stats: received 2937 chars / 2937 bytes; tail: …'ulls first,\\n         total_sales asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy run - --format json`

  ```text
  Syntax error in stdin:   --> 44:40
     |
  44 | union join count(next_year.store_name) as next_count
     |                                        ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, aggregate_over, or window_sql_over
  Location:
  ...n count(next_year.store_name) ??? as next_count # dummy join to
  ```
- `trilogy run - --format json`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...e cnt_by_name_code > 1 select ??? distinct     s.name as store_n...
  ```
- `trilogy run - --format json`

  ```text
  Syntax error in stdin:  --> 6:1
    |
  6 | by s.name, s.text_id
    | ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
     count(s.id) as store_count ??? by s.name, s.text_id ;
  ```
- `trilogy run - --format json`

  ```text
  Syntax error in stdin:  --> 6:1
    |
  6 | by ss.store.name, ss.store.text_id
    | ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...s.ticket_number) as num_sales ??? by ss.store.name, ss.store.tex...
  ```
- `trilogy file write query64.preql --content import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.store_sales as ss;
import raw.store_r…cs.ext_list_price) by ss.item.id as item_cat_list,
    sum(cr.refunded_cash + cr.reversed_charge + cr.store_credit) by cr.item.id as item_cat_refund;`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
    --> 62:42
     |
  62 |     sum(cs.ext_list_price) by ss.item.id as item_cat_list,
     |                                          ^---
     |
     = expected limit, ORDERING_DIRECTION, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, window_sql_over, or OVER_COMPONENT_REF
  Location:
  ...ext_list_price) by ss.item.id ??? as item_cat_list,     sum(cr.r...

  Write stats: received 2633 chars / 2633 bytes; tail: …'_charge + cr.store_credit) by cr.item.id as item_cat_refund;'.
  ```
- `trilogy file write query67.preql --content import raw.store_sales as ss;

# Summed sales = coalesce(ext_sales_price, 0)
auto summed_sales <- sum(coalesce(ss.…of_year asc nulls first,
    ss.store.text_id asc nulls first,
    summed_sales asc nulls first,
    within_category_rank asc nulls first
limit 100;
`

  ```text
  refused to write 'query67.preql': not syntactically valid Trilogy.

  Parse error:
    --> 39:1
     |
  39 | having
     | ^---
     |
     = expected limit or order_by
  Location:
  ...th_of_year, ss.store.text_id) ??? having     within_category_ran...

  Hint: HAVING must come *before* the `by rollup/cube/grouping sets` clause in Trilogy (the reverse of SQL's `GROUP BY ... HAVING`). Reorder to:
      select <cols> having <cond> by rollup (<keys>) order by <cols> limit <n>;

  Write stats: received 1905 chars / 1905 bytes; tail: …' first,\\n    within_category_rank asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query87.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Rowset: unique (last_…alog or web combos
# Use presence flag approach
where
    store_combos.last_name not in catalog_combos.last_name
    or True
select count(*) as test;`

  ```text
  refused to write 'query87.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ....last_name     or True select ??? count(*) as test;

  Write stats: received 992 chars / 992 bytes; tail: …'atalog_combos.last_name\\n    or True\\nselect count(*) as test;'.
  ```

### `planner-recursion`

- `trilogy run query67.preql`

  ```text
  Resolution error in query67.preql: query could not be planned; this is a bug.
  ```
- `trilogy run query67.preql`

  ```text
  Resolution error in query67.preql: query could not be planned; this is a bug.
  ```
- `trilogy run query67.preql`

  ```text
  Resolution error in query67.preql: query could not be planned; this is a bug.
  ```
- `trilogy run query67.preql`

  ```text
  Resolution error in query67.preql: query could not be planned; this is a bug.
  ```
- `trilogy run query67.preql`

  ```text
  Resolution error in query67.preql: query could not be planned; this is a bug.
  ```

### `cli-misuse`

- `trilogy explore raw/sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/sales.preql' does not exist.
  ```
- `trilogy explore raw/sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/sales.preql' does not exist.
  ```
