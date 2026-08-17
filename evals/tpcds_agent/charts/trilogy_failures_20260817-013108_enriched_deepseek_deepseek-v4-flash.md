# Trilogy failure analysis — 20260817-013109

- Run `20260817-013108_enriched_deepseek_deepseek-v4-flash` | `deepseek/deepseek-v4-flash` | sf=1
- `trilogy` calls: 1267 | failed: 79 (6%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `disabled-tool` | 26 | 33% |
| `other` | 20 | 25% |
| `undefined-concept` | 13 | 16% |
| `syntax-parse` | 12 | 15% |
| `join-resolution` | 5 | 6% |
| `cli-misuse` | 3 | 4% |

## Detail

### `disabled-tool`

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
- `trilogy file read raw/catalog_store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/_q04_agent_rowset_union_join.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/address.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe store_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under root/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read answer_1256642804.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read trilogy.toml`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read answer_1256642804.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read answer_1256642804.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read answer_1256642804.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/reason.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read answer_1256642804.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under root/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under root/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read .git/HEAD`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `other`

- `trilogy file write answer_507046194.preql --run`

  ```text
  Resolution error in answer_507046194.preql: WHERE input(s) ['ss.return_store.state'] cannot be related to the query outputs ['ss.return_customer.id', 'ss.return_customer.sk', 'ss.return_store.sk']: no join or merge connects the filter's source to any output-producing source. Add a join/merge relating them, or select a concept from the filter's model.
  ```
- `trilogy file write probe5.preql --run-and-delete`

  ```text
  Resolution error in probe5.preql: WHERE input(s) ['ss.return_store.state'] cannot be related to the query outputs ['ss.return_customer.id', 'ss.return_customer.sk', 'ss.return_store.sk']: no join or merge connects the filter's source to any output-producing source. Add a join/merge relating them, or select a concept from the filter's model.
  ```
- `trilogy file write probe3.preql --run-and-delete`

  ```text
  Syntax error in probe3.preql: ORDER BY references 'cs.sale_date.year', which is not in the SELECT projection (line 3). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --cs.sale_date.year order by cs.sale_date.year asc`.
  ```
- `trilogy file write probe6.preql --run-and-delete`

  ```text
  Unexpected error in probe6.preql: Invalid input concepts to node! ['cat_avg.category'] are missing non-hidden parent nodes; have {'cat_avg.avg_price', 'item.category'} and hidden {'cat_avg.category'} from root {'cat_avg.avg_price', 'item.category'}
  ```
- `trilogy file write probe_mech.preql --run-and-delete --param zips=24128,76232,65084,87816,83926`

  ```text
  Resolution error in probe_mech.preql: Could not resolve condition existence arguments ['local._virt_func_split_4785012549328100']
  ```
- `trilogy file write probe_verify.preql --run-and-delete`

  ```text
  Syntax error in probe_verify.preql: HAVING filters on a dimension outside the SELECT projection, but the select has no grain key to anchor a post-aggregation semijoin (line 17). Move the filter to WHERE to filter before aggregation.
  ```
- `trilogy file write probe10.preql --run-and-delete`

  ```text
  Syntax error in probe10.preql: Cannot compare INTEGER (ref:s.item.sk) and STRING (ref:s.item.id) of different types with operator != in ref:s.item.sk != ref:s.item.id
  ```
- `trilogy file write probe3.preql --run-and-delete`

  ```text
  Allocation failure\n[SQL: \nWITH \nhard as (\nSELECT\n    \"ws_sale_date_date\".\"D_MOY\" as \"ws_sale_date_month_of_year\",\n    \"ws_sale_date_date\".\"D_YEAR\" as \"ws_sale_date_year\",\n    \"ws_web_sales\".\"WS_BILL_CUSTOMER_SK\" as \"ws_billing_customer_sk\",\n    \"ws_web_sales\".\"WS_ITEM_SK\" as \"ws_item_sk\",\n    \"ws_web_sales\".\"WS_ORDER_NUMBER\" as \"ws_order_number\"\nFROM\n    \"web_sales\" as \"ws_web_sales\"\n    INNER JOIN \"date_dim\" as \"ws_sale_date_date\" on \"ws_web_sales\".\"WS_SOLD_DATE_SK\" = \"ws_sale_date_date\".\"D_DATE_SK\"\nWHERE\n    \"ws_sale_date_date\".\
  …
     \"slow\".\"_combined_mx\" as \"combined_mx\",\n    \"slow\".\"_combined_n_best_match\" as \"combined_n_best_match\",\n    \"slow\".\"_combined_n_both\" as \"combined_n_both\",\n    \"slow\".\"_combined_n_freq_match\" as \"combined_n_freq_match\"\nFROM\n    \"slow\")\nSELECT\n    \"spiritual\".\"combined_n_freq_match\" as \"n_freq_match\",\n    \"spiritual\".\"combined_n_best_match\" as \"n_best_match\",\n    \"spiritual\".\"combined_n_both\" as \"n_both\",\n    \"spiritual\".\"combined_mx\" as \"mx\"\nFROM\n    \"spiritual\"]\n(Background on this error at: https://sqlalche.me/e/20/e3q8)"
  }
  ```
- `trilogy file write probe7.preql --run-and-delete`

  ```text
  Resolution error in probe7.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: cs.item.items_at_cs_item_sk_at_local__cs_all_item_sk onto cs.billing_customer.customers_at_cs_billing_customer_sk_at_local__cs_all_cust_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe7.preql --run-and-delete`

  ```text
  Resolution error in probe7.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: cs.item.items_at_cs_item_sk_at_local__cs_all_item_sk onto cs.billing_customer.customers_at_cs_billing_customer_sk_at_local__cs_all_cust_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe8.preql --run-and-delete`

  ```text
  Resolution error in probe8.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: ss.item.items_at_ss_item_sk_at_local__ss_pairs_item_sk onto ss.customer.customers_at_ss_customer_sk_at_local__ss_pairs_cust_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_order.preql --run-and-delete`

  ```text
  Syntax error in probe_order.preql: ORDER BY references 'local.parent', which is not in the SELECT projection (line 11). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --local.parent order by local.parent asc`.
  ```
- `trilogy file write probe_main.preql --run-and-delete`

  ```text
  Unexpected error in probe_main.preql: (_duckdb.BinderException) Binder Error: column D_WEEK_SEQ must appear in the GROUP BY clause or be used in an aggregate function
  [SQL:
  WITH
  uneven as (
  SELECT
      1 as "__preql_internal_all_rows",
      max("all_sales_sale_date_date"."D_WEEK_SEQ") as "tw"
  FROM
      "date_dim" as "all_sales_sale_date_date"
  WHERE
      cast("all_sales_sale_date_date"."D_DATE" as date) = date '2000-01-03'

  HAVING
      "all_sales_sale_date_date"."D_WEEK_SEQ" = max("all_sales_sale_date_date"."D_WEEK_SEQ")
  ),
  cheerful as (
  SELECT
       'CATALOG'  as "all_sales_channel",
      "all_sales_catalog_sales_unified"."CS_EXT_SALES_PRICE" as "all_sales_ext_sales_price",
      "all_sales_catalog_sales_unified"."CS_ITEM_SK" as "all_sales_item_sk"
  FROM
      "catalog_sales" as "all_sales_catalog_sales_unified"
  UNION ALL
  SELECT
       'STORE'  as "all_sales_channel",
      "all_sales_store_sales_unified"."SS_EXT_SALES_PRICE" as "all_sales_ext_sales_price",
      "all_sales_store_sales_unified"."SS_ITEM_SK" as "all_sales_item_sk"
  FROM
      "store_sales" as "all_sales_store_sales_unified"
  UNION ALL
  SELECT
       'WEB'  as "all_sales_channel",
      "all_sales_web_sales_unified"."WS_EXT_SALES_PRICE" as "all_sales_ext_sales_price",
      "all_sales_web_sales_unified"."WS_ITEM_SK" as "all_sales_item_sk"
  FROM
      "web_sales" as "all_sales_web_sales_unified"),
  cooperative as (
  SELECT
      "all_sales_item_items"."I_ITEM_ID" as "item_code",
      1 as "__preql_internal_all_rows",
      sum(CASE WHEN "cheerful"."all_sales_channel" = 'CATALOG' THEN "cheerful"."all_sales_ext_sales_price" ELSE NULL END) as "catalog_total",
      sum(CASE WHEN "cheerful"."all_sales_channel" = 'STORE' THEN "cheerful"."all_sales_ext_sales_price" ELSE NULL END) as "store_total",
      sum(CASE WHEN "cheerful"."all_sales_channel" = 'WEB' THEN "cheerful"."all_sales_ext_sales_price" ELSE NULL END) as "web_total"
  FROM
      "cheerful"
      INNER JOIN "item" as "all_sales_item_items" on "cheerful"."all_sales_item_sk" = "all_sales_item_items"."I_ITEM_SK"
  GROUP BY
      1
  HAVING
      "store_total" is not null
  ),
  concerned as (
  SELECT
      "cooperative"."catalog_total" as "catalog_total",
      "cooperative"."item_code" as "item_code",
      "cooperative"."store_total" as "store_total",
      "cooperative"."web_total" as "web_total",
      "uneven"."tw" as "tw"
  FROM
      "cooperative"
      INNER JOIN "uneven" on "cooperative"."__preql_internal_all_rows" = "uneven"."__preql_internal_all_rows")
  SELECT
      "concerned"."item_code" as "item_code",
      "concerned"."store_total" as "store_total",
      "concerned"."catalog_total" as "catalog_total",
      "concerned"."web_total" as "web_total",
      "concerned"."tw" as "tw"
  FROM
      "concerned"
  WHERE
      "concerned"."catalog_total" is not null and "concerned"."web_total" is not null

  ORDER BY
      "concerned"."item_code" asc nulls first,
      "concerned"."store_total" asc nulls first
  LIMIT (5)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy file write probe_pair.preql --run-and-delete`

  ```text
  Syntax error in probe_pair.preql: HAVING filters on a dimension outside the SELECT projection, but the select has no grain key to anchor a post-aggregation semijoin (line 4). Move the filter to WHERE to filter before aggregation.
  ```
- `trilogy file write probe_days.preql --run-and-delete`

  ```text
  Syntax error in probe_days.preql: ORDER BY references 'ss.sale_date.day_of_week', which is not in the SELECT projection (line 2). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --ss.sale_date.day_of_week order by ss.sale_date.day_of_week asc`.
  ```
- `trilogy file write answer_3036656719.preql --run`

  ```text
  Syntax error in answer_3036656719.preql: Output column 'rnk' renames 'local.rnk' back to the name of an existing concept 'rnk' (defined at line 5) that 'local.rnk' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'rnk_out').
  ```
- `trilogy file write probe_validate.preql --run-and-delete`

  ```text
  Syntax error in probe_validate.preql: HAVING references 'yearly.curr_qty', which is not defined (line 18). Check for a typo or import the relevant concept.
  ```
- `trilogy file write answer_2869182220.preql --run`

  ```text
  Resolution error in answer_2869182220.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: ss.item.items_at_ss_item_sk_at_local__return_lines_item_sk onto ss.return_customer_demographic.customer_demographics_at_ss_return_customer_demographic_sk_at_local__return_lines_demo_key. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write answer_2852230229.preql --run`

  ```text
  Syntax error in answer_2852230229.preql: ORDER BY references 'local.parent', which is not in the SELECT projection (line 11). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --local.parent order by local.parent asc`.
  ```
- `trilogy file write probe3.preql --run-and-delete`

  ```text
  Unexpected error in probe3.preql: Could not render the query: Missing source reference to ss.quantity; Missing source reference to ss.sales_price; Missing source reference to ss.return_quantity. A planned reference has no backing source CTE -- typically an unsupported cross-rowset or membership shape the planner could not wire. Review the rowset/join structure (or file an issue if the query looks valid).

  Full SQL with sentinel(s):

  WITH
  cheerful as (
  SELECT
      "ss_store_sales"."SS_CUSTOMER_SK" as "ss_customer_sk",
      CONCAT(cast(coalesce("ss_store_returns"."SR_TICKET_NUMBER","ss_store_sales"."SS_TICKET_NUMBER") as string), '-', cast(coalesce("ss_store_returns"."SR_ITEM_SK","ss_store_sales"."SS_ITEM_SK") as string)) as "ss_line_item"
  FROM
      "store_sales" as "ss_store_sales"
      LEFT OUTER JOIN "store_returns" as "ss_store_returns" on "ss_store_sales"."SS_ITEM_SK" = "ss_store_returns"."SR_ITEM_SK" AND "ss_store_sales"."SS_TICKET_NUMBER" = "ss_store_returns"."SR_TICKET_NUMBER"
      RIGHT OUTER JOIN "reason" as "ss_return_reason_reason" on "ss_store_returns"."SR_REASON_SK" = "ss_return_reason_reason"."R_REASON_SK"
  WHERE
      "ss_return_reason_reason"."R_REASON_DESC" = 'reason 28'
  ),
  cooperative as (
  SELECT
      "cheerful"."ss_customer_sk" as "ss_customer_sk"
  FROM
      "cheerful"
  GROUP BY
      1),
  questionable as (
  SELECT
      count(distinct "cooperative"."ss_customer_sk") as "customers"
  FROM
      "cooperative"),
  thoughtful as (
  SELECT
      count("cheerful"."ss_line_item") as "total_lines",
      count(CASE WHEN ( INVALID_REFERENCE_BUG<Missing source reference to ss.quantity> is null or INVALID_REFERENCE_BUG<Missing source reference to ss.sales_price> is null ) THEN "cheerful"."ss_line_item" ELSE NULL END) as "null_qty_price_lines",
      count(CASE WHEN INVALID_REFERENCE_BUG<Missing source reference to ss.return_quantity> is not null THEN "cheerful"."ss_line_item" ELSE NULL END) as "with_return_qty"
  FROM
      "cheerful")
  SELECT
      "thoughtful"."total_lines" as "total_lines",
      "thoughtful"."null_qty_price_lines" as "null_qty_price_lines",
      "thoughtful"."with_return_qty" as "with_return_qty",
      "questionable"."customers" as "customers"
  FROM
      "thoughtful"
      INNER JOIN "questionable" on 1=1
  ```

### `undefined-concept`

- `trilogy file write probe2.preql --run-and-delete`

  ```text
  Syntax error in probe2.preql: Undefined concept: ss_best_placeholder.
  ```
- `trilogy file write answer_3036656719.preql --run`

  ```text
  Syntax error in answer_3036656719.preql: Undefined concept: _virt_agg_grouping_449731194548500.
  ```
- `trilogy file write answer_3036656719.preql --run`

  ```text
  Syntax error in answer_3036656719.preql: Undefined concept: _virt_agg_grouping_6045822588004077.
  ```
- `trilogy file write answer_3036656719.preql --run`

  ```text
  Syntax error in answer_3036656719.preql: Undefined concept: _virt_agg_grouping_9165205873002694.
  ```
- `trilogy file write probe2.preql --run-and-delete`

  ```text
  Syntax error in probe2.preql: Undefined concept: _virt_agg_grouping_3513081987384927.
  ```
- `trilogy file write probe4.preql --run-and-delete`

  ```text
  Syntax error in probe4.preql: Undefined concept: _virt_agg_grouping_3513081987384927.
  ```
- `trilogy file write probe5.preql --run-and-delete`

  ```text
  Syntax error in probe5.preql: Undefined concept: _virt_agg_grouping_3513081987384927.
  ```
- `trilogy file write probe6.preql --run-and-delete`

  ```text
  Syntax error in probe6.preql: Undefined concept: _virt_agg_grouping_3513081987384927.
  ```
- `trilogy file write answer_2874833976.preql --run`

  ```text
  Syntax error in answer_2874833976.preql: Undefined concept: _virt_agg_grouping_9769516219811737.
  ```
- `trilogy file write probe5.preql --run-and-delete`

  ```text
  Syntax error in probe5.preql: Undefined concept: _virt_agg_grouping_4453574046401692.
  ```
- `trilogy file write probe8.preql --run-and-delete`

  ```text
  Syntax error in probe8.preql: Undefined concept: _virt_agg_grouping_4453574046401692.
  ```
- `trilogy file write probe_prev.preql --run-and-delete`

  ```text
  Syntax error in probe_prev.preql: 3 undefined concept references; fix all before re-running:
    - ss.customer_id (line 2, col 26, in SELECT); did you mean: ss.customer.id, ss.store_id, ss.customer.sk?
    - ss.store_id (line 2, col 42, in SELECT); did you mean: ss.store.id, ss.customer_id, ss.store.zip?
    - ss.date_id (line 2, col 55, in SELECT); did you mean: ss.store_id, ss.sale_date.id, ss.item.id?
  ```
- `trilogy file write probe_cutoff.preql --run-and-delete`

  ```text
  Syntax error in probe_cutoff.preql: 6 undefined concept references; fix all before re-running:
    - local.store_name (line 29, col 3, in ORDER BY); did you mean: ss.store.name, store_company_name, ss.store.street_name?
    - local.category (line 30, col 3, in ORDER BY); did you mean: ss.item.category?
    - local.class (line 31, col 3, in ORDER BY); did you mean: ss.item.class?
    - local.brand (line 32, col 3, in ORDER BY)
    - local.store_company_name (line 33, col 3, in ORDER BY); did you mean: ss.store.company_name, ss.return_store.company_name, store_name?
    - local.month_of_year (line 34, col 3, in ORDER BY); did you mean: ss.sale_date.month_of_year, ss.return_date.month_of_year, ss.customer.first_sales_date.month_of_year, ss.customer.first_shipto_date.month_of_year, ss.return_customer.first_sales_date.month_of_year, ss.return_customer.first_shipto_date.month_of_year?
  ```

### `syntax-parse`

- `trilogy file write probe_bi.preql --run-and-delete`

  ```text
  refused to write 'probe_bi.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...'2000-09-06'::date)
         )
   ??? group by 1, 2
   order by 1 asc
  ```
- `trilogy file write probe_mech.preql --run-and-delete --param zips=24128,76232,65084,87816,83926`

  ```text
  refused to write 'probe_mech.preql': not syntactically valid Trilogy.

  Parse error:
    --> 13:21
     |
  13 | where array_contains(split(zips, ','), pref_zip.zip)
     |                     ^---
     |
     = expected limit, order_by, THEN_LA, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...s prefix
   where array_contains ??? (split(zips, ','), pref_zip.zi...
  ```
- `trilogy file write probe_final.preql --run-and-delete --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  refused to write 'probe_final.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [211]: Expression in `by` clause must be wrapped in parens - write `by (expr1, expr2, ...)`. Bare identifiers (`by a, b`) work without parens, but any function call, cast, or other expression needs them.
  Location:
  ...efix,
       count(ss.store.sk) ??? by substring(ss.store.zip, 1,
  ```
- `trilogy file write probe3.preql --run-and-delete`

  ```text
  refused to write 'probe3.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:21
    |
  4 | with model_lines as (
    |                     ^---
    |
    = expected select_statement, tvf_union_invocation, tvf_except_invocation, or tvf_intersect_invocation
  Location:
  ...as ss;

   with model_lines as ??? (
     select csr.item.sk as ite...
  ```
- `trilogy file write probe1.preql --run-and-delete`

  ```text
  refused to write 'probe1.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [211]: Expression in `by` clause must be wrapped in parens - write `by (expr1, expr2, ...)`. Bare identifiers (`by a, b`) work without parens, but any function call, cast, or other expression needs them.
  Location:
  ...e.year between 2000 and 2003) ??? by ss.item.sk, substring(ss.it...
  ```
- `trilogy file write answer_1965638525.preql --run`

  ```text
  refused to write 'answer_1965638525.preql': not syntactically valid Trilogy.

  Parse error:
    --> 17:42
     |
  17 | having ss.customer.current_address.city <> ss.pos_address.city
     |                                          ^---
     |
     = expected sum_operator
  Location:
  ...ustomer.current_address.city < ??? > ss.pos_address.city
   order b...
  ```
- `trilogy file write probe3.preql --run-and-delete`

  ```text
  refused to write 'probe3.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...ate.month_seq in wm.mseq
       ??? group by ss.customer.sk;

   se...
  ```
- `trilogy file write probe_count.preql --run-and-delete`

  ```text
  refused to write 'probe_count.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...l.item_code) as n_qualifying
   ??? from qual;
  ```
- `trilogy file write probe4.preql --run-and-delete`

  ```text
  refused to write 'probe4.preql': not syntactically valid Trilogy.

  Parse error:
   --> 6:234
    |
  6 |     count(ss.line_item ? (ss.is_returned and ss.item.color in ('purple','burlywood','indian','spring','floral','medium') and ss.item.current_price >= 65 and ss.item.current_price <= 74 and ss.pos_customer_demographic.marital_status <> ss.customer.current_demographics.marital_status)) as n_filt_all,
    |                                                                                                                                                                                                                                          ^---
    |
    = expected sum_operator
  Location:
  ...r_demographic.marital_status < ??? > ss.customer.current_demograp...
  ```
- `trilogy file write answer_3063407983.preql --run`

  ```text
  refused to write 'answer_3063407983.preql': not syntactically valid Trilogy.

  Parse error:
   --> 9:41
    |
  9 |   and ss.customer.current_address.city <> ss.pos_address.city
    |                                         ^---
    |
    = expected sum_operator
  Location:
  ...ustomer.current_address.city < ??? > ss.pos_address.city
   select
  ```
- `trilogy file write probe_1772060640e.preql --run-and-delete`

  ```text
  refused to write 'probe_1772060640e.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...es.ticket_number is not null
   ??? group by bucket;
  ```
- `trilogy file write probe3.preql --run-and-delete`

  ```text
  refused to write 'probe3.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:1
    |
  7 | union (
    | ^---
    |
    = expected join_clause, metadata, limit, order_by, where, having, or select_grouping
  Location:
   in target_week)) as ret_qty
   ??? union (
     select 'CATALOG' as...
  ```

### `join-resolution`

- `trilogy file write probe_check.preql --run-and-delete`

  ```text
  Resolution error in probe_check.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 18). The requested concepts split into 2 disconnected subgraphs: {cu_zip, via_cu.zip}; {ss_zip, via_ss.zip}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file write answer_3553309440.preql --run`

  ```text
  Resolution error in answer_3553309440.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {dec1998.mseq, dec1998.mseq}; {_cust_totals_cust_sk, _cust_totals_total, ss.customer.current_address.county, ss.customer.current_address.state, ss.customer.sk, ss.sale_date.month_seq, ss.sale_date.month_seq, ss.store.county, ss.store.state}. Are you missing a join or merge statement to relate them?
  Note: the membership predicate(s) `(ss.customer.sk) in (qual.cust_sk)` span these subgraphs, but membership only filters rows on its left side — it does not join the two sides, so it cannot relate them for outputs or grouping. To combine values from both sides, author a query-scoped join or a merge on shared keys.
  ```
- `trilogy file write probe_main.preql --run-and-delete`

  ```text
  Resolution error in probe_main.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 8). The requested concepts split into 2 disconnected subgraphs: {_subquery_13_38.d.week_seq}; {all_sales.sale_date.week_seq, catalog_total, item_code, store_total, web_total}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file write probe2.preql --run-and-delete`

  ```text
  Resolution error in probe2.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 6). The requested concepts split into 2 disconnected subgraphs: {a.channel, a.channel_dim_id, a.channel_dim_text_id}; {store_id, st.sk}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file write probe_verify.preql --run-and-delete`

  ```text
  Resolution error in probe_verify.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {_only_cat_c}; {_only_cat_i}. Are you missing a join or merge statement to relate them?
  ```

### `cli-misuse`

- `trilogy file show raw/all_sales.preql`

  ```text
  No such command 'show'.
  ```
- `trilogy explore raw/web_returns.preql`

  ```text
  Invalid value for 'PATH': File 'raw/web_returns.preql' does not exist.
  ```
- `trilogy file write answer_4140546834.preql --run --all-rows`

  ```text
  No such option: --all-rows
  ```
