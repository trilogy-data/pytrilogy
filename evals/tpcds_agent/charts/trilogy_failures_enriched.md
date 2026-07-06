# Trilogy failure analysis — 20260706-135543

- Run `20260706-135542_enriched` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 1267 | failed: 139 (11%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 81 | 58% |
| `syntax-parse` | 40 | 29% |
| `cli-misuse` | 6 | 4% |
| `join-resolution` | 6 | 4% |
| `type-error` | 4 | 3% |
| `syntax-missing-alias` | 2 | 1% |

## Detail

### `other`

- `trilogy run query02.preql`

  ```text
  Syntax error in query02.preql: Undefined concept: cur.ws. Suggestions: ['all_sales.ws', 'totals.all_sales.ws']
  ```
- `trilogy file read query02.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query04.preql`

  ```text
  Syntax error in query04.preql: Undefined concept: sv1.cust_id. Suggestions: ['store_value.cust_id', 'web_value.cust_id', 'ss.customer.id', 'catalog_value.cust_id', 'store_cust_2002.cust_id']
  ```
- `trilogy file read query04.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query06.preql`

  ```text
  Resolution error in query06.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 6). The requested concepts split into 2 disconnected subgraphs: {cat_avg_price}; {qualifying_line_items, state, ss.customer.address.id, ss.date.month_of_year, ss.date.year, ss.item.category, ss.item.current_price}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query06.preql`

  ```text
  Resolution error in query06.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 7). The requested concepts split into 2 disconnected subgraphs: {cat_avg_price}; {qualifying_line_items, state, ss.customer.address.id, ss.date.month_of_year, ss.date.year, ss.item.category, ss.item.current_price}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Syntax error in query08.preql: Undefined concept: customer.preferred_cust_flag.
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Syntax error in query08.preql: Undefined concept: customer.preferred_cust_flag.
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Syntax error in query08.preql: Undefined concept: customer.preferred_cust_flag.
  ```
- `trilogy run query09.preql`

  ```text
  Syntax error in query09.preql: Undefined concept: store_sales.ext_discount_amount. Suggestions: ['store_sales.quantity', 'ext_discount_amount']
  ```
- `trilogy run query09.preql`

  ```text
  Syntax error in query09.preql: Undefined concept: store_sales.quantity. Suggestions: ['store.date.quarter', 'return_quantity', 'store.county', 'quantity']
  ```
- `trilogy run query10.preql`

  ```text
  Resolution error in query10.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 40). The requested concepts split into 2 disconnected subgraphs: {college_dependent_count, credit_rating, dependent_count, education_status, employed_dependent_count, gender, marital_status, purchase_estimate}; {customer_count, customer_count2, customer_count3, customer_count4, customer_count5, customer_count6, store_customers.cust_id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query10.preql`

  ```text
  Syntax error in query10.preql: Undefined concept: ws.customer.demographics.id. Suggestions: ['ws.ship_customer.demographics.id', 'ws.return_customer.demographics.id', 'ws.billing_customer.demographics.id', 'ss.customer.demographics.id', 'ss.customer_demographic.id', 'ss.item.id']
  ```
- `trilogy run query10.preql`

  ```text
  Syntax error in query10.preql: Duplicate select output for local.customer_count; Line: 18
  ```
- `trilogy run query13.preql`

  ```text
  Syntax error in query13.preql: 6 undefined concept references; fix all before re-running:
    - ss.customer_demographics.marital_status (line 6, col 6, in WHERE); did you mean: ss.customer_demographic.marital_status, ss.customer.demographics.marital_status, ss.return_customer.demographics.marital_status?
    - ss.customer_demographics.education_status (line 6, col 56, in WHERE); did you mean: ss.customer_demographic.education_status, ss.customer.demographics.education_status, ss.return_customer.demographics.education_status?
    - ss.customer_demographics.marital_status (line 7, col 9, in WHERE); did you mean: ss.customer_demographic.marital_status, ss.customer.demographics.marital_status, ss.return_customer.demographics.marital_status?
    - ss.customer_demographics.education_status (line 7, col 59, in WHERE); did you mean: ss.customer_demographic.education_status, ss.customer.demographics.education_status, ss.return_customer.demographics.education_status?
    - ss.customer_demographics.marital_status (line 8, col 9, in WHERE); did you mean: ss.customer_demographic.marital_status, ss.customer.demographics.marital_status, ss.return_customer.demographics.marital_status?
    - ss.customer_demographics.education_status (line 8, col 59, in WHERE); did you mean: ss.customer_demographic.education_status, ss.customer.demographics.education_status, ss.return_customer.demographics.education_status?
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 41 column 41 (char 1301). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query14.preql`

  ```text
  Unexpected error in query14.preql: Internal planner error: could not resolve union/multiselect output 'local._all_combos_b' against CTE 'fabulous' (it is not in that CTE's outputs ['local._nov_data_brand_id', 'local._nov_data_category_id', 'local._nov_data_channel', 'local._nov_data_class_id', 'nov_data.brand_id', 'nov_data.category_id', 'nov_data.channel', 'nov_data.class_id']). If this came from an ORDER BY on a union column, order by the projected output column instead.
  ```
- `trilogy run query14.preql`

  ```text
  Unexpected error in query14.preql: Internal planner error: could not resolve union/multiselect output 'local._all_combos_b' against CTE 'yellow' (it is not in that CTE's outputs ['local._filtered_data_brand_id', 'local._filtered_data_category_id', 'local._filtered_data_channel', 'local._filtered_data_class_id', 'local._filtered_data_sale_count', 'local._filtered_data_total_sales']). If this came from an ORDER BY on a union column, order by the projected output column instead.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query17.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query18.preql`

  ```text
  Syntax error in query18.preql: Comparison `cs.billing_customer_demographic.gender = 'Female'` can never match enum field 'cs.billing_customer_demographic.gender', which contains only these values: 'M', 'F'. It is always false and should be removed.
  ```
- `trilogy file read query25.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query29.preql`

  ```text
  Syntax error in query29.preql: Comparison `sr.return_date.month_of_year <= 12` matches every value of enum field 'sr.return_date.month_of_year', which contains only these values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12. It is always true and should be removed.
  ```
- `trilogy file read query29.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query35.preql`

  ```text
  Resolution error in query35.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 35). The requested concepts split into 2 disconnected subgraphs: {avg_college_dependent_count, avg_dependent_count, avg_employed_dependent_count, college_dependent_count, dependent_count, employed_dependent_count, gender, marital_status, max_college_dependent_count, max_dependent_count, max_employed_dependent_count, min_college_dependent_count, min_dependent_count, min_employed_dependent_count, state}; {customer_count, customer_count2, customer_count3, store_custs.cust_id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query35.preql`

  ```text
  Resolution error in query35.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 26). The requested concepts split into 2 disconnected subgraphs: {avg_college_dependent_count, avg_dependent_count, avg_employed_dependent_count, college_dependent_count, dependent_count, employed_dependent_count, gender, marital_status, max_college_dependent_count, max_dependent_count, max_employed_dependent_count, min_college_dependent_count, min_dependent_count, min_employed_dependent_count, state}; {customer_count, customer_count2, customer_count3, store_custs.cust_id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query37.preql`

  ```text
  Resolution error in query37.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 6). The requested concepts split into 2 disconnected subgraphs: {inv.date.date, inv.quantity_on_hand}; {item.current_price, item.id, item.manufacturer_id, description, item_code}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read query37.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query38.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query40.preql`

  ```text
  Syntax error in query40.preql: Undefined concept: local.cr.
  ```
- `trilogy file read raw/item.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query46.preql`

  ```text
  Syntax error in query46.preql: 13 undefined concept references; fix all before re-running:
    - customer.last_name (line 15, col 5, in SELECT); did you mean: store_sales.customer.last_name, customer.first_name, store_sales.return_customer.last_name?
    - customer.first_name (line 16, col 5, in SELECT); did you mean: store_sales.customer.first_name, customer.last_name, store_sales.return_customer.first_name?
    - date.day_of_week (line 5, col 6, in WHERE); did you mean: store_sales.date.day_of_week, store_sales.store.date.day_of_week, store_sales.return_store.date.day_of_week, store_sales.return_date.day_of_week, store_sales.customer.first_sales_date.day_of_week, store_sales.customer.first_shipto_date.day_of_week?
    - date.year (line 7, col 9, in WHERE); did you mean: store_sales.date.year, store_sales.store.date.year, store_sales.return_store.date.year, store_sales.return_date.year, store_sales.customer.first_sales_date.year, store_sales.customer.first_shipto_date.year?
    - store.city (line 9, col 9, in WHERE); did you mean: store_sales.store.city, customer_city, customer.address.city, sale_city, store_sales.customer.address.city, store_sales.return_customer.address.city?
    - household_demographic.dependent_count (line 11, col 10, in WHERE); did you mean: store_sales.customer.household_demographic.dependent_count, store_sales.return_customer.household_demographic.dependent_count, store_sales.household_demographic.dependent_count, household_demographic.vehicle_count, store_sales.customer.demographics.dependent_count, store_sales.return_customer.demographics.dependent_count?
    - household_demographic.vehicle_count (line 11, col 55, in WHERE); did you mean: store_sales.customer.household_demographic.vehicle_count, store_sales.return_customer.household_demographic.vehicle_count, store_sales.household_demographic.vehicle_count, household_demographic.dependent_count?
    - customer.address.city (line 13, col 9, in WHERE); did you mean: store_sales.customer.address.city, customer.last_name, sale_address.city, store_sales.customer.address.county, store_sales.return_customer.address.city, store_sales.store.city?
    - sale_address.city (line 13, col 34, in WHERE); did you mean: store_sales.sale_address.city, customer.address.city, sale_city, store_sales.customer.address.city, store_sales.return_customer.address.city, store_sales.store.city?
    - customer.last_name (line 23, col 5, in ORDER BY); did you mean: store_sales.customer.last_name, customer.first_name, store_sales.return_customer.last_name?
    - customer.first_name (line 24, col 5, in ORDER BY); did you mean: store_sales.customer.first_name, customer.last_name, store_sales.return_customer.first_name?
    - customer.address.city (line 25, col 5, in ORDER BY); did you mean: store_sales.customer.address.city, customer.last_name, sale_address.city, store_sales.customer.address.county, store_sales.return_customer.address.city, store_sales.store.city?
    - sale_address.city (line 26, col 5, in ORDER BY); did you mean: store_sales.sale_address.city, customer.address.city, sale_city, store_sales.customer.address.city, store_sales.return_customer.address.city, store_sales.store.city?
  ```
- `trilogy run query49.preql`

  ```text
  Unexpected error in query49.preql: (_duckdb.BinderException) Binder Error: column "s_channel" must appear in the GROUP BY clause or must be part of an aggregate function.
  Either add it to the GROUP BY list, or use "ANY_VALUE(s_channel)" if the exact value of "s_channel" is not important.

  LINE 120:     LOWER("vacuous"."s_channel")  asc,
                      ^
  [SQL:
  WITH
  cheerful as (
  SELECT
       'CATALOG'  as "s_channel",
      "s_catalog_returns_unified"."CR_ITEM_SK" as "s_item_id",
      "s_catalog_returns_unified"."CR_ORDER_NUMBER" as "s_order_id",
      "s_catalog_returns_unified"."CR_RETURN_AMOUNT" as "s_return_amount",
      "s_catalog_returns_unified"."CR_RETURN_QUANTITY" as "s_return_quantity"
  FROM
      "catalog_returns" as "s_catalog_returns_unified"
  WHERE
      "s_catalog_returns_unified"."CR_RETURN_AMOUNT" > 10000

  UNION ALL
  SELECT
       'STORE'  as "s_channel",
      "s_store_returns_unified"."SR_ITEM_SK" as "s_item_id",
      "s_store_returns_unified"."SR_TICKET_NUMBER" as "s_order_id",
      "s_store_returns_unified"."SR_RETURN_AMT" as "s_return_amount",
      "s_store_returns_unified"."SR_RETURN_QUANTITY" as "s_return_quantity"
  FROM
      "store_returns" as "s_store_returns_unified"
  WHERE
      "s_store_returns_unified"."SR_RETURN_AMT" > 10000

  UNION ALL
  SELECT
       'WEB'  as "s_channel",
      "s_web_returns_unified"."WR_ITEM_SK" as "s_item_id",
      "s_web_returns_unified"."WR_ORDER_NUMBER" as "s_order_id",
      "s_web_returns_unified"."WR_RETURN_AMT" as "s_return_amount",
      "s_web_returns_unified"."WR_RETURN_QUANTITY" as "s_return_quantity"
  FROM
      "web_returns" as "s_web_returns_unified"
  WHERE
      "s_web_returns_unified"."WR_RETURN_AMT" > 10000
  ),
  abundant as (
  SELECT
       'CATALOG'  as "s_channel",
      "s_catalog_sales_unified"."CS_ITEM_SK" as "s_item_id",
      "s_catalog_sales_unified"."CS_NET_PAID" as "s_net_paid",
      "s_catalog_sales_unified"."CS_ORDER_NUMBER" as "s_order_id",
      "s_catalog_sales_unified"."CS_QUANTITY" as "s_quantity"
  FROM
      "catalog_sales" as "s_catalog_sales_unified"
      INNER JOIN "date_dim" as "s_date_date" on "s_catalog_sales_unified"."CS_SOLD_DATE_SK" = "s_date_date"."D_DATE_SK"
  WHERE
      "s_catalog_sales_unified"."CS_NET_PROFIT" > 1 and "s_catalog_sales_unified"."CS_NET_PAID" > 0 and "s_catalog_sales_unified"."CS_QUANTITY" > 0 and "s_date_date"."D_YEAR" = 2001 and "s_date_date"."D_MOY" = 12

  UNION ALL
  SELECT
       'STORE'  as "s_channel",
      "s_store_sales_unified"."SS_ITEM_SK" as "s_item_id",
      "s_store_sales_unified"."SS_NET_PAID" as "s_net_paid",
      "s_store_sales_unified"."SS_TICKET_NUMBER" as "s_order_id",
      "s_store_sales_unified"."SS_QUANTITY" as "s_quantity"
  FROM
      "store_sales" as "s_store_sales_unified"
      INNER JOIN "date_dim" as "s_date_date" on "s_store_sales_unified"."SS_SOLD_DATE_SK" = "s_date_date"."D_DATE_SK"
  WHERE
      "s_store_sales_unified"."SS_NET_PROFIT" > 1 and "s_store_sales_unified"."SS_NET_PAID" > 0 and "s_store_sales_unified"."SS_QUANTITY" > 0 and "s_date_date"."D_YEAR" = 2001 and "s_date_date"."D_MOY" = 12

  UNION ALL
  SELECT
       'WEB'  as "s_channel",
      "s_web_sales_unified"."WS_ITEM_SK" as "s_item_id",
      "s_web_sales_unified"."WS_NET_PAID" as "s_net_paid",
      "s_web_sales_unified"."WS_ORDER_NUMBER" as "s_order_id",
      "s_web_sales_unified"."WS_QUANTITY" as "s_quantity"
  FROM
      "web_sales" as "s_web_sales_unified"
      INNER JOIN "date_dim" as "s_date_date" on "s_web_sales_unified"."WS_SOLD_DATE_SK" = "s_date_date"."D_DATE_SK"
  WHERE
      "s_web_sales_unified"."WS_NET_PROFIT" > 1 and "s_web_sales_unified"."WS_NET_PAID" > 0 and "s_web_sales_unified"."WS_QUANTITY" > 0 and "s_date_date"."D_YEAR" = 2001 and "s_date_date"."D_MOY" = 12
  ),
  yummy as (
  SELECT
      "abundant"."s_item_id" as "s_item_id",
      "cheerful"."s_channel" as "s_channel",
      sum("abundant"."s_net_paid") as "_virt_agg_sum_2571789086077271",
      sum("abundant"."s_quantity") as "_virt_agg_sum_4660333936551281",
      sum("cheerful"."s_return_amount") as "_virt_agg_sum_1538854875751125",
      sum("cheerful"."s_return_quantity") as "_virt_agg_sum_4670517508894896"
  FROM
      "abundant"
      INNER JOIN "cheerful" on "abundant"."s_channel" = "cheerful"."s_channel" AND "abundant"."s_item_id" = "cheerful"."s_item_id" AND "abundant"."s_order_id" = "cheerful"."s_order_id"
  GROUP BY
      1,
      2),
  vacuous as (
  SELECT
      "yummy"."_virt_agg_sum_4670517508894896" / "yummy"."_virt_agg_sum_4660333936551281" as "return_ratio",
      "yummy"."s_channel" as "s_channel",
      "yummy"."s_item_id" as "item",
      LOWER("yummy"."s_channel")  as "channel",
      rank() over (partition by "yummy"."s_channel" order by "yummy"."_virt_agg_sum_1538854875751125" / "yummy"."_virt_agg_sum_2571789086077271" asc ) as "currency_rank",
      rank() over (partition by "yummy"."s_channel" order by "yummy"."_virt_agg_sum_4670517508894896" / "yummy"."_virt_agg_sum_4660333936551281" asc ) as "return_rank"
  FROM
      "yummy")
  SELECT
      "vacuous"."channel" as "channel",
      "vacuous"."item" as "item",
      "vacuous"."return_ratio" as "return_ratio",
      "vacuous"."return_rank" as "return_rank",
      "vacuous"."currency_rank" as "currency_rank"
  FROM
      "vacuous"
  WHERE
      "vacuous"."return_rank" <= 10 or "vacuous"."currency_rank" <= 10

  GROUP BY
      1,
      2,
      3,
      4,
      5
  ORDER BY
      LOWER("vacuous"."s_channel")  asc,
      "vacuous"."return_rank" asc,
      "vacuous"."currency_rank" asc,
      "vacuous"."item" asc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy file read query49.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file write query51.preql -c import raw.store_sales as ss;
import raw.web_sales as ws;

# Per-item, per-day sum of unit price (sales_price) for store …ly.item
union join store_daily.d = web_daily.d
having web_running_max > store_running_max
order by item asc nulls first, d asc nulls first
limit 100;`

  ```text
  cannot import name 'detect_having_after_grouping' from 'trilogy.parsing.v2.errors' (C:\Users\ethan\coding_projects\pytrilogy_two\trilogy\parsing\v2\errors.py)
  ```
- `trilogy file write query51.preql --from-file query51.preql`

  ```text
  cannot import name 'detect_having_after_grouping' from 'trilogy.parsing.v2.errors' (C:\Users\ethan\coding_projects\pytrilogy_two\trilogy\parsing\v2\errors.py)
  ```
- `trilogy file read query54.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query56.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query57.preql`

  ```text
  Syntax error in query57.preql: Nothing was executed: parsed 5 definition statement(s) (4 rowsets, 1 import) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy run test_join2.preql`

  ```text
  Resolution error in test_join2.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 6). The requested concepts split into 2 disconnected subgraphs: {_aaa_cnt, _aaa_wk, ss.date.year}; {_aaa_nm}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read query59.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw/catalog_sales:cs --import raw/catalog_returns:cr select cs.item.id, sum(cs.ext_list_price) as cat_list_price, sum(cr.refunded_cash + cr.reversed_charge + cr.store_credit) as cat_refund limit 5;`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {cs.item.id, cat_list_price}; {cat_refund}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run --import raw/catalog_sales:cs --import raw/catalog_returns:cr auto cat_list_price_by_item <- sum(cs.ext_list_price) by cs.item.id; auto cat_refun…e_credit) by cr.item.id; select cs.item.id, cat_list_price_by_item, cat_refund_by_item where cat_list_price_by_item > 2 * cat_refund_by_item limit 5;`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {cs.item.id, cat_list_price_by_item}; {cat_refund_by_item}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Undefined concept: a.product_name. Suggestions: ['ss.item.product_name', 'cs.item.product_name', 'cr.item.product_name', 'cr.sales.item.product_name', 'agg_store_sales.ss.item.product_name']
  ```
- `trilogy run --import raw.store_sales:ss with test_rs as select ss.item.text_id as item_id, ss.date.year as yr, count(ss.line_item) as cnt where ss.date.year …, a.yr, a.cnt, b.yr, b.cnt subset join a.item_id = b.item_id and a.yr = 1999 and b.yr = 2000 subset join test_rs = a subset join test_rs = b limit 5;`

  ```text
  Syntax error in stdin: Undefined concept: a.item_id. Suggestions: ['b.item_id', 'ss.item.id', 'test_rs.item_id']
  ```
- `trilogy run query65.preql`

  ```text
  Syntax error in query65.preql: ORDER BY references 'ss.store.id', which is not in the SELECT projection (line 9). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --ss.store.id order by ss.store.id asc`.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query67.preql`

  ```text
  Syntax error in query67.preql: 15 undefined concept references; fix all before re-running:
    - item.category (line 12, col 5, in SELECT); did you mean: store_sales.item.category, item.class, store_sales.item.category_id?
    - item.class (line 13, col 5, in SELECT); did you mean: store_sales.item.class, item.category?
    - item.brand_name (line 14, col 5, in SELECT); did you mean: store_sales.item.brand_name, item.product_name?
    - item.product_name (line 15, col 5, in SELECT); did you mean: store_sales.item.product_name, item.brand_name?
    - date.year (line 16, col 5, in SELECT); did you mean: store_sales.date.year, store_sales.store.date.year, store_sales.return_store.date.year, date.quarter, date.month_of_year, store_sales.return_date.year?
    - date.quarter (line 17, col 5, in SELECT); did you mean: store_sales.date.quarter, store_sales.store.date.quarter, store_sales.return_store.date.quarter, date.year, store_sales.return_date.quarter, store_sales.customer.first_sales_date.quarter?
    - date.month_of_year (line 18, col 5, in SELECT); did you mean: store_sales.date.month_of_year, store_sales.store.date.month_of_year, store_sales.return_store.date.month_of_year, date.year, store_sales.return_date.month_of_year, store_sales.customer.first_sales_date.month_of_year?
    - item.category (line 26, col 5, in ORDER BY); did you mean: store_sales.item.category, item.class, store_sales.item.category_id?
    - item.class (line 27, col 5, in ORDER BY); did you mean: store_sales.item.class, item.category?
    - item.brand_name (line 28, col 5, in ORDER BY); did you mean: store_sales.item.brand_name, item.product_name?
    - item.product_name (line 29, col 5, in ORDER BY); did you mean: store_sales.item.product_name, item.brand_name?
    - date.year (line 30, col 5, in ORDER BY); did you mean: store_sales.date.year, store_sales.store.date.year, store_sales.return_store.date.year, date.quarter, date.month_of_year, store_sales.return_date.year?
    - date.quarter (line 31, col 5, in ORDER BY); did you mean: store_sales.date.quarter, store_sales.store.date.quarter, store_sales.return_store.date.quarter, date.year, store_sales.return_date.quarter, store_sales.customer.first_sales_date.quarter?
    - date.month_of_year (line 32, col 5, in ORDER BY); did you mean: store_sales.date.month_of_year, store_sales.store.date.month_of_year, store_sales.return_store.date.month_of_year, date.year, store_sales.return_date.month_of_year, store_sales.customer.first_sales_date.month_of_year?
    - store.text_id (line 33, col 5, in ORDER BY); did you mean: store_sales.store.date.text_id, store_sales.store.text_id, store_sales.time.text_id, store_sales.item.text_id, store_sales.date.text_id, store_sales.return_date.text_id?
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query71.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query72.preql`

  ```text
  Resolution error in query72.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {cs.bill_household_demographic.buy_potential, cs.billing_customer_demographic.marital_status, cs.days_to_ship, cs.item.id, cs.quantity, cs.sold_date.week_seq, cs.sold_date.year, item_description, no_promo_orders, total_orders, week_sequence, with_promo_orders}; {inv.date.week_seq, inv.item.id, inv.quantity_on_hand, warehouse_name}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query74.preql duckdb`

  ```text
  Resolution error in query74.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 13). The requested concepts split into 2 disconnected subgraphs: {customer_code, store_net_2001, store_net_2002, store.customer.first_name, store.customer.id, store.customer.last_name}; {web_net_2001, web_net_2002}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 24 column 13 (char 1315). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query77.preql`

  ```text
  Syntax error in query77.preql: ORDER BY contains aggregate `grouping(s.channel)` (line 4), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(s.channel) as g order by g desc`.
  ```
- `trilogy run query78.preql`

  ```text
  Syntax error in query78.preql: 3 undefined concept references; fix all before re-running:
    - store_qty (line 71, col 3, in ORDER BY); did you mean: other_qty, store_agg.store_qty, store_sales.quantity?
    - store_wholesale_cost (line 72, col 3, in ORDER BY); did you mean: store_sales.wholesale_cost, other_wholesale_cost, store_sales.ext_wholesale_cost, store_agg.store_wholesale_cost?
    - store_sales_price (line 73, col 3, in ORDER BY); did you mean: store_sales.list_price, store_sales.sales_price, other_sales_price, store_agg.store_sales_price?
  ```
- `trilogy file read query79.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query82.preql duckdb`

  ```text
  Resolution error in query82.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {inv.date.date, inv.item.text_id, inv.quantity_on_hand}; {current_price, description, item_code, ss.item.current_price, ss.item.manufacturer_id, ss.item.text_id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run --import raw/web_returns:wr select wr.item.text_id, sum(wr.return_quantity) as web_qty where wr.return_date.week_seq in (5244,5257,5264) limit 5;`

  ```text
  Syntax error in stdin: Undefined concept: wr.item.text_id (line 2, col 8, in SELECT). Suggestions: ['wr.web_sales.item.text_id', 'wr.time.text_id', 'wr.store.text_id', 'wr.store.date.text_id', 'wr.web_sales.date.text_id', 'wr.web_sales.ship_date.text_id']
  ```
- `trilogy file read query84.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query85.preql`

  ```text
  Syntax error in query85.preql: Comparison `wr.returning_demographic.marital_status = 'Married'` can never match enum field 'wr.returning_demographic.marital_status', which contains only these values: 'M', 'S', 'D', 'W', 'U'. It is always false and should be removed.
  ```
- `trilogy run query86.preql`

  ```text
  Syntax error in query86.preql: 9 undefined concept references; fix all before re-running:
    - rollup_result.item_category (line 22, col 5, in SELECT); did you mean: rollup_result.item_class, rollup_result.hierarchy_level, ws.item.category, item_category?
    - rollup_result.item_class (line 23, col 5, in SELECT); did you mean: rollup_result.item_category, rollup_result.hierarchy_level, rollup_result.total_net_paid, item_class?
    - rollup_result.total_net_paid (line 24, col 5, in SELECT); did you mean: total_net_paid?
    - rollup_result.hierarchy_level (line 25, col 5, in SELECT); did you mean: rollup_result.item_class, rollup_result.item_category, hierarchy_level?
    - rollup_result.item_category (line 20, in SELECT); did you mean: rollup_result.item_class, rollup_result.hierarchy_level, ws.item.category, item_category?
    - rollup_result.item_class (line 20, in SELECT); did you mean: rollup_result.item_category, rollup_result.hierarchy_level, rollup_result.total_net_paid, item_class?
    - rollup_result.total_net_paid (line 32, col 24, in SELECT); did you mean: total_net_paid?
    - rollup_result.hierarchy_level (line 33, col 10, in ORDER BY); did you mean: rollup_result.item_class, rollup_result.item_category, hierarchy_level?
    - rollup_result.item_category (line 34, col 10, in ORDER BY); did you mean: rollup_result.item_class, rollup_result.hierarchy_level, ws.item.category, item_category?
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query87.preql`

  ```text
  Unexpected error in query87.preql: composite membership operands must be concepts
  ```
- `trilogy run query90.preql`

  ```text
  Syntax error in query90.preql: Output column 'morning_count' renames 'local.morning_count' back to the name of an existing concept 'morning_count' (defined at line 5) that 'local.morning_count' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'morning_count_out').
  ```
- `trilogy run query90.preql`

  ```text
  Syntax error in query90.preql: Output column 'ratio' renames 'local.ratio' back to the name of an existing concept 'ratio' (defined at line 17) that 'local.ratio' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'ratio_out').
  ```
- `trilogy file read query90.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query92.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query94.preql duckdb`

  ```text
  Syntax error in query94.preql: Undefined concept: warehouse.id. Suggestions: ['ws.warehouse.id', 'ws.warehouse.city', 'ws.warehouse.text_id', 'ws.item.id', 'ws.date.id', 'ws.ship_date.id']
  ```
- `trilogy run query97.preql`

  ```text
  Syntax error in query97.preql: Undefined concept: in_store. Suggestions: ['only_store']
  ```

### `syntax-parse`

- `trilogy file write query02.preql --content import raw.web_sales as web;
import raw.catalog_sales as cat;
import raw.date as d;

# Combine web and catalog sal… 7 columns per week_seq
select
    cur_future.ws as week_sequence,
    ...
union join cur_future.ws = wk2001.ws
order by cur_future.ws
limit 100000;
`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...in 2001 with wk2001 as select ??? distinct d.week_seq as ws wher...

  Write stats: received 1210 chars / 1210 bytes; tail: …'_future.ws = wk2001.ws\\norder by cur_future.ws\\nlimit 100000;\\n'.
  ```
- `trilogy run --import raw/all_sales:s select s.channel, s.channel_dim_text_id, count(s.channel_dim_text_id) as cnt, sum(s.ext_sales_price) as total_sales where s.date.date between '2000-08-23'::date and '2000-09-06'::date and s.channel_dim_id is not null group by s.channel, s.channel_dim_text_id limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
   s.channel_dim_id is not null ??? group by s.channel, s.channel_...
  ```
- `trilogy file write query09.preql --content import raw.store_sales;

select
    case when sum(row_counter ? store_sales.quantity between 1 and 20) > 74129
   …e_sales.quantity between 81 and 100)
         else avg(store_sales.net_paid ? store_sales.quantity between 81 and 100)
    end as bucket_81_100
by *;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
    --> 24:1
     |
  24 | by *;
     | ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...100)     end as bucket_81_100 ??? by *;

  Write stats: received 1428 chars / 1428 bytes; tail: …'.quantity between 81 and 100)\\n    end as bucket_81_100\\nby *;'.
  ```
- `trilogy file write query13.preql --content import raw.store_sales as ss;

# Year 2001 filter
where ss.date.year = 2001

# A sale qualifies when it satisfies …vg_extended_sales_price,
  avg(ss.ext_wholesale_cost) as avg_extended_wholesale_cost,
  sum(ss.ext_wholesale_cost) as total_extended_wholesale_cost
;`

  ```text
  refused to write 'query13.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:1
    |
  7 | where
    | ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
  ...one address-and-profit clause ??? where   # Demographic-and-pric...

  Write stats: received 1558 chars / 1558 bytes; tail: …'um(ss.ext_wholesale_cost) as total_extended_wholesale_cost\\n;'.
  ```
- `trilogy file write query14.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Compute the (brand_id… first,
         grouping(nov2001.class_id) asc, class_id nulls first,
         grouping(nov2001.category_id) asc, category_id nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
     --> 144:1
      |
  144 | having sum(nov2001.total_sales) > (select overall_avg.threshold from overall_avg)
      | ^---
      |
      = expected limit or order_by
  Location:
  ...lass_id, nov2001.category_id) ??? having sum(nov2001.total_sales...

  Hint: HAVING must come *before* the `by rollup/cube/grouping sets` clause in Trilogy (the reverse of SQL's `GROUP BY ... HAVING`). Reorder to:
      select <cols> having <cond> by rollup (<keys>) order by <cols> limit <n>;

  Write stats: received 6041 chars / 6041 bytes; tail: …'nov2001.category_id) asc, category_id nulls first\\nlimit 100;'.
  ```
- `trilogy file write query14.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Compute (brand_id, cl…s null then 0 else 1 end, class_id nulls first,
         case when nov_data.category_id is null then 0 else 1 end, category_id nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...) > (select overall_all.total ??? from overall_all) / (select ov...

  Write stats: received 5084 chars / 5084 bytes; tail: …'s null then 0 else 1 end, category_id nulls first\\nlimit 100;'.
  ```
- `trilogy file write query14.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Compute (brand_id, cl…s null then 0 else 1 end, class_id nulls first,
         case when nov_data.category_id is null then 0 else 1 end, category_id nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [212]: A `by <grain>` clause must attach directly to an aggregate, not to an expression that wraps one (e.g. `coalesce(...)`, `round(...)`, arithmetic). Move the grain inside, next to the aggregate - write `coalesce(sum(x) by store.id, 0)` - or compute the grouped aggregate first and wrap it: `auto m <- sum(x) by store.id;` then `coalesce(m, 0)`.
  Location:
  ...otal) / sum(overall_all.cnt)) ??? by rollup (nov_data.channel, n...

  Write stats: received 4997 chars / 4997 bytes; tail: …'s null then 0 else 1 end, category_id nulls first\\nlimit 100;'.
  ```
- `trilogy file write query14.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Distinct (brand_id, c…s null then 0 else 1 end, class_id nulls first,
         case when filtered.category_id is null then 0 else 1 end, category_id nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [212]: A `by <grain>` clause must attach directly to an aggregate, not to an expression that wraps one (e.g. `coalesce(...)`, `round(...)`, arithmetic). Move the grain inside, next to the aggregate - write `coalesce(sum(x) by store.id, 0)` - or compute the grouped aggregate first and wrap it: `auto m <- sum(x) by store.id;` then `coalesce(m, 0)`.
  Location:
  ...al) / sum(overall_stats.cnt)) ??? by rollup (filtered.channel, f...

  Write stats: received 5466 chars / 5466 bytes; tail: …'s null then 0 else 1 end, category_id nulls first\\nlimit 100;'.
  ```
- `trilogy file write query25.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;

where ss.date.year = 2001
  and ss.date.month_of_ye…cs.sold_date.month_of_year >= 4
  and cs.sold_date.month_of_year <= 10
  sum(cs.net_profit) as catalog_sale_net_profit
order by 1, 2, 3, 4
limit 100;`

  ```text
  refused to write 'query25.preql': not syntactically valid Trilogy.

  Parse error:
    --> 23:3
     |
  23 |   sum(cs.net_profit) as catalog_sale_net_profit
     |   ^---
     |
     = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...ld_date.month_of_year <= 10   ??? sum(cs.net_profit) as catalog_...

  Write stats: received 774 chars / 774 bytes; tail: …'t) as catalog_sale_net_profit\\norder by 1, 2, 3, 4\\nlimit 100;'.
  ```
- `trilogy file write query28.preql --content import raw.store_sales as ss;

# Bucket 1: quantity 0-5, (list_price 8-18 OR coupon_amt 459-1459 OR wholesale_cost…cket5_unique_list_price,
    b6_avg as bucket6_avg_list_price,
    b6_cnt as bucket6_count_list_price,
    b6_ucnt as bucket6_unique_list_price
by *;`

  ```text
  refused to write 'query28.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [213]: A `by <grain>` clause must follow an aggregate, but the expression before it has none. If the `by` sits inside an aggregate's parentheses (`max(x by *)`), move it outside the call: `max(x) by *`. To take each distinct value once per grain, wrap it in `group(...)` - e.g. `group(item.current_price) by item.id, item.category`. For a reduction, use an aggregate: `sum(x) by ...`, `avg(x) by ...`, `max(x) by ...`.
  Location:
   as bucket6_unique_list_price ??? by *;

  Write stats: received 4876 chars / 4876 bytes; tail: …'t_list_price,\\n    b6_ucnt as bucket6_unique_list_price\\nby *;'.
  ```
- `trilogy file write query31.preql --content import raw.store_sales as st;
import raw.web_sales as web;

# For each county and quarter in 2000, compute store s…ot.web_q3 / web_pivot.web_q2 as web_q3_over_q2,
    store_pivot.store_q3 / store_pivot.store_q2 as store_q3_over_q2
order by
    store_pivot.county
;`

  ```text
  refused to write 'query31.preql': not syntactically valid Trilogy.

  Parse error:
    --> 42:50
     |
  42 | subset join store_pivot.county = web_pivot.county;
     |                                                  ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
  ...ivot.county = web_pivot.county ??? ;  where     # web Q2/Q1 > sto...

  Write stats: received 2231 chars / 2231 bytes; tail: …'ore_q2 as store_q3_over_q2\\norder by\\n    store_pivot.county\\n;'.
  ```
- `trilogy file write query35.preql --content import raw.customer as cust;
import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sales as…tal_status nulls first,
    dependent_count nulls first,
    employed_dependent_count nulls first,
    college_dependent_count nulls first
limit 100;`

  ```text
  refused to write 'query35.preql': not syntactically valid Trilogy.

  Parse error:
    --> 37:1
     |
  37 | where
     | ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
  ...store_custs.cust_id = cust.id ??? where     store_custs.cust_id

  Write stats: received 2528 chars / 2528 bytes; tail: …'ls first,\\n    college_dependent_count nulls first\\nlimit 100;'.
  ```
- `trilogy file write query36.preql -c import raw.store_sales as ss;
import raw.item as item;
import raw.date as d;
import raw.store as store;

# Filters: year ….class)
where ss.store.state = 'TN' and ss.date.year = 2001
order by level desc nulls first, category asc nulls first, rnk asc nulls first
limit 100;`

  ```text
  refused to write 'query36.preql': not syntactically valid Trilogy.

  Parse error:
    --> 34:1
     |
  34 | where ss.store.state = 'TN' and ss.date.year = 2001
     | ^---
     |
     = expected limit or order_by
  Location:
  ...item.category, ss.item.class) ??? where ss.store.state = 'TN' an...

  Write stats: received 1473 chars / 1479 bytes; tail: …'st, category asc nulls first, rnk asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query38.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Get (last_name, first…_name
where store_combos.sale_date_id is not null
  and catalog_combos.sale_date_id is not null
  and web_combos.sale_date_id is not null
limit 100
;`

  ```text
  refused to write 'query38.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...esent in all three select     ??? count(*) as cnt union join sto...

  Write stats: received 1297 chars / 1297 bytes; tail: …'t null\\n  and web_combos.sale_date_id is not null\\nlimit 100\\n;'.
  ```
- `trilogy file write query40.preql --content import raw/catalog_sales.preql as cs;

where
  cs.item.current_price between 0.99 and 1.49
  and cs.sold_date.date…turns.preql
  on cs.item.id = cr.item.id
  and cs.order_number = cr.order_number
order by
  cs.warehouse.state asc,
  cs.item.text_id asc
limit 100;
`

  ```text
  refused to write 'query40.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/catalog_sales.preql as cs;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/catalog_sales.preql as cs;...

  Write stats: received 700 chars / 700 bytes; tail: …'\\n  cs.warehouse.state asc,\\n  cs.item.text_id asc\\nlimit 100;\\n'.
  ```
- `trilogy file write query40.preql --content import raw.catalog_sales as cs;

where
  cs.item.current_price between 0.99 and 1.49
  and cs.sold_date.date betwe…log_returns
  on cs.item.id = cr.item.id
  and cs.order_number = cr.order_number
order by
  cs.warehouse.state asc,
  cs.item.text_id asc
limit 100;
`

  ```text
  refused to write 'query40.preql': not syntactically valid Trilogy.

  Parse error:
    --> 13:3
     |
  13 |   on cs.item.id = cr.item.id
     |   ^---
     |
     = expected limit, order_by, where, having, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, select_grouping, or JOIN_TYPE
  Location:
  ...in cr = raw.catalog_returns   ??? on cs.item.id = cr.item.id   a...

  Write stats: received 688 chars / 688 bytes; tail: …'\\n  cs.warehouse.state asc,\\n  cs.item.text_id asc\\nlimit 100;\\n'.
  ```
- `trilogy file write query41.preql`

  ```text
  refused to write 'query41.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [213]: A `by <grain>` clause must follow an aggregate, but the expression before it has none. If the `by` sits inside an aggregate's parentheses (`max(x by *)`), move it outside the call: `max(x) by *`. To take each distinct value once per grain, wrap it in `group(...)` - e.g. `group(item.current_price) by item.id, item.category`. For a reduction, use an aggregate: `sum(x) by ...`, `avg(x) by ...`, `max(x) by ...`.
  Location:
  ...' and item.size = 'N/A')
       ??? by item.manufact
     ) >= 2;


  Write stats: received 1259 chars / 1259 bytes; tail: …'roduct_name\r\\norder by\r\\n  item.product_name asc\r\\nlimit 100;\r\\n'.
  ```
- `trilogy file write query41.preql`

  ```text
  refused to write 'query41.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [213]: A `by <grain>` clause must follow an aggregate, but the expression before it has none. If the `by` sits inside an aggregate's parentheses (`max(x by *)`), move it outside the call: `max(x) by *`. To take each distinct value once per grain, wrap it in `group(...)` - e.g. `group(item.current_price) by item.id, item.category`. For a reduction, use an aggregate: `sum(x) by ...`, `avg(x) by ...`, `max(x) by ...`.
  Location:

       count(profile_items.id ??? by item.manufact) >= 2;

   # F...

  Write stats: received 1384 chars / 1384 bytes; tail: …'roduct_name\r\\norder by\r\\n  item.product_name asc\r\\nlimit 100;\r\\n'.
  ```
- `trilogy file write query41.preql`

  ```text
  refused to write 'query41.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...t in shared_manufacts
   select ??? distinct
     item.product_name

  Write stats: received 1398 chars / 1398 bytes; tail: …'roduct_name\r\\norder by\r\\n  item.product_name asc\r\\nlimit 100;\r\\n'.
  ```
- `trilogy run --import raw.item:item -`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...Gross' and item.size = 'N/A') ??? group by item.category, item.c...
  ```
- `trilogy file write query51.preql -c import raw.store_sales as ss;
import raw.web_sales as ws;

# Per-item, per-day sum of unit price (sales_price) for store …y.item
union join store_daily.d = web_daily.d
having web_running_max > store_running_max 
order by item asc nulls first, d asc nulls first
limit 100;`

  ```text
  refused to write 'query51.preql': not syntactically valid Trilogy.

  Parse error:
   --> 5:1
    |
  5 | rowset store_daily as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
   for store sales in year 2000 ??? rowset store_daily as where ss...

  Write stats: received 1815 chars / 1815 bytes; tail: …'\\norder by item asc nulls first, d asc nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw/item:item select distinct item.category, item.class where item.category = 'Women' limit 20;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...port raw.item as item; select ??? distinct item.category, item.c...
  ```
- `trilogy run --import raw/store_sales:ss --import raw/catalog_sales:cs select ss.customer.id, ss.date.month_seq, ss.store.county, ss.customer.address.county, …'Women' and cs.item.class = 'maternity' and cs.date.year = 1998 and cs.date.month_of_year = 12) and ss.date.month_seq between 1188 and 1190 limit 20;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...ere ss.customer.id in (select ??? distinct cs.billing_customer.i...
  ```
- `trilogy file write query57.preql --content import raw.catalog_sales as cs;

# Monthly total of per-unit sales price for each (category, brand, call_center)
w…ed.monthly_total - sequenced.avg_monthly_sales) asc nulls first,
    1 asc, 2 asc, 3 asc, 4 asc, 5 asc, 6 asc, 7 asc, 8 asc, 9 asc, 10 asc
limit 100;`

  ```text
  refused to write 'query57.preql': not syntactically valid Trilogy.

  Parse error:
    --> 56:6
     |
  56 |     -- Rel deviation > 0.1
     |      ^---
     |
     = expected access_chain
  Location:
   next_month_total having     - ??? - Rel deviation > 0.1     sequ...

  Write stats: received 2460 chars / 2460 bytes; tail: …' 4 asc, 5 asc, 6 asc, 7 asc, 8 asc, 9 asc, 10 asc\\nlimit 100;'.
  ```
- `trilogy file write query58.preql --content import raw/store_sales as ss;
import raw/catalog_sales as cs;
import raw/web_sales as ws;

# Rowset: store channel… * 0.9 and catalog_by_item.catalog_total * 1.1
order by
    store_by_item.item_code nulls first,
    store_by_item.store_total nulls first
limit 100;`

  ```text
  refused to write 'query58.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/store_sales as ss;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/store_sales as ss; import

  Write stats: received 2665 chars / 2665 bytes; tail: …' first,\\n    store_by_item.store_total nulls first\\nlimit 100;'.
  ```
- `trilogy file write query63.preql --content import raw.store_sales as ss;

# Restrict to store sales in 2000 with a recorded store
where ss.date.year = 2000
 ….item.manager_id as manager_id,
    monthly_total,
    avg_monthly
order by
    manager_id asc,
    avg_monthly asc,
    monthly_total asc
limit 100;`

  ```text
  refused to write 'query63.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block - they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...l of unit price (sales_price) ??? auto monthly_total <- sum(ss.s...

  Write stats: received 1255 chars / 1255 bytes; tail: …'d asc,\\n    avg_monthly asc,\\n    monthly_total asc\\nlimit 100;'.
  ```
- `trilogy run --import raw/catalog_sales:cs --import raw/catalog_returns:cr select cs.item.id, sum(cs.ext_list_price) as cat_list_price, sum(cr.refunded_cash + cr.reversed_charge + cr.store_credit) as cat_refund from stdin limit 5;`

  ```text
  Syntax error in stdin: Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...r.store_credit) as cat_refund ??? from stdin limit 5;
  ```
- `trilogy run --import raw/catalog_sales:cs --import raw/catalog_returns:cr select cs.item.id, sum(cs.ext_list_price) as cat_list_price sum(cr.refunded_cash + cr.reversed_charge + cr.store_credit) as cat_refund limit 5;`

  ```text
  Syntax error in stdin:  --> 3:61
    |
  3 | select cs.item.id, sum(cs.ext_list_price) as cat_list_price sum(cr.refunded_cash + cr.reversed_charge + cr.store_credit) as cat_refund limit 5;
    |                                                             ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...list_price) as cat_list_price ??? sum(cr.refunded_cash + cr.reve...
  ```
- `trilogy file write query64.preql --content import raw/store_sales as ss;
import raw/catalog_sales as cs;
import raw/catalog_returns as cr;

# Per-item catalo…ear_2000,
    paired.cnt_2000
order by
    paired.product_name,
    paired.store_name,
    paired.cnt_2000,
    paired.ws_1999,
    paired.ws_2000
;
`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/store_sales as ss;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/store_sales as ss; import

  Write stats: received 3246 chars / 3246 bytes; tail: …'  paired.cnt_2000,\\n    paired.ws_1999,\\n    paired.ws_2000\\n;\\n'.
  ```
- `trilogy file write query70.preql --content import raw.store_sales as ss;

# For store sales whose sold date falls in the year 2000, report total net profit
#…thin_parent_rank
by rollup (ss.store.state, ss.store.county)
order by
    hierarchy_level desc,
    ss.store.state,
    within_parent_rank
limit 100;`

  ```text
  refused to write 'query70.preql': not syntactically valid Trilogy.

  Parse error:
    --> 14:28
     |
  14 |     grouping(ss.store.state, ss.store.county) as hierarchy_level,
     |                            ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...t,     grouping(ss.store.state ??? , ss.store.county) as hierarch...

  Write stats: received 881 chars / 881 bytes; tail: …' desc,\\n    ss.store.state,\\n    within_parent_rank\\nlimit 100;'.
  ```
- `trilogy file write query73.preql --content import raw.store_sales as ss;

where 
    ss.date.year in (1999, 2000, 2001)
    and ss.date.day_of_month between …tem_count between 1 and 5

order by
    line_item_count desc,
    ss.customer.last_name asc,
    ss.ticket_number asc,
    ss.customer.first_name asc`

  ```text
  refused to write 'query73.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [202]: Missing closing semicolon? Statements must be terminated with a semicolon `;`.
  Location:
      ss.customer.first_name asc ???

  Write stats: received 800 chars / 800 bytes; tail: …'sc,\\n    ss.ticket_number asc,\\n    ss.customer.first_name asc'.
  ```
- `trilogy file write query77.preql --content import raw.all_sales as s;

# Filter: sales sold in the period, returns whose return date in the period
where s.da…s.channel) asc,
    grouping(s.channel, s.outlet_id) asc,
    s.channel asc nulls first,
    s.outlet_id asc nulls first,
    returns desc
limit 100;`

  ```text
  refused to write 'query77.preql': not syntactically valid Trilogy.

  Parse error:
    --> 14:33
     |
  14 |     case when grouping(s.channel, s.outlet_id) = 1 then null
     |                                 ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
    case when grouping(s.channel ??? , s.outlet_id) = 1 then null

  Write stats: received 1331 chars / 1331 bytes; tail: …'    s.outlet_id asc nulls first,\\n    returns desc\\nlimit 100;'.
  ```
- `trilogy run --import raw/store_returns:sr select sr.item.text_id, sum(sr.return_quantity) as store_qty where sr.return_date.week_seq in (5244,5257,5264) group by sr.item.text_id limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ....week_seq in (5244,5257,5264) ??? group by sr.item.text_id limit...
  ```
- `trilogy run --import raw/store_returns:sr --import raw/catalog_returns:cr --import raw/web_returns:wr select sr.item.text_id, sum(sr.return_quantity) as stor…eturn_date.week_seq in (5244,5257,5264) union join sr.item.text_id = cr.item.text_id union join sr.item.text_id = wr.web_sales.item.text_id limit 10;`

  ```text
  Syntax error in stdin:  --> 4:273
    |
  4 | select sr.item.text_id, sum(sr.return_quantity) as store_qty, sum(cr.return_quantity) as cat_qty, sum(wr.return_quantity) as web_qty where sr.return_date.week_seq in (5244,5257,5264) and cr.date.week_seq in (5244,5257,5264) and wr.return_date.week_seq in (5244,5257,5264) union join sr.item.text_id = cr.item.text_id union join sr.item.text_id = wr.web_sales.item.text_id limit 10;
    |                                                                                                                                                                                                                                                                                 ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ....week_seq in (5244,5257,5264) ??? union join sr.item.text_id = c...
  ```
- `trilogy file write query84.preql -e -c import raw.store_returns as sr;
import raw.customer as cust;

# Join: return's demographic matches any customer's curr…er_code,
    concat(coalesce(cust.last_name, ''), ', ', coalesce(cust.first_name, '')) as full_name
order by
    customer_code nulls first
limit 100;`

  ```text
  refused to write 'query84.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:1
    |
  7 | where
    | ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
  ...ic.id = cust.demographics.id  ??? where     cust.address.city =

  Write stats: received 602 chars / 602 bytes; tail: …' full_name\\norder by\\n    customer_code nulls first\\nlimit 100;'.
  ```
- `trilogy file write query84.preql -e -c import raw.store_returns as sr;
import raw.customer as cust;

where
    cust.address.city = 'Edgewood'
    and cust.ho…s ticket,
    --sr.item.id as item_id,
union join sr.customer_demographic.id = cust.demographics.id
order by
    customer_code nulls first
limit 100;`

  ```text
  refused to write 'query84.preql': not syntactically valid Trilogy.

  Parse error:
    --> 12:5
     |
  12 |     --sr.ticket_number as ticket,
     |     ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...t_name, '')) as full_name     ??? --sr.ticket_number as ticket,

  Write stats: received 592 chars / 592 bytes; tail: …'raphics.id\\norder by\\n    customer_code nulls first\\nlimit 100;'.
  ```
- `trilogy file write query86.preql --content import raw.web_sales as ws;

# Filter: only sales where the sold date is in year 2000
where ws.date.year = 2000

#…tem.class)
order by hierarchy_level desc nulls first,
         item_category asc nulls first,
         within_parent_rank asc nulls first
limit 100;
`

  ```text
  refused to write 'query86.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block - they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...oncepts for the rollup output ??? auto total_net_paid <- sum(ws....

  Write stats: received 933 chars / 933 bytes; tail: …'rst,\\n         within_parent_rank asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query86.preql --content import raw.web_sales as ws;

# Manual rollup: level 0 (category + class), level 1 (category), level 2 (grand total…y combined.hierarchy_level desc nulls first,
         combined.item_category asc nulls first,
         within_parent_rank asc nulls first
limit 100;
`

  ```text
  refused to write 'query86.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...tal_net_paid, hierarchy_level ??? from level_0),     (select ite...

  Write stats: received 1654 chars / 1654 bytes; tail: …'rst,\\n         within_parent_rank asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query87.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…distinct(concat(store_tuples.store_last_name, '-', store_tuples.store_first_name, '-', store_tuples.store_sale_date::string)) as unique_combo_count
;`

  ```text
  refused to write 'query87.preql': not syntactically valid Trilogy.

  Parse error:
   --> 6:1
    |
  6 | rowset store_tuples as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...ate) from store sales in 2000 ??? rowset store_tuples as where s...

  Write stats: received 1655 chars / 1655 bytes; tail: …'ore_tuples.store_sale_date::string)) as unique_combo_count\\n;'.
  ```
- `trilogy file write query91.preql --content import raw.catalog_returns as cr;

where
  cr.date.year = 1998
  and cr.date.month_of_year = 11
  and cr.billing_c…question
  -- cr.billing_customer.demographics.marital_status,
  -- cr.billing_customer.demographics.education_status
order by
  total_net_loss desc;`

  ```text
  refused to write 'query91.preql': not syntactically valid Trilogy.

  Parse error:
    --> 17:3
     |
  17 |   -- hidden grouping by marital_status and education per the question
     |   ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...net_loss) as total_net_loss   ??? -- hidden grouping by marital_...

  Write stats: received 887 chars / 887 bytes; tail: …'emographics.education_status\\norder by\\n  total_net_loss desc;'.
  ```

### `cli-misuse`

- `trilogy explore raw/sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/sales.preql' does not exist.
  ```
- `trilogy explore raw/date_dim.preql`

  ```text
  Invalid value for 'PATH': File 'raw/date_dim.preql' does not exist.
  ```
- `trilogy explore raw/`

  ```text
  Invalid value for 'PATH': File 'raw/' is a directory.
  ```
- `trilogy explore raw/date_dim.preql`

  ```text
  Invalid value for 'PATH': File 'raw/date_dim.preql' does not exist.
  ```
- `trilogy explore raw/all_sales.preql --show imports,datasources`

  ```text
  Invalid value for '--show': 'imports,datasources' is not one of 'all', 'concepts', 'datasources', 'imports', 'groups'.
  ```
- `trilogy explore raw/date_dim.preql`

  ```text
  Invalid value for 'PATH': File 'raw/date_dim.preql' does not exist.
  ```

### `join-resolution`

- `trilogy run --import raw.store_sales:ss --import raw.catalog_sales:cs --import raw.web_sales:ws select sum(ss.quantity * ss.list_price) + sum(cs.quantity * c… + sum(ws.row_counter) as cnt where ss.date.year between 1999 and 2001 and cs.date.year between 1999 and 2001 and ws.date.year between 1999 and 2001;`

  ```text
  Resolution error in stdin: Could not resolve connections for query with output ['local.total<Purpose.METRIC>Derivation.BASIC>', 'local.cnt<Purpose.METRIC>Derivation.BASIC>'] from current model.
  ```
- `trilogy run query59.preql`

  ```text
  Resolution error in query59.preql: Could not resolve connections for query with output ['local._next_year_store_name<Purpose.PROPERTY>Derivation.BASIC>', 'local._next_year_store_code<Purpose.PROPERTY>Derivation.BASIC>', 'local._next_year_wk_seq<Purpose.PROPERTY>Derivation.BASIC>', 'local._next_year_sun<Purpose.METRIC>Derivation.AGGREGATE>', 'local._next_year_mon<Purpose.METRIC>Derivation.AGGREGATE>', 'local._next_year_tue<Purpose.METRIC>Derivation.AGGREGATE>', 'local._next_year_wed<Purpose.METRIC>Derivation.AGGREGATE>', 'local._next_year_thu<Purpose.METRIC>Derivation.AGGREGATE>', 'local._next_year_fri<Purpose.METRIC>Derivation.AGGREGATE>', 'local._next_year_sat<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run query59.preql`

  ```text
  Resolution error in query59.preql: Could not resolve connections for query with output ['local._next_year_store_name<Purpose.PROPERTY>Derivation.BASIC>', 'local._next_year_store_code<Purpose.PROPERTY>Derivation.BASIC>', 'local._next_year_wk_seq<Purpose.PROPERTY>Derivation.BASIC>', 'local._next_year_sun<Purpose.METRIC>Derivation.AGGREGATE>', 'local._next_year_mon<Purpose.METRIC>Derivation.AGGREGATE>', 'local._next_year_tue<Purpose.METRIC>Derivation.AGGREGATE>', 'local._next_year_wed<Purpose.METRIC>Derivation.AGGREGATE>', 'local._next_year_thu<Purpose.METRIC>Derivation.AGGREGATE>', 'local._next_year_fri<Purpose.METRIC>Derivation.AGGREGATE>', 'local._next_year_sat<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run query59.preql`

  ```text
  Resolution error in query59.preql: Could not resolve connections for query with output ['local._next_year_store_name<Purpose.PROPERTY>Derivation.BASIC>', 'local._next_year_store_code<Purpose.PROPERTY>Derivation.BASIC>', 'local._next_year_wk_seq<Purpose.PROPERTY>Derivation.BASIC>', 'local._next_year_sun<Purpose.METRIC>Derivation.AGGREGATE>', 'local._next_year_mon<Purpose.METRIC>Derivation.AGGREGATE>', 'local._next_year_tue<Purpose.METRIC>Derivation.AGGREGATE>', 'local._next_year_wed<Purpose.METRIC>Derivation.AGGREGATE>', 'local._next_year_thu<Purpose.METRIC>Derivation.AGGREGATE>', 'local._next_year_fri<Purpose.METRIC>Derivation.AGGREGATE>', 'local._next_year_sat<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run query59.preql`

  ```text
  Resolution error in query59.preql: Could not resolve connections for query with output ['local._next_year_store_name<Purpose.PROPERTY>Derivation.BASIC>', 'local._next_year_store_code<Purpose.PROPERTY>Derivation.BASIC>', 'local._next_year_wk_seq<Purpose.PROPERTY>Derivation.BASIC>', 'local._next_year_sun<Purpose.METRIC>Derivation.AGGREGATE>', 'local._next_year_mon<Purpose.METRIC>Derivation.AGGREGATE>', 'local._next_year_tue<Purpose.METRIC>Derivation.AGGREGATE>', 'local._next_year_wed<Purpose.METRIC>Derivation.AGGREGATE>', 'local._next_year_thu<Purpose.METRIC>Derivation.AGGREGATE>', 'local._next_year_fri<Purpose.METRIC>Derivation.AGGREGATE>', 'local._next_year_sat<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run test_join.preql`

  ```text
  Resolution error in test_join.preql: Could not resolve connections for query with output ['local._next_year_store_name<Purpose.PROPERTY>Derivation.BASIC>', 'local._next_year_wk_seq<Purpose.PROPERTY>Derivation.BASIC>', 'local._next_year_cnt<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```

### `type-error`

- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Type error in query08.preql: Invalid argument type 'ArrayType<STRING>' passed into SUBSTRING function in position 1 from concept: qualifying_zips.zip_val. Valid: 'STRING'.
  ```
- `trilogy run --import raw/all_sales:s select s.channel, count(s.order_id) as cnt, sum(s.sales_price) as sales, sum(s.return_amount) as returns, sum(s.net_profit) as profit where s.date.date between '2000-08-23' and '2000-09-22' and s.item.current_price > 50 and s.promotion.channel_tv = 'N' limit 10;`

  ```text
  Syntax error in stdin: Cannot use BETWEEN with incompatible types DATE and STRING (low)
  ```
- `trilogy run query87.preql`

  ```text
  Unexpected error in query87.preql: Tuple elements have incompatible types STRING and DATE
  ```
- `trilogy run query87.preql`

  ```text
  Unexpected error in query87.preql: Tuple elements have incompatible types STRING and DATE
  ```

### `syntax-missing-alias`

- `trilogy run --import raw/store_sales:ss --import raw/web_sales:ws select ss.customer.id, sum(ss.ext_sales_price) where ss.customer.id in (select ws.billing_c…and ss.date.month_seq between 1188 and 1190 and ss.store.county = ss.customer.address.county and ss.store.state = ss.customer.address.state limit 20;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `sum(ss.ext_sales_price) as ext_sales_price_sum`
  Location:
  ...r.id, sum(ss.ext_sales_price) ??? where ss.customer.id in (selec...
  ```
- `trilogy run --import raw/store_sales:ss select ss.is_returned, count(ss.line_item) limit 5;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(ss.line_item) as line_item_count`
  Location:
  ...returned, count(ss.line_item) ??? limit 5;
  ```
