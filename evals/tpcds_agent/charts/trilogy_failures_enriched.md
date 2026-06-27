# Trilogy failure analysis — 20260627-181845

- Run `20260627-181845` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 1358 | failed: 149 (11%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 97 | 65% |
| `syntax-parse` | 32 | 21% |
| `cli-misuse` | 7 | 5% |
| `syntax-missing-alias` | 7 | 5% |
| `type-error` | 2 | 1% |
| `file-not-found` | 2 | 1% |
| `join-resolution` | 2 | 1% |

## Detail

### `other`

- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query04.preql`

  ```text
  Syntax error in query04.preql: Undefined concept: sa.customer_id. Suggestions: ['store_annual.customer_id', 'catalog_annual.customer_id', 'web_annual.customer_id', 'store_cust_2002.customer_id', 'ss.customer.id', 'ss.customer.text_id']
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query06.preql`

  ```text
  Syntax error in query06.preql: 2 undefined concept references; fix all before re-running:
    - item.category (line 11, col 7, in WHERE); did you mean: store_sales.item.category, store_sales.item.category_id?
    - item.current_price (line 13, col 7, in WHERE); did you mean: store_sales.item.current_price?
  ```
- `trilogy run query06.preql`

  ```text
  Resolution error in query06.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 11). The requested concepts split into 2 disconnected subgraphs: {local.cat_avg_price}; {local.line_item_count, local.state, store_sales.customer.address.id, store_sales.date.month_of_year, store_sales.date.year, store_sales.item.category, store_sales.item.current_price}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query06.preql`

  ```text
  Syntax error in query06.preql: Undefined concept: item.category. Suggestions: ['store_sales.item.category', 'store_sales.item.category_id']
  ```
- `trilogy run query06.preql`

  ```text
  Resolution error in query06.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 17). The requested concepts split into 2 disconnected subgraphs: {local.cat_avg_price}; {local.line_item_count, local.state, store_sales.customer.address.id, store_sales.date.month_of_year, store_sales.date.year, store_sales.item.category, store_sales.item.current_price}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Syntax error in query08.preql: Undefined concept: customer.preferred_cust_flag. Suggestions: ['store_sales.customer.preferred_cust_flag', 'store_sales.return_customer.preferred_cust_flag']
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Unexpected error in query08.preql: (_duckdb.BinderException) Binder Error: Cannot compare values of type VARCHAR and VARCHAR[] in IN/ANY/ALL clause - an explicit cast is required

  LINE 29: ...tionable"."store_sales_customer_address_zip" ELSE NULL END in (select quizzical."_virt_func_split_4785012549328100...
                                                                         ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "store_sales_customer_address_customer_address"."CA_ZIP" as "store_sales_customer_address_zip",
      count(CASE WHEN "store_sales_customer_customers"."C_PREFERRED_CUST_FLAG" = 'Y' THEN "store_sales_customer_customers"."C_CUSTOMER_SK" ELSE NULL END) as "_virt_agg_count_5181521508845034"
  FROM
      "customer" as "store_sales_customer_customers"
      INNER JOIN "customer_address" as "store_sales_customer_address_customer_address" on "store_sales_customer_customers"."C_CURRENT_ADDR_SK" = "store_sales_customer_address_customer_address"."CA_ADDRESS_SK"
  GROUP BY
      1),
  quizzical as (
  SELECT
      STRING_SPLIT( $1 , ',' ) as "_virt_func_split_4785012549328100"
  ),
  questionable as (
  SELECT
      "cheerful"."_virt_agg_count_5181521508845034" as "_virt_agg_count_5181521508845034",
      "store_sales_customer_address_customer_address"."CA_ZIP" as "store_sales_customer_address_zip"
  FROM
      "cheerful"
      INNER JOIN "customer_address" as "store_sales_customer_address_customer_address" on "cheerful"."store_sales_customer_address_zip" = "store_sales_customer_address_customer_address"."CA_ZIP"),
  abundant as (
  SELECT
      CASE WHEN ( "questionable"."_virt_agg_count_5181521508845034" ) > 10 THEN "questionable"."store_sales_customer_address_zip" ELSE NULL END as "qualifying_zip"
  FROM
      "questionable"
  WHERE
      CASE WHEN ( "questionable"."_virt_agg_count_5181521508845034" ) > 10 THEN "questionable"."store_sales_customer_address_zip" ELSE NULL END in (select quizzical."_virt_func_split_4785012549328100" from quizzical where quizzical."_virt_func_split_4785012549328100" is not null)
  ),
  uneven as (
  SELECT
      SUBSTRING("abundant"."qualifying_zip",1,2) as "_virt_func_substring_1010830310934997"
  FROM
      "abundant"
  GROUP BY
      1)
  SELECT
      "store_sales_store_store"."S_STORE_NAME" as "store_name",
      sum("store_sales_store_sales"."SS_NET_PROFIT") as "total_net_profit"
  FROM
      "store_sales" as "store_sales_store_sales"
      INNER JOIN "date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
      INNER JOIN "store" as "store_sales_store_store" on "store_sales_store_sales"."SS_STORE_SK" = "store_sales_store_store"."S_STORE_SK"
  WHERE
      "store_sales_date_date"."D_YEAR" = 1998 and "store_sales_date_date"."D_QOY" = 2 and SUBSTRING("store_sales_store_store"."S_ZIP",1,2) in (select uneven."_virt_func_substring_1010830310934997" from uneven where uneven."_virt_func_substring_1010830310934997" is not null)

  GROUP BY
      1
  ORDER BY
      "store_name" asc
  LIMIT (100)]
  [parameters: ('24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,28577,55565,17183,54601,67897,22752 ... (2101 characters truncated) ... 26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576',)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy file read query08.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query10.preql`

  ```text
  Resolution error in query10.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 10). The requested concepts split into 3 disconnected subgraphs: {cs.ship_customer.id}; {local.customer_count, local.customer_count2, local.customer_count3, local.customer_count4, local.customer_count5, local.customer_count6, ss.customer.address.county, ss.customer.demographics.college_dependent_count, ss.customer.demographics.credit_rating, ss.customer.demographics.dependent_count, ss.customer.demographics.education_status, ss.customer.demographics.employed_dependent_count, ss.customer.demographics.gender, ss.customer.demographics.id, ss.customer.demographics.marital_status, ss.customer.demographics.purchase_estimate, ss.date.month_of_year, ss.date.year}; {ws.billing_customer.id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query10.preql`

  ```text
  Syntax error in query10.preql: 2 undefined concept references; fix all before re-running:
    - web_customers.billing_customer_id (line 27, col 26, in WHERE); did you mean: web_customers.ws.billing_customer.id, ws.billing_customer.id, ws.billing_customer.text_id?
    - catalog_customers.ship_customer_id (line 27, col 81, in WHERE); did you mean: catalog_customers.cs.ship_customer.id, web_customers.billing_customer_id, web_customers.ws.billing_customer.id?
  ```
- `trilogy file read query10.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query11.preql`

  ```text
  Syntax error in query11.preql: Ambiguous reference 'base_customers.cid': matches ['base_customers.store_rev.cid', 'base_customers.web_rev.cid']. Qualify the full path to disambiguate.
  ```
- `trilogy run query13.preql`

  ```text
  Syntax error in query13.preql: Comparison `store_sales.customer_demographic.marital_status = 'Married'` can never match enum field 'store_sales.customer_demographic.marital_status', which contains only these values: 'M', 'S', 'D', 'W', 'U'. It is always false and should be removed.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: HAVING references 's.channel', which is not in the SELECT projection (line 8). To make them available, you may add it to  the SELECT. Prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --s.channel
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query14.preql`

  ```text
  Unexpected error in query14.preql: Tuple must have same type for all elements
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: HAVING references 'local.overall_avg_sale', which is not in the SELECT projection (line 19). To make them available, you may add it to  the SELECT. Prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.overall_avg_sale
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy file read query14.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query18.preql`

  ```text
  Syntax error in query18.preql: Comparison `cs.bill_customer_demographic.gender = 'Female'` can never match enum field 'cs.bill_customer_demographic.gender', which contains only these values: 'M', 'F'. It is always false and should be removed.
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: HAVING references 'max_row.max_val', which is not in the SELECT projection (line 31). To make them available, you may add it to  the SELECT. Prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --max_row.max_val
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query24.preql`

  ```text
  Syntax error in query24.preql: HAVING references 'local.first_stage_avg', which is not in the SELECT projection (line 27). To make them available, you may add it to  the SELECT. Prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.first_stage_avg
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query25.preql --import raw.store_sales:store_sales select store_sales.date.year, store_sales.date.month_of_year, count(store_sales.line_item) as cnt where store_sales.date.year = 2001 and store_sales.date.month_of_year = 4 limit 5;`

  ```text
  --import only applies to inline queries, not file/directory inputs.
  ```
- `trilogy run query25.preql`

  ```text
  Syntax error in query25.preql: 4 undefined concept references; fix all before re-running:
    - local.item_code (line 45, col 10, in ORDER BY); did you mean: store_sales_with_returns.item_code, store_code?
    - local.item_description (line 45, col 21, in ORDER BY); did you mean: store_sales_with_returns.item_description?
    - local.store_code (line 45, col 39, in ORDER BY); did you mean: store_sales_with_returns.store_code, item_code, store_name?
    - local.store_name (line 45, col 51, in ORDER BY); did you mean: store_sales_with_returns.store_name, store_code, store_sales.store.name, store_sales.time.id?
  ```
- `trilogy file read query25.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query27.preql`

  ```text
  Syntax error in query27.preql: 3 undefined concept references; fix all before re-running:
    - ss.customer_demographics.gender (line 6, col 9, in WHERE); did you mean: ss.customer.demographics.gender, ss.return_customer.demographics.gender, ss.customer_demographic.gender, ss.customer_demographic.id?
    - ss.customer_demographics.marital_status (line 7, col 9, in WHERE); did you mean: ss.customer.demographics.marital_status, ss.return_customer.demographics.marital_status, ss.customer_demographic.marital_status?
    - ss.customer_demographics.education_status (line 8, col 9, in WHERE); did you mean: ss.customer.demographics.education_status, ss.return_customer.demographics.education_status, ss.customer_demographic.education_status?
  ```
- `trilogy run query29.preql`

  ```text
  Syntax error in query29.preql: Comparison `sr.return_date.month_of_year <= 12` matches every value of enum field 'sr.return_date.month_of_year', which contains only these values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12. It is always true and should be removed.
  ```
- `trilogy run query30.preql`

  ```text
  Syntax error in query30.preql: 11 undefined concept references; fix all before re-running:
    - local.salutation (line 26, col 25, in ORDER BY); did you mean: wr.web_sales.billing_customer.salutation, wr.web_sales.ship_customer.salutation, wr.web_sales.return_customer.salutation, wr.web_sales.return_refund_customer.salutation, wr.billing_customer.salutation, wr.refunded_customer.salutation?
    - local.first_name (line 26, col 37, in ORDER BY); did you mean: wr.web_sales.billing_customer.first_name, wr.web_sales.ship_customer.first_name, wr.web_sales.return_customer.first_name, wr.web_sales.return_refund_customer.first_name, wr.billing_customer.first_name, wr.refunded_customer.first_name?
    - local.last_name (line 26, col 49, in ORDER BY); did you mean: wr.web_sales.billing_customer.last_name, wr.web_sales.ship_customer.last_name, wr.web_sales.return_customer.last_name, wr.web_sales.return_refund_customer.last_name, wr.billing_customer.last_name, wr.refunded_customer.last_name?
    - local.preferred_cust_flag (line 26, col 60, in ORDER BY); did you mean: wr.web_sales.billing_customer.preferred_cust_flag, wr.web_sales.ship_customer.preferred_cust_flag, wr.web_sales.return_customer.preferred_cust_flag, wr.web_sales.return_refund_customer.preferred_cust_flag, wr.billing_customer.preferred_cust_flag, wr.refunded_customer.preferred_cust_flag?
    - local.birth_day (line 26, col 81, in ORDER BY); did you mean: wr.web_sales.billing_customer.birth_day, wr.web_sales.ship_customer.birth_day, wr.web_sales.return_customer.birth_day, wr.web_sales.return_refund_customer.birth_day, wr.billing_customer.birth_day, wr.refunded_customer.birth_day?
    - local.birth_month (line 26, col 92, in ORDER BY); did you mean: wr.web_sales.billing_customer.birth_month, wr.web_sales.ship_customer.birth_month, wr.web_sales.return_customer.birth_month, wr.web_sales.return_refund_customer.birth_month, wr.billing_customer.birth_month, wr.refunded_customer.birth_month?
    - local.birth_year (line 26, col 105, in ORDER BY); did you mean: wr.web_sales.billing_customer.birth_year, wr.web_sales.ship_customer.birth_year, wr.web_sales.return_customer.birth_year, wr.web_sales.return_refund_customer.birth_year, wr.billing_customer.birth_year, wr.refunded_customer.birth_year?
    - local.birth_country (line 26, col 117, in ORDER BY); did you mean: wr.web_sales.billing_customer.birth_country, wr.web_sales.ship_customer.birth_country, wr.web_sales.return_customer.birth_country, wr.web_sales.return_refund_customer.birth_country, wr.billing_customer.birth_country, wr.refunded_customer.birth_country?
    - local.login (line 26, col 132, in ORDER BY); did you mean: wr.web_sales.billing_customer.login, wr.web_sales.ship_customer.login, wr.web_sales.return_customer.login, wr.web_sales.return_refund_customer.login, wr.billing_customer.login, wr.refunded_customer.login?
    - local.email_address (line 26, col 139, in ORDER BY); did you mean: wr.web_sales.billing_customer.email_address, wr.web_sales.ship_customer.email_address, wr.web_sales.return_customer.email_address, wr.web_sales.return_refund_customer.email_address, wr.billing_customer.email_address, wr.refunded_customer.email_address?
    - local.last_review_date (line 26, col 154, in ORDER BY); did you mean: wr.web_sales.billing_customer.last_review_date, wr.web_sales.ship_customer.last_review_date, wr.web_sales.return_customer.last_review_date, wr.web_sales.return_refund_customer.last_review_date, wr.billing_customer.last_review_date, wr.refunded_customer.last_review_date?
  ```
- `trilogy run query30.preql`

  ```text
  Syntax error in query30.preql: HAVING references 'local.avg_state_amount', which is not in the SELECT projection (line 9). To make them available, you may add it to  the SELECT. Prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.avg_state_amount
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 58 column 3 (char 2221). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query35.preql`

  ```text
  Resolution error in query35.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 31). The requested concepts split into 2 disconnected subgraphs: {local.avg_col, local.avg_dep, local.avg_emp, local.college_dependent_count, local.dependent_count, local.employed_dependent_count, local.gender, local.marital_status, local.max_col, local.max_dep, local.max_emp, local.min_col, local.min_dep, local.min_emp, local.row_count, local.state}; {store_sales.customer.id, store_sales.date.quarter, store_sales.date.year}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read query35.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query38.preql`

  ```text
  Resolution error in query38.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 31). The requested concepts split into 3 disconnected subgraphs: {catalog_combos.first_name, catalog_combos.last_name, catalog_combos.sale_date}; {store_combos.first_name, store_combos.last_name, store_combos.sale_date}; {web_combos.first_name, web_combos.last_name, web_combos.sale_date}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query39.preql`

  ```text
  Resolution error in query39.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 8). The requested concepts split into 4 disconnected subgraphs: {d.year, local._monthly_stats_mon}; {local._monthly_stats_item_id}; {local._monthly_stats_mn, local._monthly_stats_sd}; {local._monthly_stats_warehouse_id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query39.preql`

  ```text
  Resolution error in query39.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 8). The requested concepts split into 4 disconnected subgraphs: {d.year, local._monthly_stats_mon}; {local._monthly_stats_item_id}; {local._monthly_stats_mn, local._monthly_stats_sd}; {local._monthly_stats_warehouse_id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/catalog_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query43.preql`

  ```text
  Syntax error in query43.preql: Undefined concept: date.year (line 4, col 3, in WHERE). Suggestions: ['sales.date.year', 'sales.store.date.year', 'sales.return_store.date.year', 'sales.return_date.year', 'sales.customer.first_sales_date.year', 'sales.customer.first_shipto_date.year']
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query44.preql`

  ```text
  Syntax error in query44.preql: HAVING references 'local.threshold', which is not in the SELECT projection (line 14). To make them available, you may add it to  the SELECT. Prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.threshold
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query44.preql`

  ```text
  Syntax error in query44.preql: HAVING references 'local.threshold', which is not in the SELECT projection (line 11). To make them available, you may add it to  the SELECT. Prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.threshold
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query46.preql`

  ```text
  Syntax error in query46.preql: 11 undefined concept references; fix all before re-running:
    - customer.last_name (line 11, col 3, in SELECT); did you mean: ss.customer.last_name, ss.return_customer.last_name, customer.first_name, ss.customer.first_name?
    - customer.first_name (line 12, col 3, in SELECT); did you mean: ss.customer.first_name, ss.return_customer.first_name, customer.last_name, ss.customer.last_name?
    - date.year (line 4, col 3, in WHERE); did you mean: ss.date.year, ss.store.date.year, ss.return_store.date.year, ss.return_date.year, ss.customer.first_sales_date.year, ss.customer.first_shipto_date.year?
    - date.day_of_week (line 5, col 7, in WHERE); did you mean: ss.date.day_of_week, ss.store.date.day_of_week, ss.return_store.date.day_of_week, ss.return_date.day_of_week, ss.customer.first_sales_date.day_of_week, ss.customer.first_shipto_date.day_of_week?
    - store.city (line 6, col 7, in WHERE); did you mean: ss.store.city, ss.customer.address.city, ss.return_customer.address.city, ss.return_store.city, ss.sale_address.city, customer.address.city?
    - household_demographic.dependent_count (line 7, col 8, in WHERE); did you mean: ss.customer.household_demographic.dependent_count, ss.return_customer.household_demographic.dependent_count, ss.household_demographic.dependent_count, ss.customer.demographics.dependent_count, ss.return_customer.demographics.dependent_count, ss.customer_demographic.dependent_count?
    - household_demographic.vehicle_count (line 7, col 53, in WHERE); did you mean: ss.customer.household_demographic.vehicle_count, ss.return_customer.household_demographic.vehicle_count, ss.household_demographic.vehicle_count, household_demographic.dependent_count?
    - customer.address.city (line 8, col 7, in WHERE); did you mean: ss.customer.address.city, ss.return_customer.address.city, ss.store.city, ss.return_store.city, ss.sale_address.city, store.city?
    - sale_address.city (line 8, col 32, in WHERE); did you mean: ss.sale_address.city, ss.customer.address.city, ss.return_customer.address.city, ss.store.city, ss.return_store.city, store.city?
    - customer.last_name (line 20, col 3, in ORDER BY); did you mean: ss.customer.last_name, ss.return_customer.last_name, customer.first_name, ss.customer.first_name?
    - customer.first_name (line 21, col 3, in ORDER BY); did you mean: ss.customer.first_name, ss.return_customer.first_name, customer.last_name, ss.customer.last_name?
  ```
- `trilogy run query53.preql`

  ```text
  Syntax error in query53.preql: Output column 'avg_quarterly_sales' renames 'local.avg_quarterly_sales' back to the name of an existing concept 'avg_quarterly_sales' (defined at line 23) that 'local.avg_quarterly_sales' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'avg_quarterly_sales_out').
  ```
- `trilogy file read query53.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query56.preql`

  ```text
  Resolution error in query56.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 5). The requested concepts split into 2 disconnected subgraphs: {item.color, local.item_code}; {local.total}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run --import raw.all_sales:all_sales select date.date, date.week_seq where date.date = '2000-01-03'::date limit 10;`

  ```text
  Syntax error in stdin: 3 undefined concept references; fix all before re-running:
    - date.date (line 2, col 8, in SELECT); did you mean: all_sales.date.date, all_sales.return_date.date, all_sales.billing_customer.first_sales_date.date, all_sales.billing_customer.first_shipto_date.date, all_sales.ship_customer.first_sales_date.date, all_sales.ship_customer.first_shipto_date.date?
    - date.week_seq (line 2, col 19, in SELECT); did you mean: all_sales.date.week_seq, all_sales.return_date.week_seq, all_sales.billing_customer.first_sales_date.week_seq, all_sales.billing_customer.first_shipto_date.week_seq, all_sales.ship_customer.first_sales_date.week_seq, all_sales.ship_customer.first_shipto_date.week_seq?
    - date.date (line 2, col 39, in WHERE); did you mean: all_sales.date.date, all_sales.return_date.date, all_sales.billing_customer.first_sales_date.date, all_sales.billing_customer.first_shipto_date.date, all_sales.ship_customer.first_sales_date.date, all_sales.ship_customer.first_shipto_date.date?
  ```
- `trilogy run query58.preql`

  ```text
  Syntax error in query58.preql: Output column 'store_total' renames 'local.store_total' back to the name of an existing concept 'store_total' (defined at line 4) that 'local.store_total' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'store_total_out').
  ```
- `trilogy run query58.preql`

  ```text
  Syntax error in query58.preql: Output column 'store_total' renames 'local.store_total' back to the name of an existing concept 'store_total' (defined at line 4) that 'local.store_total' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'store_total_out').
  ```
- `trilogy file read query58.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query59.preql duckdb`

  ```text
  Syntax error in query59.preql: 2 undefined concept references; fix all before re-running:
    - store_name (line 63, col 10, in ORDER BY); did you mean: this_year.store_name, next_year.store_name, ss.store.name, ss.store.street_name?
    - store_code (line 63, col 38, in ORDER BY); did you mean: this_year.store_code, next_year.store_code, _this_year_store_code?
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 56 column 3 (char 2697). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 27). The requested concepts split into 3 disconnected subgraphs: {item_cat_ext_list.cat_ext_list_sum}; {item_cat_refund.cat_refund_sum}; {local.coupon_amt_sum, local.cust_city, local.cust_street_name, local.cust_street_num, local.cust_zip, local.first_sales_year, local.first_shipto_year, local.item_id, local.line_cnt, local.list_price_sum, local.sale_city, local.sale_street_name, local.sale_street_num, local.sale_year, local.sale_zip, local.store_name, local.store_zip, local.wholesale_cost_sum, ss.customer.demographics.marital_status, ss.customer_demographic.marital_status, ss.item.color, ss.item.current_price, ss.item.id, ss.item.product_name, ss.ticket_number}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: HAVING references 'local.cat_ext_list_sum', 'local.cat_refund_sum', which are not in the SELECT projection (line 28). To make them available, you may add them to  the SELECT. Prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.cat_ext_list_sum, --local.cat_refund_sum
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 28). The requested concepts split into 3 disconnected subgraphs: {item_cat_ext_list.cat_ext_list_sum}; {item_cat_refund.cat_refund_sum}; {local.coupon_amt_sum, local.cust_city, local.cust_street_name, local.cust_street_num, local.cust_zip, local.first_sales_year, local.first_shipto_year, local.item_id, local.line_cnt, local.list_price_sum, local.sale_city, local.sale_street_name, local.sale_street_num, local.sale_year, local.sale_zip, local.store_name, local.store_zip, local.wholesale_cost_sum, ss.customer.demographics.marital_status, ss.customer_demographic.marital_status, ss.item.color, ss.item.current_price, ss.item.id, ss.item.product_name, ss.ticket_number}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Undefined concept: item_cat_ext_list. Suggestions: ['item_cat_ext_list.item_id', '_item_cat_ext_list_item_id', 'item_cat_ext_list.cat_ext_list_sum']
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: HAVING references 'item_cat_ext_list.cat_ext_list_sum', 'item_cat_refund.cat_refund_sum', which are not in the SELECT projection (line 23). To make them available, you may add them to  the SELECT. Prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --item_cat_ext_list.cat_ext_list_sum, --item_cat_refund.cat_refund_sum
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 23). The requested concepts split into 3 disconnected subgraphs: {item_cat_ext_list.cat_ext_list_sum}; {item_cat_refund.cat_refund_sum}; {local.coupon_amt_sum, local.cust_city, local.cust_street_name, local.cust_street_num, local.cust_zip, local.first_sales_year, local.first_shipto_year, local.item_id, local.line_cnt, local.list_price_sum, local.product_name, local.sale_city, local.sale_street_name, local.sale_street_num, local.sale_year, local.sale_zip, local.store_name, local.store_zip, local.wholesale_cost_sum, ss.customer.demographics.marital_status, ss.customer_demographic.marital_status, ss.item.color, ss.item.current_price, ss.item.id, ss.ticket_number}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 78). The requested concepts split into 2 disconnected subgraphs: {cat_qual_items.cat_ext_list_sum, cat_qual_items.cat_refund_sum}; {y1999.coupon_amt_sum, y1999.cust_city, y1999.cust_street_name, y1999.cust_street_num, y1999.cust_zip, y1999.item_id, y1999.line_cnt, y1999.list_price_sum, y1999.product_name, y1999.sale_city, y1999.sale_street_name, y1999.sale_street_num, y1999.sale_zip, y1999.store_name, y1999.store_zip, y1999.wholesale_cost_sum, y2000.coupon_amt_sum, y2000.item_id, y2000.line_cnt, y2000.list_price_sum, y2000.wholesale_cost_sum}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: HAVING references 'cat_qual_items.cat_ext_list_sum', 'cat_qual_items.cat_refund_sum', which are not in the SELECT projection (line 24). To make them available, you may add them to  the SELECT. Prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --cat_qual_items.cat_ext_list_sum, --cat_qual_items.cat_refund_sum
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query64.preql`

  ```text
  zero-length delimited identifier at or near \"\"\"\"\n\nLINE 253: ...\".\"id_ss\".\"ticket_number_at_ss_item_id_ss_ticket_number']\".\"\" in (select juicy.\"cat_qual_items_item_id\" from juicy...\n                                                                        ^\n[SQL: \nWITH \nwakeful as (\nSELECT\n    \"cr_catalog_returns\".\"CR_ITEM_SK\" as \"cr_item_id\",\n    sum(( coalesce(\"cr_catalog_returns\".\"CR_REFUNDED_CASH\",0) + coalesce(\"cr_catalog_returns\".\"CR_REVERSED_CHARGE\",0) ) + coalesce(\"cr_catalog_returns\".\"CR_STORE_CREDIT\",0)) as \"_cat_qual_items_cat_refund_sum\"\nFRO
  …
  "y2000_store_zip\" = \"wooden\".\"y1999_store_zip\"\nWHERE\n    \"obsolete\".\"y2000_line_cnt\" <= \"wooden\".\"y1999_line_cnt\"\n\nORDER BY \n    \"wooden\".\"y1999_product_name\" asc,\n    \"wooden\".\"y1999_store_name\" asc,\n    \"line_cnt2000\" asc,\n    \"ws1999\" asc,\n    \"ws2000\" asc\nLIMIT (1000)]\n(Background on this error at: https://sqlalche.me/e/20/f405)",
    "error_type": "ProgrammingError"
  }
  {
    "event": "output_truncated",
    "dropped_events": 2,
    "note": "Output exceeded the tool cap; trailing events dropped. Narrow the call (--regex, --show, fewer rows) to see the rest."
  }
  ```
- `trilogy run query65.preql`

  ```text
  Syntax error in query65.preql: ORDER BY references 'item_rev.store_id', which is not in the SELECT projection (line 21). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --item_rev.store_id order by item_rev.store_id asc`.
  ```
- `trilogy file read raw/catalog_sales.preql`

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
- `trilogy file read query69.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query70.preql`

  ```text
  Syntax error in query70.preql: HAVING references 'local.g_state', which is not in the SELECT projection (line 31). To make them available, you may add it to  the SELECT. Prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.g_state
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query72.preql`

  ```text
  Syntax error in query72.preql: HAVING references 'inv.quantity_on_hand', 'cs.quantity', which are not in the SELECT projection (line 4). To make them available, you may add them to  the SELECT. Prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --inv.quantity_on_hand, --cs.quantity
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy file read query72.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query73.preql`

  ```text
  Syntax error in query73.preql: ORDER BY references 'ss.customer.id', which is not in the SELECT projection (line 3). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --ss.customer.id order by ss.customer.id asc`.
  ```
- `trilogy run query74.preql`

  ```text
  Resolution error in query74.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 25). The requested concepts split into 2 disconnected subgraphs: {store_by_cust.cust_code, store_by_cust.fname, store_by_cust.lname, store_by_cust.store_2001, store_by_cust.store_2002}; {web_by_cust.cust_code, web_by_cust.web_2001, web_by_cust.web_2002}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query78.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query81.preql`

  ```text
  Syntax error in query81.preql: HAVING references 'local.state_avg', which is not in the SELECT projection (line 9). To make them available, you may add it to  the SELECT. Prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.state_avg
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query82.preql`

  ```text
  Syntax error in query82.preql: 4 undefined concept references; fix all before re-running:
    - item.current_price (line 10, col 7, in WHERE); did you mean: store_sales.item.current_price, inventory.item.current_price, current_price?
    - item.manufacturer_id (line 11, col 7, in WHERE); did you mean: store_sales.item.manufacturer_id, inventory.item.manufacturer_id, item.text_id?
    - item.id (line 12, col 7, in WHERE); did you mean: store_sales.item.id, inventory.item.id, store_sales.date.id, store_sales.return_date.id, store_sales.time.id, store_sales.return_time.id?
    - item.text_id (line 21, col 10, in ORDER BY); did you mean: store_sales.item.text_id, inventory.item.text_id, store_sales.date.text_id, store_sales.return_date.text_id, store_sales.time.text_id, store_sales.return_time.text_id?
  ```
- `trilogy run query82.preql`

  ```text
  Resolution error in query82.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 10). The requested concepts split into 2 disconnected subgraphs: {inventory.date.date, inventory.quantity_on_hand}; {local.current_price, local.description, local.item_code, store_sales.item.current_price, store_sales.item.id, store_sales.item.manufacturer_id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query83.preql`

  ```text
  Syntax error in query83.preql: Ambiguous reference 'all_items.item_code': matches ['all_items.catalog_agg.item_code', 'all_items.store_agg.item_code', 'all_items.web_agg.item_code']. Qualify the full path to disambiguate.
  ```
- `trilogy run query83.preql`

  ```text
  Unexpected error in query83.preql: (_duckdb.BinderException) Binder Error: column "combined_store_agg_store_qty" must appear in the GROUP BY clause or must be part of an aggregate function.
  Either add it to the GROUP BY list, or use "ANY_VALUE(combined_store_agg_store_qty)" if the exact value of "combined_store_agg_store_qty" is not important.

  LINE 119:     "scrawny"."combined_store_agg_store_qty" asc nulls first
                ^
  [SQL:
  WITH
  abhorrent as (
  SELECT
      "wr_web_sales_item_items"."I_ITEM_ID" as "_web_agg_item_code",
      count("wr_web_returns"."WR_ORDER_NUMBER") as "_web_agg_web_row_count",
      sum("wr_web_returns"."WR_RETURN_QUANTITY") as "_web_agg_web_qty"
  FROM
      "web_returns" as "wr_web_returns"
      INNER JOIN "date_dim" as "wr_return_date_date" on "wr_web_returns"."WR_RETURNED_DATE_SK" = "wr_return_date_date"."D_DATE_SK"
      INNER JOIN "item" as "wr_web_sales_item_items" on "wr_web_returns"."WR_ITEM_SK" = "wr_web_sales_item_items"."I_ITEM_SK"
  WHERE
      "wr_return_date_date"."D_WEEK_SEQ" in (5244,5257,5264)

  GROUP BY
      1),
  yummy as (
  SELECT
      "sr_item_items"."I_ITEM_ID" as "_store_agg_item_code",
      count("sr_store_returns"."SR_RETURN_QUANTITY") as "_store_agg_store_non_null_qty_count",
      count("sr_store_returns"."SR_TICKET_NUMBER") as "_store_agg_store_row_count",
      sum("sr_store_returns"."SR_RETURN_QUANTITY") as "_store_agg_store_qty"
  FROM
      "store_returns" as "sr_store_returns"
      INNER JOIN "item" as "sr_item_items" on "sr_store_returns"."SR_ITEM_SK" = "sr_item_items"."I_ITEM_SK"
      INNER JOIN "date_dim" as "sr_return_date_date" on "sr_store_returns"."SR_RETURNED_DATE_SK" = "sr_return_date_date"."D_DATE_SK"
  WHERE
      "sr_return_date_date"."D_WEEK_SEQ" in (5244,5257,5264)

  GROUP BY
      1),
  cheerful as (
  SELECT
      "cr_item_items"."I_ITEM_ID" as "_catalog_agg_item_code",
      count("cr_catalog_returns"."CR_ORDER_NUMBER") as "_catalog_agg_catalog_row_count",
      count("cr_catalog_returns"."CR_RETURN_QUANTITY") as "_catalog_agg_catalog_non_null_qty_count",
      sum("cr_catalog_returns"."CR_RETURN_QUANTITY") as "_catalog_agg_catalog_qty"
  FROM
      "catalog_returns" as "cr_catalog_returns"
      INNER JOIN "date_dim" as "cr_date_date" on "cr_catalog_returns"."CR_RETURNED_DATE_SK" = "cr_date_date"."D_DATE_SK"
      INNER JOIN "item" as "cr_item_items" on "cr_catalog_returns"."CR_ITEM_SK" = "cr_item_items"."I_ITEM_SK"
  WHERE
      "cr_date_date"."D_WEEK_SEQ" in (5244,5257,5264)

  GROUP BY
      1),
  late as (
  SELECT
      "abhorrent"."_web_agg_item_code" as "web_agg_item_code",
      "abhorrent"."_web_agg_web_qty" as "web_agg_web_qty",
      "abhorrent"."_web_agg_web_row_count" as "web_agg_web_row_count"
  FROM
      "abhorrent"),
  vacuous as (
  SELECT
      "yummy"."_store_agg_item_code" as "store_agg_item_code",
      "yummy"."_store_agg_store_non_null_qty_count" as "store_agg_store_non_null_qty_count",
      "yummy"."_store_agg_store_qty" as "store_agg_store_qty",
      "yummy"."_store_agg_store_row_count" as "store_agg_store_row_count"
  FROM
      "yummy"),
  cooperative as (
  SELECT
      "cheerful"."_catalog_agg_catalog_non_null_qty_count" as "catalog_agg_catalog_non_null_qty_count",
      "cheerful"."_catalog_agg_catalog_qty" as "catalog_agg_catalog_qty",
      "cheerful"."_catalog_agg_catalog_row_count" as "catalog_agg_catalog_row_count",
      "cheerful"."_catalog_agg_item_code" as "catalog_agg_item_code"
  FROM
      "cheerful"),
  macho as (
  SELECT
      "cooperative"."catalog_agg_catalog_non_null_qty_count" as "catalog_agg_catalog_non_null_qty_count",
      "cooperative"."catalog_agg_catalog_qty" as "catalog_agg_catalog_qty",
      "cooperative"."catalog_agg_catalog_row_count" as "catalog_agg_catalog_row_count",
      "late"."web_agg_web_qty" as "web_agg_web_qty",
      "vacuous"."store_agg_item_code" as "store_agg_item_code",
      "vacuous"."store_agg_store_non_null_qty_count" as "store_agg_store_non_null_qty_count",
      "vacuous"."store_agg_store_qty" as "store_agg_store_qty",
      "vacuous"."store_agg_store_row_count" as "store_agg_store_row_count"
  FROM
      "late"
      INNER JOIN "cooperative" on "late"."web_agg_item_code" = "cooperative"."catalog_agg_item_code"
      INNER JOIN "vacuous" on "cooperative"."catalog_agg_item_code" = "vacuous"."store_agg_item_code" AND "late"."web_agg_item_code" = "vacuous"."store_agg_item_code"
  WHERE
      "vacuous"."store_agg_store_row_count" > 0 and "cooperative"."catalog_agg_catalog_row_count" > 0 and "late"."web_agg_web_row_count" > 0
  ),
  scrawny as (
  SELECT
      "macho"."catalog_agg_catalog_non_null_qty_count" as "combined_catalog_agg_catalog_non_null_qty_count",
      "macho"."catalog_agg_catalog_qty" as "combined_catalog_agg_catalog_qty",
      "macho"."catalog_agg_catalog_row_count" as "combined_catalog_agg_catalog_row_count",
      "macho"."store_agg_item_code" as "combined_store_agg_item_code",
      "macho"."store_agg_store_non_null_qty_count" as "combined_store_agg_store_non_null_qty_count",
      "macho"."store_agg_store_qty" as "combined_store_agg_store_qty",
      "macho"."store_agg_store_row_count" as "combined_store_agg_store_row_count",
      "macho"."web_agg_web_qty" as "combined_web_agg_web_qty"
  FROM
      "macho")
  SELECT
      "scrawny"."combined_store_agg_item_code" as "combined_store_agg_item_code",
      CASE
  	WHEN "scrawny"."combined_store_agg_store_non_null_qty_count" = "scrawny"."combined_store_agg_store_row_count" THEN "scrawny"."combined_store_agg_store_qty"
  	ELSE null
  	END as "store_return_quantity",
      CASE
  	WHEN "scrawny"."combined_catalog_agg_catalog_non_null_qty_count" = "scrawny"."combined_catalog_agg_catalog_row_count" THEN "scrawny"."combined_catalog_agg_catalog_qty"
  	ELSE null
  	END as "catalog_return_quantity",
      "scrawny"."combined_web_agg_web_qty" as "web_return_quantity"
  FROM
      "scrawny"
  GROUP BY
      1,
      2,
      3,
      4
  ORDER BY
      "scrawny"."combined_store_agg_item_code" asc nulls first,
      "scrawny"."combined_store_agg_store_qty" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query83.preql`

  ```text
  Syntax error in query83.preql: Undefined concept: combined.store_agg.item_code (line 63, col 10, in ORDER BY). Suggestions: ['store_agg.item_code', 'catalog_agg.item_code', 'web_agg.item_code', 'combined.item_code', 'item_code', 'combined.store_agg.store_qty']
  ```
- `trilogy run query83.preql`

  ```text
  Unexpected error in query83.preql: (_duckdb.BinderException) Binder Error: column "combined_store_qty" must appear in the GROUP BY clause or must be part of an aggregate function.
  Either add it to the GROUP BY list, or use "ANY_VALUE(combined_store_qty)" if the exact value of "combined_store_qty" is not important.

  LINE 119:     "scrawny"."combined_store_qty" asc nulls first
                ^
  [SQL:
  WITH
  abhorrent as (
  SELECT
      "wr_web_sales_item_items"."I_ITEM_ID" as "_web_agg_item_code",
      count("wr_web_returns"."WR_ORDER_NUMBER") as "_web_agg_web_row_count",
      sum("wr_web_returns"."WR_RETURN_QUANTITY") as "_web_agg_web_qty"
  FROM
      "web_returns" as "wr_web_returns"
      INNER JOIN "date_dim" as "wr_return_date_date" on "wr_web_returns"."WR_RETURNED_DATE_SK" = "wr_return_date_date"."D_DATE_SK"
      INNER JOIN "item" as "wr_web_sales_item_items" on "wr_web_returns"."WR_ITEM_SK" = "wr_web_sales_item_items"."I_ITEM_SK"
  WHERE
      "wr_return_date_date"."D_WEEK_SEQ" in (5244,5257,5264)

  GROUP BY
      1),
  yummy as (
  SELECT
      "sr_item_items"."I_ITEM_ID" as "_store_agg_item_code",
      count("sr_store_returns"."SR_RETURN_QUANTITY") as "_store_agg_store_qty_count",
      count("sr_store_returns"."SR_TICKET_NUMBER") as "_store_agg_store_row_count",
      sum("sr_store_returns"."SR_RETURN_QUANTITY") as "_store_agg_store_qty"
  FROM
      "store_returns" as "sr_store_returns"
      INNER JOIN "item" as "sr_item_items" on "sr_store_returns"."SR_ITEM_SK" = "sr_item_items"."I_ITEM_SK"
      INNER JOIN "date_dim" as "sr_return_date_date" on "sr_store_returns"."SR_RETURNED_DATE_SK" = "sr_return_date_date"."D_DATE_SK"
  WHERE
      "sr_return_date_date"."D_WEEK_SEQ" in (5244,5257,5264)

  GROUP BY
      1),
  cheerful as (
  SELECT
      "cr_item_items"."I_ITEM_ID" as "_catalog_agg_item_code",
      count("cr_catalog_returns"."CR_ORDER_NUMBER") as "_catalog_agg_catalog_row_count",
      count("cr_catalog_returns"."CR_RETURN_QUANTITY") as "_catalog_agg_catalog_qty_count",
      sum("cr_catalog_returns"."CR_RETURN_QUANTITY") as "_catalog_agg_catalog_qty"
  FROM
      "catalog_returns" as "cr_catalog_returns"
      INNER JOIN "date_dim" as "cr_date_date" on "cr_catalog_returns"."CR_RETURNED_DATE_SK" = "cr_date_date"."D_DATE_SK"
      INNER JOIN "item" as "cr_item_items" on "cr_catalog_returns"."CR_ITEM_SK" = "cr_item_items"."I_ITEM_SK"
  WHERE
      "cr_date_date"."D_WEEK_SEQ" in (5244,5257,5264)

  GROUP BY
      1),
  late as (
  SELECT
      "abhorrent"."_web_agg_item_code" as "web_agg_item_code",
      "abhorrent"."_web_agg_web_qty" as "web_agg_web_qty",
      "abhorrent"."_web_agg_web_row_count" as "web_agg_web_row_count"
  FROM
      "abhorrent"),
  vacuous as (
  SELECT
      "yummy"."_store_agg_item_code" as "store_agg_item_code",
      "yummy"."_store_agg_store_qty" as "store_agg_store_qty",
      "yummy"."_store_agg_store_qty_count" as "store_agg_store_qty_count",
      "yummy"."_store_agg_store_row_count" as "store_agg_store_row_count"
  FROM
      "yummy"),
  cooperative as (
  SELECT
      "cheerful"."_catalog_agg_catalog_qty" as "catalog_agg_catalog_qty",
      "cheerful"."_catalog_agg_catalog_qty_count" as "catalog_agg_catalog_qty_count",
      "cheerful"."_catalog_agg_catalog_row_count" as "catalog_agg_catalog_row_count",
      "cheerful"."_catalog_agg_item_code" as "catalog_agg_item_code"
  FROM
      "cheerful"),
  macho as (
  SELECT
      "cooperative"."catalog_agg_catalog_qty" as "_combined_catalog_qty",
      "cooperative"."catalog_agg_catalog_qty_count" as "_combined_catalog_qty_count",
      "cooperative"."catalog_agg_catalog_row_count" as "_combined_catalog_row_count",
      "late"."web_agg_web_qty" as "_combined_web_qty",
      "vacuous"."store_agg_item_code" as "_combined_item_code",
      "vacuous"."store_agg_store_qty" as "_combined_store_qty",
      "vacuous"."store_agg_store_qty_count" as "_combined_store_qty_count",
      "vacuous"."store_agg_store_row_count" as "_combined_store_row_count"
  FROM
      "late"
      INNER JOIN "cooperative" on "late"."web_agg_item_code" = "cooperative"."catalog_agg_item_code"
      INNER JOIN "vacuous" on "cooperative"."catalog_agg_item_code" = "vacuous"."store_agg_item_code" AND "late"."web_agg_item_code" = "vacuous"."store_agg_item_code"
  WHERE
      "vacuous"."store_agg_store_row_count" > 0 and "cooperative"."catalog_agg_catalog_row_count" > 0 and "late"."web_agg_web_row_count" > 0
  ),
  scrawny as (
  SELECT
      "macho"."_combined_catalog_qty" as "combined_catalog_qty",
      "macho"."_combined_catalog_qty_count" as "combined_catalog_qty_count",
      "macho"."_combined_catalog_row_count" as "combined_catalog_row_count",
      "macho"."_combined_item_code" as "combined_item_code",
      "macho"."_combined_store_qty" as "combined_store_qty",
      "macho"."_combined_store_qty_count" as "combined_store_qty_count",
      "macho"."_combined_store_row_count" as "combined_store_row_count",
      "macho"."_combined_web_qty" as "combined_web_qty"
  FROM
      "macho")
  SELECT
      "scrawny"."combined_item_code" as "item_code",
      CASE
  	WHEN "scrawny"."combined_store_qty_count" = "scrawny"."combined_store_row_count" THEN "scrawny"."combined_store_qty"
  	ELSE null
  	END as "store_return_quantity",
      CASE
  	WHEN "scrawny"."combined_catalog_qty_count" = "scrawny"."combined_catalog_row_count" THEN "scrawny"."combined_catalog_qty"
  	ELSE null
  	END as "catalog_return_quantity",
      "scrawny"."combined_web_qty" as "web_return_quantity"
  FROM
      "scrawny"
  GROUP BY
      1,
      2,
      3,
      4
  ORDER BY
      "item_code" asc nulls first,
      "scrawny"."combined_store_qty" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query83.preql`

  ```text
  Syntax error in query83.preql: Duplicate select output for local.__st; Line: 59
  ```
- `trilogy run query83.preql`

  ```text
  Unexpected error in query83.preql: (_duckdb.BinderException) Binder Error: column "combined_store_qty" must appear in the GROUP BY clause or must be part of an aggregate function.
  Either add it to the GROUP BY list, or use "ANY_VALUE(combined_store_qty)" if the exact value of "combined_store_qty" is not important.

  LINE 139:     "scrawny"."combined_store_qty" asc nulls first
                ^
  [SQL:
  WITH
  abhorrent as (
  SELECT
      "wr_web_sales_item_items"."I_ITEM_ID" as "_web_agg_item_code",
      count("wr_web_returns"."WR_ORDER_NUMBER") as "_web_agg_web_row_count",
      sum("wr_web_returns"."WR_RETURN_QUANTITY") as "_web_agg_web_qty"
  FROM
      "web_returns" as "wr_web_returns"
      INNER JOIN "date_dim" as "wr_return_date_date" on "wr_web_returns"."WR_RETURNED_DATE_SK" = "wr_return_date_date"."D_DATE_SK"
      INNER JOIN "item" as "wr_web_sales_item_items" on "wr_web_returns"."WR_ITEM_SK" = "wr_web_sales_item_items"."I_ITEM_SK"
  WHERE
      "wr_return_date_date"."D_WEEK_SEQ" in (5244,5257,5264)

  GROUP BY
      1),
  yummy as (
  SELECT
      "sr_item_items"."I_ITEM_ID" as "_store_agg_item_code",
      count("sr_store_returns"."SR_RETURN_QUANTITY") as "_store_agg_store_qty_count",
      count("sr_store_returns"."SR_TICKET_NUMBER") as "_store_agg_store_row_count",
      sum("sr_store_returns"."SR_RETURN_QUANTITY") as "_store_agg_store_qty"
  FROM
      "store_returns" as "sr_store_returns"
      INNER JOIN "item" as "sr_item_items" on "sr_store_returns"."SR_ITEM_SK" = "sr_item_items"."I_ITEM_SK"
      INNER JOIN "date_dim" as "sr_return_date_date" on "sr_store_returns"."SR_RETURNED_DATE_SK" = "sr_return_date_date"."D_DATE_SK"
  WHERE
      "sr_return_date_date"."D_WEEK_SEQ" in (5244,5257,5264)

  GROUP BY
      1),
  cheerful as (
  SELECT
      "cr_item_items"."I_ITEM_ID" as "_catalog_agg_item_code",
      count("cr_catalog_returns"."CR_ORDER_NUMBER") as "_catalog_agg_catalog_row_count",
      count("cr_catalog_returns"."CR_RETURN_QUANTITY") as "_catalog_agg_catalog_qty_count",
      sum("cr_catalog_returns"."CR_RETURN_QUANTITY") as "_catalog_agg_catalog_qty"
  FROM
      "catalog_returns" as "cr_catalog_returns"
      INNER JOIN "date_dim" as "cr_date_date" on "cr_catalog_returns"."CR_RETURNED_DATE_SK" = "cr_date_date"."D_DATE_SK"
      INNER JOIN "item" as "cr_item_items" on "cr_catalog_returns"."CR_ITEM_SK" = "cr_item_items"."I_ITEM_SK"
  WHERE
      "cr_date_date"."D_WEEK_SEQ" in (5244,5257,5264)

  GROUP BY
      1),
  late as (
  SELECT
      "abhorrent"."_web_agg_item_code" as "web_agg_item_code",
      "abhorrent"."_web_agg_web_qty" as "web_agg_web_qty",
      "abhorrent"."_web_agg_web_row_count" as "web_agg_web_row_count"
  FROM
      "abhorrent"),
  vacuous as (
  SELECT
      "yummy"."_store_agg_item_code" as "store_agg_item_code",
      "yummy"."_store_agg_store_qty" as "store_agg_store_qty",
      "yummy"."_store_agg_store_qty_count" as "store_agg_store_qty_count",
      "yummy"."_store_agg_store_row_count" as "store_agg_store_row_count"
  FROM
      "yummy"),
  cooperative as (
  SELECT
      "cheerful"."_catalog_agg_catalog_qty" as "catalog_agg_catalog_qty",
      "cheerful"."_catalog_agg_catalog_qty_count" as "catalog_agg_catalog_qty_count",
      "cheerful"."_catalog_agg_catalog_row_count" as "catalog_agg_catalog_row_count",
      "cheerful"."_catalog_agg_item_code" as "catalog_agg_item_code"
  FROM
      "cheerful"),
  macho as (
  SELECT
      "cooperative"."catalog_agg_catalog_qty" as "_combined_catalog_qty",
      "cooperative"."catalog_agg_catalog_qty_count" as "_combined_catalog_qty_count",
      "cooperative"."catalog_agg_catalog_row_count" as "_combined_catalog_row_count",
      "late"."web_agg_web_qty" as "_combined_web_qty",
      "vacuous"."store_agg_item_code" as "_combined_item_code",
      "vacuous"."store_agg_store_qty" as "_combined_store_qty",
      "vacuous"."store_agg_store_qty_count" as "_combined_store_qty_count",
      "vacuous"."store_agg_store_row_count" as "_combined_store_row_count"
  FROM
      "late"
      INNER JOIN "cooperative" on "late"."web_agg_item_code" = "cooperative"."catalog_agg_item_code"
      INNER JOIN "vacuous" on "cooperative"."catalog_agg_item_code" = "vacuous"."store_agg_item_code" AND "late"."web_agg_item_code" = "vacuous"."store_agg_item_code"
  WHERE
      "vacuous"."store_agg_store_row_count" > 0 and "cooperative"."catalog_agg_catalog_row_count" > 0 and "late"."web_agg_web_row_count" > 0
  ),
  scrawny as (
  SELECT
      "macho"."_combined_catalog_qty" as "combined_catalog_qty",
      "macho"."_combined_catalog_qty_count" as "combined_catalog_qty_count",
      "macho"."_combined_catalog_row_count" as "combined_catalog_row_count",
      "macho"."_combined_item_code" as "combined_item_code",
      "macho"."_combined_store_qty" as "combined_store_qty",
      "macho"."_combined_store_qty_count" as "combined_store_qty_count",
      "macho"."_combined_store_row_count" as "combined_store_row_count",
      "macho"."_combined_web_qty" as "combined_web_qty"
  FROM
      "macho")
  SELECT
      "scrawny"."combined_item_code" as "item_code",
      CASE
  	WHEN "scrawny"."combined_store_qty_count" = "scrawny"."combined_store_row_count" THEN "scrawny"."combined_store_qty"
  	ELSE null
  	END as "store_return_quantity",
      CASE
  	WHEN "scrawny"."combined_store_qty_count" = "scrawny"."combined_store_row_count" and "scrawny"."combined_catalog_qty_count" = "scrawny"."combined_catalog_row_count" THEN ( ( "scrawny"."combined_store_qty" / cast((( "scrawny"."combined_store_qty" + "scrawny"."combined_catalog_qty" ) + "scrawny"."combined_web_qty") as numeric) ) / 3 ) * 100
  	ELSE null
  	END as "store_pct",
      CASE
  	WHEN "scrawny"."combined_catalog_qty_count" = "scrawny"."combined_catalog_row_count" THEN "scrawny"."combined_catalog_qty"
  	ELSE null
  	END as "catalog_return_quantity",
      CASE
  	WHEN "scrawny"."combined_store_qty_count" = "scrawny"."combined_store_row_count" and "scrawny"."combined_catalog_qty_count" = "scrawny"."combined_catalog_row_count" THEN ( ( "scrawny"."combined_catalog_qty" / cast((( "scrawny"."combined_store_qty" + "scrawny"."combined_catalog_qty" ) + "scrawny"."combined_web_qty") as numeric) ) / 3 ) * 100
  	ELSE null
  	END as "catalog_pct",
      "scrawny"."combined_web_qty" as "web_return_quantity",
      CASE
  	WHEN "scrawny"."combined_store_qty_count" = "scrawny"."combined_store_row_count" and "scrawny"."combined_catalog_qty_count" = "scrawny"."combined_catalog_row_count" THEN ( ( "scrawny"."combined_web_qty" / cast((( "scrawny"."combined_store_qty" + "scrawny"."combined_catalog_qty" ) + "scrawny"."combined_web_qty") as numeric) ) / 3 ) * 100
  	ELSE null
  	END as "web_pct",
      CASE
  	WHEN "scrawny"."combined_store_qty_count" = "scrawny"."combined_store_row_count" and "scrawny"."combined_catalog_qty_count" = "scrawny"."combined_catalog_row_count" THEN cast((( "scrawny"."combined_store_qty" + "scrawny"."combined_catalog_qty" ) + "scrawny"."combined_web_qty") as numeric) / 3
  	ELSE null
  	END as "three_channel_avg"
  FROM
      "scrawny"
  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8
  ORDER BY
      "item_code" asc nulls first,
      "scrawny"."combined_store_qty" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy file read query86.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query89.preql`

  ```text
  Syntax error in query89.preql: Undefined concept: monthly_totals.ss.item.category. Suggestions: ['ss.item.category', 'monthly_totals.category', '_monthly_totals_category']
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query96.preql`

  ```text
  Syntax error in query96.preql: 3 undefined concept references; fix all before re-running:
    - store.name (line 3, col 7, in WHERE); did you mean: store_sales.store.name, store_sales.return_store.name?
    - time.hour (line 4, col 7, in WHERE); did you mean: store_sales.time.hour, store_sales.return_time.hour, time.minute?
    - time.minute (line 5, col 7, in WHERE); did you mean: store_sales.time.minute, store_sales.return_time.minute, time.hour?
  ```
- `trilogy file read query96.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query97.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy run --import raw.all_sales:all_sales select all_sales.channel, count(all_sales.channel) as cnt by rollup all_sales.channel; duckdb`

  ```text
  Syntax error in stdin:  --> 2:59
    |
  2 | select all_sales.channel, count(all_sales.channel) as cnt by rollup all_sales.channel;
    |                                                           ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...unt(all_sales.channel) as cnt ??? by rollup all_sales.channel;
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.channel, count(all_sales.channel) as cnt by all_sales.channel; duckdb`

  ```text
  Syntax error in stdin:  --> 2:59
    |
  2 | select all_sales.channel, count(all_sales.channel) as cnt by all_sales.channel;
    |                                                           ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...unt(all_sales.channel) as cnt ??? by all_sales.channel;
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.channel, count(all_sales.order_id) as cnt group by all_sales.channel; duckdb`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...nt(all_sales.order_id) as cnt ??? group by all_sales.channel;
  ```
- `trilogy file write query10.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sales as cs;

# Customers whose curre….demographics.dependent_count,
    ss.customer.demographics.employed_dependent_count,
    ss.customer.demographics.college_dependent_count
limit 100;`

  ```text
  refused to write 'query10.preql': not syntactically valid Trilogy.

  Parse error:
   --> 6:26
    |
  6 | auto target_counties <- {'Rush County', 'Toole County', 'Jefferson County', 'Dona Ana County', 'La Porte County'};
    |                          ^---
    |
    = expected null_lit or MULTILINE_STRING
  Location:
  ...ties auto target_counties <- { ??? 'Rush County', 'Toole County',...

  Write stats: received 2357 chars / 2357 bytes; tail: …' ss.customer.demographics.college_dependent_count\\nlimit 100;'.
  ```
- `trilogy file write query11.preql --content # Query 11: Customers whose web revenue grew faster than store revenue from 2001 to 2002
# Revenue = sum(ext_list_… end
order by
    c.text_id nulls first,
    c.first_name nulls first,
    c.last_name nulls first,
    c.preferred_cust_flag nulls first
limit 100;
`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
    --> 36:1
     |
  36 | inner join store_rev.id = web_rev.id
     | ^---
     |
     = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...0     and web_rev.web_rev > 0 ??? inner join store_rev.id = web_...

  Write stats: received 2567 chars / 2567 bytes; tail: …'lls first,\\n    c.preferred_cust_flag nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as s;

# Overall average sale value (quantity * list_price) across all three channels, 1999-2…(s.item.class_id) desc,
    s.item.class_id asc nulls first,
    grouping(s.item.category_id) desc,
    s.item.category_id asc nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
    --> 30:5
     |
  30 |     --total_sales as _total_for_filter
     |     ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
   as total_number_of_sales     ??? --total_sales as _total_for_fi...

  Write stats: received 1447 chars / 1447 bytes; tail: …'_id) desc,\\n    s.item.category_id asc nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw/all_sales:s with q as where s.date.year between 1999 and 2001 select s.item.brand_id, s.item.class_id, s.item.category_id, --s.channel having count_distinct(s.channel) = 3 select count(q.brand_id) as cnt;`

  ```text
  Syntax error in stdin:  --> 2:161
    |
  2 | with q as where s.date.year between 1999 and 2001 select s.item.brand_id, s.item.class_id, s.item.category_id, --s.channel having count_distinct(s.channel) = 3 select count(q.brand_id) as cnt;
    |                                                                                                                                                                 ^---
    |
    = expected limit, order_by, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...count_distinct(s.channel) = 3 ??? select count(q.brand_id) as cn...
  ```
- `trilogy file write query16.preql --content import raw/catalog_sales as cs;
import raw/catalog_returns as cr;

# Set of order numbers that have catalog return…as order_count,
    sum(cs.ext_ship_cost) as total_ext_ship_cost,
    sum(cs.net_profit) as total_net_profit
order by
    order_count desc
limit 100;`

  ```text
  refused to write 'query16.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/catalog_sales as cs;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/catalog_sales as cs; impor...

  Write stats: received 902 chars / 902 bytes; tail: …'as total_net_profit\\norder by\\n    order_count desc\\nlimit 100;'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…ame,
    sum(combined_sales.channel_total) as total_sales
order by last_name nulls first, first_name nulls first, total_sales nulls first
limit 100;
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block — they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...ng by first 30 chars of desc) ??? rowset frequent_item_groups as...

  Write stats: received 2950 chars / 2950 bytes; tail: …' first_name nulls first, total_sales nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…t_name,
    sum(combined_sales.line_total) as total_sales
order by last_name nulls first, first_name nulls first, total_sales nulls first
limit 100;
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
   --> 6:1
    |
  6 | rowset frequent_groups as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
   30 chars of item description ??? rowset frequent_groups as wher...

  Write stats: received 2352 chars / 2352 bytes; tail: …' first_name nulls first, total_sales nulls first\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.item.text_id as ic, store_sales.item.desc as id, store_sales.store.text_id as sc, store_s…re_sales.return_date.year = 2001 and store_sales.return_date.month_of_year between 4 and 10 group by ic, id, sc, sn order by ic, id, sc, sn limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...onth_of_year between 4 and 10 ??? group by ic, id, sc, sn order
  ```
- `trilogy run --import raw.store_sales:store_sales --import raw.catalog_sales:catalog_sales select store_sales.item.text_id as ic, count(store_sales.line_item)…h_of_year between 4 and 10 inner join catalog_sales.bill_customer.id = store_sales.customer.id = catalog_sales.item.id = store_sales.item.id limit 5;`

  ```text
  Syntax error in stdin:  --> 3:345
    |
  3 | select store_sales.item.text_id as ic, count(store_sales.line_item) as cnt where store_sales.date.year = 2001 and store_sales.date.month_of_year = 4 and store_sales.is_returned = true and store_sales.return_customer.id = store_sales.customer.id and store_sales.return_date.year = 2001 and store_sales.return_date.month_of_year between 4 and 10 inner join catalog_sales.bill_customer.id = store_sales.customer.id = catalog_sales.item.id = store_sales.item.id limit 5;
    |                                                                                                                                                                                                                                                                                                                                                         ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...onth_of_year between 4 and 10 ??? inner join catalog_sales.bill_...
  ```
- `trilogy run --import raw.store_sales:store_sales --import raw.catalog_sales:catalog_sales select count(store_sales.line_item) as cnt where store_sales.date.y…h_of_year between 4 and 10 inner join catalog_sales.bill_customer.id = store_sales.customer.id = catalog_sales.item.id = store_sales.item.id limit 5;`

  ```text
  Syntax error in stdin:  --> 3:313
    |
  3 | select count(store_sales.line_item) as cnt where store_sales.date.year = 2001 and store_sales.date.month_of_year = 4 and store_sales.is_returned = true and store_sales.return_customer.id = store_sales.customer.id and store_sales.return_date.year = 2001 and store_sales.return_date.month_of_year between 4 and 10 inner join catalog_sales.bill_customer.id = store_sales.customer.id = catalog_sales.item.id = store_sales.item.id limit 5;
    |                                                                                                                                                                                                                                                                                                                         ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...onth_of_year between 4 and 10 ??? inner join catalog_sales.bill_...
  ```
- `trilogy run --import raw.store_sales:store_sales select count(store_sales.line_item) as cnt inner join ... where store_sales.date.year = 2001 and store_sales.date.month_of_year = 4 and store_sales.is_returned = true limit 5;`

  ```text
  Syntax error in stdin:  --> 2:55
    |
  2 | select count(store_sales.line_item) as cnt inner join ... where store_sales.date.year = 2001 and store_sales.date.month_of_year = 4 and store_sales.is_returned = true limit 5;
    |                                                       ^---
    |
    = expected sum_operator
  Location:
  ....line_item) as cnt inner join ??? ... where store_sales.date.yea...
  ```
- `trilogy file write query35.preql --content import raw.store_sales as store_sales;

# Customers who had a store sale in Q1-Q3 of 2002
with store_cust as
where…al_status nulls first,
    dependent_count nulls first,
    employed_dependent_count nulls first,
    college_dependent_count nulls first
limit 100
;`

  ```text
  refused to write 'query35.preql': not syntactically valid Trilogy.

  Parse error:
    --> 13:1
     |
  13 | import raw.web_sales as web_sales
     | ^---
     |
     = expected select_statement or tvf_union_invocation
  Location:
  ...1-Q3 of 2002 with web_cust as ??? import raw.web_sales as web_sa...

  Write stats: received 2190 chars / 2190 bytes; tail: …'s first,\\n    college_dependent_count nulls first\\nlimit 100\\n;'.
  ```
- `trilogy file write query41.preql --content import raw.item as item;

# First, identify items that match any of the 8 detailed attribute profiles
with profile…_id between 1 and 500
    and item.manufact in profile_items.manufact
select distinct
    item.product_name
order by
    item.product_name
limit 100;`

  ```text
  refused to write 'query41.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword — a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...profile_items.manufact select ??? distinct     item.product_name...

  Write stats: received 1374 chars / 1374 bytes; tail: …' item.product_name\\norder by\\n    item.product_name\\nlimit 100;'.
  ```
- `trilogy file write query43.preql --content import raw.store_sales as sales;

# Filtered aggregate macros for each day of week
def sunday_sales -> sum(sales.s…as wednesday,
  @thursday_sales as thursday,
  @friday_sales as friday,
  @saturday_sales as saturday
order by
  store_name,
  store_code
limit 100;
`

  ```text
  refused to write 'query43.preql': not syntactically valid Trilogy.

  Parse error:
    --> 18:3
     |
  18 |   @sunday_sales as sunday,
     |   ^---
     |
     = expected select_item, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...tore.text_id as store_code,   ??? @sunday_sales as sunday,   @mo...

  Write stats: received 985 chars / 985 bytes; tail: …' as saturday\\norder by\\n  store_name,\\n  store_code\\nlimit 100;\\n'.
  ```
- `trilogy file write query60.preql --content import raw.all_sales as sales;

# Items whose item_code matches any Music-category item
with music_items as
select…les.ext_sales_price) as total_extended_sales_price
order by
    item_code asc nulls first,
    total_extended_sales_price asc nulls first
limit 100;
`

  ```text
  refused to write 'query60.preql': not syntactically valid Trilogy.

  Parse error:
    --> 13:18
     |
  13 |     month_of_year(sales.date.date) = 9
     |                  ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
   items where     month_of_year ??? (sales.date.date) = 9     and

  Write stats: received 647 chars / 647 bytes; tail: …',\\n    total_extended_sales_price asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query67.preql --content import raw.store_sales as store_sales;

auto summed_sales <- coalesce(sum(store_sales.ext_sales_price), 0);

auto … nulls first,
    month_of_year asc nulls first,
    store_code asc nulls first,
    summed_sales asc nulls first,
    rnk asc nulls first
limit 100;`

  ```text
  refused to write 'query67.preql': not syntactically valid Trilogy.

  Parse error:
    --> 41:1
     |
  41 | having rnk <= 100
     | ^---
     |
     = expected limit or order_by
  Location:
    store_sales.store.text_id ) ??? having rnk <= 100 order by

  Write stats: received 1503 chars / 1503 bytes; tail: …'ed_sales asc nulls first,\\n    rnk asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query70.preql --content import raw.store_sales as ss;

# Filter to year 2000
where ss.date.year = 2000

# Define grouping levels
auto g_st… in top5_states
    or g_state = 1
by rollup (ss.store.state, ss.store.county)
order by level desc, ss.store.state asc nulls last, rnk asc
limit 100;`

  ```text
  refused to write 'query70.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block — they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...000  # Define grouping levels ??? auto g_state <- grouping(ss.st...

  Write stats: received 1203 chars / 1203 bytes; tail: …'evel desc, ss.store.state asc nulls last, rnk asc\\nlimit 100;'.
  ```
- `trilogy run --import raw.store_sales:ss select ss.date.year, count(ss.line_item) as cnt, sum(ss.net_profit) as total_net_profit group by ss.date.year order by ss.date.year;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...t_profit) as total_net_profit ??? group by ss.date.year order by...
  ```
- `trilogy run --import raw.store_sales:ss select ss.date.year, count(ss.line_item) as cnt by ss.date.year order by ss.date.year;`

  ```text
  Syntax error in stdin:  --> 2:49
    |
  2 | select ss.date.year, count(ss.line_item) as cnt by ss.date.year order by ss.date.year;
    |                                                 ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...r, count(ss.line_item) as cnt ??? by ss.date.year order by ss.da...
  ```
- `trilogy file write query73.preql --content import raw.store_sales as ss;

where 
    ss.date.year in (1999, 2000, 2001)
    and ss.date.day_of_month between …  line_item_count between 1 and 5
order by
    line_item_count desc,
    ss.customer.last_name asc,
    ss.ticket_number asc,
    ss.customer.id asc;`

  ```text
  refused to write 'query73.preql': not syntactically valid Trilogy.

  Parse error:
    --> 17:5
     |
  17 |     --ss.customer.id
     |     ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ..._item) as line_item_count     ??? --ss.customer.id having     li...

  Write stats: received 865 chars / 865 bytes; tail: …'_name asc,\\n    ss.ticket_number asc,\\n    ss.customer.id asc;'.
  ```
- `trilogy file write query74.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;

# Store sales totals per customer per year
rowset store…2002 / web_by_cust.web_2001) > (store_by_cust.store_2002 / store_by_cust.store_2001)
order by
    store_by_cust.cust_code asc nulls first
limit 100;
`

  ```text
  refused to write 'query74.preql': not syntactically valid Trilogy.

  Parse error:
   --> 5:1
    |
  5 | rowset store_by_cust as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
   totals per customer per year ??? rowset store_by_cust as select...

  Write stats: received 1105 chars / 1105 bytes; tail: …'r by\\n    store_by_cust.cust_code asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query75.preql --content import raw.all_sales as sales;

# Step 1: Create a deduplicated per-line rowset
# Each row is one distinct (year, …= 2001 and a2002.yr = 2002
    and a2001.total_qty > 0
    and a2002.total_qty / a2001.total_qty < 0.9
order by qty_diff asc, amt_diff asc
limit 100;`

  ```text
  refused to write 'query75.preql': not syntactically valid Trilogy.

  Parse error:
    --> 43:22
     |
  43 | inner join agg_lines a2001
     |                      ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...amt_diff inner join agg_lines ??? a2001     = agg_lines a2002

  Write stats: received 1931 chars / 1931 bytes; tail: …'tal_qty < 0.9\\norder by qty_diff asc, amt_diff asc\\nlimit 100;'.
  ```
- `trilogy file write query76.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_sale…c nulls first,
    combined.year asc nulls first,
    combined.quarter_of_year asc nulls first,
    combined.item_category asc nulls first
limit 100;`

  ```text
  refused to write 'query76.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` — a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ..._count, total_ext_sales_price) ???  select     combined.channel_...

  Write stats: received 2042 chars / 2042 bytes; tail: …'first,\\n    combined.item_category asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query77.preql --content import raw.all_sales as sales;

select
    case when grouping(sales.channel) = 1 then 'ALL CHANNELS'
         else…d is not null or grouping(sales.outlet_id) = 1
order by _level asc, channel_label asc nulls first, outlet_id asc nulls first, returns desc
limit 100;`

  ```text
  refused to write 'query77.preql': not syntactically valid Trilogy.

  Parse error:
    --> 18:1
     |
  18 | having sales.outlet_id is not null or grouping(sales.outlet_id) = 1
     | ^---
     |
     = expected limit or order_by
  Location:
  ...les.channel, sales.outlet_id) ??? having sales.outlet_id is not

  Write stats: received 1194 chars / 1194 bytes; tail: …'ls first, outlet_id asc nulls first, returns desc\\nlimit 100;'.
  ```
- `trilogy file write query77.preql --content import raw.all_sales as sales;

select
    case when grouping(sales.channel) = 1 then 'ALL CHANNELS'
         else…outlet_id) = 1 or sales.outlet_id is not null)
order by _level asc, channel_label asc nulls first, outlet_id asc nulls first, returns desc
limit 100;`

  ```text
  refused to write 'query77.preql': not syntactically valid Trilogy.

  Parse error:
    --> 12:6
     |
  12 |     --     else sales.outlet_id end as show_outlet,
     |      ^---
     |
     = expected access_chain
  Location:
  ...outlet_id) = 1 then null     - ??? -     else sales.outlet_id end...

  Write stats: received 1306 chars / 1306 bytes; tail: …'ls first, outlet_id asc nulls first, returns desc\\nlimit 100;'.
  ```
- `trilogy file write query82.preql --content import raw.store_sales as store_sales;
import raw.inventory as inventory;

# Stage 1: Items that had at least one …ode,
    store_sales.item.desc as description,
    store_sales.item.current_price as current_price

order by store_sales.item.text_id asc
limit 100;
`

  ```text
  refused to write 'query82.preql': not syntactically valid Trilogy.

  Parse error:
   --> 6:1
    |
  6 | rowset inv_items as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...nd 500 during the date range. ??? rowset inv_items as where inve...

  Write stats: received 810 chars / 810 bytes; tail: …'ent_price\\n\\norder by store_sales.item.text_id asc\\nlimit 100;\\n'.
  ```
- `trilogy file write query95.preql --content import raw.web_sales as ws;

where
    ws.ship_date.date between '1999-02-01'::date and '1999-04-02'::date
    and…rder_count,
    sum(ws.ext_ship_cost) as total_extended_shipping_cost,
    sum(ws.net_profit) as total_net_profit
order by
    order_count
limit 100;`

  ```text
  refused to write 'query95.preql': not syntactically valid Trilogy.

  Parse error:
    --> 14:1
     |
  14 | select
     | ^---
     |
     = expected limit, order_by, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...= 2     and has_return = true ??? select     ws.order_number as

  Write stats: received 549 chars / 549 bytes; tail: …'fit) as total_net_profit\\norder by\\n    order_count\\nlimit 100;'.
  ```
- `trilogy file write query95.preql --content import raw.web_sales as ws;

# Stage 1: Identify orders meeting the warehouse and return conditions
rowset qualify…rder_count,
    sum(ws.ext_ship_cost) as total_extended_shipping_cost,
    sum(ws.net_profit) as total_net_profit
order by
    order_count
limit 100;`

  ```text
  refused to write 'query95.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:1
    |
  4 | rowset qualifying_orders as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...rehouse and return conditions ??? rowset qualifying_orders as wh...

  Write stats: received 941 chars / 941 bytes; tail: …'fit) as total_net_profit\\norder by\\n    order_count\\nlimit 100;'.
  ```
- `trilogy file write query97.preql --content import raw.customer as c;
import raw.item as i;

# We need to combine store_sales and catalog_sales data.
# Using …ned.channel_2 = 1 then 1 else 0 end) as catalog_only,
    sum(case when combined.channel = 1 and combined.channel_2 = 1 then 1 else 0 end) as both
;
`

  ```text
  refused to write 'query97.preql': not syntactically valid Trilogy.

  Parse error:
   --> 8:6
    |
  8 |     (import raw.store_sales as ss;
    |      ^---
    |
    = expected select_statement
  Location:
   with combined as union(     ( ??? import raw.store_sales as ss;

  Write stats: received 874 chars / 874 bytes; tail: …'= 1 and combined.channel_2 = 1 then 1 else 0 end) as both\\n;\\n'.
  ```

### `cli-misuse`

- `trilogy run -i raw/all_sales select all_sales.channel, all_sales.channel_dim_text_id, all_sales.channel_dim_id, all_sales.date.date limit 5;`

  ```text
  'raw/all_sales' looks like a file path, not a dialect. The dialect argument comes AFTER the input file.
    Try: trilogy run raw/all_sales <dialect>
  ```
- `trilogy explore raw/customer_address.preql`

  ```text
  Invalid value for 'PATH': File 'raw/customer_address.preql' does not exist.
  ```
- `trilogy explore raw/bill_customer_demographic.preql`

  ```text
  Invalid value for 'PATH': File 'raw/bill_customer_demographic.preql' does not exist.
  ```
- `trilogy explore raw/customer_demographics.preql`

  ```text
  Invalid value for 'PATH': File 'raw/customer_demographics.preql' does not exist.
  ```
- `trilogy explore --show concepts --purpose metric --purpose property --purpose key raw/`

  ```text
  Invalid value for 'PATH': File 'raw/' is a directory.
  ```
- `trilogy run query49.preql --format json`

  ```text
  '--format' is not a valid dialect. The dialect argument comes after the input file and any options.
    Try: trilogy run <input> [<dialect>]
  ```
- `trilogy explore raw/sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/sales.preql' does not exist.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw.all_sales:all_sales select all_sales.channel, all_sales.is_returned, all_sales.return_channel_dim_text_id, all_sales.return_date.date limit 10 where all_sales.is_returned is not null; duckdb`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `all_sales.return_date.date as all_sales_return_date_date`
  Location:
  ...d, all_sales.return_date.date ??? limit 10 where all_sales.is_re...
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.channel, all_sales.is_returned, all_sales.return_channel_dim_text_id limit 10 where all_sales.is_returned is not null; duckdb`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `all_sales.return_channel_dim_text_id as all_sales_return_channel_dim_text_id`
  Location:
  ...es.return_channel_dim_text_id ??? limit 10 where all_sales.is_re...
  ```
- `trilogy file write query61.preql --content import raw.store_sales as store_sales;

where
    store_sales.date.year = 1998
    and store_sales.date.month_of_y…es.ext_sales_price) as overall_total,
    promo_total * 100.0 / nullif(overall_total, 0) as promo_pct
order by promo_total, overall_total
limit 100;
`

  ```text
  refused to write 'query61.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `sum(store_sales.ext_sales_price) filter (where store_sales.promotion.channel_dmail = 'Y'
          or store_sales.promotion.channel_email = 'Y'
          or store_sales.promotion.channel_tv = 'Y') as y_sum`
  Location:
  ...(store_sales.ext_sales_price) ??? filter (where store_sales.prom...

  Write stats: received 650 chars / 650 bytes; tail: …'as promo_pct\\norder by promo_total, overall_total\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.date.year, count(store_sales.ticket_number) where store_sales.date.year = 2000 limit 5;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(store_sales.ticket_number) as ticket_number_count`
  Location:
  ...nt(store_sales.ticket_number) ??? where store_sales.date.year =
  ```
- `trilogy file write query80.preql --content import raw.all_sales as all_sales;

# Filter: period, item price > 50, promotion TV = 'N', and outlet recorded
whe…ng(outlet_identifier) as _level
by rollup (channel, outlet_identifier)
order by channel asc nulls first, outlet_identifier asc nulls first
limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `grouping for level tracking
      --grouping(channel) + grouping(outlet_identifier) as grouping_for_level_tracking_grouping_cha`
  Location:
   0) as profit,     --grouping ??? for level tracking     --group...

  Write stats: received 1274 chars / 1274 bytes; tail: …'sc nulls first, outlet_identifier asc nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.store_sales:ss select ss.date.year, count(ss.ticket_number) where ss.date.year = 2000 and ss.customer.id is not null limit 10;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(ss.ticket_number) as ticket_number_count`
  Location:
  ...year, count(ss.ticket_number) ??? where ss.date.year = 2000 and
  ```
- `trilogy run --import raw.catalog_sales:cs select cs.sold_date.year, count(cs.order_number) where cs.sold_date.year = 2000 and cs.bill_customer.id is not null limit 10;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(cs.order_number) as order_number_count`
  Location:
  ....year, count(cs.order_number) ??? where cs.sold_date.year = 2000...
  ```

### `type-error`

- `trilogy run query22.preql`

  ```text
  Unexpected error in query22.preql: Invalid argument type 'INTEGER' passed into YEAR function in position 1 from concept: inventory.date.id. Valid: 'DATE', 'DATETIME', 'TIMESTAMP'.
  ```
- `trilogy run query92.preql`

  ```text
  Syntax error in query92.preql: Cannot use BETWEEN with incompatible types DATE and STRING (low)
  ```

### `file-not-found`

- `trilogy run query37.preql`

  ```text
  Unexpected error in query37.preql: (_duckdb.CatalogException) Catalog Error: Table with name cs_item_items does not exist!
  Did you mean "item"?

  LINE 10: ..."."inv_item_sk" in (select cs_item_items."cs_item_id" from cs_item_items where cs_item_items."cs_item_id" is not null)
                                                                         ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "inv_warehouse_inventory"."inv_date_sk" as "inv_date_id",
      "inv_warehouse_inventory"."inv_item_sk" as "inv_item_id"
  FROM
      "inventory" as "inv_warehouse_inventory"
  WHERE
      "inv_warehouse_inventory"."inv_quantity_on_hand" BETWEEN 100 AND 500 and "inv_warehouse_inventory"."inv_item_sk" in (select cs_item_items."cs_item_id" from cs_item_items where cs_item_items."cs_item_id" is not null)

  GROUP BY
      1,
      2,
      "inv_warehouse_inventory"."inv_quantity_on_hand")
  SELECT
      "inv_item_items"."I_ITEM_ID" as "item_code",
      "inv_item_items"."I_ITEM_DESC" as "description",
      "inv_item_items"."I_CURRENT_PRICE" as "inv_item_current_price"
  FROM
      "cheerful"
      INNER JOIN "date_dim" as "inv_date_date" on "cheerful"."inv_date_id" = "inv_date_date"."D_DATE_SK"
      INNER JOIN "item" as "inv_item_items" on "cheerful"."inv_item_id" = "inv_item_items"."I_ITEM_SK"
  WHERE
      "inv_item_items"."I_CURRENT_PRICE" BETWEEN 68 AND 98 and "inv_item_items"."I_MANUFACT_ID" in (677,940,694,808) and cast("inv_date_date"."D_DATE" as date) BETWEEN date '2000-02-01' AND date '2000-04-01' and "inv_item_items"."I_ITEM_SK" in (select cs_item_items."I_ITEM_SK" from item as cs_item_items where cs_item_items."I_ITEM_SK" is not null)

  GROUP BY
      1,
      2,
      3
  ORDER BY
      "item_code" asc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query64.preql`

  ```text
  Table with name sr_item_items does not exist!\nDid you mean \"item\"?\n\nLINE 50: ...\".\"SS_ITEM_SK\" in (select sr_item_items.\"sr_item_id\" from sr_item_items where sr_item_items.\"sr_item_id\" is not null)\n                                                                      ^\n[SQL: \nWITH \npremium as (\nSELECT\n    \"sr_store_returns\".\"SR_TICKET_NUMBER\" as \"sr_ticket_number\"\nFROM\n    \"store_returns\" as \"sr_store_returns\"\nGROUP BY\n    1),\nwakeful as (\nSELECT\n    \"cr_catalog_returns\".\"CR_ITEM_SK\" as \"cr_item_id\",\n    sum(( coalesce(\"cr_catalog_returns\".\"CR_REFU
  …
  nt\" <= \"cool\".\"y1999_line_cnt\"\n\nORDER BY \n    \"cool\".\"y1999_product_name\" asc,\n    \"cool\".\"y1999_store_name\" asc,\n    \"line_cnt2000\" asc,\n    \"ws1999\" asc,\n    \"ws2000\" asc\nLIMIT (1000)]\n(Background on this error at: https://sqlalche.me/e/20/f405)",
    "error_type": "ProgrammingError"
  }
  {
    "event": "summary",
    "statements": 1,
    "duration_ms": 18.527,
    "ok": false,
    "rows": 0
  }
  {
    "event": "output_truncated",
    "dropped_events": 1,
    "note": "Output exceeded the tool cap; trailing events dropped. Narrow the call (--regex, --show, fewer rows) to see the rest."
  }
  ```

### `join-resolution`

- `trilogy run query57.preql`

  ```text
  Resolution error in query57.preql: Could not resolve connections for query with output ['it.category<Purpose.PROPERTY>Derivation.ROOT>', 'it.brand_name<Purpose.PROPERTY>Derivation.ROOT>', 'local.call_center<Purpose.PROPERTY>Derivation.BASIC>', 'cs.sold_date.year<Purpose.PROPERTY>Derivation.ROOT>', 'cs.sold_date.month_of_year<Purpose.PROPERTY>Derivation.ROOT>', 'local.avg_monthly_sales<Purpose.METRIC>Derivation.BASIC>', 'local.this_month_total<Purpose.METRIC>Derivation.BASIC>', 'local.prior_month_total<Purpose.PROPERTY>Derivation.WINDOW>', 'local.next_month_total<Purpose.PROPERTY>Derivation.WINDOW>'] from current model.
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Could not resolve connections for query with output ['local.qualifying_items<Purpose.PROPERTY>Derivation.FILTER>'] from current model.
  ```
