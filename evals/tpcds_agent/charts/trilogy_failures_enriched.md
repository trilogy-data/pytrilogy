# Trilogy failure analysis — 20260711-042547

- Run `20260711-042547_enriched` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 1219 | failed: 106 (9%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 65 | 61% |
| `syntax-parse` | 35 | 33% |
| `cli-misuse` | 4 | 4% |
| `type-error` | 1 | 1% |
| `syntax-missing-alias` | 1 | 1% |

## Detail

### `other`

- `trilogy file read query05.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query06.preql`

  ```text
  Resolution error in query06.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 10). The requested concepts split into 4 disconnected subgraphs: {cust.address.sk, state}; {date.month_of_year, date.year}; {item.category}; {line_item_count}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read query06.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Syntax error in query08.preql: Undefined concept: ss.sale_address.customer.preferred_cust_flag. Suggestions: ['ss.customer.preferred_cust_flag', 'ss.return_customer.preferred_cust_flag', 'ss.sale_address.street_name']
  ```
- `trilogy run query10.preql`

  ```text
  Resolution error in query10.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 8). The requested concepts split into 4 disconnected subgraphs: {c.address.county, c.demographics.sk, c.sk, college_dependent_count, credit_rating, customer_count_a, customer_count_b, customer_count_c, customer_count_d, customer_count_e, customer_count_f, dependent_count, education_status, employed_dependent_count, gender, marital_status, purchase_estimate}; {cs.sold_date.month_of_year, cs.sold_date.year}; {ss.date.month_of_year, ss.date.year}; {ws.date.month_of_year, ws.date.year}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run --import raw.customer:c --import raw.store_sales:ss select count(c.sk) where c.sk in ss.customer.sk and ss.date.year = 2002 and ss.date.month_of_year in (1,2,3,4) limit 5;`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {c.sk}; {ss.date.month_of_year, ss.date.year}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read query10.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query11.preql`

  ```text
  Syntax error in query11.preql: Output column 'ss_rev_2001' renames 'local.ss_rev_2001' back to the name of an existing concept 'ss_rev_2001' (defined at line 6) that 'local.ss_rev_2001' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'ss_rev_2001_out').
  ```
- `trilogy file read query11.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read trilogy.toml`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query17.preql`

  ```text
  Syntax error in query17.preql: Undefined concept: cs.item_sk. Suggestions: ['cs.item.sk', 'cs.item.size', 'cs.item.desc', 'ss_base.item_sk', 'ss.item.sk']
  ```
- `trilogy run query18.preql`

  ```text
  Syntax error in query18.preql: ORDER BY contains aggregate `grouping(local.country)` (line 3), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(local.country) as g order by g desc`.
  ```
- `trilogy file read query18.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query24.preql duckdb`

  ```text
  Import error in query24.preql: Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: raw.store_sales?
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query25.preql`

  ```text
  Syntax error in query25.preql: 4 undefined concept references; fix all before re-running:
    - local.item_code (line 47, col 10, in ORDER BY); did you mean: store_sales_with_returns.item_code, item_desc, store_code?
    - local.item_desc (line 47, col 21, in ORDER BY); did you mean: store_sales_with_returns.item_desc, item_code, ss.item.desc, cs.item.desc?
    - local.store_code (line 47, col 32, in ORDER BY); did you mean: store_sales_with_returns.store_code, item_code, store_name, ss.store_credit?
    - local.store_name (line 47, col 44, in ORDER BY); did you mean: store_sales_with_returns.store_name, ss.store.name, store_code, ss.store.street_name?
  ```
- `trilogy run query29.preql`

  ```text
  Syntax error in query29.preql: Comparison `ss.return_date.month_of_year <= 12` matches every value of enum field 'ss.return_date.month_of_year', which contains only these values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12. It is always true and should be removed.
  ```
- `trilogy run query31.preql`

  ```text
  Syntax error in query31.preql: Nothing was executed: parsed 4 definition statement(s) (2 imports, 2 rowsets) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy run query35.preql`

  ```text
  Unexpected error in query35.preql: Could not render the query: Missing source reference to ws.billing_customer.sk. A planned reference has no backing source CTE -- typically an unsupported cross-rowset or membership shape the planner could not wire. Review the rowset/join structure (or file an issue if the query looks valid).

  Full SQL with sentinel(s):

  WITH
  cooperative as (
  SELECT
      "cs_catalog_sales"."CS_SHIP_CUSTOMER_SK" as "_catalog_cust_cust_sk"
  FROM
      "catalog_sales" as "cs_catalog_sales"
      INNER JOIN "date_dim" as "cs_sold_date_date" on "cs_catalog_sales"."CS_SOLD_DATE_SK" = "cs_sold_date_date"."D_DATE_SK"
  WHERE
      "cs_sold_date_date"."D_YEAR" = 2002 and "cs_sold_date_date"."D_QOY" in (1,2,3)

  GROUP BY
      1),
  questionable as (
  SELECT
      "cooperative"."_catalog_cust_cust_sk" as "catalog_cust_cust_sk"
  FROM
      "cooperative"
  WHERE
      coalesce(INVALID_REFERENCE_BUG<Missing source reference to ws.billing_customer.sk>) is not null or coalesce("cooperative"."_catalog_cust_cust_sk") is not null
  ),
  abundant as (
  SELECT
      "questionable"."catalog_cust_cust_sk" as "_target_cust_c_sk"
  FROM
      "questionable"),
  uneven as (
  SELECT
      "abundant"."_target_cust_c_sk" as "target_cust_c_sk"
  FROM
      "abundant"
  GROUP BY
      1),
  young as (
  SELECT
      "c_address_customer_address"."CA_STATE" as "c_address_state",
      "c_customers"."C_CUSTOMER_SK" as "c_sk",
      "c_demographics_customer_demographics"."CD_DEP_COLLEGE_COUNT" as "c_demographics_college_dependent_count",
      "c_demographics_customer_demographics"."CD_DEP_COUNT" as "c_demographics_dependent_count",
      "c_demographics_customer_demographics"."CD_DEP_EMPLOYED_COUNT" as "c_demographics_employed_dependent_count",
      "c_demographics_customer_demographics"."CD_GENDER" as "c_demographics_gender",
      "c_demographics_customer_demographics"."CD_MARITAL_STATUS" as "c_demographics_marital_status"
  FROM
      "customer" as "c_customers"
      INNER JOIN "customer_address" as "c_address_customer_address" on "c_customers"."C_CURRENT_ADDR_SK" = "c_address_customer_address"."CA_ADDRESS_SK"
      INNER JOIN "customer_demographics" as "c_demographics_customer_demographics" on "c_customers"."C_CURRENT_CDEMO_SK" = "c_demographics_customer_demographics"."CD_DEMO_SK"
  WHERE
      "c_customers"."C_CUSTOMER_SK" in (select uneven."target_cust_c_sk" from uneven where uneven."target_cust_c_sk" is not null) and "c_customers"."C_CURRENT_CDEMO_SK" is not null

  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7),
  vacuous as (
  SELECT
      "c_address_customer_address"."CA_STATE" as "c_address_state",
      "c_demographics_customer_demographics"."CD_DEP_COLLEGE_COUNT" as "c_demographics_college_dependent_count",
      "c_demographics_customer_demographics"."CD_DEP_COUNT" as "c_demographics_dependent_count",
      "c_demographics_customer_demographics"."CD_DEP_EMPLOYED_COUNT" as "c_demographics_employed_dependent_count",
      "c_demographics_customer_demographics"."CD_GENDER" as "c_demographics_gender",
      "c_demographics_customer_demographics"."CD_MARITAL_STATUS" as "c_demographics_marital_status"
  FROM
      "customer" as "c_customers"
      INNER JOIN "customer_address" as "c_address_customer_address" on "c_customers"."C_CURRENT_ADDR_SK" = "c_address_customer_address"."CA_ADDRESS_SK"
      INNER JOIN "customer_demographics" as "c_demographics_customer_demographics" on "c_customers"."C_CURRENT_CDEMO_SK" = "c_demographics_customer_demographics"."CD_DEMO_SK"
  WHERE
      "c_customers"."C_CUSTOMER_SK" in (select uneven."target_cust_c_sk" from uneven where uneven."target_cust_c_sk" is not null) and "c_customers"."C_CURRENT_CDEMO_SK" is not null

  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      "c_customers"."C_CURRENT_CDEMO_SK"),
  sparkling as (
  SELECT
      "young"."c_address_state" as "c_address_state",
      "young"."c_demographics_college_dependent_count" as "c_demographics_college_dependent_count",
      "young"."c_demographics_dependent_count" as "c_demographics_dependent_count",
      "young"."c_demographics_employed_dependent_count" as "c_demographics_employed_dependent_count",
      "young"."c_demographics_gender" as "c_demographics_gender",
      "young"."c_demographics_marital_status" as "c_demographics_marital_status",
      count("young"."c_sk") as "cnt1",
      count("young"."c_sk") as "cnt2",
      count("young"."c_sk") as "cnt3"
  FROM
      "young"
  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6),
  concerned as (
  SELECT
      "vacuous"."c_address_state" as "c_address_state",
      "vacuous"."c_demographics_college_dependent_count" as "c_demographics_college_dependent_count",
      "vacuous"."c_demographics_dependent_count" as "c_demographics_dependent_count",
      "vacuous"."c_demographics_employed_dependent_count" as "c_demographics_employed_dependent_count",
      "vacuous"."c_demographics_gender" as "c_demographics_gender",
      "vacuous"."c_demographics_marital_status" as "c_demographics_marital_status",
      avg("vacuous"."c_demographics_college_dependent_count") as "avg3",
      avg("vacuous"."c_demographics_dependent_count") as "avg1",
      avg("vacuous"."c_demographics_employed_dependent_count") as "avg2",
      max("vacuous"."c_demographics_college_dependent_count") as "max3",
      max("vacuous"."c_demographics_dependent_count") as "max1",
      max("vacuous"."c_demographics_employed_dependent_count") as "max2",
      min("vacuous"."c_demographics_college_dependent_count") as "min3",
      min("vacuous"."c_demographics_dependent_count") as "min1",
      min("vacuous"."c_demographics_employed_dependent_count") as "min2"
  FROM
      "vacuous"
  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6)
  SELECT
      "concerned"."c_address_state" as "state",
      "concerned"."c_demographics_gender" as "gender",
      "concerned"."c_demographics_marital_status" as "marital_status",
      "concerned"."c_demographics_dependent_count" as "dep_count",
      "concerned"."c_demographics_employed_dependent_count" as "emp_dep_count",
      "concerned"."c_demographics_college_dependent_count" as "col_dep_count",
      "concerned"."c_demographics_dependent_count" as "f1",
      "sparkling"."cnt1" as "cnt1",
      "concerned"."min1" as "min1",
      "concerned"."max1" as "max1",
      "concerned"."avg1" as "avg1",
      "concerned"."c_demographics_employed_dependent_count" as "f2",
      "sparkling"."cnt2" as "cnt2",
      "concerned"."min2" as "min2",
      "concerned"."max2" as "max2",
      "concerned"."avg2" as "avg2",
      "concerned"."c_demographics_college_dependent_count" as "f3",
      "sparkling"."cnt3" as "cnt3",
      "concerned"."min3" as "min3",
      "concerned"."max3" as "max3",
      "concerned"."avg3" as "avg3"
  FROM
      "sparkling"
      INNER JOIN "concerned" on "sparkling"."c_address_state" is not distinct from "concerned"."c_address_state" AND "sparkling"."c_demographics_college_dependent_count" is not distinct from "concerned"."c_demographics_college_dependent_count" AND "sparkling"."c_demographics_dependent_count" is not distinct from "concerned"."c_demographics_dependent_count" AND "sparkling"."c_demographics_employed_dependent_count" is not distinct from "concerned"."c_demographics_employed_dependent_count" AND "sparkling"."c_demographics_gender" is not distinct from "concerned"."c_demographics_gender" AND "sparkling"."c_demographics_marital_status" is not distinct from "concerned"."c_demographics_marital_status"
  ORDER BY
      "state" asc nulls first,
      "gender" asc nulls first,
      "marital_status" asc nulls first,
      "dep_count" asc nulls first,
      "emp_dep_count" asc nulls first,
      "col_dep_count" asc nulls first
  LIMIT (100)
  ```
- `trilogy file read query36.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query37.preql`

  ```text
  Resolution error in query37.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 9). The requested concepts split into 2 disconnected subgraphs: {inv.date.date, inv.quantity_on_hand}; {item.current_price, item.manufacturer_id, item.sk, current_price, description, item_code}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read query38.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query40.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query41.preql`

  ```text
  Syntax error in query41.preql: Undefined concept: item.category.
  ```
- `trilogy file read query41.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw/store_sales:store_sales select store.sk, sale_address.sk, net_profit limit 10;`

  ```text
  Syntax error in stdin: 3 undefined concept references; fix all before re-running:
    - store.sk (line 2, col 8, in SELECT); did you mean: store_sales.store.sk, store_sales.store.date.sk, store_sales.item.sk, store_sales.date.sk, store_sales.time.sk, sale_address.sk?
    - sale_address.sk (line 2, col 18, in SELECT); did you mean: store_sales.sale_address.sk, store_sales.return_address.sk, store_sales.customer.address.sk, store_sales.return_customer.address.sk, store_sales.date.sk, store.sk?
    - local.net_profit (line 2, col 35, in SELECT); did you mean: store_sales.net_profit?
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query45.preql`

  ```text
  Syntax error in query45.preql: Undefined concept: item.sk. Suggestions: ['ws.item.sk', 'ws.date.sk', 'ws.time.sk', 'ws.web_site.sk', 'ws.ship_date.sk', 'ws.ship_customer.sk']
  ```
- `trilogy file read query49.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query52.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query56.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query62.preql`

  ```text
  Syntax error in query62.preql: Undefined concept: ship_date.year (line 4, col 3, in WHERE). Suggestions: ['web_sales.ship_date.year', 'web_sales.date.year', 'web_sales.return_date.year', 'web_sales.ship_customer.first_sales_date.year', 'web_sales.ship_customer.first_shipto_date.year', 'web_sales.return_customer.first_shipto_date.year']
  ```
- `trilogy run query63.preql`

  ```text
  Syntax error in query63.preql: Undefined concept: local.month_of_year (line 17, col 5, in SELECT). Suggestions: ['sales.date.month_of_year', 'sales.store.date.month_of_year', 'sales.return_date.month_of_year', 'sales.return_store.date.month_of_year', 'sales.customer.first_sales_date.month_of_year', 'sales.customer.first_shipto_date.month_of_year']
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 24). The requested concepts split into 2 disconnected subgraphs: {item.color, item.current_price, item.id, _store_agg_item_id, _store_agg_product_name}; {_store_agg_ca_city, _store_agg_ca_street_name, _store_agg_ca_street_num, _store_agg_ca_zip, _store_agg_cnt, _store_agg_first_sales_year, _store_agg_first_ship_year, _store_agg_sa_city, _store_agg_sa_street_name, _store_agg_sa_street_num, _store_agg_sa_zip, _store_agg_sale_year, _store_agg_store_name, _store_agg_store_zip, _store_agg_sum_coupon, _store_agg_sum_list, _store_agg_sum_wholesale, ss.customer.demographics.marital_status, ss.customer_demographic.marital_status, ss.is_returned}.
    - `item.color` is disconnected — did you mean `ss.item.color`? (connected to the other concepts)
    - `item.current_price` is disconnected — did you mean `ss.item.current_price`? (connected to the other concepts)
    - `item.id` is disconnected — did you mean `ss.item.id`? (connected to the other concepts)
  These look like separately-imported copies of models already reachable through a connected import; chain through that path (e.g. `ss.item.color`) instead of importing a second, disconnected copy.
  ```
- `trilogy run query65.preql`

  ```text
  Syntax error in query65.preql: ORDER BY references 'ss.store.sk', which is not in the SELECT projection (line 9). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --ss.store.sk order by ss.store.sk asc`.
  ```
- `trilogy file read query65.preql`

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
- `trilogy run --import raw/web_sales:ws select ws.net_paid, ws.net_paid_inc_tax, ws.net_profit, ws.ext_sales_price, ws.sales_price, ws.quantity limit 3;`

  ```text
  Syntax error in stdin: Undefined concept: ws.net_paid_inc_tax (line 2, col 21, in SELECT). Suggestions: ['ws.net_paid', 'ws.return_amount_inc_tax', 'ws.return_tax']
  ```
- `trilogy run query67.preql`

  ```text
  Syntax error in query67.preql: `by rollup (…)` requires at least one aggregate (or re-aggregable pre-aggregated measure) in the select to group; found none.
  ```
- `trilogy run query67_check.preql`

  ```text
  Syntax error in query67_check.preql: Nothing was executed: parsed 2 definition statement(s) (1 import, 1 rowset) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy file read query67.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query74.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query75.preql`

  ```text
  Syntax error in query75.preql: 2 undefined concept references; fix all before re-running:
    - y2001.yearly.total_qty (line 47, col 7, in WHERE); did you mean: y2001.yearly.cat_id, y2001.yearly.manuf_id, y2001.yearly.class_id, y2002.yearly.total_qty, yearly.total_qty, yearly.total_amt?
    - y2002.yearly.total_qty (line 48, col 7, in WHERE); did you mean: y2002.yearly.cat_id, y2002.yearly.manuf_id, y2002.yearly.class_id, y2001.yearly.total_qty, yearly.total_qty, yearly.total_amt?
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query78.preql`

  ```text
  Syntax error in query78.preql: Undefined concept: date.year. Suggestions: ['store.date.year', 'web.date.year', 'catalog.date.year', 'store.store.date.year', 'store.return_store.date.year', 'web.ship_date.year']
  ```
- `trilogy file read query79.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file write query80.preql -e -c`

  ```text
  --escapes only applies to an inline `--content <value>`; stdin already supports real newlines, so drop `-e` when piping content.
  ```
- `trilogy file read raw/repro.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw\store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query89.preql`

  ```text
  Syntax error in query89.preql: 17 undefined concept references; fix all before re-running:
    - item.category (line 11, col 5, in SELECT); did you mean: store_sales.item.category, item.class, store_sales.item.category_id?
    - item.class (line 12, col 5, in SELECT); did you mean: store_sales.item.class, item.category?
    - item.brand_name (line 13, col 5, in SELECT); did you mean: store_sales.item.brand_name?
    - store.name (line 14, col 5, in SELECT); did you mean: store_sales.store.name, store.company_name, store_sales.return_store.name?
    - store.company_name (line 15, col 5, in SELECT); did you mean: store_sales.store.company_name, store.name, store_sales.return_store.company_name?
    - date.month_of_year (line 16, col 5, in SELECT); did you mean: store_sales.date.month_of_year, store_sales.store.date.month_of_year, store_sales.return_store.date.month_of_year, date.year, store_sales.return_date.month_of_year, store_sales.customer.first_sales_date.month_of_year?
    - date.year (line 7, col 5, in WHERE); did you mean: store_sales.date.year, store_sales.store.date.year, store_sales.return_store.date.year, date.month_of_year, store_sales.return_date.year, store_sales.customer.first_sales_date.year?
    - item.category (line 8, col 11, in WHERE); did you mean: store_sales.item.category, item.class, store_sales.item.category_id?
    - item.class (line 8, col 65, in WHERE); did you mean: store_sales.item.class, item.category?
    - item.category (line 9, col 14, in WHERE); did you mean: store_sales.item.category, item.class, store_sales.item.category_id?
    - item.class (line 9, col 61, in WHERE); did you mean: store_sales.item.class, item.category?
    - store.name (line 24, col 5, in ORDER BY); did you mean: store_sales.store.name, store.company_name, store_sales.return_store.name?
    - item.category (line 25, col 5, in ORDER BY); did you mean: store_sales.item.category, item.class, store_sales.item.category_id?
    - item.class (line 26, col 5, in ORDER BY); did you mean: store_sales.item.class, item.category?
    - item.brand_name (line 27, col 5, in ORDER BY); did you mean: store_sales.item.brand_name?
    - store.company_name (line 28, col 5, in ORDER BY); did you mean: store_sales.store.company_name, store.name, store_sales.return_store.company_name?
    - date.month_of_year (line 29, col 5, in ORDER BY); did you mean: store_sales.date.month_of_year, store_sales.store.date.month_of_year, store_sales.return_store.date.month_of_year, date.year, store_sales.return_date.month_of_year, store_sales.customer.first_sales_date.month_of_year?
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run query93.preql --import raw.store_sales:ss select distinct ss.return_reason.desc, ss.return_reason.id limit 50;`

  ```text
  --import only applies to inline queries, not file/directory inputs.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe web_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy run --import raw/all_sales:all_sales select all_sales.channel, all_sales.channel_dim_text_id, min(all_sales.channel_dim_id) as min_id, count(all_sales.channel) as cnt group by all_sales.channel, all_sales.channel_dim_text_id order by all_sales.channel, all_sales.channel_dim_text_id limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...unt(all_sales.channel) as cnt ??? group by all_sales.channel, al...
  ```
- `trilogy run --import raw/all_sales:all_sales select all_sales.channel, all_sales.return_channel_dim_text_id, count(all_sales.return_channel_dim_text_id) as c… not null group by all_sales.channel, all_sales.return_channel_dim_text_id order by all_sales.channel, all_sales.return_channel_dim_text_id limit 20;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...annel_dim_text_id is not null ??? group by all_sales.channel, al...
  ```
- `trilogy run --import raw/all_sales:all_sales select all_sales.channel, all_sales.is_returned, sum(all_sales.ext_sales_price) as total_sales, sum(all_sales.return_amount) as total_returns where all_sales.channel='STORE' group by all_sales.channel, all_sales.is_returned limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...ere all_sales.channel='STORE' ??? group by all_sales.channel, al...
  ```
- `trilogy run --import raw/all_sales:all_sales select all_sales.channel, all_sales.return_channel_dim_text_id, all_sales.return_date.date as rdate, sum(all_sal…ween '2000-08-23'::date and '2000-09-06'::date group by all_sales.channel, all_sales.return_channel_dim_text_id, all_sales.return_date.date limit 20;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...::date and '2000-09-06'::date ??? group by all_sales.channel, al...
  ```
- `trilogy run --import raw.store_sales:ss select ss.customer.address.state, count(ss.line_item) as cnt where ss.item.category is not null and ss.customer.addre…tem.current_price) by ss.item.category group by ss.customer.address.state having cnt >= 10 order by cnt asc, ss.customer.address.state asc limit 100;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...nt_price) by ss.item.category ??? group by ss.customer.address.s...
  ```
- `trilogy file write query09.preql`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [213]: A `by <grain>` clause must follow an aggregate, but the expression before it has none. If the `by` sits inside an aggregate's parentheses (`max(x by *)`), move it outside the call: `max(x) by *`. To take each distinct value once per grain, wrap it in `group(...)` - e.g. `group(item.current_price) by item.id, item.category`. For a reduction, use an aggregate: `sum(x) by ...`, `avg(x) by ...`, `max(x) by ...`.
  Location:
  ..._81_100 end as bucket_81_100
   ??? by *;


  Write stats: received 1755 chars / 1755 bytes; tail: …'isc_81_100 else avg_net_81_100 end as bucket_81_100\r\\nby *;\r\\n'.
  ```
- `trilogy file write query09_check.preql`

  ```text
  refused to write 'query09_check.preql': not syntactically valid Trilogy.

  Parse error:
    --> 19:1
     |
  19 | by *;
     | ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
   100) by * as avg_net_81_100
   ??? by *;


  Write stats: received 1260 chars / 1260 bytes; tail: …'quantity between 81 and 100) by * as avg_net_81_100\r\\nby *;\r\\n'.
  ```
- `trilogy file write query09_check.preql`

  ```text
  refused to write 'query09_check.preql': not syntactically valid Trilogy.

  Parse error:
   --> 9:1
    |
  9 | by *;
    | ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...en 81 and 100) as cnt_81_100
   ??? by *;


  Write stats: received 403 chars / 403 bytes; tail: …'tem ? ss.quantity between 81 and 100) as cnt_81_100\r\\nby *;\r\\n'.
  ```
- `trilogy file write query10.preql --content import raw.customer as c;
import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sales as cs…ducation_status,
    purchase_estimate,
    credit_rating,
    dependent_count,
    employed_dependent_count,
    college_dependent_count
limit 100;
`

  ```text
  refused to write 'query10.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:1
    |
  7 | rowset store_customers as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...ne store sale in Jan-Apr 2002 ??? rowset store_customers as wher...

  Write stats: received 2030 chars / 2030 bytes; tail: …'yed_dependent_count,\\n    college_dependent_count\\nlimit 100;\\n'.
  ```
- `trilogy file write query10.preql --content import raw.customer as c;
import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sales as cs…ducation_status,
    purchase_estimate,
    credit_rating,
    dependent_count,
    employed_dependent_count,
    college_dependent_count
limit 100;
`

  ```text
  refused to write 'query10.preql': not syntactically valid Trilogy.

  Parse error:
    --> 25:33
     |
  25 |   and c.sk in (web_customers.sk union catalog_customers.sk)
     |                                 ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...and c.sk in (web_customers.sk ??? union catalog_customers.sk) se...

  Write stats: received 1872 chars / 1872 bytes; tail: …'yed_dependent_count,\\n    college_dependent_count\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw.address:a select distinct a.county where a.county like '%Dona%' or a.county like '%Rush%' or a.county like '%Toole%' or a.county like '%Jefferson%' or a.county like '%La Porte%' limit 10;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...port raw.address as a; select ??? distinct a.county where a.coun...
  ```
- `trilogy file write query14.preql --content import raw.all_sales as sales;

# Overall average sale value (quantity * list_price) in 1999-2001 across all chann…ory_id)
order by channel nulls first, sales.item.brand_id nulls first, sales.item.class_id nulls first, sales.item.category_id nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
    --> 40:1
     |
  40 | by rollup (channel, sales.item.brand_id, sales.item.class_id, sales.item.category_id)
     | ^---
     |
     = expected limit, order_by, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
   (select overall_avg.avg_val) ??? by rollup (channel, sales.item...

  Hint: the `by rollup/cube/grouping sets` clause must come *before* HAVING in Trilogy (same order as SQL's `GROUP BY ... HAVING`). Reorder to:
      select <cols> by rollup (<keys>) having <cond> order by <cols> limit <n>;

  Write stats: received 1674 chars / 1674 bytes; tail: …'d nulls first, sales.item.category_id nulls first\\nlimit 100;'.
  ```
- `trilogy file write query17.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;

# Store-sale ticket quantity: from store_sales sold…in cs.cust_sk = ss_filtered.cust_sk
order by cs.item_id asc nulls first
limit 100;

where cs.sold_date.year = 2001 or cs.sold_date.year = 2002
select`

  ```text
  refused to write 'query17.preql': not syntactically valid Trilogy.

  Parse error:
    --> 68:7
     |
  68 | select
     |       ^---
     |
     = expected select_item
  Location:
  ...s.sold_date.year = 2002 select ???

  Write stats: received 2321 chars / 2321 bytes; tail: …' cs.sold_date.year = 2001 or cs.sold_date.year = 2002\\nselect'.
  ```
- `trilogy run --import raw.store_sales:ss select ss.store.id, ss.store.name, count(ss.ticket_number) as cnt where ss.date.year = 2001 and ss.date.month_of_year = 4 and ss.is_returned = True group by 1,2 order by 1 limit 20;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...= 4 and ss.is_returned = True ??? group by 1,2 order by 1 limit
  ```
- `trilogy file write query27.preql --content import raw.store_sales as store_sales;

where
  store_sales.date.year = 2002
  and store_sales.store.state = 'TN'
….sales_price) as avg_unit_price
by rollup (store_sales.item.id, store_sales.store.state)
order by item_code nulls first, state nulls first
limit 100;`

  ```text
  refused to write 'query27.preql': not syntactically valid Trilogy.

  Parse error:
    --> 12:31
     |
  12 |   grouping(store_sales.item.id, store_sales.store.state) as group_indicator,
     |                               ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
    grouping(store_sales.item.id ??? , store_sales.store.state) as

  Write stats: received 765 chars / 765 bytes; tail: …'order by item_code nulls first, state nulls first\\nlimit 100;'.
  ```
- `trilogy run query29.preql`

  ```text
  Syntax error in query29.preql: Syntax [220]: Filter or stray clause after a `join`? A query-scoped join `inner|left|full join <a> = <b>` takes only key equalities - to join on multiple keys, chain `= c` (one equivalence group) or separate distinct groups with `and` (`a = b and c = d`); STACK another `join` clause for a different join type. Note `and` joins KEY EQUALITIES only, not filters. Joins go right after the `select` list (preferred, SQL-like) or before `select`; the order is `where` -> `select` <cols> -> join(s) -> `having` -> `order by` -> `limit`. Filter input rows in `where` (before `select`); filter a joined or aggregated RESULT in `having` (select the field, hide it with a leading `--`). Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...ear 1999, 2000, or 2001   and ??? cs.sold_date.year in (1999, 20...
  ```
- `trilogy file write query31.preql --content import raw/store_sales as store;
import raw/web_sales as web;

# Store sales: extended sales price by county, year…
select
    web.bill_address.county as county,
    web.date.year as yr,
    web.date.quarter as qtr,
    sum(web.ext_sales_price) as web_ext_price
;
`

  ```text
  refused to write 'query31.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/store_sales as store;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/store_sales as store; impo...

  Write stats: received 598 chars / 598 bytes; tail: …'ter as qtr,\\n    sum(web.ext_sales_price) as web_ext_price\\n;\\n'.
  ```
- `trilogy file write query32.preql --content import raw/catalog_sales as cs;

# Per-item average extended discount amount across ALL catalog sales for that ite…:date and '2000-04-26'::date
select
    sum(cs.discount_amount ? cs.discount_amount > 1.3 * item_avg_discount) as total_extended_discount
limit 100;
`

  ```text
  refused to write 'query32.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/catalog_sales as cs;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/catalog_sales as cs;  # Pe...

  Write stats: received 436 chars / 436 bytes; tail: …' * item_avg_discount) as total_extended_discount\\nlimit 100;\\n'.
  ```
- `trilogy file write query38.preql -e -c import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

with matching as intersect(…ws.billing_customer.first_name,
        ws.date.date as sale_date
  )
) -> (last_name, first_name, sale_date)

select
    count(*) as cnt
limit 100;
`

  ```text
  refused to write 'query38.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...t_name, first_name, sale_date) ???  select     count(*) as cnt l...

  Write stats: received 807 chars / 807 bytes; tail: …'rst_name, sale_date)\\n\\nselect\\n    count(*) as cnt\\nlimit 100;\\n'.
  ```
- `trilogy file write query38.preql -e -c import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

with matching as intersect(…s.billing_customer.first_name,
        ws.date.date as sale_date
  )
) -> (last_name, first_name, sale_date);

select
    count(*) as cnt
limit 100;
`

  ```text
  refused to write 'query38.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...name, sale_date);  select     ??? count(*) as cnt limit 100;

  Write stats: received 808 chars / 808 bytes; tail: …'st_name, sale_date);\\n\\nselect\\n    count(*) as cnt\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw.store_sales:ss --import raw.web_sales:ws with test as intersect((where ss.date.year = 2000 and ss.customer.sk is not null select ss.…ll select ws.billing_customer.last_name, ws.billing_customer.first_name, ws.date.date as sd)) -> (ln, fn, sd) limit 10; select count(test.ln) as cnt;`

  ```text
  Syntax error in stdin: Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...e.date as sd)) -> (ln, fn, sd) ??? limit 10; select count(test.l...
  ```
- `trilogy file write query41.preql --content # Query 41: Unique product names of items whose manufacturer_id is between 1 and 500
# and whose manufact is share…ufacturer_id between 1 and 500
  and item.manufact in matching_manufacts
select distinct
  item.product_name
order by
  item.product_name
limit 100;
`

  ```text
  refused to write 'query41.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
   in matching_manufacts select ??? distinct   item.product_name o...

  Write stats: received 1295 chars / 1295 bytes; tail: …'  item.product_name\\norder by\\n  item.product_name\\nlimit 100;\\n'.
  ```
- `trilogy file write query51.preql --content import raw.store_sales as ss
import raw.web_sales as ws

# Per-item per-day unit price totals for store channel
wi….d) as d,
    coalesce(ss_daily.ss_amt, 0) as ss_daily_total,
    coalesce(ws_daily.ws_amt, 0) as ws_daily_total
order by 1, 2 nulls first
limit 100;`

  ```text
  refused to write 'query51.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw.store_sales as ss
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw.store_sales as ss import r...

  Write stats: received 832 chars / 832 bytes; tail: …'t, 0) as ws_daily_total\\norder by 1, 2 nulls first\\nlimit 100;'.
  ```
- `trilogy file write query51.preql`

  ```text
  refused to write 'query51.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw.store_sales as ss
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw.store_sales as ss
   import

  Write stats: received 864 chars / 864 bytes; tail: …') as ws_daily_total\r\\norder by 1, 2 nulls first\r\\nlimit 100;\r\\n'.
  ```
- `trilogy file write query54.preql --content import raw.catalog_sales as cs;
import raw.web_sales as ws;
import raw.store_sales as ss;
import raw.item as item;…er_count,
    round(sum(store_window.ss.ext_sales_price) / 50) * 50 as segment_times_50
order by segment, customer_count, segment_times_50
limit 100;`

  ```text
  refused to write 'query54.preql': not syntactically valid Trilogy.

  Parse error:
    --> 30:6
     |
  30 |     (import raw.catalog_sales as cs;
     |      ^---
     |
     = expected select_statement
  Location:
   with qual_all as union(     ( ??? import raw.catalog_sales as cs...

  Write stats: received 2393 chars / 2393 bytes; tail: …'rder by segment, customer_count, segment_times_50\\nlimit 100;'.
  ```
- `trilogy file write query54.preql --content import raw.catalog_sales as cs;
import raw.web_sales as ws;
import raw.store_sales as ss;

# Qualifying customers …88 and 1190
  and ss.store.county = ss.customer.address.county
  and ss.store.state = ss.customer.address.state
select
    ss.customer.sk
;
limit 10;`

  ```text
  refused to write 'query54.preql': not syntactically valid Trilogy.

  Parse error:
    --> 27:1
     |
  27 | limit 10;
     | ^---
     |
     = expected EOI, block, or show_statement
  Location:
  ...e select     ss.customer.sk ; ??? limit 10;

  Write stats: received 976 chars / 976 bytes; tail: …'customer.address.state\\nselect\\n    ss.customer.sk\\n;\\nlimit 10;'.
  ```
- `trilogy run --import raw/web_sales:ws select coalesce(sum(ws.ext_sales_price ? ws.date.month_of_year = 1) / nullif(max(ws.warehouse.square_feet), 0), 0) as test_val, sum(ws.ext_sales_price ? ws.date.month_of_year = 1) as test_sum, max(ws.warehouse.square_feet) as test_sqft, coalsece(1/0, 0) limit 1;`

  ```text
  Syntax error in stdin:  --> 2:246
    |
  2 | select coalesce(sum(ws.ext_sales_price ? ws.date.month_of_year = 1) / nullif(max(ws.warehouse.square_feet), 0), 0) as test_val, sum(ws.ext_sales_price ? ws.date.month_of_year = 1) as test_sum, max(ws.warehouse.square_feet) as test_sqft, coalsece(1/0, 0) limit 1;
    |                                                                                                                                                                                                                                                      ^---
    |
    = expected limit, order_by, where, having, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, select_grouping, or JOIN_TYPE
  Location:
  ...e_feet) as test_sqft, coalsece ??? (1/0, 0) limit 1;
  ```
- `trilogy run --import raw.store_sales:ss select ss.store.state, sum(ss.net_profit) as total_profit where ss.date.year = 2000 and ss.store.state is not null group by ss.store.state order by total_profit desc limit 20;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...nd ss.store.state is not null ??? group by ss.store.state order
  ```
- `trilogy file write query72.preql --content import raw/catalog_sales as cs;
import raw/inventory as inv;

where
    cs.sold_date.year = 1999
    and cs.bill_h…rder by
    total_orders desc nulls first,
    item_description nulls first,
    warehouse_name nulls first,
    week_sequence nulls first
limit 100;`

  ```text
  refused to write 'query72.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/catalog_sales as cs;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/catalog_sales as cs; impor...

  Write stats: received 937 chars / 937 bytes; tail: …'e_name nulls first,\\n    week_sequence nulls first\\nlimit 100;'.
  ```
- `trilogy file write query74.preql -e -c import raw.store_sales as store;
import raw.web_sales as web;

where
  store.date.year in (2001, 2002)
  and web.date.…d as customer_code,
  store.customer.first_name as first_name,
  store.customer.last_name as last_name
order by customer_code nulls first
limit 100;
`

  ```text
  refused to write 'query74.preql': not syntactically valid Trilogy.

  Parse error:
    --> 24:4
     |
  24 | -- select the 3 output cols
     |    ^---
     |
     = expected PURPOSE, PROPERTY, UNIQUE, or AUTO
  Location:
  ...ode nulls first limit 100; -- ??? select the 3 output cols with

  Write stats: received 993 chars / 993 bytes; tail: …' as last_name\\norder by customer_code nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query77.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

-- Store channel: sales…ed.profit) as profit
by rollup (combined.channel, combined.outlet)
order by channel asc nulls first, outlet asc nulls first, returns desc
limit 100;
`

  ```text
  refused to write 'query77.preql': not syntactically valid Trilogy.

  Parse error:
   --> 5:4
    |
  5 | -- Store channel: sales aggregated by store
    |    ^---
    |
    = expected PURPOSE, PROPERTY, UNIQUE, or AUTO
  Location:
  ...port raw.web_sales as ws;  -- ??? Store channel: sales aggregate...

  Write stats: received 4088 chars / 4088 bytes; tail: …'ulls first, outlet asc nulls first, returns desc\\nlimit 100;\\n'.
  ```
- `trilogy run - --import raw/all_sales:as`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...nel, count(as.channel) as cnt ??? group by as.channel order by a...
  ```
- `trilogy file write query87.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Store sales (year 200…, sale_date from web_combos)
) -> (last_name string?, first_name string?, sale_date date)
;

select count(*) as unique_combinations
from only_store
;`

  ```text
  refused to write 'query87.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
      (select ln, fn, sale_date ??? from store_combos),     (selec...

  Write stats: received 1156 chars / 1156 bytes; tail: …'\\n;\\n\\nselect count(*) as unique_combinations\\nfrom only_store\\n;'.
  ```
- `trilogy file write query87.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Store-only (last_name…sale_date)
) -> (last_name string?, first_name string?, sale_date date);

select count(only_store.sale_date) as unique_combinations
from only_store
;`

  ```text
  refused to write 'query87.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ..._date) as unique_combinations ??? from only_store ;

  Write stats: received 1077 chars / 1077 bytes; tail: …'ly_store.sale_date) as unique_combinations\\nfrom only_store\\n;'.
  ```
- `trilogy run --import raw.web_sales:web with candidate_orders as 
where web.ship_date.date between '1999-02-01'::date and '1999-04-02'::date 
  and web.ship_a…nd web.web_site.company_name = 'pri'
select distinct web.order_number as order_id;

select candidate_orders.order_id 
from candidate_orders
limit 10;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...e.company_name = 'pri' select ??? distinct web.order_number as o...
  ```

### `cli-misuse`

- `trilogy explore raw/sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/sales.preql' does not exist.
  ```
- `trilogy explore raw/store_returns.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_returns.preql' does not exist.
  ```
- `trilogy explore raw/sales_fact.preql`

  ```text
  Invalid value for 'PATH': File 'raw/sales_fact.preql' does not exist.
  ```
- `trilogy explore raw/web_returns.preql`

  ```text
  Invalid value for 'PATH': File 'raw/web_returns.preql' does not exist.
  ```

### `type-error`

- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Type error in query08.preql: Invalid argument type 'ArrayType<STRING>' passed into SUBSTRING function in position 1 from concept: local.qualifying_zips. Valid: 'STRING'.
  ```

### `syntax-missing-alias`

- `trilogy file write query95.preql -c import raw.web_sales as ws;

# First: base filter on ship_date, address, web_site
# Then per-order: check multi-warehouse…te totals from original fact
  sum(ws.ext_ship_cost) as total_ext_ship_cost,
  sum(ws.net_profit) as total_net_profit
order by
  order_cnt
limit 100;`

  ```text
  refused to write 'query95.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `need to re-compute totals from original fact
    sum(ws.ext_ship_cost) as need_to_re_compute_totals_from_original_`
  Location:
  ...mber) as order_cnt,   -- need ??? to re-compute totals from orig...

  Write stats: received 764 chars / 764 bytes; tail: …'_profit) as total_net_profit\\norder by\\n  order_cnt\\nlimit 100;'.
  ```
