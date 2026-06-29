# Trilogy failure analysis — 20260629-030016

- Run `20260629-030015_enriched` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 1294 | failed: 100 (8%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 69 | 69% |
| `syntax-parse` | 28 | 28% |
| `syntax-missing-alias` | 2 | 2% |
| `type-error` | 1 | 1% |

## Detail

### `other`

- `trilogy run query02.preql duckdb -e -c import raw.all_sales as s;
where s.channel in ('WEB', 'CATALOG')
  and s.date.year = 2001
select
    s.date.week_seq as wk,
    count(s.date.day_of_week ? true) as days_with_sales
order by wk asc
limit 55;`

  ```text
  Environment variable must be in KEY=VALUE format or be a path to an existing env file: -c
  ```
- `trilogy file read query02.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query03.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query04.preql`

  ```text
  Syntax error in query04.preql: Undefined concept: s1.cust_id. Suggestions: ['store_val_2001.cust_id', 'store_val_2002.cust_id', 'catalog_val_2001.cust_id', 'catalog_val_2002.cust_id', 'web_val_2001.cust_id', 'web_val_2002.cust_id']
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
  Syntax error in query06.preql: 5 undefined concept references; fix all before re-running:
    - item.category (line 8, col 9, in WHERE); did you mean: store_sales.item.category, store_sales.item.category_id?
    - item.current_price (line 10, col 9, in WHERE); did you mean: store_sales.item.current_price?
    - item.current_price (line 3, in WHERE); did you mean: store_sales.item.current_price?
    - item.category (line 10, col 63, in WHERE); did you mean: store_sales.item.category, store_sales.item.category_id?
    - customer.address.id (line 12, col 9, in WHERE); did you mean: store_sales.customer.address.id, store_sales.item.id, store_sales.date.id, store_sales.return_date.id, store_sales.time.id, store_sales.return_time.id?
  ```
- `trilogy run query06.preql`

  ```text
  Resolution error in query06.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 13). The requested concepts split into 2 disconnected subgraphs: {local.avg_price_by_cat}; {local.line_item_count, local.state, store_sales.customer.address.id, store_sales.date.month_of_year, store_sales.date.year, store_sales.item.category, store_sales.item.current_price}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query06.preql`

  ```text
  Resolution error in query06.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 7). The requested concepts split into 2 disconnected subgraphs: {local.cat_avg_price}; {local.line_item_count, local.state, store_sales.customer.address.id, store_sales.date.month_of_year, store_sales.date.year, store_sales.item.category, store_sales.item.current_price}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read query06.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query07.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query10.preql duckdb`

  ```text
  Syntax error in query10.preql: Undefined concept: customer.address.county. Suggestions: ['store_sales.customer.address.county', 'store_sales.return_customer.address.county', 'store_sales.store.county', 'store_sales.return_store.county', 'store_sales.sale_address.county', 'web_sales.billing_customer.address.county']
  ```
- `trilogy file read query11.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query12.preql`

  ```text
  Syntax error in query12.preql: 6 undefined concept references; fix all before re-running:
    - item.category (line 4, col 7, in WHERE); did you mean: ws.item.category, item_category, ws.item.category_id?
    - date.date (line 5, col 7, in WHERE); did you mean: ws.date.date, ws.ship_date.date, ws.return_date.date, ws.billing_customer.first_sales_date.date, ws.billing_customer.first_shipto_date.date, ws.ship_customer.first_sales_date.date?
    - item.category (line 15, col 5, in ORDER BY); did you mean: ws.item.category, item_category, ws.item.category_id?
    - item.class (line 16, col 5, in ORDER BY); did you mean: ws.item.class, item_class, ws.item.class_id?
    - item.text_id (line 17, col 5, in ORDER BY); did you mean: ws.item.text_id, ws.date.text_id, ws.ship_date.text_id, ws.return_date.text_id, ws.time.text_id, ws.return_time.text_id?
    - item.desc (line 18, col 5, in ORDER BY); did you mean: ws.item.desc, item_code, item_description?
  ```
- `trilogy run query13.preql`

  ```text
  Syntax error in query13.preql: 23 undefined concept references; fix all before re-running:
    - local.quantity (line 7, in SELECT); did you mean: store_sales.quantity?
    - local.ext_sales_price (line 7, in SELECT); did you mean: store_sales.ext_sales_price, sales_price, avg_extended_sales_price?
    - local.ext_wholesale_cost (line 7, in SELECT); did you mean: store_sales.ext_wholesale_cost, avg_extended_wholesale_cost, total_extended_wholesale_cost?
    - date.year (line 7, col 7, in WHERE); did you mean: store_sales.date.year, store_sales.store.date.year, store_sales.return_store.date.year, store_sales.return_date.year, store_sales.customer.first_sales_date.year, store_sales.customer.first_shipto_date.year?
    - customer_demographic.marital_status (line 10, col 6, in WHERE); did you mean: store_sales.customer_demographic.marital_status, store_sales.customer.demographics.marital_status, store_sales.return_customer.demographics.marital_status, customer_demographic.education_status?
    - customer_demographic.education_status (line 10, col 52, in WHERE); did you mean: store_sales.customer_demographic.education_status, store_sales.customer.demographics.education_status, store_sales.return_customer.demographics.education_status, customer_demographic.marital_status?
    - local.sales_price (line 10, col 114, in WHERE); did you mean: store_sales.sales_price, ext_sales_price, store_sales.list_price?
    - household_demographic.dependent_count (line 10, col 150, in WHERE); did you mean: store_sales.customer.household_demographic.dependent_count, store_sales.return_customer.household_demographic.dependent_count, store_sales.household_demographic.dependent_count, store_sales.customer.demographics.dependent_count, store_sales.return_customer.demographics.dependent_count, store_sales.customer_demographic.dependent_count?
    - customer_demographic.marital_status (line 11, col 9, in WHERE); did you mean: store_sales.customer_demographic.marital_status, store_sales.customer.demographics.marital_status, store_sales.return_customer.demographics.marital_status, customer_demographic.education_status?
    - customer_demographic.education_status (line 11, col 55, in WHERE); did you mean: store_sales.customer_demographic.education_status, store_sales.customer.demographics.education_status, store_sales.return_customer.demographics.education_status, customer_demographic.marital_status?
    - local.sales_price (line 11, col 109, in WHERE); did you mean: store_sales.sales_price, ext_sales_price, store_sales.list_price?
    - household_demographic.dependent_count (line 11, col 144, in WHERE); did you mean: store_sales.customer.household_demographic.dependent_count, store_sales.return_customer.household_demographic.dependent_count, store_sales.household_demographic.dependent_count, store_sales.customer.demographics.dependent_count, store_sales.return_customer.demographics.dependent_count, store_sales.customer_demographic.dependent_count?
    - customer_demographic.marital_status (line 12, col 9, in WHERE); did you mean: store_sales.customer_demographic.marital_status, store_sales.customer.demographics.marital_status, store_sales.return_customer.demographics.marital_status, customer_demographic.education_status?
    - customer_demographic.education_status (line 12, col 55, in WHERE); did you mean: store_sales.customer_demographic.education_status, store_sales.customer.demographics.education_status, store_sales.return_customer.demographics.education_status, customer_demographic.marital_status?
    - local.sales_price (line 12, col 113, in WHERE); did you mean: store_sales.sales_price, ext_sales_price, store_sales.list_price?
    - household_demographic.dependent_count (line 12, col 149, in WHERE); did you mean: store_sales.customer.household_demographic.dependent_count, store_sales.return_customer.household_demographic.dependent_count, store_sales.household_demographic.dependent_count, store_sales.customer.demographics.dependent_count, store_sales.return_customer.demographics.dependent_count, store_sales.customer_demographic.dependent_count?
    - sale_address.country (line 15, col 7, in WHERE); did you mean: store_sales.sale_address.country, store_sales.customer.address.country, store_sales.return_customer.address.country, store_sales.sale_address.county, sale_address.state?
    - sale_address.state (line 17, col 6, in WHERE); did you mean: store_sales.sale_address.state, store_sales.customer.address.state, store_sales.return_customer.address.state, store_sales.store.state, store_sales.return_store.state, sale_address.country?
    - local.net_profit (line 17, col 45, in WHERE); did you mean: store_sales.net_profit?
    - sale_address.state (line 18, col 9, in WHERE); did you mean: store_sales.sale_address.state, store_sales.customer.address.state, store_sales.return_customer.address.state, store_sales.store.state, store_sales.return_store.state, sale_address.country?
    - local.net_profit (line 18, col 54, in WHERE); did you mean: store_sales.net_profit?
    - sale_address.state (line 19, col 9, in WHERE); did you mean: store_sales.sale_address.state, store_sales.customer.address.state, store_sales.return_customer.address.state, store_sales.store.state, store_sales.return_store.state, sale_address.country?
    - local.net_profit (line 19, col 54, in WHERE); did you mean: store_sales.net_profit?
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: ORDER BY contains aggregate `grouping(s.channel)` (line 20), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(s.channel) as g order by g desc`.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 1 column 1108 (char 1107). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query17.preql`

  ```text
  Syntax error in query17.preql: Undefined concept: sr.customer.id. Suggestions: ['ss.item.id', 'ss.date.id', 'ss.return_date.id', 'ss.time.id', 'ss.return_time.id', 'ss.customer.demographics.id']
  ```
- `trilogy run query18.preql`

  ```text
  Syntax error in query18.preql: ORDER BY contains aggregate `grouping(local.country)` (line 3), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(local.country) as g order by g desc`.
  ```
- `trilogy file read query18.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query19.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query20.preql`

  ```text
  Resolution error in query20.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 6). The requested concepts split into 3 disconnected subgraphs: {item.category, local.current_price, local.item_category, local.item_class, local.item_code, local.item_description}; {local.pct_of_class_sales, local.total_ext_sales_price}; {sold_date.date}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query21.preql`

  ```text
  Syntax error in query21.preql: Cannot compare INTEGER (ref:inv.date.id) and DATE (constant(2000-02-10)) of different types with operator >= in ref:inv.date.id >= constant(2000-02-10)
  ```
- `trilogy file read query22.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 41 column 13 (char 1707). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 43 column 12 (char 1605). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query35.preql`

  ```text
  Resolution error in query35.preql: Cannot resolve cross-rowset INNER join store_customers.c_id = web_customers.c_id: it intersects two independent rowsets but the collapse dropped store_customers.c_id, silently losing the intersection. Rewrite the intersection as a `union(...)` of the arms with a channel marker, then keep tuples whose `count_distinct(channel)` equals the number of arms.
  ```
- `trilogy run query37.preql`

  ```text
  Resolution error in query37.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 5). The requested concepts split into 2 disconnected subgraphs: {inv.date.date, inv.quantity_on_hand}; {item.current_price, item.desc, item.id, item.manufacturer_id, item.text_id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read query40.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query43.preql`

  ```text
  Syntax error in query43.preql: Undefined concept: date.year (line 3, col 7, in WHERE). Suggestions: ['ss.date.year', 'ss.store.date.year', 'ss.return_store.date.year', 'ss.return_date.year', 'ss.customer.first_sales_date.year', 'ss.customer.first_shipto_date.year']
  ```
- `trilogy file read query47.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run ./query48.preql`

  ```text
  Syntax error in query48.preql: Comparison `store_sales.customer_demographic.marital_status = 'Married'` can never match enum field 'store_sales.customer_demographic.marital_status', which contains only these values: 'M', 'S', 'D', 'W', 'U'. It is always false and should be removed.
  ```
- `trilogy run query49.preql`

  ```text
  Syntax error in query49.preql: Undefined concept: channel (line 42, col 5, in ORDER BY). Suggestions: ['sales.channel', 'qualifying_sales.sales.channel', 'item_metrics.qualifying_sales.sales.channel']
  ```
- `trilogy run query49.preql`

  ```text
  Syntax error in query49.preql: ORDER BY references 'item_metrics.total_return_amt', which is not in the SELECT projection (line 33). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --item_metrics.total_return_amt order by item_metrics.total_return_amt asc`.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query56.preql`

  ```text
  Resolution error in query56.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 8). The requested concepts split into 2 disconnected subgraphs: {local.item_code}; {local.total}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query56.preql`

  ```text
  Resolution error in query56.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 18). The requested concepts split into 2 disconnected subgraphs: {local.item_code}; {local.total}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query56.preql`

  ```text
  Resolution error in query56.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 14). The requested concepts split into 2 disconnected subgraphs: {local.item_code}; {local.total}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query57.preql`

  ```text
  Syntax error in query57.preql: Comparison `cs.sold_date.month_of_year between 1 and 12` matches every value of enum field 'cs.sold_date.month_of_year', which contains only these values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12. It is always true and should be removed.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 57 column 12 (char 2875). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file read query59.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 46 column 20 (char 1915). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query65.preql`

  ```text
  Syntax error in query65.preql: ORDER BY references 'ss.store.id', which is not in the SELECT projection (line 11). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --ss.store.id order by ss.store.id asc`.
  ```
- `trilogy file read query65.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read query66.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query67.preql`

  ```text
  Syntax error in query67.preql: Undefined concept: item.category. Suggestions: ['store_sales.item.category', 'rollup_data.item.category', 'store_sales.item.category_id']
  ```
- `trilogy run test_rank13.preql`

  ```text
  Unexpected error in test_rank13.preql: (_duckdb.BinderException) Binder Error: column "county" must appear in the GROUP BY clause or must be part of an aggregate function.
  Either add it to the GROUP BY list, or use "ANY_VALUE(county)" if the exact value of "county" is not important.

  LINE 47:     "cheerful"."county" as "county",
               ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "ss_store_sales"."SS_NET_PROFIT" as "ss_net_profit",
      "ss_store_store"."S_COUNTY" as "ss_store_county",
      "ss_store_store"."S_STATE" as "ss_store_state",
      coalesce("ss_store_store"."S_COUNTY",'~~all') as "county",
      coalesce("ss_store_store"."S_STATE",'~~total') as "state"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
      INNER JOIN "store" as "ss_store_store" on "ss_store_sales"."SS_STORE_SK" = "ss_store_store"."S_STORE_SK"
  WHERE
      "ss_date_date"."D_YEAR" = 2000 and "ss_store_store"."S_STATE" = 'TN'
  ),
  cooperative as (
  SELECT
      "ss_store_store"."S_COUNTY" as "ss_store_county",
      "ss_store_store"."S_STATE" as "ss_store_state",
      coalesce("ss_store_store"."S_COUNTY",'~~all') as "county",
      coalesce("ss_store_store"."S_STATE",'~~total') as "state"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
      INNER JOIN "store" as "ss_store_store" on "ss_store_sales"."SS_STORE_SK" = "ss_store_store"."S_STORE_SK"
  WHERE
      "ss_date_date"."D_YEAR" = 2000 and "ss_store_store"."S_STATE" = 'TN'

  GROUP BY
      1,
      2,
      3,
      4),
  abundant as (
  SELECT
      "cheerful"."ss_store_county" as "ss_store_county",
      "cheerful"."ss_store_state" as "ss_store_state",
      grouping("cheerful"."ss_store_county") as "_virt_agg_grouping_9558422171575547",
      grouping("cheerful"."ss_store_state") as "_virt_agg_grouping_5741674531181097"
  FROM
      "cheerful"
  GROUP BY
      ROLLUP (2, 1)),
  thoughtful as (
  SELECT
      "cheerful"."county" as "county",
      "cheerful"."state" as "state",
      sum("cheerful"."ss_net_profit") as "np_sum"
  FROM
      "cheerful"
  GROUP BY
      ROLLUP ("cheerful"."ss_store_state", "cheerful"."ss_store_county")),
  questionable as (
  SELECT
      "cooperative"."ss_store_county" as "ss_store_county",
      "cooperative"."ss_store_state" as "ss_store_state",
      "thoughtful"."np_sum" as "np_sum"
  FROM
      "cooperative"
      INNER JOIN "thoughtful" on "cooperative"."county" is not distinct from "thoughtful"."county" AND "cooperative"."state" is not distinct from "thoughtful"."state"),
  uneven as (
  SELECT
      "abundant"."_virt_agg_grouping_9558422171575547" + "abundant"."_virt_agg_grouping_5741674531181097" as "lvl",
      "questionable"."np_sum" as "np_sum",
      coalesce(coalesce("abundant"."ss_store_county","questionable"."ss_store_county"),'~~all') as "county",
      coalesce(coalesce("abundant"."ss_store_state","questionable"."ss_store_state"),'~~total') as "state"
  FROM
      "abundant"
      INNER JOIN "questionable" on "abundant"."ss_store_county" is not distinct from "questionable"."ss_store_county" AND "abundant"."ss_store_state" is not distinct from "questionable"."ss_store_state")
  SELECT
      "uneven"."state" as "state",
      "uneven"."county" as "county",
      "uneven"."np_sum" as "total_np",
      "uneven"."lvl" as "level",
      rank() over (partition by "uneven"."lvl" order by "uneven"."np_sum" desc ) as "rn"
  FROM
      "uneven"
  ORDER BY
      "level" desc,
      "uneven"."state" asc nulls last,
      "rn" asc
  LIMIT (10)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file write --content import raw.all_sales as s; query71.preql`

  ```text
  trilogy file write: `--content` takes a SINGLE string argument. Your args list put 2 separate tokens after --content (treating it like a shell command). In a tool call, pass the entire file body as one string element after --content, with newlines embedded literally — e.g.
    {"args": ["file", "write", "query70.preql", "--content", "import raw.store_sales as store_sales;\n\nselect ..."]}
  Alternatively use `--escapes` with a single-line `\n`-escaped string.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe store_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy database describe web_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy database describe catalog_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query80.preql`

  ```text
  Syntax error in query80.preql: Undefined concept: cs.return_net_loss. Suggestions: ['ss.return_net_loss', 'cs.return_amount', 'ss.return_store.gmt_offset']
  ```
- `trilogy run query80.preql`

  ```text
  Syntax error in query80.preql: ORDER BY contains aggregate `grouping(local.channel_label)` (line 4), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(local.channel_label) as g order by g desc`.
  ```
- `trilogy file read query83.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.store_returns:sr --import raw.customer:c select c.text_id, c.first_name, c.last_name, sr.ticket_number, sr.item.id limit 5;`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {c.first_name, c.last_name, c.text_id}; {sr.item.id, sr.ticket_number}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read query84.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query86.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query89.preql`

  ```text
  Resolution error in query89.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {item.brand_name, item.category, item.class}; {local.avg_annual_for_group, local.total_unit_price, ss.date.month_of_year, ss.date.year, ss.store.company_name, ss.store.name}.
    - `item.brand_name` is disconnected — did you mean `ss.item.brand_name`? (connected to the other concepts)
    - `item.category` is disconnected — did you mean `ss.item.category`? (connected to the other concepts)
    - `item.class` is disconnected — did you mean `ss.item.class`? (connected to the other concepts)
  These look like separately-imported copies of models already reachable through a connected import; chain through that path (e.g. `ss.item.brand_name`) instead of importing a second, disconnected copy.
  ```
- `trilogy agent-info syntax example aggregate-in-where`

  ```text
  Unknown syntax example: 'aggregate-in-where'

  Available Trilogy syntax examples — print one with `trilogy agent-info syntax example <name>`:

  - `query-structure` — the clause order of a query (`where` -> `select` <cols> -> join(s) -> `having` -> `order by` -> `limit`) and how to define a rowset (a NAME then a select); joins sit right after the select list — filter a joined or aggregated RESULT in `having`, input rows in `where`
  - `filtered-aggregate` — `sum(x ? cond)` / `count(x ? cond)` aggregate just the matching rows; to COUNT ROWS count the unique grain/row key, not a non-unique sub-key;
  …
  total/total rows and to sort by level
  - `rank-over-rollup` — rank rollup subtotals/leaves with a SINGLE `rank(a,b) over (partition by level, parent ...)` — not separate ranks per level
  - `staged-membership` — compute a membership set in a `rowset` (keys meeting a count/HAVING), then filter the main query with `<key> in <rowset>.<col>`
  - `correlated-exists-via-grouped-counts` — translate `EXISTS other` / `NOT EXISTS other matching` over the same model into two `count(...) by <grain>` compared in `where` (`> 1` = another exists, `= 1` = no other matches) — don't filter on a boolean-of-aggregate
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy file write query02.preql -e -c import raw.web_sales as web;
import raw.catalog_sales as cat;

# Daily sales aggregated for both channels
with combine…eq asc) as future_sales
;

select
    future_lagged.week_seq,
    round(future_lagged.daily_sales / future_lagged.future_sales, 2) as ratio_sunday
;
`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` — a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...f_week, day_name, daily_sales) ???  # Filter to week sequences t...

  Write stats: received 934 chars / 934 bytes; tail: …'ly_sales / future_lagged.future_sales, 2) as ratio_sunday\\n;\\n'.
  ```
- `trilogy file write query02.preql -e -c import raw.all_sales as s;

# Daily sales from web and catalog channels, aggregated by week_seq and day_of_week
with d…
    @thu_ratio(lagged.wk) as thursday,
    @fri_ratio(lagged.wk) as friday,
    @sat_ratio(lagged.wk) as saturday,
order by lagged.wk nulls first
;
`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword — a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
   in ('WEB', 'CATALOG') select ??? distinct s.date.week_seq as w2...

  Write stats: received 2276 chars / 2276 bytes; tail: …'io(lagged.wk) as saturday,\\norder by lagged.wk nulls first\\n;\\n'.
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.channel, all_sales.is_returned, count(all_sales.order_id) as cnt where all_sales.channel='CATALOG' group by all_sales.channel, all_sales.is_returned;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...e all_sales.channel='CATALOG' ??? group by all_sales.channel, al...
  ```
- `trilogy file write query06.preql -e -c import raw.store_sales as store_sales;

# Average of each distinct item's current price per category (each item counte…tate as state,
    count(store_sales.line_item) as line_item_count
order by
    line_item_count asc nulls first,
    state asc nulls first
limit 100;`

  ```text
  refused to write 'query06.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:46
    |
  4 | auto item_price_by_cat <- item.current_price by item.id, item.category;
    |                                              ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ..._by_cat <- item.current_price ??? by item.id, item.category; aut...

  Write stats: received 819 chars / 819 bytes; tail: …'_count asc nulls first,\\n    state asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query11.preql --content import raw.store_sales as store;
import raw.web_sales as web;

# Store revenue by customer and year
rowset store_r…ode nulls first,
    cust_info.first_name nulls first,
    cust_info.last_name nulls first,
    cust_info.preferred_cust_flag nulls first
limit 100;
`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
   --> 5:1
    |
  5 | rowset store_rev as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
   revenue by customer and year ??? rowset store_rev as select

  Write stats: received 2022 chars / 2022 bytes; tail: …'t,\\n    cust_info.preferred_cust_flag nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as s;

# Overall average sale value (quantity * list_price) across all channels, 1999-2001
au…id) desc,
    s.channel nulls first,
    s.item.brand_id nulls first,
    s.item.class_id nulls first,
    s.item.category_id nulls first
limit 100;
`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
    --> 22:20
     |
  22 |   and month_of_year(s.date.date) = 11
     |                    ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
  ...te) = 2001   and month_of_year ??? (s.date.date) = 11   and s.ite...

  Write stats: received 1485 chars / 1485 bytes; tail: …' nulls first,\\n    s.item.category_id nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query17.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;

# Store sales in 20…ltered.item_code asc nulls first,
         ss_filtered.item_description asc nulls first,
         ss_filtered.store_state asc nulls first
limit 100;
`

  ```text
  refused to write 'query17.preql': not syntactically valid Trilogy.

  Parse error:
   --> 6:1
    |
  6 | rowset ss_filtered as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...as cs;  # Store sales in 2001 ??? rowset ss_filtered as where ss...

  Write stats: received 2351 chars / 2351 bytes; tail: …'         ss_filtered.store_state asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…) as total_sales
order by
    combined.last_name asc nulls first,
    combined.first_name asc nulls first,
    total_sales asc nulls first
limit 100;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
    --> 12:70
     |
  12 | auto frequent_prefix <- item_desc_prefix ? (count(store_sales.item.id, store_sales.date.date) by item_desc_prefix, store_sales.date.date > 4);
     |                                                                      ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...x ? (count(store_sales.item.id ??? , store_sales.date.date) by it...

  Write stats: received 2403 chars / 2403 bytes; tail: …' asc nulls first,\\n    total_sales asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…) as total_sales
order by
    combined.last_name asc nulls first,
    combined.first_name asc nulls first,
    total_sales asc nulls first
limit 100;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
    --> 20:32
     |
  20 |     ? (count(item_date_pairs.id, item_date_pairs.sold_date) by item_date_pairs.desc_prefix) > 4;
     |                                ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
     ? (count(item_date_pairs.id ??? , item_date_pairs.sold_date) b...

  Write stats: received 2570 chars / 2570 bytes; tail: …' asc nulls first,\\n    total_sales asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…) as total_sales
order by
    combined.last_name asc nulls first,
    combined.first_name asc nulls first,
    total_sales asc nulls first
limit 100;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
    --> 30:5
     |
  30 |     where store_sales.date.year in (2000, 2001, 2002, 2003)
     |     ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, window_sql_over, or OVER_COMPONENT_REF
  Location:
  ...y store_sales.customer.id     ??? where store_sales.date.year in...

  Write stats: received 2678 chars / 2678 bytes; tail: …' asc nulls first,\\n    total_sales asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query24.preql --content import raw.store_sales as store_sales;

# Row-level filters: only sales with returns, store market=8, ZIP match, c…ing
    stage2_sum > 0.05 * avg_stage1
order by
    store_sales.customer.last_name,
    store_sales.customer.first_name,
    store_sales.store.name;
`

  ```text
  refused to write 'query24.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block — they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...: sum net_paid per full grain ??? auto stage1_sum <- sum(store_s...

  Write stats: received 1300 chars / 1300 bytes; tail: …'tore_sales.customer.first_name,\\n    store_sales.store.name;\\n'.
  ```
- `trilogy file write query27.preql --content import raw.store_sales as ss;

# Store sales in 2002, TN stores, point-of-sale demographics: single, male, College…ender = 'M'
  and ss.customer_demographic.education_status = 'College'
order by item_code asc nulls first,
         state asc nulls first
limit 100;
`

  ```text
  refused to write 'query27.preql': not syntactically valid Trilogy.

  Parse error:
    --> 16:1
     |
  16 | where ss.date.year = 2002
     | ^---
     |
     = expected limit or order_by
  Location:
  ...item.text_id, ss.store.state) ??? where ss.date.year = 2002   an...

  Write stats: received 945 chars / 945 bytes; tail: …' asc nulls first,\\n         state asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query41.preql --content import raw.item as item;

# Items matching the 8 detailed attribute profiles
auto profile_matches <- item.id ? (
 …_id between 1 and 500
  and item.manufact in manuf_with_profile_match
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
  ...nuf_with_profile_match select ??? distinct     item.product_name...

  Write stats: received 1534 chars / 1534 bytes; tail: …' item.product_name\\norder by\\n    item.product_name\\nlimit 100;'.
  ```
- `trilogy file write query43.preql --content import raw.store_sales as ss;

# Filtered aggregates for each day of week - summing unit price (sales_price)
def s… @thursday_sales as thursday,
    @friday_sales as friday,
    @saturday_sales as saturday
order by
    store_name asc,
    store_code asc
limit 100;`

  ```text
  refused to write 'query43.preql': not syntactically valid Trilogy.

  Parse error:
    --> 17:5
     |
  17 |     @sunday_sales as sunday,
     |     ^---
     |
     = expected select_item, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...re.text_id as store_code,     ??? @sunday_sales as sunday,     @...

  Write stats: received 979 chars / 979 bytes; tail: …'y\\norder by\\n    store_name asc,\\n    store_code asc\\nlimit 100;'.
  ```
- `trilogy run --import raw.store_sales:ss select ss.store.id, count(ss.ticket_number) as cnt group by ss.store.id order by cnt desc limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...ount(ss.ticket_number) as cnt ??? group by ss.store.id order by
  ```
- `trilogy file write query63.preql --content import raw.store_sales as ss;

# Per-item manager id for responsible merchandise manager
# Filter: store sales in …lg #1')
    )
  )
select
    ss.item.manager_id,
    --sum(ss.sales_price) as monthly_total
    ss.date.month_of_year,
having
    monthly_total > 0;
`

  ```text
  refused to write 'query63.preql': not syntactically valid Trilogy.

  Parse error:
    --> 22:5
     |
  22 |     ss.date.month_of_year,
     |     ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...s_price) as monthly_total     ??? ss.date.month_of_year, having

  Write stats: received 932 chars / 932 bytes; tail: …'al\\n    ss.date.month_of_year,\\nhaving\\n    monthly_total > 0;\\n'.
  ```
- `trilogy file write query64.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

# Per-item aggreg…
    sum(ss.ext_wholesale_cost) as total_wholesale_cost,
    sum(ss.ext_list_price) as total_list_price,
    sum(ss.coupon_amt) as total_coupon_amt
;`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
    --> 11:3
     |
  11 |   inner join cs.item.id = cr.item.id
     |   ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, window_sql_over, or OVER_COMPONENT_REF
  Location:
  ...store_credit) by cs.item.id   ??? inner join cs.item.id = cr.ite...

  Write stats: received 2274 chars / 2274 bytes; tail: …'tal_list_price,\\n    sum(ss.coupon_amt) as total_coupon_amt\\n;'.
  ```
- `trilogy file write query64.preql --escapes -c import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.stor…t_list_price) as total_list_price,
    sum(ss.coupon_amt) as total_coupon_amt
inner join catalog_item_agg on catalog_item_agg.item_id = ss.item.id
;
`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
    --> 51:29
     |
  51 | inner join catalog_item_agg on catalog_item_agg.item_id = ss.item.id
     |                             ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...t inner join catalog_item_agg ??? on catalog_item_agg.item_id =

  Write stats: received 2078 chars / 2078 bytes; tail: …'catalog_item_agg on catalog_item_agg.item_id = ss.item.id\\n;\\n'.
  ```
- `trilogy file write query65.preql --content import raw.store_sales as ss;

# For store sales in year 1998, compute per-(store, item) revenue = sum of the unit…ev
order by
    store_name asc nulls first,
    item_desc asc nulls first,
    ss.store.id asc nulls first,
    ss.item.id asc nulls first
limit 100;`

  ```text
  refused to write 'query65.preql': not syntactically valid Trilogy.

  Parse error:
    --> 12:5
     |
  12 |     -- Compute store-level average of per-item revenues, grouped by store
     |     ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ....brand_name as item_brand     ??? -- Compute store-level average...

  Write stats: received 735 chars / 735 bytes; tail: …'d asc nulls first,\\n    ss.item.id asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query67.preql --content import raw.store_sales as store_sales;

# The summed sales measure: coalesce to treat null as 0
auto summed_sales …    date.month_of_year asc nulls first,
    store.text_id asc nulls first,
    summed_sales asc nulls first,
    cat_rank asc nulls first
limit 100;
`

  ```text
  refused to write 'query67.preql': not syntactically valid Trilogy.

  Parse error:
    --> 33:1
     |
  33 | having
     | ^---
     |
     = expected limit or order_by
  Location:
  ...month_of_year, store.text_id) ??? having     cat_rank <= 100 ord...

  Write stats: received 1604 chars / 1604 bytes; tail: …'es asc nulls first,\\n    cat_rank asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write test_rank5.preql --content import raw.store_sales as ss;

where ss.date.year = 2000
  and ss.store.state in (select ss.store.state where s…000)
select
    ss.store.state,
    sum(ss.net_profit) as total_np,
    rank(ss.store.state) over (order by sum(ss.net_profit) desc) as rnk
limit 10;`

  ```text
  refused to write 'test_rank5.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy does not support subqueries — joins are auto-resolved from dotted paths. To filter on a value that lives on a related dimension, reference its dot-path directly. Example: instead of `where ss.store_id in (select store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
   2000   and ss.store.state in ??? (select ss.store.state where s...

  Write stats: received 275 chars / 275 bytes; tail: …'te) over (order by sum(ss.net_profit) desc) as rnk\\nlimit 10;'.
  ```
- `trilogy file write test_rank12.preql --content import raw.store_sales as ss;

where ss.date.year = 2000
  and ss.store.state = 'TN'
select
    ss.store.state…) as rn
by rollup (ss.store.state, ss.store.county)
where level = 1 or level = 2
order by level desc, ss.store.state asc nulls last, rn asc
limit 10;`

  ```text
  refused to write 'test_rank12.preql': not syntactically valid Trilogy.

  Parse error:
    --> 12:1
     |
  12 | where level = 1 or level = 2
     | ^---
     |
     = expected limit or order_by
  Location:
  ...store.state, ss.store.county) ??? where level = 1 or level = 2 o...

  Write stats: received 452 chars / 452 bytes; tail: …' level desc, ss.store.state asc nulls last, rn asc\\nlimit 10;'.
  ```
- `trilogy file write query94.preql --content import raw.web_sales as web_sales;

# Order-level: number of distinct warehouses per order
auto warehouse_count <-…rs.total_ship_cost) as total_extended_shipping_cost,
    sum(qualifying_orders.total_net_profit) as total_net_profit
order by order_count
limit 100;
`

  ```text
  refused to write 'query94.preql': not syntactically valid Trilogy.

  Parse error:
    --> 16:1
     |
  16 | rowset qualifying_orders as
     | ^---
     |
     = expected EOI, block, or show_statement
  Location:
  ...qualifying orders in a rowset ??? rowset qualifying_orders as wh...

  Write stats: received 1184 chars / 1184 bytes; tail: …'profit) as total_net_profit\\norder by order_count\\nlimit 100;\\n'.
  ```
- `trilogy file write query94.preql --content import raw.web_sales as web_sales;

# Rowset with ALL order-level stats (no filters, across all lines)
with order_…ines.order_id
# group by order to aggregate filtered lines
select
    order_id,
    sum(ship_cost) as ship_cost_sum,
    sum(profit) as profit_sum
;
`

  ```text
  refused to write 'query94.preql': not syntactically valid Trilogy.

  Parse error:
    --> 33:1
     |
  33 | select
     | ^---
     |
     = expected limit, order_by, where, having, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, select_grouping, or JOIN_TYPE
  Location:
  ...r to aggregate filtered lines ??? select     order_id,     sum(s...

  Write stats: received 1332 chars / 1332 bytes; tail: …'hip_cost) as ship_cost_sum,\\n    sum(profit) as profit_sum\\n;\\n'.
  ```
- `trilogy file write query97.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;

# Store sales: unique (customer, item) where sold d…elect s.cust_id, s.item_id from store_pairs s
        full join catalog_pairs c on s.cust_id = c.cust_id and s.item_id = c.item_id
    )
)
limit 100;`

  ```text
  refused to write 'query97.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...then 1 else null end) as both ??? from (     select         coal...

  Write stats: received 1324 chars / 1324 bytes; tail: …'_id = c.cust_id and s.item_id = c.item_id\\n    )\\n)\\nlimit 100;'.
  ```
- `trilogy file write query97.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;

# Store sales: unique (customer, item) where sold d…store_pairs s on s.cust_id = c.cust_id = s.item_id = c.item_id
inner join catalog_pairs c on s.cust_id = c.cust_id = s.item_id = c.item_id
limit 100;`

  ```text
  refused to write 'query97.preql': not syntactically valid Trilogy.

  Parse error:
    --> 27:24
     |
  27 | inner join store_pairs s on s.cust_id = c.cust_id = s.item_id = c.item_id
     |                        ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...s both inner join store_pairs ??? s on s.cust_id = c.cust_id = s...

  Write stats: received 1073 chars / 1073 bytes; tail: …' on s.cust_id = c.cust_id = s.item_id = c.item_id\\nlimit 100;'.
  ```
- `trilogy file write query97.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;

# Store sales: unique (customer, item) where sold d…_id = catalog_pairs.cust_id = s.item_id = catalog_pairs.item_id
full join catalog_pairs c on s.cust_id = c.cust_id = s.item_id = c.item_id
limit 100;`

  ```text
  refused to write 'query97.preql': not syntactically valid Trilogy.

  Parse error:
    --> 27:23
     |
  27 | full join store_pairs s on s.cust_id = catalog_pairs.cust_id = s.item_id = catalog_pairs.item_id
     |                       ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...as both full join store_pairs ??? s on s.cust_id = catalog_pairs...

  Write stats: received 1100 chars / 1100 bytes; tail: …' on s.cust_id = c.cust_id = s.item_id = c.item_id\\nlimit 100;'.
  ```
- `trilogy file write query97.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;

# Store sales: unique (customer, item) where sold d…n 1 else 0 end) as both
full join store_pairs on store_pairs.cust_id = catalog_pairs.cust_id = store_pairs.item_id = catalog_pairs.item_id
limit 100;`

  ```text
  refused to write 'query97.preql': not syntactically valid Trilogy.

  Parse error:
    --> 27:23
     |
  27 | full join store_pairs on store_pairs.cust_id = catalog_pairs.cust_id = store_pairs.item_id = catalog_pairs.item_id
     |                       ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...as both full join store_pairs ??? on store_pairs.cust_id = catal...

  Write stats: received 1109 chars / 1109 bytes; tail: …'_id = store_pairs.item_id = catalog_pairs.item_id\\nlimit 100;'.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw.store_sales:ss --import raw.catalog_sales:cs --import raw.web_sales:ws select ss.customer.text_id, ss.date.year, sum(ss.ext_list_pri… ss.ext_wholesale_cost - ss.ext_discount_amount + ss.ext_sales_price) where ss.customer.text_id = 'AAAAAAAADIIOAAAA' and ss.date.year in (2001,2002);`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `sum(ss.ext_list_price - ss.ext_wholesale_cost - ss.ext_discount_amount + ss.ext_sales_price) as ext_sales_price_sum`
  Location:
  ..._amount + ss.ext_sales_price) ??? where ss.customer.text_id = 'A...
  ```
- `trilogy run --import raw/store_sales:store_sales select count(store_sales.ticket_number) limit 5;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(store_sales.ticket_number) as ticket_number_count`
  Location:
  ...nt(store_sales.ticket_number) ??? limit 5;
  ```

### `type-error`

- `trilogy run query77.preql`

  ```text
  Syntax error in query77.preql: Cannot use BETWEEN with incompatible types DATE and STRING (low)
  ```
