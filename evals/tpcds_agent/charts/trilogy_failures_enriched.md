# Trilogy failure analysis — 20260707-033936

- Run `20260707-033936` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 652 | failed: 69 (11%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 42 | 61% |
| `syntax-parse` | 20 | 29% |
| `syntax-missing-alias` | 3 | 4% |
| `planner-recursion` | 2 | 3% |
| `cli-misuse` | 1 | 1% |
| `undefined-concept` | 1 | 1% |

## Detail

### `other`

- `trilogy run query02.preql`

  ```text
  Syntax error in query02.preql: Undefined concept: local.nxt_ws. Suggestions: ['nxt_sales']
  ```
- `trilogy file read query02.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.all_sales:s select s.channel, count(s.row_counter) as cnt where s.date.date between '2000-08-23'::date and '2000-09-06'::date and s.channel_dim_id is not null;`

  ```text
  Syntax error in stdin: Undefined concept: s.row_counter (line 2, in SELECT).
  ```
- `trilogy run query11.preql`

  ```text
  Syntax error in query11.preql: 3 undefined concept references; fix all before re-running:
    - first_name (line 59, col 5, in ORDER BY); did you mean: all.billing_customer.first_name, all.ship_customer.first_name, all.purchasing_customer.first_name, store_2001.all.billing_customer.first_name?
    - last_name (line 60, col 5, in ORDER BY); did you mean: all.billing_customer.last_name, all.ship_customer.last_name, all.purchasing_customer.last_name, store_2001.all.billing_customer.last_name?
    - preferred_cust_flag (line 61, col 5, in ORDER BY); did you mean: all.ship_customer.preferred_cust_flag, all.billing_customer.preferred_cust_flag, all.purchasing_customer.preferred_cust_flag, store_2002.all.billing_customer.preferred_cust_flag?
  ```
- `trilogy run query11.preql`

  ```text
  Syntax error in query11.preql: 13 undefined concept references; fix all before re-running:
    - store_2001.all.billing_customer.revenue (line 40, col 5, in WHERE); did you mean: store_2001.all.billing_customer.id, store_2001.all.billing_customer.first_name, store_2001.all.billing_customer.last_name, store_2002.all.billing_customer.revenue, web_2001.all.billing_customer.revenue, web_2002.all.billing_customer.revenue?
    - store_2002.all.billing_customer.revenue (line 41, col 9, in WHERE); did you mean: store_2002.all.billing_customer.id, store_2002.all.billing_customer.preferred_cust_flag, store_2002.revenue, store_2001.all.billing_customer.revenue, web_2002.all.billing_customer.revenue, web_2001.all.billing_customer.revenue?
    - web_2001.all.billing_customer.revenue (line 42, col 9, in WHERE); did you mean: web_2001.all.billing_customer.id, web_2001.revenue, web_2002.all.billing_customer.revenue, store_2001.all.billing_customer.revenue, store_2002.all.billing_customer.revenue, store_2001.revenue?
    - web_2002.all.billing_customer.revenue (line 43, col 9, in WHERE); did you mean: web_2002.all.billing_customer.id, web_2002.revenue, web_2001.all.billing_customer.revenue, store_2002.all.billing_customer.revenue, store_2001.all.billing_customer.revenue, store_2001.revenue?
    - store_2001.all.billing_customer.revenue (line 45, col 9, in WHERE); did you mean: store_2001.all.billing_customer.id, store_2001.all.billing_customer.first_name, store_2001.all.billing_customer.last_name, store_2002.all.billing_customer.revenue, web_2001.all.billing_customer.revenue, web_2002.all.billing_customer.revenue?
    - web_2001.all.billing_customer.revenue (line 46, col 9, in WHERE); did you mean: web_2001.all.billing_customer.id, web_2001.revenue, web_2002.all.billing_customer.revenue, store_2001.all.billing_customer.revenue, store_2002.all.billing_customer.revenue, store_2001.revenue?
    - web_2002.all.billing_customer.revenue (line 38, in WHERE); did you mean: web_2002.all.billing_customer.id, web_2002.revenue, web_2001.all.billing_customer.revenue, store_2002.all.billing_customer.revenue, store_2001.all.billing_customer.revenue, store_2001.revenue?
    - web_2001.all.billing_customer.revenue (line 38, in WHERE); did you mean: web_2001.all.billing_customer.id, web_2001.revenue, web_2002.all.billing_customer.revenue, store_2001.all.billing_customer.revenue, store_2002.all.billing_customer.revenue, store_2001.revenue?
    - store_2002.all.billing_customer.revenue (line 38, in WHERE); did you mean: store_2002.all.billing_customer.id, store_2002.all.billing_customer.preferred_cust_flag, store_2002.revenue, store_2001.all.billing_customer.revenue, web_2002.all.billing_customer.revenue, web_2001.all.billing_customer.revenue?
    - store_2001.all.billing_customer.revenue (line 38, in WHERE); did you mean: store_2001.all.billing_customer.id, store_2001.all.billing_customer.first_name, store_2001.all.billing_customer.last_name, store_2002.all.billing_customer.revenue, web_2001.all.billing_customer.revenue, web_2002.all.billing_customer.revenue?
    - first_name (line 59, col 5, in ORDER BY); did you mean: all.billing_customer.first_name, all.ship_customer.first_name, all.purchasing_customer.first_name, store_2001.all.billing_customer.first_name?
    - last_name (line 60, col 5, in ORDER BY); did you mean: all.billing_customer.last_name, all.ship_customer.last_name, all.purchasing_customer.last_name, store_2001.all.billing_customer.last_name?
    - preferred_cust_flag (line 61, col 5, in ORDER BY); did you mean: all.ship_customer.preferred_cust_flag, all.billing_customer.preferred_cust_flag, all.purchasing_customer.preferred_cust_flag, store_2002.all.billing_customer.preferred_cust_flag?
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.store_sales:store --import raw.catalog_sales:catalog where store.date.year >= 2000 and store.date.year <= 2003 and store.date.date i…date.month_of_year = 2 select store.item.text_id, count(store.line_item) as cnt, sum(count(store.line_item)) over () as total having cnt > 4 limit 5;`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {catalog.sold_date.month_of_year, catalog.sold_date.year}; {cnt, total, store.date.date, store.date.year, store.item.text_id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run --import raw.store_sales:store --import raw.catalog_sales:catalog where store.date.year >= 2000 and store.date.year <= 2003 and store.customer.id…re.quantity * store.sales_price) as store_total having store_total > 118133.26 and store.customer.id in (select catalog.billing_customer.id) limit 5;`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {catalog.sold_date.month_of_year, catalog.sold_date.year}; {store_total, store.customer.id, store.date.year}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query23.preql`

  ```text
  Resolution error in query23.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 33). The requested concepts split into 2 disconnected subgraphs: {catalog.billing_customer.id, catalog.item.text_id, catalog.sold_date.month_of_year, catalog.sold_date.year, ___tvf_arm_0_first_name, ___tvf_arm_0_last_name, ___tvf_arm_0_total_sales}; {customer_store_totals.store_total}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read query23.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 1 column 2016 (char 2015). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query54.preql`

  ```text
  Syntax error in query54.preql: Undefined concept: date.year. Suggestions: ['cs.date.year', 'ws.date.year', 'ss.date.year', 'ss.store.date.year', 'ss.return_store.date.year', 'cs.ship_date.year']
  ```
- `trilogy run query54.preql`

  ```text
  Unexpected error in query54.preql: (_duckdb.BinderException) Binder Error: Table "ss_store_store" does not have a column named "S_CLOSED_DATE"

  Candidate bindings: : "s_closed_date_sk"

  LINE 74:     INNER JOIN "date_dim" as "ss_store_date_date" on "ss_store_store"."S_CLOSED_DATE" = "ss_store_date_date"...
                                                                ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "cs_catalog_sales"."CS_BILL_CUSTOMER_SK" as "___tvf_arm_0_cust_id"
  FROM
      "catalog_sales" as "cs_catalog_sales"
      INNER JOIN "item" as "cs_item_items" on "cs_catalog_sales"."CS_ITEM_SK" = "cs_item_items"."I_ITEM_SK"
      INNER JOIN "date_dim" as "cs_sold_date_date" on "cs_catalog_sales"."CS_SOLD_DATE_SK" = "cs_sold_date_date"."D_DATE_SK"
  WHERE
      "cs_sold_date_date"."D_YEAR" = 1998 and "cs_sold_date_date"."D_MOY" = 12 and "cs_item_items"."I_CATEGORY" = 'Women' and "cs_item_items"."I_CLASS" = 'maternity' and "cs_catalog_sales"."CS_BILL_CUSTOMER_SK" is not null

  GROUP BY
      1),
  uneven as (
  SELECT
      "ws_web_sales"."WS_BILL_CUSTOMER_SK" as "___tvf_arm_1_cust_id"
  FROM
      "web_sales" as "ws_web_sales"
      INNER JOIN "date_dim" as "ws_date_date" on "ws_web_sales"."WS_SOLD_DATE_SK" = "ws_date_date"."D_DATE_SK"
      INNER JOIN "item" as "ws_item_items" on "ws_web_sales"."WS_ITEM_SK" = "ws_item_items"."I_ITEM_SK"
  WHERE
      "ws_date_date"."D_YEAR" = 1998 and "ws_date_date"."D_MOY" = 12 and "ws_item_items"."I_CATEGORY" = 'Women' and "ws_item_items"."I_CLASS" = 'maternity' and "ws_web_sales"."WS_BILL_CUSTOMER_SK" is not null

  GROUP BY
      1),
  young as (
  SELECT
      min(CASE WHEN "cs_date_date"."D_YEAR" = 1998 and "cs_date_date"."D_MOY" = 12 THEN "cs_date_date"."D_MONTH_SEQ" ELSE NULL END) as "dec1998_month_seq"
  FROM
      "date_dim" as "cs_date_date"),
  juicy as (
  SELECT
      "cheerful"."___tvf_arm_0_cust_id" as "_all_qualifying_cust_id"
  FROM
      "cheerful"
  UNION ALL
  SELECT
      "uneven"."___tvf_arm_1_cust_id" as "_all_qualifying_cust_id"
  FROM
      "uneven"),
  vacuous as (
  SELECT
      "juicy"."_all_qualifying_cust_id" as "_all_qualifying_cust_id"
  FROM
      "juicy"),
  concerned as (
  SELECT
      "vacuous"."_all_qualifying_cust_id" as "all_qualifying_cust_id"
  FROM
      "vacuous"),
  charming as (
  SELECT
      "ss_store_sales"."SS_CUSTOMER_SK" as "ss_customer_id",
      "ss_store_sales"."SS_STORE_SK" as "ss_store_id"
  FROM
      "store_sales" as "ss_store_sales"
  WHERE
      "ss_store_sales"."SS_CUSTOMER_SK" in (select concerned."all_qualifying_cust_id" from concerned where concerned."all_qualifying_cust_id" is not null) and "ss_store_sales"."SS_CUSTOMER_SK" is not null

  GROUP BY
      1,
      2),
  kaput as (
  SELECT
      "ss_store_date_date"."D_MONTH_SEQ" as "ss_store_date_month_seq",
      "ss_store_sales"."SS_EXT_SALES_PRICE" as "ss_ext_sales_price",
      "ss_store_sales"."SS_ITEM_SK" as "ss_item_id",
      "ss_store_sales"."SS_TICKET_NUMBER" as "ss_ticket_number"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "store" as "ss_store_store" on "ss_store_sales"."SS_STORE_SK" = "ss_store_store"."S_STORE_SK"
      INNER JOIN "customer" as "ss_customer_customers" on "ss_store_sales"."SS_CUSTOMER_SK" = "ss_customer_customers"."C_CUSTOMER_SK"
      INNER JOIN "date_dim" as "ss_store_date_date" on "ss_store_store"."S_CLOSED_DATE" = "ss_store_date_date"."D_DATE_SK"
      INNER JOIN "customer_address" as "ss_customer_address_customer_address" on "ss_customer_customers"."C_CURRENT_ADDR_SK" = "ss_customer_address_customer_address"."CA_ADDRESS_SK"
  WHERE
      "ss_store_sales"."SS_CUSTOMER_SK" in (select concerned."all_qualifying_cust_id" from concerned where concerned."all_qualifying_cust_id" is not null) and "ss_store_store"."S_COUNTY" = "ss_customer_address_customer_address"."CA_COUNTY" and "ss_store_store"."S_STATE" = "ss_customer_address_customer_address"."CA_STATE" and "ss_store_sales"."SS_CUSTOMER_SK" is not null
  ),
  protective as (
  SELECT
      "charming"."ss_customer_id" as "ss_customer_id",
      "ss_store_date_date"."D_MONTH_SEQ" as "ss_store_date_month_seq"
  FROM
      "charming"
      INNER JOIN "store" as "ss_store_store" on "charming"."ss_store_id" = "ss_store_store"."S_STORE_SK"
      INNER JOIN "customer" as "ss_customer_customers" on "charming"."ss_customer_id" = "ss_customer_customers"."C_CUSTOMER_SK"
      INNER JOIN "date_dim" as "ss_store_date_date" on "ss_store_store"."S_CLOSED_DATE" = "ss_store_date_date"."D_DATE_SK"
      INNER JOIN "customer_address" as "ss_customer_address_customer_address" on "ss_customer_customers"."C_CURRENT_ADDR_SK" = "ss_customer_address_customer_address"."CA_ADDRESS_SK"
  WHERE
      "ss_store_store"."S_COUNTY" = "ss_customer_address_customer_address"."CA_COUNTY" and "ss_store_store"."S_STATE" = "ss_customer_address_customer_address"."CA_STATE"
  ),
  divergent as (
  SELECT
      "kaput"."ss_ext_sales_price" as "ss_ext_sales_price"
  FROM
      "young"
      INNER JOIN "kaput" on 1=1
  WHERE
      "kaput"."ss_store_date_month_seq" BETWEEN "young"."dec1998_month_seq" + 1 AND "young"."dec1998_month_seq" + 3

  GROUP BY
      1,
      "kaput"."ss_item_id",
      "kaput"."ss_ticket_number"),
  premium as (
  SELECT
      "protective"."ss_customer_id" as "ss_customer_id"
  FROM
      "young"
      INNER JOIN "protective" on 1=1
  WHERE
      "protective"."ss_store_date_month_seq" BETWEEN "young"."dec1998_month_seq" + 1 AND "young"."dec1998_month_seq" + 3

  GROUP BY
      1),
  busy as (
  SELECT
      round(sum("divergent"."ss_ext_sales_price") / 50,0) * 50 as "segment_times_50",
      round(sum("divergent"."ss_ext_sales_price") / 50,0) as "segment"
  FROM
      "divergent"),
  puzzled as (
  SELECT
      count("premium"."ss_customer_id") as "customer_count"
  FROM
      "premium")
  SELECT
      "busy"."segment" as "segment",
      coalesce("puzzled"."customer_count",0) as "customer_count",
      "busy"."segment_times_50" as "segment_times_50"
  FROM
      "busy"
      FULL JOIN "puzzled" on 1=1
  ORDER BY
      "busy"."segment" asc nulls first,
      coalesce("puzzled"."customer_count",0) asc nulls first,
      "busy"."segment_times_50" asc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Nothing was executed: parsed 11 definition statement(s) (8 rowsets, 3 imports) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy run query75.preql`

  ```text
  Resolution error in query75.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {yr2001.all_sales.item.brand_id, yr2001.all_sales.item.category_id, yr2001.all_sales.item.class_id, yr2001.all_sales.item.manufacturer_id, yr2001.amt_01, yr2001.qty_01, yr2002.all_sales.item.brand_id, yr2002.all_sales.item.category_id, yr2002.all_sales.item.class_id, yr2002.all_sales.item.manufacturer_id, yr2002.amt_02, yr2002.qty_02}
  ```
- `trilogy file read query80.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query83.preql duckdb`

  ```text
  Syntax error in query83.preql: Undefined concept: wr.item.text_id. Suggestions: ['wr.web_sales.item.text_id', 'wr.time.text_id', 'wr.store.text_id', 'sr.item.text_id', 'cr.item.text_id', 'sr.store.date.text_id']
  ```
- `trilogy file read query83.preql`

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
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query06.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query16.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query18.preql`

  ```text
  Syntax error in query18.preql: ORDER BY contains aggregate `grouping(local.country)` (line 3), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(local.country) as g order by g desc`.
  ```
- `trilogy run query18.preql`

  ```text
  Unexpected error in query18.preql: (_duckdb.BinderException) Binder Error: GROUPING function is not supported here
  [SQL:
  WITH
  cooperative as (
  SELECT
      "cs_billing_customer_address_customer_address"."CA_COUNTRY" as "country",
      "cs_billing_customer_address_customer_address"."CA_COUNTY" as "county",
      "cs_billing_customer_address_customer_address"."CA_STATE" as "state",
      "cs_billing_customer_customers"."C_BIRTH_YEAR" as "cs_billing_customer_birth_year",
      "cs_billing_customer_demographic_customer_demographics"."CD_DEP_COUNT" as "cs_billing_customer_demographic_dependent_count",
      "cs_catalog_sales"."CS_COUPON_AMT" as "cs_coupon_amt",
      "cs_catalog_sales"."CS_ITEM_SK" as "item_code",
      "cs_catalog_sales"."CS_LIST_PRICE" as "cs_list_price",
      "cs_catalog_sales"."CS_NET_PROFIT" as "cs_net_profit",
      "cs_catalog_sales"."CS_QUANTITY" as "cs_quantity",
      "cs_catalog_sales"."CS_SALES_PRICE" as "cs_sales_price"
  FROM
      "catalog_sales" as "cs_catalog_sales"
      INNER JOIN "date_dim" as "cs_sold_date_date" on "cs_catalog_sales"."CS_SOLD_DATE_SK" = "cs_sold_date_date"."D_DATE_SK"
      INNER JOIN "customer" as "cs_billing_customer_customers" on "cs_catalog_sales"."CS_BILL_CUSTOMER_SK" = "cs_billing_customer_customers"."C_CUSTOMER_SK"
      INNER JOIN "customer_address" as "cs_billing_customer_address_customer_address" on "cs_billing_customer_customers"."C_CURRENT_ADDR_SK" = "cs_billing_customer_address_customer_address"."CA_ADDRESS_SK"
      INNER JOIN "customer_demographics" as "cs_billing_customer_demographic_customer_demographics" on "cs_catalog_sales"."CS_BILL_CDEMO_SK" = "cs_billing_customer_demographic_customer_demographics"."CD_DEMO_SK"
  WHERE
      "cs_sold_date_date"."D_YEAR" = 1998 and "cs_billing_customer_demographic_customer_demographics"."CD_GENDER" = 'F' and "cs_billing_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS" = 'Unknown' and "cs_billing_customer_customers"."C_BIRTH_MONTH" in (1,2,6,8,9,12) and "cs_billing_customer_address_customer_address"."CA_STATE" in ('MS','IN','ND','OK','NM','VA')
  )
  SELECT
      "cooperative"."item_code" as "item_code",
      "cooperative"."country" as "country",
      "cooperative"."state" as "state",
      "cooperative"."county" as "county",
      avg("cooperative"."cs_quantity") as "avg_ticket_quantity",
      avg("cooperative"."cs_list_price") as "avg_per_line_list_price",
      avg("cooperative"."cs_coupon_amt") as "avg_per_line_coupon_amt",
      avg("cooperative"."cs_sales_price") as "avg_per_line_sales_price",
      avg("cooperative"."cs_net_profit") as "avg_per_line_net_profit",
      avg("cooperative"."cs_billing_customer_birth_year") as "avg_customer_birth_year",
      avg("cooperative"."cs_billing_customer_demographic_dependent_count") as "avg_dependent_count"
  FROM
      "cooperative"
  GROUP BY
      ROLLUP (2, 3, 4, 1)
  ORDER BY
      "cooperative"."country" asc nulls first,
      "cooperative"."state" asc nulls first,
      "cooperative"."county" asc nulls first,
      "cooperative"."item_code" asc nulls first,
      MIN(grouping("cooperative"."country")) asc,
      MIN(grouping("cooperative"."state")) asc,
      MIN(grouping("cooperative"."county")) asc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query24.preql`

  ```text
  Unexpected error in query24.preql: Unsupported datatype NUMBER for parameter zero.
  ```
- `trilogy file read query24.preql`

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
- `trilogy database describe catalog_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read query74.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe catalog_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy database describe store_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy database describe web_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/time.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy file write query05.preql --content import raw.all_sales as s;

# Sales side: sales in the date range with non-null entity
with sales_data as
where s.…tal_sales) is not null or sum(combined.total_returns) is not null
order by channel_label asc nulls first, entity_id_label asc nulls first
limit 100;
`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
          0 as return_loss      ??? from sales_data),     (select

  Write stats: received 2199 chars / 2199 bytes; tail: …'asc nulls first, entity_id_label asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw/all_sales:all_sales select distinct all_sales.channel;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...ll_sales as all_sales; select ??? distinct all_sales.channel;
  ```
- `trilogy file write query14.preql --content import raw.all_sales as all_sales;

# (brand, class, category) combos appearing in ALL 3 channels during 1999-2001… nulls first,
    all_sales.item.brand_id nulls first,
    all_sales.item.class_id nulls first,
    all_sales.item.category_id nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
    --> 16:5
     |
  16 |     where all_sales.date.year >= 1999 and all_sales.date.year <= 2001;
     |     ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, aggregate_over, or window_sql_over
  Location:
  ...y * all_sales.list_price)     ??? where all_sales.date.year >= 1...

  Hint: the `by rollup/cube/grouping sets` clause must come *before* HAVING in Trilogy (same order as SQL's `GROUP BY ... HAVING`). Reorder to:
      select <cols> by rollup (<keys>) having <cond> order by <cols> limit <n>;

  Write stats: received 1472 chars / 1472 bytes; tail: …'first,\\n    all_sales.item.category_id nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.store_sales:store select store.date.year, count(store.line_item) as cnt, sum(store.quantity * store.sales_price) as total where store.date.year = 2000 and store.customer.id is not null group by store.date.year order by store.date.year limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...store.customer.id is not null ??? group by store.date.year order...
  ```
- `trilogy run --import raw.store_sales:store where store.date.year >= 2000 and store.date.year <= 2003 select count(store.item.text_id) as cnt, store.item.text…) as d, store.date.date as sd group by store.item.text_id, substring(store.item.desc,1,30), store.date.date having cnt > 4 order by cnt desc limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...) as d, store.date.date as sd ??? group by store.item.text_id, s...
  ```
- `trilogy run --import raw.store_sales:store --import raw.catalog_sales:catalog where catalog.sold_date.year = 2000 and catalog.sold_date.month_of_year = 2 sel…(store.line_item) > 4
)
having total_sales > 0
order by last_name asc nulls first, first_name asc nulls first, total_sales asc nulls first
limit 100;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...d store.date.year <= 2003     ??? group by store.item.text_id, s...
  ```
- `trilogy run --import raw.store_sales:store --import raw.catalog_sales:catalog where catalog.sold_date.year = 2000 and catalog.sold_date.month_of_year = 2
  a…t_price) as total_sales
having total_sales > 0
order by last_name asc nulls first, first_name asc nulls first, total_sales asc nulls first
limit 100;`

  ```text
  Syntax error in stdin:   --> 13:5
     |
  13 |     select store.item.text_id, substring(store.item.desc, 1, 30) as dp, store.date.date as sd
     |     ^---
     |
     = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...d store.date.year <= 2003     ??? select store.item.text_id, sub...
  ```
- `trilogy file write query54.preql --content import raw/catalog_sales as cs;
import raw/web_sales as ws;
import raw/store_sales as ss;

# Find month_seq for De…_price) / 50) * 50 as segment_times_50
order by
    segment asc nulls first,
    customer_count asc nulls first,
    segment_times_50 asc
limit 100;
`

  ```text
  refused to write 'query54.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/catalog_sales as cs;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/catalog_sales as cs; impor...

  Write stats: received 1796 chars / 1796 bytes; tail: …'_count asc nulls first,\\n    segment_times_50 asc\\nlimit 100;\\n'.
  ```
- `trilogy file write query54.preql --content import raw.catalog_sales as cs;
import raw.web_sales as ws;
import raw.store_sales as ss;

# Find month_seq for De…_price) / 50) * 50 as segment_times_50
order by
    segment asc nulls first,
    customer_count asc nulls first,
    segment_times_50 asc
limit 100;
`

  ```text
  refused to write 'query54.preql': not syntactically valid Trilogy.

  Parse error:
    --> 29:31
     |
  29 | with all_qualifying as union((qualifying_customers), (qualifying_customers_web)) -> (cust_id);
     |                               ^---
     |
     = expected select_statement
  Location:
  ...with all_qualifying as union(( ??? qualifying_customers), (qualif...

  Write stats: received 1892 chars / 1892 bytes; tail: …'_count asc nulls first,\\n    segment_times_50 asc\\nlimit 100;\\n'.
  ```
- `trilogy file write query59.preql --content import raw.store_sales as ss;

# This year (2001) weekly sales by day of week - sum of unit price
def week_day_sum…k_seq - 52
order by this_year.store_name asc nulls first,
    this_year.store_code asc nulls first,
    this_year.week_seq asc nulls first
limit 100;`

  ```text
  refused to write 'query59.preql': not syntactically valid Trilogy.

  Parse error:
   --> 6:1
    |
  6 | rowset this_year as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
   ss.date.day_of_week = dow);  ??? rowset this_year as where ss.d...

  Write stats: received 2826 chars / 2826 bytes; tail: …'lls first,\\n    this_year.week_seq asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query84.preql --content import raw.store_returns as sr;
import raw.customer as c;

where
  c.address.city = 'Edgewood'
  and c.household_d…ame,
  sr.ticket_number --,
  sr.item.id --

union join sr.customer_demographic.id = c.demographics.id

order by customer_code nulls first
limit 100;`

  ```text
  refused to write 'query84.preql': not syntactically valid Trilogy.

  Parse error:
    --> 12:21
     |
  12 |   sr.ticket_number --,
     |                     ^---
     |
     = expected access_chain
  Location:
  ...ull_name,   sr.ticket_number - ??? -,   sr.item.id --  union join...

  Write stats: received 459 chars / 459 bytes; tail: …'mographics.id\\n\\norder by customer_code nulls first\\nlimit 100;'.
  ```
- `trilogy file write query84.preql --content import raw.store_returns as sr;
import raw.customer as c;

where
  c.address.city = 'Edgewood'
  and c.household_d…name,
  sr.ticket_number --
  sr.item.id --

union join sr.customer_demographic.id = c.demographics.id

order by customer_code nulls first
limit 100;`

  ```text
  refused to write 'query84.preql': not syntactically valid Trilogy.

  Parse error:
    --> 12:21
     |
  12 |   sr.ticket_number --
     |                     ^---
     |
     = expected access_chain
  Location:
  ...ull_name,   sr.ticket_number - ??? -   sr.item.id --  union join

  Write stats: received 458 chars / 458 bytes; tail: …'mographics.id\\n\\norder by customer_code nulls first\\nlimit 100;'.
  ```
- `trilogy file write query84.preql --content import raw.store_returns as sr;
import raw.customer as c;

where
  c.address.city = 'Edgewood'
  and c.household_d…ame,
  sr.ticket_number --,
  sr.item.id --

union join sr.customer_demographic.id = c.demographics.id

order by customer_code nulls first
limit 100;`

  ```text
  refused to write 'query84.preql': not syntactically valid Trilogy.

  Parse error:
    --> 12:21
     |
  12 |   sr.ticket_number --,
     |                     ^---
     |
     = expected access_chain
  Location:
  ...ull_name,   sr.ticket_number - ??? -,   sr.item.id --  union join...

  Write stats: received 459 chars / 459 bytes; tail: …'mographics.id\\n\\norder by customer_code nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.store_sales:ss select ss.customer.last_name as last, ss.customer.first_name as first, ss.date.date as sale_date, ss.date.year as yr limit 5 where ss.date.year = 2000 and ss.customer.id is not null;`

  ```text
  Syntax error in stdin:  --> 2:118
    |
  2 | select ss.customer.last_name as last, ss.customer.first_name as first, ss.date.date as sale_date, ss.date.year as yr limit 5 where ss.date.year = 2000 and ss.customer.id is not null;
    |                                                                                                                      ^---
    |
    = expected metadata, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...sale_date, ss.date.year as yr ??? limit 5 where ss.date.year = 2...
  ```
- `trilogy file write query06.preql --content import raw.store_sales as ss;

# Distinct item prices per category (one row per item.id)
auto item_price_by_cat <-…m) as line_item_count
having
    line_item_count >= 10
order by line_item_count asc nulls first, ss.customer.address.state asc nulls first
limit 100;`

  ```text
  refused to write 'query06.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [213]: A `by <grain>` clause must follow an aggregate, but the expression before it has none. If the `by` sits inside an aggregate's parentheses (`max(x by *)`), move it outside the call: `max(x) by *`. To take each distinct value once per grain, wrap it in `group(...)` - e.g. `group(item.current_price) by item.id, item.category`. For a reduction, use an aggregate: `sum(x) by ...`, `avg(x) by ...`, `max(x) by ...`.
  Location:
  ..._cat <- ss.item.current_price ??? by ss.item.id, ss.item.categor...

  Write stats: received 698 chars / 698 bytes; tail: …' first, ss.customer.address.state asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query16.preql --content import raw/catalog_sales as cs;

# Orders that ship from multiple warehouses (at least 2 distinct warehouses)
auto…der_number,
    sum(cs.ext_ship_cost) as total_ext_ship_cost,
    sum(cs.net_profit) as total_net_profit
order by
    cs.order_number desc
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
  import ??? raw/catalog_sales as cs;  # Or...

  Write stats: received 772 chars / 772 bytes; tail: …'otal_net_profit\\norder by\\n    cs.order_number desc\\nlimit 100;'.
  ```
- `trilogy file write query41.preql`

  ```text
  refused to write 'query41.preql': not syntactically valid Trilogy.

  Parse error:
    --> 22:1
     |
  22 | ;
     | ^---
     |
     = expected EOI, block, or show_statement
  Location:
  ...oss' and item.size = 'N/A');
   ??? ;

   # Get manufacturer text v...

  Write stats: received 1644 chars / 1644 bytes; tail: …'ile_manufacts\r\\norder by\r\\n    item.product_name\r\\nlimit 100;\r\\n'.
  ```
- `trilogy file write query41.preql`

  ```text
  refused to write 'query41.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...one profile item
   select
       ??? distinct item.product_name
   wh...

  Write stats: received 1439 chars / 1439 bytes; tail: …'ile_manufacts\r\\norder by\r\\n    item.product_name\r\\nlimit 100;\r\\n'.
  ```
- `trilogy file write query70.preql --content import raw.store_sales as ss;

# Top 5 states by store net profit in year 2000, considering only known stores
with…llup (ss.store.state, ss.store.county)
having hierarchy_level >= 0
order by
    hierarchy_level desc,
    ss.store.state asc,
    rnk asc
limit 100
;`

  ```text
  refused to write 'query70.preql': not syntactically valid Trilogy.

  Parse error:
    --> 22:28
     |
  22 |     grouping(ss.store.state, ss.store.county) as hierarchy_level,
     |                            ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...t,     grouping(ss.store.state ??? , ss.store.county) as hierarch...

  Write stats: received 1086 chars / 1086 bytes; tail: …'_level desc,\\n    ss.store.state asc,\\n    rnk asc\\nlimit 100\\n;'.
  ```
- `trilogy file write query76.preql -e -c import raw.store_sales as store;
import raw.web_sales as web;
import raw.catalog_sales as catalog;

with combined as u…ined.missing_ref asc nulls first,
  combined.year asc nulls first,
  combined.quarter asc nulls first,
  combined.category asc nulls first
limit 100;`

  ```text
  refused to write 'query76.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ..._count, total_ext_sales_price) ???  select   combined.channel,

  Write stats: received 1709 chars / 1709 bytes; tail: …' nulls first,\\n  combined.category asc nulls first\\nlimit 100;'.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw.all_sales:s select s.channel, s.channel_dim_text_id, sum(s.ext_sales_price) where s.date.date between '2000-08-23'::date and '2000-09-06'::date and s.channel_dim_id is not null group by s.channel, s.channel_dim_text_id limit 5;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `sum(s.ext_sales_price) as ext_sales_price_sum`
  Location:
  ...xt_id, sum(s.ext_sales_price) ??? where s.date.date between '200...
  ```
- `trilogy run --import raw.store_sales:ss select ss.customer.last_name, ss.customer.first_name, ss.date.date, ss.date.year limit 5 where ss.date.year = 2000 and ss.customer.id is not null;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `ss.date.year as ss_date_year`
  Location:
  ...e, ss.date.date, ss.date.year ??? limit 5 where ss.date.year = 2...
  ```
- `trilogy run --import raw.store_sales:ss select ss.customer.last_name, ss.customer.first_name, ss.date.date, ss.date.year limit 5 where ss.date.year = 2000 and ss.customer.id is not null;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `ss.date.year as ss_date_year`
  Location:
  ...e, ss.date.date, ss.date.year ??? limit 5 where ss.date.year = 2...
  ```

### `planner-recursion`

- `trilogy run query75.preql`

  ```text
  Resolution error in query75.preql: query could not be planned; this is a bug.
  ```
- `trilogy run query75.preql`

  ```text
  Resolution error in query75.preql: query could not be planned; this is a bug.
  ```

### `cli-misuse`

- `trilogy explore raw/`

  ```text
  Invalid value for 'PATH': File 'raw/' is a directory.
  ```

### `undefined-concept`

- `trilogy explore query11.preql`

  ```text
  Failed to parse query11.preql: (UndefinedConceptException(...), '3 undefined concept references; fix all before re-running:\n  - first_name (line 59, col 5, in ORDER BY); did you mean: all.billing_customer.first_name, all.ship_customer.first_name, all.purchasing_customer.first_name, store_2001.all.billing_customer.first_name?\n  - last_name (line 60, col 5, in ORDER BY); did you mean: all.billing_customer.last_name, all.ship_customer.last_name, all.purchasing_customer.last_name, store_2001.all.billing_customer.last_name?\n  - preferred_cust_flag (line 61, col 5, in ORDER BY); did you mean: all.ship_customer.preferred_cust_flag, all.billing_customer.preferred_cust_flag, all.purchasing_customer.preferred_cust_flag, store_2002.all.billing_customer.preferred_cust_flag?')
  ```
