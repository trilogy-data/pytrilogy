# Trilogy failure analysis — 20260811-145004

- Run `20260811-145002_enriched_noise` | `deepseek/deepseek-v4-flash` | sf=1
- `trilogy` calls: 421 | failed: 24 (6%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `disabled-tool` | 14 | 58% |
| `syntax-parse` | 5 | 21% |
| `other` | 2 | 8% |
| `join-resolution` | 2 | 8% |
| `file-not-found` | 1 | 4% |

## Detail

### `disabled-tool`

- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read probe1.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read verify_chunk1.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read verify_total.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read probe10.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read check_counts.preql`

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
- `trilogy file read check_counts.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read myprobe10.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read probeG.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read probe_check.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read myprobe2.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy file write answer_3705756794.preql`

  ```text
  refused to write 'answer_3705756794.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...amt, profit_amt, ret_loss_amt) ???

   select
       combined.chann...
  ```
- `trilogy file write probe6.preql`

  ```text
  refused to write 'probe6.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...ere ss.sale_date.year = 2001
   ??? group by ss.sale_date.year, ss...
  ```
- `trilogy file write probe11.preql`

  ```text
  refused to write 'probe11.preql': not syntactically valid Trilogy.

  Parse error:
    --> 14:1
     |
  14 | subset join cat_avg_rs.category = ss.item.category
     | ^---
     |
     = expected limit, order_by, THEN_LA, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ....sale_date.month_of_year = 1
   ??? subset join cat_avg_rs.categor...
  ```
- `trilogy file write probe11.preql`

  ```text
  refused to write 'probe11.preql': not syntactically valid Trilogy.

  Parse error:
    --> 14:1
     |
  14 | subset join cat_avg_rs.category = ss.item.category
     | ^---
     |
     = expected limit, order_by, THEN_LA, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ....sale_date.month_of_year = 1
   ??? subset join cat_avg_rs.categor...
  ```
- `trilogy file write answer_2133330107.preql`

  ```text
  refused to write 'answer_2133330107.preql': not syntactically valid Trilogy.

  Parse error:
   --> 6:57
    |
  6 |   and substring(ss.customer.current_address.zip, 1, 5) <> substring(ss.store.zip, 1, 5)
    |                                                         ^---
    |
    = expected sum_operator
  Location:
  ...r.current_address.zip, 1, 5) < ??? > substring(ss.store.zip, 1, 5...
  ```

### `other`

- `trilogy run answer_507046194.preql`

  ```text
  Resolution error in answer_507046194.preql: WHERE input(s) ['ss.return_store.state'] cannot be related to the query outputs ['ss.return_customer.id', 'ss.return_customer.sk', 'ss.return_store.sk']: no join or merge connects the filter's source to any output-producing source. Add a join/merge relating them, or select a concept from the filter's model.
  ```
- `trilogy run verify_alt.preql`

  ```text
  Unexpected error in verify_alt.preql: (_duckdb.ParserException) Parser Error: zero-length delimited identifier at or near """"

  LINE 100: ...er_id_at_ws_billing_customer_id_at_wb02_cid_at_wb02_cid']"."","friendly"."INVALID_ALIAS: [MODELS_EXECUTE] Concept local...
                                                                          ^
  [SQL:
  WITH
  kaput as (
  SELECT
      "ws_billing_customer_customers"."C_CUSTOMER_ID" as "wb02_cid",
      sum("ws_web_sales"."WS_EXT_LIST_PRICE" - "ws_web_sales"."WS_EXT_DISCOUNT_AMT") as "wb02_rev"
  FROM
      "fact_web_sales" as "ws_web_sales"
      INNER JOIN "dim_date_dim" as "ws_sale_date_date" on "ws_web_sales"."WS_SOLD_DATE_SK" = "ws_sale_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "dim_customer" as "ws_billing_customer_customers" on "ws_web_sales"."WS_BILL_CUSTOMER_SK" = "ws_billing_customer_customers"."C_CUSTOMER_SK"
  WHERE
      "ws_sale_date_date"."D_YEAR" = 2002

  GROUP BY
      1),
  protective as (
  SELECT
      "kaput"."wb02_cid" as "wb02_cid",
      "kaput"."wb02_rev" as "web_02"
  FROM
      "kaput"),
  sweltering as (
  SELECT
      "ws_billing_customer_customers"."C_CUSTOMER_ID" as "wb01_cid",
      sum("ws_web_sales"."WS_EXT_LIST_PRICE" - "ws_web_sales"."WS_EXT_DISCOUNT_AMT") as "wb01_rev"
  FROM
      "fact_web_sales" as "ws_web_sales"
      INNER JOIN "dim_date_dim" as "ws_sale_date_date" on "ws_web_sales"."WS_SOLD_DATE_SK" = "ws_sale_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "dim_customer" as "ws_billing_customer_customers" on "ws_web_sales"."WS_BILL_CUSTOMER_SK" = "ws_billing_customer_customers"."C_CUSTOMER_SK"
  WHERE
      "ws_sale_date_date"."D_YEAR" = 2001

  GROUP BY
      1),
  friendly as (
  SELECT
      "sweltering"."wb01_cid" as "wb01_cid",
      "sweltering"."wb01_rev" as "web_01"
  FROM
      "sweltering"),
  uneven as (
  SELECT
      "ss_customer_customers"."C_CUSTOMER_ID" as "st02_cid",
      sum("ss_store_sales"."SS_EXT_LIST_PRICE" - "ss_store_sales"."SS_EXT_DISCOUNT_AMT") as "st02_rev"
  FROM
      "fact_store_sales" as "ss_store_sales"
      INNER JOIN "dim_date_dim" as "ss_sale_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_sale_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "dim_customer" as "ss_customer_customers" on "ss_store_sales"."SS_CUSTOMER_SK" = "ss_customer_customers"."C_CUSTOMER_SK"
  WHERE
      "ss_sale_date_date"."D_YEAR" = 2002

  GROUP BY
      1),
  concerned as (
  SELECT
      "uneven"."st02_cid" as "st02_cid",
      "uneven"."st02_rev" as "store_02"
  FROM
      "uneven"),
  cheerful as (
  SELECT
      "ss_customer_customers"."C_CUSTOMER_ID" as "st01_cid",
      sum("ss_store_sales"."SS_EXT_LIST_PRICE" - "ss_store_sales"."SS_EXT_DISCOUNT_AMT") as "st01_rev"
  FROM
      "fact_store_sales" as "ss_store_sales"
      INNER JOIN "dim_date_dim" as "ss_sale_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_sale_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "dim_customer" as "ss_customer_customers" on "ss_store_sales"."SS_CUSTOMER_SK" = "ss_customer_customers"."C_CUSTOMER_SK"
  WHERE
      "ss_sale_date_date"."D_YEAR" = 2001

  GROUP BY
      1),
  abundant as (
  SELECT
      "cheerful"."st01_cid" as "customer_id",
      "cheerful"."st01_rev" as "store_01"
  FROM
      "cheerful")
  SELECT
      coalesce("abundant"."customer_id","concerned"."customer_id","friendly"."customer_id","protective"."wb02_cid") as "customer_id",
      "abundant"."store_01" as "store_01",
      "concerned"."store_02" as "store_02",
      "friendly"."web_01" as "web_01",
      "protective"."web_02" as "web_02"
  FROM
      "abundant"
      INNER JOIN "protective" on "abundant"."customer_id" is not distinct from "protective"."wb02_cid"
      INNER JOIN "friendly" on "abundant"."customer_id" is not distinct from "friendly"."wb01_cid" AND "protective"."wb02_cid" is not distinct from "friendly"."wb01_cid"
      INNER JOIN "concerned" on "abundant"."customer_id" is not distinct from "concerned"."st02_cid" AND "friendly"."wb01_cid" is not distinct from "concerned"."st02_cid" AND "protective"."wb02_cid" is not distinct from "concerned"."st02_cid"
  WHERE
      "abundant"."store_01" is not null and "concerned"."store_02" is not null and "friendly"."web_01" is not null and "protective"."web_02" is not null and "abundant"."store_01" > 0 and "friendly"."web_01" > 0 and "protective"."web_02" * "abundant"."store_01" > "concerned"."store_02" * "friendly"."web_01"

  GROUP BY
      1,
      2,
      3,
      4,
      5
  ORDER BY
      coalesce("abundant"."customer_id","concerned"."INVALID_ALIAS: [MODELS_EXECUTE] Concept local"."customer_id@Grain<Abstract> not found on ss"."customer"."customers_at_ss_customer_sk_join_ss"."sale_date"."date_at_ss_sale_date_sk_join_ss"."store_sales_at_ss_item_sk_ss_ticket_number_at_ss_item_sk_ss_ticket_number_filtered_by_150136837477875_grouped_by_ss"."customer"."id_at_ss_customer_id_at_ss_customer_id_at_st01_cid_at_local_customer_id_st01_cid_join_ss"."customer"."customers_at_ss_customer_sk_join_ss"."sale_date"."date_at_ss_sale_date_sk_join_ss"."store_sales_at_ss_item_sk_ss_ticket_number_at_ss_item_sk_ss_ticket_number_filtered_by_6121797038643255_grouped_by_ss"."customer"."id_at_ss_customer_id_at_ss_customer_id_at_st02_cid_at_st02_cid_join_ws"."billing_customer"."customers_at_ws_billing_customer_sk_join_ws"."sale_date"."date_at_ws_sale_date_sk_join_ws"."web_sales_at_ws_item_sk_ws_order_number_at_ws_item_sk_ws_order_number_filtered_by_726435513328491_grouped_by_ws"."billing_customer"."id_at_ws_billing_customer_id_at_ws_billing_customer_id_at_wb01_cid_at_wb01_cid_join_ws"."billing_customer"."customers_at_ws_billing_customer_sk_join_ws"."sale_date"."date_at_ws_sale_date_sk_join_ws"."web_sales_at_ws_item_sk_ws_order_number_at_ws_item_sk_ws_order_number_filtered_by_8397427776121232_grouped_by_ws"."billing_customer"."id_at_ws_billing_customer_id_at_ws_billing_customer_id_at_wb02_cid_at_wb02_cid_grouped_by_local"."customer_id_at_local_customer_id_local_store_02_local_web_01_local_web_02; have ['local"."customer_id@Grain<local"."customer_id,local"."store_02,local"."web_01,local"."web_02>', 'local"."store_01@Grain<local"."customer_id,local"."store_02,local"."web_01,local"."web_02>', 'local"."store_02@Grain<local"."customer_id,local"."store_02,local"."web_01,local"."web_02>', 'local"."web_01@Grain<local"."customer_id,local"."store_02,local"."web_01,local"."web_02>', 'local"."web_02@Grain<local"."customer_id,local"."store_02,local"."web_01,local"."web_02>'] from ['ss"."customer"."customers_at_ss_customer_sk_join_ss"."sale_date"."date_at_ss_sale_date_sk_join_ss"."store_sales_at_ss_item_sk_ss_ticket_number_at_ss_item_sk_ss_ticket_number_filtered_by_150136837477875_grouped_by_ss"."customer"."id_at_ss_customer_id_at_ss_customer_id_at_st01_cid_at_local_customer_id_st01_cid', 'ss"."customer"."customers_at_ss_customer_sk_join_ss"."sale_date"."date_at_ss_sale_date_sk_join_ss"."store_sales_at_ss_item_sk_ss_ticket_number_at_ss_item_sk_ss_ticket_number_filtered_by_6121797038643255_grouped_by_ss"."customer"."id_at_ss_customer_id_at_ss_customer_id_at_st02_cid_at_st02_cid', 'ws"."billing_customer"."customers_at_ws_billing_customer_sk_join_ws"."sale_date"."date_at_ws_sale_date_sk_join_ws"."web_sales_at_ws_item_sk_ws_order_number_at_ws_item_sk_ws_order_number_filtered_by_726435513328491_grouped_by_ws"."billing_customer"."id_at_ws_billing_customer_id_at_ws_billing_customer_id_at_wb01_cid_at_wb01_cid', 'ws"."billing_customer"."customers_at_ws_billing_customer_sk_join_ws"."sale_date"."date_at_ws_sale_date_sk_join_ws"."web_sales_at_ws_item_sk_ws_order_number_at_ws_item_sk_ws_order_number_filtered_by_8397427776121232_grouped_by_ws"."billing_customer"."id_at_ws_billing_customer_id_at_ws_billing_customer_id_at_wb02_cid_at_wb02_cid']"."","friendly"."INVALID_ALIAS: [MODELS_EXECUTE] Concept local"."customer_id@Grain<Abstract> not found on ss"."customer"."customers_at_ss_customer_sk_join_ss"."sale_date"."date_at_ss_sale_date_sk_join_ss"."store_sales_at_ss_item_sk_ss_ticket_number_at_ss_item_sk_ss_ticket_number_filtered_by_150136837477875_grouped_by_ss"."customer"."id_at_ss_customer_id_at_ss_customer_id_at_st01_cid_at_local_customer_id_st01_cid_join_ss"."customer"."customers_at_ss_customer_sk_join_ss"."sale_date"."date_at_ss_sale_date_sk_join_ss"."store_sales_at_ss_item_sk_ss_ticket_number_at_ss_item_sk_ss_ticket_number_filtered_by_6121797038643255_grouped_by_ss"."customer"."id_at_ss_customer_id_at_ss_customer_id_at_st02_cid_at_st02_cid_join_ws"."billing_customer"."customers_at_ws_billing_customer_sk_join_ws"."sale_date"."date_at_ws_sale_date_sk_join_ws"."web_sales_at_ws_item_sk_ws_order_number_at_ws_item_sk_ws_order_number_filtered_by_726435513328491_grouped_by_ws"."billing_customer"."id_at_ws_billing_customer_id_at_ws_billing_customer_id_at_wb01_cid_at_wb01_cid_join_ws"."billing_customer"."customers_at_ws_billing_customer_sk_join_ws"."sale_date"."date_at_ws_sale_date_sk_join_ws"."web_sales_at_ws_item_sk_ws_order_number_at_ws_item_sk_ws_order_number_filtered_by_8397427776121232_grouped_by_ws"."billing_customer"."id_at_ws_billing_customer_id_at_ws_billing_customer_id_at_wb02_cid_at_wb02_cid_grouped_by_local"."customer_id_at_local_customer_id_local_store_02_local_web_01_local_web_02; have ['local"."customer_id@Grain<local"."customer_id,local"."store_02,local"."web_01,local"."web_02>', 'local"."store_01@Grain<local"."customer_id,local"."store_02,local"."web_01,local"."web_02>', 'local"."store_02@Grain<local"."customer_id,local"."store_02,local"."web_01,local"."web_02>', 'local"."web_01@Grain<local"."customer_id,local"."store_02,local"."web_01,local"."web_02>', 'local"."web_02@Grain<local"."customer_id,local"."store_02,local"."web_01,local"."web_02>'] from ['ss"."customer"."customers_at_ss_customer_sk_join_ss"."sale_date"."date_at_ss_sale_date_sk_join_ss"."store_sales_at_ss_item_sk_ss_ticket_number_at_ss_item_sk_ss_ticket_number_filtered_by_150136837477875_grouped_by_ss"."customer"."id_at_ss_customer_id_at_ss_customer_id_at_st01_cid_at_local_customer_id_st01_cid', 'ss"."customer"."customers_at_ss_customer_sk_join_ss"."sale_date"."date_at_ss_sale_date_sk_join_ss"."store_sales_at_ss_item_sk_ss_ticket_number_at_ss_item_sk_ss_ticket_number_filtered_by_6121797038643255_grouped_by_ss"."customer"."id_at_ss_customer_id_at_ss_customer_id_at_st02_cid_at_st02_cid', 'ws"."billing_customer"."customers_at_ws_billing_customer_sk_join_ws"."sale_date"."date_at_ws_sale_date_sk_join_ws"."web_sales_at_ws_item_sk_ws_order_number_at_ws_item_sk_ws_order_number_filtered_by_726435513328491_grouped_by_ws"."billing_customer"."id_at_ws_billing_customer_id_at_ws_billing_customer_id_at_wb01_cid_at_wb01_cid', 'ws"."billing_customer"."customers_at_ws_billing_customer_sk_join_ws"."sale_date"."date_at_ws_sale_date_sk_join_ws"."web_sales_at_ws_item_sk_ws_order_number_at_ws_item_sk_ws_order_number_filtered_by_8397427776121232_grouped_by_ws"."billing_customer"."id_at_ws_billing_customer_id_at_ws_billing_customer_id_at_wb02_cid_at_wb02_cid']"."","protective"."wb02_cid") asc nulls first
  LIMIT (200)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```

### `join-resolution`

- `trilogy run probe9.preql`

  ```text
  Resolution error in probe9.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {avg_used}; {ss.item.category, ss.sale_date.month_of_year, ss.sale_date.year}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run probe9.preql`

  ```text
  Resolution error in probe9.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {avg_used}; {ss.item.category, ss.sale_date.month_of_year, ss.sale_date.year}. Are you missing a join or merge statement to relate them?
  ```

### `file-not-found`

- `trilogy run probe.preql`

  ```text
  Input 'probe.preql' does not exist.
  ```
