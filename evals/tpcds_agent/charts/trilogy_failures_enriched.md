# Trilogy failure analysis — 20260712-204358

- Run `20260712-204357_enriched` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 1122 | failed: 95 (8%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 57 | 60% |
| `syntax-parse` | 35 | 37% |
| `type-error` | 2 | 2% |
| `syntax-missing-alias` | 1 | 1% |

## Detail

### `other`

- `trilogy run answer_3863442186.preql`

  ```text
  Unexpected error in answer_3863442186.preql: Could not render the query: Missing source reference to ss.customer.first_name; Missing source reference to ss.customer.last_name; Missing source reference to ss.customer.preferred_cust_flag. A planned reference has no backing source CTE -- typically an unsupported cross-rowset or membership shape the planner could not wire. Review the rowset/join structure (or file an issue if the query looks valid).

  Full SQL with sentinel(s):

  WITH
  waggish as (
  SELECT
      "cs_billing_customer_customers"."C_CUSTOMER_ID" as "_cs_2001_cid",
      sum(( ( "cs_catalog_sales"."CS_EXT_LIST_PRICE" - "cs_catalog_sales"."CS_EXT_WHOLESALE_COST" ) - "cs_catalog_sales"."CS_EXT_DISCOUNT_AMT" ) + "cs_catalog_sales"."CS_EXT_SALES_PRICE") as "_cs_2001_val"
  FROM
      "catalog_sales" as "cs_catalog_sales"
      INNER JOIN "date_dim" as "cs_sold_date_date" on "cs_catalog_sales"."CS_SOLD_DATE_SK" = "cs_sold_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "customer" as "cs_billing_customer_customers" on "cs_catalog_sales"."CS_BILL_CUSTOMER_SK" = "cs_billing_customer_customers"."C_CUSTOMER_SK"
  WHERE
      "cs_sold_date_date"."D_YEAR" = 2001

  GROUP BY
      1),
  cheerful as (
  SELECT
      "cs_billing_customer_customers"."C_CUSTOMER_ID" as "_cs_2002_cid",
      sum(( ( "cs_catalog_sales"."CS_EXT_LIST_PRICE" - "cs_catalog_sales"."CS_EXT_WHOLESALE_COST" ) - "cs_catalog_sales"."CS_EXT_DISCOUNT_AMT" ) + "cs_catalog_sales"."CS_EXT_SALES_PRICE") as "_cs_2002_val"
  FROM
      "catalog_sales" as "cs_catalog_sales"
      INNER JOIN "date_dim" as "cs_sold_date_date" on "cs_catalog_sales"."CS_SOLD_DATE_SK" = "cs_sold_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "customer" as "cs_billing_customer_customers" on "cs_catalog_sales"."CS_BILL_CUSTOMER_SK" = "cs_billing_customer_customers"."C_CUSTOMER_SK"
  WHERE
      "cs_sold_date_date"."D_YEAR" = 2002

  GROUP BY
      1),
  busy as (
  SELECT
      "ss_customer_customers"."C_CUSTOMER_ID" as "_ss_2001_cid",
      sum(( ( "ss_store_sales"."SS_EXT_LIST_PRICE" - "ss_store_sales"."SS_EXT_WHOLESALE_COST" ) - "ss_store_sales"."SS_EXT_DISCOUNT_AMT" ) + "ss_store_sales"."SS_EXT_SALES_PRICE") as "_ss_2001_val"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "customer" as "ss_customer_customers" on "ss_store_sales"."SS_CUSTOMER_SK" = "ss_customer_customers"."C_CUSTOMER_SK"
  WHERE
      "ss_date_date"."D_YEAR" = 2001

  GROUP BY
      1),
  yummy as (
  SELECT
      "ss_customer_customers"."C_CUSTOMER_ID" as "_ss_2002_cid",
      sum(( ( "ss_store_sales"."SS_EXT_LIST_PRICE" - "ss_store_sales"."SS_EXT_WHOLESALE_COST" ) - "ss_store_sales"."SS_EXT_DISCOUNT_AMT" ) + "ss_store_sales"."SS_EXT_SALES_PRICE") as "_ss_2002_val"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "customer" as "ss_customer_customers" on "ss_store_sales"."SS_CUSTOMER_SK" = "ss_customer_customers"."C_CUSTOMER_SK"
  WHERE
      "ss_date_date"."D_YEAR" = 2002

  GROUP BY
      1),
  macho as (
  SELECT
      "ws_billing_customer_customers"."C_CUSTOMER_ID" as "_ws_2001_cid",
      sum(( ( "ws_web_sales"."WS_EXT_LIST_PRICE" - "ws_web_sales"."WS_EXT_WHOLESALE_COST" ) - "ws_web_sales"."WS_EXT_DISCOUNT_AMT" ) + "ws_web_sales"."WS_EXT_SALES_PRICE") as "_ws_2001_val"
  FROM
      "web_sales" as "ws_web_sales"
      INNER JOIN "date_dim" as "ws_date_date" on "ws_web_sales"."WS_SOLD_DATE_SK" = "ws_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "customer" as "ws_billing_customer_customers" on "ws_web_sales"."WS_BILL_CUSTOMER_SK" = "ws_billing_customer_customers"."C_CUSTOMER_SK"
  WHERE
      "ws_date_date"."D_YEAR" = 2001

  GROUP BY
      1),
  abhorrent as (
  SELECT
      "ws_billing_customer_customers"."C_CUSTOMER_ID" as "_ws_2002_cid",
      sum(( ( "ws_web_sales"."WS_EXT_LIST_PRICE" - "ws_web_sales"."WS_EXT_WHOLESALE_COST" ) - "ws_web_sales"."WS_EXT_DISCOUNT_AMT" ) + "ws_web_sales"."WS_EXT_SALES_PRICE") as "_ws_2002_val"
  FROM
      "web_sales" as "ws_web_sales"
      INNER JOIN "date_dim" as "ws_date_date" on "ws_web_sales"."WS_SOLD_DATE_SK" = "ws_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "customer" as "ws_billing_customer_customers" on "ws_web_sales"."WS_BILL_CUSTOMER_SK" = "ws_billing_customer_customers"."C_CUSTOMER_SK"
  WHERE
      "ws_date_date"."D_YEAR" = 2002

  GROUP BY
      1),
  puffy as (
  SELECT
      "waggish"."_cs_2001_cid" as "cs_2001_cid",
      "waggish"."_cs_2001_val" as "cs_2001_val"
  FROM
      "waggish"),
  cooperative as (
  SELECT
      "cheerful"."_cs_2002_cid" as "cs_2002_cid",
      "cheerful"."_cs_2002_val" as "cs_2002_val"
  FROM
      "cheerful"),
  protective as (
  SELECT
      "busy"."_ss_2001_cid" as "ss_2001_cid",
      "busy"."_ss_2001_val" as "ss_2001_val"
  FROM
      "busy"),
  vacuous as (
  SELECT
      "yummy"."_ss_2002_cid" as "ss_2002_cid",
      "yummy"."_ss_2002_val" as "ss_2002_val"
  FROM
      "yummy"),
  friendly as (
  SELECT
      "macho"."_ws_2001_cid" as "ws_2001_cid",
      "macho"."_ws_2001_val" as "ws_2001_val"
  FROM
      "macho"),
  late as (
  SELECT
      "abhorrent"."_ws_2002_cid" as "ws_2002_cid",
      "abhorrent"."_ws_2002_val" as "ws_2002_val"
  FROM
      "abhorrent"),
  kaput as (
  SELECT
      "friendly"."ws_2001_val" as "ws_2001_val",
      "late"."ws_2002_val" as "ws_2002_val",
      coalesce("friendly"."ws_2001_cid","late"."ws_2002_cid") as "ws_2001_cid",
      coalesce("friendly"."ws_2001_cid","late"."ws_2002_cid") as "ws_2002_cid"
  FROM
      "friendly"
      INNER JOIN "late" on "friendly"."ws_2001_cid" = "late"."ws_2002_cid"
  WHERE
      "friendly"."ws_2001_val" > 0 and "late"."ws_2002_val" > 0
  ),
  divergent as (
  SELECT
      "kaput"."ws_2001_val" as "ws_2001_val",
      "kaput"."ws_2002_val" as "ws_2002_val",
      "vacuous"."ss_2002_val" as "ss_2002_val",
      coalesce("kaput"."ws_2001_cid","vacuous"."ss_2002_cid") as "ws_2001_cid",
      coalesce("kaput"."ws_2002_cid","vacuous"."ss_2002_cid") as "ss_2002_cid",
      coalesce("kaput"."ws_2002_cid","vacuous"."ss_2002_cid") as "ws_2002_cid"
  FROM
      "kaput"
      INNER JOIN "vacuous" on "kaput"."ws_2001_cid" = "vacuous"."ss_2002_cid"
  WHERE
      "vacuous"."ss_2002_val" > 0 and "kaput"."ws_2001_val" > 0 and "kaput"."ws_2002_val" > 0
  ),
  premium as (
  SELECT
      "divergent"."ss_2002_val" as "ss_2002_val",
      "divergent"."ws_2001_val" as "ws_2001_val",
      "divergent"."ws_2002_val" as "ws_2002_val",
      "protective"."ss_2001_val" as "ss_2001_val",
      coalesce("divergent"."ss_2002_cid","protective"."ss_2001_cid") as "ss_2002_cid",
      coalesce("divergent"."ws_2001_cid","protective"."ss_2001_cid") as "ws_2001_cid",
      coalesce("divergent"."ws_2002_cid","protective"."ss_2001_cid") as "ss_2001_cid",
      coalesce("divergent"."ws_2002_cid","protective"."ss_2001_cid") as "ws_2002_cid"
  FROM
      "divergent"
      INNER JOIN "protective" on "divergent"."ss_2002_cid" = "protective"."ss_2001_cid"
  WHERE
      "protective"."ss_2001_val" > 0 and "divergent"."ss_2002_val" > 0 and "divergent"."ws_2001_val" > 0 and "divergent"."ws_2002_val" > 0
  ),
  puzzled as (
  SELECT
      "cooperative"."cs_2002_val" as "cs_2002_val",
      "premium"."ss_2001_val" as "ss_2001_val",
      "premium"."ss_2002_val" as "ss_2002_val",
      "premium"."ws_2001_val" as "ws_2001_val",
      "premium"."ws_2002_val" as "ws_2002_val",
      coalesce("cooperative"."cs_2002_cid","premium"."ss_2001_cid") as "ss_2001_cid",
      coalesce("cooperative"."cs_2002_cid","premium"."ws_2002_cid") as "cs_2002_cid",
      coalesce("cooperative"."cs_2002_cid","premium"."ws_2002_cid") as "ws_2002_cid"
  FROM
      "premium"
      INNER JOIN "cooperative" on "premium"."ss_2001_cid" = "cooperative"."cs_2002_cid"
  WHERE
      "premium"."ss_2001_val" > 0 and "premium"."ss_2002_val" > 0 and "cooperative"."cs_2002_val" > 0 and "premium"."ws_2001_val" > 0 and "premium"."ws_2002_val" > 0
  )
  SELECT
      "puzzled"."ss_2001_cid" as "customer_id",
      INVALID_REFERENCE_BUG<Missing source reference to ss.customer.first_name> as "first_name",
      INVALID_REFERENCE_BUG<Missing source reference to ss.customer.last_name> as "last_name",
      INVALID_REFERENCE_BUG<Missing source reference to ss.customer.preferred_cust_flag> as "preferred_cust_flag"
  FROM
      "puzzled"
      INNER JOIN "puffy" on "puzzled"."cs_2002_cid" = "puffy"."cs_2001_cid"
  WHERE
      "puffy"."cs_2001_val" > 0 and ( "puzzled"."cs_2002_val" / "puffy"."cs_2001_val" ) > ( "puzzled"."ss_2002_val" / "puzzled"."ss_2001_val" ) and ( "puzzled"."cs_2002_val" / "puffy"."cs_2001_val" ) > ( "puzzled"."ws_2002_val" / "puzzled"."ws_2001_val" )

  GROUP BY
      1,
      2,
      3,
      4
  ORDER BY
      "customer_id" asc nulls first,
      "first_name" asc nulls first,
      "last_name" asc nulls first,
      "preferred_cust_flag" asc nulls first
  LIMIT (100)
  ```
- `trilogy file delete answer_3863442186_check.preql`

  ```text
  No such path: answer_3863442186_check.preql
  ```
- `trilogy file read answer_3705756794.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_883027685.preql`

  ```text
  Resolution error in answer_883027685.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 9). The requested concepts split into 2 disconnected subgraphs: {cat_avg_price}; {line_item_count, ss.customer.address.sk, ss.customer.address.state, ss.date.month_of_year, ss.date.year, ss.item.category, ss.item.current_price}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read answer_883027685.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3697706765.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Resolution error in answer_3697706765.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 31). The requested concepts split into 2 disconnected subgraphs: {store_name, store.zip}; {total_net_profit, sales.date.quarter, sales.date.year}.
    - `store.zip` is disconnected — did you mean `sales.store.zip`? (connected to the other concepts)
  These look like separately-imported copies of models already reachable through a connected import; chain through that path (e.g. `sales.store.zip`) instead of importing a second, disconnected copy.
  ```
- `trilogy file read answer_3697706765.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_219203908.preql`

  ```text
  Syntax error in answer_219203908.preql: Nothing was executed: parsed 5 definition statement(s) (4 rowsets, 1 import) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read answer_765177085.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_2604809012.preql`

  ```text
  Syntax error in answer_2604809012.preql: Undefined concept: item.current_price. Suggestions: ['inv.item.current_price']
  ```
- `trilogy run --import raw.all_sales:all --import raw.catalog_sales:cat where cat.sold_date.year = 2000 and cat.sold_date.month_of_year = 2 and cat.item.sk in …l.date.year in (2000,2001,2002,2003) select all.item.sk, count(all.sale_line_item_counter) as cnt having cnt > 4) select count(cat.line_item) as cnt;`

  ```text
  Unexpected error in stdin: a `(select ...)` subquery must select exactly one column
  ```
- `trilogy run --import raw.all_sales:all --import raw.catalog_sales:cat where cat.sold_date.year = 2000 and cat.sold_date.month_of_year = 2 and cat.item.sk in …l.date.year in (2000,2001,2002,2003) select all.item.sk, count(all.sale_line_item_counter) as cnt having cnt > 4) select count(cat.line_item) as cnt;`

  ```text
  Unexpected error in stdin: a `(select ...)` subquery must select exactly one column
  ```
- `trilogy run --import raw.all_sales:all --import raw.catalog_sales:cat where cat.sold_date.year=2000 and cat.sold_date.month_of_year=2 and cat.item.sk in (whe…l.date.year in (2000,2001,2002,2003) select all.item.sk, count(all.sale_line_item_counter) as cnt having cnt > 4) select count(cat.line_item) as cnt;`

  ```text
  Unexpected error in stdin: a `(select ...)` subquery must select exactly one column
  ```
- `trilogy file read answer_3825713089.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_1798498862.preql`

  ```text
  Syntax error in answer_1798498862.preql: Comparison `ss.return_date.month_of_year <= 12` matches every value of enum field 'ss.return_date.month_of_year', which contains only these values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12. It is always true and should be removed.
  ```
- `trilogy run --import raw.store_sales:ss --import raw.catalog_sales:cs select count(ss.ticket_number) as cnt where ss.date.year = 1999 and ss.date.month_of_ye…urn_customer.sk = ss.customer.sk and ss.customer.sk in cs.billing_customer.sk and ss.item.sk in cs.item.sk and cs.sold_date.year in (1999,2000,2001);`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {cs.sold_date.year}; {ss.customer.sk, ss.date.month_of_year, ss.date.year, ss.is_returned, ss.item.sk, ss.return_customer.sk, ss.return_date.month_of_year, ss.return_date.year}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read answer_4207382245.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_525180526.preql`

  ```text
  Syntax error in answer_525180526.preql: Undefined concept: item.manufacturer_id. Suggestions: ['cs.item.manufacturer_id', 'cs.item.manager_id', 'cs.item.manufact']
  ```
- `trilogy run answer_3770074305.preql`

  ```text
  Syntax error in answer_3770074305.preql: Nothing was executed: parsed 4 definition statement(s) (2 imports, 2 rowsets) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy run answer_751385098.preql`

  ```text
  Syntax error in answer_751385098.preql: Undefined concept: local.store_last_name. Suggestions: ['store_combos.store_last_name', 'ss.customer.last_name', 'ss.store.street_name']
  ```
- `trilogy agent-info syntax example intersect-setops`

  ```text
  Unknown syntax example: 'intersect-setops'

  Available Trilogy syntax examples - print one with `trilogy agent-info syntax example <name>`:

  - `query-structure` - the clause order of a query (`where` -> `select` <cols> -> join(s) -> `having` -> `order by` -> `limit`) and how to define a rowset (a NAME then a select); joins sit right after the select list - filter a joined or aggregated RESULT in `having`, input rows in `where`
  - `filtered-aggregate` - `sum(x ? cond)` / `count(x ? cond)` aggregate just the matching rows; to COUNT ROWS count the unique grain/row key, not a non-unique sub-key; `b
  …
  subtotal/total rows and to sort by level
  - `rank-over-rollup` - rank rollup subtotals/leaves with a SINGLE `rank(a,b) over (partition by level, parent ...)` - not separate ranks per level
  - `staged-membership` - compute a membership set in a `rowset` (keys meeting a count/HAVING), then filter the main query with `<key> in <rowset>.<col>`
  - `correlated-exists-via-grouped-counts` - translate `EXISTS other` / `NOT EXISTS other matching` over the same model into two `count(...) by <grain>` compared in `where` (`> 1` = another exists, `= 1` = no other matches) - pin the correlation grain with `by`
  ```
- `trilogy file read answer_142117636.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3079393690.preql`

  ```text
  Resolution error in answer_3079393690.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {friday, monday, saturday, sunday, thursday, tuesday, wednesday, ss.date.year}; {store_code, store_name, st.gmt_offset}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_374591292.preql`

  ```text
  Resolution error in answer_374591292.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {ranked_best.asc_rank, ranked_best.item_profits.ss.item.product_name, ranked_worst.item_profits.ss.item.product_name}
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run answer_968172492.preql`

  ```text
  Syntax error in answer_968172492.preql: Undefined concept: item.id. Suggestions: ['ws.item.id', 'ws.date.id', 'ws.time.id', 'ws.web_site.id', 'ws.ship_date.id', 'ws.ship_customer.id']
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_2118989494.preql`

  ```text
  Syntax error in answer_2118989494.preql: Nothing was executed: parsed 4 definition statement(s) (3 rowsets, 1 import) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy run raw/date.preql duckdb --param debug=true`

  ```text
  Syntax error in raw\date.preql: Nothing was executed: parsed 4 definition statement(s) (1 concept, 1 datasource, 1 import, 1 property) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy run answer_3553309440.preql duckdb`

  ```text
  Syntax error in answer_3553309440.preql: Undefined concept: ws.sold_date.year. Suggestions: ['ws.date.year', 'ws.ship_date.year', 'ws.return_date.year', 'cs.sold_date.year', 'cs.ship_date.year', 'cs.date.year']
  ```
- `trilogy run answer_3553309440.preql duckdb`

  ```text
  Unexpected error in answer_3553309440.preql: (_duckdb.OutOfRangeException) Out of Range Error: Overflow in multiplication of INT32 (105249413 * 50)!
  [SQL:
  WITH
  puzzled as (
  SELECT
      cast(round(sum("ss_store_sales"."SS_EXT_SALES_PRICE") / 50,0) as int) * 50 as "segment_times_50"
  FROM
      "store_sales" as "ss_store_sales"
  HAVING
      sum("ss_store_sales"."SS_EXT_SALES_PRICE") > 0
  ),
  protective as (
  SELECT
      cast(round(sum("ss_store_sales"."SS_EXT_SALES_PRICE") / 50,0) as int) as "segment"
  FROM
      "store_sales" as "ss_store_sales"
  HAVING
      sum("ss_store_sales"."SS_EXT_SALES_PRICE") > 0
  ),
  divergent as (
  SELECT
      sum("ss_store_sales"."SS_EXT_SALES_PRICE") as "total_spend"
  FROM
      "store_sales" as "ss_store_sales"
  HAVING
      "total_spend" > 0
  ),
  waggish as (
  SELECT
      "puzzled"."segment_times_50" as "segment_times_50"
  FROM
      "puzzled"),
  premium as (
  SELECT
      "protective"."segment" as "segment"
  FROM
      "protective"),
  busy as (
  SELECT
      count("ss_customer_customers"."C_CUSTOMER_SK") as "customer_count"
  FROM
      "divergent"
      LEFT OUTER JOIN "customer" as "ss_customer_customers" on 1=1)
  SELECT
      "premium"."segment" as "segment",
      coalesce("busy"."customer_count",0) as "customer_count",
      "waggish"."segment_times_50" as "segment_times_50"
  FROM
      "busy"
      FULL JOIN "premium" on 1=1
      FULL JOIN "waggish" on 1=1
  ORDER BY
      "premium"."segment" asc nulls first,
      coalesce("busy"."customer_count",0) asc nulls first,
      "waggish"."segment_times_50" asc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/9h9h)
  ```
- `trilogy run answer_3553309440.preql duckdb`

  ```text
  Resolution error in answer_3553309440.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 12). The requested concepts split into 3 disconnected subgraphs: {cs.billing_customer.sk, cs.item.category, cs.item.class, cs.sold_date.month_of_year, cs.sold_date.year}; {cust_sk, total_spend}; {ws.billing_customer.sk, ws.date.month_of_year, ws.date.year, ws.item.category, ws.item.class}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_2986518257.preql`

  ```text
  Resolution error in answer_2986518257.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 7). The requested concepts split into 3 disconnected subgraphs: {cc.name}; {cs.sold_date.month_of_year, cs.sold_date.month_seq, cs.sold_date.year, _monthly_totals_monthly_total}; {it.brand_name, it.category}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_2986518257.preql`

  ```text
  Resolution error in answer_2986518257.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {sequ.monthly_totals.cs.call_center.name, sequ.monthly_totals.cs.item.brand_name, sequ.monthly_totals.cs.item.category, sequ.monthly_totals.cs.sold_date.month_of_year, sequ.monthly_totals.cs.sold_date.month_seq, sequ.monthly_totals.cs.sold_date.year, sequ.monthly_totals.monthly_total, sequ.seq, year_avg.avg_monthly_sales}
  ```
- `trilogy file read answer_1484301313.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3544057080.preql`

  ```text
  Syntax error in answer_3544057080.preql: Undefined concept: y2000.item_id. Suggestions: ['y2000.store_zip', 'y2000.store_name', 'y2000.line_count', 'agg_rows.item_id']
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run answer_1256642802.preql duckdb`

  ```text
  Syntax error in answer_1256642802.preql: ORDER BY references 'item_revenue.store_id', which is not in the SELECT projection (line 27). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --item_revenue.store_id order by item_revenue.store_id asc`.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_1772060640.preql`

  ```text
  Syntax error in answer_1772060640.preql: ORDER BY references 'ticket_lines.customer_sk', which is not in the SELECT projection (line 24). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --ticket_lines.customer_sk order by ticket_lines.customer_sk asc`.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 113 column 12 (char 3927). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run answer_3210116865.preql`

  ```text
  Syntax error in answer_3210116865.preql: ORDER BY contains aggregate `grouping(combined.channel)` (line 130), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(combined.channel) as g order by g desc`.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 105 column 2 (char 3815). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run answer_1226264875.preql`

  ```text
  Syntax error in answer_1226264875.preql: 6 undefined concept references; fix all before re-running:
    - store_qty (line 81, col 5, in ORDER BY); did you mean: store_nr.store_qty, store_nr.yr, ss.store.city?
    - store_wc_sum (line 82, col 5, in ORDER BY); did you mean: store_nr.store_wc_sum, store_nr.cust_sk, store_nr.store_sp_sum?
    - store_sp_sum (line 83, col 5, in ORDER BY); did you mean: store_nr.store_sp_sum, store_nr.store_wc_sum, store_nr.cust_sk?
    - other_qty (line 84, col 5, in ORDER BY); did you mean: other_nr.other_qty, other_nr.yr?
    - other_wc_sum (line 85, col 5, in ORDER BY); did you mean: other_nr.other_wc_sum, other_nr.cust_sk, web_nr.web_wc_sum?
    - other_sp_sum (line 86, col 5, in ORDER BY); did you mean: other_nr.other_sp_sum, other_nr.cust_sk, web_nr.web_sp_sum?
  ```
- `trilogy run answer_943796012.preql`

  ```text
  Syntax error in answer_943796012.preql: `by rollup (…)` requires at least one aggregate (or re-aggregable pre-aggregated measure) in the select to group; found none.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 73 column 3 (char 2448). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run answer_2869182220.preql`

  ```text
  Resolution error in answer_2869182220.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 8). The requested concepts split into 2 disconnected subgraphs: {customer.address.city, customer.demographics.sk, customer.household_demographic.income_band.lower_bound, customer.household_demographic.income_band.upper_bound, customer_code, full_name}; {store_sales.is_returned, store_sales.return_customer_demographic.sk}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 43 column 12 (char 1343). Re-issue the call with valid JSON arguments.
  ```

### `syntax-parse`

- `trilogy file write answer_1858999935.preql --content import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Combine catalog and web sales: total sale…ales ? channel_combined.dow = 6), 53) over (order by channel_combined.week_seq),
        2
    ) as sat_ratio
order by channel_combined.week_seq asc;`

  ```text
  refused to write 'answer_1858999935.preql': not syntactically valid Trilogy.

  Parse error:
   --> 6:5
    |
  6 |     select
    |     ^---
    |
    = expected tvf_rel_arg
  Location:
  ...hannel_combined as union(     ??? select         cs.sold_date.we...

  Write stats: received 2260 chars / 2260 bytes; tail: …'2\\n    ) as sat_ratio\\norder by channel_combined.week_seq asc;'.
  ```
- `trilogy file write answer_3863442186_check.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Quick…al as val_2001,
    ws_2002.val as val_2002,
    ws_2002.val / ws_2001.val as growth_rate
union join ws_2001.cid = ws_2002.cid

order by channel asc;`

  ```text
  refused to write 'answer_3863442186_check.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
   / ss_2001.val as growth_rate ??? union join ss_2001.cid = ss_20...

  Write stats: received 1666 chars / 1666 bytes; tail: …'\\nunion join ws_2001.cid = ws_2002.cid\\n\\norder by channel asc;'.
  ```
- `trilogy file write answer_3705756794.preql --content import raw.all_sales as all_sales;

# Sales arm: entity = channel_dim_text_id, metrics = ext_sales_price…urn_loss, 0.0)) as net_profit
by rollup (channel_type, entity_identifier)
order by channel_type nulls first, entity_identifier nulls first
limit 100;`

  ```text
  refused to write 'answer_3705756794.preql': not syntactically valid Trilogy.

  Parse error:
    --> 31:6
     |
  31 |     (sales_arm),
     |      ^---
     |
     = expected select_statement
  Location:
   with combined as union(     ( ??? sales_arm),     (returns_arm)

  Write stats: received 2187 chars / 2187 bytes; tail: …'l_type nulls first, entity_identifier nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw/all_sales:all_sales --format json select all_sales.channel, count(all_sales.channel_dim_text_id) as cnt where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date and all_sales.channel_dim_text_id is not null group by all_sales.channel;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...annel_dim_text_id is not null ??? group by all_sales.channel;
  ```
- `trilogy file write answer_883027685.preql --content import raw.store_sales as ss;

# Compute the average current price per category across distinct items (it….category

having
    line_item_count >= 10

order by
    line_item_count asc nulls first,
    ss.customer.address.state asc nulls first

limit 100;
`

  ```text
  refused to write 'answer_883027685.preql': not syntactically valid Trilogy.

  Parse error:
   --> 5:1
    |
  5 | import raw.item as item
    | ^---
    |
    = expected select_statement, tvf_union_invocation, tvf_except_invocation, or tvf_intersect_invocation
  Location:
  ...siness IDs) with cat_price as ??? import raw.item as item select...

  Write stats: received 708 chars / 708 bytes; tail: …',\\n    ss.customer.address.state asc nulls first\\n\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_883027685.preql --content import raw.store_sales as ss;
import raw.item as item;

# For each distinct item business ID, get one cur….category

having
    line_item_count >= 10

order by
    line_item_count asc nulls first,
    ss.customer.address.state asc nulls first

limit 100;
`

  ```text
  refused to write 'answer_883027685.preql': not syntactically valid Trilogy.

  Parse error:
   --> 5:1
    |
  5 | (rowsets deduplicate to the select grain, so each item.id appears once)
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...ne current_price and category ??? (rowsets deduplicate to the se...

  Write stats: received 921 chars / 921 bytes; tail: …',\\n    ss.customer.address.state asc nulls first\\n\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_2928586490.preql --content import raw.all_sales as all;
import raw.store_sales as store;
import raw.catalog_sales as cat;
import ra…ere
    combined.total_sales > 0
order by
    combined.last_name,
    combined.first_name,
    sum(combined.total_sales) with nulls first
limit 100;
`

  ```text
  refused to write 'answer_2928586490.preql': not syntactically valid Trilogy.

  Parse error:
    --> 92:31
     |
  92 |     sum(combined.total_sales) with nulls first
     |                               ^---
     |
     = expected limit, ORDERING_DIRECTION, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, aggregate_over, or window_sql_over
  Location:
      sum(combined.total_sales) ??? with nulls first limit 100;

  Write stats: received 2671 chars / 2671 bytes; tail: …',\\n    sum(combined.total_sales) with nulls first\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw.all_sales:all --import raw.catalog_sales:cat --import raw.web_sales:web rowset frequent_items <- where all.date.year in (2000,2001,2…ere cat.sold_date.year=2000 and cat.sold_date.month_of_year=2 and cat.item.sk in (select frequent_items.item_sk) select count(cat.line_item) as cnt);`

  ```text
  Syntax error in stdin: Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...ving triple_count > 4; select ??? count(*) from (where cat.sold_...
  ```
- `trilogy file write answer_1798498862.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;

# Define the set of (customer, item) pair…as store_name,
    sum(ss.quantity) as ss_qty,
    sum(ss.return_quantity) as sr_qty
order by item_code, item_desc, store_code, store_name
limit 100;`

  ```text
  refused to write 'answer_1798498862.preql': not syntactically valid Trilogy.

  Parse error:
   --> 5:31
    |
  5 | auto cs_cust_item <- distinct cs.billing_customer.sk, cs.item.sk ? cs.sold_date.year in (1999, 2000, 2001);
    |                               ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...auto cs_cust_item <- distinct ??? cs.billing_customer.sk, cs.ite...

  Write stats: received 865 chars / 865 bytes; tail: …'r by item_code, item_desc, store_code, store_name\\nlimit 100;'.
  ```
- `trilogy run --import raw.store_sales:ss --import raw.catalog_sales:cs select ss.store.id as sid, count(ss.ticket_number) as cnt where ss.date.year = 1999 and ss.date.month_of_year = 9 and ss.is_returned and ss.return_date.year = 1999 and ss.return_date.month_of_year >= 9 group by sid order by sid limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...eturn_date.month_of_year >= 9 ??? group by sid order by sid limi...
  ```
- `trilogy file write answer_4207382245.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;

# Store sales aggregates by county, year, qua…ales_total ? store_totals.quarter = 1) as store_q3_q2

having
    web_q2_q1 > store_q2_q1
    and web_q3_q2 > store_q3_q2

order by
    county asc
;
`

  ```text
  refused to write 'answer_4207382245.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...unty and year, compute ratios ??? subset join store_totals.count...

  Write stats: received 1461 chars / 1461 bytes; tail: …'\\n    and web_q3_q2 > store_q3_q2\\n\\norder by\\n    county asc\\n;\\n'.
  ```
- `trilogy file write answer_4140546834.preql --content import raw.store_sales as store_sales;

auto gross_margin_ratio <- sum(store_sales.net_profit) / sum(sto… = 2001 and store_sales.store.state = 'TN'
order by level desc nulls first, store_sales.item.category asc nulls first, rnk asc nulls first
limit 100;`

  ```text
  refused to write 'answer_4140546834.preql': not syntactically valid Trilogy.

  Parse error:
    --> 20:1
     |
  20 | where store_sales.date.year = 2001 and store_sales.store.state = 'TN'
     | ^---
     |
     = expected limit, order_by, or having
  Location:
  ...gory, store_sales.item.class) ??? where store_sales.date.year =

  Write stats: received 924 chars / 924 bytes; tail: …'tem.category asc nulls first, rnk asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write answer_4140546834.preql --content import raw.store_sales as store_sales;

where store_sales.date.year = 2001 and store_sales.store.state =…les.item.category, store_sales.item.class)
order by level desc nulls first, store_sales.item.category asc nulls first, rnk asc nulls first
limit 100;`

  ```text
  refused to write 'answer_4140546834.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block - they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...ore_sales.store.state = 'TN'  ??? auto gross_margin_ratio <- sum...

  Write stats: received 925 chars / 925 bytes; tail: …'tem.category asc nulls first, rnk asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write answer_751385098.preql -e -c import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

with all_intersect…billing_customer.first_name,
         ws.date.date
    )
) -> (lname, fname, sdate)
;

select
    count(all_intersection.sdate) as result
limit 100
;`

  ```text
  refused to write 'answer_751385098.preql': not syntactically valid Trilogy.

  Parse error:
   --> 6:6
    |
  6 |     (import raw.store_sales as ss;
    |      ^---
    |
    = expected select_statement
  Location:
  ...tersection as intersect(     ( ??? import raw.store_sales as ss;

  Write stats: received 906 chars / 906 bytes; tail: …'lect\\n    count(all_intersection.sdate) as result\\nlimit 100\\n;'.
  ```
- `trilogy run --import raw.item:item select item.manufact, item.manufacturer_id, item.category, item.color, item.units, item.size where ((item.category='Books'…') or (item.category='Electronics' and item.color='mint' and item.units='Gross' and item.size='N/A')) and item.manufacturer_id not between 1 and 500;`

  ```text
  Syntax error in stdin:  --> 2:879
    |
  2 | select item.manufact, item.manufacturer_id, item.category, item.color, item.units, item.size where ((item.category='Books' and item.color='tan' and item.units='Oz' and item.size='N/A') or (item.category='Electronics' and item.color='purple' and item.units='Ton' and item.size='N/A') or (item.category='Men' and item.color='misty' and item.units='Box' and item.size='medium') or (item.category='Books' and item.color='medium' and item.units='Tsp' and item.size='N/A') or (item.category='Books' and item.color='midnight' and item.units='Gram' and item.size='N/A') or (item.category='Books' and item.color='pale' and item.units='Pound' and item.size='N/A') or (item.category='Electronics' and item.color='khaki' and item.units='Pallet' and item.size='N/A') or (item.category='Electronics' and item.color='mint' and item.units='Gross' and item.size='N/A')) and item.manufacturer_id not between 1 and 500;
    |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...A')) and item.manufacturer_id ??? not between 1 and 500;
  ```
- `trilogy run --import raw.item:item select distinct item.manufact where (item.category='Books' and item.color='tan' and item.units='Oz' and item.size='N/A') o…khaki' and item.units'Pallet' and item.size='N/A') or (item.category='Electronics' and item.color='mint' and item.units='Gross' and item.size='N/A');`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...port raw.item as item; select ??? distinct item.manufact where (...
  ```
- `trilogy file write answer_3560698360.preql --content import raw.store_sales as store;
import raw.web_sales as web;

# Web channel: per-item per-day sum of sa…ng.date_sk
where web_running.web_running_max > store_running.store_running_max
order by item_sk asc nulls first, sale_date asc nulls first
limit 100;`

  ```text
  refused to write 'answer_3560698360.preql': not syntactically valid Trilogy.

  Parse error:
    --> 21:147
     |
  21 |     max(sum(web_daily.web_daily_price) over (partition by web_daily.item_sk order by web_daily.date_sk asc)) over (partition by web_daily.item_sk order by web_daily.date_sk asc rows between unbounded preceding and current row) as web_running_max
     |                                                                                                                                                   ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...artition by web_daily.item_sk ??? order by web_daily.date_sk asc...

  Write stats: received 2257 chars / 2257 bytes; tail: …'tem_sk asc nulls first, sale_date asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write answer_3560698360.preql --content import raw.store_sales as store;
import raw.web_sales as web;

# Web channel: per-item per-day sum of sa…_final.date_sk
where web_final.web_running_max > store_final.store_running_max
order by item_sk asc nulls first, sale_date asc nulls first
limit 100;`

  ```text
  refused to write 'answer_3560698360.preql': not syntactically valid Trilogy.

  Parse error:
    --> 31:71
     |
  31 |     max(web_run.web_running_total) over (partition by web_run.item_sk order by web_run.date_sk asc rows between unbounded preceding and current row) as web_running_max
     |                                                                       ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...(partition by web_run.item_sk ??? order by web_run.date_sk asc r...

  Write stats: received 2411 chars / 2411 bytes; tail: …'tem_sk asc nulls first, sale_date asc nulls first\\nlimit 100;'.
  ```
- `trilogy run - duckdb`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...d d.month_of_year = 12 select ??? distinct d.month_seq;
  ```
- `trilogy run --import raw/store_sales:ss select ss.store.name, ss.date.week_seq, sum(ss.sales_price ? ss.date.day_of_week = 1) as mon_sum where ss.date.year = 2001 and ss.date.week_seq in (5270, 5322) group by ss.store.name, ss.date.week_seq order by ss.store.name, ss.date.week_seq;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...date.week_seq in (5270, 5322) ??? group by ss.store.name, ss.dat...
  ```
- `trilogy run --import raw/store_sales:ss select ss.store.name, ss.store.id, ss.date.year, count(ss.ticket_number) where ss.store.id is null group by ss.store.name, ss.store.id, ss.date.year;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...er) where ss.store.id is null ??? group by ss.store.name, ss.sto...
  ```
- `trilogy file write answer_3544057080.preql`

  ```text
  refused to write 'answer_3544057080.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...tore_name = y2000.store_name
   ??? union join agg_rows.store_zip

  Write stats: received 3159 chars / 3159 bytes; tail: …'ne_count,\r\\n    agg_rows.sum_wcost,\r\\n    y2000.sum_wcost\r\\n;\r\\n'.
  ```
- `trilogy file write answer_3544057080.preql`

  ```text
  refused to write 'answer_3544057080.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ..._id + store_name + store_zip
   ??? union join agg_rows.item_id =

  Write stats: received 3055 chars / 3055 bytes; tail: …'ne_count,\r\\n    agg_rows.sum_wcost,\r\\n    y2000.sum_wcost\r\\n;\r\\n'.
  ```
- `trilogy file write answer_1256642802.preql --content import raw.store_sales as ss;

# Per-item revenue at (store, item) grain: sum of unit prices
with item_r…ls first,
    item_revenue.item_desc asc nulls first,
    item_revenue.store_id asc nulls first,
    item_revenue.item_id asc nulls first
limit 100
;`

  ```text
  refused to write 'answer_1256642802.preql': not syntactically valid Trilogy.

  Parse error:
    --> 27:3
     |
  27 | --item_revenue.store_id,
     |   ^---
     |
     = expected PURPOSE, PROPERTY, UNIQUE, or AUTO
  Location:
  ...v ;  # Join them and filter -- ??? item_revenue.store_id, --item_...

  Write stats: received 1216 chars / 1216 bytes; tail: …' first,\\n    item_revenue.item_id asc nulls first\\nlimit 100\\n;'.
  ```
- `trilogy file write answer_3979964698.preql -e -c import raw.catalog_sales as catalog;
import raw.web_sales as web;

# Combine web and catalog using union
wit…= 11) as net_nov,
  sum(combined.monthly_net ? combined.mo = 12) as net_dec
order by combined.wh_name nulls first, combined.yr nulls first
limit 100;`

  ```text
  refused to write 'answer_3979964698.preql': not syntactically valid Trilogy.

  Parse error:
    --> 12:15
     |
  12 |     --private warehouse.sk
     |               ^---
     |
     = expected limit, order_by, where, having, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, select_grouping, or JOIN_TYPE
  Location:
   null    select     --private ??? warehouse.sk     catalog.wareh...

  Write stats: received 4538 chars / 4538 bytes; tail: …'ined.wh_name nulls first, combined.yr nulls first\\nlimit 100;'.
  ```
- `trilogy file write answer_2091535883.preql --content import raw.catalog_sales as cs;
import raw.inventory as inv;

# Filter catalog sales: year 1999, househo…tity
order by
    total_count desc,
    item_description asc,
    inventory_warehouse_name asc,
    sold_week_sequence asc
    nulls first
limit 100;`

  ```text
  refused to write 'answer_2091535883.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
   as with_promotion_count,     ??? count(*) as total_count having...

  Write stats: received 1170 chars / 1170 bytes; tail: …'e asc,\\n    sold_week_sequence asc\\n    nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.item:item select distinct item.category limit 20;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...port raw.item as item; select ??? distinct item.category limit 2...
  ```
- `trilogy file write answer_3046445280.preql --content import raw.all_sales as s;

# Net quantity: sold minus returned, null if sold is null
auto net_qty <- ca…ufacturer_id
where y2001.qty is not null
  and y2002.qty is not null
  and y2002.qty / y2001.qty < 0.9
order by qty_diff asc, amt_diff asc
limit 100;`

  ```text
  refused to write 'answer_3046445280.preql': not syntactically valid Trilogy.

  Parse error:
    --> 10:1
     |
  10 | rowset y2001 as
     | ^---
     |
     = expected EOI, block, or show_statement
  Location:
  ...e null end;  # 2001 aggregate ??? rowset y2001 as where s.item.c...

  Write stats: received 1675 chars / 1675 bytes; tail: …'001.qty < 0.9\\norder by qty_diff asc, amt_diff asc\\nlimit 100;'.
  ```
- `trilogy file write answer_747155668.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sales as cs;

# Two-stage mi…ined.total_ext_sales_price
order by combined.channel, combined.missing_ref, combined.year, combined.quarter, combined.category nulls first
limit 100;`

  ```text
  refused to write 'answer_747155668.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...total_ext_sales_price numeric) ???  select   combined.channel,

  Write stats: received 2330 chars / 2330 bytes; tail: …', combined.quarter, combined.category nulls first\\nlimit 100;'.
  ```
- `trilogy file write answer_943796012.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as…nel is null and combined.outlet_identifier is null)
order by
    combined.channel nulls first,
    combined.outlet_identifier nulls first
limit 100;
`

  ```text
  refused to write 'answer_943796012.preql': not syntactically valid Trilogy.

  Parse error:
    --> 55:6
     |
  55 |     (store_channel),
     |      ^---
     |
     = expected select_statement
  Location:
   with combined as union(     ( ??? store_channel),     (catalog_c...

  Write stats: received 2568 chars / 2568 bytes; tail: …'irst,\\n    combined.outlet_identifier nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_943796012.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as…nel is null and combined.outlet_identifier is null)
order by
    combined.channel nulls first,
    combined.outlet_identifier nulls first
limit 100;
`

  ```text
  refused to write 'answer_943796012.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...ifier, sales, returns, profit) ???  select     combined.channel,...

  Write stats: received 2380 chars / 2380 bytes; tail: …'irst,\\n    combined.outlet_identifier nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_4111870542.preql`

  ```text
  refused to write 'answer_4111870542.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:23
    |
  7 | auto target_weeks <- {5244, 5257, 5264};
    |                       ^---
    |
    = expected FLOAT_LITERAL_PART, BOOL_KW, null_lit, or MULTILINE_STRING
  Location:
  ...uences
   auto target_weeks <- { ??? 5244, 5257, 5264};

   # --- Pe...

  Write stats: received 3652 chars / 3652 bytes; tail: …'         filtered.store_return_qty nulls first\r\\nlimit 100;\r\\n'.
  ```
- `trilogy file write answer_2852230229.preql --content import raw.web_sales as ws;

where year(ws.date.date) = 2000
select
    ws.item.category,
    ws.item.cl…archy_level desc nulls first,
    case when hierarchy_level = 0 then ws.item.category end nulls first,
    total_net_paid desc nulls first
limit 100;`

  ```text
  refused to write 'answer_2852230229.preql': not syntactically valid Trilogy.

  Parse error:
   --> 8:30
    |
  8 |     grouping(ws.item.category, ws.item.class) as hierarchy_level
    |                              ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
       grouping(ws.item.category ??? , ws.item.class) as hierarchy_...

  Write stats: received 424 chars / 424 bytes; tail: …' nulls first,\\n    total_net_paid desc nulls first\\nlimit 100;'.
  ```
- `trilogy file write answer_210925266.preql --content import raw.store_sales as store;
import raw.catalog_sales as catalog;
import raw.web_sales as web;

# Sto…rst_name, web_combos.sale_date)
) -> (last_name, first_name, sale_date);

# Count them
select
    count(only_store.sale_date) as unique_combo_count
;`

  ```text
  refused to write 'answer_210925266.preql': not syntactically valid Trilogy.

  Parse error:
    --> 10:4
     |
  10 |   -- Actually: consider only sales that have a billing customer on record.
     |    ^---
     |
     = expected access_chain
  Location:
   exist? No - re-reading...   - ??? - Actually: consider only sale...

  Write stats: received 1886 chars / 1886 bytes; tail: …'lect\\n    count(only_store.sale_date) as unique_combo_count\\n;'.
  ```
- `trilogy run --import raw.reason:reason select distinct reason.id limit 50;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
   raw.reason as reason; select ??? distinct reason.id limit 50;
  ```

### `type-error`

- `trilogy run answer_3697706765.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Type error in answer_3697706765.preql: Invalid argument type 'ArrayType<STRING>' passed into SUBSTRING function in position 1 from concept: local.qualifying_zips. Valid: 'STRING'.
  ```
- `trilogy run answer_3770074305.preql`

  ```text
  Syntax error in answer_3770074305.preql: Cannot use BETWEEN with incompatible types DATE and STRING (low)
  ```

### `syntax-missing-alias`

- `trilogy file write answer_2910545909.preql --content import raw.store_sales as ss;

where
    ss.date.day_name = 'Monday'
    and ss.date.year between 1999 a…,
    first_name asc nulls first,
    city_display asc nulls first,
    total_net_profit asc nulls first,
    ticket_number asc nulls last
limit 100;`

  ```text
  refused to write 'answer_2910545909.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `grouping fields to ensure proper grain:
      ss.customer.sk as grouping_fields_to_ensure_proper_grain_s`
  Location:
  ...al_net_profit,     --grouping ??? fields to ensure proper grain:...

  Write stats: received 881 chars / 881 bytes; tail: …'asc nulls first,\\n    ticket_number asc nulls last\\nlimit 100;'.
  ```
