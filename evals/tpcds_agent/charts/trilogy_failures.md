# Trilogy failure analysis — 20260629-001912

- Run `20260629-001912` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 302 | failed: 56 (19%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 34 | 61% |
| `syntax-parse` | 17 | 30% |
| `syntax-missing-alias` | 2 | 4% |
| `type-error` | 2 | 4% |
| `join-resolution` | 1 | 2% |

## Detail

### `other`

- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 54 column 13 (char 2314). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query05.preql duckdb`

  ```text
  Syntax error in query05.preql: ORDER BY contains aggregate `grouping(combined.channel)` (line 44), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(combined.channel) as g order by g desc`.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 55 column 4 (char 1858). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query14.preql`

  ```text
  Resolution error in query14.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 4 disconnected subgraphs: {combined.brand_id, combined.category_id, combined.channel, combined.class_id}; {local._level, local.brand_id, local.category_id, local.channel, local.class_id}; {local._virt_agg_grouping_3634143651272896}; {local.group_sales}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query14.preql`

  ```text
  Referenced table \"vacuous\" not found!\nCandidate tables: \"ss_item_items\"\n\nLINE 202: ...(\"ss_item_items\".\"I_CATEGORY_ID\" as string)) ) in (((((cast(\"vacuous\".\"store_bcc_brand_id\" as string) || '|') || cast...\n                                                                         ^\n[SQL: \nWITH \nyummy as (\nSELECT\n    \"ss_store_sales\".\"SS_ITEM_SK\" as \"ss_item_id\",\n    \"ss_store_sales\".\"SS_SOLD_DATE_SK\" as \"ss_date_id\"\nFROM\n    \"store_sales\" as \"ss_store_sales\"\nGROUP BY\n    1,\n    2),\nabhorrent as (\nSELECT\n    \"ws_web_sales\".\"WS_ITEM_SK\" as \"ws_i
  …
  brand_id\" asc nulls first,\n    \"trite\".\"filtered_combined_class_id\" asc nulls first,\n    \"trite\".\"filtered_combined_category_id\" asc nulls first\nLIMIT (100)]\n[parameters: ('store', 'catalog', 'web')]\n(Background on this error at: https://sqlalche.me/e/20/f405)",
    "error_type": "ProgrammingError"
  }
  {
    "event": "summary",
    "statements": 1,
    "duration_ms": 781.197,
    "ok": false,
    "rows": 0
  }
  {
    "event": "output_truncated",
    "dropped_events": 1,
    "note": "Output exceeded the tool cap; trailing events dropped. Narrow the call (--regex, --show, fewer rows) to see the rest."
  }
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: Have {'GroupNode<cs.item.category_id,cs.item.class_id,local.in_catalog_99_01...1 more>': None, 'GroupNode<local.in_store_99_01,ss.item.category_id,ss.item.class_id...1 more>': None} and need ss.date.year@Grain<ss.date.id> between 1999 and 2001 and local.in_store_99_01 > 0 and local.in_catalog_99_01 > 0 and local.in_web_99_01 > 0
  ```
- `trilogy run query14.preql`

  ```text
  Resolution error in query14.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 3 disconnected subgraphs: {combined.brand_id, combined.category_id, combined.channel, combined.class_id}; {local._level, local.brand_id, local.category_id, local.channel, local.class_id}; {local._virt_agg_grouping_3634143651272896}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: ORDER BY contains aggregate `grouping(combined.brand_id)` (line 84), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(combined.brand_id) as g order by g desc`.
  ```
- `trilogy run query14.preql`

  ```text
  Resolution error in query14.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 3 disconnected subgraphs: {combined.brand_id, combined.category_id, combined.channel, combined.class_id}; {local._virt_agg_grouping_3634143651272896}; {local.brand_id, local.category_id, local.channel, local.class_id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: WHERE clause aggregate `sum(multiply(ref:ss.quantity,ref:ss.list_price))` is also computed in the SELECT (as `___tvf_arm_0_total_sales`); aggregate filters must use the HAVING clause - e.g. `having ___tvf_arm_0_total_sales > ...`; Line: 51
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: ORDER BY contains aggregate `grouping(filtered.channel)` (line 95), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(filtered.channel) as g order by g desc`.
  ```
- `trilogy run query14.preql`

  ```text
  Referenced table \"scrawny\" not found!\nCandidate tables: \"ss_date_date\"\n\nLINE 217: ...(\"ss_item_items\".\"I_CATEGORY_ID\" as string)) ) in (((((cast(\"scrawny\".\"all3_bcc_brand_id\" as string) || '|') || cast...\n                                                                         ^\n[SQL: \nWITH \nyummy as (\nSELECT\n    \"ss_store_sales\".\"SS_ITEM_SK\" as \"ss_item_id\"\nFROM\n    \"store_sales\" as \"ss_store_sales\"\n    INNER JOIN \"date_dim\" as \"ss_date_date\" on \"ss_store_sales\".\"SS_SOLD_DATE_SK\" = \"ss_date_date\".\"D_DATE_SK\"\nWHERE\n    \"ss_date_date\".\"D_YEAR\
  …
  ".\"filtered_brand_id\" asc nulls first,\n    \"tearful\".\"filtered_class_id\" asc nulls first,\n    \"tearful\".\"filtered_category_id\" asc nulls first\nLIMIT (100)]\n[parameters: ('catalog', 'web', 'store')]\n(Background on this error at: https://sqlalche.me/e/20/f405)",
    "error_type": "ProgrammingError"
  }
  {
    "event": "summary",
    "statements": 1,
    "duration_ms": 3048.572,
    "ok": false,
    "rows": 0
  }
  {
    "event": "output_truncated",
    "dropped_events": 1,
    "note": "Output exceeded the tool cap; trailing events dropped. Narrow the call (--regex, --show, fewer rows) to see the rest."
  }
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: ORDER BY contains aggregate `grouping(filtered.channel)` (line 77), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(filtered.channel) as g order by g desc`.
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: ORDER BY contains aggregate `grouping(combined.channel)` (line 86), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(combined.channel) as g order by g desc`.
  ```
- `trilogy run query14.preql`

  ```text
  Referenced table \"scrawny\" not found!\nCandidate tables: \"ss_date_date\"\n\nLINE 217: ...(\"ss_item_items\".\"I_CATEGORY_ID\" as string)) ) in (((((cast(\"scrawny\".\"all3_brand_id\" as string) || '|') || cast(\"scrawny...\n                                                                         ^\n[SQL: \nWITH \nyummy as (\nSELECT\n    \"ss_store_sales\".\"SS_ITEM_SK\" as \"ss_item_id\"\nFROM\n    \"store_sales\" as \"ss_store_sales\"\n    INNER JOIN \"date_dim\" as \"ss_date_date\" on \"ss_store_sales\".\"SS_SOLD_DATE_SK\" = \"ss_date_date\".\"D_DATE_SK\"\nWHERE\n    \"ss_date_date\".\"D
  …
  n\".\"combined_brand_id\" asc nulls first,\n    \"wooden\".\"combined_class_id\" asc nulls first,\n    \"wooden\".\"combined_category_id\" asc nulls first\nLIMIT (100)]\n[parameters: ('catalog', 'web', 'store')]\n(Background on this error at: https://sqlalche.me/e/20/f405)",
    "error_type": "ProgrammingError"
  }
  {
    "event": "summary",
    "statements": 1,
    "duration_ms": 4540.542,
    "ok": false,
    "rows": 0
  }
  {
    "event": "output_truncated",
    "dropped_events": 1,
    "note": "Output exceeded the tool cap; trailing events dropped. Narrow the call (--regex, --show, fewer rows) to see the rest."
  }
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: Undefined concept: catalog_sales.date. Suggestions: ['catalog_sales.date.id', 'catalog_sales.date.text_id', 'catalog_sales.date._date_string', 'catalog_sales.date.date', 'catalog_sales.date.year', 'catalog_sales.date.day_of_week']
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Undefined concept: store_sales.customer_demographics.marital_status. Suggestions: ['store_sales.customer.demographics.marital_status', 'store_sales.return_customer.demographics.marital_status', 'store_sales.customer_demographic.marital_status', 'store_returns.billing_customer.demographics.marital_status', 'store_returns.customer_demographic.marital_status', 'catalog_returns.sales.ship_customer.demographics.marital_status']
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 26). The requested concepts split into 2 disconnected subgraphs: {local._base_agg_cust_city, local._base_agg_cust_street_name, local._base_agg_cust_street_number, local._base_agg_cust_zip, local._base_agg_first_sales_year, local._base_agg_first_shipto_year, local._base_agg_item_text_id, local._base_agg_product_name, local._base_agg_sale_city, local._base_agg_sale_line_count, local._base_agg_sale_street_name, local._base_agg_sale_street_number, local._base_agg_sale_year, local._base_agg_sale_zip, local._base_agg_store_name, local._base_agg_store_zip, local._base_agg_total_coupon_amt, local._base_agg_total_list_price, local._base_agg_total_wholesale_cost, store_sales.customer.demographics.marital_status, store_sales.customer_demographic.marital_status, store_sales.is_returned, store_sales.item.id}; {local.item_color_ok, local.item_price_ok}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query70.preql`

  ```text
  Unexpected error in query70.preql: (_duckdb.BinderException) Binder Error: GROUPING statement cannot be used without groups

  LINE 161: 	WHEN grouping("sweltering"."ss_store_state") = 0 THEN "sweltering...
                 ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "ss_store_store"."S_STATE" as "_top_states_state"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "store" as "ss_store_store" on "ss_store_sales"."SS_STORE_SK" = "ss_store_store"."S_STORE_SK"
  WHERE
      "ss_date_date"."D_YEAR" = 2000

  GROUP BY
      1),
  questionable as (
  SELECT
      "ss_store_sales"."SS_STORE_SK" as "ss_store_id"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
  WHERE
      "ss_date_date"."D_YEAR" = 2000

  GROUP BY
      1,
      coalesce("ss_date_date"."D_DATE_SK","ss_store_sales"."SS_SOLD_DATE_SK")),
  cooperative as (
  SELECT
      "cheerful"."_top_states_state" as "top_states_state"
  FROM
      "cheerful"),
  abhorrent as (
  SELECT
      "ss_store_sales"."SS_NET_PROFIT" as "ss_net_profit",
      "ss_store_store"."S_COUNTY" as "ss_store_county",
      "ss_store_store"."S_STATE" as "ss_store_state"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
      INNER JOIN "store" as "ss_store_store" on "ss_store_sales"."SS_STORE_SK" = "ss_store_store"."S_STORE_SK"
  WHERE
      "ss_date_date"."D_YEAR" = 2000 and "ss_store_store"."S_STATE" in (select cooperative."top_states_state" from cooperative where cooperative."top_states_state" is not null)

  GROUP BY
      1,
      2,
      3,
      "ss_store_sales"."SS_ITEM_SK",
      "ss_store_sales"."SS_STORE_SK",
      "ss_store_sales"."SS_TICKET_NUMBER"),
  juicy as (
  SELECT
      "ss_store_sales"."SS_NET_PROFIT" as "ss_net_profit",
      "ss_store_store"."S_STATE" as "ss_store_state"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
      INNER JOIN "store" as "ss_store_store" on "ss_store_sales"."SS_STORE_SK" = "ss_store_store"."S_STORE_SK"
  WHERE
      "ss_date_date"."D_YEAR" = 2000 and "ss_store_store"."S_STATE" in (select cooperative."top_states_state" from cooperative where cooperative."top_states_state" is not null)

  GROUP BY
      1,
      2,
      "ss_store_sales"."SS_ITEM_SK",
      "ss_store_sales"."SS_TICKET_NUMBER"),
  yummy as (
  SELECT
      "ss_store_store"."S_COUNTY" as "ss_store_county",
      "ss_store_store"."S_STATE" as "ss_store_state"
  FROM
      "questionable"
      INNER JOIN "store" as "ss_store_store" on "questionable"."ss_store_id" = "ss_store_store"."S_STORE_SK"
  WHERE
      "ss_store_store"."S_STATE" in (select cooperative."top_states_state" from cooperative where cooperative."top_states_state" is not null)

  GROUP BY
      1,
      2),
  abundant as (
  SELECT
      "ss_store_store"."S_COUNTY" as "ss_store_county",
      "ss_store_store"."S_STATE" as "ss_store_state"
  FROM
      "questionable"
      INNER JOIN "store" as "ss_store_store" on "questionable"."ss_store_id" = "ss_store_store"."S_STORE_SK"
  WHERE
      "ss_store_store"."S_STATE" in (select cooperative."top_states_state" from cooperative where cooperative."top_states_state" is not null)

  GROUP BY
      1,
      2,
      "questionable"."ss_store_id"),
  sweltering as (
  SELECT
      "abhorrent"."ss_store_county" as "ss_store_county",
      "abhorrent"."ss_store_state" as "ss_store_state",
      grouping("abhorrent"."ss_store_county") as "_virt_agg_grouping_9558422171575547",
      grouping("abhorrent"."ss_store_state") as "_virt_agg_grouping_5741674531181097",
      sum("abhorrent"."ss_net_profit") as "total_net_profit"
  FROM
      "abhorrent"
  GROUP BY
      ROLLUP (2, 1)),
  vacuous as (
  SELECT
      "juicy"."ss_store_state" as "ss_store_state",
      sum("juicy"."ss_net_profit") as "_virt_agg_sum_1705566463686330"
  FROM
      "juicy"
  GROUP BY
      1),
  uneven as (
  SELECT
      "abundant"."ss_store_county" as "ss_store_county",
      "abundant"."ss_store_state" as "ss_store_state",
      grouping("abundant"."ss_store_county") as "_virt_agg_grouping_4453574046401692",
      grouping("abundant"."ss_store_state") as "_virt_agg_grouping_9769516219811737"
  FROM
      "abundant"
  GROUP BY
      1,
      2),
  concerned as (
  SELECT
      "vacuous"."_virt_agg_sum_1705566463686330" as "_virt_agg_sum_1705566463686330",
      "yummy"."ss_store_county" as "ss_store_county",
      "yummy"."ss_store_state" as "ss_store_state"
  FROM
      "yummy"
      LEFT OUTER JOIN "vacuous" on "yummy"."ss_store_state" = "vacuous"."ss_store_state"),
  young as (
  SELECT
      coalesce("concerned"."ss_store_county","uneven"."ss_store_county") as "ss_store_county",
      coalesce("concerned"."ss_store_state","uneven"."ss_store_state") as "ss_store_state",
      rank() over (partition by "uneven"."_virt_agg_grouping_4453574046401692" + "uneven"."_virt_agg_grouping_9769516219811737",CASE
  	WHEN "uneven"."_virt_agg_grouping_4453574046401692" = 0 THEN coalesce("concerned"."ss_store_state","uneven"."ss_store_state")
  	ELSE null
  	END order by "concerned"."_virt_agg_sum_1705566463686330" desc ) as "within_parent_rank"
  FROM
      "concerned"
      FULL JOIN "uneven" on "concerned"."ss_store_county" is not distinct from "uneven"."ss_store_county" AND "concerned"."ss_store_state" = "uneven"."ss_store_state")
  SELECT
      CASE
  	WHEN "sweltering"."_virt_agg_grouping_5741674531181097" = 1 THEN 'ALL STATES'
  	ELSE "sweltering"."ss_store_state"
  	END as "state",
      CASE
  	WHEN "sweltering"."_virt_agg_grouping_9558422171575547" = 1 THEN 'all counties'
  	ELSE "sweltering"."ss_store_county"
  	END as "county",
      "sweltering"."total_net_profit" as "total_net_profit",
      "sweltering"."_virt_agg_grouping_9558422171575547" + "sweltering"."_virt_agg_grouping_5741674531181097" as "hierarchy_level",
      "young"."within_parent_rank" as "within_parent_rank"
  FROM
      "sweltering"
      LEFT OUTER JOIN "young" on "sweltering"."ss_store_county" = "young"."ss_store_county" AND "sweltering"."ss_store_state" = "young"."ss_store_state"
  ORDER BY
      "hierarchy_level" desc,
      CASE
  	WHEN grouping("sweltering"."ss_store_state") = 0 THEN "sweltering"."ss_store_state"
  	ELSE null
  	END asc nulls first,
      "young"."within_parent_rank" asc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy file read query70.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query75.preql --format json`

  ```text
  Syntax error in query75.preql: Output column 'prev_qty' renames 'local.prev_qty' back to the name of an existing concept 'prev_qty' (defined at line 31) that 'local.prev_qty' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'prev_qty_out').
  ```
- `trilogy run query75.preql --format json`

  ```text
  Syntax error in query75.preql: Undefined concept: yearly.deduped.yr. Suggestions: ['deduped.yr', 'yearly.yr', '_deduped_yr']
  ```
- `trilogy run query75.preql --format json`

  ```text
  Syntax error in query75.preql: Undefined concept: yearly.deduped.yr. Suggestions: ['deduped.yr', 'yearly.yr', '_deduped_yr']
  ```
- `trilogy run query75.preql --format json`

  ```text
  Syntax error in query75.preql: Undefined concept: deduped.all_sales.item.brand_id. Suggestions: ['all_sales.item.brand_id', 'deduped.brand_id', 'brand_id', 'all_sales.item.brand_name', 'all_sales.item.manager_id']
  ```
- `trilogy run query75.preql --format json`

  ```text
  Syntax error in query75.preql: Undefined concept: deduped.all_sales.item.brand_id. Suggestions: ['all_sales.item.brand_id', 'deduped.brand_id', 'brand_id', 'all_sales.item.brand_name', 'all_sales.item.manager_id']
  ```
- `trilogy run query75.preql --format json`

  ```text
  Syntax error in query75.preql: Undefined concept: deduped.all_sales.item.brand_id. Suggestions: ['all_sales.item.brand_id', 'deduped.brand_id', 'all_sales.item.brand_name', 'all_sales.item.manager_id']
  ```
- `trilogy run query75.preql --format json`

  ```text
  Syntax error in query75.preql: ORDER BY references 'deduped.net_amt', which is not in the SELECT projection (line 18). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --deduped.net_amt order by deduped.net_amt asc`.
  ```
- `trilogy run query75.preql --format json`

  ```text
  Syntax error in query75.preql: Undefined concept: yearly_agg.per_line.yr. Suggestions: ['per_line.yr', 'yearly_agg.yr', '_yearly_agg_yr']
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query78.preql`

  ```text
  trilogy error: subprocess timed out after 600s.
  ```
- `trilogy run query97.preql`

  ```text
  Unexpected error in query97.preql: Cannot join `store_set.cust_id` to itself (`store_set.cust_id` resolves to the same key `store_set.cust_id`), which degenerates to `1=1`. Join distinct keys (e.g. separate rowset outputs or distinct expressions).
  ```

### `syntax-parse`

- `trilogy file write query02.preql --content import raw.web_sales as web;
import raw.catalog_sales as cat;

# Aggregate each channel by (week_seq, day_of_week)…  @day_ratio(4) as thursday,
    @day_ratio(5) as friday,
    @day_ratio(6) as saturday
order by daily_with_next.week_seq asc nulls first
limit 100;
`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
    --> 41:63
     |
  41 | auto ws_2001 <- min(web.date.week_seq ? web.date.year = 2001) as ws_min;
     |                                                               ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, aggregate_over, or window_sql_over
  Location:
  ...k_seq ? web.date.year = 2001) ??? as ws_min; auto ws_2001_max <-...

  Write stats: received 1926 chars / 1926 bytes; tail: …'rder by daily_with_next.week_seq asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as s;

# Channel type label from enum
auto ch_label <- case
    when s.channel = 'STORE' then…r by grouping(combined.channel) + grouping(combined.entity) asc,
    combined.channel asc nulls first,
    combined.entity asc nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` — a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...s_amt, return_amt, profit_amt) ??? select     case when grouping...

  Write stats: received 2141 chars / 2141 bytes; tail: …' nulls first,\\n    combined.entity asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query14.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;
import raw.item as item;…st,
         combined.brand_id asc nulls first,
         combined.class_id asc nulls first,
         combined.category_id asc nulls first
limit 100;
`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
    --> 26:6
     |
  26 |     (import raw.store_sales as ss;
     |      ^---
     |
     = expected select_statement
  Location:
   with combined as union(     ( ??? import raw.store_sales as ss;

  Write stats: received 4217 chars / 4217 bytes; tail: …'t,\\n         combined.category_id asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query14.preql -e -c import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Overall average sale valu…st,
         combined.brand_id asc nulls first,
         combined.class_id asc nulls first,
         combined.category_id asc nulls first
limit 100;
`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
     --> 127:1
      |
  127 | having
      | ^---
      |
      = expected limit or order_by
  Location:
  ...ass_id, combined.category_id) ??? having     sum(combined.total_...

  Write stats: received 5658 chars / 5658 bytes; tail: …'t,\\n         combined.category_id asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query14.preql -e -c import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Overall average sale valu…st,
         combined.brand_id asc nulls first,
         combined.class_id asc nulls first,
         combined.category_id asc nulls first
limit 100;
`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
     --> 127:1
      |
  127 | having
      | ^---
      |
      = expected limit or order_by
  Location:
  ...ass_id, combined.category_id) ??? having     sum(combined.total_...

  Write stats: received 5658 chars / 5658 bytes; tail: …'t,\\n         combined.category_id asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query14.preql -e -c import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Overall average sale valu…st,
         combined.brand_id asc nulls first,
         combined.class_id asc nulls first,
         combined.category_id asc nulls first
limit 100;
`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
    --> 17:6
     |
  17 | auto bcc_key(brand_id int, class_id int, category_id int) <- brand_id::string || '|' || class_id::string || '|' || category_id::string;
     |      ^---
     |
     = expected prop_ident or prop_ident_wildcard
  Location:
  ...);  # Key builder helper auto ??? bcc_key(brand_id int, class_id...

  Write stats: received 5775 chars / 5775 bytes; tail: …'t,\\n         combined.category_id asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query14.preql -e -c import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Overall average sale valu…st,
         combined.brand_id asc nulls first,
         combined.class_id asc nulls first,
         combined.category_id asc nulls first
limit 100;
`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
    --> 19:6
     |
  19 |     (import raw.store_sales as ss;
     |      ^---
     |
     = expected select_statement
  Location:
   with combined as union(     ( ??? import raw.store_sales as ss;

  Write stats: received 6113 chars / 6113 bytes; tail: …'t,\\n         combined.category_id asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query14.preql -e -c import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Overall average sale valu…st,
         combined.brand_id asc nulls first,
         combined.class_id asc nulls first,
         combined.category_id asc nulls first
limit 100;
`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
    --> 23:17
     |
  23 |       rowset <- select cs.item.brand_id as b, cs.item.class_id as c, cs.item.category_id as cat
     |                 ^---
     |
     = expected access_chain
  Location:
  ...:string) in (       rowset <- ??? select cs.item.brand_id as b,

  Write stats: received 4706 chars / 4706 bytes; tail: …'t,\\n         combined.category_id asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query14.preql -e -c import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Overall average sale valu…st,
         combined.brand_id asc nulls first,
         combined.class_id asc nulls first,
         combined.category_id asc nulls first
limit 100;
`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy does not support subqueries — joins are auto-resolved from dotted paths. To filter on a value that lives on a related dimension, reference its dot-path directly. Example: instead of `where ss.store_id in (select store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ....item.category_id::string) in ??? (       select cs.item.brand_i...

  Write stats: received 4845 chars / 4845 bytes; tail: …'t,\\n         combined.category_id asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query14.preql -e -c import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Overall average sale valu…brand_id as brand_id,
        ws.item.class_id as class_id,
        ws.item.category_id as category_id
    )
) -> (brand_id, class_id, category_id);
`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword — a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...een 1999 and 2001      select ??? distinct         ss.item.brand...

  Write stats: received 2242 chars / 2242 bytes; tail: …'s category_id\\n    )\\n) -> (brand_id, class_id, category_id);\\n'.
  ```
- `trilogy file write query14.preql -e -c import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Overall average sale valu….item.brand_id::string || '|' || ws.item.class_id::string || '|' || ws.item.category_id::string
      where ws.date.year between 1999 and 2001
  )
;
`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy does not support subqueries — joins are auto-resolved from dotted paths. To filter on a value that lives on a related dimension, reference its dot-path directly. Example: instead of `where ss.store_id in (select store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ....item.category_id::string) in ??? (       select cs.item.brand_i...

  Write stats: received 1509 chars / 1509 bytes; tail: …'string\\n      where ws.date.year between 1999 and 2001\\n  )\\n;\\n'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…sum(combined.line_total) as total_sales
order by
    combined.last_name asc,
    combined.first_name asc,
    total_sales asc nulls first
limit 100;
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:66
    |
  7 | auto frequent_item_desc <- item.desc ? (count(store_sales.item.id, store_sales.date.date ? year(store_sales.date.date) between 2000 and 2003) by substring(item.desc, 1, 30)) > 4;
    |                                                                  ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...c ? (count(store_sales.item.id ??? , store_sales.date.date ? year...

  Write stats: received 2625 chars / 2625 bytes; tail: …'.first_name asc,\\n    total_sales asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query70.preql -e -c import raw.store_sales as ss;

# Top 5 states by total net profit from store sales in year 2000
auto top_states <- 
  … hierarchy_level,
by rollup (ss.store.state, ss.store.county)
order by hierarchy_level desc, state asc nulls first, county asc nulls first
limit 100;`

  ```text
  refused to write 'query70.preql': not syntactically valid Trilogy.

  Parse error:
   --> 5:5
    |
  5 |     select
    |     ^---
    |
    = expected _and_conditional
  Location:
   2000 auto top_states <-      ??? select         ss.store.state

  Write stats: received 841 chars / 841 bytes; tail: …'sc, state asc nulls first, county asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query75.preql --content import raw.all_sales as all_sales;

# Deduplicate per-line records before aggregating
rowset deduped <- select
   …diff
having
    prev_qty is not null
    and (yearly.total_qty::float / prev_qty::float) < 0.9
order by
    qty_diff asc,
    amt_diff asc
limit 100;`

  ```text
  refused to write 'query75.preql': not syntactically valid Trilogy.

  Parse error:
    --> 18:1
     |
  18 | rowset yearly as
     | ^---
     |
     = expected EOI, block, or show_statement
  Location:
  ...egate to year + dimension IDs ??? rowset yearly as select     de...

  Write stats: received 1982 chars / 1982 bytes; tail: …'< 0.9\\norder by\\n    qty_diff asc,\\n    amt_diff asc\\nlimit 100;'.
  ```
- `trilogy run --import raw/all_sales:all_sales --format json select all_sales.date.year, count(all_sales.item.id) as cnt where all_sales.item.category = 'Books' group by 1 order by 1;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...sales.item.category = 'Books' ??? group by 1 order by 1;
  ```
- `trilogy file write query78.preql -e -c import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sales as cs;

# Store sales: year 2000, i…tore_wc desc,
    store_agg.store_sp desc,
    other_agg.other_qty asc,
    other_agg.other_wc asc,
    other_agg.other_sp asc,
    ratio
limit 100;
`

  ```text
  refused to write 'query78.preql': not syntactically valid Trilogy.

  Parse error:
   --> 6:1
    |
  6 | rowset store_agg as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...fied customer, never returned ??? rowset store_agg as where ss.d...

  Write stats: received 2617 chars / 2617 bytes; tail: …'er_wc asc,\\n    other_agg.other_sp asc,\\n    ratio\\nlimit 100;\\n'.
  ```
- `trilogy file write query97.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;

# Distinct (customer, item) pairs from store sales …m.id as item_id
;

# Try a simpler full join - just on cust_id
select count(*) as total
full join store_set.cust_id = catalog_set.cust_id
limit 100;
`

  ```text
  refused to write 'query97.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [223]: `*` is not a valid argument — Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) — e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...join - just on cust_id select ??? count(*) as total full join st...

  Write stats: received 675 chars / 675 bytes; tail: …'ull join store_set.cust_id = catalog_set.cust_id\\nlimit 100;\\n'.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw.date:date select min(date.week_seq), max(date.week_seq) where date.year = 2001;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `min(date.week_seq) as week_seq_min`
  Location:
  ...ate; select min(date.week_seq) ??? , max(date.week_seq) where dat...
  ```
- `trilogy file write query14.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Overall average sale …st,
         combined.brand_id asc nulls first,
         combined.class_id asc nulls first,
         combined.category_id asc nulls first
limit 100;
`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `with catalog_bcc as with_catalog_bcc`
  Location:
  ...egory_id as category_id, with ??? catalog_bcc as where cs.sold_d...

  Write stats: received 5707 chars / 5707 bytes; tail: …'t,\\n         combined.category_id asc nulls first\\nlimit 100;\\n'.
  ```

### `type-error`

- `trilogy run query14.preql`

  ```text
  Type error in query14.preql: Invalid argument type 'INTEGER' passed into CONCAT function in position 1 from concept: ss.item.brand_id. Valid: 'STRING'.
  ```
- `trilogy run query14.preql`

  ```text
  Type error in query14.preql: Invalid argument type 'INTEGER' passed into CONCAT function in position 1 from concept: ss.item.brand_id. Valid: 'STRING'.
  ```

### `join-resolution`

- `trilogy run query14.preql`

  ```text
  Resolution error in query14.preql: Could not resolve connections for query with output ['local.___tvf_arm_0_channel<Purpose.CONSTANT>Derivation.CONSTANT>', 'local.___tvf_arm_0_brand_id<Purpose.PROPERTY>Derivation.BASIC>', 'local.___tvf_arm_0_class_id<Purpose.PROPERTY>Derivation.BASIC>', 'local.___tvf_arm_0_category_id<Purpose.PROPERTY>Derivation.BASIC>', 'local.___tvf_arm_0_total_sales<Purpose.METRIC>Derivation.AGGREGATE>', 'local.___tvf_arm_0_total_count<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
